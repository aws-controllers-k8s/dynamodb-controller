// Copyright Amazon.com Inc. or its affiliates. All Rights Reserved.
//
// Licensed under the Apache License, Version 2.0 (the "License"). You may
// not use this file except in compliance with the License. A copy of the
// License is located at
//
//     http://aws.amazon.com/apache2.0/
//
// or in the "license" file accompanying this file. This file is distributed
// on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either
// express or implied. See the License for the specific language governing
// permissions and limitations under the License.

package table

import (
	"context"
	"errors"
	"strings"
	"time"

	ackcompare "github.com/aws-controllers-k8s/runtime/pkg/compare"
	ackerr "github.com/aws-controllers-k8s/runtime/pkg/errors"
	ackrequeue "github.com/aws-controllers-k8s/runtime/pkg/requeue"
	ackrtlog "github.com/aws-controllers-k8s/runtime/pkg/runtime/log"
	ackutil "github.com/aws-controllers-k8s/runtime/pkg/util"
	svcsdk "github.com/aws/aws-sdk-go/service/dynamodb"
	corev1 "k8s.io/api/core/v1"

	"github.com/aws-controllers-k8s/dynamodb-controller/apis/v1alpha1"
)

var (
	ErrTableDeleting = errors.New("Table in 'DELETING' state, cannot be modified or deleted")
	ErrTableCreating = errors.New("Table in 'CREATING' state, cannot be modified or deleted")
	ErrTableUpdating = errors.New("Table in 'UPDATING' state, cannot be modified or deleted")
)

var (
	// TerminalStatuses are the status strings that are terminal states for a
	// DynamoDB table
	TerminalStatuses = []v1alpha1.TableStatus_SDK{
		v1alpha1.TableStatus_SDK_ARCHIVING,
		v1alpha1.TableStatus_SDK_DELETING,
	}
)

var DefaultTTLEnabledValue = false

var (
	requeueWaitWhileDeleting = ackrequeue.NeededAfter(
		ErrTableDeleting,
		5*time.Second,
	)
	requeueWaitWhileCreating = ackrequeue.NeededAfter(
		ErrTableCreating,
		5*time.Second,
	)
	requeueWaitWhileUpdating = ackrequeue.NeededAfter(
		ErrTableUpdating,
		5*time.Second,
	)
)

// tableHasTerminalStatus returns whether the supplied Dynamodb table is in a
// terminal state
func tableHasTerminalStatus(r *resource) bool {
	if r.ko.Status.TableStatus == nil {
		return false
	}
	ts := *r.ko.Status.TableStatus
	for _, s := range TerminalStatuses {
		if ts == string(s) {
			return true
		}
	}
	return false
}

// isTableCreating returns true if the supplied DynamodbDB table is in the process
// of being created
func isTableCreating(r *resource) bool {
	if r.ko.Status.TableStatus == nil {
		return false
	}
	dbis := *r.ko.Status.TableStatus
	return dbis == string(v1alpha1.TableStatus_SDK_CREATING)
}

// isTableDeleting returns true if the supplied DynamodbDB table is in the process
// of being deleted
func isTableDeleting(r *resource) bool {
	if r.ko.Status.TableStatus == nil {
		return false
	}
	dbis := *r.ko.Status.TableStatus
	return dbis == string(v1alpha1.TableStatus_SDK_DELETING)
}

// isTableUpdating returns true if the supplied DynamodbDB table is in the process
// of being deleted
func isTableUpdating(r *resource) bool {
	if r.ko.Status.TableStatus == nil {
		return false
	}
	dbis := *r.ko.Status.TableStatus
	return dbis == string(v1alpha1.TableStatus_SDK_UPDATING)
}

func (rm *resourceManager) customUpdateTable(
	ctx context.Context,
	desired *resource,
	latest *resource,
	delta *ackcompare.Delta,
) (updated *resource, err error) {
	rlog := ackrtlog.FromContext(ctx)
	exit := rlog.Trace("rm.customUpdateTable")
	defer func(err error) { exit(err) }(err)

	if isTableDeleting(latest) {
		msg := "table is currently being deleted"
		setSyncedCondition(desired, corev1.ConditionFalse, &msg, nil)
		return desired, requeueWaitWhileDeleting
	}
	if isTableCreating(latest) {
		msg := "table is currently being created"
		setSyncedCondition(desired, corev1.ConditionFalse, &msg, nil)
		return desired, requeueWaitWhileCreating
	}
	if isTableUpdating(latest) {
		msg := "table is currently being updated"
		setSyncedCondition(desired, corev1.ConditionFalse, &msg, nil)
		return desired, requeueWaitWhileUpdating
	}
	if tableHasTerminalStatus(latest) {
		msg := "table is in '" + *latest.ko.Status.TableStatus + "' status"
		setTerminalCondition(desired, corev1.ConditionTrue, &msg, nil)
		setSyncedCondition(desired, corev1.ConditionTrue, nil, nil)
		return desired, nil
	}

	// Merge in the information we read from the API call above to the copy of
	// the original Kubernetes object we passed to the function
	ko := desired.ko.DeepCopy()
	rm.setStatusDefaults(ko)

	if delta.DifferentAt("Spec.TimeToLive") {
		if err := rm.syncTTL(ctx, latest, desired); err != nil {
			// Ignore "already disabled errors"
			if awsErr, ok := ackerr.AWSError(err); ok && !(awsErr.Code() == "ValidationException" &&
				strings.HasPrefix(awsErr.Message(), "TimeToLive is already disabled")) {
				return nil, err
			}
		}
	}
	if delta.DifferentAt("Spec.Tags") {
		if err := rm.syncTableTags(ctx, latest, desired); err != nil {
			return nil, err
		}
	}

	// TODO(hilalymh): support updating all table field
	return &resource{ko}, nil
}

// syncTTL updates a dynamodb table's TimeToLive property.
func (rm *resourceManager) syncTTL(
	ctx context.Context,
	latest *resource,
	desired *resource,
) (err error) {
	rlog := ackrtlog.FromContext(ctx)
	exit := rlog.Trace("rm.syncTTL")
	defer func(err error) { exit(err) }(err)

	spec := &svcsdk.TimeToLiveSpecification{}
	if desired.ko.Spec.TimeToLive != nil {
		spec.AttributeName = desired.ko.Spec.TimeToLive.AttributeName
		spec.Enabled = desired.ko.Spec.TimeToLive.Enabled
	} else {
		// In order to disable the TTL, we can't simply call the
		// `UpdateTimeToLive` method with an empty specification. Instead, we
		// must explicitly set the enabled to false and provide the attribute
		// name of the existing TTL.
		currentAttrName := ""
		if latest.ko.Spec.TimeToLive.AttributeName != nil {
			currentAttrName = *latest.ko.Spec.TimeToLive.AttributeName
		}

		spec.SetAttributeName(currentAttrName)
		spec.SetEnabled(false)
	}

	_, err = rm.sdkapi.UpdateTimeToLiveWithContext(
		ctx,
		&svcsdk.UpdateTimeToLiveInput{
			TableName:               desired.ko.Spec.TableName,
			TimeToLiveSpecification: spec,
		},
	)
	rm.metrics.RecordAPICall("UPDATE", "UpdateTimeToLive", err)
	return err
}

// syncTableTags updates a dynamodb table tags.
//
// TODO(hilalymh): move this function to a common utility file. This function can be reused
// to tag GlobalTable resources.
func (rm *resourceManager) syncTableTags(
	ctx context.Context,
	latest *resource,
	desired *resource,
) (err error) {
	rlog := ackrtlog.FromContext(ctx)
	exit := rlog.Trace("rm.syncTableTags")
	defer func(err error) { exit(err) }(err)

	added, updated, removed := computeTagsDelta(latest.ko.Spec.Tags, desired.ko.Spec.Tags)

	// There are no API calls to update an existing tag. To update a tag we will have to first
	// delete it and then recreate it with the new value.

	// Tags to remove
	for _, updatedTag := range updated {
		removed = append(removed, updatedTag.Key)
	}
	// Tags to create
	added = append(added, updated...)

	if len(removed) > 0 {
		_, err = rm.sdkapi.UntagResourceWithContext(
			ctx,
			&svcsdk.UntagResourceInput{
				ResourceArn: (*string)(latest.ko.Status.ACKResourceMetadata.ARN),
				TagKeys:     removed,
			},
		)
		rm.metrics.RecordAPICall("UPDATE", "UntagResource", err)
		if err != nil {
			return err
		}
	}

	if len(added) > 0 {
		_, err = rm.sdkapi.TagResourceWithContext(
			ctx,
			&svcsdk.TagResourceInput{
				ResourceArn: (*string)(latest.ko.Status.ACKResourceMetadata.ARN),
				Tags:        sdkTagsFromResourceTags(added),
			},
		)
		rm.metrics.RecordAPICall("UPDATE", "TagResource", err)
		if err != nil {
			return err
		}
	}
	return nil
}

// equalTags returns true if two Tag arrays are equal regardless of the order
// of their elements.
func equalTags(
	a []*v1alpha1.Tag,
	b []*v1alpha1.Tag,
) bool {
	added, updated, removed := computeTagsDelta(a, b)
	return len(added) == 0 && len(updated) == 0 && len(removed) == 0
}

// resourceTagsFromSDKTags transforms a *svcsdk.Tag array to a *v1alpha1.Tag array.
func resourceTagsFromSDKTags(svcTags []*svcsdk.Tag) []*v1alpha1.Tag {
	tags := make([]*v1alpha1.Tag, len(svcTags))
	for i := range svcTags {
		tags[i] = &v1alpha1.Tag{
			Key:   svcTags[i].Key,
			Value: svcTags[i].Value,
		}
	}
	return tags
}

// svcTagsFromResourceTags transforms a *v1alpha1.Tag array to a *svcsdk.Tag array.
func sdkTagsFromResourceTags(rTags []*v1alpha1.Tag) []*svcsdk.Tag {
	tags := make([]*svcsdk.Tag, len(rTags))
	for i := range rTags {
		tags[i] = &svcsdk.Tag{
			Key:   rTags[i].Key,
			Value: rTags[i].Value,
		}
	}
	return tags
}

// computeTagsDelta compares two Tag arrays and return three different list
// containing the added, updated and removed tags.
// The removed tags only contains the Key of tags
func computeTagsDelta(
	a []*v1alpha1.Tag,
	b []*v1alpha1.Tag,
) (added, updated []*v1alpha1.Tag, removed []*string) {
	var visitedIndexes []string
mainLoop:
	for _, aElement := range a {
		visitedIndexes = append(visitedIndexes, *aElement.Key)
		for _, bElement := range b {
			if equalStrings(aElement.Key, bElement.Key) {
				if !equalStrings(aElement.Value, bElement.Value) {
					updated = append(updated, bElement)
				}
				continue mainLoop
			}
		}
		removed = append(removed, aElement.Key)
	}
	for _, bElement := range b {
		if !ackutil.InStrings(*bElement.Key, visitedIndexes) {
			added = append(added, bElement)
		}
	}
	return added, updated, removed
}

func equalStrings(a, b *string) bool {
	if a == nil {
		return b == nil || *b == ""
	}
	return (*a == "" && b == nil) || *a == *b
}

// setResourceAdditionalFields will describe the fields that are not return by
// DescribeTable calls
func (rm *resourceManager) setResourceAdditionalFields(
	ctx context.Context,
	ko *v1alpha1.Table,
) (err error) {
	rlog := ackrtlog.FromContext(ctx)
	exit := rlog.Trace("rm.setResourceAdditionalFields")
	defer func(err error) { exit(err) }(err)

	ko.Spec.Tags, err = rm.getResourceTagsPagesWithContext(ctx, string(*ko.Status.ACKResourceMetadata.ARN))
	if err != nil {
		return err
	}
	ko.Spec.TimeToLive, err = rm.getResourceTTLWithContext(ctx, ko.Spec.TableName)
	if err != nil {
		return err
	}

	return nil
}

// getResourceTagsPagesWithContext queries the list of tags of a given resource.
func (rm *resourceManager) getResourceTagsPagesWithContext(ctx context.Context, resourceARN string) ([]*v1alpha1.Tag, error) {
	var err error
	rlog := ackrtlog.FromContext(ctx)
	exit := rlog.Trace("rm.getResourceTagsPagesWithContext")
	defer func(err error) { exit(err) }(err)

	tags := []*v1alpha1.Tag{}

	var token *string = nil
	for {
		var listTagsOfResourceOutput *svcsdk.ListTagsOfResourceOutput
		listTagsOfResourceOutput, err = rm.sdkapi.ListTagsOfResourceWithContext(
			ctx,
			&svcsdk.ListTagsOfResourceInput{
				NextToken:   token,
				ResourceArn: &resourceARN,
			},
		)
		rm.metrics.RecordAPICall("GET", "ListTagsOfResource", err)
		if err != nil {
			return nil, err
		}
		tags = append(tags, resourceTagsFromSDKTags(listTagsOfResourceOutput.Tags)...)
		if listTagsOfResourceOutput.NextToken == nil {
			break
		}
		token = listTagsOfResourceOutput.NextToken
	}
	return tags, nil
}

// getResourceTTLWithContext queries the table TTL of a given resource.
func (rm *resourceManager) getResourceTTLWithContext(ctx context.Context, tableName *string) (*v1alpha1.TimeToLiveSpecification, error) {
	var err error
	rlog := ackrtlog.FromContext(ctx)
	exit := rlog.Trace("rm.getResourceTTLWithContext")
	defer func(err error) { exit(err) }(err)

	res, err := rm.sdkapi.DescribeTimeToLiveWithContext(
		ctx,
		&svcsdk.DescribeTimeToLiveInput{
			TableName: tableName,
		},
	)
	rm.metrics.RecordAPICall("GET", "DescribeTimeToLive", err)
	if err != nil {
		return nil, err
	}

	// Treat status "ENABLING" and "ENABLED" as `Enabled` == true
	isEnabled := *res.TimeToLiveDescription.TimeToLiveStatus == svcsdk.TimeToLiveStatusEnabled ||
		*res.TimeToLiveDescription.TimeToLiveStatus == svcsdk.TimeToLiveStatusEnabling

	return &v1alpha1.TimeToLiveSpecification{
		AttributeName: res.TimeToLiveDescription.AttributeName,
		Enabled:       &isEnabled,
	}, nil
}

func customPreCompare(
	delta *ackcompare.Delta,
	a *resource,
	b *resource,
) {
	// TODO(hilalymh): customDeltaFunctions for AttributeDefintions
	// TODO(hilalymh): customDeltaFunctions for GlobalSecondaryIndexes

	if len(a.ko.Spec.Tags) != len(b.ko.Spec.Tags) {
		delta.Add("Spec.Tags", a.ko.Spec.Tags, b.ko.Spec.Tags)
	} else if a.ko.Spec.Tags != nil && b.ko.Spec.Tags != nil {
		if !equalTags(a.ko.Spec.Tags, b.ko.Spec.Tags) {
			delta.Add("Spec.Tags", a.ko.Spec.Tags, b.ko.Spec.Tags)
		}
	}

	if a.ko.Spec.TimeToLive == nil && b.ko.Spec.TimeToLive != nil {
		a.ko.Spec.TimeToLive = &v1alpha1.TimeToLiveSpecification{
			Enabled: &DefaultTTLEnabledValue,
		}
	}
}
