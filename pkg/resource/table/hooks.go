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
	"fmt"
	"time"

	"github.com/aws-controllers-k8s/dynamodb-controller/apis/v1alpha1"
	svcapitypes "github.com/aws-controllers-k8s/dynamodb-controller/apis/v1alpha1"
	ackcompare "github.com/aws-controllers-k8s/runtime/pkg/compare"
	ackrequeue "github.com/aws-controllers-k8s/runtime/pkg/requeue"
	ackrtlog "github.com/aws-controllers-k8s/runtime/pkg/runtime/log"
	"github.com/aws/aws-sdk-go/aws"
	svcsdk "github.com/aws/aws-sdk-go/service/dynamodb"
	corev1 "k8s.io/api/core/v1"
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
	fmt.Println("\nCalled custom update")

	rlog := ackrtlog.FromContext(ctx)
	exit := rlog.Trace("rm.customUpdateTable")
	defer exit(err)

	// Merge in the information we read from the API call above to the copy of
	// the original Kubernetes object we passed to the function
	ko := desired.ko.DeepCopy()
	rm.setStatusDefaults(ko)

	// Quoting from https://docs.aws.amazon.com/amazondynamodb/latest/APIReference/API_UpdateTable.html
	//
	// You can only perform one of the following operations at once:
	// - Modify the provisioned throughput settings of the table.
	// - Enable or disable DynamoDB Streams on the table.
	// - Remove a global secondary index from the table.
	//
	// Create a new global secondary index on the table. After the index begins
	// backfilling, you can use UpdateTable to perform other operations. UpdateTable
	// is an asynchronous operation; while it is executing, the table status changes
	// from ACTIVE to UPDATING. While it is UPDATING, you cannot issue another
	// UpdateTable request. When the table returns to the ACTIVE state, the
	// UpdateTable operation is complete.

	if delta.DifferentAt("Spec.AttributeDefinitions") ||
		delta.DifferentAt("Spec.BillingMode") ||
		delta.DifferentAt("Spec.StreamSpecification") ||
		delta.DifferentAt("Spec.SEESpecification") {

		fmt.Println("+++++++++++++++++++++++++")
		debugAll(desired.ko.Spec.AttributeDefinitions, latest.ko.Spec.AttributeDefinitions)

		if err := rm.syncTable(ctx, desired, delta); err != nil {
			return nil, err
		}
	}

	if delta.DifferentAt("Spec.Tags") {
		if err := rm.syncTableTags(ctx, latest, desired); err != nil {
			return nil, err
		}
	}

	// We only want to call one those updates at once. Priority to the fastest
	// operations.
	switch {
	case delta.DifferentAt("Spec.ProvisionedThroughput"):
		if err := rm.syncTableProvisionedThroughput(ctx, desired); err != nil {
			return nil, err
		}

	case delta.DifferentAt("Spec.GlobalSecondaryIndexes"):
		if !delta.DifferentAt("Spec.AttributeDefinitions") {
			return nil, fmt.Errorf("TODO")
		}

		if err := rm.syncTableGlobalSecondaryIndexes(ctx, latest, desired); err != nil {
			return nil, err
		}
	}

	return &resource{ko}, nil
}

func (rm *resourceManager) syncTableGlobalSecondaryIndexes(
	ctx context.Context,
	latest *resource,
	desired *resource,
) (err error) {
	rlog := ackrtlog.FromContext(ctx)
	exit := rlog.Trace("rm.syncTableGlobalSecondaryIndexes")
	defer exit(err)

	input, err := rm.newUpdateTableGlobalSecondaryIndexUpdatesPayload(ctx, latest, desired)
	if err != nil {
		return err
	}

	_, err = rm.sdkapi.UpdateTable(input)
	rm.metrics.RecordAPICall("UPDATE", "UpdateTable", err)
	if err != nil {
		return err
	}
	return
}

func (rm *resourceManager) newUpdateTableGlobalSecondaryIndexUpdatesPayload(
	ctx context.Context,
	latest *resource,
	desired *resource,
) (*svcsdk.UpdateTableInput, error) {
	addedGSIs, updatedGSIs, removedGSIs := computeGlobalSecondaryIndexDelta(
		latest.ko.Spec.GlobalSecondaryIndexes,
		desired.ko.Spec.GlobalSecondaryIndexes,
	)
	input := &svcsdk.UpdateTableInput{
		TableName: aws.String(*latest.ko.Spec.TableName),
	}
	for _, addedGSI := range addedGSIs {
		update := &svcsdk.GlobalSecondaryIndexUpdate{
			Create: &svcsdk.CreateGlobalSecondaryIndexAction{
				IndexName:             aws.String(*addedGSI.IndexName),
				Projection:            newSDKProjection(addedGSI.Projection),
				KeySchema:             newSDKKeySchemaArray(addedGSI.KeySchema),
				ProvisionedThroughput: newSDKProvisionedThroughtput(addedGSI.ProvisionedThroughput),
			},
		}
		input.GlobalSecondaryIndexUpdates = append(input.GlobalSecondaryIndexUpdates, update)
	}

	for _, updatedGSI := range updatedGSIs {
		update := &svcsdk.GlobalSecondaryIndexUpdate{
			Update: &svcsdk.UpdateGlobalSecondaryIndexAction{
				IndexName:             aws.String(*updatedGSI.IndexName),
				ProvisionedThroughput: newSDKProvisionedThroughtput(updatedGSI.ProvisionedThroughput),
			},
		}
		input.GlobalSecondaryIndexUpdates = append(input.GlobalSecondaryIndexUpdates, update)
	}

	for _, removedGSI := range removedGSIs {
		update := &svcsdk.GlobalSecondaryIndexUpdate{
			Delete: &svcsdk.DeleteGlobalSecondaryIndexAction{
				IndexName: &removedGSI,
			},
		}
		input.GlobalSecondaryIndexUpdates = append(input.GlobalSecondaryIndexUpdates, update)
	}

	return input, nil
}

func newSDKProvisionedThroughtput(pt *v1alpha1.ProvisionedThroughput) *svcsdk.ProvisionedThroughput {
	provisionedThroughput := &svcsdk.ProvisionedThroughput{}
	if pt != nil {
		if pt.ReadCapacityUnits != nil {
			provisionedThroughput.ReadCapacityUnits = aws.Int64(*pt.ReadCapacityUnits)
		} else {
			provisionedThroughput.ReadCapacityUnits = aws.Int64(0)
		}
		if pt.WriteCapacityUnits != nil {
			provisionedThroughput.WriteCapacityUnits = aws.Int64(*pt.WriteCapacityUnits)
		} else {
			provisionedThroughput.WriteCapacityUnits = aws.Int64(0)
		}
	} else {
		provisionedThroughput.ReadCapacityUnits = aws.Int64(0)
		provisionedThroughput.WriteCapacityUnits = aws.Int64(0)
	}
	return provisionedThroughput
}

func newSDKProjection(p *v1alpha1.Projection) *svcsdk.Projection {
	projection := &svcsdk.Projection{}
	if p != nil {
		if p.ProjectionType != nil {
			projection.ProjectionType = aws.String(*p.ProjectionType)
		} else {
			projection.ProjectionType = aws.String("")
		}
		if p.NonKeyAttributes != nil {
			projection.NonKeyAttributes = p.NonKeyAttributes
		} else {
			projection.NonKeyAttributes = []*string{}
		}
	} else {
		projection.ProjectionType = aws.String("")
		projection.NonKeyAttributes = []*string{}
	}
	return projection
}

func newSDKKeySchemaArray(kss []*v1alpha1.KeySchemaElement) []*svcsdk.KeySchemaElement {
	keySchemas := []*svcsdk.KeySchemaElement{}
	for _, ks := range kss {
		keySchema := &svcsdk.KeySchemaElement{}
		if ks != nil {
			if ks.AttributeName != nil {
				keySchema.AttributeName = aws.String(*ks.AttributeName)
			} else {
				keySchema.AttributeName = aws.String("")
			}
			if ks.KeyType != nil {
				keySchema.KeyType = aws.String(*ks.KeyType)
			} else {
				keySchema.KeyType = aws.String("")
			}
		} else {
			keySchema.KeyType = aws.String("")
			keySchema.AttributeName = aws.String("")
		}
	}
	return keySchemas
}

func (rm *resourceManager) syncTableProvisionedThroughput(
	ctx context.Context,
	r *resource,
) (err error) {
	rlog := ackrtlog.FromContext(ctx)
	exit := rlog.Trace("rm.syncTableProvisionedThroughput")
	defer exit(err)

	input := &svcsdk.UpdateTableInput{
		TableName:             aws.String(*r.ko.Spec.TableName),
		ProvisionedThroughput: &svcsdk.ProvisionedThroughput{},
	}
	if r.ko.Spec.ProvisionedThroughput != nil {
		if r.ko.Spec.ProvisionedThroughput.ReadCapacityUnits != nil {
			input.ProvisionedThroughput.ReadCapacityUnits = aws.Int64(*r.ko.Spec.ProvisionedThroughput.ReadCapacityUnits)
		} else {
			input.ProvisionedThroughput.ReadCapacityUnits = aws.Int64(0)
		}

		if r.ko.Spec.ProvisionedThroughput.WriteCapacityUnits != nil {
			input.ProvisionedThroughput.WriteCapacityUnits = aws.Int64(*r.ko.Spec.ProvisionedThroughput.WriteCapacityUnits)
		} else {
			input.ProvisionedThroughput.WriteCapacityUnits = aws.Int64(0)
		}
	} else {
		input.ProvisionedThroughput.ReadCapacityUnits = aws.Int64(0)
		input.ProvisionedThroughput.WriteCapacityUnits = aws.Int64(0)
	}

	_, err = rm.sdkapi.UpdateTable(input)
	rm.metrics.RecordAPICall("UPDATE", "UpdateTable", err)
	if err != nil {
		return err
	}
	return err
}

func (rm *resourceManager) syncTable(
	ctx context.Context,
	r *resource,
	delta *ackcompare.Delta,
) (err error) {
	rlog := ackrtlog.FromContext(ctx)
	exit := rlog.Trace("rm.syncTable")
	defer exit(err)

	input, err := rm.newUpdateTablePayload(ctx, r, delta)
	if err != nil {
		return err
	}

	_, err = rm.sdkapi.UpdateTable(input)
	rm.metrics.RecordAPICall("UPDATE", "UpdateTable", err)
	if err != nil {
		return err
	}
	return nil
}

func (rm *resourceManager) newUpdateTablePayload(
	ctx context.Context,
	r *resource,
	delta *ackcompare.Delta,
) (*svcsdk.UpdateTableInput, error) {
	input := &svcsdk.UpdateTableInput{
		TableName: aws.String(*r.ko.Spec.TableName),
	}
	switch {
	case delta.DifferentAt("Spec.BillingMode"):
		input.BillingMode = aws.String(*r.ko.Spec.BillingMode)
	case delta.DifferentAt("Spec.StreamSpecification"):
		input.StreamSpecification = &svcsdk.StreamSpecification{
			StreamEnabled:  aws.Bool(*r.ko.Spec.StreamSpecification.StreamEnabled),
			StreamViewType: aws.String(*r.ko.Spec.StreamSpecification.StreamViewType),
		}
	case delta.DifferentAt("Spec.SEESpecification"):
		input.SSESpecification = &svcsdk.SSESpecification{
			Enabled:        aws.Bool(*r.ko.Spec.SSESpecification.Enabled),
			SSEType:        aws.String(*r.ko.Spec.SSESpecification.SSEType),
			KMSMasterKeyId: aws.String(*r.ko.Spec.SSESpecification.KMSMasterKeyID),
		}
	}
	return input, nil
}

func (rm *resourceManager) syncTableTags(
	ctx context.Context,
	latest *resource,
	desired *resource,
) (err error) {
	rlog := ackrtlog.FromContext(ctx)
	exit := rlog.Trace("rm.syncTableTags")
	defer exit(err)

	added, updated, removed := computeTagsDelta(latest.ko.Spec.Tags, desired.ko.Spec.Tags)

	// There are no called to update an existing tag. Hence for those we should remove them
	// then add them again.

	for _, updatedTag := range updated {
		removed = append(removed, updatedTag.Key)
	}
	added = append(added, updated...)

	if len(removed) > 0 {
		_, err = rm.sdkapi.UntagResourceWithContext(
			ctx,
			&svcsdk.UntagResourceInput{
				ResourceArn: (*string)(latest.ko.Status.ACKResourceMetadata.ARN),
				TagKeys:     removed,
			},
		)
		rm.metrics.RecordAPICall("GET", "UntagResource", err)
		if err != nil {
			return err
		}
	}

	if len(added) > 0 {
		_, err = rm.sdkapi.TagResourceWithContext(
			ctx,
			&svcsdk.TagResourceInput{
				ResourceArn: (*string)(latest.ko.Status.ACKResourceMetadata.ARN),
				Tags:        tagsFromResourceTags(added),
			},
		)
		rm.metrics.RecordAPICall("GET", "UntagResource", err)
		if err != nil {
			return err
		}

		return err
	}

	return nil
}

// setResourceAdditionalFields will describe the fields that are not return by
// GetFunctionConcurrency calls
func (rm *resourceManager) setResourceAdditionalFields(
	ctx context.Context,
	ko *svcapitypes.Table,
) (err error) {
	rlog := ackrtlog.FromContext(ctx)
	exit := rlog.Trace("rm.setResourceAdditionalFields")
	defer exit(err)

	ko.Spec.Tags, err = rm.getResourceTagsPagesWithContext(ctx, string(*ko.Status.ACKResourceMetadata.ARN))
	return err
}

func (rm *resourceManager) getResourceTagsPagesWithContext(ctx context.Context, resourceARN string) ([]*v1alpha1.Tag, error) {
	var err error
	rlog := ackrtlog.FromContext(ctx)
	exit := rlog.Trace("rm.getResourceTagsPagesWithContext")
	defer exit(err)

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
		tags = append(tags, tagsFromSDKTags(listTagsOfResourceOutput.Tags)...)
		if listTagsOfResourceOutput.NextToken == nil {
			break
		}
	}
	return tags, nil
}

func tagsFromSDKTags(svcTags []*svcsdk.Tag) []*v1alpha1.Tag {
	tags := make([]*v1alpha1.Tag, len(svcTags))
	for i := range svcTags {
		tags[i] = &v1alpha1.Tag{
			Key:   svcTags[i].Key,
			Value: svcTags[i].Value,
		}
	}
	return tags
}

func tagsFromResourceTags(rTags []*v1alpha1.Tag) []*svcsdk.Tag {
	tags := make([]*svcsdk.Tag, len(rTags))
	for i := range rTags {
		tags[i] = &svcsdk.Tag{
			Key:   rTags[i].Key,
			Value: rTags[i].Value,
		}
	}
	return tags
}
