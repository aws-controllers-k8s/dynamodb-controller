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
	"fmt"
	"strings"
	"time"

	ackcompare "github.com/aws-controllers-k8s/runtime/pkg/compare"
	ackerr "github.com/aws-controllers-k8s/runtime/pkg/errors"
	ackrequeue "github.com/aws-controllers-k8s/runtime/pkg/requeue"
	ackrtlog "github.com/aws-controllers-k8s/runtime/pkg/runtime/log"
	ackutil "github.com/aws-controllers-k8s/runtime/pkg/util"
	"github.com/aws/aws-sdk-go/aws"
	svcsdk "github.com/aws/aws-sdk-go/service/dynamodb"
	corev1 "k8s.io/api/core/v1"

	"github.com/aws-controllers-k8s/dynamodb-controller/apis/v1alpha1"
)

var (
	ErrTableDeleting = fmt.Errorf(
		"Table in '%v' state, cannot be modified or deleted",
		svcsdk.TableStatusDeleting,
	)
	ErrTableCreating = fmt.Errorf(
		"Table in '%v' state, cannot be modified or deleted",
		svcsdk.TableStatusCreating,
	)
	ErrTableUpdating = fmt.Errorf(
		"Table in '%v' state, cannot be modified or deleted",
		svcsdk.TableStatusUpdating,
	)
	ErrTableGSIsUpdating = fmt.Errorf(
		"Table GSIs in '%v' state, cannot be modified or deleted",
		svcsdk.IndexStatusCreating,
	)
)

// TerminalStatuses are the status strings that are terminal states for a
// DynamoDB table
var TerminalStatuses = []v1alpha1.TableStatus_SDK{
	v1alpha1.TableStatus_SDK_ARCHIVING,
	v1alpha1.TableStatus_SDK_DELETING,
}

var (
	DefaultTTLEnabledValue  = false
	DefaultPITREnabledValue = false
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
	requeueWaitGSIReady = ackrequeue.NeededAfter(
		ErrTableGSIsUpdating,
		10*time.Second,
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

	if immutableFieldChanges := rm.getImmutableFieldChanges(delta); len(immutableFieldChanges) > 0 {
		msg := fmt.Sprintf(
			"Immutable Spec fields have been modified: %s",
			strings.Join(immutableFieldChanges, ","),
		)
		return nil, ackerr.NewTerminalError(fmt.Errorf(msg))
	}

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

	if delta.DifferentAt("Spec.Tags") {
		if err := rm.syncTableTags(ctx, desired, latest); err != nil {
			return nil, err
		}
	}
	if !delta.DifferentExcept("Spec.Tags") {
		return &resource{ko}, nil
	}

	if delta.DifferentAt("Spec.TimeToLive") {
		if err := rm.syncTTL(ctx, desired, latest); err != nil {
			// Ignore "already disabled errors"
			if awsErr, ok := ackerr.AWSError(err); ok && !(awsErr.Code() == "ValidationException" &&
				strings.HasPrefix(awsErr.Message(), "TimeToLive is already disabled")) {
				return nil, err
			}
		}
	}

	if delta.DifferentAt("Spec.SSESpecification") {
		if err := rm.syncTableSSESpecification(ctx, desired); err != nil {
			return nil, fmt.Errorf("cannot update table %v", err)
		}
	}

	if delta.DifferentAt("Spec.BillingMode") ||
		delta.DifferentAt("Spec.TableClass") {
		if err := rm.syncTable(ctx, desired, delta); err != nil {
			return nil, fmt.Errorf("cannot update table %v", err)
		}
	}

	if delta.DifferentAt("Spec.ContinuousBackups") {
		err = rm.syncContinuousBackup(ctx, desired)
		if err != nil {
			return nil, fmt.Errorf("cannot update table %v", err)
		}
	}

	// We want to update fast fields first
	// Then attributes
	// then GSI
	if delta.DifferentExcept("Spec.Tags", "Spec.TimeToLive") {
		switch {
		case delta.DifferentAt("Spec.StreamSpecification"):
			if err := rm.syncTable(ctx, desired, delta); err != nil {
				return nil, err
			}
		case delta.DifferentAt("Spec.ProvisionedThroughput"):
			if err := rm.syncTableProvisionedThroughput(ctx, desired); err != nil {
				return nil, err
			}
		case delta.DifferentAt("Spec.GlobalSecondaryIndexes") && delta.DifferentAt("Spec.AttributeDefinitions"):
			if err := rm.syncTableGlobalSecondaryIndexes(ctx, latest, desired); err != nil {
				if awsErr, ok := ackerr.AWSError(err); ok &&
					awsErr.Code() == "LimitExceededException" {
					return nil, requeueWaitGSIReady
				}
				return nil, err
			}
		}
	}

	return &resource{ko}, requeueWaitWhileUpdating
}

// syncTable updates a given table billing mode, stream specification
// or SSE specification.
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

// newUpdateTablePayload constructs the updateTableInput object.
func (rm *resourceManager) newUpdateTablePayload(
	ctx context.Context,
	r *resource,
	delta *ackcompare.Delta,
) (*svcsdk.UpdateTableInput, error) {
	input := &svcsdk.UpdateTableInput{
		TableName: aws.String(*r.ko.Spec.TableName),
	}

	if delta.DifferentAt("Spec.BillingMode") {
		if r.ko.Spec.BillingMode != nil {
			input.BillingMode = aws.String(*r.ko.Spec.BillingMode)
		} else {
			// set biling mode to the default value `PROVISIONED`
			input.BillingMode = aws.String(svcsdk.BillingModeProvisioned)
		}
		if *input.BillingMode == svcsdk.BillingModeProvisioned {
			input.ProvisionedThroughput = &svcsdk.ProvisionedThroughput{}
			if r.ko.Spec.ProvisionedThroughput != nil {
				if r.ko.Spec.ProvisionedThroughput.ReadCapacityUnits != nil {
					input.ProvisionedThroughput.ReadCapacityUnits = aws.Int64(
						*r.ko.Spec.ProvisionedThroughput.ReadCapacityUnits,
					)
				} else {
					input.ProvisionedThroughput.ReadCapacityUnits = aws.Int64(0)
				}

				if r.ko.Spec.ProvisionedThroughput.WriteCapacityUnits != nil {
					input.ProvisionedThroughput.WriteCapacityUnits = aws.Int64(
						*r.ko.Spec.ProvisionedThroughput.WriteCapacityUnits,
					)
				} else {
					input.ProvisionedThroughput.WriteCapacityUnits = aws.Int64(0)
				}
			}
		}
	}
	if delta.DifferentAt("Spec.StreamSpecification") {
		if r.ko.Spec.StreamSpecification != nil {
			if r.ko.Spec.StreamSpecification.StreamEnabled != nil {
				input.StreamSpecification = &svcsdk.StreamSpecification{
					StreamEnabled: aws.Bool(*r.ko.Spec.StreamSpecification.StreamEnabled),
				}
				// Only set streamViewType when streamSpefication is enabled and streamViewType is non-nil.
				if *r.ko.Spec.StreamSpecification.StreamEnabled &&
					r.ko.Spec.StreamSpecification.StreamViewType != nil {
					input.StreamSpecification.StreamViewType = aws.String(
						*r.ko.Spec.StreamSpecification.StreamViewType,
					)
				}
			} else {
				input.StreamSpecification = &svcsdk.StreamSpecification{
					StreamEnabled: aws.Bool(false),
				}
			}
		}
	}
	if delta.DifferentAt("Spec.TableClass") {
		if r.ko.Spec.TableClass != nil {
			input.TableClass = aws.String(*r.ko.Spec.TableClass)
		}
	}

	return input, nil
}

// syncTableSSESpecification updates a given table SSE Specification
func (rm *resourceManager) syncTableSSESpecification(
	ctx context.Context,
	r *resource,
) (err error) {
	rlog := ackrtlog.FromContext(ctx)
	exit := rlog.Trace("rm.syncTableSSESpecification")
	defer exit(err)

	input := &svcsdk.UpdateTableInput{
		TableName: aws.String(*r.ko.Spec.TableName),
	}
	if r.ko.Spec.SSESpecification != nil {
		input.SSESpecification = &svcsdk.SSESpecification{}
		if r.ko.Spec.SSESpecification.Enabled != nil {
			input.SSESpecification.Enabled = aws.Bool(*r.ko.Spec.SSESpecification.Enabled)
			if *input.SSESpecification.Enabled {
				if r.ko.Spec.SSESpecification.SSEType != nil {
					input.SSESpecification.SSEType = aws.String(*r.ko.Spec.SSESpecification.SSEType)
				}
				if r.ko.Spec.SSESpecification.KMSMasterKeyID != nil {
					input.SSESpecification.KMSMasterKeyId = aws.String(
						*r.ko.Spec.SSESpecification.KMSMasterKeyID,
					)
				}
			}
		} else {
			input.SSESpecification = &svcsdk.SSESpecification{
				Enabled: aws.Bool(false),
			}
		}
	}

	_, err = rm.sdkapi.UpdateTable(input)
	rm.metrics.RecordAPICall("UPDATE", "UpdateTable", err)
	if err != nil {
		return err
	}
	return err
}

// syncTableProvisionedThroughput updates a given table provisioned throughputs
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
			input.ProvisionedThroughput.ReadCapacityUnits = aws.Int64(
				*r.ko.Spec.ProvisionedThroughput.ReadCapacityUnits,
			)
		} else {
			input.ProvisionedThroughput.ReadCapacityUnits = aws.Int64(0)
		}

		if r.ko.Spec.ProvisionedThroughput.WriteCapacityUnits != nil {
			input.ProvisionedThroughput.WriteCapacityUnits = aws.Int64(
				*r.ko.Spec.ProvisionedThroughput.WriteCapacityUnits,
			)
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

// setResourceAdditionalFields will describe the fields that are not return by
// DescribeTable calls
func (rm *resourceManager) setResourceAdditionalFields(
	ctx context.Context,
	ko *v1alpha1.Table,
) (err error) {
	rlog := ackrtlog.FromContext(ctx)
	exit := rlog.Trace("rm.setResourceAdditionalFields")
	defer func(err error) { exit(err) }(err)

	if tags, err := rm.getResourceTagsPagesWithContext(ctx, string(*ko.Status.ACKResourceMetadata.ARN)); err != nil {
		return err
	} else {
		ko.Spec.Tags = tags
	}

	if ttlSpec, err := rm.getResourceTTLWithContext(ctx, ko.Spec.TableName); err != nil {
		return err
	} else {
		ko.Spec.TimeToLive = ttlSpec
	}

	if pitrSpec, err := rm.getResourcePointInTimeRecoveryWithContext(ctx, ko.Spec.TableName); err != nil {
		return err
	} else {
		ko.Spec.ContinuousBackups = pitrSpec
	}

	return nil
}

func customPreCompare(
	delta *ackcompare.Delta,
	a *resource,
	b *resource,
) {
	if ackcompare.HasNilDifference(a.ko.Spec.SSESpecification, b.ko.Spec.SSESpecification) {
		if a.ko.Spec.SSESpecification != nil && b.ko.Spec.SSESpecification == nil {
			if *a.ko.Spec.SSESpecification.Enabled {
				delta.Add(
					"Spec.SSESpecification",
					a.ko.Spec.SSESpecification,
					b.ko.Spec.SSESpecification,
				)
			}
		} else {
			delta.Add("Spec.SSESpecification", a.ko.Spec.SSESpecification, b.ko.Spec.SSESpecification)
		}
	} else if a.ko.Spec.SSESpecification != nil && b.ko.Spec.SSESpecification != nil {
		if ackcompare.HasNilDifference(a.ko.Spec.SSESpecification.Enabled, b.ko.Spec.SSESpecification.Enabled) {
			delta.Add("Spec.SSESpecification.Enabled", a.ko.Spec.SSESpecification.Enabled, b.ko.Spec.SSESpecification.Enabled)
		} else if a.ko.Spec.SSESpecification.Enabled != nil && b.ko.Spec.SSESpecification.Enabled != nil {
			if *a.ko.Spec.SSESpecification.Enabled != *b.ko.Spec.SSESpecification.Enabled {
				delta.Add("Spec.SSESpecification.Enabled", a.ko.Spec.SSESpecification.Enabled, b.ko.Spec.SSESpecification.Enabled)
			}
		}
		if ackcompare.HasNilDifference(a.ko.Spec.SSESpecification.KMSMasterKeyID, b.ko.Spec.SSESpecification.KMSMasterKeyID) {
			if a.ko.Spec.SSESpecification.KMSMasterKeyID != nil {
				delta.Add("Spec.SSESpecification.KMSMasterKeyID", a.ko.Spec.SSESpecification.KMSMasterKeyID, b.ko.Spec.SSESpecification.KMSMasterKeyID)
			}
		} else if a.ko.Spec.SSESpecification.KMSMasterKeyID != nil && b.ko.Spec.SSESpecification.KMSMasterKeyID != nil {
			if *a.ko.Spec.SSESpecification.KMSMasterKeyID != *b.ko.Spec.SSESpecification.KMSMasterKeyID {
				delta.Add("Spec.SSESpecification.KMSMasterKeyID", a.ko.Spec.SSESpecification.KMSMasterKeyID, b.ko.Spec.SSESpecification.KMSMasterKeyID)
			}
		}
		if ackcompare.HasNilDifference(a.ko.Spec.SSESpecification.SSEType, b.ko.Spec.SSESpecification.SSEType) {
			delta.Add("Spec.SSESpecification.SSEType", a.ko.Spec.SSESpecification.SSEType, b.ko.Spec.SSESpecification.SSEType)
		} else if a.ko.Spec.SSESpecification.SSEType != nil && b.ko.Spec.SSESpecification.SSEType != nil {
			if *a.ko.Spec.SSESpecification.SSEType != *b.ko.Spec.SSESpecification.SSEType {
				delta.Add("Spec.SSESpecification.SSEType", a.ko.Spec.SSESpecification.SSEType, b.ko.Spec.SSESpecification.SSEType)
			}
		}
	}

	if len(a.ko.Spec.KeySchema) != len(b.ko.Spec.KeySchema) {
		delta.Add("Spec.KeySchema", a.ko.Spec.KeySchema, b.ko.Spec.KeySchema)
	} else if a.ko.Spec.KeySchema != nil && b.ko.Spec.KeySchema != nil {
		if !equalKeySchemaArrays(a.ko.Spec.KeySchema, b.ko.Spec.KeySchema) {
			delta.Add("Spec.KeySchema", a.ko.Spec.KeySchema, b.ko.Spec.KeySchema)
		}
	}

	if len(a.ko.Spec.AttributeDefinitions) != len(b.ko.Spec.AttributeDefinitions) {
		delta.Add(
			"Spec.AttributeDefinitions",
			a.ko.Spec.AttributeDefinitions,
			b.ko.Spec.AttributeDefinitions,
		)
	} else if a.ko.Spec.AttributeDefinitions != nil && b.ko.Spec.AttributeDefinitions != nil {
		if !equalAttributeDefinitions(a.ko.Spec.AttributeDefinitions, b.ko.Spec.AttributeDefinitions) {
			delta.Add("Spec.AttributeDefinitions", a.ko.Spec.AttributeDefinitions, b.ko.Spec.AttributeDefinitions)
		}
	}

	if len(a.ko.Spec.GlobalSecondaryIndexes) != len(b.ko.Spec.GlobalSecondaryIndexes) {
		delta.Add(
			"Spec.GlobalSecondaryIndexes",
			a.ko.Spec.GlobalSecondaryIndexes,
			b.ko.Spec.GlobalSecondaryIndexes,
		)
	} else if a.ko.Spec.GlobalSecondaryIndexes != nil && b.ko.Spec.GlobalSecondaryIndexes != nil {
		if !equalGlobalSecondaryIndexesArrays(a.ko.Spec.GlobalSecondaryIndexes, b.ko.Spec.GlobalSecondaryIndexes) {
			delta.Add("Spec.GlobalSecondaryIndexes", a.ko.Spec.GlobalSecondaryIndexes, b.ko.Spec.GlobalSecondaryIndexes)
		}
	}

	if len(a.ko.Spec.LocalSecondaryIndexes) != len(b.ko.Spec.LocalSecondaryIndexes) {
		delta.Add(
			"Spec.LocalSecondaryIndexes",
			a.ko.Spec.LocalSecondaryIndexes,
			b.ko.Spec.LocalSecondaryIndexes,
		)
	} else if a.ko.Spec.LocalSecondaryIndexes != nil && b.ko.Spec.LocalSecondaryIndexes != nil {
		if !equalLocalSecondaryIndexesArrays(a.ko.Spec.LocalSecondaryIndexes, b.ko.Spec.LocalSecondaryIndexes) {
			delta.Add("Spec.LocalSecondaryIndexes", a.ko.Spec.LocalSecondaryIndexes, b.ko.Spec.LocalSecondaryIndexes)
		}
	}

	if a.ko.Spec.BillingMode == nil {
		a.ko.Spec.BillingMode = aws.String(string(v1alpha1.BillingMode_PROVISIONED))
	}
	if a.ko.Spec.TableClass == nil {
		a.ko.Spec.TableClass = aws.String(string(v1alpha1.TableClass_STANDARD))
	}
	// See https://github.com/aws-controllers-k8s/community/issues/1595
	if aws.StringValue(a.ko.Spec.BillingMode) == string(v1alpha1.BillingMode_PAY_PER_REQUEST) {
		a.ko.Spec.ProvisionedThroughput = nil
	}
	if aws.StringValue(b.ko.Spec.BillingMode) == string(v1alpha1.BillingMode_PAY_PER_REQUEST) {
		b.ko.Spec.ProvisionedThroughput = nil
	}

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
	if a.ko.Spec.ContinuousBackups == nil && b.ko.Spec.ContinuousBackups != nil &&
		b.ko.Spec.ContinuousBackups.PointInTimeRecoveryEnabled != nil {
		a.ko.Spec.ContinuousBackups = &v1alpha1.PointInTimeRecoverySpecification{
			PointInTimeRecoveryEnabled: &DefaultPITREnabledValue,
		}
	}
}

// equalAttributeDefinitions return whether two AttributeDefinition arrays are equal or not.
func equalAttributeDefinitions(a, b []*v1alpha1.AttributeDefinition) bool {
	for _, aElement := range a {
		found := false
		for _, bElement := range b {
			if equalStrings(aElement.AttributeName, bElement.AttributeName) {
				found = true
				if !equalStrings(aElement.AttributeType, bElement.AttributeType) {
					return false
				}
			}
		}
		if !found {
			return false
		}
	}
	return true
}

// equalKeySchemaArrays return whether two KeySchemaElement arrays are equal or not.
func equalKeySchemaArrays(
	a []*v1alpha1.KeySchemaElement,
	b []*v1alpha1.KeySchemaElement,
) bool {
	for _, aElement := range a {
		found := false
		for _, bElement := range b {
			if equalStrings(aElement.AttributeName, bElement.AttributeName) {
				found = true
				if !equalStrings(aElement.KeyType, bElement.KeyType) {
					return false
				}
			}
		}
		if !found {
			return false
		}
	}
	return true
}

// newSDKAttributesDefinition builds a new []*svcsdk.AttributeDefinition
func newSDKAttributesDefinition(ads []*v1alpha1.AttributeDefinition) []*svcsdk.AttributeDefinition {
	attributeDefintions := []*svcsdk.AttributeDefinition{}
	for _, ad := range ads {
		attributeDefintion := &svcsdk.AttributeDefinition{}
		if ad != nil {
			if ad.AttributeName != nil {
				attributeDefintion.AttributeName = aws.String(*ad.AttributeName)
			} else {
				attributeDefintion.AttributeName = aws.String("")
			}
			if ad.AttributeType != nil {
				attributeDefintion.AttributeType = aws.String(*ad.AttributeType)
			} else {
				attributeDefintion.AttributeType = aws.String("")
			}
		} else {
			attributeDefintion.AttributeType = aws.String("")
			attributeDefintion.AttributeName = aws.String("")
		}
		attributeDefintions = append(attributeDefintions, attributeDefintion)
	}
	return attributeDefintions
}

func computeLocalSecondaryIndexDelta(
	a []*v1alpha1.LocalSecondaryIndex,
	b []*v1alpha1.LocalSecondaryIndex,
) (added, updated []*v1alpha1.LocalSecondaryIndex, removed []string) {
	var visitedIndexes []string
loopA:
	for _, aElement := range a {
		visitedIndexes = append(visitedIndexes, *aElement.IndexName)
		for _, bElement := range b {
			if *aElement.IndexName == *bElement.IndexName {
				if !equalLocalSecondaryIndexes(aElement, bElement) {
					updated = append(updated, bElement)
				}
				continue loopA
			}
		}
		removed = append(removed, *aElement.IndexName)

	}
	for _, bElement := range b {
		if !ackutil.InStrings(*bElement.IndexName, visitedIndexes) {
			added = append(added, bElement)
		}
	}
	return added, updated, removed
}

// equalLocalSecondaryIndexesArrays returns true if two LocalSecondaryIndex arrays are equal regardless
// of the order of their elements.
func equalLocalSecondaryIndexesArrays(
	a []*v1alpha1.LocalSecondaryIndex,
	b []*v1alpha1.LocalSecondaryIndex,
) bool {
	added, updated, removed := computeLocalSecondaryIndexDelta(a, b)
	return len(added) == 0 && len(updated) == 0 && len(removed) == 0
}

// equalLocalSecondaryIndexes returns whether two LocalSecondaryIndex objects are
// equal or not.
func equalLocalSecondaryIndexes(
	a *v1alpha1.LocalSecondaryIndex,
	b *v1alpha1.LocalSecondaryIndex,
) bool {
	if ackcompare.HasNilDifference(a.Projection, b.Projection) {
		return false
	}
	if a.Projection != nil && b.Projection != nil {
		if !equalStrings(a.Projection.ProjectionType, b.Projection.ProjectionType) {
			return false
		}
		if !ackcompare.SliceStringPEqual(
			a.Projection.NonKeyAttributes,
			b.Projection.NonKeyAttributes,
		) {
			return false
		}
	}
	if len(a.KeySchema) != len(b.KeySchema) {
		return false
	} else if len(a.KeySchema) > 0 {
		if !equalKeySchemaArrays(a.KeySchema, b.KeySchema) {
			return false
		}
	}
	return true
}
