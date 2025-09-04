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

	ackcompare "github.com/aws-controllers-k8s/runtime/pkg/compare"
	ackerr "github.com/aws-controllers-k8s/runtime/pkg/errors"
	ackrtlog "github.com/aws-controllers-k8s/runtime/pkg/runtime/log"
	ackutil "github.com/aws-controllers-k8s/runtime/pkg/util"
	"github.com/aws/aws-sdk-go-v2/aws"
	svcsdk "github.com/aws/aws-sdk-go-v2/service/dynamodb"
	svcsdktypes "github.com/aws/aws-sdk-go-v2/service/dynamodb/types"

	"github.com/aws-controllers-k8s/dynamodb-controller/apis/v1alpha1"
)

// canUpdateTableGSIs return true if it's possible to update table GSIs.
// we can only perform one GSI create/update/delete at once.
func canUpdateTableGSIs(r *resource) bool {
	for _, gsiDescription := range r.ko.Status.GlobalSecondaryIndexesDescriptions {
		if *gsiDescription.IndexStatus != string(svcsdktypes.IndexStatusActive) {
			return false
		}
	}
	return true
}

// computeGlobalSecondaryIndexDelta compares two GlobalSecondaryIndex arrays and
// return three different list containing the added, updated and removed
// GlobalSecondaryIndex. The removed array only contains the IndexName of the
// GlobalSecondaryIndex.
func computeGlobalSecondaryIndexDelta(
	a []*v1alpha1.GlobalSecondaryIndex,
	b []*v1alpha1.GlobalSecondaryIndex,
) (added, updated []*v1alpha1.GlobalSecondaryIndex, removed []string) {
	var visitedIndexes []string
loopA:
	for _, aElement := range a {
		visitedIndexes = append(visitedIndexes, *aElement.IndexName)
		for _, bElement := range b {
			if *aElement.IndexName == *bElement.IndexName {
				if !equalGlobalSecondaryIndexes(aElement, bElement) {
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

// equalGlobalSecondaryIndexesArrays returns true if two GlobalSecondaryIndex
// arrays are equal regardless of the order of their elements.
func equalGlobalSecondaryIndexesArrays(
	a []*v1alpha1.GlobalSecondaryIndex,
	b []*v1alpha1.GlobalSecondaryIndex,
) bool {
	added, updated, removed := computeGlobalSecondaryIndexDelta(a, b)
	return len(added) == 0 && len(updated) == 0 && len(removed) == 0
}

// equalGlobalSecondaryIndexes returns whether two GlobalSecondaryIndex objects are
// equal or not.
func equalGlobalSecondaryIndexes(
	a *v1alpha1.GlobalSecondaryIndex,
	b *v1alpha1.GlobalSecondaryIndex,
) bool {
	if ackcompare.HasNilDifference(a.ProvisionedThroughput, b.ProvisionedThroughput) {
		if !isPayPerRequestMode(a, b) {
			return false
		}
	}
	if a.ProvisionedThroughput != nil && b.ProvisionedThroughput != nil {
		if !equalInt64s(a.ProvisionedThroughput.ReadCapacityUnits, b.ProvisionedThroughput.ReadCapacityUnits) {
			return false
		}
		if !equalInt64s(a.ProvisionedThroughput.WriteCapacityUnits, b.ProvisionedThroughput.WriteCapacityUnits) {
			return false
		}
	}
	if ackcompare.HasNilDifference(a.Projection, b.Projection) {
		return false
	}
	if a.Projection != nil && b.Projection != nil {
		if !equalStrings(a.Projection.ProjectionType, b.Projection.ProjectionType) {
			return false
		}
		if !ackcompare.SliceStringPEqual(a.Projection.NonKeyAttributes, b.Projection.NonKeyAttributes) {
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

// isPayPerRequestMode catches the exceptional case for GSI with PAY_PER_REQUEST billing mode
// if a.ProvisionedThroughput is nil and b.ProvisionedThroughput is not nil but with 0 capacity
// because aws set the default value to 0 for provisioned throughput when billing mode is PAY_PER_REQUEST
// see https://docs.aws.amazon.com/amazondynamodb/latest/APIReference/API_ProvisionedThroughput.html
func isPayPerRequestMode(desired *v1alpha1.GlobalSecondaryIndex, latest *v1alpha1.GlobalSecondaryIndex) bool {
	if desired.ProvisionedThroughput == nil &&
		latest.ProvisionedThroughput != nil &&
		aws.ToInt64(latest.ProvisionedThroughput.WriteCapacityUnits) == 0 &&
		aws.ToInt64(latest.ProvisionedThroughput.ReadCapacityUnits) == 0 {
		return true
	}

	// this case should not happen with dynamodb request validation, but just in case
	if desired.ProvisionedThroughput != nil &&
		latest.ProvisionedThroughput == nil &&
		aws.ToInt64(desired.ProvisionedThroughput.WriteCapacityUnits) == 0 &&
		aws.ToInt64(desired.ProvisionedThroughput.ReadCapacityUnits) == 0 {
		return true
	}

	return false
}

// Delete GSIs removed in the desired spec.
func (rm *resourceManager) deleteGSIs(ctx context.Context, desired *resource, latest *resource) (err error) {
	rlog := ackrtlog.FromContext(ctx)
	exit := rlog.Trace("rm.deleteGSIs")
	defer exit(err)

	_, _, removedGSIs := computeGlobalSecondaryIndexDelta(
		latest.ko.Spec.GlobalSecondaryIndexes,
		desired.ko.Spec.GlobalSecondaryIndexes,
	)
	if len(removedGSIs) == 0 {
		return nil
	}

	if !canUpdateTableGSIs(latest) {
		return requeueWaitGSIReady
	}
	gsiDeletesInQueue := len(removedGSIs) - 1

	input := &svcsdk.UpdateTableInput{
		TableName:            aws.String(*latest.ko.Spec.TableName),
		AttributeDefinitions: newSDKAttributesDefinition(desired.ko.Spec.AttributeDefinitions),
	}

	for _, removedGSI := range removedGSIs {
		update := svcsdktypes.GlobalSecondaryIndexUpdate{
			Delete: &svcsdktypes.DeleteGlobalSecondaryIndexAction{
				IndexName: &removedGSI,
			},
		}
		input.GlobalSecondaryIndexUpdates = append(input.GlobalSecondaryIndexUpdates, update)
		// We can only remove, update or add one GSI at once. Hence we return the update call input
		// after we find the first removed GSI.
		break
	}

	_, err = rm.sdkapi.UpdateTable(ctx, input)
	rm.metrics.RecordAPICall("UPDATE", "UpdateTable", err)
	if err != nil {
		if awsErr, ok := ackerr.AWSError(err); ok &&
			awsErr.ErrorCode() == "LimitExceededException" {
			return requeueWaitGSIReady
		}
		return err
	}

	if gsiDeletesInQueue > 0 {
		return requeueWaitGSIReady
	}

	return nil
}

// Update GSIs changed in the desired spec.
func (rm *resourceManager) updateGSIs(ctx context.Context, desired *resource, latest *resource) (err error) {
	rlog := ackrtlog.FromContext(ctx)
	exit := rlog.Trace("rm.deleteGSIs")
	defer exit(err)

	_, updatedGSIs, _ := computeGlobalSecondaryIndexDelta(
		latest.ko.Spec.GlobalSecondaryIndexes,
		desired.ko.Spec.GlobalSecondaryIndexes,
	)
	if len(updatedGSIs) == 0 {
		return nil
	}

	if !canUpdateTableGSIs(latest) {
		return requeueWaitGSIReady
	}

	gsiUpdatesInQueue := len(updatedGSIs) - 1

	input := &svcsdk.UpdateTableInput{
		TableName:            aws.String(*latest.ko.Spec.TableName),
		AttributeDefinitions: newSDKAttributesDefinition(desired.ko.Spec.AttributeDefinitions),
	}

	for _, updatedGSI := range updatedGSIs {
		update := svcsdktypes.GlobalSecondaryIndexUpdate{
			Update: &svcsdktypes.UpdateGlobalSecondaryIndexAction{
				IndexName:             aws.String(*updatedGSI.IndexName),
				ProvisionedThroughput: newSDKProvisionedThroughput(updatedGSI.ProvisionedThroughput),
			},
		}
		input.GlobalSecondaryIndexUpdates = append(input.GlobalSecondaryIndexUpdates, update)
		// We can only remove, update or add one GSI at once. Hence we return the update call input
		// after we find the first updated GSI.
		break
	}

	_, err = rm.sdkapi.UpdateTable(ctx, input)
	rm.metrics.RecordAPICall("UPDATE", "UpdateTable", err)
	if err != nil {
		if awsErr, ok := ackerr.AWSError(err); ok &&
			awsErr.ErrorCode() == "LimitExceededException" {
			return requeueWaitGSIReady
		}
		return err
	}

	if gsiUpdatesInQueue > 0 {
		return requeueWaitGSIReady
	}

	return nil
}

// Add GSIs added in the desired spec.
func (rm *resourceManager) addGSIs(ctx context.Context, desired *resource, latest *resource) (err error) {
	rlog := ackrtlog.FromContext(ctx)
	exit := rlog.Trace("rm.deleteGSIs")
	defer exit(err)

	addedGSIs, _, _ := computeGlobalSecondaryIndexDelta(
		latest.ko.Spec.GlobalSecondaryIndexes,
		desired.ko.Spec.GlobalSecondaryIndexes,
	)
	if len(addedGSIs) == 0 {
		return nil
	}

	if !canUpdateTableGSIs(latest) {
		return requeueWaitGSIReady
	}

	gsiCreatesInQueue := len(addedGSIs) - 1

	input := &svcsdk.UpdateTableInput{
		TableName:            aws.String(*latest.ko.Spec.TableName),
		AttributeDefinitions: newSDKAttributesDefinition(desired.ko.Spec.AttributeDefinitions),
	}

	for _, addedGSI := range addedGSIs {
		update := svcsdktypes.GlobalSecondaryIndexUpdate{
			Create: &svcsdktypes.CreateGlobalSecondaryIndexAction{
				IndexName:             aws.String(*addedGSI.IndexName),
				Projection:            newSDKProjection(addedGSI.Projection),
				KeySchema:             newSDKKeySchemaArray(addedGSI.KeySchema),
				ProvisionedThroughput: newSDKProvisionedThroughput(addedGSI.ProvisionedThroughput),
			},
		}
		input.GlobalSecondaryIndexUpdates = append(input.GlobalSecondaryIndexUpdates, update)
		// We can only remove, update or add one GSI at once. Hence we return the update call input
		// after we find the first added GSI.
		break
	}

	_, err = rm.sdkapi.UpdateTable(ctx, input)
	rm.metrics.RecordAPICall("UPDATE", "UpdateTable", err)
	if err != nil {
		if awsErr, ok := ackerr.AWSError(err); ok &&
			awsErr.ErrorCode() == "LimitExceededException" {
			return requeueWaitGSIReady
		}
		return err
	}

	if gsiCreatesInQueue > 0 {
		return requeueWaitGSIReady
	}

	return nil
}

// newSDKProvisionedThroughput builds a new *svcsdk.ProvisionedThroughput
func newSDKProvisionedThroughput(pt *v1alpha1.ProvisionedThroughput) *svcsdktypes.ProvisionedThroughput {
	if pt == nil {
		return nil
	}
	provisionedThroughput := &svcsdktypes.ProvisionedThroughput{
		// ref: https://docs.aws.amazon.com/amazondynamodb/latest/APIReference/API_ProvisionedThroughput.html
		// Minimum capacity units is 1 when using provisioned capacity mode
		ReadCapacityUnits:  aws.Int64(1),
		WriteCapacityUnits: aws.Int64(1),
	}
	if pt.ReadCapacityUnits != nil {
		provisionedThroughput.ReadCapacityUnits = aws.Int64(*pt.ReadCapacityUnits)
	}

	if pt.WriteCapacityUnits != nil {
		provisionedThroughput.WriteCapacityUnits = aws.Int64(*pt.WriteCapacityUnits)
	}
	return provisionedThroughput
}

// newSDKProjection builds a new *svcsdk.Projection
func newSDKProjection(p *v1alpha1.Projection) *svcsdktypes.Projection {
	projection := &svcsdktypes.Projection{}
	if p != nil {
		if p.ProjectionType != nil {
			projection.ProjectionType = svcsdktypes.ProjectionType(*p.ProjectionType)
		} else {
			projection.ProjectionType = svcsdktypes.ProjectionType("")
		}
		if p.NonKeyAttributes != nil {
			projection.NonKeyAttributes = aws.ToStringSlice(p.NonKeyAttributes)
		}
	} else {
		projection.ProjectionType = svcsdktypes.ProjectionType("")
		projection.NonKeyAttributes = nil
	}
	return projection
}

// newSDKKeySchemaArray builds a new []*svcsdk.KeySchemaElement
func newSDKKeySchemaArray(kss []*v1alpha1.KeySchemaElement) []svcsdktypes.KeySchemaElement {
	keySchemas := []svcsdktypes.KeySchemaElement{}
	for _, ks := range kss {
		keySchema := svcsdktypes.KeySchemaElement{}
		if ks != nil {
			if ks.AttributeName != nil {
				keySchema.AttributeName = aws.String(*ks.AttributeName)
			} else {
				keySchema.AttributeName = aws.String("")
			}
			if ks.KeyType != nil {
				keySchema.KeyType = svcsdktypes.KeyType(*ks.KeyType)
			} else {
				keySchema.KeyType = svcsdktypes.KeyType("")
			}
		} else {
			keySchema.KeyType = svcsdktypes.KeyType("")
			keySchema.AttributeName = aws.String("")
		}
		keySchemas = append(keySchemas, keySchema)
	}
	return keySchemas
}
