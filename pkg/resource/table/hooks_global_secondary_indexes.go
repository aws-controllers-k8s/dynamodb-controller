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
	ackrtlog "github.com/aws-controllers-k8s/runtime/pkg/runtime/log"
	ackutil "github.com/aws-controllers-k8s/runtime/pkg/util"
	"github.com/aws/aws-sdk-go/aws"
	svcsdk "github.com/aws/aws-sdk-go/service/dynamodb"

	"github.com/aws-controllers-k8s/dynamodb-controller/apis/v1alpha1"
)

// canUpdateTableGSIs return true if it's possible to update table GSIs.
// we can only perform one GSI create/update/delete at once.
func canUpdateTableGSIs(r *resource) bool {
	for _, gsiDescription := range r.ko.Status.GlobalSecondaryIndexesDescriptions {
		if *gsiDescription.IndexStatus != svcsdk.IndexStatusActive {
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
		aws.Int64Value(latest.ProvisionedThroughput.WriteCapacityUnits) == 0 &&
		aws.Int64Value(latest.ProvisionedThroughput.ReadCapacityUnits) == 0 {
		return true
	}

	// this case should not happen with dynamodb request validation, but just in case
	if desired.ProvisionedThroughput != nil &&
		latest.ProvisionedThroughput == nil &&
		aws.Int64Value(desired.ProvisionedThroughput.WriteCapacityUnits) == 0 &&
		aws.Int64Value(desired.ProvisionedThroughput.ReadCapacityUnits) == 0 {
		return true
	}

	return false
}

// syncTableGlobalSecondaryIndexes updates a global table secondary indexes.
func (rm *resourceManager) syncTableGlobalSecondaryIndexes(
	ctx context.Context,
	latest *resource,
	desired *resource,
) (err error) {
	rlog := ackrtlog.FromContext(ctx)
	exit := rlog.Trace("rm.syncTableGlobalSecondaryIndexes")
	defer exit(err)

	if !canUpdateTableGSIs(latest) {
		return requeueWaitGSIReady
	}
	input, gsiInQueue, err := rm.newUpdateTableGlobalSecondaryIndexUpdatesPayload(ctx, latest, desired)
	if err != nil {
		return err
	}

	_, err = rm.sdkapi.UpdateTable(input)
	rm.metrics.RecordAPICall("UPDATE", "UpdateTable", err)
	if err != nil {
		return err
	}
	if gsiInQueue > 0 {
		return requeueWaitGSIReady
	}
	return nil
}

func (rm *resourceManager) newUpdateTableGlobalSecondaryIndexUpdatesPayload(
	ctx context.Context,
	latest *resource,
	desired *resource,
) (input *svcsdk.UpdateTableInput, gsisInQueue int, err error) {
	addedGSIs, updatedGSIs, removedGSIs := computeGlobalSecondaryIndexDelta(
		latest.ko.Spec.GlobalSecondaryIndexes,
		desired.ko.Spec.GlobalSecondaryIndexes,
	)
	input = &svcsdk.UpdateTableInput{
		TableName:            aws.String(*latest.ko.Spec.TableName),
		AttributeDefinitions: newSDKAttributesDefinition(desired.ko.Spec.AttributeDefinitions),
	}

	// If we know that we're still gonna need to update another GSI we return a value gt 0.
	gsisInQueue = len(addedGSIs) + len(updatedGSIs) + len(removedGSIs) - 1

	// Althought this sounds a little bit weird but it makes sense iterate on an array
	// and directly return if we find an element. Range works well with nil arrays...

	for _, addedGSI := range addedGSIs {
		update := &svcsdk.GlobalSecondaryIndexUpdate{
			Create: &svcsdk.CreateGlobalSecondaryIndexAction{
				IndexName:             aws.String(*addedGSI.IndexName),
				Projection:            newSDKProjection(addedGSI.Projection),
				KeySchema:             newSDKKeySchemaArray(addedGSI.KeySchema),
				ProvisionedThroughput: newSDKProvisionedThroughput(addedGSI.ProvisionedThroughput),
			},
		}
		input.GlobalSecondaryIndexUpdates = append(input.GlobalSecondaryIndexUpdates, update)
		// We can only remove, update or add one GSI at once. Hence we return the update call input
		// after we find the first added GSI.
		return input, gsisInQueue, nil
	}

	for _, updatedGSI := range updatedGSIs {
		update := &svcsdk.GlobalSecondaryIndexUpdate{
			Update: &svcsdk.UpdateGlobalSecondaryIndexAction{
				IndexName:             aws.String(*updatedGSI.IndexName),
				ProvisionedThroughput: newSDKProvisionedThroughput(updatedGSI.ProvisionedThroughput),
			},
		}
		input.GlobalSecondaryIndexUpdates = append(input.GlobalSecondaryIndexUpdates, update)
		// We can only remove, update or add one GSI at once. Hence we return the update call input
		// after we find the first updated GSI.
		return input, gsisInQueue, nil
	}

	for _, removedGSI := range removedGSIs {
		update := &svcsdk.GlobalSecondaryIndexUpdate{
			Delete: &svcsdk.DeleteGlobalSecondaryIndexAction{
				IndexName: &removedGSI,
			},
		}
		input.GlobalSecondaryIndexUpdates = append(input.GlobalSecondaryIndexUpdates, update)
		// We can only remove, update or add one GSI at once. Hence we return the update call input
		// after we find the first removed GSI.
		return input, gsisInQueue, nil
	}
	return input, gsisInQueue, nil
}

// newSDKProvisionedThroughput builds a new *svcsdk.ProvisionedThroughput
func newSDKProvisionedThroughput(pt *v1alpha1.ProvisionedThroughput) *svcsdk.ProvisionedThroughput {
	if pt == nil {
		return nil
	}
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

// newSDKProjection builds a new *svcsdk.Projection
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
			projection.NonKeyAttributes = nil
		}
	} else {
		projection.ProjectionType = aws.String("")
		projection.NonKeyAttributes = []*string{}
	}
	return projection
}

// newSDKKeySchemaArray builds a new []*svcsdk.KeySchemaElement
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
		keySchemas = append(keySchemas, keySchema)
	}
	return keySchemas
}
