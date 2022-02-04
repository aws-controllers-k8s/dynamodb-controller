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
					updated = append(updated, aElement)
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

func equalGlobalSecondaryIndexesArrays(
	a []*v1alpha1.GlobalSecondaryIndex,
	b []*v1alpha1.GlobalSecondaryIndex,
) bool {
	added, updated, removed := computeGlobalSecondaryIndexDelta(a, b)
	return len(added) == 0 && len(updated) == 0 && len(removed) == 0
}

func equalGlobalSecondaryIndexes(
	a *v1alpha1.GlobalSecondaryIndex,
	b *v1alpha1.GlobalSecondaryIndex,
) bool {
	if ackcompare.HasNilDifference(a.ProvisionedThroughput, b.ProvisionedThroughput) {
		return false
	}
	if a.ProvisionedThroughput != nil && b.ProvisionedThroughput != nil {
		if !equalInt64(a.ProvisionedThroughput.ReadCapacityUnits, b.ProvisionedThroughput.ReadCapacityUnits) {
			return false
		}
		if equalInt64(a.ProvisionedThroughput.WriteCapacityUnits, b.ProvisionedThroughput.WriteCapacityUnits) {
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
	return true
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
