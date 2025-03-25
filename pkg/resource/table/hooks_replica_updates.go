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

	ackrtlog "github.com/aws-controllers-k8s/runtime/pkg/runtime/log"
	"github.com/aws/aws-sdk-go-v2/aws"
	svcsdk "github.com/aws/aws-sdk-go-v2/service/dynamodb"
	svcsdktypes "github.com/aws/aws-sdk-go-v2/service/dynamodb/types"

	"github.com/aws-controllers-k8s/dynamodb-controller/apis/v1alpha1"
	svcapitypes "github.com/aws-controllers-k8s/dynamodb-controller/apis/v1alpha1"
)

// equalCreateReplicationGroupMemberActions compares two CreateReplicationGroupMemberAction objects
func equalCreateReplicationGroupMemberActions(a, b *v1alpha1.CreateReplicationGroupMemberAction) bool {
	if !equalStrings(a.RegionName, b.RegionName) {
		return false
	}
	if !equalStrings(a.KMSMasterKeyID, b.KMSMasterKeyID) {
		return false
	}
	if !equalStrings(a.TableClassOverride, b.TableClassOverride) {
		return false
	}
	if a.ProvisionedThroughputOverride != nil && b.ProvisionedThroughputOverride != nil {
		if !equalInt64s(a.ProvisionedThroughputOverride.ReadCapacityUnits, b.ProvisionedThroughputOverride.ReadCapacityUnits) {
			return false
		}
	} else if (a.ProvisionedThroughputOverride == nil) != (b.ProvisionedThroughputOverride == nil) {
		return false
	}

	return equalReplicaGlobalSecondaryIndexArrays(a.GlobalSecondaryIndexes, b.GlobalSecondaryIndexes)
}

// equalReplicaGlobalSecondaryIndexes compares two ReplicaGlobalSecondaryIndex objects
func equalReplicaGlobalSecondaryIndexes(
	a *v1alpha1.ReplicaGlobalSecondaryIndex,
	b *v1alpha1.ReplicaGlobalSecondaryIndex,
) bool {
	if !equalStrings(a.IndexName, b.IndexName) {
		return false
	}

	if a.ProvisionedThroughputOverride != nil && b.ProvisionedThroughputOverride != nil {
		if !equalInt64s(a.ProvisionedThroughputOverride.ReadCapacityUnits, b.ProvisionedThroughputOverride.ReadCapacityUnits) {
			return false
		}
	} else if (a.ProvisionedThroughputOverride == nil) != (b.ProvisionedThroughputOverride == nil) {
		return false
	}

	return true
}

// equalReplicaGlobalSecondaryIndexArrays compares two arrays of ReplicaGlobalSecondaryIndex objects
func equalReplicaGlobalSecondaryIndexArrays(
	a []*v1alpha1.ReplicaGlobalSecondaryIndex,
	b []*v1alpha1.ReplicaGlobalSecondaryIndex,
) bool {
	if len(a) != len(b) {
		return false
	}

	aGSIMap := make(map[string]*v1alpha1.ReplicaGlobalSecondaryIndex)
	bGSIMap := make(map[string]*v1alpha1.ReplicaGlobalSecondaryIndex)

	for _, gsi := range a {
		if gsi.IndexName != nil {
			aGSIMap[*gsi.IndexName] = gsi
		}
	}

	for _, gsi := range b {
		if gsi.IndexName != nil {
			bGSIMap[*gsi.IndexName] = gsi
		}
	}

	for indexName, aGSI := range aGSIMap {
		bGSI, exists := bGSIMap[indexName]
		if !exists {
			return false
		}

		if !equalReplicaGlobalSecondaryIndexes(aGSI, bGSI) {
			return false
		}
	}

	return true
}

// equalReplicaArrays returns whether two CreateReplicationGroupMemberAction arrays are equal or not.
func equalReplicaArrays(a, b []*v1alpha1.CreateReplicationGroupMemberAction) bool {
	if len(a) != len(b) {
		return false
	}

	aMap := make(map[string]*v1alpha1.CreateReplicationGroupMemberAction)
	bMap := make(map[string]*v1alpha1.CreateReplicationGroupMemberAction)

	for _, replica := range a {
		if replica.RegionName != nil {
			aMap[*replica.RegionName] = replica
		}
	}

	for _, replica := range b {
		if replica.RegionName != nil {
			bMap[*replica.RegionName] = replica
		}
	}

	for regionName, aReplica := range aMap {
		bReplica, exists := bMap[regionName]
		if !exists {
			return false
		}

		if !equalCreateReplicationGroupMemberActions(aReplica, bReplica) {
			return false
		}
	}

	for regionName := range bMap {
		if _, exists := aMap[regionName]; !exists {
			return false
		}
	}

	return true
}

// createReplicaUpdate creates a ReplicationGroupUpdate for creating a new replica
func createReplicaUpdate(replica *v1alpha1.CreateReplicationGroupMemberAction) svcsdktypes.ReplicationGroupUpdate {
	replicaUpdate := svcsdktypes.ReplicationGroupUpdate{}
	createAction := &svcsdktypes.CreateReplicationGroupMemberAction{}

	if replica.RegionName != nil {
		createAction.RegionName = aws.String(*replica.RegionName)
	}

	if replica.KMSMasterKeyID != nil {
		createAction.KMSMasterKeyId = aws.String(*replica.KMSMasterKeyID)
	}

	if replica.TableClassOverride != nil {
		createAction.TableClassOverride = svcsdktypes.TableClass(*replica.TableClassOverride)
	}

	if replica.ProvisionedThroughputOverride != nil {
		createAction.ProvisionedThroughputOverride = &svcsdktypes.ProvisionedThroughputOverride{}
		if replica.ProvisionedThroughputOverride.ReadCapacityUnits != nil {
			createAction.ProvisionedThroughputOverride.ReadCapacityUnits = replica.ProvisionedThroughputOverride.ReadCapacityUnits
		}
	}

	if replica.GlobalSecondaryIndexes != nil {
		gsiList := []svcsdktypes.ReplicaGlobalSecondaryIndex{}
		for _, gsi := range replica.GlobalSecondaryIndexes {
			replicaGSI := svcsdktypes.ReplicaGlobalSecondaryIndex{}
			if gsi.IndexName != nil {
				replicaGSI.IndexName = gsi.IndexName
			}
			if gsi.ProvisionedThroughputOverride != nil {
				replicaGSI.ProvisionedThroughputOverride = &svcsdktypes.ProvisionedThroughputOverride{}
				if gsi.ProvisionedThroughputOverride.ReadCapacityUnits != nil {
					replicaGSI.ProvisionedThroughputOverride.ReadCapacityUnits = gsi.ProvisionedThroughputOverride.ReadCapacityUnits
				}
			}
			gsiList = append(gsiList, replicaGSI)
		}
		createAction.GlobalSecondaryIndexes = gsiList
	}

	replicaUpdate.Create = createAction
	return replicaUpdate
}

// updateReplicaUpdate creates a ReplicationGroupUpdate for updating an existing replica
func updateReplicaUpdate(replica *v1alpha1.CreateReplicationGroupMemberAction) svcsdktypes.ReplicationGroupUpdate {
	replicaUpdate := svcsdktypes.ReplicationGroupUpdate{}
	updateAction := &svcsdktypes.UpdateReplicationGroupMemberAction{}

	if replica.RegionName != nil {
		updateAction.RegionName = aws.String(*replica.RegionName)
		// RegionName is required but doesn't count as a update
	}

	if replica.KMSMasterKeyID != nil {
		updateAction.KMSMasterKeyId = aws.String(*replica.KMSMasterKeyID)
	}

	if replica.TableClassOverride != nil {
		updateAction.TableClassOverride = svcsdktypes.TableClass(*replica.TableClassOverride)
	}

	if replica.ProvisionedThroughputOverride != nil &&
		replica.ProvisionedThroughputOverride.ReadCapacityUnits != nil {
		updateAction.ProvisionedThroughputOverride = &svcsdktypes.ProvisionedThroughputOverride{
			ReadCapacityUnits: replica.ProvisionedThroughputOverride.ReadCapacityUnits,
		}
	}

	// Only include GSIs that have provisioned throughput overrides
	var gsisWithOverrides []svcsdktypes.ReplicaGlobalSecondaryIndex
	for _, gsi := range replica.GlobalSecondaryIndexes {
		if gsi.IndexName != nil && gsi.ProvisionedThroughputOverride != nil &&
			gsi.ProvisionedThroughputOverride.ReadCapacityUnits != nil {
			gsisWithOverrides = append(gsisWithOverrides, svcsdktypes.ReplicaGlobalSecondaryIndex{
				IndexName: aws.String(*gsi.IndexName),
				ProvisionedThroughputOverride: &svcsdktypes.ProvisionedThroughputOverride{
					ReadCapacityUnits: gsi.ProvisionedThroughputOverride.ReadCapacityUnits,
				},
			})
		}
	}

	if len(gsisWithOverrides) > 0 {
		updateAction.GlobalSecondaryIndexes = gsisWithOverrides
	}

	// Check if there are any actual updates to perform
	// replica GSI updates are invalid updates since the GSI already exists on the source table
	hasUpdates := updateAction.KMSMasterKeyId != nil ||
		updateAction.TableClassOverride != "" ||
		updateAction.ProvisionedThroughputOverride != nil ||
		len(updateAction.GlobalSecondaryIndexes) > 0

	if hasUpdates {
		replicaUpdate.Update = updateAction
		return replicaUpdate
	}

	// If no valid updates, return an empty ReplicationGroupUpdate
	return svcsdktypes.ReplicationGroupUpdate{
		Update: nil,
	}
}

// deleteReplicaUpdate creates a ReplicationGroupUpdate for deleting an existing replica
func deleteReplicaUpdate(regionName string) svcsdktypes.ReplicationGroupUpdate {
	return svcsdktypes.ReplicationGroupUpdate{
		Delete: &svcsdktypes.DeleteReplicationGroupMemberAction{
			RegionName: aws.String(regionName),
		},
	}
}

// hasStreamSpecificationWithNewAndOldImages checks if the table has DynamoDB Streams enabled
// with the stream containing both the new and the old images of the item.
func hasStreamSpecificationWithNewAndOldImages(r *resource) bool {
	StreamEnabled := r.ko.Spec.StreamSpecification != nil &&
		r.ko.Spec.StreamSpecification.StreamEnabled != nil &&
		*r.ko.Spec.StreamSpecification.StreamEnabled
	StreamViewType := r.ko.Spec.StreamSpecification != nil &&
		r.ko.Spec.StreamSpecification.StreamViewType != nil &&
		*r.ko.Spec.StreamSpecification.StreamViewType == "NEW_AND_OLD_IMAGES"
	return StreamEnabled && StreamViewType
}

// syncReplicas updates the replica configuration for a table
func (rm *resourceManager) syncReplicas(
	ctx context.Context,
	latest *resource,
	desired *resource,
) (err error) {
	rlog := ackrtlog.FromContext(ctx)
	exit := rlog.Trace("rm.syncReplicas")
	defer func() {
		exit(err)
	}()

	input, replicasInQueue, err := rm.newUpdateTableReplicaUpdatesOneAtATimePayload(ctx, latest, desired)
	if err != nil {
		return err
	}

	// Call the UpdateTable API
	_, err = rm.sdkapi.UpdateTable(ctx, input)
	rm.metrics.RecordAPICall("UPDATE", "UpdateTable", err)
	if err != nil {
		return err
	}

	// If there are more replicas to process, requeue
	if replicasInQueue > 0 {
		rlog.Debug("more replica updates pending, will requeue",
			"table", *latest.ko.Spec.TableName,
			"remaining_updates", replicasInQueue)
		return requeueWaitWhileUpdating
	}

	return nil
}

// newUpdateTableReplicaUpdatesOneAtATimePayload creates the UpdateTable input payload for replica updates,
// processing only one replica at a time
func (rm *resourceManager) newUpdateTableReplicaUpdatesOneAtATimePayload(
	ctx context.Context,
	latest *resource,
	desired *resource,
) (input *svcsdk.UpdateTableInput, replicasInQueue int, err error) {
	rlog := ackrtlog.FromContext(ctx)
	exit := rlog.Trace("rm.newUpdateTableReplicaUpdatesOneAtATimePayload")
	defer func() {
		exit(err)
	}()

	createReplicas, updateReplicas, deleteRegions := computeReplicaupdatesDelta(latest, desired)

	input = &svcsdk.UpdateTableInput{
		TableName:      aws.String(*desired.ko.Spec.TableName),
		ReplicaUpdates: []svcsdktypes.ReplicationGroupUpdate{},
	}

	totalReplicasOperations := len(createReplicas) + len(updateReplicas) + len(deleteRegions)
	replicasInQueue = totalReplicasOperations - 1

	// Process replica updates in order: create, update, delete
	// We'll only perform one replica action at a time

	if len(createReplicas) > 0 {
		replica := *createReplicas[0]
		if checkIfReplicasInProgress(latest.ko.Status.Replicas, *replica.RegionName) {
			return nil, 0, requeueWaitReplicasActive
		}
		rlog.Debug("creating replica in region", "table", *desired.ko.Spec.TableName, "region", *replica.RegionName)
		input.ReplicaUpdates = append(input.ReplicaUpdates, createReplicaUpdate(createReplicas[0]))
		return input, replicasInQueue, nil
	}

	if len(updateReplicas) > 0 {
		replica := *updateReplicas[0]
		if checkIfReplicasInProgress(latest.ko.Status.Replicas, *replica.RegionName) {
			return nil, 0, requeueWaitReplicasActive
		}
		rlog.Debug("updating replica in region", "table", *desired.ko.Spec.TableName, "region", *replica.RegionName)
		updateReplica := updateReplicaUpdate(updateReplicas[0])
		if updateReplica.Update == nil {
			return nil, 0, requeueWaitReplicasActive
		}
		input.ReplicaUpdates = append(input.ReplicaUpdates, updateReplica)
		return input, replicasInQueue, nil
	}

	if len(deleteRegions) > 0 {
		replica := deleteRegions[0]
		if checkIfReplicasInProgress(latest.ko.Status.Replicas, replica) {
			return nil, 0, requeueWaitReplicasActive
		}
		rlog.Debug("deleting replica in region", "table", *desired.ko.Spec.TableName, "region", replica)
		input.ReplicaUpdates = append(input.ReplicaUpdates, deleteReplicaUpdate(deleteRegions[0]))
		return input, replicasInQueue, nil
	}

	return input, replicasInQueue, nil
}

// computeReplicaupdatesDelta calculates the replica updates needed to reconcile the latest state with the desired state
// Returns three slices: replicas to create, replicas to update, and region names to delete
func computeReplicaupdatesDelta(
	latest *resource,
	desired *resource,
) (
	createReplicas []*v1alpha1.CreateReplicationGroupMemberAction,
	updateReplicas []*v1alpha1.CreateReplicationGroupMemberAction,
	deleteRegions []string,
) {
	latestReplicas := make(map[string]*v1alpha1.CreateReplicationGroupMemberAction)
	if latest.ko.Spec.TableReplicas != nil {
		for _, replica := range latest.ko.Spec.TableReplicas {
			if replica.RegionName != nil {
				latestReplicas[*replica.RegionName] = replica
			}
		}
	}

	desiredReplicas := make(map[string]*v1alpha1.CreateReplicationGroupMemberAction)
	if desired != nil && desired.ko.Spec.TableReplicas != nil {
		for _, replica := range desired.ko.Spec.TableReplicas {
			if replica.RegionName != nil {
				desiredReplicas[*replica.RegionName] = replica
			}
		}
	}

	// Calculate replicas to create or update
	for desiredRegion, desiredReplica := range desiredReplicas {
		existingReplica, exists := latestReplicas[desiredRegion]
		if !exists {
			createReplicas = append(createReplicas, desiredReplica)
		} else if !equalCreateReplicationGroupMemberActions(existingReplica, desiredReplica) {
			updateReplicas = append(updateReplicas, desiredReplica)
		}
	}

	// Calculate regions to delete
	for regionName := range latestReplicas {
		if _, exists := desiredReplicas[regionName]; !exists {
			deleteRegions = append(deleteRegions, regionName)
		}
	}

	return createReplicas, updateReplicas, deleteRegions
}

func setTableReplicas(ko *svcapitypes.Table, replicas []svcsdktypes.ReplicaDescription) {
	if len(replicas) > 0 {
		tableReplicas := []*v1alpha1.CreateReplicationGroupMemberAction{}
		for _, replica := range replicas {
			replicaElem := &v1alpha1.CreateReplicationGroupMemberAction{}
			if replica.RegionName != nil {
				replicaElem.RegionName = replica.RegionName
			}
			if replica.KMSMasterKeyId != nil {
				replicaElem.KMSMasterKeyID = replica.KMSMasterKeyId
			}
			if replica.ProvisionedThroughputOverride != nil {
				replicaElem.ProvisionedThroughputOverride = &v1alpha1.ProvisionedThroughputOverride{
					ReadCapacityUnits: replica.ProvisionedThroughputOverride.ReadCapacityUnits,
				}
			}
			if replica.GlobalSecondaryIndexes != nil {
				gsiList := []*v1alpha1.ReplicaGlobalSecondaryIndex{}
				for _, gsi := range replica.GlobalSecondaryIndexes {
					gsiElem := &v1alpha1.ReplicaGlobalSecondaryIndex{
						IndexName: gsi.IndexName,
					}
					if gsi.ProvisionedThroughputOverride != nil {
						gsiElem.ProvisionedThroughputOverride = &v1alpha1.ProvisionedThroughputOverride{
							ReadCapacityUnits: gsi.ProvisionedThroughputOverride.ReadCapacityUnits,
						}
					}
					gsiList = append(gsiList, gsiElem)
				}
				replicaElem.GlobalSecondaryIndexes = gsiList
			}
			if replica.ReplicaTableClassSummary != nil && replica.ReplicaTableClassSummary.TableClass != "" {
				replicaElem.TableClassOverride = aws.String(string(replica.ReplicaTableClassSummary.TableClass))
			}
			tableReplicas = append(tableReplicas, replicaElem)
		}
		ko.Spec.TableReplicas = tableReplicas
	} else {
		ko.Spec.TableReplicas = nil
	}
}

func checkIfReplicasInProgress(ReplicaDescription []*svcapitypes.ReplicaDescription, regionName string) bool {
	for _, replica := range ReplicaDescription {
		if *replica.RegionName == regionName {
			replicaStatus := replica.ReplicaStatus
			if *replicaStatus == string(svcsdktypes.ReplicaStatusCreating) || *replicaStatus == string(svcsdktypes.ReplicaStatusDeleting) || *replicaStatus == string(svcsdktypes.ReplicaStatusUpdating) {
				return true
			}
		}
	}

	return false
}
