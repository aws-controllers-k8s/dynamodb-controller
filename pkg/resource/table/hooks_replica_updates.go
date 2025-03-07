package table

import (
	"context"
	"errors"

	ackerr "github.com/aws-controllers-k8s/runtime/pkg/errors"
	ackrtlog "github.com/aws-controllers-k8s/runtime/pkg/runtime/log"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws-controllers-k8s/dynamodb-controller/apis/v1alpha1"
	svcsdk "github.com/aws/aws-sdk-go-v2/service/dynamodb"
	svcsdktypes "github.com/aws/aws-sdk-go-v2/service/dynamodb/types"
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
				replicaGSI.IndexName = aws.String(*gsi.IndexName)
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
	}

	if replica.KMSMasterKeyID != nil {
		updateAction.KMSMasterKeyId = aws.String(*replica.KMSMasterKeyID)
	}

	if replica.TableClassOverride != nil {
		updateAction.TableClassOverride = svcsdktypes.TableClass(*replica.TableClassOverride)
	}

	if replica.ProvisionedThroughputOverride != nil {
		updateAction.ProvisionedThroughputOverride = &svcsdktypes.ProvisionedThroughputOverride{}
		if replica.ProvisionedThroughputOverride.ReadCapacityUnits != nil {
			updateAction.ProvisionedThroughputOverride.ReadCapacityUnits = replica.ProvisionedThroughputOverride.ReadCapacityUnits
		}
	}

	if replica.GlobalSecondaryIndexes != nil {
		gsiList := []svcsdktypes.ReplicaGlobalSecondaryIndex{}
		for _, gsi := range replica.GlobalSecondaryIndexes {
			replicaGSI := svcsdktypes.ReplicaGlobalSecondaryIndex{}
			if gsi.IndexName != nil {
				replicaGSI.IndexName = aws.String(*gsi.IndexName)
			}
			if gsi.ProvisionedThroughputOverride != nil {
				replicaGSI.ProvisionedThroughputOverride = &svcsdktypes.ProvisionedThroughputOverride{}
				if gsi.ProvisionedThroughputOverride.ReadCapacityUnits != nil {
					replicaGSI.ProvisionedThroughputOverride.ReadCapacityUnits = gsi.ProvisionedThroughputOverride.ReadCapacityUnits
				}
			}
			gsiList = append(gsiList, replicaGSI)
		}
		updateAction.GlobalSecondaryIndexes = gsiList
	}

	replicaUpdate.Update = updateAction
	return replicaUpdate
}

// deleteReplicaUpdate creates a ReplicationGroupUpdate for deleting an existing replica
func deleteReplicaUpdate(regionName string) svcsdktypes.ReplicationGroupUpdate {
	return svcsdktypes.ReplicationGroupUpdate{
		Delete: &svcsdktypes.DeleteReplicationGroupMemberAction{
			RegionName: aws.String(regionName),
		},
	}
}

// canUpdateTableReplicas returns true if it's possible to update table replicas.
// We can only modify replicas when they are in ACTIVE state.
func canUpdateTableReplicas(r *resource) bool {
	if isTableCreating(r) || isTableDeleting(r) || isTableUpdating(r) {
		return false
	}

	// Check if any replica is not in ACTIVE state
	if r.ko.Status.ReplicasDescriptions != nil {
		for _, replicaDesc := range r.ko.Status.ReplicasDescriptions {
			if replicaDesc.RegionName != nil && replicaDesc.ReplicaStatus != nil {
				if *replicaDesc.ReplicaStatus != string(svcsdktypes.ReplicaStatusActive) {
					return false
				}
			}
		}
	}
	return true
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

// syncReplicaUpdates updates the replica configuration for a table
func (rm *resourceManager) syncReplicaUpdates(
	ctx context.Context,
	latest *resource,
	desired *resource,
) (err error) {
	rlog := ackrtlog.FromContext(ctx)
	exit := rlog.Trace("rm.syncReplicaUpdates")
	defer exit(err)

	if !hasStreamSpecificationWithNewAndOldImages(desired) {
		msg := "table must have DynamoDB Streams enabled with StreamViewType set to NEW_AND_OLD_IMAGES for replica updates"
		rlog.Debug(msg)
		return ackerr.NewTerminalError(errors.New(msg))
	}

	if !canUpdateTableReplicas(latest) {
		return requeueWaitForReplicasActive
	}

	if isTableUpdating(latest) {
		return requeueWaitWhileUpdating
	}

	input, replicasInQueue, err := rm.newUpdateTableReplicaUpdatesOneAtATimePayload(ctx, latest, desired)
	if err != nil {
		return err
	}

	// If there are no updates to make, we don't need to requeue
	if len(input.ReplicaUpdates) == 0 {
		return nil
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
	defer exit(err)

	createReplicas, updateReplicas, deleteRegions := calculateReplicaUpdates(latest, desired)

	input = &svcsdk.UpdateTableInput{
		TableName:      aws.String(*desired.ko.Spec.TableName),
		ReplicaUpdates: []svcsdktypes.ReplicationGroupUpdate{},
	}

	totalReplicasOperations := len(createReplicas) + len(updateReplicas) + len(deleteRegions)
	replicasInQueue = totalReplicasOperations - 1

	// Process replica updates in order: create, update, delete
	// We'll only perform one replica action at a time

	if len(createReplicas) > 0 {
		region := *createReplicas[0].RegionName
		rlog.Debug("creating replica in region", "table", *desired.ko.Spec.TableName, "region", region)
		input.ReplicaUpdates = append(input.ReplicaUpdates, createReplicaUpdate(createReplicas[0]))
		return input, replicasInQueue, nil
	}

	if len(updateReplicas) > 0 {
		region := *updateReplicas[0].RegionName
		rlog.Debug("updating replica in region", "table", *desired.ko.Spec.TableName, "region", region)
		input.ReplicaUpdates = append(input.ReplicaUpdates, updateReplicaUpdate(updateReplicas[0]))
		return input, replicasInQueue, nil
	}

	if len(deleteRegions) > 0 {
		region := deleteRegions[0]
		rlog.Debug("deleting replica in region", "table", *desired.ko.Spec.TableName, "region", region)
		input.ReplicaUpdates = append(input.ReplicaUpdates, deleteReplicaUpdate(deleteRegions[0]))
		return input, replicasInQueue, nil
	}

	return input, replicasInQueue, nil
}

// calculateReplicaUpdates calculates the replica updates needed to reconcile the latest state with the desired state
// Returns three slices: replicas to create, replicas to update, and region names to delete
func calculateReplicaUpdates(
	latest *resource,
	desired *resource,
) (
	createReplicas []*v1alpha1.CreateReplicationGroupMemberAction,
	updateReplicas []*v1alpha1.CreateReplicationGroupMemberAction,
	deleteRegions []string,
) {
	existingRegions := make(map[string]*v1alpha1.CreateReplicationGroupMemberAction)
	if latest != nil && latest.ko.Spec.Replicas != nil {
		for _, replica := range latest.ko.Spec.Replicas {
			if replica.RegionName != nil {
				existingRegions[*replica.RegionName] = replica
			}
		}
	}

	desiredRegions := make(map[string]*v1alpha1.CreateReplicationGroupMemberAction)
	if desired != nil && desired.ko.Spec.Replicas != nil {
		for _, replica := range desired.ko.Spec.Replicas {
			if replica.RegionName != nil {
				desiredRegions[*replica.RegionName] = replica
			}
		}
	}

	// Calculate replicas to create or update
	for regionName, desiredReplica := range desiredRegions {
		existingReplica, exists := existingRegions[regionName]
		if !exists {
			createReplicas = append(createReplicas, desiredReplica)
		} else if !equalCreateReplicationGroupMemberActions(existingReplica, desiredReplica) {
			updateReplicas = append(updateReplicas, desiredReplica)
		}
	}

	// Calculate regions to delete
	for regionName := range existingRegions {
		if _, exists := desiredRegions[regionName]; !exists {
			deleteRegions = append(deleteRegions, regionName)
		}
	}

	return createReplicas, updateReplicas, deleteRegions
}
