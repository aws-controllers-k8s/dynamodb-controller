    if desired.ko.Spec.TimeToLive != nil {
		if err := rm.syncTTL(ctx, desired, &resource{ko}); err != nil {
			return nil, err
		}
	}
	// Check if replicas were specified during creation
	if len(desired.ko.Spec.ReplicationGroup) > 0 {
		ko.Spec.ReplicationGroup = desired.ko.Spec.ReplicationGroup
		
		// Return with a requeue to process replica updates
		// This will trigger the reconciliation loop again, which will call syncReplicaUpdates
		return &resource{ko}, requeueWaitWhileUpdating
	}