    if desired.ko.Spec.TimeToLive != nil {
		if err := rm.syncTTL(ctx, desired, &resource{ko}); err != nil {
			return nil, err
		}
	}
	// Check if replicas were specified during creation
	if desired.ko.Spec.Replicas != nil && len(desired.ko.Spec.Replicas) > 0 {
		// Copy the replica configuration to the new resource
		ko.Spec.Replicas = desired.ko.Spec.Replicas
		
		// Return with a requeue to process replica updates
		// This will trigger the reconciliation loop again, which will call syncReplicaUpdates
		return &resource{ko}, requeueWaitWhileUpdating
	}