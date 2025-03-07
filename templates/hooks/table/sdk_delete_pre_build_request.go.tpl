	if isTableDeleting(r) {
		return nil, requeueWaitWhileDeleting
	}
	if isTableUpdating(r) {
		return nil, requeueWaitWhileUpdating
	}

	// If there are replicas, we need to remove them before deleting the table
	if r.ko.Spec.Replicas != nil && len(r.ko.Spec.Replicas) > 0 {
		// Create a desired state with no replicas
		desired := &resource{
			ko: r.ko.DeepCopy(),
		}
		desired.ko.Spec.Replicas = nil

		// Call syncReplicaUpdates to remove all replicas
		err := rm.syncReplicaUpdates(ctx, r, desired)
		if err != nil {
			if err == requeueWaitWhileUpdating {
				// This is expected - we need to wait for the replica removal to complete
				return nil, err
			}
			return nil, err
		}
	}

	r.ko.Spec.Replicas = nil
