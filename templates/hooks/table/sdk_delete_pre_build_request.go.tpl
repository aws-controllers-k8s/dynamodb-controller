	if isTableDeleting(r) {
		return nil, requeueWaitWhileDeleting
	}
	if isTableUpdating(r) {
		return nil, requeueWaitWhileUpdating
	}

	// If there are replicas, we need to remove them before deleting the table
	if len(r.ko.Spec.ReplicationGroup) > 0 {
		desired := &resource{
			ko: r.ko.DeepCopy(),
		}
		desired.ko.Spec.ReplicationGroup = nil

		err := rm.syncReplicaUpdates(ctx, r, desired)
		if err != nil {
			return nil, err
		}
	}
