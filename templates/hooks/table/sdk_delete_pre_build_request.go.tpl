	if isTableDeleting(r) {
		return nil, requeueWaitWhileDeleting
	}
	if isTableUpdating(r) {
		return nil, requeueWaitWhileUpdating
	}

	// If there are replicas, we need to remove them before deleting the table
	if len(r.ko.Spec.TableReplicas) > 0 {
		desired := &resource{
			ko: r.ko.DeepCopy(),
		}
		desired.ko.Spec.TableReplicas = nil

		err := rm.syncReplicas(ctx, r, desired)
		if err != nil {
			return nil, err
		}
		// Requeue to wait for replica removal to complete before attempting table deletion
		// When syncReplicas returns an error other than requeue
		return r, requeueWaitWhileDeleting
	}
