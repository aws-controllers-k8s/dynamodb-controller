	if isTableDeleting(r) {
		return requeueWaitWhileDeleting
	}
	if isTableUpdating(r) {
		return requeueWaitWhileUpdating
	}