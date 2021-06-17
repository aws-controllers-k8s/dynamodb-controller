	if isBackupCreating(&resource{ko}) {
		return &resource{ko}, requeueWaitWhileCreating
	}