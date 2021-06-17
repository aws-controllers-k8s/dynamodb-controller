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

package backup

import (
	"errors"

	ackrequeue "github.com/aws-controllers-k8s/runtime/pkg/requeue"

	"github.com/aws-controllers-k8s/dynamodb-controller/apis/v1alpha1"
)

var (
	// TerminalStatuses are the status strings that are terminal states for a
	// backup.
	TerminalStatuses = []v1alpha1.BackupStatus_SDK{}
)

var (
	requeueWaitWhileCreating = ackrequeue.NeededAfter(
		errors.New("Backup in 'CREATING' state, cannot be modified or deleted."),
		ackrequeue.DefaultRequeueAfterDuration,
	)
)

// backupHasTerminalStatus returns whether the supplied backup is in a
// terminal state
func backupHasTerminalStatus(r *resource) bool {
	if r.ko.Status.BackupStatus == nil {
		return false
	}
	ts := *r.ko.Status.BackupStatus
	for _, s := range TerminalStatuses {
		if ts == string(s) {
			return true
		}
	}
	return false
}

// isBackupCreating returns true if the supplied Dynamodb backup is in the process
// of being created
func isBackupCreating(r *resource) bool {
	if r.ko.Status.BackupStatus == nil {
		return false
	}
	dbis := *r.ko.Status.BackupStatus
	return dbis == string(v1alpha1.BackupStatus_SDK_CREATING)
}
