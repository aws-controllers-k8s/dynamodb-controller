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
	svcsdk "github.com/aws/aws-sdk-go-v2/service/dynamodb"
	svcsdktypes "github.com/aws/aws-sdk-go-v2/service/dynamodb/types"

	"github.com/aws-controllers-k8s/dynamodb-controller/apis/v1alpha1"
)

// syncContinuousBackup syncs the PointInTimeRecoverySpecification of the dynamodb table.
func (rm *resourceManager) syncContinuousBackup(
	ctx context.Context,
	desired *resource,
) (err error) {
	rlog := ackrtlog.FromContext(ctx)
	exit := rlog.Trace("rm.syncContinuousBackup")
	defer func(err error) { exit(err) }(err)

	pitrSpec := &svcsdktypes.PointInTimeRecoverySpecification{}
	if desired.ko.Spec.ContinuousBackups != nil &&
		desired.ko.Spec.ContinuousBackups.PointInTimeRecoveryEnabled != nil {
		pitrSpec.PointInTimeRecoveryEnabled = desired.ko.Spec.ContinuousBackups.PointInTimeRecoveryEnabled
	}

	_, err = rm.sdkapi.UpdateContinuousBackups(
		ctx,
		&svcsdk.UpdateContinuousBackupsInput{
			TableName:                        desired.ko.Spec.TableName,
			PointInTimeRecoverySpecification: pitrSpec,
		},
	)
	rm.metrics.RecordAPICall("UPDATE", "UpdateContinuousBackups", err)
	return err
}

// getResourcePointInTimeRecoveryWithContext gets the PointInTimeRecoverySpecification of the dynamodb table.
func (rm *resourceManager) getResourcePointInTimeRecoveryWithContext(
	ctx context.Context,
	tableName *string,
) (*v1alpha1.PointInTimeRecoverySpecification, error) {
	var err error
	rlog := ackrtlog.FromContext(ctx)
	exit := rlog.Trace("rm.getResourcePointInTimeRecoveryWithContext")
	defer func(err error) { exit(err) }(err)

	res, err := rm.sdkapi.DescribeContinuousBackups(
		ctx,
		&svcsdk.DescribeContinuousBackupsInput{
			TableName: tableName,
		},
	)

	rm.metrics.RecordAPICall("GET", "DescribeContinuousBackups", err)
	if err != nil {
		return nil, err
	}

	isEnabled := false
	if res.ContinuousBackupsDescription != nil {
		isEnabled = res.ContinuousBackupsDescription.PointInTimeRecoveryDescription.PointInTimeRecoveryStatus == svcsdktypes.PointInTimeRecoveryStatusEnabled
	}

	return &v1alpha1.PointInTimeRecoverySpecification{
		PointInTimeRecoveryEnabled: &isEnabled,
	}, nil
}
