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
	svcsdk "github.com/aws/aws-sdk-go/service/dynamodb"

	"github.com/aws-controllers-k8s/dynamodb-controller/apis/v1alpha1"
)

// syncTTL updates a dynamodb table's TimeToLive property.
func (rm *resourceManager) syncTTL(
	ctx context.Context,
	desired *resource,
	latest *resource,
) (err error) {
	rlog := ackrtlog.FromContext(ctx)
	exit := rlog.Trace("rm.syncTTL")
	defer func(err error) { exit(err) }(err)

	ttlSpec := &svcsdk.TimeToLiveSpecification{}
	if desired.ko.Spec.TimeToLive != nil {
		ttlSpec.AttributeName = desired.ko.Spec.TimeToLive.AttributeName
		ttlSpec.Enabled = desired.ko.Spec.TimeToLive.Enabled
	} else {
		// In order to disable the TTL, we can't simply call the
		// `UpdateTimeToLive` method with an empty specification. Instead, we
		// must explicitly set the enabled to false and provide the attribute
		// name of the existing TTL.
		currentAttrName := ""
		if latest.ko.Spec.TimeToLive != nil &&
			latest.ko.Spec.TimeToLive.AttributeName != nil {
			currentAttrName = *latest.ko.Spec.TimeToLive.AttributeName
		}

		ttlSpec.SetAttributeName(currentAttrName)
		ttlSpec.SetEnabled(false)
	}

	_, err = rm.sdkapi.UpdateTimeToLiveWithContext(
		ctx,
		&svcsdk.UpdateTimeToLiveInput{
			TableName:               desired.ko.Spec.TableName,
			TimeToLiveSpecification: ttlSpec,
		},
	)
	rm.metrics.RecordAPICall("UPDATE", "UpdateTimeToLive", err)
	return err
}

// getResourceTTLWithContext queries the table TTL of a given resource.
func (rm *resourceManager) getResourceTTLWithContext(ctx context.Context, tableName *string) (*v1alpha1.TimeToLiveSpecification, error) {
	var err error
	rlog := ackrtlog.FromContext(ctx)
	exit := rlog.Trace("rm.getResourceTTLWithContext")
	defer func(err error) { exit(err) }(err)

	res, err := rm.sdkapi.DescribeTimeToLiveWithContext(
		ctx,
		&svcsdk.DescribeTimeToLiveInput{
			TableName: tableName,
		},
	)
	rm.metrics.RecordAPICall("GET", "DescribeTimeToLive", err)
	if err != nil {
		return nil, err
	}

	// Treat status "ENABLING" and "ENABLED" as `Enabled` == true
	isEnabled := *res.TimeToLiveDescription.TimeToLiveStatus == svcsdk.TimeToLiveStatusEnabled ||
		*res.TimeToLiveDescription.TimeToLiveStatus == svcsdk.TimeToLiveStatusEnabling

	return &v1alpha1.TimeToLiveSpecification{
		AttributeName: res.TimeToLiveDescription.AttributeName,
		Enabled:       &isEnabled,
	}, nil
}
