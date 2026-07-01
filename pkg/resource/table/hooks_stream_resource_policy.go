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
	"errors"

	ackcompare "github.com/aws-controllers-k8s/runtime/pkg/compare"
	ackrtlog "github.com/aws-controllers-k8s/runtime/pkg/runtime/log"
	svcsdk "github.com/aws/aws-sdk-go-v2/service/dynamodb"
	svcsdktypes "github.com/aws/aws-sdk-go-v2/service/dynamodb/types"
)

// syncStreamResourcePolicy attaches, updates, or removes the resource-based
// policy on the table's DynamoDB stream. The policy is applied to the stream
// ARN (Status.LatestStreamARN), which is distinct from the table ARN. The same
// PutResourcePolicy/DeleteResourcePolicy APIs are used for both tables and
// streams.
func (rm *resourceManager) syncStreamResourcePolicy(
	ctx context.Context,
	desired *resource,
	latest *resource,
) (err error) {
	rlog := ackrtlog.FromContext(ctx)
	exit := rlog.Trace("rm.syncStreamResourcePolicy")
	defer func(err error) { exit(err) }(err)

	if desired.ko.Spec.StreamResourcePolicy == nil {
		return rm.deleteStreamResourcePolicy(ctx, latest)
	}

	return rm.putStreamResourcePolicy(ctx, desired, latest)
}

// putStreamResourcePolicy attaches or updates the resource-based policy on the
// table's DynamoDB stream. The stream ARN is read from the latest resource's
// status since it is assigned by DynamoDB when Streams is enabled.
func (rm *resourceManager) putStreamResourcePolicy(
	ctx context.Context,
	desired *resource,
	latest *resource,
) (err error) {
	rlog := ackrtlog.FromContext(ctx)
	exit := rlog.Trace("rm.putStreamResourcePolicy")
	defer func(err error) { exit(err) }(err)

	if desired.ko.Spec.StreamResourcePolicy == nil {
		return nil
	}

	streamARN := latest.ko.Status.LatestStreamARN
	if streamARN == nil || *streamARN == "" {
		return errors.New("stream ARN is required to put stream resource policy")
	}

	_, err = rm.sdkapi.PutResourcePolicy(
		ctx,
		&svcsdk.PutResourcePolicyInput{
			ResourceArn: streamARN,
			Policy:      desired.ko.Spec.StreamResourcePolicy,
		},
	)
	rm.metrics.RecordAPICall("UPDATE", "PutResourcePolicy", err)
	return err
}

// deleteStreamResourcePolicy removes the resource-based policy from the table's
// DynamoDB stream.
func (rm *resourceManager) deleteStreamResourcePolicy(
	ctx context.Context,
	latest *resource,
) (err error) {
	rlog := ackrtlog.FromContext(ctx)
	exit := rlog.Trace("rm.deleteStreamResourcePolicy")
	defer func(err error) { exit(err) }(err)

	streamARN := latest.ko.Status.LatestStreamARN
	if streamARN == nil || *streamARN == "" {
		// No stream (or no stream ARN yet) means there is no stream policy to
		// remove.
		return nil
	}

	_, err = rm.sdkapi.DeleteResourcePolicy(
		ctx,
		&svcsdk.DeleteResourcePolicyInput{
			ResourceArn: streamARN,
		},
	)
	rm.metrics.RecordAPICall("DELETE", "DeleteResourcePolicy", err)
	if err != nil {
		var policyNotFoundErr *svcsdktypes.PolicyNotFoundException
		if errors.As(err, &policyNotFoundErr) {
			// Policy already doesn't exist, this is a success case.
			return nil
		}
	}

	return err
}

// compareStreamResourcePolicyDocument records a delta when the desired and
// latest stream resource-policy documents differ. See
// compareResourcePolicyDocument / policyDocumentsDiffer for why a JSON-aware
// comparison is required.
func compareStreamResourcePolicyDocument(
	delta *ackcompare.Delta,
	a *resource,
	b *resource,
) {
	if policyDocumentsDiffer(a.ko.Spec.StreamResourcePolicy, b.ko.Spec.StreamResourcePolicy) {
		delta.Add(
			"Spec.StreamResourcePolicy",
			a.ko.Spec.StreamResourcePolicy,
			b.ko.Spec.StreamResourcePolicy,
		)
	}
}
