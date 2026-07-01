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
	"encoding/json"
	"errors"
	"reflect"

	ackcompare "github.com/aws-controllers-k8s/runtime/pkg/compare"
	ackerr "github.com/aws-controllers-k8s/runtime/pkg/errors"
	ackrtlog "github.com/aws-controllers-k8s/runtime/pkg/runtime/log"
	svcsdk "github.com/aws/aws-sdk-go-v2/service/dynamodb"
	svcsdktypes "github.com/aws/aws-sdk-go-v2/service/dynamodb/types"
	awsiampolicy "github.com/micahhausler/aws-iam-policy/policy"
)

// syncResourcePolicy updates a DynamoDB table's resource-based policy.
func (rm *resourceManager) syncResourcePolicy(
	ctx context.Context,
	desired *resource,
	latest *resource,
) (err error) {
	rlog := ackrtlog.FromContext(ctx)
	exit := rlog.Trace("rm.syncResourcePolicy")
	defer func(err error) { exit(err) }(err)

	if desired.ko.Spec.ResourcePolicy == nil {
		return rm.deleteResourcePolicy(ctx, latest)
	}

	return rm.putResourcePolicy(ctx, desired)
}

// putResourcePolicy attaches or updates a resource-based policy to a DynamoDB table.
func (rm *resourceManager) putResourcePolicy(
	ctx context.Context,
	r *resource,
) (err error) {
	rlog := ackrtlog.FromContext(ctx)
	exit := rlog.Trace("rm.putResourcePolicy")
	defer func(err error) { exit(err) }(err)

	if r.ko.Spec.ResourcePolicy == nil {
		return nil
	}

	tableARN := (*string)(r.ko.Status.ACKResourceMetadata.ARN)
	if tableARN == nil || *tableARN == "" {
		return errors.New("table ARN is required to put resource policy")
	}

	_, err = rm.sdkapi.PutResourcePolicy(
		ctx,
		&svcsdk.PutResourcePolicyInput{
			ResourceArn: tableARN,
			Policy:      r.ko.Spec.ResourcePolicy,
		},
	)
	rm.metrics.RecordAPICall("UPDATE", "PutResourcePolicy", err)
	return err
}

// deleteResourcePolicy removes a resource-based policy from a DynamoDB table.
func (rm *resourceManager) deleteResourcePolicy(
	ctx context.Context,
	r *resource,
) (err error) {
	rlog := ackrtlog.FromContext(ctx)
	exit := rlog.Trace("rm.deleteResourcePolicy")
	defer func(err error) { exit(err) }(err)

	tableARN := (*string)(r.ko.Status.ACKResourceMetadata.ARN)
	if tableARN == nil || *tableARN == "" {
		return errors.New("table ARN is required to delete resource policy")
	}

	_, err = rm.sdkapi.DeleteResourcePolicy(
		ctx,
		&svcsdk.DeleteResourcePolicyInput{
			ResourceArn: tableARN,
		},
	)
	rm.metrics.RecordAPICall("DELETE", "DeleteResourcePolicy", err)
	if err != nil {
		var policyNotFoundErr *svcsdktypes.PolicyNotFoundException
		if errors.As(err, &policyNotFoundErr) {
			// Policy already doesn't exist, this is a success case
			return nil
		}
	}

	return err
}

// getResourcePolicyWithContext retrieves the resource-based policy of a DynamoDB table.
func (rm *resourceManager) getResourcePolicyWithContext(
	ctx context.Context,
	tableARN *string,
) (*string, error) {
	var err error
	rlog := ackrtlog.FromContext(ctx)
	exit := rlog.Trace("rm.getResourcePolicyWithContext")
	defer func(err error) { exit(err) }(err)

	if tableARN == nil || *tableARN == "" {
		return nil, errors.New("table ARN is required to get resource policy")
	}

	res, err := rm.sdkapi.GetResourcePolicy(
		ctx,
		&svcsdk.GetResourcePolicyInput{
			ResourceArn: tableARN,
		},
	)
	rm.metrics.RecordAPICall("GET", "GetResourcePolicy", nil)
	if err != nil {
		if awsErr, ok := ackerr.AWSError(err); ok && awsErr.ErrorCode() == "PolicyNotFoundException" {
			return nil, nil
		}
		return nil, err
	}

	return res.Policy, nil
}

// compareResourcePolicyDocument is a custom comparison function for
// ResourcePolicy documents. The reason why we need a custom function for
// this field is to handle the variability in shapes of JSON objects representing
// IAM policies, especially when it comes to statements, actions, and other fields.
func compareResourcePolicyDocument(
	delta *ackcompare.Delta,
	a *resource,
	b *resource,
) {
	if policyDocumentsDiffer(a.ko.Spec.ResourcePolicy, b.ko.Spec.ResourcePolicy) {
		delta.Add("Spec.ResourcePolicy", a.ko.Spec.ResourcePolicy, b.ko.Spec.ResourcePolicy)
	}
}

// policyDocumentsDiffer reports whether two IAM resource-policy JSON documents
// differ semantically. It handles the variability in shapes of JSON objects
// representing IAM policies, especially when it comes to statements, actions,
// and other fields, as well as differences in whitespace and key ordering. To
// do so it uses a custom json.Unmarshaller crafted for this need: @micahhausler
// built a library dedicated to it: github.com/micahhausler/aws-iam-policy.
//
// Copied from IAM Controller: https://github.com/aws-controllers-k8s/iam-controller/blob/main/pkg/resource/role/hooks.go#L398-L432
// Based on review feedback: https://github.com/aws-controllers-k8s/dynamodb-controller/pull/154#discussion_r2443876840
func policyDocumentsDiffer(a, b *string) bool {
	// If exactly one policy is nil, they're different.
	if ackcompare.HasNilDifference(a, b) {
		return true
	}
	// If both policies are nil, there's no difference.
	if a == nil && b == nil {
		return false
	}

	// At this point, both policies are non-nil, so compare their JSON content.
	var policyDocumentA awsiampolicy.Policy
	_ = json.Unmarshal([]byte(*a), &policyDocumentA)
	var policyDocumentB awsiampolicy.Policy
	_ = json.Unmarshal([]byte(*b), &policyDocumentB)

	return !reflect.DeepEqual(policyDocumentA, policyDocumentB)
}
