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
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"

	"github.com/aws-controllers-k8s/dynamodb-controller/apis/v1alpha1"
	compare "github.com/aws-controllers-k8s/runtime/pkg/compare"
)

func Test_compareStreamResourcePolicyDocument(t *testing.T) {
	streamPolicy := func(policy *string) *resource {
		return &resource{
			ko: &v1alpha1.Table{
				Spec: v1alpha1.TableSpec{
					StreamResourcePolicy: policy,
				},
			},
		}
	}

	tests := []struct {
		name          string
		a             *resource
		b             *resource
		wantDifferent bool
	}{
		{
			name:          "both policies are nil",
			a:             streamPolicy(nil),
			b:             streamPolicy(nil),
			wantDifferent: false,
		},
		{
			name:          "desired policy is set, latest policy is nil",
			a:             streamPolicy(aws.String(`{"Version": "2012-10-17"}`)),
			b:             streamPolicy(nil),
			wantDifferent: true,
		},
		{
			name:          "desired policy is nil, latest policy is set",
			a:             streamPolicy(nil),
			b:             streamPolicy(aws.String(`{"Version": "2012-10-17"}`)),
			wantDifferent: true,
		},
		{
			name: "same policy content with different whitespace formatting",
			a: streamPolicy(aws.String(`{
				"Version": "2012-10-17",
				"Statement": [
					{
						"Effect": "Allow",
						"Principal": {"AWS": "arn:aws:iam::123456789012:root"},
						"Action": ["dynamodb:GetRecords", "dynamodb:GetShardIterator"],
						"Resource": "arn:aws:dynamodb:us-west-2:123456789012:table/MyTable/stream/2024-01-01T00:00:00.000"
					}
				]
			}`)),
			b:             streamPolicy(aws.String(`{"Version":"2012-10-17","Statement":[{"Effect":"Allow","Principal":{"AWS":"arn:aws:iam::123456789012:root"},"Action":["dynamodb:GetRecords","dynamodb:GetShardIterator"],"Resource":"arn:aws:dynamodb:us-west-2:123456789012:table/MyTable/stream/2024-01-01T00:00:00.000"}]}`)),
			wantDifferent: false,
		},
		{
			name: "different actions in statement",
			a: streamPolicy(aws.String(`{
				"Version": "2012-10-17",
				"Statement": [
					{
						"Effect": "Allow",
						"Principal": {"AWS": "arn:aws:iam::123456789012:root"},
						"Action": ["dynamodb:GetRecords"],
						"Resource": "arn:aws:dynamodb:us-west-2:123456789012:table/MyTable/stream/2024-01-01T00:00:00.000"
					}
				]
			}`)),
			b: streamPolicy(aws.String(`{
				"Version": "2012-10-17",
				"Statement": [
					{
						"Effect": "Allow",
						"Principal": {"AWS": "arn:aws:iam::123456789012:root"},
						"Action": ["dynamodb:GetRecords", "dynamodb:GetShardIterator"],
						"Resource": "arn:aws:dynamodb:us-west-2:123456789012:table/MyTable/stream/2024-01-01T00:00:00.000"
					}
				]
			}`)),
			wantDifferent: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			delta := compare.NewDelta()
			compareStreamResourcePolicyDocument(delta, tt.a, tt.b)
			gotDifferent := delta.DifferentAt("Spec.StreamResourcePolicy")
			if gotDifferent != tt.wantDifferent {
				t.Errorf(
					"compareStreamResourcePolicyDocument() DifferentAt = %v, want %v",
					gotDifferent, tt.wantDifferent,
				)
			}
		})
	}
}
