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

	"github.com/aws/aws-sdk-go/aws"

	"github.com/aws-controllers-k8s/dynamodb-controller/apis/v1alpha1"
	compare "github.com/aws-controllers-k8s/runtime/pkg/compare"
)

func Test_compareResourcePolicyDocument(t *testing.T) {
	type args struct {
		a *resource
		b *resource
	}
	tests := []struct {
		name          string
		args          args
		wantDifferent bool
	}{
		{
			name: "both policies are nil",
			args: args{
				a: &resource{
					ko: &v1alpha1.Table{
						Spec: v1alpha1.TableSpec{
							ResourcePolicy: nil,
						},
					},
				},
				b: &resource{
					ko: &v1alpha1.Table{
						Spec: v1alpha1.TableSpec{
							ResourcePolicy: nil,
						},
					},
				},
			},
			wantDifferent: false,
		},
		{
			name: "desired policy is set, latest policy is nil",
			args: args{
				a: &resource{
					ko: &v1alpha1.Table{
						Spec: v1alpha1.TableSpec{
							ResourcePolicy: aws.String(`{"Version": "2012-10-17"}`),
						},
					},
				},
				b: &resource{
					ko: &v1alpha1.Table{
						Spec: v1alpha1.TableSpec{
							ResourcePolicy: nil,
						},
					},
				},
			},
			wantDifferent: true,
		},
		{
			name: "identical policy documents",
			args: args{
				a: &resource{
					ko: &v1alpha1.Table{
						Spec: v1alpha1.TableSpec{
							ResourcePolicy: aws.String(`{
								"Version": "2012-10-17",
								"Statement": [
									{
										"Effect": "Allow",
										"Principal": {"AWS": "arn:aws:iam::123456789012:root"},
										"Action": ["dynamodb:GetItem", "dynamodb:Query"],
										"Resource": "arn:aws:dynamodb:us-west-2:123456789012:table/MyTable"
									}
								]
							}`),
						},
					},
				},
				b: &resource{
					ko: &v1alpha1.Table{
						Spec: v1alpha1.TableSpec{
							ResourcePolicy: aws.String(`{
								"Version": "2012-10-17",
								"Statement": [
									{
										"Effect": "Allow",
										"Principal": {"AWS": "arn:aws:iam::123456789012:root"},
										"Action": ["dynamodb:GetItem", "dynamodb:Query"],
										"Resource": "arn:aws:dynamodb:us-west-2:123456789012:table/MyTable"
									}
								]
							}`),
						},
					},
				},
			},
			wantDifferent: false,
		},
		{
			name: "same policy content with different whitespace formatting",
			args: args{
				a: &resource{
					ko: &v1alpha1.Table{
						Spec: v1alpha1.TableSpec{
							ResourcePolicy: aws.String(`{
								"Version": "2012-10-17",
								"Statement": [
									{
										"Effect": "Allow",
										"Principal": {"AWS": "arn:aws:iam::123456789012:root"},
										"Action": ["dynamodb:GetItem", "dynamodb:Query"],
										"Resource": "arn:aws:dynamodb:us-west-2:123456789012:table/MyTable"
									}
								]
							}`),
						},
					},
				},
				b: &resource{
					ko: &v1alpha1.Table{
						Spec: v1alpha1.TableSpec{
							ResourcePolicy: aws.String(`{"Version":"2012-10-17","Statement":[{"Effect":"Allow","Principal":{"AWS":"arn:aws:iam::123456789012:root"},"Action":["dynamodb:GetItem","dynamodb:Query"],"Resource":"arn:aws:dynamodb:us-west-2:123456789012:table/MyTable"}]}`),
						},
					},
				},
			},
			wantDifferent: false,
		},
		{
			name: "different effect in statement",
			args: args{
				a: &resource{
					ko: &v1alpha1.Table{
						Spec: v1alpha1.TableSpec{
							ResourcePolicy: aws.String(`{
								"Version": "2012-10-17",
								"Statement": [
									{
										"Effect": "Allow",
										"Principal": {"AWS": "arn:aws:iam::123456789012:root"},
										"Action": ["dynamodb:GetItem"],
										"Resource": "arn:aws:dynamodb:us-west-2:123456789012:table/MyTable"
									}
								]
							}`),
						},
					},
				},
				b: &resource{
					ko: &v1alpha1.Table{
						Spec: v1alpha1.TableSpec{
							ResourcePolicy: aws.String(`{
								"Version": "2012-10-17",
								"Statement": [
									{
										"Effect": "Deny",
										"Principal": {"AWS": "arn:aws:iam::123456789012:root"},
										"Action": ["dynamodb:GetItem"],
										"Resource": "arn:aws:dynamodb:us-west-2:123456789012:table/MyTable"
									}
								]
							}`),
						},
					},
				},
			},
			wantDifferent: true,
		},
		{
			name: "different actions in statement",
			args: args{
				a: &resource{
					ko: &v1alpha1.Table{
						Spec: v1alpha1.TableSpec{
							ResourcePolicy: aws.String(`{
								"Version": "2012-10-17",
								"Statement": [
									{
										"Effect": "Allow",
										"Principal": {"AWS": "arn:aws:iam::123456789012:root"},
										"Action": ["dynamodb:GetItem"],
										"Resource": "arn:aws:dynamodb:us-west-2:123456789012:table/MyTable"
									}
								]
							}`),
						},
					},
				},
				b: &resource{
					ko: &v1alpha1.Table{
						Spec: v1alpha1.TableSpec{
							ResourcePolicy: aws.String(`{
								"Version": "2012-10-17",
								"Statement": [
									{
										"Effect": "Allow",
										"Principal": {"AWS": "arn:aws:iam::123456789012:root"},
										"Action": ["dynamodb:Query"],
										"Resource": "arn:aws:dynamodb:us-west-2:123456789012:table/MyTable"
									}
								]
							}`),
						},
					},
				},
			},
			wantDifferent: true,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			delta := &compare.Delta{}
			compareResourcePolicyDocument(delta, tt.args.a, tt.args.b)
			if got := delta.DifferentAt("Spec.ResourcePolicy"); got != tt.wantDifferent {
				t.Errorf("compareResourcePolicyDocument() difference = %v, want %v", got, tt.wantDifferent)
			}
		})
	}
}
