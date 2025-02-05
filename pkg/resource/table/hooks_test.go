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
	"reflect"
	"testing"

	"github.com/aws-controllers-k8s/runtime/pkg/compare"
	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/stretchr/testify/require"

	"github.com/aws-controllers-k8s/dynamodb-controller/apis/v1alpha1"
)

var (
	Tag1 = &v1alpha1.Tag{
		Key:   aws.String("k1"),
		Value: aws.String("v1"),
	}
	Tag2 = &v1alpha1.Tag{
		Key:   aws.String("k2"),
		Value: aws.String("v2"),
	}
	Tag2Updated = &v1alpha1.Tag{
		Key:   aws.String("k2"),
		Value: aws.String("v2-updated"),
	}
	Tag3 = &v1alpha1.Tag{
		Key:   aws.String("k3"),
		Value: aws.String("v3"),
	}
)

func Test_computeTagsDelta(t *testing.T) {
	type args struct {
		a []*v1alpha1.Tag
		b []*v1alpha1.Tag
	}
	tests := []struct {
		name        string
		args        args
		wantAdded   []*v1alpha1.Tag
		wantRemoved []string
	}{
		{
			name: "nil arrays",
			args: args{
				a: nil,
				b: nil,
			},
			wantAdded:   nil,
			wantRemoved: nil,
		},
		{
			name: "empty arrays",
			args: args{
				a: []*v1alpha1.Tag{},
				b: []*v1alpha1.Tag{},
			},
			wantAdded:   nil,
			wantRemoved: nil,
		},
		{
			name: "added tags",
			args: args{
				a: []*v1alpha1.Tag{Tag1, Tag2},
				b: []*v1alpha1.Tag{},
			},
			wantAdded:   []*v1alpha1.Tag{Tag1, Tag2},
			wantRemoved: nil,
		},
		{
			name: "removed tags",
			args: args{
				a: nil,
				b: []*v1alpha1.Tag{Tag1, Tag2},
			},
			wantAdded:   nil,
			wantRemoved: []string{"k1", "k2"},
		},
		{
			name: "updated tags",
			args: args{
				a: []*v1alpha1.Tag{Tag1, Tag2Updated},
				b: []*v1alpha1.Tag{Tag1, Tag2},
			},
			wantAdded:   []*v1alpha1.Tag{Tag2Updated},
			wantRemoved: nil,
		},
		{
			name: "added, updated and removed tags",
			args: args{
				a: []*v1alpha1.Tag{Tag2Updated, Tag3},
				// remove Tag1, update Tag2 and add Tag3
				b: []*v1alpha1.Tag{Tag1, Tag2},
			},
			wantAdded:   []*v1alpha1.Tag{Tag2Updated, Tag3},
			wantRemoved: []string{"k1"},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			gotAdded, gotRemoved := computeTagsDelta(tt.args.a, tt.args.b)
			if !reflect.DeepEqual(gotAdded, tt.wantAdded) {
				t.Errorf("computeTagsDelta() gotAdded = %v, want %v", gotAdded, tt.wantAdded)
			}
			if !reflect.DeepEqual(gotRemoved, tt.wantRemoved) {
				t.Errorf("computeTagsDelta() gotRemoved = %v, want %v", gotRemoved, tt.wantRemoved)
			}
		})
	}
}

func Test_customPreCompare(t *testing.T) {
	t.Run("when billing mode is PAY_PER_REQUEST, ProvisionedThroughput should be nil", func(t *testing.T) {
		a := &resource{ko: &v1alpha1.Table{
			Spec: v1alpha1.TableSpec{
				BillingMode:           aws.String(string(v1alpha1.BillingMode_PAY_PER_REQUEST)),
				ProvisionedThroughput: &v1alpha1.ProvisionedThroughput{},
			},
		}}

		b := &resource{ko: &v1alpha1.Table{
			Spec: v1alpha1.TableSpec{
				BillingMode:           aws.String(string(v1alpha1.BillingMode_PAY_PER_REQUEST)),
				ProvisionedThroughput: &v1alpha1.ProvisionedThroughput{},
			},
		}}
		delta := &compare.Delta{}
		customPreCompare(delta, a, b)
		if a.ko.Spec.ProvisionedThroughput != nil {
			t.Errorf("a.Spec.ProvisionedThroughput should be nil, but got %+v", a.ko.Spec.ProvisionedThroughput)
		}

		if b.ko.Spec.ProvisionedThroughput != nil {
			t.Errorf("b.Spec.ProvisionedThroughput should be nil, but got %+v", a.ko.Spec.ProvisionedThroughput)
		}
	})

	t.Run("GSI ProvisionedThroughput should be equal when nil and 0 capacity", func(t *testing.T) {
		a := &resource{ko: &v1alpha1.Table{
			Spec: v1alpha1.TableSpec{
				BillingMode:           aws.String(string(v1alpha1.BillingMode_PAY_PER_REQUEST)),
				ProvisionedThroughput: &v1alpha1.ProvisionedThroughput{},
				GlobalSecondaryIndexes: []*v1alpha1.GlobalSecondaryIndex{
					{
						IndexName: aws.String("index1"),
						KeySchema: []*v1alpha1.KeySchemaElement{
							{
								AttributeName: aws.String("id"),
								KeyType:       aws.String("HASH"),
							},
							{
								AttributeName: aws.String("email"),
								KeyType:       aws.String("RANGE"),
							},
						},
						Projection: &v1alpha1.Projection{
							ProjectionType: aws.String("ALL"),
						},
						ProvisionedThroughput: nil,
					},
				},
			},
		}}

		b := &resource{ko: &v1alpha1.Table{
			Spec: v1alpha1.TableSpec{
				BillingMode:           aws.String(string(v1alpha1.BillingMode_PAY_PER_REQUEST)),
				ProvisionedThroughput: &v1alpha1.ProvisionedThroughput{},
				GlobalSecondaryIndexes: []*v1alpha1.GlobalSecondaryIndex{
					{
						IndexName: aws.String("index1"),
						KeySchema: []*v1alpha1.KeySchemaElement{
							{
								AttributeName: aws.String("id"),
								KeyType:       aws.String("HASH"),
							},
							{
								AttributeName: aws.String("email"),
								KeyType:       aws.String("RANGE"),
							},
						},
						Projection: &v1alpha1.Projection{
							ProjectionType: aws.String("ALL"),
						},
						ProvisionedThroughput: &v1alpha1.ProvisionedThroughput{
							ReadCapacityUnits:  aws.Int64(0),
							WriteCapacityUnits: aws.Int64(0),
						},
					},
				},
			},
		}}
		delta := &compare.Delta{}
		customPreCompare(delta, a, b)
		require.False(t, delta.DifferentAt("Spec.GlobalSecondaryIndexes"))

		// the following case should not happen, just in case
		c := &resource{ko: &v1alpha1.Table{
			Spec: v1alpha1.TableSpec{
				BillingMode:           aws.String(string(v1alpha1.BillingMode_PAY_PER_REQUEST)),
				ProvisionedThroughput: &v1alpha1.ProvisionedThroughput{},
				GlobalSecondaryIndexes: []*v1alpha1.GlobalSecondaryIndex{
					{
						IndexName: aws.String("index1"),
						KeySchema: []*v1alpha1.KeySchemaElement{
							{
								AttributeName: aws.String("id"),
								KeyType:       aws.String("HASH"),
							},
							{
								AttributeName: aws.String("email"),
								KeyType:       aws.String("RANGE"),
							},
						},
						Projection: &v1alpha1.Projection{
							ProjectionType: aws.String("ALL"),
						},
						ProvisionedThroughput: nil,
					},
				},
			},
		}}

		d := &resource{ko: &v1alpha1.Table{
			Spec: v1alpha1.TableSpec{
				BillingMode:           aws.String(string(v1alpha1.BillingMode_PAY_PER_REQUEST)),
				ProvisionedThroughput: &v1alpha1.ProvisionedThroughput{},
				GlobalSecondaryIndexes: []*v1alpha1.GlobalSecondaryIndex{
					{
						IndexName: aws.String("index1"),
						KeySchema: []*v1alpha1.KeySchemaElement{
							{
								AttributeName: aws.String("id"),
								KeyType:       aws.String("HASH"),
							},
							{
								AttributeName: aws.String("email"),
								KeyType:       aws.String("RANGE"),
							},
						},
						Projection: &v1alpha1.Projection{
							ProjectionType: aws.String("ALL"),
						},
						ProvisionedThroughput: &v1alpha1.ProvisionedThroughput{
							ReadCapacityUnits:  aws.Int64(0),
							WriteCapacityUnits: aws.Int64(0),
						},
					},
				},
			},
		}}
		customPreCompare(delta, c, d)
		require.False(t, delta.DifferentAt("Spec.GlobalSecondaryIndexes"))
	})
}

func Test_newResourceDelta_customDeltaFunction_AttributeDefinitions(t *testing.T) {
	type args struct {
		a *resource
		b *resource
	}
	tests := []struct {
		name string
		args args
		want bool
	}{
		{
			name: "both desired and latest are nil",
			args: args{
				a: &resource{
					ko: &v1alpha1.Table{
						Spec: v1alpha1.TableSpec{
							AttributeDefinitions: nil,
						},
					},
				},
				b: &resource{
					ko: &v1alpha1.Table{
						Spec: v1alpha1.TableSpec{
							AttributeDefinitions: nil,
						},
					},
				},
			},
			want: true,
		},
		{
			name: "desired is not nil",
			args: args{
				a: &resource{
					ko: &v1alpha1.Table{
						Spec: v1alpha1.TableSpec{
							AttributeDefinitions: []*v1alpha1.AttributeDefinition{
								{
									AttributeName: aws.String("externalId"),
									AttributeType: aws.String("S"),
								},
							},
						},
					},
				},
				b: &resource{
					ko: &v1alpha1.Table{
						Spec: v1alpha1.TableSpec{
							AttributeDefinitions: nil,
						},
					},
				},
			},
			want: false,
		},
		{
			name: "latest is not nil",
			args: args{
				a: &resource{
					ko: &v1alpha1.Table{
						Spec: v1alpha1.TableSpec{
							AttributeDefinitions: nil,
						},
					},
				},
				b: &resource{
					ko: &v1alpha1.Table{
						Spec: v1alpha1.TableSpec{
							AttributeDefinitions: []*v1alpha1.AttributeDefinition{
								{
									AttributeName: aws.String("externalId"),
									AttributeType: aws.String("S"),
								},
							},
						},
					},
				},
			},
			want: false,
		},
		{
			name: "desired and latest are equal",
			args: args{
				a: &resource{
					ko: &v1alpha1.Table{
						Spec: v1alpha1.TableSpec{
							AttributeDefinitions: []*v1alpha1.AttributeDefinition{
								{
									AttributeName: aws.String("externalId"),
									AttributeType: aws.String("S"),
								},
								{
									AttributeName: aws.String("id"),
									AttributeType: aws.String("S"),
								},
							},
						},
					},
				},
				b: &resource{
					ko: &v1alpha1.Table{
						Spec: v1alpha1.TableSpec{
							AttributeDefinitions: []*v1alpha1.AttributeDefinition{
								{
									AttributeName: aws.String("id"),
									AttributeType: aws.String("S"),
								},
								{
									AttributeName: aws.String("externalId"),
									AttributeType: aws.String("S"),
								},
							},
						},
					},
				},
			},
			want: true,
		},
		{
			name: "desired is updated",
			args: args{
				a: &resource{
					ko: &v1alpha1.Table{
						Spec: v1alpha1.TableSpec{
							AttributeDefinitions: []*v1alpha1.AttributeDefinition{
								{
									AttributeName: aws.String("externalId"),
									AttributeType: aws.String("N"),
								},
								{
									AttributeName: aws.String("id"),
									AttributeType: aws.String("S"),
								},
							},
						},
					},
				},
				b: &resource{
					ko: &v1alpha1.Table{
						Spec: v1alpha1.TableSpec{
							AttributeDefinitions: []*v1alpha1.AttributeDefinition{
								{
									AttributeName: aws.String("id"),
									AttributeType: aws.String("S"),
								},
								{
									AttributeName: aws.String("externalId"),
									AttributeType: aws.String("S"),
								},
							},
						},
					},
				},
			},
			want: false,
		},
		{
			name: "removed in desired",
			args: args{
				a: &resource{
					ko: &v1alpha1.Table{
						Spec: v1alpha1.TableSpec{
							AttributeDefinitions: []*v1alpha1.AttributeDefinition{
								{
									AttributeName: aws.String("id"),
									AttributeType: aws.String("S"),
								},
							},
						},
					},
				},
				b: &resource{
					ko: &v1alpha1.Table{
						Spec: v1alpha1.TableSpec{
							AttributeDefinitions: []*v1alpha1.AttributeDefinition{
								{
									AttributeName: aws.String("id"),
									AttributeType: aws.String("S"),
								},
								{
									AttributeName: aws.String("externalId"),
									AttributeType: aws.String("S"),
								},
							},
						},
					},
				},
			},
			want: false,
		},
		{
			name: "added in desired",
			args: args{
				a: &resource{
					ko: &v1alpha1.Table{
						Spec: v1alpha1.TableSpec{
							AttributeDefinitions: []*v1alpha1.AttributeDefinition{
								{
									AttributeName: aws.String("id"),
									AttributeType: aws.String("S"),
								},
								{
									AttributeName: aws.String("externalId"),
									AttributeType: aws.String("S"),
								},
							},
						},
					},
				},
				b: &resource{
					ko: &v1alpha1.Table{
						Spec: v1alpha1.TableSpec{
							AttributeDefinitions: []*v1alpha1.AttributeDefinition{
								{
									AttributeName: aws.String("id"),
									AttributeType: aws.String("S"),
								},
							},
						},
					},
				},
			},
			want: false,
		},
	}

	isEqual := func(delta *compare.Delta) bool {
		return !delta.DifferentAt("Spec.AttributeDefinitions")
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if delta := newResourceDelta(tt.args.a, tt.args.b); isEqual(delta) != tt.want {
				t.Errorf("Compare attribution defintions should be %v", tt.want)
			}
		})
	}
}
