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
	"bytes"
	"context"
	"io"
	"net/http"
	"reflect"
	"testing"

	ackv1alpha1 "github.com/aws-controllers-k8s/runtime/apis/core/v1alpha1"
	"github.com/aws-controllers-k8s/runtime/pkg/compare"
	ackerr "github.com/aws-controllers-k8s/runtime/pkg/errors"
	ackmetrics "github.com/aws-controllers-k8s/runtime/pkg/metrics"
	"github.com/aws/aws-sdk-go-v2/aws"
	svcsdk "github.com/aws/aws-sdk-go-v2/service/dynamodb"
	"github.com/stretchr/testify/require"
	corev1 "k8s.io/api/core/v1"

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

const testTableARN = "arn:aws:dynamodb:us-west-2:000000000000:table/test-table"

// validationExceptionHTTPClient answers every request with the 400
// ValidationException that DynamoDB returns for an invalid parameter, so the
// update paths below fail with a real, fully deserialized smithy.APIError
// rather than a hand-rolled stand-in.
type validationExceptionHTTPClient struct{}

func (validationExceptionHTTPClient) Do(*http.Request) (*http.Response, error) {
	body := `{"__type":"com.amazon.coral.validate#ValidationException",` +
		`"message":"Invalid table-class parameter provided. Please try again with a ` +
		`valid table-class value: [STANDARD, STANDARD_INFREQUENT_ACCESS]."}`
	return &http.Response{
		StatusCode: http.StatusBadRequest,
		Status:     "400 Bad Request",
		Header: http.Header{
			"Content-Type":     []string{"application/x-amz-json-1.0"},
			"X-Amzn-Errortype": []string{"ValidationException"},
			"X-Amzn-Requestid": []string{"TESTREQUESTID"},
		},
		Body: io.NopCloser(bytes.NewReader([]byte(body))),
	}, nil
}

// fakeCredentialsProvider satisfies the signer without touching the
// environment, a config file, or IMDS. Declared locally rather than using
// aws-sdk-go-v2/credentials, which is only an indirect dependency of this
// module -- importing it would promote it to a direct require and make the
// committed go.mod disagree with `go mod tidy` in the code-gen check.
type fakeCredentialsProvider struct{}

func (fakeCredentialsProvider) Retrieve(context.Context) (aws.Credentials, error) {
	return aws.Credentials{
		AccessKeyID:     "AKIAFAKEFAKEFAKEFAKE",
		SecretAccessKey: "fake",
		Source:          "hooks_test",
	}, nil
}

// newFailingResourceManager returns a resourceManager whose DynamoDB client
// always fails with ValidationException. No real credentials are resolved and
// no network call is made; retries are disabled so each test drives exactly
// one request.
func newFailingResourceManager() *resourceManager {
	return &resourceManager{
		metrics: ackmetrics.NewMetrics("dynamodb"),
		sdkapi: svcsdk.NewFromConfig(aws.Config{
			Region:           "us-west-2",
			Credentials:      fakeCredentialsProvider{},
			HTTPClient:       validationExceptionHTTPClient{},
			RetryMaxAttempts: 1,
		}),
	}
}

// newTestTable builds a minimally valid ACTIVE table resource. Fields that the
// update paths dereference unconditionally (TableName, BillingMode) are always
// populated.
func newTestTable() *resource {
	return &resource{ko: &v1alpha1.Table{
		Spec: v1alpha1.TableSpec{
			TableName:   aws.String("test-table"),
			BillingMode: aws.String("PAY_PER_REQUEST"),
			TableClass:  aws.String("STANDARD"),
		},
		Status: v1alpha1.TableStatus{
			TableStatus: aws.String("ACTIVE"),
			ACKResourceMetadata: &ackv1alpha1.ResourceMetadata{
				ARN: (*ackv1alpha1.AWSResourceName)(aws.String(testTableARN)),
			},
		},
	}}
}

// Test_customUpdateTable_preservesTerminalAWSError asserts that every update
// path in customUpdateTable that wraps an AWS SDK error keeps that error
// reachable through the error chain, so terminalAWSError can still recognize
// the ValidationException listed under generator.yaml's terminal_codes.
//
// Wrapping with %v instead of %w flattens the AWS error to a string and
// defeats the errors.As lookup, which is what caused
// https://github.com/aws-controllers-k8s/community/issues/3006 -- the
// controller reported ACK.Recoverable and retried an invalid update forever.
//
// Each case names the hooks.go wrap site it covers.
func Test_customUpdateTable_preservesTerminalAWSError(t *testing.T) {
	tests := []struct {
		name    string
		setup   func(desired *resource)
		deltaAt string
	}{
		{
			// syncTable -> "cannot update table %w"
			name:    "TableClass update wraps UpdateTable error",
			deltaAt: "Spec.TableClass",
			setup: func(desired *resource) {
				desired.ko.Spec.TableClass = aws.String("NONEXISTENT_CLASS")
			},
		},
		{
			// customUpdateTable -> "cannot update table %w"
			name:    "SSESpecification update wraps UpdateTable error",
			deltaAt: "Spec.SSESpecification",
			setup: func(desired *resource) {
				desired.ko.Spec.SSESpecification = &v1alpha1.SSESpecification{
					Enabled: aws.Bool(true),
					SSEType: aws.String("KMS"),
				}
			},
		},
		{
			// customUpdateTable -> "cannot update table %w"
			name:    "ContinuousBackups update wraps UpdateContinuousBackups error",
			deltaAt: "Spec.ContinuousBackups",
			setup: func(desired *resource) {
				desired.ko.Spec.ContinuousBackups = &v1alpha1.PointInTimeRecoverySpecification{
					PointInTimeRecoveryEnabled: aws.Bool(true),
				}
			},
		},
		{
			// customUpdateTable -> "cannot update table resource policy %w"
			name:    "ResourcePolicy update wraps PutResourcePolicy error",
			deltaAt: "Spec.ResourcePolicy",
			setup: func(desired *resource) {
				desired.ko.Spec.ResourcePolicy = aws.String(`{"Version":"2012-10-17","Statement":[]}`)
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			rm := newFailingResourceManager()
			desired, latest := newTestTable(), newTestTable()
			tt.setup(desired)

			delta := compare.NewDelta()
			delta.Add(tt.deltaAt, desired, latest)

			_, err := rm.customUpdateTable(context.Background(), desired, latest, delta)
			require.Error(t, err)
			require.True(t, rm.terminalAWSError(err),
				"ValidationException must stay reachable through the wrap for %s; "+
					"wrap the AWS error with %%w, not %%v. got: %v", tt.deltaAt, err)
		})
	}
}

// Test_customUpdateTable_setsTerminalCondition asserts the user-visible
// outcome reported in community#3006: an invalid tableClass update must settle
// on ACK.Terminal, and onError must return ackerr.Terminal so the runtime
// stops requeuing instead of retrying the doomed update forever.
func Test_customUpdateTable_setsTerminalCondition(t *testing.T) {
	rm := newFailingResourceManager()
	desired, latest := newTestTable(), newTestTable()
	desired.ko.Spec.TableClass = aws.String("NONEXISTENT_CLASS")

	delta := compare.NewDelta()
	delta.Add("Spec.TableClass", desired, latest)

	_, err := rm.customUpdateTable(context.Background(), desired, latest, delta)
	require.Error(t, err)

	updated, onErr := rm.onError(latest, err)
	require.Equal(t, ackerr.Terminal, onErr,
		"a terminal AWS error must short-circuit the requeue loop")

	var terminal, recoverable *ackv1alpha1.Condition
	for _, c := range updated.Conditions() {
		switch c.Type {
		case ackv1alpha1.ConditionTypeTerminal:
			terminal = c
		case ackv1alpha1.ConditionTypeRecoverable:
			recoverable = c
		}
	}

	require.NotNil(t, terminal, "ACK.Terminal condition must be set")
	require.Equal(t, corev1.ConditionTrue, terminal.Status)
	require.NotNil(t, terminal.Message)
	require.Contains(t, *terminal.Message, "ValidationException")
	require.Nil(t, recoverable, "ACK.Recoverable must not be set for a terminal error")
}

// okHTTPClient answers every request with an empty successful JSON body, which
// is enough for the SDK to deserialize an UpdateTable response.
type okHTTPClient struct{}

func (okHTTPClient) Do(*http.Request) (*http.Response, error) {
	return &http.Response{
		StatusCode: http.StatusOK,
		Status:     "200 OK",
		Header: http.Header{
			"Content-Type":     []string{"application/x-amz-json-1.0"},
			"X-Amzn-Requestid": []string{"TESTREQUESTID"},
		},
		Body: io.NopCloser(bytes.NewReader([]byte(`{}`))),
	}, nil
}

// Test_deleteGSIs_requeuesAfterSingleDelete guards the update ordering in
// customUpdateTable. deleteGSIs used to return nil once it had issued the last
// queued deletion, which let customUpdateTable continue into syncTable and
// issue a second UpdateTable while the table was still UPDATING and the removed
// index still present. AWS rejects that with
//
//	ValidationException: ... ProvisionedThroughput must be specified for index: <name>
//
// which is a terminal code for this resource, so the table would stop
// reconciling instead of retrying. Every successful deletion must requeue.
//
// See https://github.com/aws-controllers-k8s/community/issues/3006
func Test_deleteGSIs_requeuesAfterSingleDelete(t *testing.T) {
	rm := &resourceManager{
		metrics: ackmetrics.NewMetrics("dynamodb"),
		sdkapi: svcsdk.NewFromConfig(aws.Config{
			Region:           "us-west-2",
			Credentials:      fakeCredentialsProvider{},
			HTTPClient:       okHTTPClient{},
			RetryMaxAttempts: 1,
		}),
	}

	activeGSI := func(name string) *v1alpha1.GlobalSecondaryIndexDescription {
		return &v1alpha1.GlobalSecondaryIndexDescription{
			IndexName:   aws.String(name),
			IndexStatus: aws.String("ACTIVE"),
		}
	}

	desired, latest := newTestTable(), newTestTable()
	latest.ko.Status.GlobalSecondaryIndexesDescriptions =
		[]*v1alpha1.GlobalSecondaryIndexDescription{activeGSI("GSI1"), activeGSI("GSI2")}

	// A single removed index: the case that previously returned nil.
	err := rm.deleteGSIs(context.Background(), desired, latest, []string{"GSI2"})
	require.Equal(t, requeueWaitGSIReady, err,
		"a successful GSI deletion must requeue so the table settles before "+
			"customUpdateTable attempts any further table property update")
}
