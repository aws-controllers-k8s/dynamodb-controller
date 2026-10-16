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
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"testing"

	"github.com/aws-controllers-k8s/runtime/pkg/compare"
	ackmetrics "github.com/aws-controllers-k8s/runtime/pkg/metrics"
	"github.com/aws/aws-sdk-go-v2/aws"
	svcsdk "github.com/aws/aws-sdk-go-v2/service/dynamodb"
	"github.com/aws/smithy-go"
	"github.com/stretchr/testify/require"

	"github.com/aws-controllers-k8s/dynamodb-controller/apis/v1alpha1"
)

func ttl(name *string, enabled *bool) *v1alpha1.TimeToLiveSpecification {
	return &v1alpha1.TimeToLiveSpecification{AttributeName: name, Enabled: enabled}
}

func Test_newResourceDelta_TimeToLive(t *testing.T) {
	tests := []struct {
		name     string
		desired  *v1alpha1.TimeToLiveSpecification
		latest   *v1alpha1.TimeToLiveSpecification
		wantDiff bool
	}{
		{"disabled with name vs disabled without name", ttl(aws.String("expireAt"), aws.Bool(false)), ttl(nil, aws.Bool(false)), false},
		{"disabled with nil enabled vs disabled", ttl(aws.String("expireAt"), nil), ttl(nil, aws.Bool(false)), false},
		{"nil vs disabled", nil, ttl(nil, aws.Bool(false)), false},
		{"nil vs nil", nil, nil, false},
		{"nil vs enabled", nil, ttl(aws.String("expireAt"), aws.Bool(true)), true},
		{"enabled vs disabled", ttl(aws.String("expireAt"), aws.Bool(true)), ttl(nil, aws.Bool(false)), true},
		{"disabled vs enabled", ttl(aws.String("expireAt"), aws.Bool(false)), ttl(aws.String("expireAt"), aws.Bool(true)), true},
		{"enabled same name", ttl(aws.String("expireAt"), aws.Bool(true)), ttl(aws.String("expireAt"), aws.Bool(true)), false},
		{"enabled different name", ttl(aws.String("expireAt"), aws.Bool(true)), ttl(aws.String("ttl"), aws.Bool(true)), true},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			desired, latest := newTestTable(), newTestTable()
			desired.ko.Spec.TimeToLive = tt.desired
			latest.ko.Spec.TimeToLive = tt.latest
			desiredBefore := desired.ko.Spec.TimeToLive.DeepCopy()

			delta := newResourceDelta(desired, latest)
			require.Equal(t, tt.wantDiff, delta.DifferentAt("Spec.TimeToLive"))
			require.Equal(t, desiredBefore, desired.ko.Spec.TimeToLive)
		})
	}
}

func Test_syncTTL_noopWhenBothDisabled(t *testing.T) {
	rm := newFailingResourceManager()
	desired, latest := newTestTable(), newTestTable()
	desired.ko.Spec.TimeToLive = ttl(aws.String("expireAt"), aws.Bool(false))
	latest.ko.Spec.TimeToLive = ttl(nil, aws.Bool(false))
	require.NoError(t, rm.syncTTL(context.Background(), desired, latest))

	desired.ko.Spec.TimeToLive = nil
	require.NoError(t, rm.syncTTL(context.Background(), desired, latest))

	desired.ko.Spec.TimeToLive = ttl(aws.String("expireAt"), aws.Bool(true))
	require.Error(t, rm.syncTTL(context.Background(), desired, latest))
}

type recordingHTTPClient struct {
	bodies *[]map[string]any
}

func (c recordingHTTPClient) Do(req *http.Request) (*http.Response, error) {
	var body map[string]any
	if err := json.NewDecoder(req.Body).Decode(&body); err != nil {
		return nil, err
	}
	*c.bodies = append(*c.bodies, body)
	return &http.Response{
		StatusCode: http.StatusOK,
		Status:     "200 OK",
		Header:     http.Header{"Content-Type": []string{"application/x-amz-json-1.0"}},
		Body:       io.NopCloser(bytes.NewReader([]byte(`{}`))),
	}, nil
}

func Test_syncTTL_removedSpecDisablesWithCurrentAttributeName(t *testing.T) {
	var bodies []map[string]any
	rm := &resourceManager{
		metrics: ackmetrics.NewMetrics("dynamodb"),
		sdkapi: svcsdk.NewFromConfig(aws.Config{
			Region:           "us-west-2",
			Credentials:      fakeCredentialsProvider{},
			HTTPClient:       recordingHTTPClient{bodies: &bodies},
			RetryMaxAttempts: 1,
		}),
	}
	desired, latest := newTestTable(), newTestTable()
	desired.ko.Spec.TimeToLive = nil
	latest.ko.Spec.TimeToLive = ttl(aws.String("expiration"), aws.Bool(true))

	delta := newResourceDelta(desired, latest)
	require.True(t, delta.DifferentAt("Spec.TimeToLive"))
	require.Nil(t, desired.ko.Spec.TimeToLive)

	require.NoError(t, rm.syncTTL(context.Background(), desired, latest))
	require.Len(t, bodies, 1)
	require.Equal(t, map[string]any{
		"AttributeName": "expiration",
		"Enabled":       false,
	}, bodies[0]["TimeToLiveSpecification"])
}

func Test_customUpdateTable_propagatesTTLErrors(t *testing.T) {
	rm := newFailingResourceManager()
	desired, latest := newTestTable(), newTestTable()
	desired.ko.Spec.TimeToLive = ttl(aws.String("expireAt"), aws.Bool(true))
	latest.ko.Spec.TimeToLive = ttl(nil, aws.Bool(false))

	delta := compare.NewDelta()
	delta.Add("Spec.TimeToLive", desired.ko.Spec.TimeToLive, latest.ko.Spec.TimeToLive)

	_, err := rm.customUpdateTable(context.Background(), desired, latest, delta)
	require.Error(t, err)
}

func Test_isTTLAlreadyDisabledError(t *testing.T) {
	require.True(t, isTTLAlreadyDisabledError(&smithy.GenericAPIError{
		Code:    "ValidationException",
		Message: "TimeToLive is already disabled",
	}))
	require.False(t, isTTLAlreadyDisabledError(&smithy.GenericAPIError{
		Code:    "ValidationException",
		Message: "TimeToLive is active with a different AttributeName",
	}))
	require.False(t, isTTLAlreadyDisabledError(errors.New("TimeToLive is already disabled")))
}
