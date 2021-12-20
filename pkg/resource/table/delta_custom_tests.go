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
)

func Test_compareTags(t *testing.T) {
	type args struct {
		a map[string]*string
		b map[string]*string
	}
	tests := []struct {
		name        string
		args        args
		wantAdded   map[string]*string
		wantRemoved []string
		wantUpdated map[string]*string
	}{}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {})
	}
}

func Test_computeGlobalSecondaryIndexDelta(t *testing.T) {
	type args struct {
		a map[string]*string
		b map[string]*string
	}
	tests := []struct {
		name        string
		args        args
		wantAdded   map[string]*string
		wantRemoved []string
		wantUpdated map[string]*string
	}{}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {})
	}
}
