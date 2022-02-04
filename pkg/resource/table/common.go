package table

import (
	"encoding/json"
	"fmt"
)

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

// TODO(hilalymh) Move these functions to aws-controllers-k8s/runtime

func emptyString(s *string) bool {
	if s == nil {
		return true
	}
	return *s == ""
}

func equalInt64(a, b *int64) bool {
	if a == nil {
		return b == nil || *b == 0
	}
	return (*a == 0 && b == nil) || *a == *b
}

func debug(i interface{}) {
	bytes, _ := json.MarshalIndent(i, "", "    ")
	fmt.Println(string(bytes))
}

func debugAll(is ...interface{}) {
	for _, i := range is {
		debug(i)
		debug("----")
	}
}
