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

package v1alpha1

import "fmt"

var (
	// TableProvisionedThroughputManagedByAnnotation is the annotation key used to set the management
	// style for the provisioned throughput of a DynamoDB table. This annotation can only be set on a
	// table custom resource.
	//
	// The value of this annotation must be one of the following:
	//
	// - 'external-autoscaler': The provisioned throughput is managed by an external entity. Causing
	// 							the controller to completly ignore the fields `readCapacityUnits`
	// 							and `writeCapacityUnits` of `provisionedThroughput` and not reconcile
	// 							the provisioned throughput of a table.
	//
	// - 'ack-dynamodb-controller':  The provisioned throughput is managed by the ACK controller.
	// 							Causing the controller to reconcile the provisioned throughput of the
	// 							table with the values of the `spec.provisionedThroughput` field
	// 							(`readCapacityUnits` and `writeCapacityUnits`).
	//
	// By default the provisioned throughput is managed by the controller. If the annotation is not set, or
	// the value is not one of the above, the controller will default to managing the provisioned throughput
	// as if the annotation was set to "controller".
	TableProvisionedThroughputManagedByAnnotation = fmt.Sprintf("%s/table-provisioned-throughput-managed-by", GroupVersion.Group)
)

const (
	// TableProvisionedThroughputManagedByExternalAutoscaler is the value of the
	// TableProvisionedThroughputManagedByAnnotation annotation that indicates that the provisioned
	// throughput of a table is managed by an external autoscaler.
	TableProvisionedThroughputManagedByExternalAutoscaler = "external-autoscaler"
	// TableProvisionedThroughputManagedByACKController is the value of the
	// TableProvisionedThroughputManagedByAnnotation annotation that indicates that the provisioned
	// throughput of a table is managed by the ACK controller.
	TableProvisionedThroughputManagedByACKController = "ack-dynamodb-controller"
)
