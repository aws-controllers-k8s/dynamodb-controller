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

// Code generated by ack-generate. DO NOT EDIT.

package v1alpha1

import (
	ackv1alpha1 "github.com/aws-controllers-k8s/runtime/apis/core/v1alpha1"
	"github.com/aws/aws-sdk-go/aws"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

// Hack to avoid import errors during build...
var (
	_ = &metav1.Time{}
	_ = &aws.JSONValue{}
	_ = ackv1alpha1.AWSAccountID("")
)

// Contains details of a table archival operation.
type ArchivalSummary struct {
	ArchivalBackupARN *string      `json:"archivalBackupARN,omitempty"`
	ArchivalDateTime  *metav1.Time `json:"archivalDateTime,omitempty"`
	ArchivalReason    *string      `json:"archivalReason,omitempty"`
}

// Represents an attribute for describing the schema for the table and indexes.
type AttributeDefinition struct {
	AttributeName *string `json:"attributeName,omitempty"`
	AttributeType *string `json:"attributeType,omitempty"`
}

// Represents the auto scaling settings for a global table or global secondary
// index.
type AutoScalingSettingsDescription struct {
	AutoScalingDisabled *bool   `json:"autoScalingDisabled,omitempty"`
	AutoScalingRoleARN  *string `json:"autoScalingRoleARN,omitempty"`
	MaximumUnits        *int64  `json:"maximumUnits,omitempty"`
	MinimumUnits        *int64  `json:"minimumUnits,omitempty"`
}

// Represents the auto scaling settings to be modified for a global table or
// global secondary index.
type AutoScalingSettingsUpdate struct {
	AutoScalingDisabled *bool  `json:"autoScalingDisabled,omitempty"`
	MaximumUnits        *int64 `json:"maximumUnits,omitempty"`
	MinimumUnits        *int64 `json:"minimumUnits,omitempty"`
}

// Represents the properties of a target tracking scaling policy.
type AutoScalingTargetTrackingScalingPolicyConfigurationDescription struct {
	DisableScaleIn *bool `json:"disableScaleIn,omitempty"`
}

// Represents the settings of a target tracking scaling policy that will be
// modified.
type AutoScalingTargetTrackingScalingPolicyConfigurationUpdate struct {
	DisableScaleIn *bool `json:"disableScaleIn,omitempty"`
}

// Contains the description of the backup created for the table.
type BackupDescription struct {
	// Contains the details of the backup created for the table.
	BackupDetails *BackupDetails `json:"backupDetails,omitempty"`
	// Contains the details of the table when the backup was created.
	SourceTableDetails *SourceTableDetails `json:"sourceTableDetails,omitempty"`
	// Contains the details of the features enabled on the table when the backup
	// was created. For example, LSIs, GSIs, streams, TTL.
	SourceTableFeatureDetails *SourceTableFeatureDetails `json:"sourceTableFeatureDetails,omitempty"`
}

// Contains the details of the backup created for the table.
type BackupDetails struct {
	BackupARN              *string      `json:"backupARN,omitempty"`
	BackupCreationDateTime *metav1.Time `json:"backupCreationDateTime,omitempty"`
	BackupExpiryDateTime   *metav1.Time `json:"backupExpiryDateTime,omitempty"`
	BackupName             *string      `json:"backupName,omitempty"`
	BackupSizeBytes        *int64       `json:"backupSizeBytes,omitempty"`
	BackupStatus           *string      `json:"backupStatus,omitempty"`
	BackupType             *string      `json:"backupType,omitempty"`
}

// Contains details for the backup.
type BackupSummary struct {
	BackupARN              *string      `json:"backupARN,omitempty"`
	BackupCreationDateTime *metav1.Time `json:"backupCreationDateTime,omitempty"`
	BackupExpiryDateTime   *metav1.Time `json:"backupExpiryDateTime,omitempty"`
	BackupName             *string      `json:"backupName,omitempty"`
	BackupSizeBytes        *int64       `json:"backupSizeBytes,omitempty"`
	BackupStatus           *string      `json:"backupStatus,omitempty"`
	BackupType             *string      `json:"backupType,omitempty"`
	TableARN               *string      `json:"tableARN,omitempty"`
	TableID                *string      `json:"tableID,omitempty"`
	TableName              *string      `json:"tableName,omitempty"`
}

// An error associated with a statement in a PartiQL batch that was run.
type BatchStatementError struct {
	Message *string `json:"message,omitempty"`
}

// A PartiQL batch statement response..
type BatchStatementResponse struct {
	TableName *string `json:"tableName,omitempty"`
}

// Contains the details for the read/write capacity mode. This page talks about
// PROVISIONED and PAY_PER_REQUEST billing modes. For more information about
// these modes, see Read/write capacity mode (https://docs.aws.amazon.com/amazondynamodb/latest/developerguide/HowItWorks.ReadWriteCapacityMode.html).
//
// You may need to switch to on-demand mode at least once in order to return
// a BillingModeSummary response.
type BillingModeSummary struct {
	BillingMode                       *string      `json:"billingMode,omitempty"`
	LastUpdateToPayPerRequestDateTime *metav1.Time `json:"lastUpdateToPayPerRequestDateTime,omitempty"`
}

// Represents a request to perform a check that an item exists or to check the
// condition of specific attributes of the item.
type ConditionCheck struct {
	TableName *string `json:"tableName,omitempty"`
}

// The capacity units consumed by an operation. The data returned includes the
// total provisioned throughput consumed, along with statistics for the table
// and any indexes involved in the operation. ConsumedCapacity is only returned
// if the request asked for it. For more information, see Provisioned capacity
// mode (https://docs.aws.amazon.com/amazondynamodb/latest/developerguide/provisioned-capacity-mode.html)
// in the Amazon DynamoDB Developer Guide.
type ConsumedCapacity struct {
	TableName *string `json:"tableName,omitempty"`
}

// Represents a Contributor Insights summary entry.
type ContributorInsightsSummary struct {
	IndexName *string `json:"indexName,omitempty"`
	TableName *string `json:"tableName,omitempty"`
}

// Represents a new global secondary index to be added to an existing table.
type CreateGlobalSecondaryIndexAction struct {
	IndexName *string             `json:"indexName,omitempty"`
	KeySchema []*KeySchemaElement `json:"keySchema,omitempty"`
	// Represents attributes that are copied (projected) from the table into an
	// index. These are in addition to the primary key attributes and index key
	// attributes, which are automatically projected.
	Projection *Projection `json:"projection,omitempty"`
	// Represents the provisioned throughput settings for a specified table or index.
	// The settings can be modified using the UpdateTable operation.
	//
	// For current minimum and maximum provisioned throughput values, see Service,
	// Account, and Table Quotas (https://docs.aws.amazon.com/amazondynamodb/latest/developerguide/Limits.html)
	// in the Amazon DynamoDB Developer Guide.
	ProvisionedThroughput *ProvisionedThroughput `json:"provisionedThroughput,omitempty"`
}

// Represents a replica to be added.
type CreateReplicaAction struct {
	RegionName *string `json:"regionName,omitempty"`
}

// Represents a replica to be created.
type CreateReplicationGroupMemberAction struct {
	GlobalSecondaryIndexes []*ReplicaGlobalSecondaryIndex `json:"globalSecondaryIndexes,omitempty"`
	KMSMasterKeyID         *string                        `json:"kmsMasterKeyID,omitempty"`
	// Replica-specific provisioned throughput settings. If not specified, uses
	// the source table's provisioned throughput settings.
	ProvisionedThroughputOverride *ProvisionedThroughputOverride `json:"provisionedThroughputOverride,omitempty"`
	RegionName                    *string                        `json:"regionName,omitempty"`
	TableClassOverride            *string                        `json:"tableClassOverride,omitempty"`
}

// Represents a request to perform a DeleteItem operation.
type Delete struct {
	TableName *string `json:"tableName,omitempty"`
}

// Represents a global secondary index to be deleted from an existing table.
type DeleteGlobalSecondaryIndexAction struct {
	IndexName *string `json:"indexName,omitempty"`
}

// Represents a replica to be removed.
type DeleteReplicaAction struct {
	RegionName *string `json:"regionName,omitempty"`
}

// Represents a replica to be deleted.
type DeleteReplicationGroupMemberAction struct {
	RegionName *string `json:"regionName,omitempty"`
}

// An endpoint information details.
type Endpoint struct {
	Address *string `json:"address,omitempty"`
}

// Represents a condition to be compared with an attribute value. This condition
// can be used with DeleteItem, PutItem, or UpdateItem operations; if the comparison
// evaluates to true, the operation succeeds; if not, the operation fails. You
// can use ExpectedAttributeValue in one of two different ways:
//
//   - Use AttributeValueList to specify one or more values to compare against
//     an attribute. Use ComparisonOperator to specify how you want to perform
//     the comparison. If the comparison evaluates to true, then the conditional
//     operation succeeds.
//
//   - Use Value to specify a value that DynamoDB will compare against an attribute.
//     If the values match, then ExpectedAttributeValue evaluates to true and
//     the conditional operation succeeds. Optionally, you can also set Exists
//     to false, indicating that you do not expect to find the attribute value
//     in the table. In this case, the conditional operation succeeds only if
//     the comparison evaluates to false.
//
// Value and Exists are incompatible with AttributeValueList and ComparisonOperator.
// Note that if you use both sets of parameters at once, DynamoDB will return
// a ValidationException exception.
type ExpectedAttributeValue struct {
	Exists *bool `json:"exists,omitempty"`
}

// Represents the properties of the exported table.
type ExportDescription struct {
	ItemCount *int64  `json:"itemCount,omitempty"`
	TableARN  *string `json:"tableARN,omitempty"`
	TableID   *string `json:"tableID,omitempty"`
}

// Specifies an item and related attribute values to retrieve in a TransactGetItem
// object.
type Get struct {
	TableName *string `json:"tableName,omitempty"`
}

// Represents the properties of a global secondary index.
type GlobalSecondaryIndex struct {
	IndexName *string             `json:"indexName,omitempty"`
	KeySchema []*KeySchemaElement `json:"keySchema,omitempty"`
	// Represents attributes that are copied (projected) from the table into an
	// index. These are in addition to the primary key attributes and index key
	// attributes, which are automatically projected.
	Projection *Projection `json:"projection,omitempty"`
	// Represents the provisioned throughput settings for a specified table or index.
	// The settings can be modified using the UpdateTable operation.
	//
	// For current minimum and maximum provisioned throughput values, see Service,
	// Account, and Table Quotas (https://docs.aws.amazon.com/amazondynamodb/latest/developerguide/Limits.html)
	// in the Amazon DynamoDB Developer Guide.
	ProvisionedThroughput *ProvisionedThroughput `json:"provisionedThroughput,omitempty"`
}

// Represents the auto scaling settings of a global secondary index for a global
// table that will be modified.
type GlobalSecondaryIndexAutoScalingUpdate struct {
	IndexName *string `json:"indexName,omitempty"`
}

// Represents the properties of a global secondary index.
type GlobalSecondaryIndexDescription struct {
	Backfilling    *bool               `json:"backfilling,omitempty"`
	IndexARN       *string             `json:"indexARN,omitempty"`
	IndexName      *string             `json:"indexName,omitempty"`
	IndexSizeBytes *int64              `json:"indexSizeBytes,omitempty"`
	IndexStatus    *string             `json:"indexStatus,omitempty"`
	ItemCount      *int64              `json:"itemCount,omitempty"`
	KeySchema      []*KeySchemaElement `json:"keySchema,omitempty"`
	// Represents attributes that are copied (projected) from the table into an
	// index. These are in addition to the primary key attributes and index key
	// attributes, which are automatically projected.
	Projection *Projection `json:"projection,omitempty"`
	// Represents the provisioned throughput settings for the table, consisting
	// of read and write capacity units, along with data about increases and decreases.
	ProvisionedThroughput *ProvisionedThroughputDescription `json:"provisionedThroughput,omitempty"`
}

// Represents the properties of a global secondary index for the table when
// the backup was created.
type GlobalSecondaryIndexInfo struct {
	IndexName *string             `json:"indexName,omitempty"`
	KeySchema []*KeySchemaElement `json:"keySchema,omitempty"`
	// Represents attributes that are copied (projected) from the table into an
	// index. These are in addition to the primary key attributes and index key
	// attributes, which are automatically projected.
	Projection *Projection `json:"projection,omitempty"`
	// Represents the provisioned throughput settings for a specified table or index.
	// The settings can be modified using the UpdateTable operation.
	//
	// For current minimum and maximum provisioned throughput values, see Service,
	// Account, and Table Quotas (https://docs.aws.amazon.com/amazondynamodb/latest/developerguide/Limits.html)
	// in the Amazon DynamoDB Developer Guide.
	ProvisionedThroughput *ProvisionedThroughput `json:"provisionedThroughput,omitempty"`
}

// Represents one of the following:
//
//   - A new global secondary index to be added to an existing table.
//
//   - New provisioned throughput parameters for an existing global secondary
//     index.
//
//   - An existing global secondary index to be removed from an existing table.
type GlobalSecondaryIndexUpdate struct {
	// Represents a new global secondary index to be added to an existing table.
	Create *CreateGlobalSecondaryIndexAction `json:"create,omitempty"`
	// Represents a global secondary index to be deleted from an existing table.
	Delete *DeleteGlobalSecondaryIndexAction `json:"delete,omitempty"`
	// Represents the new provisioned throughput settings to be applied to a global
	// secondary index.
	Update *UpdateGlobalSecondaryIndexAction `json:"update,omitempty"`
}

// Contains details about the global table.
type GlobalTableDescription struct {
	CreationDateTime  *metav1.Time          `json:"creationDateTime,omitempty"`
	GlobalTableARN    *string               `json:"globalTableARN,omitempty"`
	GlobalTableName   *string               `json:"globalTableName,omitempty"`
	GlobalTableStatus *string               `json:"globalTableStatus,omitempty"`
	ReplicationGroup  []*ReplicaDescription `json:"replicationGroup,omitempty"`
}

// Represents the settings of a global secondary index for a global table that
// will be modified.
type GlobalTableGlobalSecondaryIndexSettingsUpdate struct {
	IndexName                     *string `json:"indexName,omitempty"`
	ProvisionedWriteCapacityUnits *int64  `json:"provisionedWriteCapacityUnits,omitempty"`
}

// Represents the properties of a global table.
type GlobalTable_SDK struct {
	GlobalTableName  *string    `json:"globalTableName,omitempty"`
	ReplicationGroup []*Replica `json:"replicationGroup,omitempty"`
}

// Summary information about the source file for the import.
type ImportSummary struct {
	TableARN *string `json:"tableARN,omitempty"`
}

// Represents the properties of the table being imported into.
type ImportTableDescription struct {
	ProcessedSizeBytes *int64  `json:"processedSizeBytes,omitempty"`
	TableARN           *string `json:"tableARN,omitempty"`
	TableID            *string `json:"tableID,omitempty"`
}

// Represents a single element of a key schema. A key schema specifies the attributes
// that make up the primary key of a table, or the key attributes of an index.
//
// A KeySchemaElement represents exactly one attribute of the primary key. For
// example, a simple primary key would be represented by one KeySchemaElement
// (for the partition key). A composite primary key would require one KeySchemaElement
// for the partition key, and another KeySchemaElement for the sort key.
//
// A KeySchemaElement must be a scalar, top-level attribute (not a nested attribute).
// The data type must be one of String, Number, or Binary. The attribute cannot
// be nested within a List or a Map.
type KeySchemaElement struct {
	AttributeName *string `json:"attributeName,omitempty"`
	KeyType       *string `json:"keyType,omitempty"`
}

// Describes a Kinesis data stream destination.
type KinesisDataStreamDestination struct {
	DestinationStatusDescription *string `json:"destinationStatusDescription,omitempty"`
	StreamARN                    *string `json:"streamARN,omitempty"`
}

// Represents the properties of a local secondary index.
type LocalSecondaryIndex struct {
	IndexName *string             `json:"indexName,omitempty"`
	KeySchema []*KeySchemaElement `json:"keySchema,omitempty"`
	// Represents attributes that are copied (projected) from the table into an
	// index. These are in addition to the primary key attributes and index key
	// attributes, which are automatically projected.
	Projection *Projection `json:"projection,omitempty"`
}

// Represents the properties of a local secondary index.
type LocalSecondaryIndexDescription struct {
	IndexARN       *string             `json:"indexARN,omitempty"`
	IndexName      *string             `json:"indexName,omitempty"`
	IndexSizeBytes *int64              `json:"indexSizeBytes,omitempty"`
	ItemCount      *int64              `json:"itemCount,omitempty"`
	KeySchema      []*KeySchemaElement `json:"keySchema,omitempty"`
	// Represents attributes that are copied (projected) from the table into an
	// index. These are in addition to the primary key attributes and index key
	// attributes, which are automatically projected.
	Projection *Projection `json:"projection,omitempty"`
}

// Represents the properties of a local secondary index for the table when the
// backup was created.
type LocalSecondaryIndexInfo struct {
	IndexName *string             `json:"indexName,omitempty"`
	KeySchema []*KeySchemaElement `json:"keySchema,omitempty"`
	// Represents attributes that are copied (projected) from the table into an
	// index. These are in addition to the primary key attributes and index key
	// attributes, which are automatically projected.
	Projection *Projection `json:"projection,omitempty"`
}

// The description of the point in time settings applied to the table.
type PointInTimeRecoveryDescription struct {
	EarliestRestorableDateTime *metav1.Time `json:"earliestRestorableDateTime,omitempty"`
	LatestRestorableDateTime   *metav1.Time `json:"latestRestorableDateTime,omitempty"`
}

// Represents the settings used to enable point in time recovery.
type PointInTimeRecoverySpecification struct {
	PointInTimeRecoveryEnabled *bool `json:"pointInTimeRecoveryEnabled,omitempty"`
}

// Represents attributes that are copied (projected) from the table into an
// index. These are in addition to the primary key attributes and index key
// attributes, which are automatically projected.
type Projection struct {
	NonKeyAttributes []*string `json:"nonKeyAttributes,omitempty"`
	ProjectionType   *string   `json:"projectionType,omitempty"`
}

// Represents the provisioned throughput settings for a specified table or index.
// The settings can be modified using the UpdateTable operation.
//
// For current minimum and maximum provisioned throughput values, see Service,
// Account, and Table Quotas (https://docs.aws.amazon.com/amazondynamodb/latest/developerguide/Limits.html)
// in the Amazon DynamoDB Developer Guide.
type ProvisionedThroughput struct {
	ReadCapacityUnits  *int64 `json:"readCapacityUnits,omitempty"`
	WriteCapacityUnits *int64 `json:"writeCapacityUnits,omitempty"`
}

// Represents the provisioned throughput settings for the table, consisting
// of read and write capacity units, along with data about increases and decreases.
type ProvisionedThroughputDescription struct {
	LastDecreaseDateTime   *metav1.Time `json:"lastDecreaseDateTime,omitempty"`
	LastIncreaseDateTime   *metav1.Time `json:"lastIncreaseDateTime,omitempty"`
	NumberOfDecreasesToday *int64       `json:"numberOfDecreasesToday,omitempty"`
	ReadCapacityUnits      *int64       `json:"readCapacityUnits,omitempty"`
	WriteCapacityUnits     *int64       `json:"writeCapacityUnits,omitempty"`
}

// Replica-specific provisioned throughput settings. If not specified, uses
// the source table's provisioned throughput settings.
type ProvisionedThroughputOverride struct {
	ReadCapacityUnits *int64 `json:"readCapacityUnits,omitempty"`
}

// Represents a request to perform a PutItem operation.
type Put struct {
	TableName *string `json:"tableName,omitempty"`
}

// Represents the properties of a replica.
type Replica struct {
	RegionName *string `json:"regionName,omitempty"`
}

// Represents the auto scaling settings of the replica.
type ReplicaAutoScalingDescription struct {
	RegionName    *string `json:"regionName,omitempty"`
	ReplicaStatus *string `json:"replicaStatus,omitempty"`
}

// Represents the auto scaling settings of a replica that will be modified.
type ReplicaAutoScalingUpdate struct {
	RegionName *string `json:"regionName,omitempty"`
}

// Contains the details of the replica.
type ReplicaDescription struct {
	GlobalSecondaryIndexes []*ReplicaGlobalSecondaryIndexDescription `json:"globalSecondaryIndexes,omitempty"`
	KMSMasterKeyID         *string                                   `json:"kmsMasterKeyID,omitempty"`
	// Replica-specific provisioned throughput settings. If not specified, uses
	// the source table's provisioned throughput settings.
	ProvisionedThroughputOverride *ProvisionedThroughputOverride `json:"provisionedThroughputOverride,omitempty"`
	RegionName                    *string                        `json:"regionName,omitempty"`
	ReplicaInaccessibleDateTime   *metav1.Time                   `json:"replicaInaccessibleDateTime,omitempty"`
	ReplicaStatus                 *string                        `json:"replicaStatus,omitempty"`
	ReplicaStatusDescription      *string                        `json:"replicaStatusDescription,omitempty"`
	ReplicaStatusPercentProgress  *string                        `json:"replicaStatusPercentProgress,omitempty"`
	// Contains details of the table class.
	ReplicaTableClassSummary *TableClassSummary `json:"replicaTableClassSummary,omitempty"`
}

// Represents the properties of a replica global secondary index.
type ReplicaGlobalSecondaryIndex struct {
	IndexName *string `json:"indexName,omitempty"`
	// Replica-specific provisioned throughput settings. If not specified, uses
	// the source table's provisioned throughput settings.
	ProvisionedThroughputOverride *ProvisionedThroughputOverride `json:"provisionedThroughputOverride,omitempty"`
}

// Represents the auto scaling configuration for a replica global secondary
// index.
type ReplicaGlobalSecondaryIndexAutoScalingDescription struct {
	IndexName   *string `json:"indexName,omitempty"`
	IndexStatus *string `json:"indexStatus,omitempty"`
}

// Represents the auto scaling settings of a global secondary index for a replica
// that will be modified.
type ReplicaGlobalSecondaryIndexAutoScalingUpdate struct {
	IndexName *string `json:"indexName,omitempty"`
}

// Represents the properties of a replica global secondary index.
type ReplicaGlobalSecondaryIndexDescription struct {
	IndexName *string `json:"indexName,omitempty"`
	// Replica-specific provisioned throughput settings. If not specified, uses
	// the source table's provisioned throughput settings.
	ProvisionedThroughputOverride *ProvisionedThroughputOverride `json:"provisionedThroughputOverride,omitempty"`
}

// Represents the properties of a global secondary index.
type ReplicaGlobalSecondaryIndexSettingsDescription struct {
	IndexName                     *string `json:"indexName,omitempty"`
	IndexStatus                   *string `json:"indexStatus,omitempty"`
	ProvisionedReadCapacityUnits  *int64  `json:"provisionedReadCapacityUnits,omitempty"`
	ProvisionedWriteCapacityUnits *int64  `json:"provisionedWriteCapacityUnits,omitempty"`
}

// Represents the settings of a global secondary index for a global table that
// will be modified.
type ReplicaGlobalSecondaryIndexSettingsUpdate struct {
	IndexName                    *string `json:"indexName,omitempty"`
	ProvisionedReadCapacityUnits *int64  `json:"provisionedReadCapacityUnits,omitempty"`
}

// Represents the properties of a replica.
type ReplicaSettingsDescription struct {
	RegionName                           *string `json:"regionName,omitempty"`
	ReplicaProvisionedReadCapacityUnits  *int64  `json:"replicaProvisionedReadCapacityUnits,omitempty"`
	ReplicaProvisionedWriteCapacityUnits *int64  `json:"replicaProvisionedWriteCapacityUnits,omitempty"`
	ReplicaStatus                        *string `json:"replicaStatus,omitempty"`
	// Contains details of the table class.
	ReplicaTableClassSummary *TableClassSummary `json:"replicaTableClassSummary,omitempty"`
}

// Represents the settings for a global table in a Region that will be modified.
type ReplicaSettingsUpdate struct {
	RegionName                          *string `json:"regionName,omitempty"`
	ReplicaProvisionedReadCapacityUnits *int64  `json:"replicaProvisionedReadCapacityUnits,omitempty"`
	ReplicaTableClass                   *string `json:"replicaTableClass,omitempty"`
}

// Represents one of the following:
//
//   - A new replica to be added to an existing global table.
//
//   - New parameters for an existing replica.
//
//   - An existing replica to be removed from an existing global table.
type ReplicaUpdate struct {
	// Represents a replica to be added.
	Create *CreateReplicaAction `json:"create,omitempty"`
	// Represents a replica to be removed.
	Delete *DeleteReplicaAction `json:"delete,omitempty"`
}

// Represents one of the following:
//
//   - A new replica to be added to an existing regional table or global table.
//     This request invokes the CreateTableReplica action in the destination
//     Region.
//
//   - New parameters for an existing replica. This request invokes the UpdateTable
//     action in the destination Region.
//
//   - An existing replica to be deleted. The request invokes the DeleteTableReplica
//     action in the destination Region, deleting the replica and all if its
//     items in the destination Region.
//
// When you manually remove a table or global table replica, you do not automatically
// remove any associated scalable targets, scaling policies, or CloudWatch alarms.
type ReplicationGroupUpdate struct {
	// Represents a replica to be created.
	Create *CreateReplicationGroupMemberAction `json:"create,omitempty"`
	// Represents a replica to be deleted.
	Delete *DeleteReplicationGroupMemberAction `json:"delete,omitempty"`
	// Represents a replica to be modified.
	Update *UpdateReplicationGroupMemberAction `json:"update,omitempty"`
}

// Contains details for the restore.
type RestoreSummary struct {
	RestoreDateTime   *metav1.Time `json:"restoreDateTime,omitempty"`
	RestoreInProgress *bool        `json:"restoreInProgress,omitempty"`
	SourceBackupARN   *string      `json:"sourceBackupARN,omitempty"`
	SourceTableARN    *string      `json:"sourceTableARN,omitempty"`
}

// The description of the server-side encryption status on the specified table.
type SSEDescription struct {
	InaccessibleEncryptionDateTime *metav1.Time `json:"inaccessibleEncryptionDateTime,omitempty"`
	KMSMasterKeyARN                *string      `json:"kmsMasterKeyARN,omitempty"`
	SSEType                        *string      `json:"sseType,omitempty"`
	Status                         *string      `json:"status,omitempty"`
}

// Represents the settings used to enable server-side encryption.
type SSESpecification struct {
	Enabled        *bool   `json:"enabled,omitempty"`
	KMSMasterKeyID *string `json:"kmsMasterKeyID,omitempty"`
	SSEType        *string `json:"sseType,omitempty"`
}

// Contains the details of the table when the backup was created.
type SourceTableDetails struct {
	BillingMode *string             `json:"billingMode,omitempty"`
	ItemCount   *int64              `json:"itemCount,omitempty"`
	KeySchema   []*KeySchemaElement `json:"keySchema,omitempty"`
	// Represents the provisioned throughput settings for a specified table or index.
	// The settings can be modified using the UpdateTable operation.
	//
	// For current minimum and maximum provisioned throughput values, see Service,
	// Account, and Table Quotas (https://docs.aws.amazon.com/amazondynamodb/latest/developerguide/Limits.html)
	// in the Amazon DynamoDB Developer Guide.
	ProvisionedThroughput *ProvisionedThroughput `json:"provisionedThroughput,omitempty"`
	TableARN              *string                `json:"tableARN,omitempty"`
	TableCreationDateTime *metav1.Time           `json:"tableCreationDateTime,omitempty"`
	TableID               *string                `json:"tableID,omitempty"`
	TableName             *string                `json:"tableName,omitempty"`
	TableSizeBytes        *int64                 `json:"tableSizeBytes,omitempty"`
}

// Contains the details of the features enabled on the table when the backup
// was created. For example, LSIs, GSIs, streams, TTL.
type SourceTableFeatureDetails struct {
	GlobalSecondaryIndexes []*GlobalSecondaryIndexInfo `json:"globalSecondaryIndexes,omitempty"`
	LocalSecondaryIndexes  []*LocalSecondaryIndexInfo  `json:"localSecondaryIndexes,omitempty"`
	// The description of the server-side encryption status on the specified table.
	SSEDescription *SSEDescription `json:"sseDescription,omitempty"`
	// Represents the DynamoDB Streams configuration for a table in DynamoDB.
	StreamDescription *StreamSpecification `json:"streamDescription,omitempty"`
	// The description of the Time to Live (TTL) status on the specified table.
	TimeToLiveDescription *TimeToLiveDescription `json:"timeToLiveDescription,omitempty"`
}

// Represents the DynamoDB Streams configuration for a table in DynamoDB.
type StreamSpecification struct {
	StreamEnabled  *bool   `json:"streamEnabled,omitempty"`
	StreamViewType *string `json:"streamViewType,omitempty"`
}

// Represents the auto scaling configuration for a global table.
type TableAutoScalingDescription struct {
	TableName   *string `json:"tableName,omitempty"`
	TableStatus *string `json:"tableStatus,omitempty"`
}

// Contains details of the table class.
type TableClassSummary struct {
	LastUpdateDateTime *metav1.Time `json:"lastUpdateDateTime,omitempty"`
	TableClass         *string      `json:"tableClass,omitempty"`
}

// The parameters for the table created as part of the import operation.
type TableCreationParameters struct {
	AttributeDefinitions   []*AttributeDefinition  `json:"attributeDefinitions,omitempty"`
	BillingMode            *string                 `json:"billingMode,omitempty"`
	GlobalSecondaryIndexes []*GlobalSecondaryIndex `json:"globalSecondaryIndexes,omitempty"`
	KeySchema              []*KeySchemaElement     `json:"keySchema,omitempty"`
	// Represents the provisioned throughput settings for a specified table or index.
	// The settings can be modified using the UpdateTable operation.
	//
	// For current minimum and maximum provisioned throughput values, see Service,
	// Account, and Table Quotas (https://docs.aws.amazon.com/amazondynamodb/latest/developerguide/Limits.html)
	// in the Amazon DynamoDB Developer Guide.
	ProvisionedThroughput *ProvisionedThroughput `json:"provisionedThroughput,omitempty"`
	// Represents the settings used to enable server-side encryption.
	SSESpecification *SSESpecification `json:"sseSpecification,omitempty"`
	TableName        *string           `json:"tableName,omitempty"`
}

// Represents the properties of a table.
type TableDescription struct {
	// Contains details of a table archival operation.
	ArchivalSummary           *ArchivalSummary                   `json:"archivalSummary,omitempty"`
	AttributeDefinitions      []*AttributeDefinition             `json:"attributeDefinitions,omitempty"`
	CreationDateTime          *metav1.Time                       `json:"creationDateTime,omitempty"`
	DeletionProtectionEnabled *bool                              `json:"deletionProtectionEnabled,omitempty"`
	GlobalSecondaryIndexes    []*GlobalSecondaryIndexDescription `json:"globalSecondaryIndexes,omitempty"`
	GlobalTableVersion        *string                            `json:"globalTableVersion,omitempty"`
	ItemCount                 *int64                             `json:"itemCount,omitempty"`
	KeySchema                 []*KeySchemaElement                `json:"keySchema,omitempty"`
	LatestStreamARN           *string                            `json:"latestStreamARN,omitempty"`
	LatestStreamLabel         *string                            `json:"latestStreamLabel,omitempty"`
	LocalSecondaryIndexes     []*LocalSecondaryIndexDescription  `json:"localSecondaryIndexes,omitempty"`
	// Represents the provisioned throughput settings for the table, consisting
	// of read and write capacity units, along with data about increases and decreases.
	ProvisionedThroughput *ProvisionedThroughputDescription `json:"provisionedThroughput,omitempty"`
	Replicas              []*ReplicaDescription             `json:"replicas,omitempty"`
	// Contains details for the restore.
	RestoreSummary *RestoreSummary `json:"restoreSummary,omitempty"`
	// Represents the DynamoDB Streams configuration for a table in DynamoDB.
	StreamSpecification *StreamSpecification `json:"streamSpecification,omitempty"`
	TableARN            *string              `json:"tableARN,omitempty"`
	TableID             *string              `json:"tableID,omitempty"`
	TableName           *string              `json:"tableName,omitempty"`
	TableSizeBytes      *int64               `json:"tableSizeBytes,omitempty"`
	TableStatus         *string              `json:"tableStatus,omitempty"`
}

// Describes a tag. A tag is a key-value pair. You can add up to 50 tags to
// a single DynamoDB table.
//
// Amazon Web Services-assigned tag names and values are automatically assigned
// the aws: prefix, which the user cannot assign. Amazon Web Services-assigned
// tag names do not count towards the tag limit of 50. User-assigned tag names
// have the prefix user: in the Cost Allocation Report. You cannot backdate
// the application of a tag.
//
// For an overview on tagging DynamoDB resources, see Tagging for DynamoDB (https://docs.aws.amazon.com/amazondynamodb/latest/developerguide/Tagging.html)
// in the Amazon DynamoDB Developer Guide.
type Tag struct {
	Key   *string `json:"key,omitempty"`
	Value *string `json:"value,omitempty"`
}

// The description of the Time to Live (TTL) status on the specified table.
type TimeToLiveDescription struct {
	AttributeName    *string `json:"attributeName,omitempty"`
	TimeToLiveStatus *string `json:"timeToLiveStatus,omitempty"`
}

// Represents the settings used to enable or disable Time to Live (TTL) for
// the specified table.
type TimeToLiveSpecification struct {
	AttributeName *string `json:"attributeName,omitempty"`
	Enabled       *bool   `json:"enabled,omitempty"`
}

// Represents a request to perform an UpdateItem operation.
type Update struct {
	TableName *string `json:"tableName,omitempty"`
}

// Represents the new provisioned throughput settings to be applied to a global
// secondary index.
type UpdateGlobalSecondaryIndexAction struct {
	IndexName *string `json:"indexName,omitempty"`
	// Represents the provisioned throughput settings for a specified table or index.
	// The settings can be modified using the UpdateTable operation.
	//
	// For current minimum and maximum provisioned throughput values, see Service,
	// Account, and Table Quotas (https://docs.aws.amazon.com/amazondynamodb/latest/developerguide/Limits.html)
	// in the Amazon DynamoDB Developer Guide.
	ProvisionedThroughput *ProvisionedThroughput `json:"provisionedThroughput,omitempty"`
}

// Represents a replica to be modified.
type UpdateReplicationGroupMemberAction struct {
	GlobalSecondaryIndexes []*ReplicaGlobalSecondaryIndex `json:"globalSecondaryIndexes,omitempty"`
	KMSMasterKeyID         *string                        `json:"kmsMasterKeyID,omitempty"`
	// Replica-specific provisioned throughput settings. If not specified, uses
	// the source table's provisioned throughput settings.
	ProvisionedThroughputOverride *ProvisionedThroughputOverride `json:"provisionedThroughputOverride,omitempty"`
	RegionName                    *string                        `json:"regionName,omitempty"`
	TableClassOverride            *string                        `json:"tableClassOverride,omitempty"`
}
