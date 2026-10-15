<!-- generated: ack-api-quirks dynamodb 2026-10-09 model=2012-08-10 controller=34b85e6 -->
# DDB-TABLE-442: ValidationException message catalogue: 4 of the ~360 texts are transient (TTL 30-min cooldown, CREATING/UPDATING state gates)...
_Full entry and notes of one finding; its summary entry is in [service.md](../service.md). Generated from
ack-api-quirks `services/dynamodb` (model 2012-08-10, controller commit 34b85e6); edit the finding in the lab,
not here._

## Finding

- <a id="ddb-table-442"></a>**DDB-TABLE-442** `error-code` · impact high · unhandled (not handled in controller) · verified 2026-10-09
  **ValidationException message catalogue: 4 of the ~360 texts are transient (TTL 30-min cooldown, CREATING/UPDATING state gates)...**
  ValidationException (HTTP 400) carries ~360 distinct message texts in this lab's evidence. Almost all are
  PERMANENT-SPEC, but a handful are transient and must NOT be surfaced as terminal: 'Time to live has been
  modified multiple times within a fixed interval' (TRANSIENT-RETRY, ~30 min, state invisible in
  DescribeTimeToLive), 'Cannot describe time to live while table is in CREATING state' / '...DELETING state'
  (WAIT-FOR-STATE TableStatus), 'Table or Index is not in a valid state to update Key Access Insights:
  TableStatus must be ACTIVE...' (WAIT-FOR-STATE), 'Table is not in a valid state to enable Kinesis Streaming
  Destination: ...must be DISABLED or ENABLE_FAILED...' / '...must be ACTIVE to perform DISABLE...'
  (WAIT-FOR-STATE DestinationStatus), 'Operation cannot be performed while replica server-side encryption
  status is in UPDATING state' and 'Create/Update/Delete of replica is not allowed while the replica is being
  added...' (WAIT-FOR-STATE ReplicaStatus), 'Replica cannot be deleted because it has acted as a source region
  for new replica(s) being added to the table in the last 24 hours' (PERMANENT-QUOTA 24 h). Every text starts
  with one of ~12 stable prefixes ('One or more parameter values were invalid: ', 'N validation error(s)
  detected: Value ... at ...', 'Invalid Request: ', 'KMS validation error: ', 'Failed to update settings for
  global table with name: ', 'Table is not in a valid state to ...', ...).
  - ACK: terminal_codes, requeue, custom_update · ops: UpdateTable, UpdateTimeToLive, DescribeTimeToLive,
    UpdateContributorInsights, EnableKinesisStreamingDestination, CreateTable, TagResource, PutResourcePolicy
    · fields: TimeToLiveSpecification, SSESpecification, StreamSpecification
  - repro: see probe.py (live) and catalogue.py (mined); e.g. UpdateTimeToLive enable then disable within 30
    min -> cooldown text
  - handling: not handled in the controller (as of commit 34b85e6)
  - related: [DDB-TABLE-312](../table-replicas.md#ddb-table-312), [DDB-TABLE-311](../table-replicas.md#ddb-table-311), [DDB-TABLE-266](../table-replicas.md#ddb-table-266), [DDB-TABLE-267](../table-replicas.md#ddb-table-267) · evidence:
    table/creative/error-message-regions

## Notes

Classes: RETRY = TRANSIENT-RETRY, clears by itself (typical wait given); WAIT = TRANSIENT-WAIT-FOR-STATE,
clears when the named status changes; SPEC = PERMANENT-SPEC, user must change the spec / fix an external
dependency; QUOTA = PERMANENT-QUOTA, needs a quota increase or the stated 1h/24h/30d window. Messages mined
from 119 past evidence.jsonl files (9562 error records, 29 codes; mine_errors.py -> mined-catalogue.txt in
this probe dir) plus this probe's live runs in us-west-2 and us-east-1; <TBL>/<ARN>/<TS>/<N>/<REGION>/<IDX>
mark request-specific tokens. Machine-readable form: catalogue.py in the probe directory.

CATALOGUE (ValidationException):
- [RETRY ~~30 min after the previous TTL change] Time to live has been modified multiple times within a fixed
interval (ops: UpdateTimeToLive; the cooldown is invisible in DescribeTimeToLive; on global tables the text
gains a '(Service: AmazonDynamoDBv2; ...; Request ID: ...; Proxy: null)' suffix)
- [WAIT ~TableStatus=ACTIVE (~5 s)] Cannot describe time to live while table is in CREATING state: Current
table state is CREATING (ops: DescribeTimeToLive)
- [WAIT ~table gone -> then ResourceNotFound] Cannot describe time to live while table is in DELETING state:
Current table state is DELETING (ops: DescribeTimeToLive)
- [WAIT ~TableStatus=ACTIVE] Table or Index is not in a valid state to update Key Access Insights: TableStatus
must be ACTIVE to enable ContributorInsights. (ops: UpdateContributorInsights)
- [WAIT ~IndexStatus=ACTIVE] Table or Index is not in a valid state to update Key Access Insights: IndexStatus
must be ACTIVE to enable ContributorInsights. (ops: UpdateContributorInsights)
- [WAIT ~DestinationStatus DISABLED/ENABLE_FAILED (DISABLING ~2 s)] Table is not in a valid state to enable
Kinesis Streaming Destination: EnableKinesisStreamingDestination must be DISABLED or ENABLE_FAILED to perform
ENABLE operation. (ops: EnableKinesisStreamingDestination)
- [WAIT ~DestinationStatus=ACTIVE (ENABLING ~6 s); PERMANENT if the ARN was never enabled] Table is not in a
valid state to enable Kinesis Streaming Destination: KinesisStreamingDestination must be ACTIVE to perform
DISABLE operation. (ops: DisableKinesisStreamingDestination)
- [WAIT ~DestinationStatus=ACTIVE] Table is not in a valid state to enable Kinesis Streaming Destination:
Kinesis streaming is not in ACTIVE state. Updates are only allowed in ACTIVE state. TableName: <TBL>, kdsArn:
<ARN> (ops: UpdateKinesisStreamingDestination)
- [WAIT ~Replicas[].ReplicaStatus / SSEDescription.Status=ENABLED] Operation cannot be performed while replica
server-side encryption status is in UPDATING state. Please retry the request after the status is updated to
ENABLED. (ops: UpdateTable)
- [WAIT ~ReplicaStatus=ACTIVE (~20-90 s)] Create/Update/Delete of replica is not allowed while the replica is
being added to table with name: ‘<TBL>’ in region: ‘<REGION>’. (ops: UpdateTable,UpdateTimeToLive)
- [QUOTA ~24 h after the replica was added] Replica cannot be deleted because it has acted as a source region
for new replica(s) being added to the table in the last <N> hours. (ops: DeleteTable,UpdateTable)
- [RETRY ~~3-6 s after ACTIVE] Operation cannot be performed... Backups are being enabled (see
ContinuousBackupsUnavailableException 'Backups are being enabled for the table: <TBL>. Please retry later')
(ops: UpdateContinuousBackups,CreateBackup; different code (ContinuousBackupsUnavailableException) but the
same transient class)
- [SPEC] At least one of ProvisionedThroughput, BillingMode, UpdateStreamEnabled, GlobalSecondaryIndexUpdates,
SSESpecification, ReplicaUpdates, MultiAccountReplicaReady, ReplicaTransitRoleArn, MultiRegionConsistency,
DeletionProtectionEnabled, OnDemandThroughput, WarmThroughput or TableClass is required (ops: UpdateTable;
list is request-dependent: members already present in the request (e.g. MultiRegionConsistency) are omitted;
names non-public members)
- [SPEC] One or more parameter values were invalid: Server-Side Encryption modification must be the only
operation in the request (ops: UpdateTable; split into separate UpdateTable calls)
- [SPEC] One or more parameter values were invalid: TableClass modification must be the only operation in the
request (ops: UpdateTable)
- [SPEC] One or more parameter values were invalid: DeletionProtection modification must be the only operation
in the request (ops: UpdateTable)
- [SPEC] One or more parameter values were invalid: Replica modification must be the only operation in the
request (ops: UpdateTable)
- [SPEC] One or more parameter values were invalid: WarmThroughput must be the only operation in the request
(ops: UpdateTable)
- [SPEC] One or more parameter values were invalid: Requests that modify replicas or witnesses must not also
modify other fields (ops: UpdateTable)
- [SPEC] One or more parameter values were invalid: OnDemandThroughput can only be combined with BillingMode
in an UpdateTable request (ops: UpdateTable)
- [SPEC] One or more parameter values were invalid: Create global secondary index cannot be specified when
updating WarmThroughput (ops: UpdateTable)
- [SPEC] One or more parameter values were invalid: You cannot modify stream status while updating table IOPS
(ops: UpdateTable)
- [SPEC] One or more parameter values were invalid: You cannot create or delete index while updating table
IOPS (ops: UpdateTable)
- [SPEC] One or more parameter values were invalid: You cannot create or delete index while changing stream
status (ops: UpdateTable)
- [SPEC] The provisioned throughput for the table will not change. The requested value equals the current
value. Current ReadCapacityUnits provisioned for the table: <N>. Requested ReadCapacityUnits: <N>. Current
WriteCapacityUnits provisioned for the table: <N>. Requested WriteCapacityUnits: <N>. Refer to the Amazon
DynamoDB Developer Guide for current limits and how to request higher limits. (ops: UpdateTable; NO-OP
re-send: treat as success)
- [SPEC] The provisioned throughput for the index <IDX> will not change. The requested value equals the
current value. ... (ops: UpdateTable; NO-OP re-send: treat as success)
- [SPEC] One or more parameter values were invalid: Table is already encrypted by default (ops: UpdateTable;
NO-OP re-send (SSESpecification {} / Enabled:false on AWS-owned key))
- [SPEC] One or more parameter values were invalid: Table is already encrypted with given KMSMasterKeyId. Use
KMSMasterKeyId parameter if you want to change Master Key (ops: UpdateTable; NO-OP re-send of the current key
ARN)
- [SPEC] Table already has an enabled stream: TableName: <TBL> (ops: UpdateTable; NO-OP re-send of
StreamEnabled:true)
- [SPEC] Table has no stream to disable: TableName: <TBL> (ops: UpdateTable; NO-OP re-send of
StreamEnabled:false)
- [SPEC] TimeToLive is already enabled (ops: UpdateTimeToLive; NO-OP re-send)
- [SPEC] TimeToLive is already disabled (ops: UpdateTimeToLive; NO-OP re-send)
- [SPEC] TimeToLive is active on a different AttributeName: current AttributeName is <ATTR> (ops:
UpdateTimeToLive; disable first (then 30-min cooldown))
- [SPEC] Invalid Request: Precision is already set to the desired value of MICROSECOND for tableId: <UUID>,
kdsArn: <ARN> (ops: UpdateKinesisStreamingDestination; NO-OP re-send)
- [SPEC] One or more parameter values were invalid: If stream is being enabled then UpdateViewType is required
(ops: UpdateTable)
- [SPEC] One or more parameter values were invalid: If stream is being disabled, then UpdateViewType must not
be specified (ops: UpdateTable)
- [SPEC] One or more parameter values were invalid: Disabling Stream is not allowed for a Global Table
replica. (ops: UpdateTable)
- [SPEC] One or more parameter values were invalid: Table is being created with a stream enabled,
UpdateViewType is required (ops: CreateTable)
- [SPEC] One or more parameter values were invalid: Table is being created with a stream disabled,
UpdateViewType should not be specified (ops: CreateTable)
- [SPEC] One or more parameter values were invalid: Neither ReadCapacityUnits nor WriteCapacityUnits can be
specified when BillingMode is PAY_PER_REQUEST (ops: UpdateTable,CreateTable)
- [SPEC] Neither ReadCapacityUnits nor WriteCapacityUnits can be specified when BillingMode is PAY_PER_REQUEST
(ops: UpdateTable; same rule without the usual prefix)
- [SPEC] One or more parameter values were invalid: ProvisionedThroughput cannot be specified when BillingMode
is PAY_PER_REQUEST (ops: UpdateTable)
- [SPEC] One or more parameter values were invalid: ReadCapacityUnits and WriteCapacityUnits must both be
specified when BillingMode is PROVISIONED (ops: CreateTable,ImportTable)
- [SPEC] One or more parameter values were invalid: ProvisionedThroughput must be specified when BillingMode
is PROVISIONED (ops: UpdateTable)
- [SPEC] One or more parameter values were invalid: ProvisionedThroughput must be specified for index: <IDX>
(ops: UpdateTable)
- [SPEC] One or more parameter values were invalid: ProvisionedThroughput is not specified for index: <IDX>
(ops: CreateTable)
- [SPEC] One or more parameter values were invalid: ProvisionedThroughput should not be specified for index:
<IDX> when BillingMode is PAY_PER_REQUEST (ops: CreateTable)
- [SPEC] One or more parameter values were invalid: Neither ReadCapacityUnits nor WriteCapacityUnits can be
specified for index: <IDX> when BillingMode is PAY_PER_REQUEST (ops: UpdateTable)
- [SPEC] One or more parameter values were invalid: Both ReadCapacityUnits and WriteCapacityUnits must be
specified for index: <IDX> (ops: UpdateTable)
- [SPEC] One or more parameter values were invalid: The only Updates for index: <IDX> when TableThroughputMode
is PAY_PER_REQUEST can be to OnDemandThroughput, WarmThroughput (ops: UpdateTable)
- [SPEC] One or more parameter values were invalid: MaxReadRequestUnits for OnDemandThroughput cannot be
specified when the table BillingMode is PROVISIONED (ops: UpdateTable,CreateTable,ImportTable; also
'MaxWriteRequestUnits ...', 'for index : <IDX>' and a prefix-less variant)
- [SPEC] One or more parameter values were invalid: Requested MaxReadRequestUnits for OnDemandThroughput for
table is outside of valid range (ops: CreateTable,UpdateTable; 0 / negative (except -1 on UpdateTable =
clear); also MaxWriteRequestUnits and 'for index : <IDX>')
- [SPEC] Invalid Request: Requested MaxReadRequestUnits for OnDemandThroughput is outside of valid range (ops:
RestoreTableFromBackup)
- [SPEC] One or more parameter values were invalid: Requested ReadUnitsPerSecond for WarmThroughput for table
is lower than current WarmThroughput, decreasing WarmThroughput is not supported (ops: UpdateTable; also
WriteUnitsPerSecond and 'for index <IDX>')
- [SPEC] One or more parameter values were invalid: Requested ReadUnitsPerSecond for WarmThroughput for table
is lower than initial throughput for OnDemand. See: https://docs.aws.amazon.com/... (ops: CreateTable; also
'for index <IDX>')
- [SPEC] One or more parameter values were invalid: Requested ReadUnitsPerSecond for WarmThroughput for table
is lower than ReadCapacityUnits of ProvisionedThroughput (ops: CreateTable)
- [SPEC] <N> validation error(s) detected: Value '<v>' at '<field>' failed to satisfy constraint: <constraint>
(ops: all; generic shape validator: enum sets, min/max values (provisionedThroughput >= 1, limit <= 100,
recoveryPeriodInDays <= 35), lengths, regex [a-zA-Z0-9_.-]+, 'Member must not be null'; fires BEFORE existence
checks)
- [SPEC] Invalid KeySchema: The first KeySchemaElement is not a HASH key type (ops: CreateTable)
- [SPEC] Invalid KeySchema: The second KeySchemaElement is not a RANGE key type (ops: CreateTable)
- [SPEC] Invalid KeySchema: Some index key attribute have no definition (ops: CreateTable)
- [SPEC] One or more parameter values were invalid: Some index key attributes are not defined in
AttributeDefinitions. Keys: [<X>], AttributeDefinitions: [<X>] (ops: CreateTable)
- [SPEC] One or more parameter values were invalid: Some AttributeDefinitions are not used.
AttributeDefinitions: [<X>], keys used: [<X>] (ops: CreateTable)
- [SPEC] One or more parameter values were invalid: Number of attributes in KeySchema does not exactly match
number of attributes defined in AttributeDefinitions (ops: CreateTable)
- [SPEC] One or more parameter values were invalid: AttributeDefinitions is not specified for index: <IDX>
(ops: UpdateTable)
- [SPEC] Attribute Name is duplicated: <ATTR> (ops: CreateTable)
- [SPEC] Both the Hash Key and the Range Key element in the KeySchema have the same name (ops: CreateTable)
- [SPEC] The KeySchema contains multiple key attributes with the same name (ops: CreateTable)
- [SPEC] One or more parameter values were invalid: All HASH key attributes must precede RANGE key attributes
in the KeySchema (ops: CreateTable)
- [SPEC] One or more parameter values were invalid: The KeySchema exceeds the maximum allowed number of HASH
key attributes (ops: CreateTable,UpdateTable; also RANGE)
- [SPEC] One or more parameter values were invalid: List of GlobalSecondaryIndexes is empty (ops: CreateTable;
also LocalSecondaryIndexes; GlobalSecondaryIndexUpdates on UpdateTable)
- [SPEC] One or more parameter values were invalid: GlobalSecondaryIndex count exceeds the per-table limit of
<N> (ops: CreateTable; same limit is LimitExceededException via UpdateTable)
- [SPEC] One or more parameter values were invalid: Number of LocalSecondaryIndexes exceeds per-table limit of
<N> (ops: CreateTable)
- [SPEC] One or more parameter values were invalid: Duplicate index name: <IDX> (ops: CreateTable)
- [SPEC] Attempting to create an index which already exists (ops: UpdateTable)
- [SPEC] Cannot delete an index that is not a GlobalSecondaryIndex: <IDX> (ops: UpdateTable; also 'Cannot
update an index ...')
- [SPEC] One or more parameter values were invalid: Only one global secondary index update per index is
allowed simultaneously. Index: <IDX> (ops: UpdateTable)
- [SPEC] One or more parameter values were invalid: Only one global secondary index action is allowed per
GlobalSecondaryIndexUpdate object (ops: UpdateTable)
- [SPEC] One or more parameter values were invalid: One of GlobalSecondaryIndexUpdate.Update,
GlobalSecondaryIndexUpdate.Create, GlobalSecondaryIndexUpdate.Delete must not be null (ops: UpdateTable)
- [SPEC] One or more parameter values were invalid: ProjectionType is INCLUDE, but NonKeyAttributes is not
specified (ops: CreateTable; also KEYS_ONLY/ALL 'but NonKeyAttributes is specified', 'Unknown ProjectionType:
null', 'Duplicate element in NonKeyAttributes: <X>')
- [SPEC] One or more parameter values were invalid: Table KeySchema does not have a range key, which is
required when specifying a LocalSecondaryIndex (ops: CreateTable)
- [SPEC] One or more parameter values were invalid: Index KeySchema does not have the same leading hash key as
table KeySchema for index: <IDX>. index hash key: <ATTR>, table hash key: <ATTR> (ops: CreateTable; also
'Index KeySchema does not have a range key for index: <IDX>')
- [SPEC] Invalid table-class parameter provided. Please try again with a valid table-class value: [STANDARD,
STANDARD_INFREQUENT_ACCESS]. (ops: CreateTable,UpdateTable)
- [SPEC] One or more parameter values were invalid: SSEType can not be specified if Enabled is false (ops:
CreateTable,UpdateTable; also 'KMSMasterKeyId can not be specified if Enabled is false', 'SSEType KMS is
required if KMSMasterKeyId is specified', 'SSEType AES256 is not supported')
- [SPEC] KMS key disabled error: com.amazonaws.services.kms.model.DisabledException: <ARN> is disabled.
(Service: AWSKMS; Status Code: 400; Error Code: DisabledException; Request ID: <UUID>; Proxy: null) (ops:
CreateTable,UpdateTable,GetItem; external fix (enable key); carries a KMS request id)
- [SPEC] KMS validation error: com.amazonaws.services.kms.model.KMSInvalidStateException: <ARN> is pending
deletion. (Service: AWSKMS; ...) (ops: CreateTable,UpdateTable)
- [SPEC] KMS validation error: com.amazonaws.services.kms.model.NotFoundException: Alias <ARN> is not found.
(Service: AWSKMS; ...) (ops: CreateTable,UpdateTable,RestoreTableFromBackup,ImportTable; also "Key '<ARN>'
does not exist" and 'Invalid arn <REGION>' (other-region key); replica variant 'KMS validation error for
region <REGION>: ...')
- [SPEC] One or more parameter values were invalid: KMSMasterKeyId must be specified for each replica. (ops:
UpdateTable; also 'All replica keys must either be Customer Managed CMK or AWS Managed CMK.')
- [SPEC] One or more parameter values were invalid: ARNs must start with 'arn:': <X> (ops:
ListTagsOfResource,TagResource,GetResourcePolicy,PutResourcePolicy,RestoreTableToPointInTime)
- [SPEC] One or more parameter values were invalid: Provided Arn is not a DynamoDB resource arn: <ARN> (ops:
ListTagsOfResource,*ResourcePolicy)
- [SPEC] One or more parameter values were invalid: Invalid resource arn provided, only table or stream is
accepted. Provided resource arn: <ARN> (ops: ListTagsOfResource,TagResource,*ResourcePolicy; index ARNs)
- [SPEC] One or more parameter values were invalid: Invalid resource Arn: <ARN> (ops: *ResourcePolicy; other
region / partition)
- [SPEC] Invalid TableArn: Invalid ResourceArn provided as input <ARN> (ops: ListTagsOfResource; other-region
ARN)
- [SPEC] This action is only supported by accounts that match the resource owner’s account. (ops:
*ResourcePolicy,RestoreTableToPointInTime)
- [SPEC] <N> validation error detected: Invalid AWS region in '<ARN>' (ops:
DescribeTable,DescribeTimeToLive,DescribeContinuousBackups,DescribeContributorInsights; also 'Invalid AWS
partition in', 'Invalid resource type in', 'Valid ARN format is ...')
- [SPEC] Invalid Backup ARN (ops: DescribeBackup,DeleteBackup,RestoreTableFromBackup; also 'Invalid Request:
BackupArn is not valid', 'Invalid Export ARN', 'Invalid Import ARN', 'tableArn is not a valid ARN',
'sourceTableArn is not a valid ARN', 'Invalid Request: Table ARN is invalid.')
- [SPEC] The Tag Key provided is invalid, Key: <X> (ops: TagResource,UntagResource,CreateTable; also 'The Tag
Value provided is invalid, Value: <X>' (Value: null when absent), 'Tag Key cannot be prefixed with aws:, Key:
<X>', 'Duplicate Tag Keys provided as input: Duplicate Tag Key found <X>')
- [SPEC] Number of Tags exceed the current limit for the provided ResourceArn (ops:
TagResource,UntagResource,CreateTable; 50-tag limit is ValidationException, not LimitExceeded)
- [SPEC] Tag set size <N> bytes is above max size limit of <N> bytes (ops: TagResource)
- [SPEC] Atleast one Tag needs to be provided as Input. (ops: TagResource; also 'Atleast one Tag Key needs to
be provided as Input.' (UntagResource))
- [SPEC] One or more parameter values were invalid: Invalid policy document: <X> (ops: PutResourcePolicy; <X>
in {This policy contains invalid Json, Missing required field Resource, Could not parse the policy: Statement
is empty!, Syntax error at position (r,c), The Statement Ids in the policy are not unique, The following
action names are invalid: ..., The relative-id ... is invalid for ARN ...})
- [SPEC] One or more parameter values were invalid: Maximum policy size of 20480 bytes exceeded (ops:
PutResourcePolicy)
- [SPEC] Resource-based policy grants unbounded access in one or more elements. Please revise the policy to
ensure least-privilege form of access (ops: PutResourcePolicy; also 'Invalid principal in policy document.')
- [SPEC] Resource cannot be deleted as it is currently protected against deletion. Disable deletion protection
first. (ops: DeleteTable; replica variant: 'Cannot delete table <TBL> in region <REGION> because it has
deletion protection enabled. Disable deletion protection first.')
- [SPEC] Invalid Request: Cannot specify RecoveryPeriodInDays when disabling point-in-time recovery. (ops:
UpdateContinuousBackups)
- [SPEC] Invalid Request: Either one of RestoreDateTime or UseLatestRestorableTime must be specified, but not
both (ops: RestoreTableToPointInTime; also 'Both RestoreDateTime and UseLatestRestorableTime cannot be
set...', 'Must provide exactly one of: sourceTableArn, sourceTableName', "The parameter 'TableName' is
required but was not present in the request")
- [SPEC] Invalid Request: Index <IDX> does not match a secondary index that existed in the source table and
cannot be created during the restore operation (ops: RestoreTableFromBackup)
- [SPEC] Invalid Request: sseSpecificationOverride must be provided for cross-region restores (ops:
RestoreTableFromBackup,RestoreTableToPointInTime)
- [SPEC] Invalid Request: Cannot override ProvisionedThroughput if BillingMode is overridden to
PAY_PER_REQUEST (ops: RestoreTableFromBackup; also '... for index <IDX>', 'Must specify provisioned throughput
for index <IDX>', 'Cannot override MaxReadRequestUnits for OnDemandThroughput unless BillingModeOverride is
PAY_PER_REQUEST', 'Cannot override ProvisionedThroughput unless BillingMode is PROVISIONED', 'Must override
ProvisionedThroughput if BillingMode is overridden to PROVISIONED')
- [SPEC] Invalid Request: User is not allowed to delete the system backup with arn <ARN>. It will
automatically expire on <TS> (ops: DeleteBackup; carries an expiry timestamp)
- [SPEC] Failed to create a the new replica of table with name: ‘<TBL>’ because one or more replicas already
existed as tables. (ops: UpdateTable; typographic quotes)
- [SPEC] Update global table operation failed because one or more replicas were not part of the global table.
Please retry the request without these replicas: [<REGION>]. (ops: UpdateTable; also '... witnesses ...')
- [SPEC] There are no actions specified in the Replica Update Action of the request. (ops: UpdateTable; also
'Update table operation with more than one create or delete replica actions not allowed', '... more than one
type of replica actions not allowed.', 'Cannot target multiple regions with the same action', 'Cannot add or
delete the local region through ReplicaUpdates...')
- [SPEC] Region <X> is not supported. The latest version of global tables are only supported in the following
regions: [...] (ops: UpdateTable; also 'Failed to access the region: ‘<REGION>’. User is missing the
permissions since the region is disabled.' (opt-in region))
- [SPEC] Table write capacity should either be Pay-Per-Request or AutoScaled. (ops: UpdateTable; also 'GSI
write capacity should either be Pay-Per-Request or AutoScaled.')
- [SPEC] Deletion of only one table replica is not supported for global tables with MultiRegionConsistency set
to STRONG. (ops: DeleteTable,UpdateTable; MRSC family: 'Unsupported replica count ...', 'Cannot add replicas
to a global table with strong MultiRegionConsistency', 'MultiRegionConsistency parameter is unsupported on
existing global table', 'Update table delete operation does not include all witnesses.', 'Unsupported
Region(s) specified ...', 'MultiRegionConsistency must be set as STRONG when GlobalTableWitnessUpdates
parameter is present.', 'Only Replica Create actions are supported when MultiRegionConsistency parameter is
provided.', 'Cannot add a witness in the same region as an existing replica...')
- [SPEC] Value 'ENABLED_WITH_OVERRIDES' at 'GlobalTableSettingsReplicationMode' failed to satisfy constraint:
Only ENABLED and DISABLED settings replication are supported for a Regional Table. (ops: UpdateTable)
- [SPEC] Failed to update settings for global table with name: ‘<TBL>’: Parameters '<X>' are required unless
auto scaling is being disabled. (ops: UpdateTableReplicaAutoScaling; AAS facade; also '... must be left blank
when disabling auto scaling.', 'because at least one update parameter must be specified...', 'because the
regions: ‘[...]’ were specified more than once.', and AAS pass-through texts ending in '(Service:
AWSApplicationAutoScaling; ...; Request ID: <UUID>; Proxy: null)')
- [SPEC] Failed to update global table with name ‘<TBL>‘. Replicas '<REGION>', '<REGION>' BillingMode is
PayPerRequest. You must convert table's BillingMode to PROVISIONED to set parameters: '<X>'. (ops:
UpdateTableReplicaAutoScaling)
- [SPEC] One or more parameter values were invalid: DynamoDB global tables version 2017.11.29 is not
supported. We recommend using DynamoDB global tables version 2019.11.21, instead of version 2017.11.29
(Legacy). (ops: CreateGlobalTable,UpdateGlobalTable)
- [SPEC] Streaming destination cannot be updated with given parameters: UpdateKinesisStreamingConfiguration
cannot be null or contain only null values (ops: UpdateKinesisStreamingDestination; also 'Stream cannot be
disabled using EnableKinesisStreamingConfiguration parameters')
- [SPEC] Invalid Request: <export/import shape rule> (ops:
ExportTableToPointInTime,ImportTable,ListExports,ListImports; e.g. 'ExportFromTime must be provided in
IncrementalExportSpecification', 'KMS Key Id is not supported for this encryption type', 'Provided nextToken
is invalid: <hex>', 'Unsupported InputFormatOptions for the given input format: DYNAMODB_JSON.')
- [SPEC] One or more parameter values were invalid: Time range lower bound must be less than or equal to upper
bound (ops: ListBackups; also 'Invalid Request: The supplied ExclusiveStartBackupArn does not match the
region')
- [SPEC] One or more parameter values were invalid (ops: UpdateGlobalTable; bare prefix, no detail)

LIVE this run (region/label: http code latency 'message'):
us-east-1/val_ttl_cooldown: 400 ValidationException 64ms 'Time to live has been modified multiple times within
a fixed interval'
us-west-2/val_ttl_cooldown: 400 ValidationException 6ms 'Time to live has been modified multiple times within
a fixed interval'
us-east-1/creating_describe_ttl: 400 ValidationException 68ms 'Cannot describe time to live while table is in
CREATING state: Current table state is CREATING'
us-west-2/creating_describe_ttl: 400 ValidationException 12ms 'Cannot describe time to live while table is in
CREATING state: Current table state is CREATING'
us-east-1/val_update_only_name: 400 ValidationException 70ms 'At least one of ProvisionedThroughput,
BillingMode, UpdateStreamEnabled, GlobalSecondaryIndexUpdates, SSESpecification, ReplicaUpdates,
MultiAccountReplicaReady, ReplicaTransitRoleArn, MultiRegionConsistency, DeletionProtectionEnabled,
OnDemandThroughput, WarmThroughput or TableClass is required'
us-west-2/val_update_only_name: 400 ValidationException 17ms 'At least one of ProvisionedThroughput,
BillingMode, UpdateStreamEnabled, GlobalSecondaryIndexUpdates, SSESpecification, ReplicaUpdates,
MultiAccountReplicaReady, ReplicaTransitRoleArn, MultiRegionConsistency, DeletionProtectionEnabled,
OnDemandThroughput, WarmThroughput or TableClass is required'
us-east-1/val_update_mrc_only: 400 ValidationException 64ms 'At least one of ProvisionedThroughput,
BillingMode, UpdateStreamEnabled, GlobalSecondaryIndexUpdates, SSESpecification, ReplicaUpdates,
MultiAccountReplicaReady, ReplicaTransitRoleArn, DeletionProtectionEnabled, OnDemandThroughput, WarmThroughput
or TableClass is required'
us-west-2/val_update_mrc_only: 400 ValidationException 7ms 'At least one of ProvisionedThroughput,
BillingMode, UpdateStreamEnabled, GlobalSecondaryIndexUpdates, SSESpecification, ReplicaUpdates,
MultiAccountReplicaReady, ReplicaTransitRoleArn, DeletionProtectionEnabled, OnDemandThroughput, WarmThroughput
or TableClass is required'
us-east-1/val_update_attrdefs_only: 400 ValidationException 64ms 'At least one of ProvisionedThroughput,
BillingMode, UpdateStreamEnabled, GlobalSecondaryIndexUpdates, SSESpecification, ReplicaUpdates,
MultiAccountReplicaReady, ReplicaTransitRoleArn, MultiRegionConsistency, DeletionProtectionEnabled,
OnDemandThroughput, WarmThroughput or TableClass is required'
us-west-2/val_update_attrdefs_only: 400 ValidationException 7ms 'At least one of ProvisionedThroughput,
BillingMode, UpdateStreamEnabled, GlobalSecondaryIndexUpdates, SSESpecification, ReplicaUpdates,
MultiAccountReplicaReady, ReplicaTransitRoleArn, MultiRegionConsistency, DeletionProtectionEnabled,
OnDemandThroughput, WarmThroughput or TableClass is required'
us-east-1/val_sse_plus_dp: 400 ValidationException 64ms 'One or more parameter values were invalid:
Server-Side Encryption modification must be the only operation in the request'
us-west-2/val_sse_plus_dp: 400 ValidationException 7ms 'One or more parameter values were invalid: Server-Side
Encryption modification must be the only operation in the request'
us-east-1/val_pt_on_ppr: 400 ValidationException 71ms 'One or more parameter values were invalid: Neither
ReadCapacityUnits nor WriteCapacityUnits can be specified when BillingMode is PAY_PER_REQUEST'
us-west-2/val_pt_on_ppr: 400 ValidationException 12ms 'One or more parameter values were invalid: Neither
ReadCapacityUnits nor WriteCapacityUnits can be specified when BillingMode is PAY_PER_REQUEST'
us-east-1/val_delete_dp_on: 400 ValidationException 68ms 'Resource cannot be deleted as it is currently
protected against deletion. Disable deletion protection first.'
us-west-2/val_delete_dp_on: 400 ValidationException 10ms 'Resource cannot be deleted as it is currently
protected against deletion. Disable deletion protection first.'
us-east-1/val_ttl_already_enabled_other_attr: 400 ValidationException 64ms 'TimeToLive is active on a
different AttributeName: current AttributeName is ttl'
us-west-2/val_ttl_already_enabled_other_attr: 400 ValidationException 6ms 'TimeToLive is active on a different
AttributeName: current AttributeName is ttl'
us-east-1/val_stream_disable_none: 400 ValidationException 74ms 'Table has no stream to disable: TableName:
ackq-e3283a-emr-e1'
us-west-2/val_stream_disable_none: 400 ValidationException 12ms 'Table has no stream to disable: TableName:
ackq-e3283a-emr-w2'
us-east-1/val_kinesis_disable_bogus: 400 ValidationException 115ms 'Table is not in a valid state to enable
Kinesis Streaming Destination: KinesisStreamingDestination must be ACTIVE to perform DISABLE operation.'
us-west-2/val_kinesis_disable_bogus: 400 ValidationException 29ms 'Table is not in a valid state to enable
Kinesis Streaming Destination: KinesisStreamingDestination must be ACTIVE to perform DISABLE operation.'
us-east-1/ise_warm_empty_plus_pt: 400 ValidationException 74ms 'One or more parameter values were invalid:
Neither ReadCapacityUnits nor WriteCapacityUnits can be specified when BillingMode is PAY_PER_REQUEST'
us-west-2/ise_warm_empty_plus_pt: 400 ValidationException 13ms 'One or more parameter values were invalid:
Neither ReadCapacityUnits nor WriteCapacityUnits can be specified when BillingMode is PAY_PER_REQUEST'
us-east-1/ise_warm_empty_bad_name: 400 ValidationException 64ms '1 validation error detected: Value 'ab' at
'tableName' failed to satisfy constraint: Member must have length greater than or equal to 3'
us-west-2/ise_warm_empty_bad_name: 400 ValidationException 6ms '1 validation error detected: Value 'ab' at
'tableName' failed to satisfy constraint: Member must have length greater than or equal to 3'

Contradiction with [DDB-TABLE-312](../table-replicas.md#ddb-table-312), [DDB-TABLE-311](../table-replicas.md#ddb-table-311), [DDB-TABLE-266](../table-replicas.md#ddb-table-266), [DDB-TABLE-267](../table-replicas.md#ddb-table-267): 442 classifies 'Operation
cannot be performed while replica server-side encryption status is in UPDATING state' as WAIT-FOR-STATE
ReplicaStatus and the 24 h source-region text as PERMANENT-QUOTA 24 h; 312/311 show the SSE text is returned
for a disabled KMS key with nothing visible changing in DescribeTable, and 266/267 show the 24 h text clears
as soon as the sourced replica is removed Resolution: keep 442 as the catalogue; 312/311 canonical for the SSE
text (external dependency: EnableKey, not a state wait) and 266/267 for the 24 h text (retry after replica
removal, not a quota window)
