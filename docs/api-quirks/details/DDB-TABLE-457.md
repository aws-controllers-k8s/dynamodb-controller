<!-- generated: ack-api-quirks dynamodb 2026-10-09 model=2012-08-10 controller=34b85e6 -->
# DDB-TABLE-457: Degenerate-input 4xx catalogue: 82 empty-struct/empty-string/zero/-1 shapes across 20 operations are ValidationException with 6 validator...
_Full entry and notes of one finding; its summary entry is in [service.md](../service.md). Generated from
ack-api-quirks `services/dynamodb` (model 2012-08-10, controller commit 34b85e6); edit the finding in the lab,
not here._

## Finding

- <a id="ddb-table-457"></a>**DDB-TABLE-457** `request-validation` · impact medium · unhandled (not handled in controller) · verified 2026-10-09
  **Degenerate-input 4xx catalogue: 82 empty-struct/empty-string/zero/-1 shapes across 20 operations are ValidationException with 6 validator...**
  Of 98 schema-valid-but-degenerate requests reaching the service (100 sent, 2 refused client-side), 82 were
  HTTP 400 ValidationException, 3 AccessDeniedException (KMS alias/aws/s3), 2 TableInUseException (restore
  side effect, see the restore finding), 1 ResourceNotFoundException, 6 accepted, 4 HTTP 500 (HMAC key x3,
  ghost-GSI WarmThroughput). The 400 texts come in six validator styles that a message classifier must accept:
  (1) generic shape 'N validation error(s) detected: Value ... at '<camelCaseField>' failed to satisfy
  constraint: ...' (PITR {}, RecoveryPeriodInDays 0/-1, TTL {}/{Enabled}/AttributeName '', ExpectedRevisionId
  '', StreamArn '', Limit 0, BackupName '', TableName '', BillingModeOverride '', Provisioned/GSI/LSI
  overrides {} / [{}]); (2) hand-written per-field texts without the prefix: 'IndexName must be at least 3
  characters long and at most 255 characters long' (Contributor Insights), 'TableName must be at least 3
  characters long...' (legacy global-table APIs and RestoreTableFromBackup TargetTableName ''), 'Invalid
  Backup ARN', 'Invalid TableArn: Invalid Table ARN' (export with TableArn ''); (3) tag texts 'The Tag Key
  provided is invalid, Key: null' (Tags=[{}] / value-only), 'The Tag Value provided is invalid, Value: null'
  (key-only), 'The Tag Key provided is invalid, Key: ' (UntagResource ['']), 'Atleast one Tag Key needs to be
  provided as Input.'; (4) policy texts 'Invalid policy document: Syntax error at position (1,3)' for '{}',
  '...This policy contains invalid Json' for '' and '[]', '...This text appears not to be a policy' for
  'null', '...Missing required field Effect' for Statement [{}], '...Could not parse the policy: Statement is
  empty!' for Statement []; (5) 'One or more parameter values were invalid: <rule>' incl. 'Unknown
  ProjectionType: null' and the bare prefix with no detail for legacy UpdateGlobalTable ReplicaUpdates=[{}];
  (6) replica/witness texts: ReplicaUpdates Delete RegionName '' -> 'Region is not supported. The latest
  version of global tables are only supported in the following regions: [...]' (double space, 36 regions
  listed), GlobalTableWitnessUpdates [{}] / Create or Delete with RegionName '' -> counted as ABSENT ('At
  least one of ... is required'). Accepted degenerates: ListTables works normally; GSI-level
  WarmThroughput/OnDemandThroughput {} at CreateTable; OnDemandThroughput {-1,-1} on a GSI.
  - ACK: terminal_codes, none · ops: UpdateContinuousBackups, UpdateTimeToLive, TagResource, UntagResource,
    PutResourcePolicy, DeleteResourcePolicy, UpdateContributorInsights, DescribeContributorInsights,
    EnableKinesisStreamingDestination, DescribeBackup, ListTables, ListBackups, CreateBackup, DescribeTable,
    DescribeGlobalTable, UpdateGlobalTable, UpdateGlobalTableSettings, ExportTableToPointInTime, UpdateTable,
    CreateTable, RestoreTableFromBackup
  - repro: see probe.py groups G1-G4; every call is one request against an idle PAY_PER_REQUEST table (or a
    table with one GSI / one backup)
  - handling: not handled in the controller (as of commit 34b85e6)
  - related: [DDB-TABLE-437](../table-throughput-billing.md#ddb-table-437), [DDB-TABLE-107](../service.md#ddb-table-107), [DDB-TABLE-356](../table-policy-kinesis-autoscaling.md#ddb-table-356), [DDB-TABLE-223](../table-replicas.md#ddb-table-223), [DDB-TABLE-463](../table-restore.md#ddb-table-463), [DDB-TABLE-276](../table-restore.md#ddb-table-276),
    [DDB-TABLE-461](../table-streams-encryption-class.md#ddb-table-461), [DDB-TABLE-219](../table-restore.md#ddb-table-219) · evidence: table/creative/degenerate-5xx-hunt

## Notes

Full per-request table (label -> code 'message'):
create_backup_empty_name -> 400 ValidationException '2 validation errors detected: Value '' at 'backupName'
failed to satisfy constraint: Member must satisfy regular expression pattern: [a-zA-Z0-9_.-]+; Value '' at
'backupName' failed to satisfy constraint: Member must have length greater than or equal to 3'
ct_attrdefs_empty_entry -> 400 ValidationException '2 validation errors detected: Value null at
'attributeDefinitions.1.member.attributeType' failed to satisfy constraint: Member must not be null; Value
null at 'attributeDefinitions.1.member.attributeName' failed to satisfy constraint: Member must not be null'
ct_billing_empty -> 400 ValidationException '1 validation error detected: Value '' at 'billingMode' failed to
satisfy constraint: Member must satisfy enum value set: [PROVISIONED, PAY_PER_REQUEST]'
ct_gsi_name_empty -> 400 ValidationException '2 validation errors detected: Value '' at
'globalSecondaryIndexes.1.member.indexName' failed to satisfy constraint: Member must have length greater than
or equal to 3; Value '' at 'globalSecondaryIndexes.1.member.indexName' failed to satisfy constraint: Member'
ct_gsi_projection_empty -> 400 ValidationException 'One or more parameter values were invalid: Unknown
ProjectionType: null'
ct_keyschema_empty -> 400 ValidationException '1 validation error detected: Value '[]' at 'keySchema' failed
to satisfy constraint: Member must have length greater than or equal to 1'
ct_keyschema_empty_entry -> 400 ValidationException '2 validation errors detected: Value null at
'keySchema.1.member.attributeName' failed to satisfy constraint: Member must not be null; Value null at
'keySchema.1.member.keyType' failed to satisfy constraint: Member must not be null'
ct_policy_empty_object -> 400 ValidationException 'One or more parameter values were invalid: Invalid policy
document: Syntax error at position (1,3)'
ct_policy_empty_string -> 400 ValidationException 'One or more parameter values were invalid: Invalid policy
document: This policy contains invalid Json'
ct_sse_alias_aws_s3 -> 400 AccessDeniedException 'KMS key access denied error:
com.amazonaws.services.kms.model.AWSKMSException: User: arn:aws:sts::<ACCOUNT>:assumed-role/<PRINCIPAL> is not
authorized to perform: kms:CreateGrant on resource: arn:aws:kms:us-west-2:<ACCOUNT>:key/3b1a7419-d170-'
ct_sse_hmac_key -> 500 InternalServerError 'KMS internal error:
com.amazonaws.services.kms.model.AWSKMSException: EncryptionContext is supported only when creating a grant
for a symmetric encryption KMS key. (Service: AWSKMS; Status Code: 400; Error Code: ValidationException;
Request ID: 8eef99af-2e8d-4'
ct_sse_kms_key_empty -> 400 ValidationException '1 validation error detected: Value '' at
'sSESpecification.kMSMasterKeyId' failed to satisfy constraint: Member must have length greater than or equal
to 1'
ct_stream_viewtype_empty -> 400 ValidationException '1 validation error detected: Value '' at
'streamSpecification.streamViewType' failed to satisfy constraint: Member must satisfy enum value set:
[OLD_IMAGE, KEYS_ONLY, NEW_AND_OLD_IMAGES, NEW_IMAGE]'
ct_tags_empty_entry -> 400 ValidationException 'The Tag Key provided is invalid, Key: null'
ct_warm_neg_neg -> 400 ValidationException 'One or more parameter values were invalid: Requested
ReadUnitsPerSecond for WarmThroughput for table is lower than initial throughput for OnDemand. See:
https://docs.aws.amazon.com/amazondynamodb/latest/developerguide/on-demand-capacity-mode.html#on-demand-cap'
delete_policy_rev_empty -> 400 ValidationException '1 validation error detected: Value '' at
'expectedRevisionId' failed to satisfy constraint: Member must have length greater than or equal to 1'
describe_backup_empty_arn -> 400 ValidationException 'Invalid Backup ARN'
describe_table_empty_name -> 400 ValidationException '2 validation errors detected: Value '' at 'tableName'
failed to satisfy constraint: Member must have length greater than or equal to 3; Value '' at 'tableName'
failed to satisfy constraint: Member must satisfy regular expression pattern: [a-zA-Z0-9_.-]+'
export_arn_empty -> 400 ValidationException 'Invalid TableArn: Invalid Table ARN'
export_bucket_empty -> 400 ValidationException '1 validation error detected: Value '' at 's3Bucket' failed to
satisfy constraint: Member must satisfy regular expression pattern: ^[a-z0-9A-Z]+[\.\-\w]*[a-z0-9A-Z]+$'
export_incremental_empty_spec -> 400 ValidationException 'Invalid Request: ExportFromTime must be provided in
IncrementalExportSpecification'
gsi_create_warm_empty_name_empty -> 400 ValidationException '2 validation errors detected: Value '' at
'globalSecondaryIndexUpdates.1.member.create.indexName' failed to satisfy constraint: Member must have length
greater than or equal to 3; Value '' at 'globalSecondaryIndexUpdates.1.member.create.indexName' failed to sa'
gsi_update_name_only_real -> 400 ValidationException 'One or more parameter values were invalid: The only
Updates for index: gsi1 when TableThroughputMode is PAY_PER_REQUEST can be to OnDemandThroughput,
WarmThroughput'
gsi_update_odt_empty_ghost -> 400 ResourceNotFoundException 'Requested resource not found: Index ghost for
table ackq-220c6b-d5-gsi'
gsi_update_odt_empty_real -> 400 ValidationException 'One or more parameter values were invalid: The only
Updates for index: gsi1 when TableThroughputMode is PAY_PER_REQUEST can be to OnDemandThroughput,
WarmThroughput'
gsi_update_pt_empty_real -> 400 ValidationException '2 validation errors detected: Value null at
'globalSecondaryIndexUpdates.1.member.update.provisionedThroughput.writeCapacityUnits' failed to satisfy
constraint: Member must not be null; Value null at
'globalSecondaryIndexUpdates.1.member.update.provisionedThro'
gsi_update_warm_empty_ghost -> 400 ValidationException 'One or more parameter values were invalid:
WarmThroughput must have at least one of ReadUnitsPerSecond or WriteUnitsPerSecond specified for index: ghost'
gsi_update_warm_empty_ghost_missing_table -> 400 ValidationException 'One or more parameter values were
invalid: WarmThroughput must have at least one of ReadUnitsPerSecond or WriteUnitsPerSecond specified for
index: ghost'
gsi_update_warm_empty_real -> 400 ValidationException 'One or more parameter values were invalid:
WarmThroughput must have at least one of ReadUnitsPerSecond or WriteUnitsPerSecond specified for index: gsi1'
gsi_update_warm_ghost_valid_values -> 500 InternalFailure ''
gsi_update_warm_neg_real -> 400 ValidationException 'One or more parameter values were invalid: Requested
ReadUnitsPerSecond for WarmThroughput for index gsi1 is lower than current WarmThroughput, decreasing
WarmThroughput is not supported'
insights_action_empty -> 400 ValidationException '1 validation error detected: Value '' at
'contributorInsightsAction' failed to satisfy constraint: Member must satisfy enum value set: [ENABLE,
DISABLE]'
insights_describe_index_empty -> 400 ValidationException 'IndexName must be at least 3 characters long and at
most 255 characters long'
insights_index_empty -> 400 ValidationException 'IndexName must be at least 3 characters long and at most 255
characters long'
insights_mode_empty -> 400 ValidationException '1 validation error detected: Value '' at
'contributorInsightsMode' failed to satisfy constraint: Member must satisfy enum value set: [THROTTLED_KEYS,
ACCESSED_AND_THROTTLED_KEYS]'
kinesis_enable_empty_arn -> 400 ValidationException '1 validation error detected: Value '' at 'streamArn'
failed to satisfy constraint: Member must have length greater than or equal to 37'
legacy_describe_gt_empty -> 400 ValidationException 'TableName must be at least 3 characters long and at most
255 characters long'
legacy_describe_gts_empty -> 400 ValidationException 'TableName must be at least 3 characters long and at most
255 characters long'
legacy_update_gt_empty_entry -> 400 ValidationException 'One or more parameter values were invalid'
legacy_update_gts_empty_entry -> 400 ValidationException '1 validation error detected: Value null at
'replicaSettingsUpdate.1.member.regionName' failed to satisfy constraint: Member must not be null'
list_backups_limit0 -> 400 ValidationException '1 validation error detected: Value '0' at 'limit' failed to
satisfy constraint: Member must have value greater than or equal to 1'
list_tables_limit0 -> 400 ValidationException '1 validation error detected: Value '0' at 'limit' failed to
satisfy constraint: Member must have value greater than or equal to 1'
list_tables_start_empty -> 400 ValidationException '2 validation errors detected: Value '' at
'exclusiveStartTableName' failed to satisfy constraint: Member must satisfy regular expression pattern:
[a-zA-Z0-9_.-]+; Value '' at 'exclusiveStartTableName' failed to satisfy constraint: Member must have length
great'
policy_array -> 400 ValidationException 'One or more parameter values were invalid: Invalid policy document:
This policy contains invalid Json'
policy_empty_object -> 400 ValidationException 'One or more parameter values were invalid: Invalid policy
document: Syntax error at position (1,3)'
policy_empty_string -> 400 ValidationException 'One or more parameter values were invalid: Invalid policy
document: This policy contains invalid Json'
policy_null -> 400 ValidationException 'One or more parameter values were invalid: Invalid policy document:
This text appears not to be a policy'
policy_statement_empty_entry -> 400 ValidationException 'One or more parameter values were invalid: Invalid
policy document: Missing required field Effect'
policy_statement_empty_list -> 400 ValidationException 'One or more parameter values were invalid: Invalid
policy document: Could not parse the policy: Statement is empty!'
rst_billing_empty -> 400 ValidationException '1 validation error detected: Value '' at 'billingModeOverride'
failed to satisfy constraint: Member must satisfy enum value set: [PROVISIONED, PAY_PER_REQUEST]'
rst_gsi_override_empty_entry -> 400 ValidationException '3 validation errors detected: Value null at
'globalSecondaryIndexOverride.1.member.indexName' failed to satisfy constraint: Member must not be null; Value
null at 'globalSecondaryIndexOverride.1.member.keySchema' failed to satisfy constraint: Member must not b'
rst_gsi_override_name_only -> 400 ValidationException '3 validation errors detected: Value 'x' at
'globalSecondaryIndexOverride.1.member.indexName' failed to satisfy constraint: Member must have length
greater than or equal to 3; Value null at 'globalSecondaryIndexOverride.1.member.keySchema' failed to satisfy
con'
rst_lsi_override_empty_entry -> 400 ValidationException '3 validation errors detected: Value null at
'localSecondaryIndexOverride.1.member.indexName' failed to satisfy constraint: Member must not be null; Value
null at 'localSecondaryIndexOverride.1.member.keySchema' failed to satisfy constraint: Member must not be '
rst_odt_override_empty -> 400 TableInUseException 'Table: ackq-220c6b-d5-rst is already being restored from
backup: arn:aws:dynamodb:us-west-2:<ACCOUNT>:table/ackq-220c6b-d5-ppr/backup/01791525400213-63b4d36a'
rst_odt_override_zero -> 400 ValidationException 'Invalid Request: Requested MaxReadRequestUnits for
OnDemandThroughput is outside of valid range'
rst_pt_override_empty -> 400 ValidationException '2 validation errors detected: Value null at
'provisionedThroughputOverride.writeCapacityUnits' failed to satisfy constraint: Member must not be null;
Value null at 'provisionedThroughputOverride.readCapacityUnits' failed to satisfy constraint: Member must not
'
rst_pt_override_pay_per_request_empty -> 400 ValidationException '2 validation errors detected: Value null at
'provisionedThroughputOverride.writeCapacityUnits' failed to satisfy constraint: Member must not be null;
Value null at 'provisionedThroughputOverride.readCapacityUnits' failed to satisfy constraint: Member must not
'
rst_sse_override_empty -> 400 TableInUseException 'Table: ackq-220c6b-d5-rst is already being restored from
backup: arn:aws:dynamodb:us-west-2:<ACCOUNT>:table/ackq-220c6b-d5-ppr/backup/01791525400213-63b4d36a'
rst_sse_override_hmac -> 500 InternalServerError 'KMS internal error:
com.amazonaws.services.kms.model.AWSKMSException: EncryptionContext is supported only when creating a grant
for a symmetric encryption KMS key. (Service: AWSKMS; Status Code: 400; Error Code: ValidationException;
Request ID: 6b86bb8d-c0a5-4'
rst_sse_override_kms_empty_key -> 400 ValidationException '1 validation error detected: Value '' at
'sSESpecificationOverride.kMSMasterKeyId' failed to satisfy constraint: Member must have length greater than
or equal to 1'
rst_target_empty -> 400 ValidationException 'TableName must be at least 3 characters long and at most 255
characters long'
tag_empty_entry -> 400 ValidationException 'The Tag Key provided is invalid, Key: null'
tag_key_only -> 400 ValidationException 'The Tag Value provided is invalid, Value: null'
tag_value_only -> 400 ValidationException 'The Tag Key provided is invalid, Key: null'
ttl_attr_empty_string -> 400 ValidationException '1 validation error detected: Value '' at
'timeToLiveSpecification.attributeName' failed to satisfy constraint: Member must have length greater than or
equal to 1'
ttl_attr_only -> 400 ValidationException '1 validation error detected: Value null at
'timeToLiveSpecification.enabled' failed to satisfy constraint: Member must not be null'
ttl_empty_struct -> 400 ValidationException '2 validation errors detected: Value null at
'timeToLiveSpecification.attributeName' failed to satisfy constraint: Member must not be null; Value null at
'timeToLiveSpecification.enabled' failed to satisfy constraint: Member must not be null'
ttl_enabled_only -> 400 ValidationException '1 validation error detected: Value null at
'timeToLiveSpecification.attributeName' failed to satisfy constraint: Member must not be null'
ucb_disable_days0 -> 400 ValidationException '1 validation error detected: Value '0' at
'pointInTimeRecoverySpecification.recoveryPeriodInDays' failed to satisfy constraint: Member must have value
greater than or equal to 1'
ucb_empty_struct -> 400 ValidationException '1 validation error detected: Value null at
'pointInTimeRecoverySpecification.pointInTimeRecoveryEnabled' failed to satisfy constraint: Member must not be
null'
ucb_enable_days0 -> 400 ValidationException '1 validation error detected: Value '0' at
'pointInTimeRecoverySpecification.recoveryPeriodInDays' failed to satisfy constraint: Member must have value
greater than or equal to 1'
ucb_enable_days_neg -> 400 ValidationException '1 validation error detected: Value '-1' at
'pointInTimeRecoverySpecification.recoveryPeriodInDays' failed to satisfy constraint: Member must have value
greater than or equal to 1'
untag_empty_key -> 400 ValidationException 'The Tag Key provided is invalid, Key: '
untag_empty_list -> 400 ValidationException 'Atleast one Tag Key needs to be provided as Input.'
ut_attrdefs_empty_entry_plus_dp -> 400 ValidationException '2 validation errors detected: Value null at
'attributeDefinitions.1.member.attributeType' failed to satisfy constraint: Member must not be null; Value
null at 'attributeDefinitions.1.member.attributeName' failed to satisfy constraint: Member must not be null'
ut_attrdefs_type_empty_plus_dp -> 400 ValidationException '1 validation error detected: Value '' at
'attributeDefinitions.1.member.attributeType' failed to satisfy constraint: Member must satisfy enum value
set: [B, N, S]'
ut_odt_zero_zero -> 400 ValidationException 'One or more parameter values were invalid: Requested
MaxReadRequestUnits for OnDemandThroughput for table is outside of valid range'
ut_replica_create_empty_region -> 400 AccessDeniedException 'User:
arn:aws:sts::<ACCOUNT>:assumed-role/<PRINCIPAL> is not authorized to perform: dynamodb:Scan on resource:
arn:aws:dynamodb::<ACCOUNT>:table/ackq-220c6b-d5-ppr'
ut_replica_create_empty_struct -> 400 ValidationException 'One or more parameter values were invalid'
ut_replica_delete_empty_region -> 400 ValidationException 'Region is not supported. The latest version of
global tables are only supported in the following regions: [ap-south-2, ap-south-1, eu-south-1, eu-south-2,
me-central-1, il-central-1, ca-central-1, ap-east-2, mx-central-1, eu-central-1, eu-central-2, us-west-1'
ut_replica_update_empty_struct -> 400 ValidationException '1 validation error detected: Value null at
'replicaUpdates.1.member.update.regionName' failed to satisfy constraint: Member must not be null'
ut_sse_alias_aws_s3 -> 400 AccessDeniedException 'KMS key access denied error:
com.amazonaws.services.kms.model.AWSKMSException: User: arn:aws:sts::<ACCOUNT>:assumed-role/<PRINCIPAL> is not
authorized to perform: kms:CreateGrant on resource: arn:aws:kms:us-west-2:<ACCOUNT>:key/3b1a7419-d170-'
ut_sse_hmac_key -> 500 InternalServerError 'KMS internal error:
com.amazonaws.services.kms.model.AWSKMSException: EncryptionContext is supported only when creating a grant
for a symmetric encryption KMS key. (Service: AWSKMS; Status Code: 400; Error Code: ValidationException;
Request ID: 234f7469-2126-4'
ut_sse_kms_key_empty -> 400 ValidationException '1 validation error detected: Value '' at
'sSESpecification.kMSMasterKeyId' failed to satisfy constraint: Member must have length greater than or equal
to 1'
ut_stream_viewtype_empty -> 400 ValidationException '1 validation error detected: Value '' at
'streamSpecification.streamViewType' failed to satisfy constraint: Member must satisfy enum value set:
[OLD_IMAGE, KEYS_ONLY, NEW_AND_OLD_IMAGES, NEW_IMAGE]'
ut_tableclass_plus_warm_empty_gsi -> 400 ValidationException 'One or more parameter values were invalid:
WarmThroughput must have at least one of ReadUnitsPerSecond or WriteUnitsPerSecond specified for index: gsi1'
ut_warm_neg_neg -> 400 ValidationException 'One or more parameter values were invalid: Requested
ReadUnitsPerSecond for WarmThroughput for table is lower than current WarmThroughput, decreasing
WarmThroughput is not supported'
ut_warm_read_neg_only -> 400 ValidationException 'One or more parameter values were invalid: Requested
ReadUnitsPerSecond for WarmThroughput for table is lower than current WarmThroughput, decreasing
WarmThroughput is not supported'
ut_warm_zero_zero -> 400 ValidationException 'One or more parameter values were invalid: Requested
ReadUnitsPerSecond for WarmThroughput for table is lower than current WarmThroughput, decreasing
WarmThroughput is not supported'
ut_witness_create_empty_region -> 400 ValidationException 'At least one of ProvisionedThroughput, BillingMode,
UpdateStreamEnabled, GlobalSecondaryIndexUpdates, SSESpecification, ReplicaUpdates, MultiAccountReplicaReady,
ReplicaTransitRoleArn, MultiRegionConsistency, DeletionProtectionEnabled, OnDemandThroughput, Warm'
ut_witness_delete_empty_region -> 400 ValidationException 'At least one of ProvisionedThroughput, BillingMode,
UpdateStreamEnabled, GlobalSecondaryIndexUpdates, SSESpecification, ReplicaUpdates, MultiAccountReplicaReady,
ReplicaTransitRoleArn, MultiRegionConsistency, DeletionProtectionEnabled, OnDemandThroughput, Warm'
ut_witness_empty_entry -> 400 ValidationException 'At least one of ProvisionedThroughput, BillingMode,
UpdateStreamEnabled, GlobalSecondaryIndexUpdates, SSESpecification, ReplicaUpdates, MultiAccountReplicaReady,
ReplicaTransitRoleArn, MultiRegionConsistency, DeletionProtectionEnabled, OnDemandThroughput, Warm'
Accepted: ct_gsi_odt_empty, ct_gsi_warm_empty, gsi_update_odt_neg_real, kinesis_describe_ok,
ut_odt_empty_plus_billing, ut_warm_empty_plus_tableclass
Client-side rejected by botocore (Tags=[{Key:'',Value:''}] min length 1): ct_tags_empty_kv,
tag_empty_key_value

Contradiction with [DDB-TABLE-463](../table-restore.md#ddb-table-463): 457 reports two TableInUseException rows for RestoreTableFromBackup with
OnDemandThroughputOverride={} AND with SSESpecificationOverride={}; 463 isolates them: only the {}
OnDemandThroughputOverride triggers the self-conflict (3/3), SSESpecificationOverride={} on its own target
returns 200 - 457's second row hit the target already created by the first call (same TargetTableName)
Resolution: 463 canonical; 457's '2 TableInUseException (restore side effect)' is correct in count but should
not be read as SSE {} being rejected
