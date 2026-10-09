<!-- generated: ack-api-quirks dynamodb 2026-10-09 model=2012-08-10 controller=34b85e6 -->
# DDB-TABLE-015: UpdateTable with only TableName -> ValidationException 'At least one of ... is required'; GlobalSecondaryIndexUpdates=[] counts as absent
_Full entry and notes of one finding; its summary entry is in [table-indexes.md](../table-indexes.md).
Generated from ack-api-quirks `services/dynamodb` (model 2012-08-10, controller commit 34b85e6); edit the
finding in the lab, not here._

## Finding

- <a id="ddb-table-015"></a>**DDB-TABLE-015** `request-validation` · impact high · unhandled (not handled in controller) · verified 2026-10-08
  **UpdateTable with only TableName -> ValidationException 'At least one of ... is required'; GlobalSecondaryIndexUpdates=[] counts as absent**
  UpdateTable(TableName) on an ACTIVE table: ValidationException 'At least one of ProvisionedThroughput,
  BillingMode, UpdateStreamEnabled, GlobalSecondaryIndexUpdates, SSESpecification, ReplicaUpdates,
  MultiAccountReplicaReady, ReplicaTransitRoleArn, MultiRegionConsistency, DeletionProtectionEnabled,
  OnDemandThroughput, WarmThroughput or TableClass is required'. UpdateTable(TableName,
  GlobalSecondaryIndexUpdates=[]): ValidationException 'At least one of ProvisionedThroughput, BillingMode,
  UpdateStreamEnabled, GlobalSecondaryIndexUpdates, SSESpecification, ReplicaUpdates,
  MultiAccountReplicaReady, ReplicaTransitRoleArn, MultiRegionConsistency, DeletionProtectionEnabled,
  OnDemandThroughput, WarmThroughput or TableClass is required'. UpdateTable(TableName, ReplicaUpdates=[]):
  ParamValidationError 'Parameter validation failed:
  Invalid length for parameter ReplicaUpdates, value: 0, valid min length: 1'. On a missing table
  UpdateTable(TableName) -> ValidationException (existence vs validation order).
  - ACK: custom_update, compare.is_ignored+delta_pre_compare · ops: UpdateTable
  - repro: UpdateTable(TableName=<active table>) with no other fields
  - handling: not handled in the controller (as of commit 34b85e6)
  - related: [DDB-TABLE-160](../table-indexes.md#ddb-table-160), [DDB-TABLE-046](../table-indexes.md#ddb-table-046), [DDB-TABLE-047](../table-throughput-billing.md#ddb-table-047), [DDB-TABLE-437](../table-throughput-billing.md#ddb-table-437), [DDB-TABLE-449](../service.md#ddb-table-449) · evidence:
    table/error-taxonomy/missing-table-noop-update-dp

## Notes

H-T-029 confirmed.

Contradiction with [DDB-TABLE-160](../table-indexes.md#ddb-table-160): per 359's notes: 015 says UpdateTable GlobalSecondaryIndexUpdates=[] is
treated as absent ('At least one of ...' ValidationException), 160 says it is rejected as 'List of
GlobalSecondaryIndexUpdates is empty'; 359 reconciles: absent only for the at-least-one-parameter check,
rejected (and the carrier change NOT applied) once any effective member is present Resolution: keep both; 359
is canonical
