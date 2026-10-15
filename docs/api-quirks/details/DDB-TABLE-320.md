<!-- generated: ack-api-quirks dynamodb 2026-10-09 model=2012-08-10 controller=34b85e6 -->
# DDB-TABLE-320: GlobalTableSettingsReplicationMode absent by default on regional tables; appears as ENABLED_WITH_OVERRIDES once a replica exists
_Full entry and notes of one finding; its summary entry is in
[table-global-tables.md](../table-global-tables.md). Generated from ack-api-quirks `services/dynamodb` (model
2012-08-10, controller commit 34b85e6); edit the finding in the lab, not here._

## Finding

- <a id="ddb-table-320"></a>**DDB-TABLE-320** `server-default` · impact medium · unhandled (not handled in controller) · verified 2026-10-09
  **GlobalTableSettingsReplicationMode absent by default on regional tables; appears as ENABLED_WITH_OVERRIDES once a replica exists**
  DescribeTable on the regional PROVISIONED table had keys [AttributeDefinitions, BillingModeSummary,
  CreationDateTime, DeletionProtectionEnabled, ItemCount, KeySchema, LatestStreamArn, LatestStreamLabel,
  ProvisionedThroughput, StreamSpecification, TableArn, TableId, TableName, TableSizeBytes, TableStatus,
  WarmThroughput]. With one 2019.11.21 replica it gained
  GlobalTableSettingsReplicationMode=ENABLED_WITH_OVERRIDES (table level and under Replicas[]),
  GlobalTableVersion=2019.11.21 and Replicas, while BillingModeSummary disappeared. After the replica was
  removed GlobalTableVersion and Replicas were absent again.
  - ACK: is_read_only, compare.is_ignored+delta_pre_compare · ops: DescribeTable, UpdateTable · fields:
    GlobalTableSettingsReplicationMode, GlobalTableVersion, Replicas, BillingModeSummary
  - repro: DescribeTable regional -> UpdateTable ReplicaUpdates Create -> DescribeTable -> ReplicaUpdates
    Delete -> DescribeTable
  - handling: not handled in the controller (as of commit 34b85e6)
  - related: [DDB-TABLE-048](../table-global-tables.md#ddb-table-048), [DDB-TABLE-231](../table-global-tables.md#ddb-table-231), [DDB-TABLE-201](../table-global-tables.md#ddb-table-201), [DDB-TABLE-229](../table-global-tables.md#ddb-table-229), [DDB-TABLE-251](../table-replicas.md#ddb-table-251) · hypotheses: H-R-029 ·
    evidence: table/sub-resources/replica-autoscaling-facade

## Notes

Read-side of H-R-029 confirmed (never set by the caller). Setting it via UpdateTable was not attempted (other
shard).

Contradiction with [DDB-TABLE-048](../table-global-tables.md#ddb-table-048): 320: GlobalTableSettingsReplicationMode 'appears only while the table has
replicas; absent on regional tables'; 048: UpdateTable GlobalTableSettingsReplicationMode=ENABLED/DISABLED on
a regional table -> 200 and DescribeTable shows the changed path Resolution: not contradictory - 320 describes
the default (its notes say setting it was not attempted); 048 shows it is caller-settable on a regional table
while the server default ENABLED_WITH_OVERRIDES cannot be sent. Title fix for 320
