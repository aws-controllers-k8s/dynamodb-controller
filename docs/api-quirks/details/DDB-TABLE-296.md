<!-- generated: ack-api-quirks dynamodb 2026-10-09 model=2012-08-10 controller=34b85e6 -->
# DDB-TABLE-296: GSI add/delete with a replica fans out to every region; a GSI add issued at the replica endpoint is accepted and propagates back
_Full entry and notes of one finding; its summary entry is in [table-replicas.md](../table-replicas.md).
Generated from ack-api-quirks `services/dynamodb` (model 2012-08-10, controller commit 34b85e6); edit the
finding in the lab, not here._

## Finding

- <a id="ddb-table-296"></a>**DDB-TABLE-296** `cross-region` · impact high · unhandled (not handled in controller) · verified 2026-10-09
  **GSI add/delete with a replica fans out to every region; a GSI add issued at the replica endpoint is accepted and propagates back**
  UpdateTable GlobalSecondaryIndexUpdates=[Create gsi1] on the base (1-item table) -> 200 (response: GSIs=[],
  Replicas unchanged). Timeline: both regions TableStatus UPDATING for ~31s; gsi1 appears CREATING in
  us-east-1 at 31s and in A.Replicas[].GlobalSecondaryIndexes (IndexName + WarmThroughput only) at 58s; base
  back to ACTIVE at ~89s while both indexes kept backfilling; us-east-1's gsi1 ACTIVE at ~545s, base's at
  575s. During the GSI create: ReplicaUpdates Update/Create -> ResourceInUseException. UpdateTable Create gsi2
  issued at the us-east-1 endpoint -> 200 and gsi2 appeared in the base (542s total). UpdateTable Delete gsi1
  on the base -> 200, removed from both regions in ~38s; AttributeDefinitions converge in both regions.
  - ACK: custom_update, synced.when, compare.is_ignored+delta_pre_compare · ops: UpdateTable, DescribeTable ·
    fields: GlobalSecondaryIndexUpdates, GlobalSecondaryIndexes, Replicas.GlobalSecondaryIndexes
  - repro: table with replica -> UpdateTable GlobalSecondaryIndexUpdates Create -> poll both regions
  - measurements: gsi1_total_s=575.4, gsi2_from_replica_total_s=541.5, gsi_delete_total_s=38.0
  - handling: not handled in the controller (as of commit 34b85e6)
  - related: [DDB-TABLE-306](../table-policy-kinesis-autoscaling.md#ddb-table-306), [DDB-TABLE-200](../table-replicas.md#ddb-table-200), [DDB-TABLE-263](../table-global-tables.md#ddb-table-263), [DDB-TABLE-315](../table-policy-kinesis-autoscaling.md#ddb-table-315), [DDB-TABLE-327](../table-replicas.md#ddb-table-327), [DDB-TABLE-314](../table-policy-kinesis-autoscaling.md#ddb-table-314),
    [DDB-TABLE-251](../table-replicas.md#ddb-table-251), [DDB-TABLE-265](../table-replicas.md#ddb-table-265), [DDB-TABLE-294](../table-replicas.md#ddb-table-294), [DDB-TABLE-295](../table-replicas.md#ddb-table-295), [DDB-TABLE-297](../table-replicas.md#ddb-table-297), [DDB-TABLE-322](../table-policy-kinesis-autoscaling.md#ddb-table-322), [DDB-TABLE-204](../table-replicas.md#ddb-table-204),
    [DDB-TABLE-261](../table-global-tables.md#ddb-table-261), [DDB-TABLE-250](../table-replicas.md#ddb-table-250), [DDB-TABLE-226](../table-replicas.md#ddb-table-226), [DDB-TABLE-309](../table-replicas.md#ddb-table-309), [DDB-TABLE-221](../table-replicas.md#ddb-table-221) · hypotheses: H-R-017, H-R-006 ·
    evidence: table/mutation-matrix/replica-overrides

## Notes

Confirms H-R-017: schema is group-wide and symmetric (no primary region). The base returns to ACTIVE while
replica indexes are still CREATING, so readiness must consider every region's IndexStatus
(A.Replicas[].GlobalSecondaryIndexes carries no IndexStatus). GSI backfill on a 1-item table with a replica
took ~9.5 min vs ~1 min regional.
