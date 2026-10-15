<!-- generated: ack-api-quirks dynamodb 2026-10-09 model=2012-08-10 controller=34b85e6 -->
# DDB-TABLE-265: One Create/Delete replica action per UpdateTable call; a Create may be issued at any member's endpoint; Replicas[] order differs per region
_Full entry and notes of one finding; its summary entry is in [table-replicas.md](../table-replicas.md).
Generated from ack-api-quirks `services/dynamodb` (model 2012-08-10, controller commit 34b85e6); edit the
finding in the lab, not here._

## Finding

- <a id="ddb-table-265"></a>**DDB-TABLE-265** `update-granularity` · impact high · handled · verified 2026-10-09
  **One Create/Delete replica action per UpdateTable call; a Create may be issued at any member's endpoint; Replicas[] order differs per region**
  ReplicaUpdates=[Create us-east-1, Create eu-west-1] in one call -> ValidationException 'Update table
  operation with more than one create or delete replica actions not allowed' (same message for two Delete
  actions). Sequential: Create us-east-1 -> 200; Create eu-west-1 2s later (table UPDATING, replica CREATING)
  -> ResourceInUseException. Once ACTIVE, Create eu-west-1 issued at the us-east-1 endpoint -> 200 (control is
  symmetric; the endpoint region becomes the 'source' of the new replica, see the 24h-source finding).
  3-region views: Replicas[] order is creation order as seen from each region and differs per region -
  us-west-2: [us-east-1, eu-west-1]; us-east-1: [us-west-2, eu-west-1]; eu-west-1: [us-west-2, us-east-1];
  DescribeTableReplicaAutoScaling from the base lists [eu-west-1, us-east-1, us-west-2] (alphabetical, home
  included). Each region has its own TableId/CreationDateTime; GlobalTableVersion 2019.11.21 everywhere; the
  single item was readable in all three regions.
  - ACK: one-per-reconcile, custom_update, compare.is_ignored+delta_pre_compare · ops: UpdateTable,
    DescribeTable · fields: ReplicaUpdates, Replicas
  - repro: UpdateTable ReplicaUpdates=[{Create:us-east-1},{Create:eu-west-1}]; DescribeTable in all three
    regions
  - measurements: us_east_1_create_total_s_1_item=660.0, eu_west_1_create_total_s_1_item=580.0
  - handling: handled via `generator.yaml:104-109; pkg/resource/table/hooks.go:72-93; generator.yaml:27-31; pkg/resource/table/hooks_replica_updates.go:277-373; pkg/resource/table/hooks_replica_updates.go:465-476; pkg/resource/table/hooks.go:89-92; pkg/resource/table/hooks.go:720-727; pkg/resource/table/hooks_replica_updates.go:413-418; test/e2e/tests/test_table_replicas.py:200-206; test/e2e/tests/test_table.py:34`
  - related: [DDB-TABLE-200](../table-replicas.md#ddb-table-200), [DDB-TABLE-224](../table-replicas.md#ddb-table-224), [DDB-TABLE-258](../table-global-tables.md#ddb-table-258), [DDB-TABLE-223](../table-replicas.md#ddb-table-223), [DDB-TABLE-257](../table-global-tables.md#ddb-table-257), [DDB-TABLE-226](../table-replicas.md#ddb-table-226),
    [DDB-TABLE-306](../table-policy-kinesis-autoscaling.md#ddb-table-306), [DDB-TABLE-308](../table-replicas.md#ddb-table-308), [DDB-TABLE-309](../table-replicas.md#ddb-table-309), [DDB-TABLE-329](../table-replicas.md#ddb-table-329), [DDB-TABLE-187](../table-replicas.md#ddb-table-187), [DDB-TABLE-305](../table-replicas.md#ddb-table-305), [DDB-TABLE-249](../table-replicas.md#ddb-table-249),
    [DDB-TABLE-266](../table-replicas.md#ddb-table-266), [DDB-TABLE-262](../table-replicas.md#ddb-table-262), [DDB-TABLE-267](../table-replicas.md#ddb-table-267), [DDB-TABLE-297](../table-replicas.md#ddb-table-297), [DDB-TABLE-260](../table-global-tables.md#ddb-table-260), [DDB-TABLE-296](../table-replicas.md#ddb-table-296), [DDB-TABLE-251](../table-replicas.md#ddb-table-251),
    [DDB-TABLE-294](../table-replicas.md#ddb-table-294), [DDB-TABLE-295](../table-replicas.md#ddb-table-295), [DDB-TABLE-322](../table-policy-kinesis-autoscaling.md#ddb-table-322), [DDB-TABLE-232](../table-replicas.md#ddb-table-232), [DDB-TABLE-189](../table-policy-kinesis-autoscaling.md#ddb-table-189), [DDB-TABLE-229](../table-global-tables.md#ddb-table-229) · hypotheses:
    H-R-006, H-R-007, H-R-031 · evidence: table/cross-region/multi-replica-delete-semantics

## Notes

Confirms the one-action-per-call rule (H-R-006 vocabulary) and H-R-031 (order differs per region -> diff
Replicas by RegionName as a set). Two IDENTICAL Create actions for the same region in one call are
de-duplicated and accepted (table/error-taxonomy/replica-sync-validation). Both replica creations took ~10-11
min because the table held one item (empty tables: ~25s).

Contradiction with [DDB-TABLE-329](../table-replicas.md#ddb-table-329), [DDB-TABLE-226](../table-replicas.md#ddb-table-226), [DDB-TABLE-306](../table-policy-kinesis-autoscaling.md#ddb-table-306): 329 describes an 'Empty' table whose replica
took 617 s to become ACTIVE, while 226/306/265 establish ~15-50 s for empty tables and ~10-11 min for tables
holding an item Resolution: 329's 'Empty' is wrong: probe.py:143 PutItem {pk:'one'} at 00:35:59 precedes the
ReplicaUpdates.Create at 00:36:01 (evidence rows 100/104); 617 s matches the 1-item slow path of 306 (686 s)
and 265 (580-660 s). No contradiction with 226
