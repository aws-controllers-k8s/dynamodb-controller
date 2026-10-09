<!-- generated: ack-api-quirks dynamodb 2026-10-09 model=2012-08-10 controller=34b85e6 -->
# DDB-TABLE-306: Genuine CREATING window (1-item table, ~11 min): every UpdateTable incl. cancel-Delete -> ResourceInUseException; UpdateTimeToLive blocked
_Full entry and notes of one finding; its summary entry is in
[table-policy-kinesis-autoscaling.md](../table-policy-kinesis-autoscaling.md). Generated from ack-api-quirks
`services/dynamodb` (model 2012-08-10, controller commit 34b85e6); edit the finding in the lab, not here._

## Finding

- <a id="ddb-table-306"></a>**DDB-TABLE-306** `async-state-machine` · impact high · handled · verified 2026-10-09
  **Genuine CREATING window (1-item table, ~11 min): every UpdateTable incl. cancel-Delete -> ResourceInUseException; UpdateTimeToLive blocked**
  1-item PPR table: UpdateTable Create us-east-1 returned in 2s; Replicas[] showed CREATING at ~27s; us-east-1
  DescribeTable became non-NotFound at ~61s; base went ACTIVE[us-east-1 CREATING] at 27s, back to
  UPDATING[CREATING] at 332s, both ACTIVE at 641s (total 686s vs ~25s for an empty table;
  ReplicaStatusPercentProgress never populated). Ops issued at ~35s while Replicas[]=CREATING: ReplicaUpdates
  Update (TableClassOverride) / Create eu-west-1 / DeletionProtectionEnabled / Delete us-east-1 (cancel) ->
  ResourceInUseException 'The resource which you are attempting to change is in use.'; duplicate Create
  us-east-1 -> ResourceInUseException 'Global table with name: <name> already exists with replicas in regions:
  us-east-1, us-west-2.'; UpdateTimeToLive -> ValidationException 'Create/Update/Delete of replica is not
  allowed while the replica is being added to table with name: <name> in region ...' (TTL is group-wide, so it
  is blocked, unlike the empty-table case where it was accepted before the entry appeared); TagResource and
  PutItem -> 200; DescribeTableReplicaAutoScaling (base) -> ResourceNotFoundException 'Requested resource not
  found: Table: <name> not found (Service: AmazonDynamoDBv2 ...)' - it fans out to the not-yet-existing
  replica table. All us-east-1 calls at that moment -> ResourceNotFoundException (table not visible yet). Item
  written to the base during CREATING was present in the replica afterwards; tags were not copied.
  - ACK: updateable.when, deletable.when, requeue, e2e-timing · ops: UpdateTable, DeleteTable, TagResource,
    UpdateTimeToLive, PutItem, GetItem, DescribeTable, DescribeTableReplicaAutoScaling · fields:
    ReplicaUpdates, Replicas.ReplicaStatus, Replicas.ReplicaStatusPercentProgress, TableStatus
  - repro: PPR table + PutItem -> UpdateTable ReplicaUpdates=[Create us-east-1] -> wait for
    Replicas[].ReplicaStatus=CREATING -> issue each op
  - measurements: creating_visible_after_s=27.0, replica_region_visible_after_s=61.0,
    one_item_create_total_s=686.0
  - handling: handled via `generator.yaml:104-109; pkg/resource/table/hooks.go:72-93; pkg/resource/table/hooks_replica_updates.go:465-476; pkg/resource/table/hooks.go:89-92; templates/hooks/table/sdk_create_post_set_output.go.tpl:1-4; bcd26e1; test/e2e/tests/test_table_replicas.py:200-206; test/e2e/tests/test_table.py:34`
  - related: [DDB-TABLE-187](../table-replicas.md#ddb-table-187), [DDB-TABLE-232](../table-replicas.md#ddb-table-232), [DDB-TABLE-323](../table-policy-kinesis-autoscaling.md#ddb-table-323), [DDB-TABLE-226](../table-replicas.md#ddb-table-226), [DDB-TABLE-265](../table-replicas.md#ddb-table-265), [DDB-TABLE-308](../table-replicas.md#ddb-table-308),
    [DDB-TABLE-309](../table-replicas.md#ddb-table-309), [DDB-TABLE-329](../table-replicas.md#ddb-table-329), [DDB-TABLE-305](../table-replicas.md#ddb-table-305), [DDB-TABLE-249](../table-replicas.md#ddb-table-249), [DDB-TABLE-258](../table-global-tables.md#ddb-table-258), [DDB-TABLE-200](../table-replicas.md#ddb-table-200), [DDB-TABLE-296](../table-replicas.md#ddb-table-296),
    [DDB-TABLE-263](../table-global-tables.md#ddb-table-263), [DDB-TABLE-315](../table-policy-kinesis-autoscaling.md#ddb-table-315), [DDB-TABLE-327](../table-replicas.md#ddb-table-327), [DDB-TABLE-314](../table-policy-kinesis-autoscaling.md#ddb-table-314) · hypotheses: H-R-005, H-R-006, H-R-026,
    H-R-024, H-R-101 · evidence: table/dependencies/replica-prerequisites

## Notes

Confirms H-R-005 for non-empty tables (minutes, base flips ACTIVE->UPDATING->ACTIVE during the add) and
H-R-006 (ResourceInUseException for every other UpdateTable; TTL also blocked - partial refutation of
'UpdateTimeToLive succeeds'). Confirms H-R-026 part 1: a replica Create cannot be cancelled
(ResourceInUseException). DescribeTableReplicaAutoScaling is not a usable status source during CREATING
(H-R-101/H-R-114).

Contradiction with [DDB-TABLE-329](../table-replicas.md#ddb-table-329), [DDB-TABLE-226](../table-replicas.md#ddb-table-226), [DDB-TABLE-265](../table-replicas.md#ddb-table-265): 329 describes an 'Empty' table whose replica
took 617 s to become ACTIVE, while 226/306/265 establish ~15-50 s for empty tables and ~10-11 min for tables
holding an item Resolution: 329's 'Empty' is wrong: probe.py:143 PutItem {pk:'one'} at 00:35:59 precedes the
ReplicaUpdates.Create at 00:36:01 (evidence rows 100/104); 617 s matches the 1-item slow path of 306 (686 s)
and 265 (580-660 s). No contradiction with 226
