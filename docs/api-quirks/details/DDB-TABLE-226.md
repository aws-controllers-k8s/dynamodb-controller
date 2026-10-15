<!-- generated: ack-api-quirks dynamodb 2026-10-09 model=2012-08-10 controller=34b85e6 -->
# DDB-TABLE-226: Replica Create on an EMPTY table completes in ~20-30s; the UpdateTable response shows Replicas=[] (entry appears ~6s later)
_Full entry and notes of one finding; its summary entry is in [table-replicas.md](../table-replicas.md).
Generated from ack-api-quirks `services/dynamodb` (model 2012-08-10, controller commit 34b85e6); edit the
finding in the lab, not here._

## Finding

- <a id="ddb-table-226"></a>**DDB-TABLE-226** `async-state-machine` · impact high · handled · verified 2026-10-09
  **Replica Create on an EMPTY table completes in ~20-30s; the UpdateTable response shows Replicas=[] (entry appears ~6s later)**
  UpdateTable ReplicaUpdates=[Create us-east-1] on an empty PPR table returned after 2.0s with
  TableStatus=UPDATING, GlobalTableVersion=2019.11.21 and Replicas=[] (empty list, no CREATING entry yet).
  Polling both regions every 3s: A=UPDATING with Replicas=[] for 6.4s; then A Replicas=[us-east-1 CREATING]
  while us-east-1 DescribeTable still returned ResourceNotFoundException for 3.4s more; us-east-1 appeared at
  10.1s as TableStatus=CREATING with Replicas=[us-west-2 ACTIVE] and GlobalTableVersion already set; both
  regions ACTIVE at 20.9s (total 28s incl. the call). ReplicaStatusPercentProgress was never present. A later
  re-add of the same region took 23s. Non-empty tables behave very differently (see
  table/dependencies/replica-prerequisites and mutation-matrix/replica-overrides: with a single item the
  replica stays CREATING for many minutes while the base returns to ACTIVE).
  - ACK: synced.when, requeue, e2e-timing · ops: UpdateTable, DescribeTable · fields: ReplicaUpdates,
    Replicas, TableStatus
  - repro: PPR table with NEW_AND_OLD_IMAGES stream -> UpdateTable ReplicaUpdates=[Create us-east-1] -> poll
    DescribeTable in both regions every 3s
  - measurements: create_total_s=28.0, update_table_latency_ms=2046, replicas_entry_appears_s=6.7,
    replica_region_visible_s=10.1, readd_total_s=23.0
  - handling: handled via `generator.yaml:27-31; pkg/resource/table/hooks_replica_updates.go:277-373; pkg/resource/table/hooks_replica_updates.go:465-476; pkg/resource/table/hooks.go:89-92; test/e2e/tests/test_table_replicas.py:200-206; test/e2e/tests/test_table.py:34`
  - related: [DDB-TABLE-306](../table-policy-kinesis-autoscaling.md#ddb-table-306), [DDB-TABLE-265](../table-replicas.md#ddb-table-265), [DDB-TABLE-308](../table-replicas.md#ddb-table-308), [DDB-TABLE-309](../table-replicas.md#ddb-table-309), [DDB-TABLE-329](../table-replicas.md#ddb-table-329), [DDB-TABLE-187](../table-replicas.md#ddb-table-187),
    [DDB-TABLE-305](../table-replicas.md#ddb-table-305), [DDB-TABLE-249](../table-replicas.md#ddb-table-249), [DDB-TABLE-258](../table-global-tables.md#ddb-table-258), [DDB-TABLE-204](../table-replicas.md#ddb-table-204), [DDB-TABLE-261](../table-global-tables.md#ddb-table-261), [DDB-TABLE-250](../table-replicas.md#ddb-table-250), [DDB-TABLE-294](../table-replicas.md#ddb-table-294),
    [DDB-TABLE-296](../table-replicas.md#ddb-table-296), [DDB-TABLE-221](../table-replicas.md#ddb-table-221) · hypotheses: H-R-005, H-R-008 · evidence:
    table/state-machine/replica-create-timeline

## Notes

Qualifies H-R-005 (durations are seconds for an empty table; the Replicas entry lags the 200 by ~6s so a
controller reading back immediately sees no replica at all) and H-R-008 (replica region NotFound window ~10s;
A's Replicas[] is the earlier signal by ~3s).

Contradiction with [DDB-TABLE-329](../table-replicas.md#ddb-table-329), [DDB-TABLE-306](../table-policy-kinesis-autoscaling.md#ddb-table-306), [DDB-TABLE-265](../table-replicas.md#ddb-table-265): 329 describes an 'Empty' table whose replica
took 617 s to become ACTIVE, while 226/306/265 establish ~15-50 s for empty tables and ~10-11 min for tables
holding an item Resolution: 329's 'Empty' is wrong: probe.py:143 PutItem {pk:'one'} at 00:35:59 precedes the
ReplicaUpdates.Create at 00:36:01 (evidence rows 100/104); 617 s matches the 1-item slow path of 306 (686 s)
and 265 (580-660 s). No contradiction with 226
