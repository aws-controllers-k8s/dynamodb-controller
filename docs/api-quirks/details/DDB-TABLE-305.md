<!-- generated: ack-api-quirks dynamodb 2026-10-09 model=2012-08-10 controller=34b85e6 -->
# DDB-TABLE-305: No stream view-type prerequisite: replicas are created on KEYS_ONLY/NEW_IMAGE/OLD_IMAGE streams unchanged; a disabled stream is re-enabled
_Full entry and notes of one finding; its summary entry is in [table-replicas.md](../table-replicas.md).
Generated from ack-api-quirks `services/dynamodb` (model 2012-08-10, controller commit 34b85e6); edit the
finding in the lab, not here._

## Finding

- <a id="ddb-table-305"></a>**DDB-TABLE-305** `prerequisite` · impact high · handled · verified 2026-10-09
  **No stream view-type prerequisite: replicas are created on KEYS_ONLY/NEW_IMAGE/OLD_IMAGE streams unchanged; a disabled stream is re-enabled**
  Tables created with StreamSpecification KEYS_ONLY, NEW_IMAGE and OLD_IMAGE: UpdateTable
  ReplicaUpdates=[Create us-east-1] -> 200 on all three (~2s), replica ACTIVE after 14-18s, and the
  StreamViewType is left unchanged in the base AND copied as-is to the replica (us-east-1 reports the same
  KEYS_ONLY/NEW_IMAGE/OLD_IMAGE spec with its own LatestStreamArn). A table whose stream was enabled then
  disabled (StreamSpecification absent, stale LatestStreamLabel still present): Create -> 200 and the response
  already shows StreamSpecification{true, NEW_AND_OLD_IMAGES} with a NEW LatestStreamLabel; the replica gets
  NEW_AND_OLD_IMAGES too (ACTIVE after 34s).
  - ACK: custom_update, compare.is_ignored+delta_pre_compare, terminal_codes · ops: UpdateTable, DescribeTable
    · fields: ReplicaUpdates, StreamSpecification.StreamViewType
  - repro: CreateTable with StreamSpecification{KEYS_ONLY} -> UpdateTable ReplicaUpdates=[Create us-east-1] ->
    DescribeTable
  - measurements: replica_active_s_keys_only=14.4, replica_active_s_new_image=14.6,
    replica_active_s_old_image=17.8, replica_active_s_disabled_stream=33.6
  - handling: handled via `pkg/resource/table/hooks.go:292-299; pkg/resource/table/hooks_replica_updates.go:265-275; pkg/resource/table/hooks_replica_updates.go:465-476; pkg/resource/table/hooks.go:89-92`
  - related: [DDB-TABLE-226](../table-replicas.md#ddb-table-226), [DDB-TABLE-306](../table-policy-kinesis-autoscaling.md#ddb-table-306), [DDB-TABLE-265](../table-replicas.md#ddb-table-265), [DDB-TABLE-308](../table-replicas.md#ddb-table-308), [DDB-TABLE-309](../table-replicas.md#ddb-table-309), [DDB-TABLE-329](../table-replicas.md#ddb-table-329),
    [DDB-TABLE-187](../table-replicas.md#ddb-table-187), [DDB-TABLE-249](../table-replicas.md#ddb-table-249), [DDB-TABLE-258](../table-global-tables.md#ddb-table-258), [DDB-TABLE-221](../table-replicas.md#ddb-table-221), [DDB-TABLE-222](../table-replicas.md#ddb-table-222), [DDB-TABLE-231](../table-global-tables.md#ddb-table-231), [DDB-TABLE-204](../table-replicas.md#ddb-table-204),
    [DDB-TABLE-297](../table-replicas.md#ddb-table-297), [DDB-TABLE-224](../table-replicas.md#ddb-table-224) · hypotheses: H-R-002, H-R-003 · evidence:
    table/dependencies/replica-prerequisites

## Notes

Refutes H-R-002 completely (no synchronous stream prerequisite of any kind) and confirms contrarian H-R-003
only for the no-stream/disabled case (server enables NEW_AND_OLD_IMAGES). Combined with H-R-004 (stream
immutable while replicas exist) a spec with replicas + a KEYS_ONLY stream is stable, while a spec with
replicas and no stream permanently shows a server-added NEW_AND_OLD_IMAGES stream.
