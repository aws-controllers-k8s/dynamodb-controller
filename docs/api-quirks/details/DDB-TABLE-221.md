<!-- generated: ack-api-quirks dynamodb 2026-10-09 model=2012-08-10 controller=34b85e6 -->
# DDB-TABLE-221: ReplicaUpdates.Create on a table WITHOUT streams is accepted; DynamoDB silently enables a NEW_AND_OLD_IMAGES stream
_Full entry and notes of one finding; its summary entry is in [table-replicas.md](../table-replicas.md).
Generated from ack-api-quirks `services/dynamodb` (model 2012-08-10, controller commit 34b85e6); edit the
finding in the lab, not here._

## Finding

- <a id="ddb-table-221"></a>**DDB-TABLE-221** `prerequisite` · impact high · handled · verified 2026-10-09
  **ReplicaUpdates.Create on a table WITHOUT streams is accepted; DynamoDB silently enables a NEW_AND_OLD_IMAGES stream**
  Four PAY_PER_REQUEST tables created with no StreamSpecification: UpdateTable ReplicaUpdates=[Create
  us-east-1] returned 200 on all of them (latency 2-11s); the UpdateTable response already carries
  StreamSpecification{StreamEnabled:true, StreamViewType:NEW_AND_OLD_IMAGES}, LatestStreamArn and
  GlobalTableVersion=2019.11.21 while Replicas is absent/empty. The replica region table also reports a
  NEW_AND_OLD_IMAGES stream with its own LatestStreamArn. The stream is NOT removed when the last replica is
  deleted (StreamSpecification stays enabled; see D.final_regional_view: StreamSpecification absent in the
  regional view but LatestStreamArn/LatestStreamLabel still present after replica removal + stream disable).
  - ACK: compare.is_ignored+delta_pre_compare, custom_update, late_initialize · ops: UpdateTable,
    DescribeTable · fields: ReplicaUpdates, StreamSpecification, LatestStreamArn
  - repro: CreateTable PPR without StreamSpecification -> UpdateTable ReplicaUpdates=[Create us-east-1] ->
    DescribeTable
  - handling: handled via `pkg/resource/table/hooks.go:292-299; pkg/resource/table/hooks_replica_updates.go:265-275`
  - related: [DDB-TABLE-305](../table-replicas.md#ddb-table-305), [DDB-TABLE-222](../table-replicas.md#ddb-table-222), [DDB-TABLE-231](../table-global-tables.md#ddb-table-231), [DDB-TABLE-204](../table-replicas.md#ddb-table-204), [DDB-TABLE-297](../table-replicas.md#ddb-table-297), [DDB-TABLE-224](../table-replicas.md#ddb-table-224),
    [DDB-TABLE-261](../table-global-tables.md#ddb-table-261), [DDB-TABLE-250](../table-replicas.md#ddb-table-250), [DDB-TABLE-294](../table-replicas.md#ddb-table-294), [DDB-TABLE-296](../table-replicas.md#ddb-table-296), [DDB-TABLE-226](../table-replicas.md#ddb-table-226), [DDB-TABLE-309](../table-replicas.md#ddb-table-309) · hypotheses:
    H-R-002, H-R-003 · evidence: table/error-taxonomy/replica-sync-validation

## Notes

Refutes H-R-002 (no synchronous stream prerequisite), confirms contrarian H-R-003. The
KEYS_ONLY/NEW_IMAGE/OLD_IMAGE scenarios in this run were invalid (script bug: tables were created without the
stream) and are re-tested in table/dependencies/replica-prerequisites. Controller impact: a spec with replicas
and no streamSpecification will observe a server-added stream (drift) that cannot be disabled while replicas
exist.
