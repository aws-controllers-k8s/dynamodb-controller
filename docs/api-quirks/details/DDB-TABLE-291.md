<!-- generated: ack-api-quirks dynamodb 2026-10-09 model=2012-08-10 controller=34b85e6 -->
# DDB-TABLE-291: UpdateTable GSI Create on a PROVISIONED global table is rejected while ANY existing GSI lacks write autoscaling
_Full entry and notes of one finding; its summary entry is in
[table-policy-kinesis-autoscaling.md](../table-policy-kinesis-autoscaling.md). Generated from ack-api-quirks
`services/dynamodb` (model 2012-08-10, controller commit 34b85e6); edit the finding in the lab, not here._

## Finding

- <a id="ddb-table-291"></a>**DDB-TABLE-291** `prerequisite` · impact high · unhandled (not handled in controller) · verified 2026-10-09
  **UpdateTable GSI Create on a PROVISIONED global table is rejected while ANY existing GSI lacks write autoscaling**
  UpdateTable(GlobalSecondaryIndexUpdates Create gsi1, PROVISIONED 1/1) on the 2-replica PROVISIONED table ->
  {"operation": "update_table", "ok": false, "code": "ValidationException", "http_status": 400, "message":
  "GSI write capacity should either be Pay-Per-Request or AutoScaled.", "latency_ms": 884, "client_side":
  false}. Nonexistent index in UpdateTableReplicaAutoScaling -> {"operation":
  "update_table_replica_auto_scaling", "ok": false, "code": "ResourceNotFoundException", "http_status": 400,
  "message": "Failed to update settings for global table with name: ‘ackq-cf966e-as-gsi’ because the global
  secondary indexes with names: ‘[nope]’ do not exist.", "latency_ms": 769, "client_side": false}.
  - ACK: custom_update, terminal_codes · ops: UpdateTable, UpdateTableReplicaAutoScaling · fields:
    GlobalSecondaryIndexUpdates
  - repro: global PROVISIONED table -> UpdateTable add GSI
  - handling: not handled in the controller (as of commit 34b85e6)
  - related: [DDB-TABLE-249](../table-replicas.md#ddb-table-249), [DDB-TABLE-288](../table-replicas.md#ddb-table-288), [DDB-TABLE-322](../table-policy-kinesis-autoscaling.md#ddb-table-322), [DDB-TABLE-290](../table-policy-kinesis-autoscaling.md#ddb-table-290), [DDB-TABLE-194](../table-replicas.md#ddb-table-194) · hypotheses: H-R-108 ·
    evidence: table/dependencies/autoscaling-gsi-and-orphans,
    table/creative/gsi-add-on-provisioned-global-table

## Notes

CORRECTED by table/creative/gsi-add-on-provisioned-global-table - the rejection was caused by the EXISTING
gsi0, whose write autoscaling had just been disabled via UpdateTableReplicaAutoScaling, not by the new GSI; on
a global table with no un-autoscaled GSI the same UpdateTable(GlobalSecondaryIndexUpdates Create) succeeded
(200). The message ('GSI write capacity should either be Pay-Per-Request or AutoScaled.') does not name the
offending index. Consequence for a Table controller - after disabling GSI write autoscaling on a provisioned
global table every subsequent GSI-related UpdateTable fails until autoscaling is restored or the table is
PAY_PER_REQUEST. AAS RegisterScalableTarget for a not-yet-existing index -> ValidationException 'DynamoDB
index does not exist', so pre-registration is impossible.

Contradiction with [DDB-TABLE-322](../table-policy-kinesis-autoscaling.md#ddb-table-322): 291 (dependencies run) recorded UpdateTable GSI Create on a PROVISIONED
global table -> ValidationException 'GSI write capacity should either be Pay-Per-Request or AutoScaled.'; 322
shows the same call succeeding and DynamoDB auto-registering write autoscaling for the new GSI Resolution:
already reconciled in 291's notes/title: the rejection was caused by the existing gsi0 whose write autoscaling
had just been disabled (290), and the message does not name the offending index. 322 canonical for the success
path; keep 291 for the 'any un-autoscaled GSI blocks every GSI UpdateTable' rule
