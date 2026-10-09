<!-- generated: ack-api-quirks dynamodb 2026-10-09 model=2012-08-10 controller=34b85e6 -->
# DDB-TABLE-348: DeleteTable within ~1-2s of a policy Put/Delete -> ResourceInUseException (has a pending resource-based policy update) while ACTIVE
_Full entry and notes of one finding; its summary entry is in
[table-policy-kinesis-autoscaling.md](../table-policy-kinesis-autoscaling.md). Generated from ack-api-quirks
`services/dynamodb` (model 2012-08-10, controller commit 34b85e6); edit the finding in the lab, not here._

## Finding

- <a id="ddb-table-348"></a>**DDB-TABLE-348** `async-state-machine` · impact high · unhandled (not handled in controller) · verified 2026-10-09
  **DeleteTable within ~1-2s of a policy Put/Delete -> ResourceInUseException (has a pending resource-based policy update) while ACTIVE**
  TableStatus was ACTIVE, yet DeleteTable 0.2 s after PutResourcePolicy failed with ResourceInUseException
  (HTTP 400) 'Attempt to change a resource which is still in use: Table: ackq-b044e1-srs-fu0 has a pending
  resource-based policy update.'; retries at 0.5 s failed too and the delete was accepted at 1.042 s. After
  DeleteResourcePolicy the block lasted until 2.04 s (ResourceInUseException->OK).
  UpdateTable(DeletionProtectionEnabled) 0.2 s after a Put -> OK; TagResource 0.2 s after a Put -> OK (both
  admitted). A no-op equivalent re-Put (same RevisionId) does not open the window: DeleteTable 0.2 s later ->
  OK.
  - ACK: requeue, custom_delete, terminal_codes · ops: DeleteTable, PutResourcePolicy, DeleteResourcePolicy,
    UpdateTable, TagResource · fields: ResourcePolicy, TableStatus
  - repro: ACTIVE table: PutResourcePolicy; DeleteTable at 0.2, 0.5, 1.0 ... s until 200; repeat after
    DeleteResourcePolicy; repeat with UpdateTable/TagResource; repeat after a no-op re-Put
  - measurements: delete_blocked_after_put_s=1.042, delete_blocked_after_policy_delete_s=2.04
  - handling: not handled in the controller (as of commit 34b85e6)
  - related: [DDB-TABLE-206](../table-policy-kinesis-autoscaling.md#ddb-table-206), [DDB-TABLE-209](../table-policy-kinesis-autoscaling.md#ddb-table-209), [DDB-TABLE-244](../table-policy-kinesis-autoscaling.md#ddb-table-244), [DDB-TABLE-245](../table-policy-kinesis-autoscaling.md#ddb-table-245), [DDB-TABLE-246](../table-policy-kinesis-autoscaling.md#ddb-table-246), [DDB-TABLE-248](../table-policy-kinesis-autoscaling.md#ddb-table-248),
    [DDB-TABLE-205](../table-policy-kinesis-autoscaling.md#ddb-table-205), [DDB-TABLE-069](../table-streams-encryption-class.md#ddb-table-069), [DDB-TABLE-122](../table-policy-kinesis-autoscaling.md#ddb-table-122), [DDB-TABLE-236](../table-policy-kinesis-autoscaling.md#ddb-table-236), [DDB-TABLE-247](../table-policy-kinesis-autoscaling.md#ddb-table-247), [DDB-TABLE-347](../table-policy-kinesis-autoscaling.md#ddb-table-347), [DDB-TABLE-464](../service.md#ddb-table-464),
    [DDB-TABLE-445](../service.md#ddb-table-445), [DDB-TABLE-213](../table-streams-encryption-class.md#ddb-table-213), [DDB-TABLE-172](../service.md#ddb-table-172), [DDB-TABLE-119](../table-streams-encryption-class.md#ddb-table-119), [DDB-TABLE-210](../table-policy-kinesis-autoscaling.md#ddb-table-210), [DDB-TABLE-432](../table-policy-kinesis-autoscaling.md#ddb-table-432) · hypotheses:
    H-S-027, H-S-007 · evidence: table/consistency-windows/policy-stale-read-sequence

## Notes

Found by accident when the probe's cleanup DeleteTable failed right after the last policy write. Mirrors
[DDB-TABLE-069](../table-streams-encryption-class.md#ddb-table-069) ('ACTIVE does not mean deletable'): a controller that writes the policy and deletes the table in
the same reconcile (or a finalizer that runs right after a policy sync) must retry on this
ResourceInUseException rather than treat it as a conflict.
