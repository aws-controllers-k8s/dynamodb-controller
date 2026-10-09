<!-- generated: ack-api-quirks dynamodb 2026-10-09 model=2012-08-10 controller=34b85e6 -->
# DDB-TABLE-312: Replica Create with an already-disabled replica-region KMS key is rejected synchronously (SSE 'UPDATING state' message)
_Full entry and notes of one finding; its summary entry is in [table-replicas.md](../table-replicas.md).
Generated from ack-api-quirks `services/dynamodb` (model 2012-08-10, controller commit 34b85e6); edit the
finding in the lab, not here._

## Finding

- <a id="ddb-table-312"></a>**DDB-TABLE-312** `async-state-machine` · impact high · unhandled (not handled in controller) · verified 2026-10-09
  **Replica Create with an already-disabled replica-region KMS key is rejected synchronously (SSE 'UPDATING state' message)**
  Base table encrypted with a us-west-2 CMK; a us-east-1 CMK created and disabled beforehand. UpdateTable
  ReplicaUpdates=[Create us-east-1 KMSMasterKeyId=<disabled key ARN>] -> ValidationException (HTTP 400, 2.1s)
  'Operation cannot be performed while replica server-side encryption status is in UPDATING state. Please
  retry the request after the status is updated to ENABLED.' No replica entry is created; the table stays
  ACTIVE. The message suggests a transient state although the cause (disabled key) is permanent until
  EnableKey.
  - ACK: terminal_codes, requeue, custom_update · ops: UpdateTable, DescribeTable · fields:
    ReplicaUpdates.Create.KMSMasterKeyId, Replicas.ReplicaStatus, Replicas.ReplicaStatusDescription
  - repro: KMS CreateKey in us-east-1 + DisableKey; table with CMK SSE in us-west-2; UpdateTable
    ReplicaUpdates=[Create us-east-1 KMSMasterKeyId=<disabled key>]
  - handling: not handled in the controller (as of commit 34b85e6)
  - related: [DDB-TABLE-313](../table-replicas.md#ddb-table-313), [DDB-TABLE-309](../table-replicas.md#ddb-table-309), [DDB-TABLE-295](../table-replicas.md#ddb-table-295), [DDB-TABLE-328](../table-streams-encryption-class.md#ddb-table-328), [DDB-TABLE-310](../table-replicas.md#ddb-table-310), [DDB-TABLE-311](../table-replicas.md#ddb-table-311),
    [DDB-TABLE-329](../table-replicas.md#ddb-table-329), [DDB-TABLE-327](../table-replicas.md#ddb-table-327), [DDB-TABLE-442](../service.md#ddb-table-442), [DDB-TABLE-266](../table-replicas.md#ddb-table-266), [DDB-TABLE-267](../table-replicas.md#ddb-table-267) · hypotheses: H-R-023, H-R-016 ·
    evidence: table/state-machine/replica-creation-failed

## Notes

Qualifies H-R-023: with a key that is already disabled there is no asynchronous CREATION_FAILED path - the
check is synchronous but the error text is misleading ('retry after ... ENABLED'). The async case (key
disabled right after Create) is in table/state-machine/replica-kms-lifecycle (silent abort, no CREATION_FAILED
either). CREATION_FAILED could not be induced in this shard.

Contradiction with [DDB-TABLE-442](../service.md#ddb-table-442), [DDB-TABLE-311](../table-replicas.md#ddb-table-311), [DDB-TABLE-266](../table-replicas.md#ddb-table-266), [DDB-TABLE-267](../table-replicas.md#ddb-table-267): 442 classifies 'Operation
cannot be performed while replica server-side encryption status is in UPDATING state' as WAIT-FOR-STATE
ReplicaStatus and the 24 h source-region text as PERMANENT-QUOTA 24 h; 312/311 show the SSE text is returned
for a disabled KMS key with nothing visible changing in DescribeTable, and 266/267 show the 24 h text clears
as soon as the sourced replica is removed Resolution: keep 442 as the catalogue; 312/311 canonical for the SSE
text (external dependency: EnableKey, not a state wait) and 266/267 for the 24 h text (retry after replica
removal, not a quota window)
