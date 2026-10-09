<!-- generated: ack-api-quirks dynamodb 2026-10-09 model=2012-08-10 controller=34b85e6 -->
# DDB-TABLE-311: Replica CMK disabled: INACCESSIBLE status lags (none within 25 min; ~80 min in 329); control plane works, replica-region data plane fails
_Full entry and notes of one finding; its summary entry is in [table-replicas.md](../table-replicas.md).
Generated from ack-api-quirks `services/dynamodb` (model 2012-08-10, controller commit 34b85e6); edit the
finding in the lab, not here._

## Finding

- <a id="ddb-table-311"></a>**DDB-TABLE-311** `async-state-machine` · impact medium · unhandled (not handled in controller) · verified 2026-10-09
  **Replica CMK disabled: INACCESSIBLE status lags (none within 25 min; ~80 min in 329); control plane works, replica-region data plane fails**
  KMS DisableKey on the us-east-1 replica's CMK; both regions polled every 30s for 1500s: TableStatus and
  ReplicaStatus stayed ACTIVE everywhere, SSEDescription.Status stayed ENABLED, no
  ReplicaInaccessibleDateTime. Meanwhile: PutItem in us-west-2 -> 200; PutItem/GetItem in us-east-1 ->
  ValidationException 'KMS key disabled error: ... DisabledException: <key arn> is disabled'; ReplicaUpdates
  Update{TableClassOverride} on that replica -> 200; DeletionProtection on base and on the replica -> 200;
  TagResource -> 200; Create eu-west-1 -> ValidationException 'Operation cannot be performed while replica
  server-side encryption status is in UPDATING state...'; re-Create us-east-1 -> 'already existed as tables'.
  After EnableKey everything was ACTIVE at once (nothing to recover).
  - ACK: synced.when, requeue, terminal_codes · ops: UpdateTable, DescribeTable, PutItem, GetItem, TagResource
    · fields: Replicas.ReplicaStatus, Replicas.ReplicaInaccessibleDateTime, TableStatus
  - repro: replica with its own CMK ACTIVE -> kms disable-key in the replica region -> poll DescribeTable in
    both regions every 30s (bounded 1500s)
  - measurements: inaccessible_watch_s=1500, inaccessible_detect_s=null
  - handling: not handled in the controller (as of commit 34b85e6)
  - related: [DDB-TABLE-297](../table-replicas.md#ddb-table-297), [DDB-TABLE-327](../table-replicas.md#ddb-table-327), [DDB-TABLE-263](../table-global-tables.md#ddb-table-263), [DDB-TABLE-329](../table-replicas.md#ddb-table-329), [DDB-TABLE-313](../table-replicas.md#ddb-table-313), [DDB-TABLE-309](../table-replicas.md#ddb-table-309),
    [DDB-TABLE-295](../table-replicas.md#ddb-table-295), [DDB-TABLE-328](../table-streams-encryption-class.md#ddb-table-328), [DDB-TABLE-312](../table-replicas.md#ddb-table-312), [DDB-TABLE-310](../table-replicas.md#ddb-table-310), [DDB-TABLE-442](../service.md#ddb-table-442), [DDB-TABLE-266](../table-replicas.md#ddb-table-266), [DDB-TABLE-267](../table-replicas.md#ddb-table-267) ·
    hypotheses: H-R-034 · evidence: table/state-machine/replica-kms-lifecycle

## Notes

Partially refutes H-R-034: within 25 minutes the replica never reported INACCESSIBLE_ENCRYPTION_CREDENTIALS
(AWS documents a detection window; it is longer than 25 min or only applies to the table's own region). The
only signals are data-plane ValidationExceptions in the replica region and a hidden 'replica SSE status
UPDATING' that blocks new replica Creates. A controller cannot detect this from DescribeTable quickly.

Contradiction with [DDB-TABLE-442](../service.md#ddb-table-442), [DDB-TABLE-312](../table-replicas.md#ddb-table-312), [DDB-TABLE-266](../table-replicas.md#ddb-table-266), [DDB-TABLE-267](../table-replicas.md#ddb-table-267): 442 classifies 'Operation
cannot be performed while replica server-side encryption status is in UPDATING state' as WAIT-FOR-STATE
ReplicaStatus and the 24 h source-region text as PERMANENT-QUOTA 24 h; 312/311 show the SSE text is returned
for a disabled KMS key with nothing visible changing in DescribeTable, and 266/267 show the 24 h text clears
as soon as the sourced replica is removed Resolution: keep 442 as the catalogue; 312/311 canonical for the SSE
text (external dependency: EnableKey, not a state wait) and 266/267 for the 24 h text (retry after replica
removal, not a quota window)

Contradiction with [DDB-TABLE-329](../table-replicas.md#ddb-table-329): 311: disabling an ACTIVE replica's CMK produced no INACCESSIBLE status
within 25 min; 329: ReplicaStatus=INACCESSIBLE_ENCRYPTION_CREDENTIALS appeared after ~80 min Resolution: not
contradictory - 311's 1500 s watch was shorter than the lag; 329 canonical for the end state (status lags the
KMS state by a long, variable interval). Title fix for 311 so it is not read as 'never'
