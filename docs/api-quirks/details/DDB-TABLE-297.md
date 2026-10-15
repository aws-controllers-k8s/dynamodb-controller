<!-- generated: ack-api-quirks dynamodb 2026-10-09 model=2012-08-10 controller=34b85e6 -->
# DDB-TABLE-297: Deleting a replica: DeletionProtection on the replica blocks ReplicaUpdates.Delete; DeleteTable in the replica region removes the replica
_Full entry and notes of one finding; its summary entry is in [table-replicas.md](../table-replicas.md).
Generated from ack-api-quirks `services/dynamodb` (model 2012-08-10, controller commit 34b85e6); edit the
finding in the lab, not here._

## Finding

- <a id="ddb-table-297"></a>**DDB-TABLE-297** `delete-semantics` · impact high · unhandled (not handled in controller) · verified 2026-10-09
  **Deleting a replica: DeletionProtection on the replica blocks ReplicaUpdates.Delete; DeleteTable in the replica region removes the replica**
  With DeletionProtectionEnabled=true set directly on the us-east-1 replica table (which made the BASE go
  UPDATING for 34s; A.DeletionProtectionEnabled stays false): ReplicaUpdates=[Delete us-east-1] from the base
  -> ValidationException 'Cannot delete table <name> in region us-east-1 because it has deletion protection
  enabled. Disable deletion protection first.'; DeleteTable in us-east-1 -> same message. After disabling DP
  in us-east-1: DeleteTable in us-east-1 -> 200 (response TableStatus=DELETING, Replicas=[us-west-2 ACTIVE]);
  base: UPDATING 31s -> ACTIVE[us-east-1 DELETING] 140s -> UPDATING 3s -> ACTIVE with Replicas gone at 178s;
  us-east-1 ResourceNotFoundException at 190s. StreamSpecification{StreamEnabled:false} right after the entry
  vanished -> 200, but DeleteTable 11ms later -> ResourceInUseException 'Cannot delete table while stream is
  being enabled/disabled.' Settings touched from the replica side: TagResource (regional), UpdateTimeToLive
  attr change (propagated to the base within 60s - TTL is group-wide from any region), PITR (regional),
  DeletionProtection (regional).
  - ACK: custom_delete, pre-delete-cleanup, terminal_codes · ops: UpdateTable, DeleteTable, DescribeTable ·
    fields: ReplicaUpdates.Delete, DeletionProtectionEnabled, Replicas
  - repro: UpdateTable DeletionProtectionEnabled=true in us-east-1 on the replica; UpdateTable
    ReplicaUpdates=[Delete us-east-1] in us-west-2; DeleteTable in us-east-1
  - measurements: replica_direct_delete_total_s=190.4, b_dp_toggle_base_updating_s=34.1
  - handling: not handled in the controller (as of commit 34b85e6)
  - related: [DDB-TABLE-204](../table-replicas.md#ddb-table-204), [DDB-TABLE-293](../table-replicas.md#ddb-table-293), [DDB-TABLE-308](../table-replicas.md#ddb-table-308), [DDB-TABLE-222](../table-replicas.md#ddb-table-222), [DDB-TABLE-267](../table-replicas.md#ddb-table-267), [DDB-TABLE-261](../table-global-tables.md#ddb-table-261),
    [DDB-TABLE-231](../table-global-tables.md#ddb-table-231), [DDB-TABLE-266](../table-replicas.md#ddb-table-266), [DDB-TABLE-262](../table-replicas.md#ddb-table-262), [DDB-TABLE-265](../table-replicas.md#ddb-table-265), [DDB-TABLE-260](../table-global-tables.md#ddb-table-260), [DDB-TABLE-327](../table-replicas.md#ddb-table-327), [DDB-TABLE-263](../table-global-tables.md#ddb-table-263),
    [DDB-TABLE-329](../table-replicas.md#ddb-table-329), [DDB-TABLE-311](../table-replicas.md#ddb-table-311), [DDB-TABLE-221](../table-replicas.md#ddb-table-221), [DDB-TABLE-305](../table-replicas.md#ddb-table-305), [DDB-TABLE-224](../table-replicas.md#ddb-table-224), [DDB-TABLE-296](../table-replicas.md#ddb-table-296), [DDB-TABLE-251](../table-replicas.md#ddb-table-251),
    [DDB-TABLE-294](../table-replicas.md#ddb-table-294), [DDB-TABLE-295](../table-replicas.md#ddb-table-295), [DDB-TABLE-322](../table-policy-kinesis-autoscaling.md#ddb-table-322) · hypotheses: H-R-027, H-R-009, H-R-004, H-R-028 · evidence:
    table/mutation-matrix/replica-overrides

## Notes

Confirms H-R-027 (per-region deletion protection guards ReplicaUpdates.Delete; message names the region) and
H-R-009 (out-of-band DeleteTable on a replica is allowed when that replica was not a source; the base sees
DELETING then a missing entry - pure drift). Any UpdateTable on any member flips every member to UPDATING for
~30s.

Contradiction with [DDB-TABLE-329](../table-replicas.md#ddb-table-329), [DDB-TABLE-327](../table-replicas.md#ddb-table-327), [DDB-TABLE-263](../table-global-tables.md#ddb-table-263): 329 says the source's
DeletionProtectionEnabled=true 'propagated to the replica' and 327's note repeats it; 263 (MRSC,
base->replica) and 297 (EVENTUAL, replica->base) observe no propagation Resolution: 263/297 are right - DP is
per region. 329's own evidence.jsonl shows us-east-1 DeletionProtectionEnabled=false at +18 s, +80 s and +140
s after the source DP=true (02:06:06), and the replica-region DP=true call got ResourceInUseException; 327 set
DP in the replica region itself. 329's sentence and 327's note are wrong
