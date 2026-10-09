<!-- generated: ack-api-quirks dynamodb 2026-10-09 model=2012-08-10 controller=34b85e6 -->
# DDB-TABLE-327: INACCESSIBLE replica: DP update and direct DeleteTable in the replica region are accepted; ReplicaUpdates.Delete refused while DP on
_Full entry and notes of one finding; its summary entry is in [table-replicas.md](../table-replicas.md).
Generated from ack-api-quirks `services/dynamodb` (model 2012-08-10, controller commit 34b85e6); edit the
finding in the lab, not here._

## Finding

- <a id="ddb-table-327"></a>**DDB-TABLE-327** `cross-region` · impact high · unhandled (not handled in controller) · verified 2026-10-09
  **INACCESSIBLE replica: DP update and direct DeleteTable in the replica region are accepted; ReplicaUpdates.Delete refused while DP on**
  Global table (2019.11.21) us-west-2 -> us-east-1 replica encrypted with the MRK replica key; that key
  disabled; source TableStatus=ACTIVE, Replicas[].ReplicaStatus=INACCESSIBLE_ENCRYPTION_CREDENTIALS
  (Replicas[].ReplicaInaccessibleDateTime never appeared on the source side; the replica region's own
  DescribeTable shows TableStatus=INACCESSIBLE_ENCRYPTION_CREDENTIALS but no InaccessibleEncryptionDateTime
  either). In the replica region: ListTagsOfResource OK, TagResource OK, GetItem -> ValidationException 'KMS
  key disabled error: ...DisabledException', UpdateTable(DeletionProtectionEnabled=true) -> OK
  (TableStatus=UPDATING ~20 s, source UPDATING too). With DP on (attempt 1): UpdateTable ReplicaUpdates.Delete
  from the source AND a direct DeleteTable in the replica region -> ValidationException 'Cannot delete table
  <name> in region us-east-1 because it has deletion protection enabled. Disable deletion protection first.'
  After DP off on both sides (each -> OK/UPDATING, ~35 s): direct DeleteTable in the replica region -> OK,
  response TableStatus=DELETING; the source then shows ReplicaStatus=DELETING and a ReplicaUpdates.Delete
  issued 0.2 s later -> ResourceInUseException 'The resource which you are attempting to change is in use.'
  (the parent probe's ReplicaUpdates.Delete, issued 20 s after a source DP update, got the same
  ResourceInUseException). The removal itself is timed by table/cross-region/multi-region-kms-replica.
  - ACK: deletable.when, custom_update, requeue, synced.when · ops: UpdateTable, DeleteTable, DescribeTable,
    TagResource, GetItem · fields: ReplicaUpdates.Delete, Replicas.ReplicaStatus,
    Replicas.ReplicaInaccessibleDateTime, TableStatus
  - repro: Global table with a replica on a CMK; kms DisableKey in the replica region; wait for
    ReplicaStatus=INACCESSIBLE_ENCRYPTION_CREDENTIALS; UpdateTable ReplicaUpdates.Delete
  - handling: not handled in the controller (as of commit 34b85e6)
  - related: [DDB-TABLE-306](../table-policy-kinesis-autoscaling.md#ddb-table-306), [DDB-TABLE-200](../table-replicas.md#ddb-table-200), [DDB-TABLE-296](../table-replicas.md#ddb-table-296), [DDB-TABLE-263](../table-global-tables.md#ddb-table-263), [DDB-TABLE-315](../table-policy-kinesis-autoscaling.md#ddb-table-315), [DDB-TABLE-314](../table-policy-kinesis-autoscaling.md#ddb-table-314),
    [DDB-TABLE-297](../table-replicas.md#ddb-table-297), [DDB-TABLE-329](../table-replicas.md#ddb-table-329), [DDB-TABLE-311](../table-replicas.md#ddb-table-311), [DDB-TABLE-313](../table-replicas.md#ddb-table-313), [DDB-TABLE-309](../table-replicas.md#ddb-table-309), [DDB-TABLE-295](../table-replicas.md#ddb-table-295), [DDB-TABLE-328](../table-streams-encryption-class.md#ddb-table-328),
    [DDB-TABLE-312](../table-replicas.md#ddb-table-312), [DDB-TABLE-310](../table-replicas.md#ddb-table-310) · hypotheses: H-T-108, H-T-104 · evidence:
    table/dependencies/replica-delete-while-inaccessible

## Notes

H-T-108 (INACCESSIBLE stage only; ARCHIVED untested): a broken replica can be dropped with a plain DeleteTable
in its own region (2019.11.21 global tables allow that) even while INACCESSIBLE; ReplicaUpdates.Delete was not
observed succeeding in this state because every attempt collided with an in-flight DP change or the direct
delete - inconclusive, likely admitted. DeletionProtection is per replica: the source's DP=true had been
propagated to the replica by the parent probe, and both delete paths check the REPLICA's DP flag.

Contradiction with [DDB-TABLE-329](../table-replicas.md#ddb-table-329), [DDB-TABLE-263](../table-global-tables.md#ddb-table-263), [DDB-TABLE-297](../table-replicas.md#ddb-table-297): 329 says the source's
DeletionProtectionEnabled=true 'propagated to the replica' and 327's note repeats it; 263 (MRSC,
base->replica) and 297 (EVENTUAL, replica->base) observe no propagation Resolution: 263/297 are right - DP is
per region. 329's own evidence.jsonl shows us-east-1 DeletionProtectionEnabled=false at +18 s, +80 s and +140
s after the source DP=true (02:06:06), and the replica-region DP=true call got ResourceInUseException; 327 set
DP in the replica region itself. 329's sentence and 327's note are wrong
