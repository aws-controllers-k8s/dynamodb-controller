<!-- generated: ack-api-quirks dynamodb 2026-10-09 model=2012-08-10 controller=34b85e6 -->
# DDB-TABLE-329: Replica CMK disabled: ReplicaStatus INACCESSIBLE_ENCRYPTION_CREDENTIALS after ~80 min; source stays ACTIVE/writable; no InaccessibleDateTime
_Full entry and notes of one finding; its summary entry is in [table-replicas.md](../table-replicas.md).
Generated from ack-api-quirks `services/dynamodb` (model 2012-08-10, controller commit 34b85e6); edit the
finding in the lab, not here._

## Finding

- <a id="ddb-table-329"></a>**DDB-TABLE-329** `cross-region` · impact high · unhandled (not handled in controller) · verified 2026-10-09
  **Replica CMK disabled: ReplicaStatus INACCESSIBLE_ENCRYPTION_CREDENTIALS after ~80 min; source stays ACTIVE/writable; no InaccessibleDateTime**
  Empty 2019.11.21 global table, replica created with ReplicaUpdates.Create{RegionName:us-east-1,
  KMSMasterKeyId:<bare mrk id>}: source TableStatus UPDATING 0-56 s, replica CREATING from 56 s, source back
  to ACTIVE at 67 s while the replica was still CREATING, UPDATING again 311-617 s, replica ACTIVE at 617 s
  (~10 min). kms DisableKey on the us-east-1 replica key only, 60 s polling of both regions: nothing changed
  for 79 min; at +4783 s the SAME poll showed source
  Replicas[].ReplicaStatus=INACCESSIBLE_ENCRYPTION_CREDENTIALS and replica-region
  TableStatus=INACCESSIBLE_ENCRYPTION_CREDENTIALS; source TableStatus stayed ACTIVE throughout;
  Replicas[].ReplicaInaccessibleDateTime was never populated, nor was
  SSEDescription.InaccessibleEncryptionDateTime in the replica region's DescribeTable (unlike a single-region
  table, which gets InaccessibleEncryptionDateTime). In that state: source GetItem OK, source PutItem OK,
  replica-region GetItem -> ValidationException 'KMS key disabled error: ...DisabledException ... is
  disabled', replica-region ListTagsOfResource OK, source UpdateTable(DeletionProtectionEnabled=true) -> OK
  (global-table DP change: source UPDATING ~60 s and the flag propagated to the replica), and during that
  minute every other write (source DP=false, replica-region UpdateTable DP, replica-region DeleteTable, source
  ReplicaUpdates.Delete) -> ResourceInUseException 'The resource which you are attempting to change is in
  use.'. The replica was finally removed with a direct DeleteTable in us-east-1 (see
  table/dependencies/replica-delete-while-inaccessible): ReplicaStatus=DELETING, gone from Replicas[] and
  ResourceNotFoundException in us-east-1 within 3 min. EnableKey-based recovery of the replica was not
  measured.
  - ACK: synced.when, requeue, custom_update, terminal_codes · ops: DescribeTable, UpdateTable, DeleteTable,
    GetItem, PutItem · fields: Replicas.ReplicaStatus, Replicas.ReplicaInaccessibleDateTime,
    ReplicaUpdates.Delete, TableStatus
  - repro: Global table (2019.11.21) with a replica encrypted by the MRK replica key; kms DisableKey in the
    replica region; poll both regions every 60 s; then ReplicaUpdates.Delete
  - measurements: replica_create_s=617.2, replica_inaccessible_detect_s=4783.2,
    replica_direct_delete_removal_s_upper=180
  - handling: not handled in the controller (as of commit 34b85e6)
  - related: [DDB-TABLE-226](../table-replicas.md#ddb-table-226), [DDB-TABLE-306](../table-policy-kinesis-autoscaling.md#ddb-table-306), [DDB-TABLE-265](../table-replicas.md#ddb-table-265), [DDB-TABLE-308](../table-replicas.md#ddb-table-308), [DDB-TABLE-309](../table-replicas.md#ddb-table-309), [DDB-TABLE-187](../table-replicas.md#ddb-table-187),
    [DDB-TABLE-305](../table-replicas.md#ddb-table-305), [DDB-TABLE-249](../table-replicas.md#ddb-table-249), [DDB-TABLE-258](../table-global-tables.md#ddb-table-258), [DDB-TABLE-297](../table-replicas.md#ddb-table-297), [DDB-TABLE-327](../table-replicas.md#ddb-table-327), [DDB-TABLE-263](../table-global-tables.md#ddb-table-263), [DDB-TABLE-311](../table-replicas.md#ddb-table-311),
    [DDB-TABLE-313](../table-replicas.md#ddb-table-313), [DDB-TABLE-295](../table-replicas.md#ddb-table-295), [DDB-TABLE-328](../table-streams-encryption-class.md#ddb-table-328), [DDB-TABLE-312](../table-replicas.md#ddb-table-312), [DDB-TABLE-310](../table-replicas.md#ddb-table-310) · hypotheses: H-T-108 ·
    evidence: table/cross-region/multi-region-kms-replica

## Notes

H-T-108 first stage only (INACCESSIBLE replica; the ARCHIVING/ARCHIVED tail is untested). Detection took ~80
min here vs 12-75 min for single-region tables in table/state-machine/kms-inaccessible-lifecycle: the status
lags the KMS state by a long, variable interval. The source table keeps working (reads/writes OK) while one
replica is encryption-broken, and the DP flag on a global table is propagated to the replica, so a controller
enabling DP on the source also blocks deleting the broken replica (ValidationException 'Cannot delete table
... in region us-east-1 because it has deletion protection enabled').

Contradiction with [DDB-TABLE-327](../table-replicas.md#ddb-table-327), [DDB-TABLE-263](../table-global-tables.md#ddb-table-263), [DDB-TABLE-297](../table-replicas.md#ddb-table-297): 329 says the source's
DeletionProtectionEnabled=true 'propagated to the replica' and 327's note repeats it; 263 (MRSC,
base->replica) and 297 (EVENTUAL, replica->base) observe no propagation Resolution: 263/297 are right - DP is
per region. 329's own evidence.jsonl shows us-east-1 DeletionProtectionEnabled=false at +18 s, +80 s and +140
s after the source DP=true (02:06:06), and the replica-region DP=true call got ResourceInUseException; 327 set
DP in the replica region itself. 329's sentence and 327's note are wrong

Contradiction with [DDB-TABLE-226](../table-replicas.md#ddb-table-226), [DDB-TABLE-306](../table-policy-kinesis-autoscaling.md#ddb-table-306), [DDB-TABLE-265](../table-replicas.md#ddb-table-265): 329 describes an 'Empty' table whose replica
took 617 s to become ACTIVE, while 226/306/265 establish ~15-50 s for empty tables and ~10-11 min for tables
holding an item Resolution: 329's 'Empty' is wrong: probe.py:143 PutItem {pk:'one'} at 00:35:59 precedes the
ReplicaUpdates.Create at 00:36:01 (evidence rows 100/104); 617 s matches the 1-item slow path of 306 (686 s)
and 265 (580-660 s). No contradiction with 226

Contradiction with [DDB-TABLE-311](../table-replicas.md#ddb-table-311): 311: disabling an ACTIVE replica's CMK produced no INACCESSIBLE status
within 25 min; 329: ReplicaStatus=INACCESSIBLE_ENCRYPTION_CREDENTIALS appeared after ~80 min Resolution: not
contradictory - 311's 1500 s watch was shorter than the lag; 329 canonical for the end state (status lags the
KMS state by a long, variable interval). Title fix for 311 so it is not read as 'never'
