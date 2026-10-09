<!-- generated: ack-api-quirks dynamodb 2026-10-09 model=2012-08-10 controller=34b85e6 -->
# DDB-TABLE-463: RestoreTableFromBackup with OnDemandThroughputOverride={} fails 400 TableInUseException 'already being restored'...
_Full entry and notes of one finding; its summary entry is in [table-restore.md](../table-restore.md).
Generated from ack-api-quirks `services/dynamodb` (model 2012-08-10, controller commit 34b85e6); edit the
finding in the lab, not here._

## Finding

- <a id="ddb-table-463"></a>**DDB-TABLE-463** `error-code` · impact high · unhandled (not handled in controller) · verified 2026-10-09
  **RestoreTableFromBackup with OnDemandThroughputOverride={} fails 400 TableInUseException 'already being restored'...**
  A FIRST RestoreTableFromBackup for a fresh target name with OnDemandThroughputOverride={} (alone or with
  BillingModeOverride=PAY_PER_REQUEST) returns HTTP 400 TableInUseException 'Table: <target> is already being
  restored from backup: <backup arn>' after 210-350 ms - yet the target table exists at +0 s (CreationDateTime
  inside the call), restores normally (CREATING 3.5 min for an empty table) and ends ACTIVE, PAY_PER_REQUEST,
  with no OnDemandThroughput. Reproduced 3/3 on three independent backups/targets. The empty struct evidently
  crashes the handler after the restore workflow was started and an internal retry reports the self-conflict.
  A replay of the same request a few seconds later is a genuine TableInUseException (26-37 ms). Controls: a
  valid OnDemandThroughputOverride -> 200 (ODT 10/10 applied); SSESpecificationOverride={} -> 200 and restores
  with the AWS-owned key (no SSEDescription); the HMAC-key 500 creates no target (checked +0/+0.5/+2 s) and a
  corrected retry to the same name is accepted. For a controller: this 4xx must not be treated as 'someone
  else owns the target' - DescribeTable the target and adopt it; the restore-time error class ('is already
  being restored') is otherwise TRANSIENT-WAIT-FOR-STATE (TableStatus=ACTIVE) as [DDB-TABLE-219](../table-restore.md#ddb-table-219) describes.
  - ACK: custom_create, custom_find, requeue · ops: RestoreTableFromBackup · fields:
    OnDemandThroughputOverride, SSESpecificationOverride, BillingModeOverride
  - repro: CreateBackup (empty PPR table) -> RestoreTableFromBackup BackupArn=<arn> TargetTableName=<new>
    OnDemandThroughputOverride={} -> 400 TableInUseException; DescribeTable <new> -> CREATING with
    RestoreSummary
  - measurements: first_call_latency_ms=[347, 210], replay_latency_ms=37, restore_creating_s=211.4
  - handling: not handled in the controller (as of commit 34b85e6)
  - related: [DDB-TABLE-219](../table-restore.md#ddb-table-219), [DDB-TABLE-275](../table-restore.md#ddb-table-275), [DDB-TABLE-437](../table-throughput-billing.md#ddb-table-437), [DDB-TABLE-178](../table-throughput-billing.md#ddb-table-178), [DDB-TABLE-273](../table-restore.md#ddb-table-273), [DDB-TABLE-457](../service.md#ddb-table-457),
    [DDB-TABLE-276](../table-restore.md#ddb-table-276), [DDB-TABLE-461](../table-streams-encryption-class.md#ddb-table-461), [DDB-TABLE-186](../table-restore.md#ddb-table-186), [DDB-TABLE-218](../table-restore.md#ddb-table-218), [DDB-TABLE-274](../table-restore.md#ddb-table-274), [DDB-TABLE-282](../table-restore.md#ddb-table-282), [DDB-TABLE-278](../table-restore.md#ddb-table-278) ·
    evidence: table/creative/restore-odt-empty-side-effect, table/creative/degenerate-5xx-hunt

## Notes

Extends [DDB-TABLE-219](../table-restore.md#ddb-table-219)/275 (TableInUseException for CREATING restore targets) with a self-inflicted first-call
variant, and [DDB-TABLE-437](../table-throughput-billing.md#ddb-table-437)/178 ({} throughput structs) to the restore API, where the consequence is a
4xx-with-side-effect instead of a 500. Rows:
t1_odt_empty -> 400 TableInUseException 347ms target_exists@+0/+0.5/+2s=[True, True, True] 'Table:
ackq-8a381f-ro-t1 is already being restored from backup:
arn:aws:dynamodb:us-west-2:<ACCOUNT>:table/ackq-8a381f-ro-src/backup/01791526119669-fd87be64'
t2_odt_empty_plus_billing -> 400 TableInUseException 210ms target_exists@+0/+0.5/+2s=[True, True, True]
'Table: ackq-8a381f-ro-t2 is already being restored from backup:
arn:aws:dynamodb:us-west-2:<ACCOUNT>:table/ackq-8a381f-ro-src/backup/01791526119734-6e1c87a4'
t3_odt_valid_control -> 200 OK 143ms target_exists@+0/+0.5/+2s=[True, True, True] ''
t4_sse_empty -> 200 OK 108ms target_exists@+0/+0.5/+2s=[True, True, True] ''
t5_hmac_500 -> 500 InternalServerError 44ms target_exists@+0/+0.5/+2s=[False, False, False] 'KMS internal
error: com.amazonaws.services.kms.model.AWSKMSException: EncryptionContext is supported only when creating a
grant for a symmetric encryption KMS key. (Service: AWSKMS; Status Code: 400; '
t5_valid_retry_same_target -> 200 OK 147ms target_exists@+0/+0.5/+2s=[True, True, True] ''
t1_odt_empty_replay -> 400 TableInUseException 37ms target_exists@+0/+0.5/+2s=[True, True, True] 'Table:
ackq-8a381f-ro-t1 is already being restored from backup:
arn:aws:dynamodb:us-west-2:<ACCOUNT>:table/ackq-8a381f-ro-src/backup/01791526119669-fd87be64'
t4_sse_empty_replay -> 400 TableInUseException 26ms target_exists@+0/+0.5/+2s=[True, True, True] 'Table:
ackq-8a381f-ro-t4 is already being restored from backup:
arn:aws:dynamodb:us-west-2:<ACCOUNT>:table/ackq-8a381f-ro-src/backup/01791526119839-52784a4d'
Final target states: {"t1": {"status": "ACTIVE", "odt": null, "sse": null, "billing": "PAY_PER_REQUEST"},
"t2": {"status": "ACTIVE", "odt": null, "sse": null, "billing": "PAY_PER_REQUEST"}, "t3": {"status": "ACTIVE",
"odt": {"MaxReadRequestUnits": 10, "MaxWriteRequestUnits": 10}, "sse": null, "billing": "PAY_PER_REQUEST"},
"t4": {"status": "ACTIVE", "odt": null, "sse": null, "billing": "PAY_PER_REQUEST"}, "t5": {"status": "ACTIVE",
"odt": null, "sse": null, "billing": "PAY_PER_REQUEST"}}
Restore durations: t1 CREATING 211 s (watched from the start); the others were already ACTIVE when first
watched (~4 min after the calls).

Contradiction with [DDB-TABLE-457](../service.md#ddb-table-457): 457 reports two TableInUseException rows for RestoreTableFromBackup with
OnDemandThroughputOverride={} AND with SSESpecificationOverride={}; 463 isolates them: only the {}
OnDemandThroughputOverride triggers the self-conflict (3/3), SSESpecificationOverride={} on its own target
returns 200 - 457's second row hit the target already created by the first call (same TargetTableName)
Resolution: 463 canonical; 457's '2 TableInUseException (restore side effect)' is correct in count but should
not be read as SSE {} being rejected
