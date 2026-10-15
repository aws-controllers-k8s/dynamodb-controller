<!-- generated: ack-api-quirks dynamodb 2026-10-09 model=2012-08-10 controller=34b85e6 -->
# DDB-TABLE-453: PITR enable issued while a never-PITR table is DELETING is accepted (200 ENABLED) for ~1 s and creates the undeletable 35-day SYSTEM backup
_Full entry and notes of one finding; its summary entry is in
[table-subresources.md](../table-subresources.md). Generated from ack-api-quirks `services/dynamodb` (model
2012-08-10, controller commit 34b85e6); edit the finding in the lab, not here._

## Finding

- <a id="ddb-table-453"></a>**DDB-TABLE-453** `delete-semantics` · impact high · unhandled (not handled in controller) · verified 2026-10-09
  **PITR enable issued while a never-PITR table is DELETING is accepted (200 ENABLED) for ~1 s and creates the undeletable 35-day SYSTEM backup**
  Table created without PITR (PointInTimeRecoveryStatus DISABLED), DeleteTable -> DELETING.
  UpdateContinuousBackups(PointInTimeRecoveryEnabled=true) every 200 ms: 200 with
  PointInTimeRecoveryStatus=ENABLED from +0.03 s to +1.03 s (6 calls), then TableNotFoundException 'Table not
  found: <name>' from +1.23 s while DescribeTable still reports DELETING until +5.23 s. ~8 s later
  ListBackups(BackupType=SYSTEM) shows '<name>$DeletedTableBackup' (AVAILABLE, 0 bytes, BackupExpiryDateTime =
  +35 days). Reproduced with a single UpdateContinuousBackups(enable) at +0.88 s on a second never-PITR table
  (200 ENABLED; SYSTEM backup created at +5 s). DescribeBackup shows SourceTableFeatureDetails={} and
  DeleteBackup fails with ValidationException 'User is not allowed to delete the system backup with arn ... It
  will automatically expire on 2026-11-13T...'. A same-name re-create afterwards reads
  PointInTimeRecoveryStatus DISABLED (no leak).
  - ACK: pre-delete-cleanup, deletable.when, custom_delete · ops: UpdateContinuousBackups, DeleteTable,
    ListBackups, DeleteBackup · fields: PointInTimeRecoverySpecification, BackupType
  - repro: CreateTable (no PITR) -> ACTIVE; DeleteTable;
    UpdateContinuousBackups(PointInTimeRecoveryEnabled=true) within ~1 s; ListBackups(TableName,
    BackupType=SYSTEM) 10 s later
  - measurements: enable_accepted_window_s=[0.03, 1.03], system_backup_visible_after_s=8,
    backup_retention_days=35, trials=2
  - handling: not handled in the controller (as of commit 34b85e6)
  - related: [DDB-TABLE-088](../table-subresources.md#ddb-table-088), [DDB-TABLE-167](../table-subresources.md#ddb-table-167), [DDB-BACKUP-015](../backup.md#ddb-backup-015), [DDB-TABLE-113](../table-subresources.md#ddb-table-113), [DDB-TABLE-122](../table-policy-kinesis-autoscaling.md#ddb-table-122), [DDB-TABLE-236](../table-policy-kinesis-autoscaling.md#ddb-table-236),
    [DDB-TABLE-374](../service.md#ddb-table-374), [DDB-TABLE-097](../table-subresources.md#ddb-table-097), [DDB-TABLE-235](../table-policy-kinesis-autoscaling.md#ddb-table-235), [DDB-TABLE-342](../table-subresources.md#ddb-table-342), [DDB-TABLE-214](../table-policy-kinesis-autoscaling.md#ddb-table-214) · evidence:
    table/creative/deleting-lasting-effects, table/creative/clobber-followups

## Notes

Inverse of [DDB-TABLE-088](../table-subresources.md#ddb-table-088) (a disable issued while DELETING suppresses the backup). A controller whose PITR sync
races a deletion (user deleted the table out of band, or the finalizer and the spec sync run in the same
second) leaves a 35-day artifact in the account that nobody can delete. Two such backups exist from this run
(ackq-1c198e-dl-pitr$DeletedTableBackup, ackq-c05a52-fu-pitr2$DeletedTableBackup); they expire 2026-11-13.
