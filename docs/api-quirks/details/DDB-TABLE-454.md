<!-- generated: ack-api-quirks dynamodb 2026-10-09 model=2012-08-10 controller=34b85e6 -->
# DDB-TABLE-454: ExportTableToPointInTime on a DELETING source: accepted in the first ~1 s and COMPLETES after the table is gone; +1.5/+3 s -> TableNotFound
_Full entry and notes of one finding; its summary entry is in
[table-subresources.md](../table-subresources.md). Generated from ack-api-quirks `services/dynamodb` (model
2012-08-10, controller commit 34b85e6); edit the finding in the lab, not here._

## Finding

- <a id="ddb-table-454"></a>**DDB-TABLE-454** `delete-semantics` · impact medium · unhandled (not handled in controller) · verified 2026-10-09
  **ExportTableToPointInTime on a DELETING source: accepted in the first ~1 s and COMPLETES after the table is gone; +1.5/+3 s -> TableNotFound**
  PITR-on 1-item table. DeleteTable, then ExportTableToPointInTime(fresh ClientToken) at +0.04 s -> 200
  ExportStatus=IN_PROGRESS; the export reached COMPLETED ~2.5 min later (ItemCount=1, manifest written to S3)
  although DescribeTable had returned ResourceNotFoundException from +5.04 s. On two further PITR-on tables a
  single export call at +1.71 s and at +3.19 s (TableStatus still DELETING) failed with TableNotFoundException
  'Table not found: arn:aws:dynamodb:...:table/<name>'. RestoreTableToPointInTime(UseLatestRestorableTime) at
  +2.44 s on the DELETING source -> TableNotFoundException 'Table not found'.
  - ACK: pre-delete-cleanup, deletable.when · ops: ExportTableToPointInTime, RestoreTableToPointInTime,
    DeleteTable, DescribeExport
  - repro: PITR on; DeleteTable; ExportTableToPointInTime at +0.05 s (200) / +1.5 s (TableNotFound);
    DescribeExport until terminal
  - measurements: export_accepted_at_s=0.04, export_in_progress_s=15.1, export_rejected_at_s=[1.71, 3.19],
    restore_rejected_at_s=2.44
  - handling: not handled in the controller (as of commit 34b85e6)
  - related: [DDB-EXPORT-013](../export.md#ddb-export-013), [DDB-EXPORT-020](../export.md#ddb-export-020), [DDB-TABLE-100](../table-restore.md#ddb-table-100), [DDB-TABLE-374](../service.md#ddb-table-374), [DDB-BACKUP-001](../backup.md#ddb-backup-001), [DDB-BACKUP-007](../backup.md#ddb-backup-007),
    [DDB-TABLE-118](../service.md#ddb-table-118), [DDB-TABLE-087](../table-restore.md#ddb-table-087), [DDB-TABLE-455](../service.md#ddb-table-455), [DDB-TABLE-227](../table-replicas.md#ddb-table-227), [DDB-TABLE-228](../table-replicas.md#ddb-table-228), [DDB-EXPORT-021](../export.md#ddb-export-021), [DDB-EXPORT-022](../export.md#ddb-export-022),
    [DDB-EXPORT-016](../export.md#ddb-export-016), [DDB-EXPORT-009](../export.md#ddb-export-009), [DDB-EXPORT-011](../export.md#ddb-export-011), [DDB-EXPORT-012](../export.md#ddb-export-012) · evidence:
    table/creative/deleting-lasting-effects, table/creative/clobber-followups

## Notes

The PITR backend (export, restore, UpdateContinuousBackups, DescribeContinuousBackups, CreateBackup) forgets
the table ~1.0-1.6 s into DELETING, ~4 s before DescribeTable does. An export job started in that window
outlives the table as an immutable export record plus S3 objects.

Contradiction with [DDB-EXPORT-013](../export.md#ddb-export-013): 013 titles 'export ends None' for an export whose source was deleted ~1 s
after acceptance, but its own behavior shows the poll loop was throttled (ERR:ThrottlingException) and no
terminal status was ever read; 454 watched an export started +0.04 s into DELETING reach COMPLETED with
ItemCount=1 and the manifest written after the table was gone Resolution: keep both; 454 is canonical; 013's
outcome is unobserved, not None (retitled in title_fixes)

Contradiction with [DDB-TABLE-100](../table-restore.md#ddb-table-100), [DDB-TABLE-118](../service.md#ddb-table-118), [DDB-TABLE-455](../service.md#ddb-table-455): 100/118 state without a time bound that a
DELETING source is 'still restorable' and accepts
CreateBackup/UpdateContinuousBackups/DescribeContinuousBackups; 454 got TableNotFoundException from
RestoreTableToPointInTime at +2.44 s and ExportTableToPointInTime at +1.71 s into DELETING, and 455 saw
DescribeContinuousBackups flip to TableNotFoundException at +1.03 s while DescribeTable still returned
DELETING until ~5 s Resolution: all true; the backup/PITR backend forgets the table ~1-1.6 s into DELETING, ~4
s before DescribeTable does - 454/455 canonical for the boundary, 100/118 retitled to carry the ~1 s qualifier
