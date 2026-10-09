<!-- generated: ack-api-quirks dynamodb 2026-10-09 model=2012-08-10 controller=34b85e6 -->
# DDB-TABLE-278: Restore (from backup / point in time) belongs on Table as an immutable create-time field group, not a separate resource
_Full entry and notes of one finding; its summary entry is in [table-restore.md](../table-restore.md).
Generated from ack-api-quirks `services/dynamodb` (model 2012-08-10, controller commit 34b85e6); edit the
finding in the lab, not here._

## Finding

- <a id="ddb-table-278"></a>**DDB-TABLE-278** `scope` · impact high · unhandled (not handled in controller) · verified 2026-10-09
  **Restore (from backup / point in time) belongs on Table as an immutable create-time field group, not a separate resource**
  **Scope verdict: field-on-parent**
  Both restore APIs are synchronous-admission creators of an ordinary Table (TableStatus CREATING -> ACTIVE in
  2-13 min), accept the same override parameters that map onto Table spec fields (BillingMode,
  ProvisionedThroughput, OnDemandThroughput, GSIs/LSIs, SSESpecification), validate everything else
  synchronously, leave no durable RestoreSummary after ACTIVE, and the non-restorable settings (streams, TTL,
  PITR, tags, deletion protection, table class, resource policy) are applied by the normal UpdateTable /
  sub-resource calls afterwards without errors.
  - ACK: custom_create, is_immutable, post-create-nudge, annotation-shadow-state · ops:
    RestoreTableFromBackup, RestoreTableToPointInTime
  - repro: see table/round-trip/restore-* and table/error-taxonomy/restore-errors
  - handling: not handled in the controller (as of commit 34b85e6)
  - related: [DDB-TABLE-186](../table-restore.md#ddb-table-186), [DDB-TABLE-218](../table-restore.md#ddb-table-218), [DDB-TABLE-274](../table-restore.md#ddb-table-274), [DDB-TABLE-282](../table-restore.md#ddb-table-282), [DDB-TABLE-463](../table-restore.md#ddb-table-463), [DDB-TABLE-216](../table-restore.md#ddb-table-216),
    [DDB-IMPORT-005](../import.md#ddb-import-005), [DDB-IMPORT-008](../import.md#ddb-import-008), [DDB-EXPORT-020](../export.md#ddb-export-020), [DDB-BACKUP-018](../backup.md#ddb-backup-018), [DDB-EXPORT-021](../export.md#ddb-export-021) · hypotheses: H-B-040 ·
    evidence: table/error-taxonomy/restore-errors

## Notes

H-B-040 confirmed for the design (restoreFrom: {backupArn | sourceTableArn/sourceTableName +
restoreDateTime|useLatestRestorableTime} + sseSpecificationOverride mandatory when the ARN region differs);
its 'RestoreSummary as durable marker' clause is refuted, so the controller must persist the restore origin
itself and treat the field group as immutable.
