<!-- generated: ack-api-quirks dynamodb 2026-10-09 model=2012-08-10 controller=34b85e6 -->
# DDB-BACKUP-018: Backup should be a standalone, spec-immutable ACK resource keyed by BackupArn (create/read/delete/list only, outlives its Table)
_Full entry and notes of one finding; its summary entry is in [backup.md](../backup.md). Generated from
ack-api-quirks `services/dynamodb` (model 2012-08-10, controller commit 34b85e6); edit the finding in the lab,
not here._

## Finding

- <a id="ddb-backup-018"></a>**DDB-BACKUP-018** `scope` · impact high · handled · verified 2026-10-09, re-verified
  **Backup should be a standalone, spec-immutable ACK resource keyed by BackupArn (create/read/delete/list only, outlives its Table)**
  **Scope verdict: implement**
  Backup has its own ARN as the only identifier (names are not unique, no name filter), no update operation, a
  lifecycle independent of the Table (survives DeleteTable, spans table recreations), a terminal AVAILABLE
  state reachable within seconds, and is not taggable. Its only inputs are TableName and BackupName; readable
  status is BackupDetails + SourceTableDetails + SourceTableFeatureDetails. SYSTEM backups (BackupType) are
  service-managed and undeletable.
  - ACK: is_arn_primary_key, is_immutable, tags.ignore, custom_find, output_wrapper_field_path · ops:
    CreateBackup, DescribeBackup, DeleteBackup, ListBackups
  - repro: see backup/* probes
  - handling: handled via `pkg/resource/backup/sdk.go:140-158; pkg/resource/backup/sdk.go:283-292; pkg/resource/backup/sdk.go:251-258; pkg/resource/backup/hooks.go:24-28`
  - related: [DDB-BACKUP-007](../backup.md#ddb-backup-007), [DDB-BACKUP-016](../backup.md#ddb-backup-016), [DDB-BACKUP-011](../backup.md#ddb-backup-011), [DDB-BACKUP-019](../backup.md#ddb-backup-019), [DDB-EXPORT-021](../export.md#ddb-export-021), [DDB-IMPORT-005](../import.md#ddb-import-005),
    [DDB-TABLE-278](../table-restore.md#ddb-table-278) · hypotheses: H-B-037 · evidence: backup/identity/arn-list-filters,
    table/creative/reverify-set-b

## Notes

H-B-037 confirmed. Scope verdict: implement as its own CRD with spec {tableName, backupName} both immutable
(any drift = terminal condition, never delete+recreate), status mirroring BackupDescription.*, ReadOne by ARN
(status.ackResourceMetadata.arn), and a delete path that treats BackupNotFoundException as gone and
BackupInUseException as retryable. Restore is NOT a Backup concern: it is an alternative create path for Table
(see table/round-trip/restore-* findings).
