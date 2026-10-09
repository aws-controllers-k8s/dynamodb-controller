<!-- generated: ack-api-quirks dynamodb 2026-10-09 model=2012-08-10 controller=34b85e6 -->
# DDB-BACKUP-011: BackupName is not unique: three CreateBackup calls with the same name on one table succeed and yield three ARNs; no name filter exists
_Full entry and notes of one finding; its summary entry is in [backup.md](../backup.md). Generated from
ack-api-quirks `services/dynamodb` (model 2012-08-10, controller commit 34b85e6); edit the finding in the lab,
not here._

## Finding

- <a id="ddb-backup-011"></a>**DDB-BACKUP-011** `identity` · impact high · handled · verified 2026-10-09
  **BackupName is not unique: three CreateBackup calls with the same name on one table succeed and yield three ARNs; no name filter exists**
  Three back-to-back CreateBackup calls with an identical BackupName returned 200 each (first one after the
  usual 3 s post-ACTIVE window) with three distinct BackupArns (table/<name>/backup/<epoch-ms>-<8 hex>);
  ListBackups(TableName) listed all three with the same BackupName, ordered by BackupCreationDateTime.
  ListBackups offers no BackupName filter and BackupSummary carries {BackupArn, BackupCreationDateTime,
  BackupName, BackupSizeBytes, BackupStatus, BackupType, TableArn, TableId, TableName}. No UpdateBackup
  operation exists in the model (Backup ops: CreateBackup, DeleteBackup, DescribeBackup, ListBackups,
  RestoreTableFromBackup).
  - ACK: is_arn_primary_key, custom_find, list_operation.match_fields, is_immutable · ops: CreateBackup,
    ListBackups · fields: BackupName, BackupArn
  - repro: CreateBackup(name=X) x3 -> ListBackups(TableName)
  - handling: handled via `pkg/resource/backup/sdk.go:140-158; pkg/resource/backup/sdk.go:283-292`
  - related: [DDB-BACKUP-019](../backup.md#ddb-backup-019), [DDB-BACKUP-018](../backup.md#ddb-backup-018) · hypotheses: H-B-004, H-B-010, H-B-144 · evidence:
    backup/identity/arn-list-filters

## Notes

H-B-004 confirmed; H-B-010 confirmed (no update op; every spec field immutable). H-B-144 refuted for small
tables: concurrent CreateBackup calls on the same table do not collide (BackupInUseException never seen; 24
rapid creates in backup/limits/burst all succeeded). A controller must persist the ARN from the create
response immediately; a retry without it duplicates the (billed) backup, and ReadOne must be ARN-based
(name+TableName is ambiguous).
