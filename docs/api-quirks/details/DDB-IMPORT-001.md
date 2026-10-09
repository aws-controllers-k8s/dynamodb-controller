<!-- generated: ack-api-quirks dynamodb 2026-10-09 model=2012-08-10 controller=34b85e6 -->
# DDB-IMPORT-001: ImportTable returns TableArn/TableId at once but the table is ResourceNotFoundException at t+0; it appears as CREATING ~30.72s later
_Full entry and notes of one finding; its summary entry is in [import.md](../import.md). Generated from
ack-api-quirks `services/dynamodb` (model 2012-08-10, controller commit 34b85e6); edit the finding in the lab,
not here._

## Finding

- <a id="ddb-import-001"></a>**DDB-IMPORT-001** `async-state-machine` · impact high · handled · verified 2026-10-09
  **ImportTable returns TableArn/TableId at once but the table is ResourceNotFoundException at t+0; it appears as CREATING ~30.72s later**
  ImportTable returned HTTP 200 with ImportStatus=IN_PROGRESS and TableArn/TableId already set; DescribeTable
  at t+0 -> ResourceNotFoundException (TableStatus=None). The table became visible (CREATING) at ~30.72s,
  turned ACTIVE at ~110.83s and the import reached COMPLETED at ~110.83s (1-item DYNAMODB_JSON file). While
  CREATING under the import: UpdateTable=ResourceInUseException, PutItem=ResourceNotFoundException,
  CreateBackup=ContinuousBackupsUnavailableException, TagResource=ResourceNotFoundException,
  UpdateTimeToLive=ResourceInUseException, UpdateContinuousBackups=ContinuousBackupsUnavailableException,
  DescribeContinuousBackups=ok.
  - ACK: synced.when, custom_create, requeue · ops: ImportTable, DescribeTable, DescribeImport · fields:
    ImportTableDescription.TableArn, ImportTableDescription.TableId, ImportTableDescription.ImportStatus,
    Table.TableStatus
  - repro: ImportTable (PAY_PER_REQUEST, 1 key) -> DescribeTable immediately -> poll DescribeImport +
    DescribeTable every 10s
  - measurements: t1_table_visible_s=30.72, t1_table_active_s=110.83, t1_import_terminal_s=110.83,
    t1_active_minus_terminal_s=0.0, poll_interval_s=6
  - handling: handled via `generator.yaml:84-87; pkg/resource/table/sdk.go:83-86`
  - related: [DDB-BACKUP-001](../backup.md#ddb-backup-001), [DDB-TABLE-217](../table-restore.md#ddb-table-217), [DDB-TABLE-298](../table-replicas.md#ddb-table-298), [DDB-TABLE-447](../service.md#ddb-table-447), [DDB-BACKUP-002](../backup.md#ddb-backup-002), [DDB-EXPORT-014](../export.md#ddb-export-014),
    [DDB-IMPORT-018](../import.md#ddb-import-018), [DDB-IMPORT-019](../import.md#ddb-import-019), [DDB-IMPORT-017](../import.md#ddb-import-017) · hypotheses: H-B-032 · evidence:
    import/state-machine/lifecycle

## Notes

H-B-032 partially refuted: the response carries TableArn/TableId but DescribeTable at t+0 ->
ResourceNotFoundException (all t+0 ops: {'DescribeTable': 'ResourceNotFoundException',
'UpdateTable.DeletionProtection': 'ResourceNotFoundException', 'PutItem': 'ResourceNotFoundException',
'CreateBackup': 'TableNotFoundException', 'TagResource': 'ResourceNotFoundException', 'ListTagsOfResource':
'ResourceNotFoundException', 'UpdateTimeToLive': 'ResourceNotFoundException', 'UpdateContinuousBackups':
'TableNotFoundException', 'DescribeContinuousBackups': 'TableNotFoundException', 'DescribeTimeToLive':
'ResourceNotFoundException'}); the '>5 min' duration claim is refuted (terminal at ~110.83s). ACTIVE and
COMPLETED were observed in the same poll.
