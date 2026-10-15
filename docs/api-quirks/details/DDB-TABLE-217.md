<!-- generated: ack-api-quirks dynamodb 2026-10-09 model=2012-08-10 controller=34b85e6 -->
# DDB-TABLE-217: While a restore is CREATING, sub-resource/tag/UpdateTable calls fail with ResourceNotFound/TableNotFound, not InUse
_Full entry and notes of one finding; its summary entry is in [table-restore.md](../table-restore.md).
Generated from ack-api-quirks `services/dynamodb` (model 2012-08-10, controller commit 34b85e6); edit the
finding in the lab, not here._

## Finding

- <a id="ddb-table-217"></a>**DDB-TABLE-217** `async-state-machine` · impact high · unhandled (not handled in controller) · verified 2026-10-09
  **While a restore is CREATING, sub-resource/tag/UpdateTable calls fail with ResourceNotFound/TableNotFound, not InUse**
  On the CREATING restore target DescribeTable succeeded (RestoreInProgress=true) but TagResource,
  ListTagsOfResource, UpdateTimeToLive, DescribeTimeToLive, UpdateTable(DeletionProtection),
  UpdateTable(Stream), PutResourcePolicy, GetResourcePolicy and DescribeContributorInsights all returned
  ResourceNotFoundException (HTTP 400, 'Requested resource not found: Table: <name> not found'), while
  UpdateContinuousBackups, DescribeContinuousBackups and CreateBackup returned TableNotFoundException ('Table
  not found: <name>'). CreateTable with the same name returned ResourceInUseException and
  RestoreTableFromBackup into the same name TableInUseException. Once ACTIVE all of these calls succeed.
  - ACK: requeue, synced.when, exceptions.404 · ops: TagResource, ListTagsOfResource, UpdateTimeToLive,
    DescribeTimeToLive, UpdateContinuousBackups, DescribeContinuousBackups, UpdateTable, PutResourcePolicy,
    GetResourcePolicy, CreateBackup, DescribeContributorInsights
  - repro: RestoreTableFromBackup -> immediately fire the sub-resource calls against the target name -> record
    codes
  - handling: not handled in the controller (as of commit 34b85e6)
  - related: [DDB-BACKUP-001](../backup.md#ddb-backup-001), [DDB-TABLE-298](../table-replicas.md#ddb-table-298), [DDB-IMPORT-001](../import.md#ddb-import-001), [DDB-TABLE-447](../service.md#ddb-table-447), [DDB-TABLE-219](../table-restore.md#ddb-table-219), [DDB-TABLE-275](../table-restore.md#ddb-table-275),
    [DDB-TABLE-100](../table-restore.md#ddb-table-100), [DDB-TABLE-444](../service.md#ddb-table-444), [DDB-IMPORT-019](../import.md#ddb-import-019), [DDB-IMPORT-018](../import.md#ddb-import-018) · hypotheses: H-B-137 · evidence:
    table/round-trip/restore-feature-carryover

## Notes

H-B-137 largely refuted: TagResource/ListTagsOfResource do NOT work during the restore, the update calls fail
with NotFound (not ResourceInUseException), DescribeTimeToLive does not return DISABLED but NotFound; only the
DescribeContinuousBackups -> TableNotFoundException part was right. A controller that maps
ResourceNotFoundException to 'resource gone' would wrongly delete/recreate its restoring Table.
