<!-- generated: ack-api-quirks dynamodb 2026-10-09 model=2012-08-10 controller=34b85e6 -->
# DDB-TABLE-100: Restore precedence: shape > PITR on > time > target exists; DELETING target -> TableAlreadyExists; DELETING source restorable for ~1 s
_Full entry and notes of one finding; its summary entry is in [table-restore.md](../table-restore.md).
Generated from ack-api-quirks `services/dynamodb` (model 2012-08-10, controller commit 34b85e6); edit the
finding in the lab, not here._

## Finding

- <a id="ddb-table-100"></a>**DDB-TABLE-100** `error-code` · impact high · unhandled (not handled in controller) · verified 2026-10-08
  **Restore precedence: shape > PITR on > time > target exists; DELETING target -> TableAlreadyExists; DELETING source restorable for ~1 s**
  PITR off: target exists -> PointInTimeRecoveryUnavailableException (HTTP 400) 'Point in time recovery is not
  enabled for table 'ackq-30dba0-e1''; bad time -> PointInTimeRecoveryUnavailableException (HTTP 400) 'Point
  in time recovery is not enabled for table 'ackq-30dba0-e1''; both time flags -> ValidationException (HTTP 400)
  'Invalid Request: Both RestoreDateTime and UseLatestRestorableTime cannot be set for point-in-time restore
  request. Pleas'; no time flag -> ValidationException (HTTP 400) 'Invalid Request: Either one of
  RestoreDateTime or UseLatestRestorableTime must be specified, but not both'; invalid target name ->
  ValidationException (HTTP 400) '1 validation error detected: Value 'bad name!' at 'targetTableName' failed
  to satisfy constraint: Member must satisfy re'. Missing source + target exists -> TableNotFoundException
  (HTTP 400) 'Table not found: ackq-30dba0-missing'. PITR on: target exists -> TableAlreadyExistsException
  (HTTP 400) 'Table already exists: ackq-30dba0-e2'; target==source -> TableAlreadyExistsException (HTTP 400)
  'Table already exists: ackq-30dba0-e1'; both time flags -> ValidationException (HTTP 400) 'Invalid Request:
  Both RestoreDateTime and UseLatestRestorableTime cannot be set for point-in-time restore request. Pleas'; no
  time flag -> ValidationException (HTTP 400) 'Invalid Request: Either one of RestoreDateTime or
  UseLatestRestorableTime must be specified, but not both'; target exists + bad time ->
  InvalidRestoreTimeException (HTTP 400) 'RestoreDateTime '2026-10-06T23:16:32Z' must be within
  EarliestRestorableDateTime '2026-10-08T23:16:33Z' and LatestRestor'; target DELETING ->
  TableAlreadyExistsException (HTTP 400) 'Table already exists: ackq-30dba0-e2'. Source DELETING: restore ->
  OK; DescribeContinuousBackups -> OK; DescribeTimeToLive -> ValidationException (HTTP 400) 'Cannot describe
  time to live while table is in DELETING state: Current table state is DELETING'; UpdateTimeToLive -> OK;
  UpdateContinuousBackups -> OK; UpdateContributorInsights -> OK; CreateBackup -> OK.
  - ACK: terminal_codes, requeue · ops: RestoreTableToPointInTime, CreateBackup, UpdateTimeToLive,
    UpdateContinuousBackups, UpdateContributorInsights
  - repro: see behavior
  - handling: not handled in the controller (as of commit 34b85e6)
  - related: [DDB-TABLE-087](../table-restore.md#ddb-table-087), [DDB-BACKUP-001](../backup.md#ddb-backup-001), [DDB-BACKUP-007](../backup.md#ddb-backup-007), [DDB-TABLE-118](../service.md#ddb-table-118), [DDB-TABLE-454](../table-subresources.md#ddb-table-454), [DDB-TABLE-455](../service.md#ddb-table-455),
    [DDB-TABLE-227](../table-replicas.md#ddb-table-227), [DDB-TABLE-228](../table-replicas.md#ddb-table-228), [DDB-TABLE-219](../table-restore.md#ddb-table-219), [DDB-TABLE-275](../table-restore.md#ddb-table-275), [DDB-TABLE-217](../table-restore.md#ddb-table-217), [DDB-TABLE-444](../service.md#ddb-table-444), [DDB-IMPORT-019](../import.md#ddb-import-019),
    [DDB-IMPORT-018](../import.md#ddb-import-018), [DDB-TABLE-272](../table-restore.md#ddb-table-272), [DDB-TABLE-274](../table-restore.md#ddb-table-274), [DDB-TABLE-086](../table-restore.md#ddb-table-086), [DDB-EXPORT-015](../export.md#ddb-export-015), [DDB-TABLE-218](../table-restore.md#ddb-table-218) · evidence:
    table/error-taxonomy/subresource-errors

## Notes

Hypotheses: H-S-124, H-S-004. H-S-124 mostly confirmed (Table*Exception vocabulary, all HTTP 400, synchronous)
with two corrections: a target name that is currently DELETING returns TableAlreadyExistsException (not
TableInUseException), and a SOURCE table that is DELETING (PITR on) is still restorable with
UseLatestRestorableTime=true (200; the restored table went ACTIVE and was deleted by the probe). Also while
the source was DELETING: UpdateTimeToLive -> 200, UpdateContinuousBackups -> 200, UpdateContributorInsights ->
200, DescribeContinuousBackups -> 200, CreateBackup -> 200 (an AVAILABLE USER backup of the dying table was
created and had to be deleted by hand), but DescribeTimeToLive -> ValidationException 'Cannot describe time to
live while table is in DELETING state'. Both RestoreDateTime and UseLatestRestorableTime, or neither ->
ValidationException (checked before anything else).

Contradiction with [DDB-TABLE-118](../service.md#ddb-table-118), [DDB-TABLE-454](../table-subresources.md#ddb-table-454), [DDB-TABLE-455](../service.md#ddb-table-455): 100/118 state without a time bound that a
DELETING source is 'still restorable' and accepts
CreateBackup/UpdateContinuousBackups/DescribeContinuousBackups; 454 got TableNotFoundException from
RestoreTableToPointInTime at +2.44 s and ExportTableToPointInTime at +1.71 s into DELETING, and 455 saw
DescribeContinuousBackups flip to TableNotFoundException at +1.03 s while DescribeTable still returned
DELETING until ~5 s Resolution: all true; the backup/PITR backend forgets the table ~1-1.6 s into DELETING, ~4
s before DescribeTable does - 454/455 canonical for the boundary, 100/118 retitled to carry the ~1 s qualifier
