<!-- generated: ack-api-quirks dynamodb 2026-10-09 model=2012-08-10 controller=34b85e6 -->
# Table restores (RestoreTableFromBackup, RestoreTableToPointInTime)
_Restore behavior on Table: what is and is not restored, overrides, admissibility while restoring, errors and durations._
Generated from ack-api-quirks `services/dynamodb` (render date in the marker above); model 2012-08-10 (service/dynamodb v1.39.8); controller commit 34b85e6; evidence: `services/dynamodb/probes/<probe id>/` in the lab repo.

## Overview

<!-- preserved:start id=overview -->
RestoreTableFromBackup and RestoreTableToPointInTime are alternative constructors of an ordinary Table: admission is synchronous, the target runs CREATING -> ACTIVE in 1.5-13 min, the overrides map onto Table spec fields, and nothing in DescribeTable marks the table as restored once it is ACTIVE ([DDB-TABLE-278](#ddb-table-278), [DDB-TABLE-216](#ddb-table-216)). The most surprising facts are that a restore silently drops streams, TTL, PITR, tags, deletion protection, resource policy and table class, that the CREATING target answers ResourceNotFoundException to tag/TTL/UpdateTable calls, and that OnDemandThroughputOverride={} returns TableInUseException yet starts the restore ([DDB-TABLE-215](#ddb-table-215), [DDB-TABLE-217](#ddb-table-217), [DDB-TABLE-463](#ddb-table-463)).

### Rules a reconciler must respect
- What survives: a restore (backup or PITR) keeps SSE (the AWS-managed KMS key is inherited without an override), billing mode and the recorded on-demand maxima; it drops streams, TTL, PITR, tags, deletion protection, resource policy and table class - re-apply them with ordinary UpdateTable/sub-resource calls after ACTIVE, which all succeed when waiting for ACTIVE between UpdateTable calls; nothing in the controller re-applies them ([DDB-TABLE-215](#ddb-table-215), [DDB-TABLE-218](#ddb-table-218)).
- No lineage: RestoreSummary {SourceBackupArn, SourceTableArn, RestoreDateTime, RestoreInProgress} exists only while CREATING and is absent from the first ACTIVE poll on; a backup of a restored table carries none either - the controller must persist the restore origin itself, and today models nothing of the restore ([DDB-TABLE-216](#ddb-table-216), [DDB-TABLE-278](#ddb-table-278)).
- While the target is CREATING, DescribeTable works but TagResource/ListTagsOfResource/TTL/UpdateTable/policy/insights return ResourceNotFoundException and ContinuousBackups/CreateBackup return TableNotFoundException; CreateTable with the name is ResourceInUseException, another restore into it TableInUseException, and DeleteTable is ResourceInUseException until ACTIVE - the controller's generic 404 mapping would read this window as 'table gone' ([DDB-TABLE-217](#ddb-table-217), [DDB-TABLE-277](#ddb-table-277)).
- Errors are synchronous HTTP 400 and create no target, in the order shape (both/neither time flag, exactly one of SourceTableArn/SourceTableName, name regex) > PITR enabled (PointInTimeRecoveryUnavailableException) > time (InvalidRestoreTimeException quoting both bounds) > target exists; TableAlreadyExistsException covers an ACTIVE and a DELETING target (the latter clears in seconds, so requeue rather than fail), TableInUseException only a target whose own restore is CREATING; a missing source is TableNotFoundException and another account's SourceTableArn ValidationException ([DDB-TABLE-100](#ddb-table-100), [DDB-TABLE-272](#ddb-table-272), [DDB-TABLE-219](#ddb-table-219), [DDB-TABLE-275](#ddb-table-275)).
- Override validation: ProvisionedThroughputOverride requires an effective PROVISIONED mode and vice versa, OnDemandThroughputOverride requires PAY_PER_REQUEST; index overrides must match a source index by KeySchema+Projection (renames allowed, new indexes 'cannot be created during the restore'); GlobalSecondaryIndexOverride=[]/LocalSecondaryIndexOverride=[] drop all indexes while omitting them restores all (nil and empty differ); ProvisionedThroughputOverride applies to the base only and BillingModeOverride=PAY_PER_REQUEST flips GSIs to 0/0; a nonexistent BackupArn is BackupNotFoundException, a table ARN 'Invalid Backup ARN', another account AccessDeniedException ([DDB-TABLE-273](#ddb-table-273), [DDB-TABLE-279](#ddb-table-279), [DDB-TABLE-405](#ddb-table-405); [DDB-TABLE-280](table-indexes.md#ddb-table-280), [DDB-TABLE-281](table-indexes.md#ddb-table-281), table-indexes.md).
- Cross-region restores need SSESpecificationOverride ('sseSpecificationOverride must be provided for cross-region restores', also in-region when the ARN region differs from the endpoint), work for BackupArn and SourceTableArn but not SourceTableName (TableNotFoundException), and the target is invisible from the source region; SSESpecificationOverride Enabled=false restores to the AWS-owned key; unusable keys fail synchronously with no target (HMAC/RSA -> deterministic HTTP 500 that a 5xx-retry loop never escapes, disabled/missing -> ValidationException, a foreign AWS-managed key -> AccessDeniedException) ([DDB-TABLE-274](#ddb-table-274), [DDB-TABLE-276](#ddb-table-276); [DDB-TABLE-461](table-streams-encryption-class.md#ddb-table-461), table-streams-encryption-class.md).
- OnDemandThroughputOverride={} on a first restore returns 400 TableInUseException 'already being restored' although the target exists and restores normally (3/3): on that text DescribeTable the target and adopt it instead of treating it as foreign; SSESpecificationOverride={} is a plain 200 ([DDB-TABLE-463](#ddb-table-463)).
- PITR window: right after enable EarliestRestorableDateTime == LatestRestorableDateTime == enable time for >= 3 min, yet UseLatestRestorableTime works at once (restoring to the enable instant with 0 items); RestoreDateTime outside the bounds is InvalidRestoreTimeException; a DELETING source is restorable only for ~1 s (TableNotFoundException at +2.4 s), and an export started in that first second still completes ([DDB-TABLE-086](#ddb-table-086), [DDB-TABLE-100](#ddb-table-100); [DDB-TABLE-454](table-subresources.md#ddb-table-454), table-subresources.md).
- Concurrency: 50 concurrent restores per account (the 51st is LimitExceededException 'Only 50 restore operations'), no visible per-second rate (15 in 0.6 s accepted), seven restores of one source with GSI+LSI all admitted (the documented one-index-table-CREATING rule is not enforced), but one backup feeds only one restore at a time (BackupInUseException) ([DDB-TABLE-350](#ddb-table-350), [DDB-TABLE-349](#ddb-table-349), [DDB-TABLE-282](#ddb-table-282); [DDB-TABLE-388](table-indexes.md#ddb-table-388), table-indexes.md).
- Delete side: deleting a PITR-enabled table creates the undeletable 35-day SYSTEM backup '<table>$DeletedTableBackup' ~8 s later unless UpdateContinuousBackups(disable) is issued while the table is DELETING; nothing in the controller disables PITR during DELETING ([DDB-TABLE-167](table-subresources.md#ddb-table-167), [DDB-TABLE-088](table-subresources.md#ddb-table-088), table-subresources.md).

### Timing you should expect
- In-region restore of empty/1-item tables: 81-721 s with no stable floor (n=12: [DDB-TABLE-282](#ddb-table-282) 81.5-212.3 s x7, [DDB-TABLE-218](#ddb-table-218) 343.9/345.6 s, [DDB-TABLE-463](#ddb-table-463) 211 s, [DDB-TABLE-186](#ddb-table-186) 464 s, [DDB-TABLE-277](#ddb-table-277) 721 s); 50 concurrent restores all ACTIVE in 222-259 s ([DDB-TABLE-350](#ddb-table-350)); cross-region 252 s (PITR) / 767 s (backup) ([DDB-TABLE-274](#ddb-table-274)).
- UseLatestRestorableTime first succeeds 8.4 s after enabling PITR ([DDB-TABLE-086](#ddb-table-086)); the PITR backend forgets a DELETING table at ~1.0-1.6 s, ~4 s before DescribeTable 404s ([DDB-TABLE-454](table-subresources.md#ddb-table-454), table-subresources.md).
- After ACTIVE, re-applying settings: stream UPDATING 4 s, TableClass UPDATING 6 s, the rest synchronous ([DDB-TABLE-218](#ddb-table-218)).

### Known handling gaps in the controller
- No finding rendered in this document is stored as suspect-bug or partial: restore is unmodeled, so the RestoreSummary transience, the ResourceNotFound-while-CREATING window, settings re-application, the TableAlreadyExists-while-DELETING requeue, the 50-concurrent cap and the ODT={} self-conflict above are all stored as unhandled.

### Scope verdict
field-on-parent: an immutable create-time group on Table (restoreFrom: backupArn | sourceTableArn/sourceTableName + restoreDateTime|useLatestRestorableTime, plus sseSpecificationOverride mandatory for cross-region and the billing/throughput/index overrides that map onto spec fields), because both APIs are synchronous-admission creators of an ordinary Table that leave no durable marker, so the controller must persist the restore origin itself ([DDB-TABLE-278](#ddb-table-278), [DDB-TABLE-216](#ddb-table-216), [DDB-TABLE-274](#ddb-table-274)).

### Where to look next
- The Backup resource, BackupInUseException from the backup side, SYSTEM backups and what a backup records ([DDB-BACKUP-010](backup.md#ddb-backup-010), [DDB-BACKUP-015](backup.md#ddb-backup-015), [DDB-BACKUP-009](backup.md#ddb-backup-009), backup.md); the PITR lifecycle ([DDB-TABLE-083](table-subresources.md#ddb-table-083), [DDB-TABLE-085](table-subresources.md#ddb-table-085), table-subresources.md); the other Table constructor and the other PITR consumer ([DDB-IMPORT-005](import.md#ddb-import-005), import.md; [DDB-EXPORT-015](export.md#ddb-export-015), export.md); NumberOfDecreasesToday resets at 00:00 UTC ([DDB-TABLE-185](table-throughput-billing.md#ddb-table-185), table-throughput-billing.md). Evidence: services/dynamodb/probes/table/round-trip/restore-*, table/error-taxonomy/restore-errors, table/creative/restore-*.

Entries below are generated from the lab findings; low-impact items are in the appendix, long notes under details/.
<!-- preserved:end -->

## At a glance

- canonical findings: 20 (high 12 / medium 5 / low 3); duplicates folded into the appendix: 2
- handling: handled 3 · partial 0 · tracked 0 · unhandled 17 · suspect-bug 0 · n-a 0 (tracked = handled/partial whose reference is an open GitHub issue; counted as not handled)
- re-verified: 0 · last_verified: 2026-10-08..2026-10-09 · model: 2012-08-10 (service/dynamodb v1.39.8)
- categories: error-code 4, request-validation 3, async-state-machine 2, other 2, quota-limit 2, read-gap 2,
  cross-region 1, delete-semantics 1, response-fidelity 1, scope 1, server-default 1

## Operations

| operation | kind | required inputs | declared error shapes | paginated |
| --- | --- | --- | --- | --- |
| CreateBackup | create | TableName, BackupName | TableNotFoundException, TableInUseException, ContinuousBackupsUnavailableException, BackupInUseException, LimitExceededException, InternalServerError | no |
| CreateTable | create | TableName | ResourceInUseException, LimitExceededException, InternalServerError | no |
| DeleteBackup | delete | BackupArn | BackupNotFoundException, BackupInUseException, LimitExceededException, InternalServerError | no |
| DeleteTable | delete | TableName | ResourceInUseException, ResourceNotFoundException, LimitExceededException, InternalServerError | no |
| DescribeBackup | read | BackupArn | BackupNotFoundException, InternalServerError | no |
| DescribeContinuousBackups | read | TableName | TableNotFoundException, InternalServerError | no |
| DescribeContributorInsights | read | TableName | ResourceNotFoundException, InternalServerError | no |
| DescribeTable | read | TableName | ResourceNotFoundException, InternalServerError | no |
| DescribeTimeToLive | read | TableName | ResourceNotFoundException, InternalServerError | no |
| GetResourcePolicy | read | ResourceArn | ResourceNotFoundException, InternalServerError, PolicyNotFoundException | no |
| ListTagsOfResource | list | ResourceArn | ResourceNotFoundException, InternalServerError | yes |
| PutResourcePolicy | create | ResourceArn, Policy | ResourceNotFoundException, InternalServerError, LimitExceededException, PolicyNotFoundException, ResourceInUseException | no |
| RestoreTableFromBackup | create | TargetTableName, BackupArn | TableAlreadyExistsException, TableInUseException, BackupNotFoundException, BackupInUseException, LimitExceededException, InternalServerError | no |
| RestoreTableToPointInTime | create | TargetTableName | TableAlreadyExistsException, TableNotFoundException, TableInUseException, LimitExceededException, InvalidRestoreTimeException, PointInTimeRecoveryUnavailableException, InternalServerError | no |
| TagResource | tag | ResourceArn, Tags | LimitExceededException, ResourceNotFoundException, InternalServerError, ResourceInUseException | no |
| UpdateContinuousBackups | update | TableName, PointInTimeRecoverySpecification | TableNotFoundException, ContinuousBackupsUnavailableException, InternalServerError | no |
| UpdateContributorInsights | update | TableName, ContributorInsightsAction | ResourceNotFoundException, InternalServerError | no |
| UpdateTable | update | TableName | ResourceInUseException, ResourceNotFoundException, LimitExceededException, InternalServerError | no |
| UpdateTimeToLive | update | TableName, TimeToLiveSpecification | ResourceInUseException, ResourceNotFoundException, LimitExceededException, InternalServerError | no |

## State machine

- **IndexStatus**: CREATING, UPDATING, DELETING, ACTIVE (transitional: CREATING, UPDATING, DELETING)
- **PointInTimeRecoveryStatus**: ENABLED, DISABLED
- **ReplicaStatus**: CREATING, CREATION_FAILED, UPDATING, DELETING, ACTIVE, REGION_DISABLED,
  INACCESSIBLE_ENCRYPTION_CREDENTIALS, ARCHIVING, ARCHIVED, REPLICATION_NOT_AUTHORIZED (transitional:
  CREATING, UPDATING, DELETING, ARCHIVING)
- **SSEStatus**: ENABLING, ENABLED, DISABLING, DISABLED, UPDATING (transitional: ENABLING, DISABLING,
  UPDATING)
- **TableStatus**: CREATING, UPDATING, DELETING, ACTIVE, INACCESSIBLE_ENCRYPTION_CREDENTIALS, ARCHIVING,
  ARCHIVED, REPLICATION_NOT_AUTHORIZED (transitional: CREATING, UPDATING, DELETING, ARCHIVING)
- **WitnessStatus**: CREATING, DELETING, ACTIVE (transitional: CREATING, DELETING)

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
  - related: [DDB-BACKUP-001](backup.md#ddb-backup-001), [DDB-TABLE-298](table-replicas.md#ddb-table-298), [DDB-IMPORT-001](import.md#ddb-import-001), [DDB-TABLE-447](service.md#ddb-table-447), [DDB-TABLE-219](#ddb-table-219), [DDB-TABLE-275](#ddb-table-275),
    [DDB-TABLE-100](#ddb-table-100), [DDB-TABLE-444](service.md#ddb-table-444), [DDB-IMPORT-019](import.md#ddb-import-019), [DDB-IMPORT-018](import.md#ddb-import-018) · hypotheses: H-B-137 · evidence:
    table/round-trip/restore-feature-carryover
  - notes: H-B-137 largely refuted: TagResource/ListTagsOfResource do NOT work during the restore, the update
    calls fail with NotFound (not ResourceInUseException), DescribeTimeToLive does not return DISABLED but
    NotFound; only the DescribeContinuousBackups -> TableNotFoundException part was right. A...
  - full notes: [details/DDB-TABLE-217.md](details/DDB-TABLE-217.md)

- <a id="ddb-table-218"></a>**DDB-TABLE-218** `async-state-machine` · impact medium · handled · verified 2026-10-09
  **Empty/1-item restores take 1.5-8 min in-region (backup and PITR alike, no stable floor); cross-region 4-13 min; settings re-apply cleanly**
  RestoreTableFromBackup of a 1-item table reached ACTIVE after 343.9 s,
  RestoreTableToPointInTime(UseLatestRestorableTime) of the same source after 345.6 s (started 1 s after
  enabling PITR, EarliestRestorableDateTime==LatestRestorableDateTime). After ACTIVE, UpdateTable(stream)
  [UPDATING 4 s], UpdateTimeToLive, UpdateContinuousBackups, TagResource, UpdateTable(deletion protection)
  [stays ACTIVE], UpdateTable(TableClass) [UPDATING 6 s], UpdateTable(OnDemandThroughput) and
  PutResourcePolicy all succeeded in sequence with no ResourceInUseException when waiting for ACTIVE between
  UpdateTable calls.
  - ACK: post-create-nudge, e2e-timing, requeue · ops: RestoreTableFromBackup, RestoreTableToPointInTime,
    UpdateTable, UpdateTimeToLive, UpdateContinuousBackups, TagResource, PutResourcePolicy
  - repro: restore -> wait ACTIVE -> apply each non-restored setting in sequence
  - measurements: restore_from_backup_s=343.9, restore_pitr_s=345.6, stream_updating_s=4.0,
    table_class_updating_s=6.1
  - handling: handled via `templates/hooks/table/sdk_create_post_set_output.go.tpl:1-4; bcd26e1`
  - related: [DDB-TABLE-186](#ddb-table-186), [DDB-TABLE-274](#ddb-table-274), [DDB-TABLE-282](#ddb-table-282), [DDB-TABLE-463](#ddb-table-463), [DDB-TABLE-278](#ddb-table-278), [DDB-TABLE-215](#ddb-table-215),
    [DDB-BACKUP-009](backup.md#ddb-backup-009), [DDB-BACKUP-021](backup.md#ddb-backup-021), [DDB-BACKUP-004](backup.md#ddb-backup-004), [DDB-TABLE-230](table-replicas.md#ddb-table-230), [DDB-TABLE-272](#ddb-table-272), [DDB-TABLE-100](#ddb-table-100),
    [DDB-TABLE-087](#ddb-table-087), [DDB-TABLE-086](#ddb-table-086), [DDB-EXPORT-015](export.md#ddb-export-015) · hypotheses: H-B-024, H-B-040 · evidence:
    table/round-trip/restore-feature-carryover
  - notes: H-B-024 refuted (6 min, not <3 min). H-B-040's 'alternative constructor followed by ordinary
    reconciliation' is confirmed operationally; RestoreTableToPointInTime works immediately after enabling
    PITR (UseLatestRestorableTime resolved to the enable time).

## Errors

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
  - related: [DDB-TABLE-087](#ddb-table-087), [DDB-BACKUP-001](backup.md#ddb-backup-001), [DDB-BACKUP-007](backup.md#ddb-backup-007), [DDB-TABLE-118](service.md#ddb-table-118), [DDB-TABLE-454](table-subresources.md#ddb-table-454), [DDB-TABLE-455](service.md#ddb-table-455),
    [DDB-TABLE-227](table-replicas.md#ddb-table-227), [DDB-TABLE-228](table-replicas.md#ddb-table-228), [DDB-TABLE-219](#ddb-table-219), [DDB-TABLE-275](#ddb-table-275), [DDB-TABLE-217](#ddb-table-217), [DDB-TABLE-444](service.md#ddb-table-444), [DDB-IMPORT-019](import.md#ddb-import-019),
    [DDB-IMPORT-018](import.md#ddb-import-018), [DDB-TABLE-272](#ddb-table-272), [DDB-TABLE-274](#ddb-table-274), [DDB-TABLE-086](#ddb-table-086), [DDB-EXPORT-015](export.md#ddb-export-015), [DDB-TABLE-218](#ddb-table-218) · evidence:
    table/error-taxonomy/subresource-errors
  - notes: Hypotheses: H-S-124, H-S-004. H-S-124 mostly confirmed (Table*Exception vocabulary, all HTTP 400,
    synchronous) with two corrections: a target name that is currently DELETING returns
    TableAlreadyExistsException (not TableInUseException), and a SOURCE table that is DELETING (PITR on) is
    still...
  - full notes: [details/DDB-TABLE-100.md](details/DDB-TABLE-100.md)

- <a id="ddb-table-219"></a>**DDB-TABLE-219** `error-code` · impact high · unhandled (not handled in controller) · verified 2026-10-09
  **Restore target name conflicts: TableAlreadyExistsException for an ACTIVE table, TableInUseException for a CREATING restore target**
  RestoreTableFromBackup with TargetTableName of an existing ACTIVE table returned TableAlreadyExistsException
  (HTTP 400, 'Table already exists: <name>'); with the name of a table whose own restore was still CREATING it
  returned TableInUseException ('Table: <name> is already being restored from backup: <arn>'). CreateTable
  with the restoring name returned ResourceInUseException ('Table is being used: <name>').
  - ACK: terminal_codes, requeue, custom_create · ops: RestoreTableFromBackup, CreateTable · fields:
    TargetTableName
  - repro: RestoreTableFromBackup into an ACTIVE table's name; into a CREATING restore target's name;
    CreateTable with the restoring name
  - handling: not handled in the controller (as of commit 34b85e6)
  - related: [DDB-TABLE-275](#ddb-table-275), [DDB-TABLE-100](#ddb-table-100), [DDB-TABLE-217](#ddb-table-217), [DDB-TABLE-444](service.md#ddb-table-444), [DDB-IMPORT-019](import.md#ddb-import-019), [DDB-IMPORT-018](import.md#ddb-import-018),
    [DDB-TABLE-463](#ddb-table-463), [DDB-TABLE-457](service.md#ddb-table-457), [DDB-TABLE-276](#ddb-table-276), [DDB-TABLE-461](table-streams-encryption-class.md#ddb-table-461) · hypotheses: H-B-017 · evidence:
    table/round-trip/restore-feature-carryover
  - notes: H-B-017 confirmed for ACTIVE and CREATING; the DELETING case is covered by
    table/error-taxonomy/restore-errors.

- <a id="ddb-table-275"></a>**DDB-TABLE-275** `error-code` · impact medium · unhandled (not handled in controller) · verified 2026-10-09
  **Restoring into the name of a DELETING table fails with TableAlreadyExistsException, not TableInUseException**
  Immediately after DeleteTable returned (TableStatus=DELETING), RestoreTableFromBackup with that
  TargetTableName returned TableAlreadyExistsException (HTTP 400, 'Table already exists: <name>');
  RestoreTableToPointInTime from/into the same DELETING table returned PointInTimeRecoveryUnavailableException
  (PITR was off). TableInUseException was only observed for a target whose own restore is CREATING.
  - ACK: requeue, terminal_codes · ops: RestoreTableFromBackup, RestoreTableToPointInTime · fields:
    TargetTableName
  - repro: DeleteTable X -> immediately RestoreTableFromBackup(TargetTableName=X)
  - handling: not handled in the controller (as of commit 34b85e6)
  - related: [DDB-TABLE-219](#ddb-table-219), [DDB-TABLE-100](#ddb-table-100), [DDB-TABLE-217](#ddb-table-217), [DDB-TABLE-444](service.md#ddb-table-444), [DDB-IMPORT-019](import.md#ddb-import-019), [DDB-IMPORT-018](import.md#ddb-import-018) ·
    hypotheses: H-B-017 · evidence: table/error-taxonomy/restore-errors
  - notes: H-B-017 refuted for the DELETING case: TableAlreadyExistsException is therefore NOT always terminal
    (the name frees up seconds later); a controller should requeue on it while a same-named table is DELETING.

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
  being restored') is otherwise TRANSIENT-WAIT-FOR-STATE (TableStatus=ACTIVE) as [DDB-TABLE-219](#ddb-table-219) describes.
  - ACK: custom_create, custom_find, requeue · ops: RestoreTableFromBackup · fields:
    OnDemandThroughputOverride, SSESpecificationOverride, BillingModeOverride
  - repro: CreateBackup (empty PPR table) -> RestoreTableFromBackup BackupArn=<arn> TargetTableName=<new>
    OnDemandThroughputOverride={} -> 400 TableInUseException; DescribeTable <new> -> CREATING with
    RestoreSummary
  - measurements: first_call_latency_ms=[347, 210], replay_latency_ms=37, restore_creating_s=211.4
  - handling: not handled in the controller (as of commit 34b85e6)
  - related: [DDB-TABLE-219](#ddb-table-219), [DDB-TABLE-275](#ddb-table-275), [DDB-TABLE-437](table-throughput-billing.md#ddb-table-437), [DDB-TABLE-178](table-throughput-billing.md#ddb-table-178), [DDB-TABLE-273](#ddb-table-273), [DDB-TABLE-457](service.md#ddb-table-457),
    [DDB-TABLE-276](#ddb-table-276), [DDB-TABLE-461](table-streams-encryption-class.md#ddb-table-461), [DDB-TABLE-186](#ddb-table-186), [DDB-TABLE-218](#ddb-table-218), [DDB-TABLE-274](#ddb-table-274), [DDB-TABLE-282](#ddb-table-282), [DDB-TABLE-278](#ddb-table-278) ·
    evidence: table/creative/restore-odt-empty-side-effect, table/creative/degenerate-5xx-hunt
  - notes: Extends [DDB-TABLE-219](#ddb-table-219)/275 (TableInUseException for CREATING restore targets) with a self-inflicted
    first-call variant, and [DDB-TABLE-437](table-throughput-billing.md#ddb-table-437)/178 ({} throughput structs) to the restore API, where the
    consequence is a 4xx-with-side-effect instead of a 500. Rows: t1_odt_empty -> 400 TableInUseException...
  - full notes: [details/DDB-TABLE-463.md](details/DDB-TABLE-463.md)

## Request validation

- <a id="ddb-table-272"></a>**DDB-TABLE-272** `request-validation` · impact high · unhandled (not handled in controller) · verified 2026-10-09
  **RestoreTableToPointInTime request-shape errors are synchronous ValidationException/InvalidRestoreTime/PITRUnavailable; no target is created**
  Codes observed (all HTTP 400, DescribeTable(TargetTableName) -> ResourceNotFoundException afterwards):
  PITR-disabled source -> PointInTimeRecoveryUnavailableException "Point in time recovery is not enabled for
  table '<name>'" (for both UseLatestRestorableTime and RestoreDateTime); SourceTableName+SourceTableArn ->
  ValidationException 'Must provide exactly one of: sourceTableArn, sourceTableName'; neither ->
  ValidationException "The parameter 'TableName' is required"; UseLatestRestorableTime=true+RestoreDateTime ->
  ValidationException 'Both RestoreDateTime and UseLatestRestorableTime cannot be set';
  UseLatestRestorableTime=false or no time field -> ValidationException 'Either one of RestoreDateTime or
  UseLatestRestorableTime must be specified'; RestoreDateTime in the future, before EarliestRestorableDateTime
  or epoch 0 -> InvalidRestoreTimeException quoting both bounds; nonexistent SourceTableName ->
  TableNotFoundException; malformed SourceTableArn -> ValidationException 'sourceTableArn is not a valid ARN';
  SourceTableArn of another account -> ValidationException 'This action is only supported by accounts that
  match the resource owner's account'; TargetTableName with bad characters -> ValidationException (regex);
  2-char name rejected client-side.
  - ACK: terminal_codes, custom_create · ops: RestoreTableToPointInTime · fields: SourceTableName,
    SourceTableArn, UseLatestRestorableTime, RestoreDateTime, TargetTableName
  - repro: issue each malformed variant against a PITR-enabled and a PITR-disabled empty table; DescribeTable
    target afterwards
  - handling: not handled in the controller (as of commit 34b85e6)
  - related: [DDB-TABLE-100](#ddb-table-100), [DDB-TABLE-087](#ddb-table-087), [DDB-TABLE-274](#ddb-table-274), [DDB-TABLE-086](#ddb-table-086), [DDB-EXPORT-015](export.md#ddb-export-015), [DDB-TABLE-218](#ddb-table-218),
    [DDB-EXPORT-001](export.md#ddb-export-001), [DDB-EXPORT-002](export.md#ddb-export-002), [DDB-TABLE-447](service.md#ddb-table-447), [DDB-BACKUP-012](backup.md#ddb-backup-012), [DDB-TABLE-273](#ddb-table-273) · hypotheses: H-B-023 ·
    evidence: table/error-taxonomy/restore-errors
  - notes: H-B-023 confirmed in full. Right after enabling PITR, EarliestRestorableDateTime ==
    LatestRestorableDateTime == enable time, so only that exact instant (or UseLatestRestorableTime) is valid.

- <a id="ddb-table-273"></a>**DDB-TABLE-273** `request-validation` · impact high · unhandled (not handled in controller) · verified 2026-10-09
  **RestoreTableFromBackup override combinations are validated synchronously (ValidationException) incl. 'cannot create new indexes at restore'**
  HTTP 400 ValidationException messages: ProvisionedThroughputOverride without BillingModeOverride on a
  PAY_PER_REQUEST backup -> 'Cannot override ProvisionedThroughput unless BillingMode is PROVISIONED';
  BillingModeOverride=PROVISIONED without ProvisionedThroughputOverride -> 'Must override
  ProvisionedThroughput if BillingMode is overridden to PROVISIONED'; OnDemandThroughputOverride 0/0 ->
  'Requested MaxReadRequestUnits for OnDemandThroughput is outside of valid range';
  GlobalSecondaryIndexOverride naming an index absent from the backup -> 'Index <n> does not match a secondary
  index that existed in the source table and cannot be created during the restore operation';
  BillingModeOverride='BOGUS' -> enum ValidationException; SSESpecificationOverride with a nonexistent KMS
  alias -> ValidationException 'KMS validation error: ...NotFoundException: Alias ... is not found'.
  Nonexistent BackupArn -> BackupNotFoundException; a table ARN as BackupArn -> ValidationException 'Invalid
  Backup ARN'; another account's ARN -> AccessDeniedException. No target table exists after any of these.
  - ACK: terminal_codes, custom_create · ops: RestoreTableFromBackup · fields: BillingModeOverride,
    ProvisionedThroughputOverride, OnDemandThroughputOverride, GlobalSecondaryIndexOverride,
    SSESpecificationOverride, BackupArn
  - repro: RestoreTableFromBackup with each invalid override combination against an empty-table backup
  - handling: not handled in the controller (as of commit 34b85e6)
  - related: [DDB-TABLE-280](table-indexes.md#ddb-table-280), [DDB-TABLE-279](#ddb-table-279), [DDB-TABLE-281](table-indexes.md#ddb-table-281), [DDB-BACKUP-021](backup.md#ddb-backup-021), [DDB-BACKUP-017](backup.md#ddb-backup-017), [DDB-EXPORT-001](export.md#ddb-export-001),
    [DDB-EXPORT-002](export.md#ddb-export-002), [DDB-TABLE-447](service.md#ddb-table-447), [DDB-BACKUP-012](backup.md#ddb-backup-012), [DDB-TABLE-274](#ddb-table-274), [DDB-TABLE-272](#ddb-table-272), [DDB-BACKUP-005](backup.md#ddb-backup-005) · hypotheses:
    H-B-138, H-B-021, H-B-108 · evidence: table/error-taxonomy/restore-errors
  - notes: Confirms the synchronous half of H-B-138/H-B-021 (new indexes rejected; throughput overrides tied
    to the effective billing mode). KMS key validity IS checked synchronously (relevant to H-B-108).

- <a id="ddb-table-279"></a>**DDB-TABLE-279** `request-validation` · impact high · unhandled (not handled in controller) · verified 2026-10-09
  **GlobalSecondaryIndexOverride=[] / LocalSecondaryIndexOverride=[] drop all indexes on restore; omitting the parameter restores them**
  RestoreTableFromBackup with explicit empty lists GlobalSecondaryIndexOverride=[] and
  LocalSecondaryIndexOverride=[] produced an ACTIVE table with no GSIs and no LSIs; the same backup restored
  with the parameters omitted produced gsi1 (recorded throughput 5/5) and lsi1. Index-bearing and index-less
  restores of the same backup differ only by these empty lists.
  - ACK: custom_create, compare.nil_equals_zero_value, is_immutable · ops: RestoreTableFromBackup · fields:
    GlobalSecondaryIndexOverride, LocalSecondaryIndexOverride
  - repro: RestoreTableFromBackup(GlobalSecondaryIndexOverride=[], LocalSecondaryIndexOverride=[]) vs
    RestoreTableFromBackup() of a backup with 1 GSI + 1 LSI
  - handling: not handled in the controller (as of commit 34b85e6)
  - related: [DDB-TABLE-273](#ddb-table-273), [DDB-TABLE-280](table-indexes.md#ddb-table-280), [DDB-TABLE-281](table-indexes.md#ddb-table-281), [DDB-BACKUP-021](backup.md#ddb-backup-021), [DDB-BACKUP-017](backup.md#ddb-backup-017) · hypotheses: H-B-021
    · evidence: table/round-trip/restore-overrides
  - notes: H-B-021 confirmed: nil vs empty slice is semantically different on this API; a Go client must send
    an explicit empty array (no omitempty) to express 'no indexes'.

## Field behavior (defaults, normalization, shapes, immutability)

- <a id="ddb-table-215"></a>**DDB-TABLE-215** `read-gap` · impact high · unhandled (not handled in controller) · verified 2026-10-09
  **Restore drops streams, TTL, PITR, tags, deletion protection, resource policy and table class; keeps SSE, billing mode and on-demand maxima**
  RestoreTableFromBackup (no overrides) and RestoreTableToPointInTime(UseLatestRestorableTime) of the
  full-featured source produced identical ACTIVE tables: StreamSpecification absent (no LatestStreamArn),
  DescribeTimeToLive=DISABLED, PointInTimeRecoveryStatus=DISABLED, ListTagsOfResource=[], GetResourcePolicy ->
  PolicyNotFoundException, TableClassSummary absent (STANDARD), DeletionProtectionEnabled=false; but
  SSEDescription (ENABLED/KMS/same aws/dynamodb key ARN), BillingMode PAY_PER_REQUEST and OnDemandThroughput
  {MaxReadRequestUnits:100, MaxWriteRequestUnits:50} were carried over. Items: the backup restore had 1 item;
  the PITR restore to LatestRestorableDateTime (= PITR enable time) had 0.
  - ACK: post-create-nudge, custom_create, compare.is_ignored+delta_pre_compare · ops: RestoreTableFromBackup,
    RestoreTableToPointInTime, DescribeTable, DescribeTimeToLive, DescribeContinuousBackups,
    ListTagsOfResource, GetResourcePolicy · fields: StreamSpecification, TimeToLiveSpecification,
    PointInTimeRecoverySpecification, Tags, DeletionProtectionEnabled, ResourcePolicy, TableClass,
    SSESpecification, OnDemandThroughput
  - repro: full-featured source -> CreateBackup -> RestoreTableFromBackup -> wait ACTIVE -> DescribeTable +
    sub-resource Describes; diff against source
  - handling: not handled in the controller (as of commit 34b85e6)
  - related: [DDB-BACKUP-009](backup.md#ddb-backup-009), [DDB-BACKUP-021](backup.md#ddb-backup-021), [DDB-BACKUP-004](backup.md#ddb-backup-004), [DDB-TABLE-218](#ddb-table-218), [DDB-TABLE-230](table-replicas.md#ddb-table-230) · hypotheses:
    H-B-016, H-B-020, H-B-104, H-B-106 · evidence: table/round-trip/restore-feature-carryover
  - notes: H-B-016 and H-B-020 confirmed; H-B-104(a) confirmed (recorded on-demand maxima are re-applied
    without an override); H-B-106 REFUTED for the AWS-managed key: SSE (KMS) IS inherited by the restored
    table without SSESpecificationOverride (customer-managed key not tested).

- <a id="ddb-table-216"></a>**DDB-TABLE-216** `read-gap` · impact high · unhandled (not handled in controller) · verified 2026-10-09
  **RestoreSummary disappears from DescribeTable once the restored table is ACTIVE; nothing marks a table as born from a restore**
  While the restore target was CREATING, DescribeTable returned RestoreSummary {SourceBackupArn,
  SourceTableArn, RestoreDateTime, RestoreInProgress:true} (72 polls). From the first ACTIVE poll onwards the
  RestoreSummary member was absent entirely (181 polls, also after later UpdateTable calls and for the
  PITR-restored table and a cross-region restore in a sibling probe). A backup taken of the restored table
  carries no lineage either (SourceTableDetails.TableCreationDateTime = the restored table's own
  CreationDateTime).
  - ACK: custom_find, annotation-shadow-state, is_read_only · ops: RestoreTableFromBackup,
    RestoreTableToPointInTime, DescribeTable, DescribeBackup · fields: RestoreSummary,
    RestoreSummary.RestoreInProgress
  - repro: RestoreTableFromBackup -> DescribeTable every 5 s through CREATING -> ACTIVE; inspect
    RestoreSummary
  - handling: not handled in the controller (as of commit 34b85e6)
  - related: [DDB-IMPORT-005](import.md#ddb-import-005), [DDB-TABLE-278](#ddb-table-278), [DDB-IMPORT-008](import.md#ddb-import-008), [DDB-EXPORT-020](export.md#ddb-export-020) · hypotheses: H-B-040, H-B-102 ·
    evidence: table/round-trip/restore-feature-carryover
  - notes: Refutes the 'durable marker' part of H-B-040: after ACTIVE a controller cannot tell a restored
    table from a created one and must persist restoreFrom in its own status/annotations. H-B-102 confirmed (no
    lineage in backups of restored tables).

- <a id="ddb-table-276"></a>**DDB-TABLE-276** `server-default` · impact medium · unhandled (not handled in controller) · verified 2026-10-09
  **SSESpecificationOverride Enabled=false is accepted and restores to the default (AWS-owned key: no SSEDescription)**
  RestoreTableFromBackup with SSESpecificationOverride={Enabled:false} on an unencrypted (AWS-owned key)
  backup returned 200 CREATING; the restore response had no SSEDescription and the ACTIVE table reported
  SSEDescription=None.
  - ACK: compare.nil_equals_zero_value, none · ops: RestoreTableFromBackup, DescribeTable · fields:
    SSESpecificationOverride.Enabled, SSEDescription
  - repro: RestoreTableFromBackup(SSESpecificationOverride={Enabled:false})
  - handling: not handled in the controller (as of commit 34b85e6)
  - related: [DDB-TABLE-463](#ddb-table-463), [DDB-TABLE-457](service.md#ddb-table-457), [DDB-TABLE-461](table-streams-encryption-class.md#ddb-table-461), [DDB-TABLE-219](#ddb-table-219) · hypotheses: H-B-106 · evidence:
    table/error-taxonomy/restore-errors
  - notes: Companion to the SSE-inheritance finding in table/round-trip/restore-feature-carryover.

## Response fidelity and consistency

- <a id="ddb-table-086"></a>**DDB-TABLE-086** `response-fidelity` · impact medium · handled · verified 2026-10-08
  **After enable Earliest == Latest == enable time, pinned for >= 3 min; UseLatestRestorableTime restore nevertheless works at once**
  Enable response: Earliest=Latest=2026-10-08T23:04:41Z (0.9s before the read). Samples after a re-enable at
  23:05:24Z: at +0.1s/+30s/+90s/+180s both timestamps still read 23:05:24Z (lag vs wall clock 0.3 -> 180.4 s),
  i.e. LatestRestorableDateTime does not advance at all in the first 3 minutes (consistent with Latest =
  max(enable time, now - ~5 min)). Both fields are absent when PITR is DISABLED
  (PointInTimeRecoveryDescription has only PointInTimeRecoveryStatus).
  RestoreTableToPointInTime(UseLatestRestorableTime=true) succeeded 8.4 s after enable and again at 180 s
  (restoring to the enable instant; the restored tables were ACTIVE with 0 items), RestoreDateTime 1h before /
  1h after the window -> InvalidRestoreTimeException 'RestoreDateTime ... must be within
  EarliestRestorableDateTime ... and LatestRestorableDateTime ...'.
  - ACK: compare.is_ignored+delta_pre_compare, is_read_only · ops: DescribeContinuousBackups,
    RestoreTableToPointInTime · fields: PointInTimeRecoveryDescription.EarliestRestorableDateTime,
    PointInTimeRecoveryDescription.LatestRestorableDateTime
  - repro: enable PITR; DescribeContinuousBackups at 0/30/90/180s;
    RestoreTableToPointInTime(UseLatestRestorableTime=true)
  - measurements: latest_lag_s_at_0_30_90_180s=[0.3, 30.4, 90.4, 180.4],
    restore_use_latest_first_success_after_enable_s=8.4
  - handling: handled via `pkg/resource/table/hooks.go:697-718; pkg/resource/table/hooks_continuous_backup.go:36-44`
  - related: [DDB-TABLE-272](#ddb-table-272), [DDB-TABLE-100](#ddb-table-100), [DDB-TABLE-087](#ddb-table-087), [DDB-TABLE-274](#ddb-table-274), [DDB-EXPORT-015](export.md#ddb-export-015), [DDB-TABLE-218](#ddb-table-218) ·
    evidence: table/sub-resources/pitr-lifecycle
  - notes: Hypotheses: H-S-102, H-S-110. H-S-102 partially confirmed (presence rules; pinned Earliest) but
    Latest does not 'trail by ~5 min' early on - it is pinned to the enable time. H-S-110 refuted:
    UseLatestRestorableTime restore succeeds immediately after enable.

## Delete semantics

- <a id="ddb-table-277"></a>**DDB-TABLE-277** `delete-semantics` · impact high · handled · verified 2026-10-09
  **DeleteTable on a table whose restore is still CREATING: rejected with ResourceInUseException until ACTIVE**
  DeleteTable attempts against a CREATING restore target returned: 0.0s ResourceInUseException; 20.2s
  ResourceInUseException; 40.3s ResourceInUseException; 60.4s ResourceInUseException. Backup status
  during/after: AVAILABLE; DeleteBackup after the attempts -> BackupInUseException; after the target was gone
  -> 200.
  - ACK: deletable.when, requeue · ops: DeleteTable, DeleteBackup, RestoreTableFromBackup
  - repro: RestoreTableFromBackup -> DeleteTable(target) immediately and every 20 s
  - measurements: delete_timeline=[{"duration_s":720.74,"from_s":0.02,"to_s":720.76,"value":"CREATING"},
    {"duration_s":null,"from_s":720.76,"to_s":null,"value":"ACTIVE"}]
  - handling: handled via `generator.yaml:104-109; pkg/resource/table/hooks.go:72-93; generator.yaml:141-144; pkg/resource/backup/sdk.go:82-84`
  - related: [DDB-BACKUP-010](backup.md#ddb-backup-010), [DDB-TABLE-282](#ddb-table-282), [DDB-IMPORT-006](import.md#ddb-import-006), [DDB-TABLE-298](table-replicas.md#ddb-table-298) · hypotheses: H-B-137 · evidence:
    table/error-taxonomy/restore-errors
  - notes: Completes the admissibility picture for a restoring table (see
    table/round-trip/restore-feature-carryover for the other operations).

## Quotas and rate limits

- <a id="ddb-table-282"></a>**DDB-TABLE-282** `quota-limit` · impact medium · unhandled (not handled in controller) · verified 2026-10-09
  **Seven concurrent restores of one source (six with a GSI+LSI) were all admitted: the one-index-table-CREATING rule did not bite restores**
  Seven RestoreTableFromBackup calls (each from its own backup of the same table, six restoring gsi1+lsi1)
  issued within ~2 s all returned 200 CREATING with no LimitExceededException; they reached ACTIVE after
  Ra=158.1, Rb=173.1, Rc=81.5, Rd=212.3, Re=212.2, Rf=212.0, Rg=211.8 s (restore of the same backup twice is
  what fails, with BackupInUseException). A GSI on a CREATING restore target is listed without IndexStatus.
  - ACK: none, e2e-timing · ops: RestoreTableFromBackup
  - repro: 7 backups of a GSI table -> 7 RestoreTableFromBackup calls back-to-back
  - measurements: restore_since_launch_s.Ra=158.1, restore_since_launch_s.Rb=173.1,
    restore_since_launch_s.Rc=81.5, restore_since_launch_s.Rd=212.3, restore_since_launch_s.Re=212.2,
    restore_since_launch_s.Rf=212.0, restore_since_launch_s.Rg=211.8
  - handling: not handled in the controller (as of commit 34b85e6)
  - related: [DDB-BACKUP-010](backup.md#ddb-backup-010), [DDB-TABLE-277](#ddb-table-277), [DDB-TABLE-186](#ddb-table-186), [DDB-TABLE-218](#ddb-table-218), [DDB-TABLE-274](#ddb-table-274), [DDB-TABLE-463](#ddb-table-463),
    [DDB-TABLE-278](#ddb-table-278), [DDB-TABLE-443](service.md#ddb-table-443) · hypotheses: H-B-146 · evidence: table/round-trip/restore-overrides
  - notes: Not a quota test of the 50-restore limit (H-B-146 untested), but relevant to the lab's index-table
    lock: restores with indexes did not conflict with each other. Restore durations for empty tables ranged
    81-212 s here vs 136-766 s elsewhere in this shard: no stable floor.

## Cross-region

- <a id="ddb-table-274"></a>**DDB-TABLE-274** `cross-region` · impact high · unhandled (not handled in controller) · verified 2026-10-09
  **Cross-region restore works for both BackupArn and SourceTableArn when SSESpecificationOverride is given; without it ValidationException**
  From a us-east-1 client: RestoreTableFromBackup(us-west-2 BackupArn) and
  RestoreTableToPointInTime(SourceTableArn of a us-west-2 PITR table) without SSESpecificationOverride ->
  ValidationException (HTTP 400, 'Invalid Request: sseSpecificationOverride must be provided for cross-region
  restores'); the same calls with SSESpecificationOverride={Enabled:true, SSEType:KMS} returned 200 CREATING
  and produced us-east-1 tables (TableArn region us-east-1, SSEDescription with a us-east-1 aws/dynamodb key)
  after 766.6 s (backup) and 251.7 s (PITR). The same error is returned in-region when the BackupArn's region
  component differs from the endpoint. RestoreTableToPointInTime with SourceTableName (not ARN) from the other
  region -> TableNotFoundException. DescribeBackup/DeleteBackup/ListBackups in us-east-1 for the us-west-2
  backup -> BackupNotFoundException / empty list; the CREATING target is not visible from the source region.
  - ACK: custom_create, references, e2e-timing · ops: RestoreTableFromBackup, RestoreTableToPointInTime,
    DescribeBackup · fields: SSESpecificationOverride, BackupArn, SourceTableArn
  - repro: us-east-1 client: RestoreTableFromBackup(us-west-2 arn) without/with SSESpecificationOverride;
    RestoreTableToPointInTime(SourceTableArn=us-west-2 table, UseLatestRestorableTime)
  - measurements: cross_region_restore_from_backup_s=766.6, cross_region_restore_pitr_s=251.7
  - handling: not handled in the controller (as of commit 34b85e6)
  - related: [DDB-TABLE-186](#ddb-table-186), [DDB-TABLE-218](#ddb-table-218), [DDB-TABLE-282](#ddb-table-282), [DDB-TABLE-463](#ddb-table-463), [DDB-TABLE-278](#ddb-table-278), [DDB-TABLE-272](#ddb-table-272),
    [DDB-TABLE-100](#ddb-table-100), [DDB-TABLE-087](#ddb-table-087), [DDB-TABLE-086](#ddb-table-086), [DDB-EXPORT-015](export.md#ddb-export-015), [DDB-EXPORT-001](export.md#ddb-export-001), [DDB-EXPORT-002](export.md#ddb-export-002),
    [DDB-TABLE-447](service.md#ddb-table-447), [DDB-BACKUP-012](backup.md#ddb-backup-012), [DDB-TABLE-273](#ddb-table-273), [DDB-BACKUP-005](backup.md#ddb-backup-005) · hypotheses: H-B-109, H-B-012 · evidence:
    table/error-taxonomy/restore-errors
  - notes: H-B-109 confirmed, H-B-012 refuted for the restore APIs (NotFound only for Describe/Delete/List).
    Cross-region restores are 2-3x slower than in-region (13 min vs ~6 min for an empty table).

## Scope

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
  - related: [DDB-TABLE-186](#ddb-table-186), [DDB-TABLE-218](#ddb-table-218), [DDB-TABLE-274](#ddb-table-274), [DDB-TABLE-282](#ddb-table-282), [DDB-TABLE-463](#ddb-table-463), [DDB-TABLE-216](#ddb-table-216),
    [DDB-IMPORT-005](import.md#ddb-import-005), [DDB-IMPORT-008](import.md#ddb-import-008), [DDB-EXPORT-020](export.md#ddb-export-020), [DDB-BACKUP-018](backup.md#ddb-backup-018), [DDB-EXPORT-021](export.md#ddb-export-021) · hypotheses: H-B-040 ·
    evidence: table/error-taxonomy/restore-errors
  - notes: H-B-040 confirmed for the design (restoreFrom: {backupArn | sourceTableArn/sourceTableName +
    restoreDateTime|useLatestRestorableTime} + sseSpecificationOverride mandatory when the ARN region
    differs); its 'RestoreSummary as durable marker' clause is refuted, so the controller must persist the...
  - full notes: [details/DDB-TABLE-278.md](details/DDB-TABLE-278.md)

## Handling gaps (bugs to file)

None recorded for this document's findings; see [service.md 'Handling gaps summary'](service.md#handling-gaps-summary) for the service-wide list.

## E2E timing

Values are seconds unless the key says otherwise; n = trials behind the numbers ('1 run' when the finding records none).

| finding | what | measurements | n |
| --- | --- | --- | --- |
| [DDB-TABLE-086](#ddb-table-086) | After enable Earliest == Latest == enable time, pinned for >= 3 min; UseLatestRestorableTime restore nevertheless works at once | latest_lag_s_at_0_30_90_180s=[0.3, 30.4, 90.4, 180.4], restore_use_latest_first_success_after_enable_s=8.4 | 1 run |
| [DDB-TABLE-218](#ddb-table-218) | Empty/1-item restores take 1.5-8 min in-region (backup and PITR alike, no stable floor); cross-region 4-13 min; settings re-apply cleanly | restore_from_backup_s=343.9, restore_pitr_s=345.6, stream_updating_s=4.0, table_class_updating_s=6.1 | 1 run |
| [DDB-TABLE-274](#ddb-table-274) | Cross-region restore works for both BackupArn and SourceTableArn when SSESpecificationOverride is given; without it ValidationException | cross_region_restore_from_backup_s=766.6, cross_region_restore_pitr_s=251.7 | 1 run |
| [DDB-TABLE-277](#ddb-table-277) | DeleteTable on a table whose restore is still CREATING: rejected with ResourceInUseException until ACTIVE | delete_timeline=[{"duration_s":720.74,"from_s":0.02,"to_s":720.76,"value":"CREATING"}, {"duration_s":null,"from_s":720.76,"to_s":null,"value":"ACTIVE"}] | 1 run |
| [DDB-TABLE-282](#ddb-table-282) | Seven concurrent restores of one source (six with a GSI+LSI) were all admitted: the one-index-table-CREATING rule did not bite restores | restore_since_launch_s.Ra=158.1, restore_since_launch_s.Rb=173.1, restore_since_launch_s.Rc=81.5, restore_since_launch_s.Rd=212.3, restore_since_launch_s.Re=212.2, restore_since_launch_s.Rf=212.0, restore_since_launch_s.Rg=211.8 | 1 run |
| [DDB-TABLE-349](#ddb-table-349) | Doc claim C035 FALSE: RestoreTableFromBackup 'maximum rate of 10 times per second' | burst_n=15, burst_span_ms=600, burst_throttled=0, burst_latency_ms=[164, 352, 244, 299, 281, 267, 425, 245, 268, 320, 251, 476, 243, 281, 582], sequential_throttles=0 | 1 run |
| [DDB-TABLE-350](#ddb-table-350) | Doc claim C034 TRUE: up to 50 concurrent restores per account | accepted_concurrent_restores=50, first_rejection_after=50, restore_to_active_s.min=221.9, restore_to_active_s.median=246.8, restore_to_active_s.max=259.1 | 1 run |
| [DDB-TABLE-463](#ddb-table-463) | RestoreTableFromBackup with OnDemandThroughputOverride={} fails 400 TableInUseException 'already being restored'... | first_call_latency_ms=[347, 210], replay_latency_ms=37, restore_creating_s=211.4 | 1 run |

## Open questions

<!-- preserved:start id=open-questions -->
<!-- open questions and follow-up experiments; survives re-renders -->
<!-- preserved:end -->

## Appendix: low-impact and duplicate findings

| id | category | impact | status | title | related | duplicate_of |
| --- | --- | --- | --- | --- | --- | --- |
| <a id="ddb-table-087"></a>**DDB-TABLE-087** | error-code | medium | confirmed | RestoreTableToPointInTime errors: PointInTimeRecoveryUnavailable / InvalidRestoreTime / TableAlreadyExists / TableNotFound (HTTP 400) | [DDB-BACKUP-001](backup.md#ddb-backup-001), [DDB-BACKUP-007](backup.md#ddb-backup-007), [DDB-TABLE-100](#ddb-table-100), [DDB-TABLE-118](service.md#ddb-table-118), [DDB-TABLE-454](table-subresources.md#ddb-table-454), [DDB-TABLE-455](service.md#ddb-table-455), [DDB-TABLE-227](table-replicas.md#ddb-table-227), [DDB-TABLE-228](table-replicas.md#ddb-table-228), [DDB-TABLE-272](#ddb-table-272), [DDB-TABLE-274](#ddb-table-274), [DDB-TABLE-086](#ddb-table-086), [DDB-EXPORT-015](export.md#ddb-export-015), [DDB-TABLE-218](#ddb-table-218) | [DDB-TABLE-100](#ddb-table-100) |
| <a id="ddb-table-186"></a>**DDB-TABLE-186** | async-state-machine | medium | confirmed | RestoreTableFromBackup of an EMPTY table keeps the target CREATING for ~8 minutes | [DDB-TABLE-218](#ddb-table-218), [DDB-TABLE-274](#ddb-table-274), [DDB-TABLE-282](#ddb-table-282), [DDB-TABLE-463](#ddb-table-463), [DDB-TABLE-278](#ddb-table-278) | [DDB-TABLE-218](#ddb-table-218) |
| <a id="ddb-table-349"></a>**DDB-TABLE-349** | quota-limit | low | confirmed | Doc claim C035 FALSE: RestoreTableFromBackup 'maximum rate of 10 times per second' | - | - |
| <a id="ddb-table-350"></a>**DDB-TABLE-350** | other | low | confirmed | Doc claim C034 TRUE: up to 50 concurrent restores per account | [DDB-TABLE-282](#ddb-table-282) | - |
| <a id="ddb-table-405"></a>**DDB-TABLE-405** | other | low | confirmed | Doc claim C036 TRUE: RestoreTableFromBackup OnDemandThroughputOverride sets the restored on-demand table's maxima | [DDB-TABLE-281](table-indexes.md#ddb-table-281), [DDB-TABLE-280](table-indexes.md#ddb-table-280) | - |

## Supplementary notes

<!-- preserved:start -->
<!-- preserved:end -->
