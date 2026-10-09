<!-- generated: ack-api-quirks dynamodb 2026-10-09 model=2012-08-10 controller=34b85e6 -->
# Backup resource
_CreateBackup/DescribeBackup/DeleteBackup/ListBackups: lifecycle, identity, limits, scope verdict._
Generated from ack-api-quirks `services/dynamodb` (render date in the marker above); model 2012-08-10 (service/dynamodb v1.39.8); controller commit 34b85e6; evidence: `services/dynamodb/probes/<probe id>/` in the lab repo.

## Overview

<!-- preserved:start id=overview -->
Backup is a standalone, ARN-keyed, spec-immutable resource (CreateBackup/DescribeBackup/DeleteBackup/ListBackups) whose lifecycle is independent of its Table: it survives DeleteTable and spans same-name recreations ([DDB-BACKUP-018](#ddb-backup-018), [DDB-BACKUP-016](#ddb-backup-016)). The controller implements it today; the surprising facts are that BackupName is not unique and has no filter, that AVAILABLE is reached ~50 ms after a CREATING response, and that deleting a PITR-enabled table mints an undeletable 35-day SYSTEM backup ([DDB-BACKUP-011](#ddb-backup-011), [DDB-BACKUP-003](#ddb-backup-003), [DDB-BACKUP-015](#ddb-backup-015)).

### Rules a reconciler must respect
- BackupName is not unique, there is no name filter and no ClientToken: persist BackupArn from the CreateBackup response at once, because a retried create silently makes another billed backup that cannot be re-found by name (3 same-name creates -> 3 ARNs; 24 creates in 0.9 s and 60 concurrent in 0.58 s all succeed, no BackupInUse/LimitExceeded) ([DDB-BACKUP-011](#ddb-backup-011), [DDB-BACKUP-019](#ddb-backup-019), [DDB-BACKUP-022](#ddb-backup-022)).
- State machine: BackupStatus=CREATING exists only in the CreateBackup response and DescribeBackup ~50 ms later already says AVAILABLE (ACTIVE never appears); DeleteBackup returns DELETED synchronously, DescribeBackup is BackupNotFoundException (HTTP 400) immediately after and ListBackups drops the entry at once ([DDB-BACKUP-003](#ddb-backup-003), [DDB-BACKUP-005](#ddb-backup-005), [DDB-BACKUP-024](#ddb-backup-024)).
- Shape: CreateBackup returns a top-level BackupDetails while Describe/Delete wrap it in BackupDescription with SourceTableDetails/SourceTableFeatureDetails; BackupCreationDateTime is the request time at ms precision and identical across Create/Describe/List; BackupExpiryDateTime is absent for USER backups; BackupSizeBytes/ItemCount are copied from lagging table statistics (0 for a 1-item table) and SourceTableDetails.OnDemandThroughput lags the live table by minutes ([DDB-BACKUP-004](#ddb-backup-004), [DDB-BACKUP-006](#ddb-backup-006), [DDB-BACKUP-008](#ddb-backup-008), [DDB-BACKUP-017](#ddb-backup-017)).
- Table-state coupling: CreateBackup on a CREATING table is TableNotFoundException byte-identical to a missing table, and for 0-6 s after ACTIVE it is ContinuousBackupsUnavailableException 'Backups are being enabled'; UPDATING and DELETING tables accept it - consult DescribeTable before declaring terminal and requeue on the code rather than sleeping a fixed time. The controller's hooks catalog records no terminal codes for Backup, so these are retried without consulting the Table's state ([GT-DDB-091](service.md#gt-ddb-091) (controller hooks catalog entry)), and no gating at all - the e2e fixture simply waits for the table to be ACTIVE ([GT-DDB-092](service.md#gt-ddb-092) (controller hooks catalog entry)) ([DDB-BACKUP-001](#ddb-backup-001), [DDB-BACKUP-002](#ddb-backup-002)).
- A backup feeds one restore at a time: a second RestoreTableFromBackup and DeleteBackup both get BackupInUseException until the target is ACTIVE, also across regions ([DDB-BACKUP-010](#ddb-backup-010)).
- Backups outlive DeleteTable (even one created while DELETING) and span same-name recreations with an identical TableArn; only SourceTableDetails.TableId separates incarnations; ListBackups(TableName) of a deleted or nonexistent table is 200 with the surviving entries - never tie a Backup to the Table's existence ([DDB-BACKUP-007](#ddb-backup-007), [DDB-BACKUP-016](#ddb-backup-016), [DDB-BACKUP-014](#ddb-backup-014)).
- ARN matching is loose: Describe/DeleteBackup ignore the region component (a region-rewritten ARN is echoed back and deletes the real backup), an unknown region/partition/service is ValidationException 'BackupArn is not valid', another account is AccessDeniedException, another region's endpoint answers BackupNotFoundException, and the ListBackups cursor rejects a region-swapped ARN - compare echoed ARNs with care ([DDB-BACKUP-012](#ddb-backup-012)).
- SYSTEM backups: deleting a PITR-enabled table creates '<table>$DeletedTableBackup' 8-10 s later (BackupType SYSTEM, 35-day expiry, ARN suffix -00000000, empty SourceTableFeatureDetails) that no Backup CR owns and nothing can delete (DeleteBackup on it is ValidationException); ListBackups hides it unless BackupType=SYSTEM or ALL because the default filter is USER ([DDB-BACKUP-015](#ddb-backup-015), [DDB-BACKUP-014](#ddb-backup-014)).
- A backup records only streams, TTL, SSE, indexes (as *Info shapes: name, keys, projection, GSI throughput), billing/throughput and keys - not tags, PITR, table class, deletion protection or the resource policy ([DDB-BACKUP-009](#ddb-backup-009), [DDB-BACKUP-021](#ddb-backup-021)).
- Rate limits are bursty per-operation buckets with ThrottlingException 'Rate exceeded' (HTTP 400): DescribeBackup ~10/s (30 concurrent pass, back-to-back throttles after 13), ListBackups 5/s, DeleteBackup throttles after ~13 back-to-back calls - polling many Backup CRs needs client-side pacing and SDK retries would mask the throttle as latency; ListBackups Limit <= 100, TimeRange is lower-inclusive and upper-exclusive at ms precision and ExclusiveStartBackupArn is a pure cursor ([DDB-BACKUP-020](#ddb-backup-020), [DDB-BACKUP-023](#ddb-backup-023), [DDB-BACKUP-026](#ddb-backup-026), [DDB-BACKUP-014](#ddb-backup-014), [DDB-BACKUP-027](#ddb-backup-027)).

### Timing you should expect
- AVAILABLE ~50 ms after CreateBackup for empty/1-item tables (n=2 plus a 60-backup burst) ([DDB-BACKUP-003](#ddb-backup-003), [DDB-BACKUP-022](#ddb-backup-022)); post-ACTIVE unavailability window 0-6.2 s ([DDB-BACKUP-002](#ddb-backup-002)); SYSTEM backup visible 8-10 s after DeleteTable ([DDB-BACKUP-015](#ddb-backup-015)).
- Throttling: DescribeBackup 13 OK then throttled from 0.165 s, ListBackups 5 OK then throttled from 0.111 s, DeleteBackup 13 OK then throttled from 0.372 s at ~78/s - paced retries all pass ([DDB-BACKUP-020](#ddb-backup-020), [DDB-BACKUP-023](#ddb-backup-023)).
- A restore from the backup of an already deleted table reached ACTIVE in 191 s ([DDB-BACKUP-016](#ddb-backup-016)).

### Known handling gaps in the controller
- No Backup finding is stored as suspect-bug or partial; the controller consequences above (no ClientToken, no terminal codes, no table-state gating, SYSTEM backups nobody owns, client-side pacing for DescribeBackup) are derived from entries stored as handled or unhandled.

### Scope verdict
Implement (already a CRD) as spec-immutable {tableName, backupName} keyed by the status ARN - the controller's hooks catalog records that ReadOne returns NotFound until Status.ACKResourceMetadata.ARN is set and Describe/Delete use that ARN ([GT-DDB-087](service.md#gt-ddb-087) (controller hooks catalog entry)) and that sdkUpdate is a terminal NotImplemented ([GT-DDB-090](service.md#gt-ddb-090) (controller hooks catalog entry)) - create/read/delete/list only, BackupNotFoundException = gone and BackupInUseException = retry ([DDB-BACKUP-018](#ddb-backup-018)). Restore is not a Backup concern: it is an alternative create path for Table, see table-restore.md.

### Where to look next
- The consumer side: RestoreTableFromBackup overrides, what a restore drops, BackupInUseException from the Table side (table-restore.md); PITR/ContinuousBackups and the SYSTEM backup suppression via PITR disable while DELETING ([DDB-TABLE-083](table-subresources.md#ddb-table-083), [DDB-TABLE-088](table-subresources.md#ddb-table-088), [DDB-TABLE-167](table-subresources.md#ddb-table-167), table-subresources.md); backup ARNs are not taggable and the TableNotFoundException catalogue ([DDB-BACKUP-013](service.md#ddb-backup-013), [DDB-TABLE-447](service.md#ddb-table-447), [DDB-TABLE-074](service.md#ddb-table-074), service.md).
- Controller: pkg/resource/backup/{sdk.go,hooks.go}, generator.yaml Backup block (synced.when AVAILABLE/DELETED, 404 = BackupNotFoundException, tags.ignore). Evidence: services/dynamodb/probes/backup/*.

Entries below are generated from the lab findings; low-impact items are in the appendix, long notes under details/.
<!-- preserved:end -->

## At a glance

- canonical findings: 28 (high 13 / medium 8 / low 7); duplicates folded into the appendix: 0
- handling: handled 10 · partial 0 · tracked 0 · unhandled 14 · suspect-bug 0 · n-a 4 (tracked = handled/partial whose reference is an open GitHub issue; counted as not handled)
- re-verified: 3 · last_verified: 2026-10-09 · model: 2012-08-10 (service/dynamodb v1.39.8)
- categories: other 5, quota-limit 5, identity 4, delete-semantics 3, error-code 2, response-fidelity 2,
  stale-response 2, async-state-machine 1, prerequisite 1, read-gap 1, scope 1, shape-mismatch 1

## Operations

| operation | kind | required inputs | declared error shapes | paginated |
| --- | --- | --- | --- | --- |
| CreateBackup | create | TableName, BackupName | TableNotFoundException, TableInUseException, ContinuousBackupsUnavailableException, BackupInUseException, LimitExceededException, InternalServerError | no |
| DeleteBackup | delete | BackupArn | BackupNotFoundException, BackupInUseException, LimitExceededException, InternalServerError | no |
| DeleteTable | delete | TableName | ResourceInUseException, ResourceNotFoundException, LimitExceededException, InternalServerError | no |
| DescribeBackup | read | BackupArn | BackupNotFoundException, InternalServerError | no |
| ListBackups | list | - | InternalServerError | yes |
| RestoreTableFromBackup | create | TargetTableName, BackupArn | TableAlreadyExistsException, TableInUseException, BackupNotFoundException, BackupInUseException, LimitExceededException, InternalServerError | no |
| UpdateContinuousBackups | update | TableName, PointInTimeRecoverySpecification | TableNotFoundException, ContinuousBackupsUnavailableException, InternalServerError | no |
| UpdateTable | update | TableName | ResourceInUseException, ResourceNotFoundException, LimitExceededException, InternalServerError | no |

## State machine

- **BackupStatus**: CREATING, DELETED, AVAILABLE (transitional: CREATING)
- **IndexStatus**: CREATING, UPDATING, DELETING, ACTIVE (transitional: CREATING, UPDATING, DELETING)
- **SSEStatus**: ENABLING, ENABLED, DISABLING, DISABLED, UPDATING (transitional: ENABLING, DISABLING,
  UPDATING)
- **TableStatus**: CREATING, UPDATING, DELETING, ACTIVE, INACCESSIBLE_ENCRYPTION_CREDENTIALS, ARCHIVING,
  ARCHIVED, REPLICATION_NOT_AUTHORIZED (transitional: CREATING, UPDATING, DELETING, ARCHIVING)
- **TimeToLiveStatus**: ENABLING, DISABLING, ENABLED, DISABLED (transitional: ENABLING, DISABLING)

- <a id="ddb-backup-003"></a>**DDB-BACKUP-003** `async-state-machine` · impact high · handled · verified 2026-10-09
  **BackupStatus is CREATING only in the CreateBackup response; DescribeBackup already reports AVAILABLE for small tables**
  CreateBackup returned BackupDetails.BackupStatus=CREATING for an empty table and for a 1-item table, but the
  very next DescribeBackup (~50 ms later) returned AVAILABLE; polling at 2 s saw only AVAILABLE. The enum
  values observed are CREATING (create response), AVAILABLE (describe/list) and DELETED (delete response);
  ACTIVE never appears.
  - ACK: synced.when, late_initialize · ops: CreateBackup, DescribeBackup, ListBackups · fields:
    BackupDetails.BackupStatus
  - repro: CreateBackup -> note status in response -> DescribeBackup immediately and every 2 s
  - measurements: creating_duration_s_empty_table=0.0, creating_duration_s_one_item=0.0
  - handling: handled via `generator.yaml:150-155; templates/hooks/backup/sdk_read_one_post_set_output.go.tpl:1-3`
  - related: [DDB-BACKUP-008](#ddb-backup-008), [DDB-BACKUP-017](#ddb-backup-017) · hypotheses: H-B-001, H-B-115, H-B-136, H-B-144 · evidence:
    backup/state-machine/lifecycle
  - notes: H-B-001 confirmed (enum + readiness via DescribeBackup; the create response is stale immediately).
    The CREATING window is too short on small tables to test H-B-115/H-B-136/H-B-144 (ops while a backup is
    CREATING); those would need a multi-GB table.

## Field matrix

C = accepted by the create input (CreateBackup), U = by the update input, R = present in the read output.

| leaf | C | U | R | type |
| --- | --- | --- | --- | --- |
| AttributeName | - | - | x | string |
| BackupArn | - | - | x | string |
| BackupCreationDateTime | - | - | x | timestamp |
| BackupDescription | - | - | x | struct:BackupDescription |
| BackupDetails | - | - | x | struct:BackupDetails |
| BackupExpiryDateTime | - | - | x | timestamp |
| BackupName | x | - | x | string |
| BackupSizeBytes | - | - | x | long |
| BackupStatus | - | - | x | enum:BackupStatus |
| BackupType | - | - | x | enum:BackupType |
| BillingMode | - | - | x | enum:BillingMode |
| GlobalSecondaryIndexes | - | - | x | list<struct:GlobalSecondaryIndexInfo> |
| InaccessibleEncryptionDateTime | - | - | x | timestamp |
| IndexName | - | - | x | string |
| ItemCount | - | - | x | long |
| KMSMasterKeyArn | - | - | x | string |
| KeySchema | - | - | x | list<struct:KeySchemaElement> |
| KeyType | - | - | x | enum:KeyType |
| LocalSecondaryIndexes | - | - | x | list<struct:LocalSecondaryIndexInfo> |
| MaxReadRequestUnits | - | - | x | long |
| MaxWriteRequestUnits | - | - | x | long |
| NonKeyAttributes | - | - | x | list<string> |
| OnDemandThroughput | - | - | x | struct:OnDemandThroughput |
| Projection | - | - | x | struct:Projection |
| ProjectionType | - | - | x | enum:ProjectionType |
| ProvisionedThroughput | - | - | x | struct:ProvisionedThroughput |
| ReadCapacityUnits | - | - | x | long |
| SSEDescription | - | - | x | struct:SSEDescription |
| SSEType | - | - | x | enum:SSEType |
| SourceTableDetails | - | - | x | struct:SourceTableDetails |
| SourceTableFeatureDetails | - | - | x | struct:SourceTableFeatureDetails |
| Status | - | - | x | enum:SSEStatus |
| StreamDescription | - | - | x | struct:StreamSpecification |
| StreamEnabled | - | - | x | boolean |
| StreamViewType | - | - | x | enum:StreamViewType |
| TableArn | - | - | x | string |
| TableCreationDateTime | - | - | x | timestamp |
| TableId | - | - | x | string |
| TableName | x | - | x | string |
| TableSizeBytes | - | - | x | long |
| TimeToLiveDescription | - | - | x | struct:TimeToLiveDescription |
| TimeToLiveStatus | - | - | x | enum:TimeToLiveStatus |
| WriteCapacityUnits | - | - | x | long |

## Identity and lookup

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
  - related: [DDB-BACKUP-019](#ddb-backup-019), [DDB-BACKUP-018](#ddb-backup-018) · hypotheses: H-B-004, H-B-010, H-B-144 · evidence:
    backup/identity/arn-list-filters
  - notes: H-B-004 confirmed; H-B-010 confirmed (no update op; every spec field immutable). H-B-144 refuted
    for small tables: concurrent CreateBackup calls on the same table do not collide (BackupInUseException
    never seen; 24 rapid creates in backup/limits/burst all succeeded). A controller must persist the...
  - full notes: [details/DDB-BACKUP-011.md](details/DDB-BACKUP-011.md)

- <a id="ddb-backup-012"></a>**DDB-BACKUP-012** `identity` · impact high · handled · verified 2026-10-09, re-verified
  **Describe/DeleteBackup ignore the region in the ARN (and the ARN table name only for the error text); only account mismatch is AccessDenied**
  DescribeBackup with the real ARN's region component rewritten to us-east-1 or eu-west-1 (client still in
  us-west-2) returned 200 for the us-west-2 backup, echoing the rewritten ARN back in BackupDetails.BackupArn;
  DeleteBackup via such a rewritten ARN deleted the real backup (response status DELETED, DescribeBackup of
  the real ARN -> BackupNotFoundException). An unknown region (xx-fake-1), wrong partition or wrong service ->
  ValidationException 'BackupArn is not valid'; empty region -> AccessDeniedException; another account id ->
  AccessDeniedException 'Access is denied'; a table ARN or a 48-char non-ARN -> ValidationException 'Invalid
  Backup ARN'; a well-formed ARN of a nonexistent/deleted backup -> BackupNotFoundException. Calling a
  us-east-1 endpoint with the us-west-2 ARN -> BackupNotFoundException. ListBackups(ExclusiveStartBackupArn)
  with a region-swapped ARN -> ValidationException 'does not match the region'.
  - ACK: is_arn_primary_key, exceptions.404, terminal_codes · ops: DescribeBackup, DeleteBackup, ListBackups ·
    fields: BackupArn, ExclusiveStartBackupArn
  - repro: DescribeBackup with the ARN region replaced by another region; DeleteBackup likewise; compare with
    other-account/other-partition variants
  - handling: handled via `pkg/resource/backup/sdk.go:140-158; pkg/resource/backup/sdk.go:283-292`
  - related: [DDB-EXPORT-001](export.md#ddb-export-001), [DDB-EXPORT-002](export.md#ddb-export-002), [DDB-TABLE-447](service.md#ddb-table-447), [DDB-TABLE-274](table-restore.md#ddb-table-274), [DDB-TABLE-273](table-restore.md#ddb-table-273), [DDB-TABLE-272](table-restore.md#ddb-table-272),
    [DDB-BACKUP-005](#ddb-backup-005) · hypotheses: H-B-012 · evidence: backup/identity/arn-list-filters,
    table/creative/reverify-set-b
  - notes: H-B-012 partially confirmed (cross-endpoint lookup is NotFound). The region-insensitive matching
    means an ARN-keyed controller must compare ARNs it gets back (echoed, possibly rewritten) with care: the
    backup identity is effectively (account, table name, backup id).

- <a id="ddb-backup-014"></a>**DDB-BACKUP-014** `identity` · impact medium · unhandled (not handled in controller) · verified 2026-10-09
  **ListBackups: BackupType defaults to USER (SYSTEM hidden), TimeRange is [lower, upper) at ms precision, Limit<=100, 5/s**
  After deleting a PITR-enabled table, ListBackups(TableName) with no BackupType returned only the 5 USER
  backups; BackupType=SYSTEM returned the one system backup and ALL returned 6; AWS_BACKUP returned 0; 'BOGUS'
  or lowercase 'user' -> ValidationException (enum). TimeRangeLowerBound equal to a BackupCreationDateTime
  includes it, TimeRangeUpperBound equal to it excludes it, +1 ms flips both; lower > upper ->
  ValidationException 'Time range lower bound must be less than or equal to upper bound'; epoch 0 / future
  bounds accepted. Limit=101 -> ValidationException (<=100); Limit=N with exactly N matches returned no
  LastEvaluatedBackupArn. ExclusiveStartBackupArn is a pure cursor: a deleted backup's ARN or a well-formed
  nonexistent ARN resumes after that position; a 9-char string is rejected client-side (min 37). More than 5
  ListBackups per second -> ThrottlingException 'Rate exceeded' (HTTP 400); TableName of a nonexistent table
  -> 200 with an empty list.
  - ACK: custom_find, requeue, list_operation.match_fields · ops: ListBackups · fields: BackupType,
    TimeRangeLowerBound, TimeRangeUpperBound, Limit, ExclusiveStartBackupArn, LastEvaluatedBackupArn
  - repro: 3 backups -> ListBackups with each filter variant -> compare counts
  - handling: not handled in the controller (as of commit 34b85e6)
  - related: [DDB-BACKUP-015](#ddb-backup-015), [DDB-TABLE-167](table-subresources.md#ddb-table-167), [DDB-BACKUP-006](#ddb-backup-006), [DDB-TABLE-336](table-streams-encryption-class.md#ddb-table-336), [DDB-EXPORT-006](export.md#ddb-export-006), [DDB-IMPORT-010](import.md#ddb-import-010),
    [DDB-IMPORT-011](import.md#ddb-import-011), [DDB-IMPORT-016](import.md#ddb-import-016), [DDB-BACKUP-020](#ddb-backup-020), [DDB-EXPORT-007](export.md#ddb-export-007), [DDB-BACKUP-019](#ddb-backup-019) · hypotheses: H-B-112,
    H-B-113, H-B-008 · evidence: backup/identity/arn-list-filters
  - notes: H-B-113 confirmed except inverted range (rejected, not silently empty). H-B-112: cursor semantics
    confirmed, but Limit==match count did NOT return a spurious LastEvaluatedBackupArn here. H-B-008's
    'returned without a filter' is refuted: the default filter is USER.

- <a id="ddb-backup-016"></a>**DDB-BACKUP-016** `identity` · impact high · unhandled (not handled in controller) · verified 2026-10-09
  **Backups outlive their table and span recreations under the same name; only SourceTableDetails.TableId tells incarnations apart**
  After DeleteTable, DescribeBackup of the table's backups still returned AVAILABLE with full
  SourceTableDetails and ListBackups(TableName=<deleted>) still listed them. After CreateTable with the same
  name and a new CreateBackup, ListBackups(TableName, USER) returned backups of both incarnations with
  identical TableArn values and two distinct TableId values. RestoreTableFromBackup from the first
  incarnation's backup into a new name succeeded (CREATING -> ACTIVE in 191.46 s).
  - ACK: references, custom_find, scope:field-on-parent · ops: DeleteTable, DescribeBackup, ListBackups,
    RestoreTableFromBackup · fields: SourceTableDetails.TableId, SourceTableDetails.TableArn, TableName
  - repro: CreateBackup -> DeleteTable -> CreateTable same name -> CreateBackup -> ListBackups(TableName) ->
    compare TableId
  - measurements: restore_from_deleted_table_backup_s=191.46
  - handling: not handled in the controller (as of commit 34b85e6)
  - related: [DDB-BACKUP-007](#ddb-backup-007), [DDB-BACKUP-018](#ddb-backup-018) · hypotheses: H-B-007 · evidence: backup/identity/arn-list-filters
  - notes: H-B-007 confirmed.

## Errors

- <a id="ddb-backup-001"></a>**DDB-BACKUP-001** `error-code` · impact high · handled · verified 2026-10-09
  **CreateBackup on a CREATING table fails with TableNotFoundException, same code as a missing table; UPDATING/DELETING tables accept it**
  CreateBackup issued while the source table is CREATING returned TableNotFoundException (HTTP 400, 'Table not
  found: <name>'), byte-identical to the error for a nonexistent table. With the table UPDATING (stream being
  enabled) CreateBackup returned 200 with BackupStatus=CREATING, and with the table DELETING it also returned
  200 and the backup became AVAILABLE. TableInUseException (declared for CreateBackup) was never observed.
  - ACK: requeue, terminal_codes, references · ops: CreateBackup · fields: TableName
  - repro: CreateTable; immediately CreateBackup -> TableNotFoundException; UpdateTable(stream) then
    CreateBackup -> 200; DeleteTable then CreateBackup -> 200
  - handling: handled via `generator.yaml:141-144; pkg/resource/backup/sdk.go:82-84; test/e2e/tests/test_backup.py:37-75`
  - related: [DDB-TABLE-217](table-restore.md#ddb-table-217), [DDB-TABLE-298](table-replicas.md#ddb-table-298), [DDB-IMPORT-001](import.md#ddb-import-001), [DDB-TABLE-447](service.md#ddb-table-447), [DDB-BACKUP-007](#ddb-backup-007), [DDB-TABLE-100](table-restore.md#ddb-table-100),
    [DDB-TABLE-118](service.md#ddb-table-118), [DDB-TABLE-087](table-restore.md#ddb-table-087), [DDB-TABLE-454](table-subresources.md#ddb-table-454), [DDB-TABLE-455](service.md#ddb-table-455), [DDB-TABLE-227](table-replicas.md#ddb-table-227), [DDB-TABLE-228](table-replicas.md#ddb-table-228) · hypotheses:
    H-B-005 · evidence: backup/state-machine/lifecycle
  - notes: H-B-005 partially refuted: CREATING -> TableNotFoundException (not TableInUseException); DELETING
    -> accepted. A Backup controller cannot distinguish 'table does not exist' from 'table still creating' by
    code; it must look at the referenced Table's status before declaring a terminal error.

- <a id="ddb-backup-010"></a>**DDB-BACKUP-010** `error-code` · impact high · handled · verified 2026-10-09
  **A backup can feed only one restore at a time: a second RestoreTableFromBackup and DeleteBackup both fail with BackupInUseException**
  With one RestoreTableFromBackup in progress (target CREATING), a second RestoreTableFromBackup from the same
  BackupArn into another new name failed with BackupInUseException (HTTP 400, 'Backup is being used to restore
  another table: <arn>') and DeleteBackup failed with BackupInUseException ('Backup is being used to restore
  table: <arn>'); DescribeBackup still reported AVAILABLE. Both succeed once the restore target is ACTIVE
  (also observed across regions in a sibling probe: an in-region restore blocks a cross-region one and vice
  versa).
  - ACK: requeue, deletable.when, one-per-reconcile · ops: RestoreTableFromBackup, DeleteBackup · fields:
    BackupArn
  - repro: RestoreTableFromBackup(B -> T1); immediately RestoreTableFromBackup(B -> T2) and DeleteBackup(B)
  - handling: handled via `generator.yaml:141-144; pkg/resource/backup/sdk.go:82-84`
  - related: [DDB-TABLE-277](table-restore.md#ddb-table-277), [DDB-TABLE-282](table-restore.md#ddb-table-282) · hypotheses: H-B-115, H-B-017 · evidence:
    table/round-trip/restore-feature-carryover
  - notes: H-B-115 (DeleteBackup during restore) confirmed; the single-restore-per-backup rule is new: fan-out
    restores from one backup must be serialized and BackupInUseException treated as retryable on both Backup
    deletion and Table creation.

## Update granularity and ordering

- <a id="ddb-backup-002"></a>**DDB-BACKUP-002** `prerequisite` · impact high · handled · verified 2026-10-09
  **CreateBackup fails with ContinuousBackupsUnavailableException for the first ~3-6 s after a table turns ACTIVE**
  Right after DescribeTable first reported ACTIVE, CreateBackup returned ContinuousBackupsUnavailableException
  (HTTP 400, 'Backups are being enabled for the table: <name>. Please retry later') at +0.1 s and +3.2 s and
  succeeded at +6.23 s with no change to the table. The same window affects UpdateContinuousBackups (observed
  in sibling probes: 3-6 s).
  - ACK: requeue, references · ops: CreateBackup, UpdateContinuousBackups
  - repro: CreateTable -> poll until ACTIVE -> CreateBackup every 3 s and record codes
  - measurements: window_after_active_s=6.23, attempts=3
  - handling: handled via `generator.yaml:46-50; pkg/resource/table/hooks_continuous_backup.go:27-94; templates/hooks/table/sdk_create_post_set_output.go.tpl:1-4; bcd26e1; generator.yaml:141-144; pkg/resource/backup/sdk.go:82-84; test/e2e/tests/test_backup.py:37-75`
  - related: [DDB-EXPORT-014](export.md#ddb-export-014), [DDB-TABLE-447](service.md#ddb-table-447), [DDB-IMPORT-001](import.md#ddb-import-001) · hypotheses: H-B-002 · evidence:
    backup/state-machine/lifecycle
  - notes: H-B-002 confirmed in kind but the window is seconds, not minutes. Message differs from the modelled
    doc string ('Backups have not yet been enabled for this table').
  - full notes: [details/DDB-BACKUP-002.md](details/DDB-BACKUP-002.md)

## Field behavior (defaults, normalization, shapes, immutability)

- <a id="ddb-backup-004"></a>**DDB-BACKUP-004** `shape-mismatch` · impact high · handled · verified 2026-10-09, re-verified
  **CreateBackup returns top-level BackupDetails; Describe/Delete wrap it in BackupDescription with Source*Details that Create lacks**
  CreateBackup output has one member, BackupDetails {BackupArn, BackupCreationDateTime, BackupName,
  BackupSizeBytes, BackupStatus, BackupType}. DescribeBackup and DeleteBackup output BackupDescription
  {BackupDetails, SourceTableDetails, SourceTableFeatureDetails}; for a table with no streams/TTL/SSE/indexes
  SourceTableFeatureDetails is present but an empty map ({}). ListBackups summaries add
  TableArn/TableId/TableName and omit SourceTableDetails.
  - ACK: output_wrapper_field_path, custom_find, is_read_only · ops: CreateBackup, DescribeBackup,
    DeleteBackup, ListBackups · fields: BackupDetails, BackupDescription.BackupDetails,
    BackupDescription.SourceTableDetails, BackupDescription.SourceTableFeatureDetails
  - repro: CreateBackup; DescribeBackup; compare output keys
  - handling: handled via `generator.yaml:22-23; pkg/resource/backup/sdk.go:95-128`
  - related: [DDB-TABLE-215](table-restore.md#ddb-table-215), [DDB-BACKUP-009](#ddb-backup-009), [DDB-BACKUP-021](#ddb-backup-021), [DDB-TABLE-218](table-restore.md#ddb-table-218), [DDB-TABLE-230](table-replicas.md#ddb-table-230) · hypotheses: H-B-011
    · evidence: backup/state-machine/lifecycle, table/creative/reverify-set-a2
  - notes: H-B-011 confirmed. ReadOne must map status from BackupDescription.BackupDetails.*, otherwise status
    stays at the create-time CREATING.

- <a id="ddb-backup-009"></a>**DDB-BACKUP-009** `read-gap` · impact high · unhandled (not handled in controller) · verified 2026-10-09
  **A backup records only streams, TTL, SSE, indexes, billing/throughput and keys; not tags, PITR, table class, deletion protection, policy**
  For a source with stream NEW_AND_OLD_IMAGES, TTL ENABLED, SSE (KMS, aws/dynamodb key), TableClass
  STANDARD_INFREQUENT_ACCESS, deletion protection, OnDemandThroughput 100/50, 3 tags, a resource policy and
  PITR, DescribeBackup.SourceTableFeatureDetails contained exactly
  {StreamDescription{StreamEnabled,StreamViewType}, TimeToLiveDescription{TimeToLiveStatus,AttributeName},
  SSEDescription{Status,SSEType,KMSMasterKeyArn}} and SourceTableDetails
  {TableName,TableId,TableArn,TableSizeBytes,KeySchema,TableCreationDateTime,ProvisionedThroughput(0/0),OnDemandThroughput,ItemCount,BillingMode}.
  No member mentions TableClass, DeletionProtection, Tags, PointInTimeRecovery, ResourcePolicy or
  WarmThroughput.
  - ACK: is_read_only, custom_field · ops: DescribeBackup · fields:
    BackupDescription.SourceTableFeatureDetails, BackupDescription.SourceTableDetails
  - repro: CreateTable with every feature -> UpdateTimeToLive -> UpdateContinuousBackups -> CreateBackup ->
    DescribeBackup
  - handling: not handled in the controller (as of commit 34b85e6)
  - related: [DDB-TABLE-215](table-restore.md#ddb-table-215), [DDB-BACKUP-021](#ddb-backup-021), [DDB-BACKUP-004](#ddb-backup-004), [DDB-TABLE-218](table-restore.md#ddb-table-218), [DDB-TABLE-230](table-replicas.md#ddb-table-230), [DDB-BACKUP-013](service.md#ddb-backup-013),
    [DDB-BACKUP-015](#ddb-backup-015) · hypotheses: H-B-105, H-B-020, H-B-015 · evidence:
    table/round-trip/restore-feature-carryover
  - notes: H-B-105 confirmed for the members list (Stream/TTL/SSE present only when enabled: a plain table
    returns SourceTableFeatureDetails={}); H-B-020/H-B-015 confirmed on the recording side (no TableClass/Tags
    member).

## Response fidelity and consistency

- <a id="ddb-backup-006"></a>**DDB-BACKUP-006** `response-fidelity` · impact medium · unhandled (not handled in controller) · verified 2026-10-09
  **BackupCreationDateTime is the request time at ms precision and identical across Create, Describe and List; no expiry for USER backups**
  BackupCreationDateTime in the CreateBackup response was 49 ms after the caller's request timestamp, carries
  millisecond precision (e.g. .511000), and the same value was returned by DescribeBackup and ListBackups
  after the status flipped to AVAILABLE. BackupExpiryDateTime was absent (not epoch 0) in all three outputs
  for a USER backup. The 14-digit prefix of the BackupArn id (e.g. 01791504762511-2779fadd) is the creation
  time in epoch milliseconds.
  - ACK: is_read_only, compare.is_ignored+delta_pre_compare · ops: CreateBackup, DescribeBackup, ListBackups ·
    fields: BackupDetails.BackupCreationDateTime, BackupDetails.BackupExpiryDateTime, BackupDetails.BackupArn
  - repro: t0=now; CreateBackup; DescribeBackup; ListBackups(TableName); compare timestamps
  - measurements: create_time_minus_request_s=0.049
  - handling: not handled in the controller (as of commit 34b85e6)
  - related: [DDB-BACKUP-015](#ddb-backup-015), [DDB-TABLE-167](table-subresources.md#ddb-table-167), [DDB-BACKUP-014](#ddb-backup-014), [DDB-TABLE-336](table-streams-encryption-class.md#ddb-table-336) · hypotheses: H-B-101 · evidence:
    backup/state-machine/lifecycle
  - notes: H-B-101 confirmed.

- <a id="ddb-backup-017"></a>**DDB-BACKUP-017** `stale-response` · impact medium · unhandled (not handled in controller) · verified 2026-10-09
  **SourceTableDetails.OnDemandThroughput lags the live table by minutes (absent after set, stale after clearing with -1)**
  On a PAY_PER_REQUEST table SourceTableDetails always carries ProvisionedThroughput {0,0} plus BillingMode
  PAY_PER_REQUEST. OnDemandThroughput was absent for a table without maxima, still absent in a backup taken ~5
  s after UpdateTable(OnDemandThroughput 200/100) returned ACTIVE, present (200/100) in a backup taken ~3 min
  later, and still 200/100 in a backup taken right after UpdateTable(-1/-1) while DescribeTable already showed
  {-1,-1}. A table created with OnDemandThroughput records it immediately.
  - ACK: is_read_only, compare.is_ignored+delta_pre_compare · ops: DescribeBackup, UpdateTable · fields:
    SourceTableDetails.OnDemandThroughput, SourceTableDetails.ProvisionedThroughput,
    SourceTableDetails.BillingMode
  - repro: UpdateTable(OnDemandThroughput) -> CreateBackup immediately -> DescribeBackup; repeat minutes later
  - handling: not handled in the controller (as of commit 34b85e6)
  - related: [DDB-TABLE-273](table-restore.md#ddb-table-273), [DDB-TABLE-280](table-indexes.md#ddb-table-280), [DDB-TABLE-279](table-restore.md#ddb-table-279), [DDB-TABLE-281](table-indexes.md#ddb-table-281), [DDB-BACKUP-021](#ddb-backup-021), [DDB-BACKUP-008](#ddb-backup-008),
    [DDB-BACKUP-003](#ddb-backup-003) · hypotheses: H-B-103 · evidence: backup/identity/arn-list-filters
  - notes: H-B-103 confirmed on 0/0 and absent-when-unset; the -1 question is moot because the backup metadata
    is a lagging copy of table metadata (minutes). Restores re-apply the RECORDED maxima, so a restore right
    after clearing them resurrects the old limits.

- <a id="ddb-backup-021"></a>**DDB-BACKUP-021** `response-fidelity` · impact medium · unhandled (not handled in controller) · verified 2026-10-09
  **Backups record GSIs/LSIs as *Info shapes (name, keys, projection, GSI throughput only) frozen at backup time**
  SourceTableFeatureDetails.GlobalSecondaryIndexes[] members were exactly {IndexName, KeySchema, Projection,
  ProvisionedThroughput} and LocalSecondaryIndexes[] {IndexName, KeySchema, Projection}; the live
  GlobalSecondaryIndexDescription additionally has IndexArn, IndexStatus, IndexSizeBytes, ItemCount and
  WarmThroughput. After UpdateTable raised the live GSI from 5/5 to 10/10 the backup still reported 5/5 and a
  restore without overrides created the GSI with the RECORDED 5/5.
  - ACK: is_read_only, custom_create · ops: DescribeBackup, RestoreTableFromBackup · fields:
    SourceTableFeatureDetails.GlobalSecondaryIndexes, SourceTableFeatureDetails.LocalSecondaryIndexes
  - repro: PROVISIONED table with GSI 5/5 + LSI -> CreateBackup -> UpdateTable GSI 10/10 -> DescribeBackup;
    RestoreTableFromBackup -> DescribeTable GSI throughput
  - handling: not handled in the controller (as of commit 34b85e6)
  - related: [DDB-TABLE-215](table-restore.md#ddb-table-215), [DDB-BACKUP-009](#ddb-backup-009), [DDB-BACKUP-004](#ddb-backup-004), [DDB-TABLE-218](table-restore.md#ddb-table-218), [DDB-TABLE-230](table-replicas.md#ddb-table-230), [DDB-TABLE-273](table-restore.md#ddb-table-273),
    [DDB-TABLE-280](table-indexes.md#ddb-table-280), [DDB-TABLE-279](table-restore.md#ddb-table-279), [DDB-TABLE-281](table-indexes.md#ddb-table-281), [DDB-BACKUP-017](#ddb-backup-017) · hypotheses: H-B-105 · evidence:
    table/round-trip/restore-overrides
  - notes: H-B-105 confirmed (both the member list and recorded-vs-live throughput).

## Delete semantics

- <a id="ddb-backup-005"></a>**DDB-BACKUP-005** `delete-semantics` · impact high · handled · verified 2026-10-09
  **DeleteBackup returns BackupStatus=DELETED synchronously; DescribeBackup immediately returns BackupNotFoundException (HTTP 400)**
  DeleteBackup on an AVAILABLE backup returned 200 with BackupDescription.BackupDetails.BackupStatus=DELETED
  (plus full SourceTableDetails). Ten DescribeBackup calls at 1 s intervals starting right after all returned
  BackupNotFoundException HTTP 400 ('Backup not found: <arn>'); ListBackups(TableName) no longer listed it; a
  second DeleteBackup also returned BackupNotFoundException.
  - ACK: exceptions.404, deletable.when · ops: DeleteBackup, DescribeBackup, ListBackups · fields:
    BackupDetails.BackupStatus
  - repro: DeleteBackup -> DescribeBackup x10 @1s -> ListBackups(TableName) -> DeleteBackup again
  - handling: handled via `generator.yaml:150-155; templates/hooks/backup/sdk_read_one_post_set_output.go.tpl:1-3; generator.yaml:141-144; pkg/resource/backup/sdk.go:82-84; generator.yaml:150-155; test/e2e/tests/test_backup.py:135-143`
  - related: [DDB-BACKUP-012](#ddb-backup-012), [DDB-TABLE-274](table-restore.md#ddb-table-274), [DDB-TABLE-273](table-restore.md#ddb-table-273) · hypotheses: H-B-003 · evidence:
    backup/state-machine/lifecycle
  - notes: H-B-003 confirmed: no draining DELETED state is observable; NotFound must be matched by code (HTTP
    is 400).

- <a id="ddb-backup-007"></a>**DDB-BACKUP-007** `delete-semantics` · impact medium · handled · verified 2026-10-09
  **A USER backup survives DeleteTable of its source (even one created while the table was DELETING) with SourceTableDetails intact**
  DeleteTable issued immediately after CreateBackup was accepted (200, TableStatus=DELETING); CreateBackup
  issued while the table was DELETING also returned 200. After the table was gone (ResourceNotFoundException)
  both backups were AVAILABLE, DescribeBackup still returned the full SourceTableDetails (TableName, TableId,
  TableArn, KeySchema, BillingMode, TableCreationDateTime) and ListBackups(TableName=<deleted>) listed them.
  - ACK: references, pre-delete-cleanup, scope:field-on-parent · ops: DeleteTable, CreateBackup,
    DescribeBackup, ListBackups
  - repro: CreateBackup -> DeleteTable -> CreateBackup (DELETING) -> wait table gone -> DescribeBackup both
  - handling: handled via `generator.yaml:84-87; pkg/resource/table/sdk.go:83-86; test/e2e/tests/test_backup.py:37-75`
  - related: [DDB-BACKUP-001](#ddb-backup-001), [DDB-TABLE-100](table-restore.md#ddb-table-100), [DDB-TABLE-118](service.md#ddb-table-118), [DDB-TABLE-087](table-restore.md#ddb-table-087), [DDB-TABLE-454](table-subresources.md#ddb-table-454), [DDB-TABLE-455](service.md#ddb-table-455),
    [DDB-TABLE-227](table-replicas.md#ddb-table-227), [DDB-TABLE-228](table-replicas.md#ddb-table-228), [DDB-BACKUP-016](#ddb-backup-016), [DDB-BACKUP-018](#ddb-backup-018) · hypotheses: H-B-013, H-B-007 · evidence:
    backup/state-machine/lifecycle
  - notes: H-B-013 confirmed (contrarian). Backup lifecycle is independent of the Table: a Table finalizer
    must not wait for or delete backups, and a Backup CR must not be garbage-collected when its Table
    disappears.

- <a id="ddb-backup-015"></a>**DDB-BACKUP-015** `delete-semantics` · impact high · unhandled (not handled in controller) · verified 2026-10-09
  **Deleting a PITR-enabled table creates an undeletable SYSTEM backup '<table>$DeletedTableBackup' that expires after 35 days**
  ~10 s after DeleteTable of a PITR-enabled table, ListBackups(TableName, BackupType=SYSTEM) listed one
  backup: BackupName '<table>$DeletedTableBackup', ARN suffix '<epoch-ms>-00000000', BackupType SYSTEM,
  BackupStatus AVAILABLE, BackupExpiryDateTime = creation + 35 days, SourceTableFeatureDetails {}.
  DeleteBackup on it returned ValidationException (HTTP 400, 'User is not allowed to delete the system backup
  with arn <arn>. It will automatically expire on <ts>'). No SYSTEM backup appeared for a table deleted with
  PITR disabled.
  - ACK: pre-delete-cleanup, custom_delete, terminal_codes · ops: DeleteTable, ListBackups, DescribeBackup,
    DeleteBackup · fields: BackupType, BackupExpiryDateTime
  - repro: CreateTable -> UpdateContinuousBackups(PITR on) -> DeleteTable -> ListBackups(TableName,
    BackupType=SYSTEM) -> DeleteBackup
  - handling: not handled in the controller (as of commit 34b85e6)
  - related: [DDB-TABLE-167](table-subresources.md#ddb-table-167), [DDB-BACKUP-014](#ddb-backup-014), [DDB-BACKUP-006](#ddb-backup-006), [DDB-TABLE-336](table-streams-encryption-class.md#ddb-table-336), [DDB-BACKUP-013](service.md#ddb-backup-013), [DDB-BACKUP-009](#ddb-backup-009) ·
    hypotheses: H-B-008 · evidence: backup/identity/arn-list-filters
  - notes: H-B-008 confirmed (code is ValidationException, not BackupInUseException). A Backup controller
    listing/adopting must skip BackupType=SYSTEM or its finalizer never completes; sweepers cannot remove
    these for 35 days.

## Quotas and rate limits

- <a id="ddb-backup-019"></a>**DDB-BACKUP-019** `quota-limit` · impact medium · unhandled (not handled in controller) · verified 2026-10-09
  **24 CreateBackup calls on one table within 0.9 s (12 sequential + 12 concurrent) all succeed; no BackupInUse/LimitExceeded**
  12 sequential CreateBackup calls with the same BackupName completed in 0.65 s, all HTTP 200 with
  BackupStatus=CREATING; 12 more fired concurrently completed in 0.2 s, all 200. ListBackups then listed 25
  AVAILABLE backups for the table. No BackupInUseException, LimitExceededException or ThrottlingException was
  returned.
  - ACK: none · ops: CreateBackup
  - repro: CreateBackup x12 sequential then x12 concurrent on one empty table
  - measurements: sequential_wall_s=0.65, concurrent_wall_s=0.2, backups_created=24
  - handling: not handled in the controller (as of commit 34b85e6)
  - related: [DDB-BACKUP-020](#ddb-backup-020), [DDB-EXPORT-007](export.md#ddb-export-007), [DDB-BACKUP-014](#ddb-backup-014), [DDB-BACKUP-011](#ddb-backup-011), [DDB-BACKUP-018](#ddb-backup-018) · hypotheses:
    H-B-144, H-B-009 · evidence: backup/limits/burst
  - notes: H-B-144 refuted for small tables (no per-table serialization of backup creation). Documented 50/s
    CreateBackup limit not reached.

- <a id="ddb-backup-020"></a>**DDB-BACKUP-020** `quota-limit` · impact high · unhandled (not handled in controller) · verified 2026-10-09
  **DescribeBackup and ListBackups throttle with ThrottlingException 'Rate exceeded' (HTTP 400), bursty bucket: ~10/s describe, 5/s list**
  30 concurrent DescribeBackup calls (0.23 s) and 60 within 2 s all returned 200, but 40 sequential calls
  fired back-to-back got 13 successes and then 27x ThrottlingException (HTTP 400, 'Rate exceeded') starting at
  0.165 s. 12 sequential ListBackups calls: exactly 5 succeeded, the remaining 7 (from 0.111 s) returned
  ThrottlingException 'Rate exceeded'; 12 concurrent ListBackups after a 3 s pause all succeeded. 25
  DeleteBackup calls (12 sequential in 0.38 s + 13 concurrent) all succeeded. A DescribeBackup right after the
  ListBackups throttle succeeded (per-operation buckets). LimitExceededException was never returned for reads.
  - ACK: requeue, none · ops: DescribeBackup, ListBackups, DeleteBackup
  - repro: burst DescribeBackup x30 concurrent / x40 sequential; ListBackups x12 sequential; DeleteBackup x12
  - measurements: describe_sequential_ok_before_throttle=13, describe_first_throttle_at_s=0.165,
    list_ok_before_throttle=5, list_first_throttle_at_s=0.111
  - handling: not handled in the controller (as of commit 34b85e6)
  - related: [DDB-EXPORT-007](export.md#ddb-export-007), [DDB-BACKUP-014](#ddb-backup-014), [DDB-BACKUP-019](#ddb-backup-019) · hypotheses: H-B-009, H-B-031 · evidence:
    backup/limits/burst
  - notes: H-B-009 partially confirmed (small concurrent bursts pass; sustained >10/s does not). H-B-031
    refuted for DescribeBackup: the code is ThrottlingException, not LimitExceededException. A controller
    polling many Backup resources needs client-side pacing (the harness clients have retries disabled; SDK...
  - full notes: [details/DDB-BACKUP-020.md](details/DDB-BACKUP-020.md)

- <a id="ddb-backup-023"></a>**DDB-BACKUP-023** `quota-limit` · impact medium · unhandled (not handled in controller) · verified 2026-10-09
  **Doc claim C008 PARTLY: DeleteBackup 'max 10/s' - 60 back-to-back calls in 0.767 s (~78.2/s), 43 rejected**
  60 DeleteBackup calls issued back-to-back from one thread (no retries) completed in 0.767 s (~78.2 calls/s):
  histogram {'ok/200': 17, 'ThrottlingException/400': 43}, latency {'n': 60, 'min_ms': 5, 'median_ms': 7,
  'max_ms': 31}, first non-OK call {'i': 13, 't_s': 0.372, 'code': 'ThrottlingException', 'lat_ms': 7}.
  Messages: ['Rate exceeded']. Paced retries afterwards: [{'round': 0, 'attempted': 43, 'codes': {'ok': 43}}].
  [DDB-BACKUP-020](#ddb-backup-020) saw 25 deletes (12 sequential in 0.38 s + 13 concurrent) pass as well.
  - ACK: requeue · ops: DeleteBackup
  - repro: 60 backups of one table; DeleteBackup(arn) x60 sequentially, no pause, no SDK retries
  - measurements: calls=60, wall_s=0.767, rate_per_s=78.2, rejected=43, ok_before_first_non_ok=13
  - handling: not handled in the controller (as of commit 34b85e6)
  - related: [DDB-BACKUP-020](#ddb-backup-020) · evidence: backup/limits/burst, service/static/doc-claims-1
  - notes: VERDICT: PARTLY - ThrottlingException after 13 OK calls at 0.372 s: a bursty bucket of the
    documented order of magnitude, not a hard 10/s

- <a id="ddb-backup-026"></a>**DDB-BACKUP-026** `quota-limit` · impact medium · unhandled (not handled in controller) · verified 2026-10-09
  **Doc claim C011 PARTLY: DescribeBackup '10/s' - 30 concurrent and 60-in-2 s pass, but 40 back-to-back calls throttle after 13 (bursty bucket)**
  30 concurrent DescribeBackup calls (0.23 s) and 60 within 2 s all returned 200, while 40 sequential
  back-to-back calls got 13 successes then 27x ThrottlingException 'Rate exceeded' (HTTP 400) from 0.165 s
  ([DDB-BACKUP-020](#ddb-backup-020)). A limit of the documented order of magnitude exists but behaves as a burst bucket, not a
  hard 10/s; the code is ThrottlingException, not LimitExceededException.
  - ACK: requeue · ops: DescribeBackup
  - handling: not handled in the controller (as of commit 34b85e6)
  - related: [DDB-BACKUP-020](#ddb-backup-020) · evidence: backup/limits/burst, service/static/doc-claims-1
  - notes: VERDICT: PARTLY - a throttle around 10/s exists but short bursts well above it succeed;
    ThrottlingException (HTTP 400) must be treated as retryable

## Scope

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
  - related: [DDB-BACKUP-007](#ddb-backup-007), [DDB-BACKUP-016](#ddb-backup-016), [DDB-BACKUP-011](#ddb-backup-011), [DDB-BACKUP-019](#ddb-backup-019), [DDB-EXPORT-021](export.md#ddb-export-021), [DDB-IMPORT-005](import.md#ddb-import-005),
    [DDB-TABLE-278](table-restore.md#ddb-table-278) · hypotheses: H-B-037 · evidence: backup/identity/arn-list-filters,
    table/creative/reverify-set-b
  - notes: H-B-037 confirmed. Scope verdict: implement as its own CRD with spec {tableName, backupName} both
    immutable (any drift = terminal condition, never delete+recreate), status mirroring BackupDescription.*,
    ReadOne by ARN (status.ackResourceMetadata.arn), and a delete path that treats...
  - full notes: [details/DDB-BACKUP-018.md](details/DDB-BACKUP-018.md)

## Handling gaps (bugs to file)

None recorded for this document's findings; see [service.md 'Handling gaps summary'](service.md#handling-gaps-summary) for the service-wide list.

## E2E timing

Values are seconds unless the key says otherwise; n = trials behind the numbers ('1 run' when the finding records none).

| finding | what | measurements | n |
| --- | --- | --- | --- |
| [DDB-BACKUP-002](#ddb-backup-002) | CreateBackup fails with ContinuousBackupsUnavailableException for the first ~3-6 s after a table turns ACTIVE | window_after_active_s=6.23 | 3 |
| [DDB-BACKUP-006](#ddb-backup-006) | BackupCreationDateTime is the request time at ms precision and identical across Create, Describe and List; no expiry for USER backups | create_time_minus_request_s=0.049 | 1 run |
| [DDB-BACKUP-016](#ddb-backup-016) | Backups outlive their table and span recreations under the same name; only SourceTableDetails.TableId tells incarnations apart | restore_from_deleted_table_backup_s=191.46 | 1 run |
| [DDB-BACKUP-019](#ddb-backup-019) | 24 CreateBackup calls on one table within 0.9 s (12 sequential + 12 concurrent) all succeed; no BackupInUse/LimitExceeded | sequential_wall_s=0.65, concurrent_wall_s=0.2, backups_created=24 | 1 run |
| [DDB-BACKUP-020](#ddb-backup-020) | DescribeBackup and ListBackups throttle with ThrottlingException 'Rate exceeded' (HTTP 400), bursty bucket: ~10/s describe, 5/s list | describe_sequential_ok_before_throttle=13, describe_first_throttle_at_s=0.165, list_ok_before_throttle=5, list_first_throttle_at_s=0.111 | 1 run |
| [DDB-BACKUP-022](#ddb-backup-022) | Doc claim C002 FALSE: CreateBackup 'max 50/s' - 60 concurrent calls in 0.583 s (~102.9/s), 0 rejected | calls=60, wall_s=0.583, rate_per_s=102.9, rejected=0 | 1 run |
| [DDB-BACKUP-023](#ddb-backup-023) | Doc claim C008 PARTLY: DeleteBackup 'max 10/s' - 60 back-to-back calls in 0.767 s (~78.2/s), 43 rejected | calls=60, wall_s=0.767, rate_per_s=78.2, rejected=43, ok_before_first_non_ok=13 | 1 run |

## Open questions

- [DDB-BACKUP-025](#ddb-backup-025) (unverified) - Doc claim C003 UNTESTABLE: the backup consistency window (data before T-1 min
  in, after T+1 min out) is a data-plane guarantee: VERDICT: UNTESTABLE - data-plane behaviour (item
  visibility inside a backup) is out of scope for control-plane probing
- [DDB-BACKUP-029](#ddb-backup-029) (unverified) - Doc claim C049 UNTESTABLE: BackupSizeBytes is 'updated approximately every six
  hours': VERDICT: UNTESTABLE - no backup older than 6 h exists in either lab region and a 6 h soak is outside
  the probe budget.

<!-- preserved:start id=open-questions -->
<!-- open questions and follow-up experiments; survives re-renders -->
<!-- preserved:end -->

## Appendix: low-impact and duplicate findings

| id | category | impact | status | title | related | duplicate_of |
| --- | --- | --- | --- | --- | --- | --- |
| <a id="ddb-backup-008"></a>**DDB-BACKUP-008** | stale-response | low | confirmed | BackupSizeBytes and SourceTableDetails.ItemCount/TableSizeBytes are 0 for a 1-item table even after AVAILABLE | [DDB-BACKUP-017](#ddb-backup-017), [DDB-BACKUP-003](#ddb-backup-003) | - |
| <a id="ddb-backup-022"></a>**DDB-BACKUP-022** | quota-limit | low | confirmed | Doc claim C002 FALSE: CreateBackup 'max 50/s' - 60 concurrent calls in 0.583 s (~102.9/s), 0 rejected | [DDB-BACKUP-019](#ddb-backup-019) | - |
| <a id="ddb-backup-024"></a>**DDB-BACKUP-024** | other | low | confirmed | Doc claim C001 TRUE: CreateBackup is processed at once; the backup is AVAILABLE ~50 ms later, far inside 'within minutes' | [DDB-BACKUP-003](#ddb-backup-003), [DDB-BACKUP-010](#ddb-backup-010) | - |
| <a id="ddb-backup-025"></a>**DDB-BACKUP-025** | other | low | unverified | Doc claim C003 UNTESTABLE: the backup consistency window (data before T-1 min in, after T+1 min out) is a data-plane guarantee | [DDB-BACKUP-008](#ddb-backup-008) | - |
| <a id="ddb-backup-027"></a>**DDB-BACKUP-027** | other | low | confirmed | Doc claim C025 TRUE: ListBackups Limit caps the page (Limit=N returns N, max 100, LastEvaluatedBackupArn cursor) | [DDB-BACKUP-014](#ddb-backup-014) | - |
| <a id="ddb-backup-028"></a>**DDB-BACKUP-028** | other | low | confirmed | Doc claim C026 TRUE: ListBackups.Limit is 'maximum number of backups to return at once' - at most Limit entries per page, max 100 | [DDB-BACKUP-014](#ddb-backup-014) | - |
| <a id="ddb-backup-029"></a>**DDB-BACKUP-029** | other | low | unverified | Doc claim C049 UNTESTABLE: BackupSizeBytes is 'updated approximately every six hours' | [DDB-BACKUP-008](#ddb-backup-008) | - |

## Supplementary notes

<!-- preserved:start -->
<!-- preserved:end -->
