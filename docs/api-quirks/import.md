<!-- generated: ack-api-quirks dynamodb 2026-10-09 model=2012-08-10 controller=34b85e6 -->
# ImportTable (scope investigation)
_ImportTable/DescribeImport/ListImports behavior and the scope verdict for an Import resource._
Generated from ack-api-quirks `services/dynamodb` (render date in the marker above); model 2012-08-10 (service/dynamodb v1.39.8); controller commit 34b85e6; evidence: `services/dynamodb/probes/<probe id>/` in the lab repo.

## Overview

<!-- preserved:start id=overview -->
ImportTable is a Table constructor: it returns TableArn/TableId immediately, builds the table from S3 asynchronously and leaves an ordinary table that carries no import marker and accepts every mutation once the job is terminal ([DDB-IMPORT-001](#ddb-import-001), [DDB-IMPORT-005](#ddb-import-005)). The job has no cancel or delete API; DescribeImport/ListImports are the only link between table and import, and those records outlive the table ([DDB-IMPORT-006](#ddb-import-006), [DDB-IMPORT-008](#ddb-import-008)).

### Rules a reconciler must respect
- For ~30 s after ImportTable returns 200 the table is ResourceNotFoundException to every table API (then CREATING, where UpdateTable/UpdateTimeToLive are ResourceInUseException); a Table CR for the same name would attempt CreateTable in that window ([DDB-IMPORT-001](#ddb-import-001)).
- The table turns ACTIVE ~6 s before ImportStatus flips to COMPLETED and DeleteTable is admitted in that gap; TableStatus=ACTIVE is not proof the import finished, so gate on DescribeImport ([DDB-IMPORT-006](#ddb-import-006)).
- FAILED is not 'nothing happened': any bad line ends FAILED/ItemValidationError with the table ACTIVE and holding the valid rows (Imported=9 of 10, scan=9), and COMPLETED requires ErrorCount=0; never delete or re-create the table on FAILED. An S3 source mutated mid-import also ends FAILED/ItemValidationError - its entry's closing 'table is ResourceNotFoundException' line contradicts its own poll log of (FAILED, ACTIVE) and is resolved in favour of the ACTIVE table ([DDB-IMPORT-013](#ddb-import-013), [DDB-IMPORT-007](#ddb-import-007)).
- A nonexistent bucket is accepted (200) and FAILS with S3NoSuchBucket ~6 s later with no table, while DescribeImport keeps a dangling TableArn/TableId; an empty prefix is COMPLETED with an empty ACTIVE table ([DDB-IMPORT-012](#ddb-import-012)).
- ClientToken idempotency holds only while IN_PROGRESS: identical replay -> same ARN, changed S3KeyPrefix silently ignored, changed TableName -> ImportConflictException; after COMPLETED a replay is ResourceInUseException, and after the table is deleted the token returns the old import without creating anything, so rotate the token on re-create ([DDB-IMPORT-017](#ddb-import-017)).
- The table name is not reserved synchronously: concurrent ImportTable calls for one name (with or without tokens) are all accepted with distinct ImportArns and the losers end FAILED/TableAlreadyExists; an ACTIVE or DELETING name is ResourceInUseException 'Table already exists' ([DDB-IMPORT-018](#ddb-import-018), [DDB-IMPORT-019](#ddb-import-019)).
- DescribeImport echoes TableCreationParameters verbatim (KMS alias, OnDemandThroughput) and back-fills S3BucketOwner, InputCompressionType=NONE, ClientToken and CloudWatchLogGroupArn, while DescribeTable reports normalized forms; compare against one source only ([DDB-IMPORT-003](#ddb-import-003), [DDB-IMPORT-004](#ddb-import-004)).
- Not-found is ImportNotFoundException (HTTP 400); a table ARN is ValidationException 'Invalid Import ARN', another account AccessDeniedException, another region ImportNotFoundException ([DDB-IMPORT-010](#ddb-import-010)); DescribeImport bursts of 13-18 calls throttle with ThrottlingException 'Rate exceeded' ([DDB-EXPORT-007](export.md#ddb-export-007), export.md).
- S3KeyPrefix is a raw key-prefix match ('data' also matches data2/ and data.bak); an export->import round trip needs the export's data/ folder plus InputCompressionType=GZIP ([DDB-IMPORT-014](#ddb-import-014), [DDB-IMPORT-020](#ddb-import-020)).
- Nothing is implemented in the controller: any design must own the ~30 s invisibility window, the FAILED-with-partial-table outcome, token rotation and the ownership overlap with a Table CR of the same name ([DDB-IMPORT-001](#ddb-import-001), [DDB-IMPORT-013](#ddb-import-013), [DDB-IMPORT-017](#ddb-import-017), [DDB-IMPORT-005](#ddb-import-005)).

### Timing you should expect
- 1-item DYNAMODB_JSON import: table visible (CREATING) at ~31 s, ACTIVE and COMPLETED at ~111 s (n=1, [DDB-IMPORT-001](#ddb-import-001)); a 28,500-item import turned ACTIVE at ~215 s ([DDB-IMPORT-006](#ddb-import-006)); bad bucket FAILED in 5.6 s, empty prefix COMPLETED in 100 s, bad-line imports FAILED in 90-174 s ([DDB-IMPORT-012](#ddb-import-012), [DDB-IMPORT-013](#ddb-import-013)).

### Known handling gaps in the controller
- No Import finding is stored as suspect-bug or partial: there is no Import code to be wrong, and the Table-side 404 mapping and requeue constants that some entries name as 'handled' do not implement any of the rules above.

### Scope verdict
field-on-parent: model ImportTable as a create-only, immutable field group on Table (spec.importSource) rather than a standalone CRD, because the operation creates a Table and returns its ARN/TableId, has no delete or cancel, leaves no marker in DescribeTable, and the resulting table admits tags/TTL/PITR/DP/backup like any other; the import record is only a status artifact that outlives the table ([DDB-IMPORT-005](#ddb-import-005), [DDB-IMPORT-006](#ddb-import-006), [DDB-IMPORT-008](#ddb-import-008); [DDB-TABLE-278](table-restore.md#ddb-table-278), table-restore.md, is the parallel restore verdict).

### Where to look next
- Generated sections below: State machine, Idempotency, Errors, E2E timing. Siblings: the producer side of the export->import round trip ([DDB-IMPORT-020](#ddb-import-020) above; export.md), the other Table constructor and its scope reasoning (table-restore.md), the ResourceInUseException catalogue ([DDB-TABLE-444](service.md#ddb-table-444), service.md). Evidence: services/dynamodb/probes/import/*.

Entries below are generated from the lab findings; low-impact items are in the appendix, long notes under details/.
<!-- preserved:end -->

## At a glance

- canonical findings: 20 (high 8 / medium 9 / low 3); duplicates folded into the appendix: 0
- handling: handled 2 · partial 0 · tracked 0 · unhandled 18 · suspect-bug 0 · n-a 0 (tracked = handled/partial whose reference is an open GitHub issue; counted as not handled)
- re-verified: 0 · last_verified: 2026-10-09 · model: 2012-08-10 (service/dynamodb v1.39.8)
- categories: async-state-machine 3, error-code 2, idempotency 2, normalization 2, request-validation 2,
  response-fidelity 2, server-default 2, delete-semantics 1, identity 1, prerequisite 1,
  requested-vs-effective 1, scope 1

## Operations

| operation | kind | required inputs | declared error shapes | paginated |
| --- | --- | --- | --- | --- |
| CreateTable | create | TableName | ResourceInUseException, LimitExceededException, InternalServerError | no |
| DeleteTable | delete | TableName | ResourceInUseException, ResourceNotFoundException, LimitExceededException, InternalServerError | no |
| DescribeExport | read | ExportArn | ExportNotFoundException, LimitExceededException, InternalServerError | no |
| DescribeImport | read | ImportArn | ImportNotFoundException | no |
| DescribeTable | read | TableName | ResourceNotFoundException, InternalServerError | no |
| ExportTableToPointInTime | other | TableArn, S3Bucket | TableNotFoundException, PointInTimeRecoveryUnavailableException, LimitExceededException, InvalidExportTimeException, ExportConflictException, InternalServerError | no |
| ImportTable | create | S3BucketSource, InputFormat, TableCreationParameters | ResourceInUseException, LimitExceededException, ImportConflictException | no |
| ListExports | list | - | LimitExceededException, InternalServerError | no |
| ListImports | list | - | LimitExceededException | no |
| Scan | other | TableName | ProvisionedThroughputExceededException, ResourceNotFoundException, RequestLimitExceeded, InternalServerError, ThrottlingException | yes |
| TagResource | tag | ResourceArn, Tags | LimitExceededException, ResourceNotFoundException, InternalServerError, ResourceInUseException | no |
| UpdateContinuousBackups | update | TableName, PointInTimeRecoverySpecification | TableNotFoundException, ContinuousBackupsUnavailableException, InternalServerError | no |
| UpdateTable | update | TableName | ResourceInUseException, ResourceNotFoundException, LimitExceededException, InternalServerError | no |
| UpdateTimeToLive | update | TableName, TimeToLiveSpecification | ResourceInUseException, ResourceNotFoundException, LimitExceededException, InternalServerError | no |

## State machine

- **ImportStatus**: IN_PROGRESS, COMPLETED, CANCELLING, CANCELLED, FAILED (transitional: IN_PROGRESS,
  CANCELLING)
- **TableStatus**: CREATING, UPDATING, DELETING, ACTIVE, INACCESSIBLE_ENCRYPTION_CREDENTIALS, ARCHIVING,
  ARCHIVED, REPLICATION_NOT_AUTHORIZED (transitional: CREATING, UPDATING, DELETING, ARCHIVING)

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
  - related: [DDB-BACKUP-001](backup.md#ddb-backup-001), [DDB-TABLE-217](table-restore.md#ddb-table-217), [DDB-TABLE-298](table-replicas.md#ddb-table-298), [DDB-TABLE-447](service.md#ddb-table-447), [DDB-BACKUP-002](backup.md#ddb-backup-002), [DDB-EXPORT-014](export.md#ddb-export-014),
    [DDB-IMPORT-018](#ddb-import-018), [DDB-IMPORT-019](#ddb-import-019), [DDB-IMPORT-017](#ddb-import-017) · hypotheses: H-B-032 · evidence:
    import/state-machine/lifecycle
  - notes: H-B-032 partially refuted: the response carries TableArn/TableId but DescribeTable at t+0 ->
    ResourceNotFoundException (all t+0 ops: {'DescribeTable': 'ResourceNotFoundException',
    'UpdateTable.DeletionProtection': 'ResourceNotFoundException', 'PutItem': 'ResourceNotFoundException',
    'CreateBackup':...
  - full notes: [details/DDB-IMPORT-001.md](details/DDB-IMPORT-001.md)

- <a id="ddb-import-007"></a>**DDB-IMPORT-007** `async-state-machine` · impact medium · unhandled (not handled in controller) · verified 2026-10-09
  **Mutating the S3 source objects while an import is IN_PROGRESS -> final ImportStatus FAILED**
  A ~30.8 MB import (30 objects) had 15 objects deleted and 1 overwritten at t+61.5s (import status
  IN_PROGRESS at that moment). Final DescribeImport: ImportStatus=FAILED FailureCode=ItemValidationError
  FailureMessage=Some of the items failed validation checks and were not imported. Please check CloudWatch
  error logs for more details. ProcessedItemCount=18061 ImportedItemCount=18050 ErrorCount=11; the table is
  ResourceNotFoundException with ItemCount=None (Scan COUNT=None).
  - ACK: terminal_codes, docs-only · ops: ImportTable, DescribeImport
  - repro: Upload 30x1MB DYNAMODB_JSON objects -> ImportTable -> at t+60s delete half + overwrite one -> poll
    to terminal
  - handling: not handled in the controller (as of commit 34b85e6)
  - related: [DDB-IMPORT-013](#ddb-import-013), [DDB-IMPORT-014](#ddb-import-014), [DDB-IMPORT-020](#ddb-import-020), [DDB-IMPORT-012](#ddb-import-012) · hypotheses: H-B-131 · evidence:
    import/state-machine/lifecycle
  - notes: Statuses seen for t4: [['IN_PROGRESS', 'ERR:ResourceNotFoundException'], ['IN_PROGRESS',
    'ERR:ResourceNotFoundException'], ['IN_PROGRESS', 'CREATING'], ['IN_PROGRESS', 'CREATING'],
    ['IN_PROGRESS', 'CREATING'], ['IN_PROGRESS', 'CREATING'], ['FAILED', 'ACTIVE'], ['FAILED', 'ACTIVE'],
    ['FAILED',...
  - full notes: [details/DDB-IMPORT-007.md](details/DDB-IMPORT-007.md)

- <a id="ddb-import-013"></a>**DDB-IMPORT-013** `async-state-machine` · impact high · unhandled (not handled in controller) · verified 2026-10-09
  **Any bad line makes the import FAILED/ItemValidationError, yet valid rows are written (table ACTIVE with ImportedItemCount items)**
  10-line file with one item lacking the key attribute -> missing-key: ImportStatus=FAILED
  FailureCode=ItemValidationError Processed=10 Imported=9 Errors=1; table=ACTIVE ItemCount=0 scan=9. 10-line
  file with one unparseable line -> garbage-line: ImportStatus=FAILED FailureCode=ItemValidationError
  Processed=6 Imported=5 Errors=1; table=ACTIVE ItemCount=0 scan=5. FailureMessage(missing-key)='Some of the
  items failed validation checks and were not imported. Please check CloudWatch error logs for more details.'.
  - ACK: terminal_codes, synced.when · ops: ImportTable, DescribeImport, Scan · fields:
    ImportTableDescription.FailureCode, ImportTableDescription.ErrorCount,
    ImportTableDescription.ImportedItemCount
  - repro: Upload 10 DYNAMODB_JSON lines with 1 bad line -> ImportTable -> poll to terminal -> DescribeTable +
    Scan(COUNT)
  - measurements: missing_key_failed_after_s=174.3, garbage_line_failed_after_s=90.1
  - handling: not handled in the controller (as of commit 34b85e6)
  - related: [DDB-IMPORT-014](#ddb-import-014), [DDB-IMPORT-020](#ddb-import-020), [DDB-IMPORT-007](#ddb-import-007), [DDB-IMPORT-012](#ddb-import-012) · hypotheses: H-B-033, H-B-126 ·
    evidence: import/error-taxonomy/failure-modes
  - notes: H-B-126 confirmed, H-B-033 refuted: COMPLETED is only reachable with ErrorCount=0. FAILED does not
    mean empty - deleting/re-creating the table on FAILED destroys partially imported data. Service-side
    durations: missing-key 174s, garbage-line 90s.
  - full notes: [details/DDB-IMPORT-013.md](details/DDB-IMPORT-013.md)

## Field matrix

C = accepted by the create input (ImportTable - the job-start operation; the resource has no Create<Noun>, so
an ACK resource would issue this call on create), U = by the update input, R = present in the read output.

| leaf | C | U | R | type |
| --- | --- | --- | --- | --- |
| AttributeDefinitions | x | - | x | list<struct:AttributeDefinition> |
| AttributeName | x | - | x | string |
| AttributeType | x | - | x | enum:ScalarAttributeType |
| BillingMode | x | - | x | enum:BillingMode |
| ClientToken | x | - | x | string |
| CloudWatchLogGroupArn | - | - | x | string |
| Csv | x | - | x | struct:CsvOptions |
| Delimiter | x | - | x | string |
| Enabled | x | - | x | boolean |
| EndTime | - | - | x | timestamp |
| ErrorCount | - | - | x | long |
| FailureCode | - | - | x | string |
| FailureMessage | - | - | x | string |
| GlobalSecondaryIndexes | x | - | x | list<struct:GlobalSecondaryIndex> |
| HeaderList | x | - | x | list<string> |
| ImportArn | - | - | x | string |
| ImportStatus | - | - | x | enum:ImportStatus |
| ImportTableDescription | - | - | x | struct:ImportTableDescription |
| ImportedItemCount | - | - | x | long |
| IndexName | x | - | x | string |
| InputCompressionType | x | - | x | enum:InputCompressionType |
| InputFormat | x | - | x | enum:InputFormat |
| InputFormatOptions | x | - | x | struct:InputFormatOptions |
| KMSMasterKeyId | x | - | x | string |
| KeySchema | x | - | x | list<struct:KeySchemaElement> |
| KeyType | x | - | x | enum:KeyType |
| MaxReadRequestUnits | x | - | x | long |
| MaxWriteRequestUnits | x | - | x | long |
| NonKeyAttributes | x | - | x | list<string> |
| OnDemandThroughput | x | - | x | struct:OnDemandThroughput |
| ProcessedItemCount | - | - | x | long |
| ProcessedSizeBytes | - | - | x | long |
| Projection | x | - | x | struct:Projection |
| ProjectionType | x | - | x | enum:ProjectionType |
| ProvisionedThroughput | x | - | x | struct:ProvisionedThroughput |
| ReadCapacityUnits | x | - | x | long |
| ReadUnitsPerSecond | x | - | x | long |
| S3Bucket | x | - | x | string |
| S3BucketOwner | x | - | x | string |
| S3BucketSource | x | - | x | struct:S3BucketSource |
| S3KeyPrefix | x | - | x | string |
| SSESpecification | x | - | x | struct:SSESpecification |
| SSEType | x | - | x | enum:SSEType |
| StartTime | - | - | x | timestamp |
| TableArn | - | - | x | string |
| TableCreationParameters | x | - | x | struct:TableCreationParameters |
| TableId | - | - | x | string |
| TableName | x | - | x | string |
| WarmThroughput | x | - | x | struct:WarmThroughput |
| WriteCapacityUnits | x | - | x | long |
| WriteUnitsPerSecond | x | - | x | long |

## Identity and lookup

- <a id="ddb-import-008"></a>**DDB-IMPORT-008** `identity` · impact medium · unhandled (not handled in controller) · verified 2026-10-09
  **Import records outlive the imported table: DescribeImport/ListImports(TableArn) after DeleteTable**
  After the imported table was deleted (DescribeTable -> ResourceNotFoundException), DescribeImport -> ok
  (status COMPLETED, TableArn/TableId still present), ListImports(TableArn) -> 1 entries, paginated
  ListImports() still contains the import: True.
  - ACK: custom_find, exceptions.404 · ops: DeleteTable, DescribeImport, ListImports
  - repro: ImportTable -> COMPLETED -> DeleteTable -> wait ResourceNotFoundException -> DescribeImport /
    ListImports(TableArn)
  - handling: not handled in the controller (as of commit 34b85e6)
  - related: [DDB-TABLE-216](table-restore.md#ddb-table-216), [DDB-IMPORT-005](#ddb-import-005), [DDB-TABLE-278](table-restore.md#ddb-table-278), [DDB-EXPORT-020](export.md#ddb-export-020), [DDB-EXPORT-022](export.md#ddb-export-022) · hypotheses:
    H-B-142 · evidence: import/state-machine/lifecycle
  - notes: Contradiction with [DDB-EXPORT-020](export.md#ddb-export-020), [DDB-EXPORT-022](export.md#ddb-export-022): 020's title says export records are 'keyed by
    the name-based TableArn', yet its behavior shows ListExports(TableArn) -> 0 entries immediately after
    DeleteTable and still 0 after a same-name re-create (same ARN string, new TableId); 022 confirms...
  - full notes: [details/DDB-IMPORT-008.md](details/DDB-IMPORT-008.md)

## Idempotency

- <a id="ddb-import-017"></a>**DDB-IMPORT-017** `idempotency` · impact high · unhandled (not handled in controller) · verified 2026-10-09
  **ImportTable ClientToken: identical replay -> same ARN; changed S3KeyPrefix silently ignored; changed TableName -> ImportConflictException**
  While the first import was IN_PROGRESS: identical replay -> 200 same ARN as #1 (IN_PROGRESS) (HTTP 200);
  same token with a different S3KeyPrefix -> 200 same ARN as #1 (IN_PROGRESS) ''; same token with a different
  TableName -> ImportConflictException 'Import conflict: Duplicate request detected with conflicting
  parameters'. After the import COMPLETED: identical replay -> ResourceInUseException; same token + other
  prefix -> 200 same ARN as #1 (FAILED). After the imported table was deleted, the identical replay -> 200
  same ARN as #1 (FAILED) (DescribeTable afterwards: ResourceNotFoundException).
  - ACK: custom_create, terminal_codes, requeue · ops: ImportTable · fields: ClientToken,
    S3BucketSource.S3KeyPrefix, TableCreationParameters.TableName
  - repro: ImportTable(token=T) -> replay same/changed params while IN_PROGRESS, after COMPLETED, and after
    DeleteTable of the target
  - handling: not handled in the controller (as of commit 34b85e6)
  - related: [DDB-EXPORT-009](export.md#ddb-export-009), [DDB-EXPORT-010](export.md#ddb-export-010), [DDB-IMPORT-018](#ddb-import-018), [DDB-IMPORT-019](#ddb-import-019), [DDB-IMPORT-001](#ddb-import-001) · hypotheses:
    H-B-018, H-B-036 · evidence: import/idempotency/client-token
  - notes: The doc promises 'IdempotentParameterMismatch'; the wire code is ImportConflictException ('Import
    conflict: Duplicate request detected with conflicting parameters') and it fired only for a changed
    TableName, NOT for a changed S3KeyPrefix (same ARN returned, change silently ignored). While the target...
  - full notes: [details/DDB-IMPORT-017.md](details/DDB-IMPORT-017.md)

- <a id="ddb-import-018"></a>**DDB-IMPORT-018** `idempotency` · impact high · unhandled (not handled in controller) · verified 2026-10-09
  **Concurrent ImportTable calls for the same TableName are all accepted; losers end FAILED with FailureCode=TableAlreadyExists**
  Three ImportTable calls for one TableName within 1s (token T, no token, token T2) all returned 200 with
  distinct ImportArns while none of the tables existed yet. Outcome per call: {'1_first': ('FAILED',
  'TableAlreadyExists', '2026-10-09T00:25:48.913000+00:00'), '5_no_token_same_table_name': ('COMPLETED', None,
  '2026-10-09T00:26:40.259000+00:00'), '6_other_token_same_table_name': ('FAILED', 'TableAlreadyExists',
  '2026-10-09T00:25:23.904000+00:00')}. Winner: ['5_no_token_same_table_name']. ImportTable does not reserve
  the table name synchronously.
  - ACK: custom_create, terminal_codes, synced.when · ops: ImportTable, DescribeImport · fields:
    TableCreationParameters.TableName, ImportTableDescription.FailureCode
  - repro: ImportTable x3 for the same TableName within 1s -> poll DescribeImport for each to terminal
  - handling: not handled in the controller (as of commit 34b85e6)
  - related: [DDB-TABLE-219](table-restore.md#ddb-table-219), [DDB-TABLE-275](table-restore.md#ddb-table-275), [DDB-TABLE-100](table-restore.md#ddb-table-100), [DDB-TABLE-217](table-restore.md#ddb-table-217), [DDB-TABLE-444](service.md#ddb-table-444), [DDB-IMPORT-019](#ddb-import-019),
    [DDB-IMPORT-017](#ddb-import-017), [DDB-IMPORT-001](#ddb-import-001) · hypotheses: H-B-036, H-B-018 · evidence: import/idempotency/client-token
  - notes: Refutes the synchronous ResourceInUseException expectation for the CREATING-via-another-import
    case; the first-submitted import is not guaranteed to win.
  - full notes: [details/DDB-IMPORT-018.md](details/DDB-IMPORT-018.md)

## Errors

- <a id="ddb-import-010"></a>**DDB-IMPORT-010** `error-code` · impact medium · unhandled (not handled in controller) · verified 2026-10-09
  **DescribeImport/DescribeExport not-found vs malformed ARN taxonomy (ImportNotFoundException HTTP 400)**
  DescribeImport: well-formed nonexistent ImportArn -> 400 ImportNotFoundException; table ARN ->
  ValidationException 'Invalid Import ARN'; export-shaped ARN -> ValidationException; 'foo' ->
  ParamValidationError 'Parameter validation failed:
  Invalid length for parameter ImportArn, value: 3, valid min length: 37'; cross-region ImportArn ->
  ImportNotFoundException; other-account ImportArn -> AccessDeniedException; tampered id suffix ->
  ImportNotFoundException. DescribeExport: nonexistent ExportArn -> 400 ExportNotFoundException; table ARN ->
  ValidationException; import-shaped ARN -> ValidationException; 'foo' -> ParamValidationError.
  - ACK: exceptions.404, terminal_codes · ops: DescribeImport, DescribeExport
  - repro: DescribeImport/DescribeExport with fabricated, foreign-shaped and non-ARN identifiers
  - handling: not handled in the controller (as of commit 34b85e6)
  - related: [DDB-EXPORT-006](export.md#ddb-export-006), [DDB-IMPORT-011](#ddb-import-011), [DDB-IMPORT-016](#ddb-import-016), [DDB-BACKUP-014](backup.md#ddb-backup-014) · hypotheses: H-B-130 · evidence:
    import/error-taxonomy/failure-modes

- <a id="ddb-import-019"></a>**DDB-IMPORT-019** `error-code` · impact high · unhandled (not handled in controller) · verified 2026-10-09
  **ImportTable into a taken TableName: ACTIVE/DELETING -> ResourceInUseException; CREATING via another import -> accepted (new ImportArn)**
  ImportTable(TableName=<ACTIVE table>) -> ResourceInUseException (HTTP 400) 'Table already exists:
  ackq-641e07-tbl-b'. Same TableName while another import is creating it: no token -> 200 NEW import ARN
  (IN_PROGRESS) ''; other token -> 200 NEW import ARN (IN_PROGRESS). TableName of a table in DELETING ->
  ResourceInUseException 'Table already exists: ackq-641e07-tbl-b'; after it is gone -> 200 NEW import ARN
  (IN_PROGRESS). Token-less replay after the import COMPLETED -> ResourceInUseException.
  - ACK: terminal_codes, custom_create, exceptions.404 · ops: ImportTable, CreateTable, DeleteTable · fields:
    TableCreationParameters.TableName
  - repro: CreateTable TB -> ImportTable(TableName=TB); DeleteTable TB -> ImportTable(TableName=TB) while
    DELETING and after gone
  - handling: not handled in the controller (as of commit 34b85e6)
  - related: [DDB-TABLE-219](table-restore.md#ddb-table-219), [DDB-TABLE-275](table-restore.md#ddb-table-275), [DDB-TABLE-100](table-restore.md#ddb-table-100), [DDB-TABLE-217](table-restore.md#ddb-table-217), [DDB-TABLE-444](service.md#ddb-table-444), [DDB-IMPORT-018](#ddb-import-018),
    [DDB-IMPORT-017](#ddb-import-017), [DDB-IMPORT-001](#ddb-import-001) · hypotheses: H-B-036, H-B-018 · evidence: import/idempotency/client-token
  - notes: Contradiction with [DDB-TABLE-444](service.md#ddb-table-444), [DDB-IMPORT-018](#ddb-import-018): 444 classes 'Table already exists: <TBL>' as the
    ImportTable duplicate response for CREATING/ACTIVE/DELETING alike (PERMANENT); 018/019 show ImportTable
    into a name whose import is still pre-visible (~30 s, DescribeTable ResourceNotFound) is accepted...
  - full notes: [details/DDB-IMPORT-019.md](details/DDB-IMPORT-019.md)

## Request validation

- <a id="ddb-import-009"></a>**DDB-IMPORT-009** `request-validation` · impact medium · unhandled (not handled in controller) · verified 2026-10-09
  **ImportTable request validation: BillingMode/ProvisionedThroughput, PROVISIONED+OnDemandThroughput, CSV delimiter, options/format coupling**
  TableCreationParameters without BillingMode and ProvisionedThroughput -> 400 ValidationException 'One or
  more parameter values were invalid: ReadCapacityUnits and WriteCapacityUnits must both be specified when
  BillingMode is PROVISIONED'. PROVISIONED + OnDemandThroughput -> ValidationException 'One or more parameter
  values were invalid: MaxWriteRequestUnits for OnDemandThroughput cannot be specified when table BillingMode
  is PROVISIONED.'. CSV Delimiter=';;' -> ValidationException '2 validation errors detected: Value ';;' at
  'inputFormatOptions.csv.delimiter' failed to satisfy constraint: Member must satisfy regular expression
  pattern: [,;'. InputFormatOptions.Csv with InputFormat=DYNAMODB_JSON -> ValidationException 'Invalid
  Request: Unsupported InputFormatOptions for the given input format: DYNAMODB_JSON.'.
  InputCompressionType=BZIP2 -> ValidationException '1 validation error detected: Value 'BZIP2' at
  'inputCompressionType' failed to satisfy constraint: Member must satisfy enum value set: [ZSTD, NONE,
  GZIP]'. SSESpecification with a nonexistent KMS alias -> ValidationException 'KMS validation error:
  com.amazonaws.services.kms.model.NotFoundException: Alias
  arn:aws:kms:us-west-2:<ACCOUNT>:alias/ackq-does-not-exist-cc7907 is not found'.
  - ACK: terminal_codes, docs-only · ops: ImportTable · fields: TableCreationParameters.BillingMode,
    TableCreationParameters.ProvisionedThroughput, TableCreationParameters.OnDemandThroughput,
    InputFormatOptions.Csv.Delimiter, InputCompressionType,
    TableCreationParameters.SSESpecification.KMSMasterKeyId
  - repro: ImportTable with each invalid combination; record code/message
  - handling: not handled in the controller (as of commit 34b85e6)
  - hypotheses: H-B-035, H-B-135 · evidence: import/error-taxonomy/failure-modes
  - notes: H-B-035 confirmed: omitting BillingMode defaults to PROVISIONED and the request is then rejected
    for missing ReadCapacityUnits/WriteCapacityUnits. H-B-135 confirmed: OnDemandThroughput with PROVISIONED
    is rejected synchronously. SSESpecification with a nonexistent KMS alias is rejected synchronously...
  - full notes: [details/DDB-IMPORT-009.md](details/DDB-IMPORT-009.md)

## Update granularity and ordering

- <a id="ddb-import-020"></a>**DDB-IMPORT-020** `prerequisite` · impact medium · unhandled (not handled in controller) · verified 2026-10-09
  **Export->Import round trip: export root prefix -> FAILED; data/ + GZIP -> COMPLETED; data/ + NONE -> FAILED...**
  A FULL_EXPORT (DYNAMODB_JSON) of a 3-item table wrote 9 objects under
  exp/AWSDynamoDB/01791506763707-476754e8/ (manifest-summary.json, manifest-files.json, data/*.json.gz; data
  sample line: ). ImportTable results: root: FAILED FailureCode=ItemValidationError processed=7 imported=3
  errors=4 table=ACTIVE scan=3 | gzip: COMPLETED processed=3 imported=3 errors=0 table=ACTIVE scan=3 | none:
  FAILED FailureCode=ItemValidationError processed=4 imported=0 errors=4 table=ACTIVE scan=0 | auto: FAILED
  FailureCode=ItemValidationError processed=4 imported=0 errors=4 table=ACTIVE scan=0.
  - ACK: docs-only, terminal_codes, references · ops: ExportTableToPointInTime, ImportTable, DescribeImport ·
    fields: S3BucketSource.S3KeyPrefix, InputCompressionType, InputFormat
  - repro: Export 3-item PITR table -> ImportTable from <root>/ (GZIP), <root>/data/ (GZIP), <root>/data/
    (NONE), <root>/data/ (omitted) -> poll to terminal -> Scan(COUNT)
  - measurements: export_completed_s=739.41, imports_terminal_s=136.15, export_item_count=3,
    export_billed_size_bytes=0
  - handling: not handled in the controller (as of commit 34b85e6)
  - related: [DDB-IMPORT-013](#ddb-import-013), [DDB-IMPORT-014](#ddb-import-014), [DDB-IMPORT-007](#ddb-import-007), [DDB-IMPORT-012](#ddb-import-012) · hypotheses: H-B-134 · evidence:
    import/dependencies/from-export
  - notes: FailureMessages: root='Some of the items failed validation checks and were not imported. Please
    check CloudWatch error logs for more details.' none='Some of the items failed validation checks and were
    not imported. Please check CloudWatch error logs for more details.' auto='Some of the items failed...
  - full notes: [details/DDB-IMPORT-020.md](details/DDB-IMPORT-020.md)

## Field behavior (defaults, normalization, shapes, immutability)

- <a id="ddb-import-003"></a>**DDB-IMPORT-003** `server-default` · impact medium · unhandled (not handled in controller) · verified 2026-10-09
  **DescribeImport fills server defaults for a minimal ImportTable request (S3BucketOwner, InputCompressionType, ClientToken, log group)**
  For an ImportTable that sent only S3Bucket+S3KeyPrefix, InputFormat=DYNAMODB_JSON and PPR
  TableCreationParameters, DescribeImport reports S3BucketSource={"S3BucketOwner": "<ACCOUNT>", "S3Bucket":
  "ackq-002d44-bkt", "S3KeyPrefix": "t1/"}, InputCompressionType=NONE, InputFormatOptions=null,
  ClientToken=present (SDK-generated UUID),
  CloudWatchLogGroupArn=arn:aws:logs:us-west-2:<ACCOUNT>:log-group:/aws-dynamodb/imports:*.
  - ACK: compare.is_ignored+delta_pre_compare, late_initialize · ops: ImportTable, DescribeImport · fields:
    ImportTableDescription.S3BucketSource.S3BucketOwner, ImportTableDescription.InputCompressionType,
    ImportTableDescription.ClientToken, ImportTableDescription.CloudWatchLogGroupArn,
    ImportTableDescription.InputFormatOptions
  - repro: ImportTable minimal -> DescribeImport; compare members with the request
  - handling: not handled in the controller (as of commit 34b85e6)
  - related: [DDB-IMPORT-002](#ddb-import-002), [DDB-IMPORT-004](#ddb-import-004), [DDB-EXPORT-016](export.md#ddb-export-016), [DDB-EXPORT-017](export.md#ddb-export-017), [DDB-EXPORT-018](export.md#ddb-export-018) · hypotheses:
    H-B-128 · evidence: import/state-machine/lifecycle
  - notes: Compare with the request in result.yaml specs_sent.t1; TableCreationParameters echoed as
    {"TableName": "ackq-002d44-imp-t1", "AttributeDefinitions": [{"AttributeName": "pk", "AttributeType":
    "S"}], "KeySchema": [{"AttributeName": "pk", "KeyType": "HASH"}], "BillingMode": "PAY_PER_REQUEST"}.

- <a id="ddb-import-004"></a>**DDB-IMPORT-004** `normalization` · impact medium · unhandled (not handled in controller) · verified 2026-10-09
  **TableCreationParameters is echoed by DescribeImport while DescribeTable reports the normalized SSE/OnDemandThroughput forms**
  Sent SSESpecification.KMSMasterKeyId=alias/aws/dynamodb and OnDemandThroughput{100,100} with
  PAY_PER_REQUEST. DescribeImport echoes TableCreationParameters={"TableName": "ackq-002d44-imp-t2",
  "AttributeDefinitions": [{"AttributeName": "pk", "AttributeType": "S"}], "KeySchema": [{"AttributeName":
  "pk", "KeyType": "HASH"}], "BillingMode": "PAY_PER_REQUEST", "OnDemandThroughput": {"MaxReadRequestUnits":
  100, "MaxWriteRequestUnits": 100}, "SSESpecification": {"Enabled": true, "SSEType": "KMS", "KMSMasterKeyId":
  "alias/aws/dynamodb"}}; DescribeTable shows SSEDescription=null and OnDemandThroughput=null.
  - ACK: compare.is_ignored+delta_pre_compare, custom_field · ops: ImportTable, DescribeImport, DescribeTable
    · fields: TableCreationParameters.SSESpecification.KMSMasterKeyId,
    TableCreationParameters.OnDemandThroughput, Table.SSEDescription.KMSMasterKeyArn
  - repro: ImportTable with SSESpecification alias + OnDemandThroughput -> DescribeImport vs DescribeTable
    after COMPLETED
  - handling: not handled in the controller (as of commit 34b85e6)
  - related: [DDB-IMPORT-002](#ddb-import-002), [DDB-IMPORT-003](#ddb-import-003), [DDB-EXPORT-016](export.md#ddb-export-016), [DDB-EXPORT-017](export.md#ddb-export-017), [DDB-EXPORT-018](export.md#ddb-export-018) · hypotheses:
    H-B-135 · evidence: import/state-machine/lifecycle
  - notes: Import T2 final status: COMPLETED.

- <a id="ddb-import-012"></a>**DDB-IMPORT-012** `requested-vs-effective` · impact high · unhandled (not handled in controller) · verified 2026-10-09
  **ImportTable(bad bucket): 200 then FAILED S3NoSuchBucket in ~6s, no table created; empty prefix: COMPLETED with an empty ACTIVE table**
  ImportTable(S3Bucket=<nonexistent>) returned 200 ok (IN_PROGRESS). DescribeImport keeps TableArn=present.
  Final DescribeImport: bad-bucket: ImportStatus=FAILED FailureCode=S3NoSuchBucket Processed=0 Imported=0
  Errors=0; table=ResourceNotFoundException ItemCount=None scan=None (FailureMessage: The specified bucket
  does not exist (Service: Amazon S3; Status Code: 404; Error Code: NoSuchBucket; Request ID:
  YKWGHBH09YM211VG; S3 Extended Request ID: eW+Mqmru0bDZElFBows4BrnDVn0veDga4QbIP1UrxUrRa). Empty prefix case:
  empty-prefix: ImportStatus=COMPLETED FailureCode=None Processed=0 Imported=0 Errors=0; table=ACTIVE
  ItemCount=0 scan=0. Table first seen: bad-bucket=null empty-prefix={"elapsed_s": 261.5, "table_status":
  "ACTIVE", "import_status": "COMPLETED"}; table gone again at: bad-bucket=null empty-prefix=null.
  - ACK: terminal_codes, synced.when, custom_create · ops: ImportTable, DescribeImport, DescribeTable ·
    fields: S3BucketSource.S3Bucket, ImportTableDescription.FailureCode, ImportTableDescription.TableArn
  - repro: ImportTable(S3Bucket=does-not-exist) -> poll DescribeImport + DescribeTable every 15s to terminal
  - measurements: bad_bucket_failed_after_s=5.6, empty_prefix_completed_after_s=100.2
  - handling: not handled in the controller (as of commit 34b85e6)
  - related: [DDB-EXPORT-010](export.md#ddb-export-010), [DDB-EXPORT-011](export.md#ddb-export-011), [DDB-EXPORT-005](export.md#ddb-export-005), [DDB-EXPORT-012](export.md#ddb-export-012), [DDB-IMPORT-013](#ddb-import-013), [DDB-IMPORT-014](#ddb-import-014),
    [DDB-IMPORT-020](#ddb-import-020), [DDB-IMPORT-007](#ddb-import-007) · hypotheses: H-B-033, H-B-127 · evidence:
    import/error-taxonomy/failure-modes
  - notes: H-B-033 partially refuted (bad bucket does not leave an ACTIVE table; an empty prefix is COMPLETED
    not FAILED); H-B-127 confirmed for the pre-copy failure: DescribeTable never succeeded for the bad-bucket
    target although DescribeImport still returns TableArn and TableId (dangling reference)....
  - full notes: [details/DDB-IMPORT-012.md](details/DDB-IMPORT-012.md)

- <a id="ddb-import-014"></a>**DDB-IMPORT-014** `normalization` · impact medium · unhandled (not handled in controller) · verified 2026-10-09
  **S3KeyPrefix is a raw key-prefix match ('data' matched 3 objects) and a single object key is a valid prefix**
  Objects data/1.json (valid), data2/x.json (garbage) and data.bak (garbage). S3KeyPrefix='data' ->
  raw-prefix: ImportStatus=FAILED FailureCode=ItemValidationError Processed=3 Imported=1 Errors=2;
  table=ACTIVE ItemCount=0 scan=1. S3KeyPrefix='data/1.json' -> single-object: ImportStatus=COMPLETED
  FailureCode=None Processed=1 Imported=1 Errors=0; table=ACTIVE ItemCount=0 scan=1. Echoed S3KeyPrefix
  values: 'data' / 'data/1.json' (no trailing slash added).
  - ACK: docs-only, compare.is_ignored+delta_pre_compare · ops: ImportTable, DescribeImport · fields:
    S3BucketSource.S3KeyPrefix
  - repro: Upload data/1.json, data2/x.json, data.bak -> ImportTable(S3KeyPrefix='data') and ('data/1.json')
    -> poll to terminal
  - handling: not handled in the controller (as of commit 34b85e6)
  - related: [DDB-IMPORT-013](#ddb-import-013), [DDB-IMPORT-020](#ddb-import-020), [DDB-IMPORT-007](#ddb-import-007), [DDB-IMPORT-012](#ddb-import-012) · hypotheses: H-B-140 · evidence:
    import/error-taxonomy/failure-modes
  - notes: H-B-140 confirmed: 'data' also matched data2/x.json and data.bak (ErrorCount=2,
    FAILED/ItemValidationError), the single valid item was still imported; a single object key works as a
    prefix.

- <a id="ddb-import-015"></a>**DDB-IMPORT-015** `server-default` · impact medium · unhandled (not handled in controller) · verified 2026-10-09
  **CSV without InputFormatOptions: COMPLETED, options omitted in DescribeImport (no default Delimiter); GZIP on a plain file -> FAILED**
  CSV with a header row and no InputFormatOptions -> csv-plain: ImportStatus=COMPLETED FailureCode=None
  Processed=3 Imported=3 Errors=0; table=ACTIVE ItemCount=0 scan=3; DescribeImport.InputFormatOptions=null.
  CSV with Delimiter=';' + HeaderList -> csv-options: ImportStatus=COMPLETED FailureCode=None Processed=3
  Imported=3 Errors=0; table=ACTIVE ItemCount=0 scan=3; echo={"Csv": {"Delimiter": ";", "HeaderList": ["pk",
  "val"]}}. InputCompressionType=GZIP on an uncompressed file -> FAILED FailureCode=ItemValidationError
  FailureMessage='Some of the items failed validation checks and were not imported. Please check CloudWatch
  error logs for more details.'.
  - ACK: compare.is_ignored+delta_pre_compare, terminal_codes · ops: ImportTable, DescribeImport · fields:
    InputFormatOptions.Csv.Delimiter, InputFormatOptions.Csv.HeaderList, InputCompressionType
  - repro: ImportTable CSV without/with InputFormatOptions; ImportTable GZIP on plain file -> DescribeImport
    at terminal
  - handling: not handled in the controller (as of commit 34b85e6)
  - hypotheses: H-B-128 · evidence: import/error-taxonomy/failure-modes
  - notes: H-B-128 CSV half refuted: no server default Delimiter=',' is reported, InputFormatOptions is simply
    omitted; explicit options are echoed verbatim. A compression mismatch is not detected up front: it
    surfaces as ItemValidationError (ProcessedItemCount=1, ImportedItemCount=0) with the table left...
  - full notes: [details/DDB-IMPORT-015.md](details/DDB-IMPORT-015.md)

## Delete semantics

- <a id="ddb-import-006"></a>**DDB-IMPORT-006** `delete-semantics` · impact high · handled · verified 2026-10-09
  **DeleteTable on an importing table: ResourceInUseException while CREATING; accepted once ACTIVE ~6s before the import flips COMPLETED**
  DeleteTable attempts from t+30s every 15s returned (elapsed_s, code, ImportStatus, TableStatus): [(30.8,
  'ResourceInUseException', 'IN_PROGRESS', 'CREATING'), (49.4, 'ResourceInUseException', 'IN_PROGRESS',
  'CREATING'), (68.0, 'ResourceInUseException', 'IN_PROGRESS', 'CREATING'), (86.4, 'ResourceInUseException',
  'IN_PROGRESS', 'CREATING'), (104.8, 'ResourceInUseException', 'IN_PROGRESS', 'CREATING'), (123.1,
  'ResourceInUseException', 'IN_PROGRESS', 'CREATING'), (141.5, 'ResourceInUseException', 'IN_PROGRESS',
  'CREATING'), (159.9, 'ResourceInUseException', 'IN_PROGRESS', 'CREATING'), (178.3, 'ResourceInUseException',
  'IN_PROGRESS', 'CREATING'), (196.9, 'ResourceInUseException', 'IN_PROGRESS', 'CREATING'), (215.2, 'ok',
  'IN_PROGRESS', 'ACTIVE')]. Distinct (ImportStatus, TableStatus) pairs observed for the import:
  [['IN_PROGRESS', 'ERR:ResourceNotFoundException'], ['IN_PROGRESS', 'ERR:ResourceNotFoundException'],
  ['IN_PROGRESS', 'CREATING'], ['IN_PROGRESS', 'CREATING'], ['IN_PROGRESS', 'CREATING'], ['IN_PROGRESS',
  'CREATING'], ['IN_PROGRESS', 'CREATING'], ['IN_PROGRESS', 'ACTIVE'], ['COMPLETED', 'DELETING']]; final
  DescribeImport status=COMPLETED FailureCode=None FailureMessage=; final
  DescribeTable=ResourceNotFoundException.
  - ACK: custom_delete, deletable.when, terminal_codes · ops: ImportTable, DeleteTable, DescribeImport ·
    fields: ImportTableDescription.ImportStatus, ImportTableDescription.FailureCode
  - repro: ImportTable (30 MB) -> from t+30s DeleteTable(target) every 15s -> poll
    DescribeImport/DescribeTable to terminal
  - measurements: t3_delete_accepted_at_s=215.2
  - handling: handled via `generator.yaml:104-109; pkg/resource/table/hooks.go:72-93`
  - related: [DDB-TABLE-277](table-restore.md#ddb-table-277), [DDB-TABLE-298](table-replicas.md#ddb-table-298) · hypotheses: H-B-034 · evidence: import/state-machine/lifecycle
  - notes: There is no cancel API for imports. CANCELLING/CANCELLED was NOT reached: the table turns ACTIVE a
    few seconds before ImportStatus becomes COMPLETED and DeleteTable in that window is accepted while the
    import still ends COMPLETED (ImportedItemCount=28500). H-B-034 refuted for the 'accepted while...
  - full notes: [details/DDB-IMPORT-006.md](details/DDB-IMPORT-006.md)

## Scope

- <a id="ddb-import-005"></a>**DDB-IMPORT-005** `scope` · impact high · unhandled (not handled in controller) · verified 2026-10-09
  **DescribeTable of an imported table carries no import marker; post-import mutations are admitted once the import is terminal**
  **Scope verdict: field-on-parent**
  After the import reached COMPLETED the table's DescribeTable keys were None (keys mentioning import/restore:
  []). Post-import TagResource=ok, UpdateTimeToLive=ok, UpdateContinuousBackups=ok,
  UpdateTable(DeletionProtection)=ok, CreateBackup=ok; ListImports(TableArn) is the only link.
  - ACK: scope:field-on-parent, custom_create, post-create-nudge · ops: ImportTable, DescribeTable,
    ListImports, TagResource, UpdateTimeToLive, UpdateContinuousBackups, UpdateTable
  - repro: ImportTable -> wait COMPLETED -> DescribeTable keys ->
    TagResource/UpdateTimeToLive/UpdateContinuousBackups/UpdateTable
  - handling: not handled in the controller (as of commit 34b85e6)
  - related: [DDB-TABLE-216](table-restore.md#ddb-table-216), [DDB-TABLE-278](table-restore.md#ddb-table-278), [DDB-IMPORT-008](#ddb-import-008), [DDB-EXPORT-020](export.md#ddb-export-020), [DDB-BACKUP-018](backup.md#ddb-backup-018), [DDB-EXPORT-021](export.md#ddb-export-021) ·
    hypotheses: H-B-039, H-B-022 · evidence: import/state-machine/lifecycle
  - notes: Scope verdict: ImportTable is a Table constructor (creates the table, returns TableArn/TableId, no
    delete/cancel API); model it as a create-only immutable field group on Table (spec.importSource) rather
    than a standalone CRD. H-B-022 import half (no marker) tested; restore half not in this shard.

## Handling gaps (bugs to file)

None recorded for this document's findings; see [service.md 'Handling gaps summary'](service.md#handling-gaps-summary) for the service-wide list.

## E2E timing

Values are seconds unless the key says otherwise; n = trials behind the numbers ('1 run' when the finding records none).

| finding | what | measurements | n |
| --- | --- | --- | --- |
| [DDB-IMPORT-001](#ddb-import-001) | ImportTable returns TableArn/TableId at once but the table is ResourceNotFoundException at t+0; it appears as CREATING ~30.72s later | t1_table_visible_s=30.72, t1_table_active_s=110.83, t1_import_terminal_s=110.83, t1_active_minus_terminal_s=0.0, poll_interval_s=6 | 1 run |
| [DDB-IMPORT-006](#ddb-import-006) | DeleteTable on an importing table: ResourceInUseException while CREATING; accepted once ACTIVE ~6s before the import flips COMPLETED | t3_delete_accepted_at_s=215.2 | 1 run |
| [DDB-IMPORT-012](#ddb-import-012) | ImportTable(bad bucket): 200 then FAILED S3NoSuchBucket in ~6s, no table created; empty prefix: COMPLETED with an empty ACTIVE table | bad_bucket_failed_after_s=5.6, empty_prefix_completed_after_s=100.2 | 1 run |
| [DDB-IMPORT-013](#ddb-import-013) | Any bad line makes the import FAILED/ItemValidationError, yet valid rows are written (table ACTIVE with ImportedItemCount items) | missing_key_failed_after_s=174.3, garbage_line_failed_after_s=90.1 | 1 run |
| [DDB-IMPORT-020](#ddb-import-020) | Export->Import round trip: export root prefix -> FAILED; data/ + GZIP -> COMPLETED; data/ + NONE -> FAILED... | export_completed_s=739.41, imports_terminal_s=136.15, export_item_count=3, export_billed_size_bytes=0 | 1 run |

## Open questions

<!-- preserved:start id=open-questions -->
<!-- open questions and follow-up experiments; survives re-renders -->
<!-- preserved:end -->

## Appendix: low-impact and duplicate findings

| id | category | impact | status | title | related | duplicate_of |
| --- | --- | --- | --- | --- | --- | --- |
| <a id="ddb-import-002"></a>**DDB-IMPORT-002** | response-fidelity | low | confirmed | Import progress counters (ProcessedSizeBytes/ImportedItemCount/ErrorCount) presence while IN_PROGRESS vs terminal | [DDB-IMPORT-003](#ddb-import-003), [DDB-IMPORT-004](#ddb-import-004), [DDB-EXPORT-016](export.md#ddb-export-016), [DDB-EXPORT-017](export.md#ddb-export-017), [DDB-EXPORT-018](export.md#ddb-export-018) | - |
| <a id="ddb-import-011"></a>**DDB-IMPORT-011** | request-validation | low | confirmed | ListImports.PageSize / ListExports.MaxResults bounds and NextToken validation | [DDB-EXPORT-006](export.md#ddb-export-006), [DDB-IMPORT-010](#ddb-import-010), [DDB-IMPORT-016](#ddb-import-016), [DDB-BACKUP-014](backup.md#ddb-backup-014) | - |
| <a id="ddb-import-016"></a>**DDB-IMPORT-016** | response-fidelity | low | confirmed | ListImports/ListExports default page is 10 entries (NextToken set) and includes IN_PROGRESS and FAILED jobs, newest first | [DDB-EXPORT-006](export.md#ddb-export-006), [DDB-IMPORT-010](#ddb-import-010), [DDB-IMPORT-011](#ddb-import-011), [DDB-BACKUP-014](backup.md#ddb-backup-014) | - |

## Supplementary notes

<!-- preserved:start -->
<!-- preserved:end -->
