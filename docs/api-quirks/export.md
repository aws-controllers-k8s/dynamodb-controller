<!-- generated: ack-api-quirks dynamodb 2026-10-09 model=2012-08-10 controller=34b85e6 -->
# ExportTableToPointInTime (scope investigation)
_Export job lifecycle, idempotency and the scope verdict for an Export resource._
Generated from ack-api-quirks `services/dynamodb` (render date in the marker above); model 2012-08-10 (service/dynamodb v1.39.8); controller commit 34b85e6; evidence: `services/dynamodb/probes/<probe id>/` in the lab repo.

## Overview

<!-- preserved:start id=overview -->
ExportTableToPointInTime starts a write-once job that copies a PITR-enabled table's snapshot to S3; the job has no delete or cancel API, its record is immutable after COMPLETED/FAILED and outlives the source table, and the S3 objects outlive the record ([DDB-EXPORT-021](#ddb-export-021), [DDB-EXPORT-020](#ddb-export-020)). Status moves IN_PROGRESS -> COMPLETED|FAILED (never PENDING), with ItemCount, BilledSizeBytes, ExportManifest and EndTime appearing only at terminal ([DDB-EXPORT-016](#ddb-export-016)).

### Rules a reconciler must respect
- Prerequisites fail synchronously (HTTP 400): PITR off -> PointInTimeRecoveryUnavailableException, missing table -> TableNotFoundException, bare name -> ValidationException, ExportTime outside the window -> InvalidExportTimeException; the PITR check runs before any S3 check, so PITR-off must be retryable when a sibling Table CR is about to enable it ([DDB-EXPORT-001](#ddb-export-001)).
- Bucket, owner, bucket-policy and KMS problems are accepted (200 IN_PROGRESS) and surface minutes later as FAILED with FailureCode S3NoSuchBucket / S3AccessDenied / KmsNotFoundException; FAILED is terminal and FailureCode/FailureMessage must be surfaced ([DDB-EXPORT-010](#ddb-export-010), [DDB-EXPORT-011](#ddb-export-011), [DDB-EXPORT-005](#ddb-export-005)).
- ClientToken: an identical replay returns the same ARN (also after COMPLETED or FAILED); any changed parameter under the same token is ExportConflictException; token-less duplicates create two billed exports. Derive the token deterministically and mint a new one only to retry a FAILED export ([DDB-EXPORT-009](#ddb-export-009), [DDB-EXPORT-010](#ddb-export-010)).
- DescribeExport back-fills ExportFormat=DYNAMODB_JSON and S3SseAlgorithm=AES256 but never echoes ExportType (None); a spec with exportType=FULL_EXPORT drifts forever without a compare hook ([DDB-EXPORT-017](#ddb-export-017)).
- ReadOne only via DescribeExport(ExportArn): ListExports(TableArn) returns 0 entries once the source is deleted or re-created under the same name, although DescribeExport still works ([DDB-EXPORT-020](#ddb-export-020), [DDB-EXPORT-022](#ddb-export-022)).
- Not-found is ExportNotFoundException (HTTP 400), also for a real ARN called from another region; a table ARN is ValidationException 'Invalid Export ARN'; another account is AccessDeniedException ([DDB-EXPORT-006](#ddb-export-006)).
- INCREMENTAL_EXPORT requires ExportFromTime and a >= 15 min window; on a freshly PITR-enabled table it is InvalidExportTimeException (retryable) until ~16 min after enable ([DDB-EXPORT-003](#ddb-export-003), [DDB-EXPORT-004](#ddb-export-004), [DDB-EXPORT-019](#ddb-export-019)).
- The live API accepts FilterSpecification and returns it plus DestinationType=S3, both absent from the pinned SDK model ([DDB-EXPORT-008](#ddb-export-008)). ExportTableToPointInTime throttles after ~20 rapid calls (rejected calls count) and Describe bursts of 13-18 throttle too ([DDB-EXPORT-007](#ddb-export-007)).
- An export started within ~1 s of DeleteTable completes after the table is gone; attempts at +1.7 s or later are TableNotFoundException ([DDB-TABLE-454](table-subresources.md#ddb-table-454), table-subresources.md).
- Nothing is implemented in the controller: a generated resource would need a no-op delete, an immutable spec, a compare hook for ExportType and handling for SDK model drift around FilterSpecification ([DDB-EXPORT-021](#ddb-export-021), [DDB-EXPORT-017](#ddb-export-017), [DDB-EXPORT-008](#ddb-export-008)).

### Timing you should expect
- FULL_EXPORT of an empty table: 125-155 s (n=6, [DDB-EXPORT-016](#ddb-export-016)); a 3-item table: ~12 min (724 s, [DDB-EXPORT-022](#ddb-export-022)); six COMPLETED exports in [DDB-EXPORT-011](#ddb-export-011): 706-726 s; the incremental export was accepted 967 s after PITR enable ([DDB-EXPORT-019](#ddb-export-019)). Any e2e is many minutes long and status polling must be paced ([DDB-EXPORT-007](#ddb-export-007)).

### Known handling gaps in the controller
- No Export finding is stored as suspect-bug or partial: there is no Export code to be wrong, and the Table TTL template that one entry names as 'handled' does not implement any of the rules above.

### Scope verdict
implement, as a Job-like standalone Export CRD: create-only, status mirroring DescribeExport, delete = drop the finalizer only (no DeleteExport/CancelExport exists), deterministic ClientToken, never re-create after FAILED without a spec change; the create/read lifecycle is real while the delete must be a no-op ([DDB-EXPORT-021](#ddb-export-021), [DDB-EXPORT-009](#ddb-export-009), [DDB-EXPORT-010](#ddb-export-010)).

### Where to look next
- Generated sections below: State machine, Idempotency, Errors, Request validation (window rules [DDB-EXPORT-003](#ddb-export-003), [DDB-EXPORT-004](#ddb-export-004)), E2E timing. Siblings: PITR enable and the 0-6 s ContinuousBackupsUnavailable window (table-subresources.md; [DDB-BACKUP-002](backup.md#ddb-backup-002), backup.md), consuming an export ([DDB-IMPORT-020](import.md#ddb-import-020), import.md). Evidence: services/dynamodb/probes/export/*.

Entries below are generated from the lab findings; low-impact items are in the appendix, long notes under details/.
<!-- preserved:end -->

## At a glance

- canonical findings: 26 (high 6 / medium 14 / low 6); duplicates folded into the appendix: 0
- handling: handled 1 · partial 0 · tracked 0 · unhandled 21 · suspect-bug 0 · n-a 4 (tracked = handled/partial whose reference is an open GitHub issue; counted as not handled)
- re-verified: 0 · last_verified: 2026-10-09 · model: 2012-08-10 (service/dynamodb v1.39.8)
- categories: other 4, request-validation 4, error-code 2, eventual-consistency 2, idempotency 2,
  normalization 2, async-state-machine 1, codegen-artifact 1, delete-semantics 1, identity 1, prerequisite 1,
  quota-limit 1, requested-vs-effective 1, response-fidelity 1, scope 1, server-default 1

## Operations

| operation | kind | required inputs | declared error shapes | paginated |
| --- | --- | --- | --- | --- |
| CreateTable | create | TableName | ResourceInUseException, LimitExceededException, InternalServerError | no |
| DeleteTable | delete | TableName | ResourceInUseException, ResourceNotFoundException, LimitExceededException, InternalServerError | no |
| DescribeContinuousBackups | read | TableName | TableNotFoundException, InternalServerError | no |
| DescribeExport | read | ExportArn | ExportNotFoundException, LimitExceededException, InternalServerError | no |
| DescribeImport | read | ImportArn | ImportNotFoundException | no |
| ExportTableToPointInTime | other | TableArn, S3Bucket | TableNotFoundException, PointInTimeRecoveryUnavailableException, LimitExceededException, InvalidExportTimeException, ExportConflictException, InternalServerError | no |
| ImportTable | create | S3BucketSource, InputFormat, TableCreationParameters | ResourceInUseException, LimitExceededException, ImportConflictException | no |
| ListExports | list | - | LimitExceededException, InternalServerError | no |
| ListImports | list | - | LimitExceededException | no |
| RestoreTableToPointInTime | create | TargetTableName | TableAlreadyExistsException, TableNotFoundException, TableInUseException, LimitExceededException, InvalidRestoreTimeException, PointInTimeRecoveryUnavailableException, InternalServerError | no |
| UpdateContinuousBackups | update | TableName, PointInTimeRecoverySpecification | TableNotFoundException, ContinuousBackupsUnavailableException, InternalServerError | no |

## State machine

- **ExportStatus**: IN_PROGRESS, COMPLETED, FAILED (transitional: IN_PROGRESS)
- **PointInTimeRecoveryStatus**: ENABLED, DISABLED
- **TableStatus**: CREATING, UPDATING, DELETING, ACTIVE, INACCESSIBLE_ENCRYPTION_CREDENTIALS, ARCHIVING,
  ARCHIVED, REPLICATION_NOT_AUTHORIZED (transitional: CREATING, UPDATING, DELETING, ARCHIVING)

- <a id="ddb-export-016"></a>**DDB-EXPORT-016** `async-state-machine` · impact high · unhandled (not handled in controller) · verified 2026-10-09
  **FULL_EXPORT of an empty table: COMPLETED after ~152.7s (EndTime-StartTime); ExportStatus never PENDING...**
  Create response ExportStatus=IN_PROGRESS, DescribeExport at t+0 -> ok (IN_PROGRESS). Members present while
  IN_PROGRESS (create response / t+0 describe): ['ClientToken', 'ExportArn', 'ExportFormat', 'ExportStatus',
  'ExportTime', 'S3Bucket', 'S3SseAlgorithm', 'StartTime', 'TableArn', 'TableId']; at terminal:
  ['BilledSizeBytes', 'ClientToken', 'EndTime', 'ExportFormat', 'ExportManifest', 'ExportTime', 'ItemCount',
  'S3SseAlgorithm', 'StartTime'] with ItemCount=0 BilledSizeBytes=0
  ExportManifest=AWSDynamoDB/01791506414432-3f2e5aa4/manifest-summary.json. Durations (EndTime-StartTime) per
  export: {"e1": 152.7, "foo": 154.9, "foo-slash": 152.6, "slash-foo": 152.2, "ion": 153.9, "inc": 125.7}.
  Statuses seen across all exports: ['COMPLETED'].
  - ACK: synced.when, late_initialize, is_read_only · ops: ExportTableToPointInTime, DescribeExport · fields:
    ExportDescription.ExportStatus, ExportDescription.ItemCount, ExportDescription.BilledSizeBytes,
    ExportDescription.ExportManifest, ExportDescription.EndTime
  - repro: Empty PITR table -> ExportTableToPointInTime -> DescribeExport every 15s to terminal
  - measurements: e1_duration_s_from_timestamps=152.7, export_durations_s.e1=152.7,
    export_durations_s.foo=154.9, export_durations_s.foo-slash=152.6, export_durations_s.slash-foo=152.2,
    export_durations_s.ion=153.9, export_durations_s.inc=125.7, poll_interval_s=15
  - handling: not handled in the controller (as of commit 34b85e6)
  - related: [DDB-EXPORT-009](#ddb-export-009), [DDB-EXPORT-011](#ddb-export-011), [DDB-EXPORT-012](#ddb-export-012), [DDB-EXPORT-022](#ddb-export-022), [DDB-TABLE-454](table-subresources.md#ddb-table-454), [DDB-IMPORT-002](import.md#ddb-import-002),
    [DDB-IMPORT-003](import.md#ddb-import-003), [DDB-IMPORT-004](import.md#ddb-import-004), [DDB-EXPORT-017](#ddb-export-017), [DDB-EXPORT-018](#ddb-export-018) · hypotheses: H-B-028, H-B-121, H-B-030 ·
    evidence: export/state-machine/lifecycle
  - notes: ListExports(TableArn) showed the export 0.2s after creation with status IN_PROGRESS and summary
    keys ['ExportArn', 'ExportStatus', 'ExportType'] (H-B-030).

## Field matrix

C = accepted by the create input (ExportTableToPointInTime - the job-start operation; the resource has no
Create<Noun>, so an ACK resource would issue this call on create), U = by the update input, R = present in the
read output.

| leaf | C | U | R | type |
| --- | --- | --- | --- | --- |
| BilledSizeBytes | - | - | x | long |
| ClientToken | x | - | x | string |
| EndTime | - | - | x | timestamp |
| ExportArn | - | - | x | string |
| ExportDescription | - | - | x | struct:ExportDescription |
| ExportFormat | x | - | x | enum:ExportFormat |
| ExportFromTime | x | - | x | timestamp |
| ExportManifest | - | - | x | string |
| ExportStatus | - | - | x | enum:ExportStatus |
| ExportTime | x | - | x | timestamp |
| ExportToTime | x | - | x | timestamp |
| ExportType | x | - | x | enum:ExportType |
| ExportViewType | x | - | x | enum:ExportViewType |
| FailureCode | - | - | x | string |
| FailureMessage | - | - | x | string |
| IncrementalExportSpecification | x | - | x | struct:IncrementalExportSpecification |
| ItemCount | - | - | x | long |
| S3Bucket | x | - | x | string |
| S3BucketOwner | x | - | x | string |
| S3Prefix | x | - | x | string |
| S3SseAlgorithm | x | - | x | enum:S3SseAlgorithm |
| S3SseKmsKeyId | x | - | x | string |
| StartTime | - | - | x | timestamp |
| TableArn | x | - | x | string |
| TableId | - | - | x | string |

## Identity and lookup

- <a id="ddb-export-020"></a>**DDB-EXPORT-020** `identity` · impact medium · unhandled (not handled in controller) · verified 2026-10-09
  **Export records outlive the source table (DescribeExport ok) but ListExports(TableArn) returns 0 once it is deleted or re-created by name**
  After DeleteTable: DescribeExport -> ok (TableArn/TableId still present: True), ListExports(TableArn) -> 0
  entries. After re-creating a table with the same name (new TableId 5dec6036-34c3-43dd-ae21-b7de6c44e66c vs
  export TableId 8dee4e27-6be3-4358-9987-8b55b06a732e) ListExports(TableArn) -> 0 entries; ExportSummary
  carries TableId: False.
  - ACK: custom_find, list_operation.match_fields · ops: DeleteTable, DescribeExport, ListExports, CreateTable
  - repro: Export -> COMPLETED -> DeleteTable -> DescribeExport/ListExports(TableArn) -> CreateTable same name
    -> ListExports(TableArn)
  - handling: not handled in the controller (as of commit 34b85e6)
  - related: [DDB-TABLE-216](table-restore.md#ddb-table-216), [DDB-IMPORT-005](import.md#ddb-import-005), [DDB-TABLE-278](table-restore.md#ddb-table-278), [DDB-IMPORT-008](import.md#ddb-import-008), [DDB-EXPORT-013](#ddb-export-013), [DDB-TABLE-454](table-subresources.md#ddb-table-454),
    [DDB-EXPORT-021](#ddb-export-021), [DDB-EXPORT-022](#ddb-export-022) · hypotheses: H-B-142, H-B-038 · evidence: export/state-machine/lifecycle
  - notes: Contradiction with [DDB-EXPORT-022](#ddb-export-022), [DDB-IMPORT-008](import.md#ddb-import-008): 020's title says export records are 'keyed by
    the name-based TableArn', yet its behavior shows ListExports(TableArn) -> 0 entries immediately after
    DeleteTable and still 0 after a same-name re-create (same ARN string, new TableId); 022 confirms...
  - full notes: [details/DDB-EXPORT-020.md](details/DDB-EXPORT-020.md)

## Idempotency

- <a id="ddb-export-009"></a>**DDB-EXPORT-009** `idempotency` · impact high · unhandled (not handled in controller) · verified 2026-10-09
  **Export ClientToken: same params -> 200 same ARN; changed S3Prefix -> ExportConflictException...**
  Replay with identical params -> 200 same ARN. Same token + other S3Prefix -> ExportConflictException 'Export
  conflict: Duplicate request detected with conflicting parameters'. Same token + other TableArn ->
  ExportConflictException 'Export conflict: Duplicate request detected with conflicting parameters'. Same
  token + ExportFormat=ION -> ExportConflictException 'Export conflict: Duplicate request detected with
  conflicting parameters'. Two token-less identical calls -> distinct ARNs: True (both COMPLETED after 725.5s
  / COMPLETED after 724.1s). Same token string used for ImportTable -> accepted (import final COMPLETED).
  Replay of TOK1 after the export COMPLETED (~2 min later) -> 200 same ARN.
  - ACK: custom_create, terminal_codes, requeue · ops: ExportTableToPointInTime, ImportTable · fields:
    ClientToken, S3Prefix, TableArn, ExportFormat
  - repro: ExportTableToPointInTime(token=T) then replays with same/changed params; two calls without token;
    ImportTable with the same token
  - handling: not handled in the controller (as of commit 34b85e6)
  - related: [DDB-EXPORT-010](#ddb-export-010), [DDB-IMPORT-017](import.md#ddb-import-017), [DDB-EXPORT-016](#ddb-export-016), [DDB-EXPORT-011](#ddb-export-011), [DDB-EXPORT-012](#ddb-export-012), [DDB-EXPORT-022](#ddb-export-022),
    [DDB-TABLE-454](table-subresources.md#ddb-table-454) · hypotheses: H-B-019, H-B-149 · evidence: export/idempotency/client-token
  - notes: Each token-less retry is a new billed export; a controller must derive ClientToken
    deterministically.

- <a id="ddb-export-010"></a>**DDB-EXPORT-010** `idempotency` · impact high · unhandled (not handled in controller) · verified 2026-10-09
  **Nonexistent bucket: sync accepted -> FAILED FailureCode=S3NoSuchBucket; replay of the same token after FAILED -> 200 same ARN...**
  ExportTableToPointInTime(S3Bucket=<nonexistent>, token=TOK4) -> 200 IN_PROGRESS; DescribeExport reached
  FAILED FailureCode=S3NoSuchBucket (FailureMessage 'The specified bucket does not exist (Service: Amazon S3;
  Status Code: 404; Error Code: NoSuchBucket; Request ID: 3D8HBXCYW1RF4Q69; S3 Extended Request ID: /Je1p').
  Replays 16.5s after the create: identical -> 200 same ARN; same token with the bucket fixed ->
  ExportConflictException 'Export conflict: Duplicate request detected with conflicting parameters'; new token
  -> 200 (None).
  - ACK: terminal_codes, custom_create, requeue · ops: ExportTableToPointInTime, DescribeExport · fields:
    S3Bucket, ClientToken, ExportDescription.FailureCode
  - repro: Export to a nonexistent bucket with token T -> wait FAILED -> replay T identical / T with bucket
    fixed / new token
  - handling: not handled in the controller (as of commit 34b85e6)
  - related: [DDB-EXPORT-011](#ddb-export-011), [DDB-EXPORT-005](#ddb-export-005), [DDB-EXPORT-012](#ddb-export-012), [DDB-IMPORT-012](import.md#ddb-import-012), [DDB-EXPORT-009](#ddb-export-009), [DDB-IMPORT-017](import.md#ddb-import-017) ·
    hypotheses: H-B-027, H-B-122 · evidence: export/idempotency/client-token

## Errors

- <a id="ddb-export-002"></a>**DDB-EXPORT-002** `error-code` · impact medium · unhandled (not handled in controller) · verified 2026-10-09
  **Export TableArn: other account -> AccessDeniedException; ARN region ignored, name looked up in endpoint region -> TableNotFoundException**
  From the us-east-1 endpoint with a us-west-2 TableArn -> TableNotFoundException 'Table not found:
  arn:aws:dynamodb:us-west-2:<ACCOUNT>:table/ackq-86a5a7-exp-val'. TableArn with another account id ->
  AccessDeniedException 'Access is denied'. TableArn with another region -> TableNotFoundException 'Table not
  found: arn:aws:dynamodb:us-east-1:<ACCOUNT>:table/ackq-86a5a7-exp-val'. Index ARN -> ValidationException
  'Invalid Request: Table ARN is invalid.'. S3Bucket given as an S3 ARN -> ValidationException '1 validation
  error detected: Value 'arn:aws:s3:::ackq-86a5a7-bkt' at 's3Bucket' failed to satisfy co'; uppercase bucket
  name -> accepted(IN_PROGRESS); S3BucketOwner='not-an-account' -> ValidationException '1 validation error
  detected: Value 'not-an-account' at 's3BucketOwner' failed to satisfy constraint:'; ExportFormat=CSV ->
  ValidationException '1 validation error detected: Value 'CSV' at 'exportFormat' failed to satisfy
  constraint: Member must'; ExportType=PARTIAL_EXPORT -> ValidationException '1 validation error detected:
  Value 'PARTIAL_EXPORT' at 'exportType' failed to satisfy constraint: Me'; S3SseAlgorithm='aws:kms' ->
  ValidationException '1 validation error detected: Value 'aws:kms' at 's3SseAlgorithm' failed to satisfy
  constraint: Membe'.
  - ACK: terminal_codes, references · ops: ExportTableToPointInTime · fields: TableArn, S3Bucket,
    S3BucketOwner, ExportFormat, ExportType, S3SseAlgorithm
  - repro: ExportTableToPointInTime with cross-account / cross-region ARNs and malformed enum/bucket values
  - handling: not handled in the controller (as of commit 34b85e6)
  - related: [DDB-EXPORT-001](#ddb-export-001), [DDB-TABLE-447](service.md#ddb-table-447), [DDB-BACKUP-012](backup.md#ddb-backup-012), [DDB-TABLE-274](table-restore.md#ddb-table-274), [DDB-TABLE-273](table-restore.md#ddb-table-273), [DDB-TABLE-272](table-restore.md#ddb-table-272) ·
    hypotheses: H-B-124 · evidence: export/error-taxonomy/sync-validation
  - notes: H-B-124 refuted on both counts: neither case is a ValidationException. The ARN's region component
    is not validated (a us-east-1 ARN sent to us-west-2 fails with TableNotFoundException quoting the
    us-east-1 ARN; a us-west-2 ARN sent to us-east-1 likewise), and a foreign account id yields...
  - full notes: [details/DDB-EXPORT-002.md](details/DDB-EXPORT-002.md)

- <a id="ddb-export-006"></a>**DDB-EXPORT-006** `error-code` · impact medium · unhandled (not handled in controller) · verified 2026-10-09
  **DescribeExport: nonexistent -> ExportNotFoundException (400), other account -> AccessDeniedException, table ARN -> ValidationException**
  DescribeExport: nonexistent well-formed ARN -> ExportNotFoundException 'Export not found: Export not found:
  arn:aws:dynamodb:us-west-2:<ACCOUNT>:table/ackq-nope/export/0'; other-account ARN -> AccessDeniedException
  'Access is denied'; other-region ARN -> ExportNotFoundException 'Export not found: Invalid export arn';
  table ARN -> ValidationException 'Invalid Export ARN'; import-shaped ARN -> ValidationException 'Invalid
  Export ARN'; 40 x 'x' -> ValidationException 'Invalid Export ARN'; tampered id suffix ->
  ExportNotFoundException 'Export not found: Export not found:
  arn:aws:dynamodb:us-west-2:<ACCOUNT>:table/ackq-86a5a7-exp-va'; a real ARN via the us-east-1 endpoint ->
  ExportNotFoundException 'Export not found: Invalid export arn'. ListExports(MaxResults=1) NextToken fed to
  ListImports -> ValidationException 'Invalid Request: Provided nextToken is invalid:
  c3eb5df0d20ff832857aa576389ea7f9e41a9974e868d43ef0994fd86c4cf2f928e1be7d'; ListImports token fed to
  ListExports -> ValidationException 'Invalid Request: Provided nextToken is invalid:
  1948bc710b851bf3c523be4abf6c042be19f7c3e41bb2dac1fa3662538b0c23c35ba2cf7'; ListExports token reused with a
  different TableArn -> accepted(None) (0 entries) and with MaxResults=5 -> accepted(None).
  - ACK: exceptions.404, terminal_codes, custom_find · ops: DescribeExport, ListExports, ListImports
  - repro: DescribeExport with fabricated ARNs; ListExports(MaxResults=1) -> NextToken ->
    ListImports(NextToken)
  - handling: not handled in the controller (as of commit 34b85e6)
  - related: [DDB-IMPORT-010](import.md#ddb-import-010), [DDB-IMPORT-011](import.md#ddb-import-011), [DDB-IMPORT-016](import.md#ddb-import-016), [DDB-BACKUP-014](backup.md#ddb-backup-014) · hypotheses: H-B-130, H-B-141 ·
    evidence: export/error-taxonomy/sync-validation

## Request validation

- <a id="ddb-export-003"></a>**DDB-EXPORT-003** `request-validation` · impact medium · unhandled (not handled in controller) · verified 2026-10-09
  **ExportType/IncrementalExportSpecification couplings are all ValidationException; ExportFromTime is required for INCREMENTAL_EXPORT**
  IncrementalExportSpecification with ExportType omitted -> ValidationException 'Invalid Request: Export Type
  expected to be Incremental Export when incremental export specification is provided'; with
  ExportType=FULL_EXPORT -> ValidationException 'Invalid Request: When Export Type is Full Export ,
  Incremental Export Specification should not be provided'; ExportTime together with INCREMENTAL_EXPORT ->
  InvalidExportTimeException 'Incremental export period from time cannot be less than the table creation time
  : 2026-10-08T22:51:50Z'; ExportViewType=NEW_IMAGES -> ValidationException '1 validation error detected:
  Value 'NEW_IMAGES' at 'incrementalExportSpecification.exportViewType' failed to satisfy constraint: Member
  must'; INCREMENTAL_EXPORT without a spec -> ValidationException 'Invalid Request: Incremental Export
  Specification expected when export type is set as Incremental Export'; empty spec -> ValidationException
  'Invalid Request: ExportFromTime must be provided in IncrementalExportSpecification'; spec with only
  ExportViewType -> ValidationException 'Invalid Request: ExportFromTime must be provided in
  IncrementalExportSpecification'; ExportFromTime omitted -> ValidationException 'Invalid Request:
  ExportFromTime must be provided in IncrementalExportSpecification'.
  - ACK: terminal_codes, docs-only · ops: ExportTableToPointInTime · fields: ExportType, ExportTime,
    IncrementalExportSpecification.ExportViewType, IncrementalExportSpecification.ExportFromTime
  - repro: ExportTableToPointInTime with each coupling variant on a PITR table
  - handling: not handled in the controller (as of commit 34b85e6)
  - related: [DDB-EXPORT-001](#ddb-export-001), [DDB-EXPORT-004](#ddb-export-004), [DDB-EXPORT-019](#ddb-export-019) · hypotheses: H-B-118, H-B-117 · evidence:
    export/error-taxonomy/sync-validation
  - notes: H-B-118 confirmed on all four couplings (all ValidationException with explicit messages);
    ExportFromTime is effectively required for INCREMENTAL_EXPORT ('ExportFromTime must be provided') although
    the model marks it optional (H-B-117). ExportTime + INCREMENTAL_EXPORT was not isolated: the window...
  - full notes: [details/DDB-EXPORT-003.md](details/DDB-EXPORT-003.md)

- <a id="ddb-export-004"></a>**DDB-EXPORT-004** `request-validation` · impact medium · unhandled (not handled in controller) · verified 2026-10-09
  **Incremental window violations (<15 min, >24 h, before table creation, inverted, future) are all InvalidExportTimeException**
  10-minute window -> InvalidExportTimeException 'Incremental export period from time cannot be less than the
  table creation time : 2026-10-09T00:41:50Z'; 25-hour window -> InvalidExportTimeException 'Incremental
  export period from time cannot be less than the table creation time : 2026-10-07T22:51:50Z'; window entirely
  before PITR enable -> InvalidExportTimeException 'Incremental export period from time cannot be less than
  the table creation time : 2026-10-08T22:51:50Z'; from before enable, to omitted ->
  InvalidExportTimeException 'Incremental export period from time cannot be less than the table creation time
  : 2026-10-08T22:51:50Z'; from=enable+1s to omitted (fresh table) -> InvalidExportTimeException 'Difference
  between export period from time and export period to time is less than 15 minutes'; from after to ->
  InvalidExportTimeException 'Incremental export period from time cannot be less than the table creation time
  : 2026-10-08T23:51:50Z'; to in the future -> InvalidExportTimeException 'Incremental export period to time
  cannot be greater than the current time : 2026-10-09T01:51:50Z'.
  - ACK: terminal_codes, requeue · ops: ExportTableToPointInTime · fields:
    IncrementalExportSpecification.ExportFromTime, IncrementalExportSpecification.ExportToTime
  - repro: INCREMENTAL_EXPORT with each window variant ~1 minute after enabling PITR
  - handling: not handled in the controller (as of commit 34b85e6)
  - related: [DDB-EXPORT-001](#ddb-export-001), [DDB-EXPORT-003](#ddb-export-003), [DDB-EXPORT-019](#ddb-export-019) · hypotheses: H-B-117 · evidence:
    export/error-taxonomy/sync-validation
  - notes: All window violations are InvalidExportTimeException (never ValidationException), refuting the
    mixed-codes part of H-B-117. Check precedence: 'from time cannot be less than the table creation time'
    fires before the 15-min/24-h length checks (the 10-min and 25-h cases hit it because the table was <1h...
  - full notes: [details/DDB-EXPORT-004.md](details/DDB-EXPORT-004.md)

- <a id="ddb-export-005"></a>**DDB-EXPORT-005** `request-validation` · impact medium · unhandled (not handled in controller) · verified 2026-10-09
  **S3SseKmsKeyId requires S3SseAlgorithm=KMS (else ValidationException); other-region key ARN accepted, then FAILED KmsNotFoundException**
  S3SseKmsKeyId with S3SseAlgorithm omitted -> ValidationException 'Invalid Request: KMS Key Id is not
  supported for this encryption type'; with AES256 -> ValidationException 'Invalid Request: KMS Key Id is not
  supported for this encryption type'; S3SseAlgorithm=KMS with S3SseKmsKeyId='not a key' ->
  ValidationException 'Invalid Request: 1 validation error detected: Value 'not a key' at 's3SseKmsKeyId'
  failed to satisfy constraint: Member must satisfy regular'; KMS with a key ARN in another region ->
  accepted(IN_PROGRESS). Accepted ones described: {"sse-kms-xregion": {"ExportStatus": "FAILED", "ItemCount":
  null, "FailureCode": "KmsNotFoundException", "FailureMessage": "Invalid arn us-east-1 (Service: Amazon S3;
  Status Code: 400; Error Code: KMS.NotFoundException; Request ID: BR5PBK17A1JCPENA; S3 Extended Request ID:
  rkq53+9QYspPjK1O8E6MCfV7CQBORZ4juPxxtjqNnQ5qsNqlZHcf8CSf4uOvNHbmO44Yw5lpB0c=; Proxy: null)",
  "S3SseKmsKeyId": "arn:aws:kms:us-east-1:<ACCOUNT>:key/3b1a7419-d170-49d4-9a64-59dee41e3eea",
  "S3SseAlgorithm": "KMS", "IncrementalExportSpecification": null, "ExportManifest": null}}.
  - ACK: terminal_codes, compare.is_ignored+delta_pre_compare · ops: ExportTableToPointInTime, DescribeExport
    · fields: S3SseAlgorithm, S3SseKmsKeyId
  - repro: ExportTableToPointInTime with S3SseKmsKeyId/S3SseAlgorithm combinations
  - handling: not handled in the controller (as of commit 34b85e6)
  - related: [DDB-EXPORT-010](#ddb-export-010), [DDB-EXPORT-011](#ddb-export-011), [DDB-EXPORT-012](#ddb-export-012), [DDB-IMPORT-012](import.md#ddb-import-012) · hypotheses: H-B-119 · evidence:
    export/error-taxonomy/sync-validation

- <a id="ddb-export-019"></a>**DDB-EXPORT-019** `request-validation` · impact medium · unhandled (not handled in controller) · verified 2026-10-09
  **INCREMENTAL_EXPORT on a freshly PITR-enabled table: early attempt -> InvalidExportTimeException; accepted after 967 s**
  At +~2 min after enabling PITR, INCREMENTAL_EXPORT(ExportFromTime=t_enable, ExportToTime omitted) ->
  InvalidExportTimeException 'Difference between export period from time and export period to time is less
  than 15 minutes'. Retries every 60s from +15 min with ExportFromTime=t_enable+30s: [(904,
  'InvalidExportTimeException', 'Difference between export period from time and export period to time is less
  than 15 minutes'), (967, 'ok', '')]. Accepted export description: {"ExportFromTime":
  "2026-10-09T00:40:44+00:00", "ExportToTime": "2026-10-09T00:56:21+00:00", "ExportViewType":
  "NEW_AND_OLD_IMAGES"}; final: {"ExportStatus": "COMPLETED", "ItemCount": 0, "BilledSizeBytes": 10000000,
  "FailureCode": null, "IncrementalExportSpecification": {"ExportFromTime": "2026-10-09T00:40:44+00:00",
  "ExportToTime": "2026-10-09T00:56:21+00:00", "ExportViewType": "NEW_AND_OLD_IMAGES"}, "ExportTime": null}.
  - ACK: requeue, terminal_codes · ops: ExportTableToPointInTime, DescribeExport · fields: ExportType,
    IncrementalExportSpecification.ExportFromTime, IncrementalExportSpecification.ExportToTime,
    IncrementalExportSpecification.ExportViewType
  - repro: Enable PITR -> INCREMENTAL_EXPORT at +2 min, then every 60s from +15 min (ExportToTime omitted)
  - handling: not handled in the controller (as of commit 34b85e6)
  - related: [DDB-EXPORT-001](#ddb-export-001), [DDB-EXPORT-003](#ddb-export-003), [DDB-EXPORT-004](#ddb-export-004) · hypotheses: H-B-117 · evidence:
    export/state-machine/lifecycle

## Update granularity and ordering

- <a id="ddb-export-001"></a>**DDB-EXPORT-001** `prerequisite` · impact high · unhandled (not handled in controller) · verified 2026-10-09
  **Export prerequisites are synchronous: PITR off, missing table, bare name and ExportTime out of window all fail at call time (HTTP 400)**
  Without PITR: FULL export -> PointInTimeRecoveryUnavailableException 'Point in time recovery is not enabled
  for table 'ackq-86a5a7-exp-val''; INCREMENTAL -> InvalidExportTimeException 'Incremental export period from
  time cannot be less than the table creation time : 2026-10-08T22:39:12Z'; nonexistent table ->
  TableNotFoundException 'Table not found: arn:aws:dynamodb:us-west-2:<ACCOUNT>:table/ackq-nope-86a5a7'; bare
  table name as TableArn -> ValidationException 'One or more parameter values were invalid: tableArn is not a
  valid ARN'; nonexistent bucket (PITR off) -> PointInTimeRecoveryUnavailableException 'Point in time recovery
  is not enabled for table 'ackq-86a5a7-exp-val''; ListExports(TableArn) afterwards had 0 entries. With PITR:
  ExportTime=now+1h -> InvalidExportTimeException 'Export Time is invalid'; ExportTime=enable-1h ->
  InvalidExportTimeException 'Export Time is invalid'; ExportTime=now-1s -> InvalidExportTimeException 'Point
  in Time Recovery is not available for specified export time'; ExportTime=now+5s ->
  InvalidExportTimeException 'Export Time is invalid'.
  - ACK: terminal_codes, requeue, references · ops: ExportTableToPointInTime, ListExports · fields: TableArn,
    ExportTime, S3Bucket
  - repro: ExportTableToPointInTime variants against a table without and with PITR
  - handling: not handled in the controller (as of commit 34b85e6)
  - related: [DDB-EXPORT-002](#ddb-export-002), [DDB-TABLE-447](service.md#ddb-table-447), [DDB-BACKUP-012](backup.md#ddb-backup-012), [DDB-TABLE-274](table-restore.md#ddb-table-274), [DDB-TABLE-273](table-restore.md#ddb-table-273), [DDB-TABLE-272](table-restore.md#ddb-table-272),
    [DDB-EXPORT-003](#ddb-export-003), [DDB-EXPORT-004](#ddb-export-004), [DDB-EXPORT-019](#ddb-export-019) · hypotheses: H-B-026, H-B-014 · evidence:
    export/error-taxonomy/sync-validation
  - notes: All HTTP statuses are 400 (see result.yaml). The PITR check runs before the S3 bucket check (bogus
    bucket with PITR off -> same PITR error).

## Field behavior (defaults, normalization, shapes, immutability)

- <a id="ddb-export-011"></a>**DDB-EXPORT-011** `requested-vs-effective` · impact high · unhandled (not handled in controller) · verified 2026-10-09
  **Async S3 failures: wrong S3BucketOwner -> FAILED FailureCode=S3AccessDenied; deny-policy bucket -> FAILED FailureCode=S3AccessDenied...**
  All of these returned 200/IN_PROGRESS synchronously (codes: owner=ok deny=ok xreg=ok kmsbad=ok). Terminal:
  wrong owner -> FAILED FailureCode=S3AccessDenied 'Access Denied (Service: Amazon S3; Status Code: 403; Error
  Code: AccessDenied; Request ID: MJJ9NQTZ0AJRGE5Q; S3 Extended Request ID: 4DuVjhS'; deny policy -> FAILED
  FailureCode=S3AccessDenied 'User: arn:aws:sts::<ACCOUNT>:assumed-role/<PRINCIPAL> is not authorized to
  perform: s3:PutObject on resource: "arn:aws:s3:::a'; bucket in us-east-1 -> COMPLETED after 705.9s
  (S3BucketOwner described=None); with explicit correct owner -> COMPLETED after 722.7s; KMS with nonexistent
  key ARN -> FAILED FailureCode=KmsNotFoundException 'Key
  'arn:aws:kms:us-west-2:<ACCOUNT>:key/deadbeef-dead-beef-dead-beefdeadbeef' does not exist (Service: Amazon
  S3; Status Code: 400; Erro'.
  - ACK: terminal_codes, synced.when, references · ops: ExportTableToPointInTime, DescribeExport · fields:
    S3BucketOwner, S3Bucket, S3SseKmsKeyId, ExportDescription.FailureCode, ExportDescription.FailureMessage
  - repro: Exports with wrong S3BucketOwner, a bucket whose policy denies s3:PutObject, a bucket in another
    region, and a bogus KMS key ARN; poll to terminal
  - measurements: owner_duration_s=null, deny_duration_s=null, xreg_duration_s=705.9,
    xreg-owner_duration_s=722.7, kmsbad_duration_s=null, kmsnok_duration_s=724.7, kmsali_duration_s=722.9,
    badbkt_duration_s=null, tok1_duration_s=723.6, nt-a_duration_s=725.5, delsrc_duration_s=null
  - handling: not handled in the controller (as of commit 34b85e6)
  - related: [DDB-EXPORT-010](#ddb-export-010), [DDB-EXPORT-005](#ddb-export-005), [DDB-EXPORT-012](#ddb-export-012), [DDB-IMPORT-012](import.md#ddb-import-012), [DDB-EXPORT-016](#ddb-export-016), [DDB-EXPORT-009](#ddb-export-009),
    [DDB-EXPORT-022](#ddb-export-022), [DDB-TABLE-454](table-subresources.md#ddb-table-454) · hypotheses: H-B-027, H-B-123, H-B-119 · evidence:
    export/idempotency/client-token

- <a id="ddb-export-012"></a>**DDB-EXPORT-012** `normalization` · impact medium · unhandled (not handled in controller) · verified 2026-10-09
  **S3SseKmsKeyId echo: 'alias/aws/s3' described as alias/aws/s3; KMS without key -> COMPLETED after 724.7s with S3SseKmsKeyId=None**
  S3SseAlgorithm=KMS + S3SseKmsKeyId='alias/aws/s3' -> sync ok, final COMPLETED after 722.9s, described
  S3SseKmsKeyId=alias/aws/s3. S3SseAlgorithm=KMS without a key -> sync ok, final COMPLETED after 724.7s,
  described S3SseKmsKeyId=None, S3SseAlgorithm=KMS.
  - ACK: compare.is_ignored+delta_pre_compare, late_initialize · ops: ExportTableToPointInTime, DescribeExport
    · fields: S3SseAlgorithm, S3SseKmsKeyId
  - repro: Export with S3SseAlgorithm=KMS and S3SseKmsKeyId=alias/aws/s3; export with KMS and no key ->
    DescribeExport
  - handling: not handled in the controller (as of commit 34b85e6)
  - related: [DDB-EXPORT-010](#ddb-export-010), [DDB-EXPORT-011](#ddb-export-011), [DDB-EXPORT-005](#ddb-export-005), [DDB-IMPORT-012](import.md#ddb-import-012), [DDB-EXPORT-016](#ddb-export-016), [DDB-EXPORT-009](#ddb-export-009),
    [DDB-EXPORT-022](#ddb-export-022), [DDB-TABLE-454](table-subresources.md#ddb-table-454) · hypotheses: H-B-119 · evidence: export/idempotency/client-token

- <a id="ddb-export-017"></a>**DDB-EXPORT-017** `server-default` · impact medium · unhandled (not handled in controller) · verified 2026-10-09
  **DescribeExport server defaults for a minimal request: ExportFormat=DYNAMODB_JSON ExportType=None S3SseAlgorithm=AES256 S3BucketOwner=abse...**
  Request sent only TableArn, S3Bucket, ClientToken. DescribeExport at terminal: {"ExportManifest":
  "AWSDynamoDB/01791506414432-3f2e5aa4/manifest-summary.json", "EndTime": "2026-10-09T00:42:47.138000+00:00",
  "ItemCount": 0, "BilledSizeBytes": 0, "FailureCode": null, "ExportTime": "2026-10-09T00:40:14.432000+00:00",
  "StartTime": "2026-10-09T00:40:14.432000+00:00", "S3Prefix": null, "S3BucketOwner": null, "S3SseAlgorithm":
  "AES256", "S3SseKmsKeyId": null, "ExportFormat": "DYNAMODB_JSON", "ExportType": null, "ClientToken":
  "5a7154a6-a9a5-4183-902e-8945b0ee5872", "IncrementalExportSpecification": null}. Create response vs final
  describe: create-only members [], final-only ['BilledSizeBytes', 'EndTime', 'ExportManifest', 'ItemCount'],
  changed ['ExportStatus']. ExportArn id prefix minus StartTime = 0 ms.
  - ACK: compare.is_ignored+delta_pre_compare, late_initialize, compare.nil_equals_zero_value · ops:
    ExportTableToPointInTime, DescribeExport · fields: ExportDescription.ExportFormat,
    ExportDescription.ExportType, ExportDescription.S3SseAlgorithm, ExportDescription.S3BucketOwner,
    ExportDescription.S3Prefix, ExportDescription.ExportTime
  - repro: ExportTableToPointInTime(TableArn, S3Bucket) -> DescribeExport at COMPLETED
  - handling: not handled in the controller (as of commit 34b85e6)
  - related: [DDB-IMPORT-002](import.md#ddb-import-002), [DDB-IMPORT-003](import.md#ddb-import-003), [DDB-IMPORT-004](import.md#ddb-import-004), [DDB-EXPORT-016](#ddb-export-016), [DDB-EXPORT-018](#ddb-export-018) · hypotheses:
    H-B-029, H-B-123, H-B-114 · evidence: export/state-machine/lifecycle
  - notes: Repeated DescribeExport reads identical after COMPLETED: True (H-B-038). ListExports order (ids):
    ['01791507381000-82fcb5b4', '01791506415010-a460d52e', '01791506414936-ee1c3e3b',
    '01791506414860-d4f1098e', '01791506414787-1cdc3612', '01791506414432-3f2e5aa4'].

## Response fidelity and consistency

- <a id="ddb-export-014"></a>**DDB-EXPORT-014** `eventual-consistency` · impact medium · handled · verified 2026-10-09
  **UpdateContinuousBackups(PITR on) right after CreateTable->ACTIVE succeeded on the first attempt (0 s window) on both tables**
  Enabling PITR immediately after the table turned ACTIVE: {"t1": {"attempts": 1, "window_s": 0.0, "codes":
  [null]}, "t2": {"attempts": 1, "window_s": 0.0, "codes": [null]}}. Message: 'None'. Retrying every 2s
  succeeded after the window.
  - ACK: requeue, post-create-nudge · ops: CreateTable, UpdateContinuousBackups
  - repro: CreateTable -> wait ACTIVE -> UpdateContinuousBackups(PITR on) at once, retry every 2s
  - measurements: window_s_t1=0.0, window_s_t2=0.0
  - handling: handled via `templates/hooks/table/sdk_create_post_set_output.go.tpl:1-4; bcd26e1`
  - related: [DDB-BACKUP-002](backup.md#ddb-backup-002), [DDB-TABLE-447](service.md#ddb-table-447), [DDB-IMPORT-001](import.md#ddb-import-001) · hypotheses: H-B-025 · evidence:
    export/idempotency/client-token
  - notes: Also seen in export/state-machine/lifecycle and export/error-taxonomy/sync-validation (first
    attempt rejected). Prerequisite for any Export CRD that enables PITR on its source.
  - full notes: [details/DDB-EXPORT-014.md](details/DDB-EXPORT-014.md)

- <a id="ddb-export-015"></a>**DDB-EXPORT-015** `eventual-consistency` · impact medium · unhandled (not handled in controller) · verified 2026-10-09
  **After enabling PITR the first ExportTableToPointInTime was accepted at +0.0s and RestoreTableToPointInTime(latest) at +0.0s**
  UpdateContinuousBackups(PITR on) returned PointInTimeRecoveryStatus=ENABLED with
  Earliest=2026-10-09T00:40:14+00:00 Latest=2026-10-09T00:40:14+00:00. Export attempts every 5s: first attempt
  -> accepted ''; first success at +0.0s. RestoreTableToPointInTime(UseLatestRestorableTime) first success at
  +0.0s. Window per attempt in result.yaml pitr_window.attempts.
  - ACK: requeue, terminal_codes · ops: UpdateContinuousBackups, DescribeContinuousBackups,
    ExportTableToPointInTime, RestoreTableToPointInTime
  - repro: CreateTable -> UpdateContinuousBackups(PITR on) -> every 5s ExportTableToPointInTime +
    RestoreTableToPointInTime(latest) until accepted
  - measurements: export_first_ok_s=0.0, restore_first_ok_s=0.0
  - handling: not handled in the controller (as of commit 34b85e6)
  - related: [DDB-TABLE-272](table-restore.md#ddb-table-272), [DDB-TABLE-100](table-restore.md#ddb-table-100), [DDB-TABLE-087](table-restore.md#ddb-table-087), [DDB-TABLE-274](table-restore.md#ddb-table-274), [DDB-TABLE-086](table-restore.md#ddb-table-086), [DDB-TABLE-218](table-restore.md#ddb-table-218) ·
    hypotheses: H-B-025 · evidence: export/state-machine/lifecycle

## Delete semantics

- <a id="ddb-export-013"></a>**DDB-EXPORT-013** `delete-semantics` · impact medium · unhandled (not handled in controller) · verified 2026-10-09
  **Deleting the source ~1 s after ExportTableToPointInTime is accepted: DeleteTable 200; export outcome unobserved (poll throttled)**
  ExportTableToPointInTime(T2) then DeleteTable(T2) ~1s later -> ok (TableStatus DELETING). Table gone after
  15.8s while the export was ERR:ThrottlingException. Export final: None, ItemCount=None, FailureMessage='';
  DescribeExport still returns TableArn/TableId: False.
  - ACK: pre-delete-cleanup, references, docs-only · ops: ExportTableToPointInTime, DeleteTable,
    DescribeExport
  - repro: Export table T2 -> DeleteTable T2 immediately -> poll DescribeExport to terminal
  - handling: not handled in the controller (as of commit 34b85e6)
  - related: [DDB-TABLE-454](table-subresources.md#ddb-table-454), [DDB-EXPORT-020](#ddb-export-020), [DDB-EXPORT-021](#ddb-export-021), [DDB-EXPORT-022](#ddb-export-022) · hypotheses: H-B-027 · evidence:
    export/idempotency/client-token
  - notes: Contradiction with [DDB-TABLE-454](table-subresources.md#ddb-table-454): 013 titles 'export ends None' for an export whose source was
    deleted ~1 s after acceptance, but its own behavior shows the poll loop was throttled
    (ERR:ThrottlingException) and no terminal status was ever read; 454 watched an export started +0.04 s into
    DELETING...
  - full notes: [details/DDB-EXPORT-013.md](details/DDB-EXPORT-013.md)

## Quotas and rate limits

- <a id="ddb-export-007"></a>**DDB-EXPORT-007** `quota-limit` · impact medium · unhandled (not handled in controller) · verified 2026-10-09
  **ExportTableToPointInTime is throttled (ThrottlingException 'Rate exceeded', HTTP 400) after ~20 rapid calls; rejected calls count too**
  During the first run ~20 ExportTableToPointInTime calls within ~2 seconds (all but one rejected with
  validation errors) led to the next 8 calls failing with ThrottlingException 'Rate exceeded' (HTTP 400); the
  same calls succeeded when re-sent paced at 1.5s. DescribeImport/DescribeExport bursts of 13-18 calls also
  produced ThrottlingException for the tail of the burst in import/error-taxonomy/failure-modes and
  export/idempotency/client-token.
  - ACK: requeue, terminal_codes · ops: ExportTableToPointInTime, DescribeImport, DescribeExport
  - repro: Send ~25 ExportTableToPointInTime calls back-to-back (invalid ones are fine)
  - handling: not handled in the controller (as of commit 34b85e6)
  - related: [DDB-BACKUP-020](backup.md#ddb-backup-020), [DDB-BACKUP-014](backup.md#ddb-backup-014), [DDB-BACKUP-019](backup.md#ddb-backup-019) · hypotheses: H-B-130 · evidence:
    export/error-taxonomy/sync-validation
  - notes: H-B-130 claimed throttled calls return LimitExceededException; the wire code observed is
    ThrottlingException (undeclared in the model for these operations).

## Scope

- <a id="ddb-export-021"></a>**DDB-EXPORT-021** `scope` · impact high · unhandled (not handled in controller) · verified 2026-10-09
  **Export is a write-once job: no Delete/Cancel API, record immutable after terminal, S3 objects outlive the record**
  **Scope verdict: implement**
  The API has no DeleteExport/CancelExport; DescribeExport of a COMPLETED export returned identical documents
  on 3 reads (True); records survive DeleteTable of the source. A token-less retry creates another billed
  export (see export/idempotency/client-token).
  - ACK: scope:defer, custom_delete, is_read_only · ops: ExportTableToPointInTime, DescribeExport, ListExports
  - repro: Static operation list + DescribeExport repeated reads
  - handling: not handled in the controller (as of commit 34b85e6)
  - related: [DDB-EXPORT-013](#ddb-export-013), [DDB-TABLE-454](table-subresources.md#ddb-table-454), [DDB-EXPORT-020](#ddb-export-020), [DDB-EXPORT-022](#ddb-export-022), [DDB-BACKUP-018](backup.md#ddb-backup-018), [DDB-IMPORT-005](import.md#ddb-import-005),
    [DDB-TABLE-278](table-restore.md#ddb-table-278) · hypotheses: H-B-038 · evidence: export/state-machine/lifecycle
  - notes: Scope verdict: implement as a Job-like standalone Export CRD (create-only, status mirrors
    DescribeExport, delete = drop finalizer only, deterministic ClientToken, never re-create after FAILED
    without a spec change). 'implement' because the create/read lifecycle is real; delete must be a no-op.

## Codegen notes

- <a id="ddb-export-008"></a>**DDB-EXPORT-008** `codegen-artifact` · impact medium · unhandled (not handled in controller) · verified 2026-10-09
  **Live API accepts FilterSpecification and returns it plus DestinationType=S3 in DescribeExport; both absent from the SDK model**
  botocore 1.42.97 has no FilterSpecification on ExportTableToPointInTimeInput. Injected via before-call:
  unknown member 'DefinitelyNotAMember' -> accepted(IN_PROGRESS); FilterSpecification={} ->
  ValidationException 'Invalid Request: Invalid FilterSpecification: The specification can not be empty';
  FilterSpecification={FilterExpression:'attribute_exists(pk)'} -> accepted(IN_PROGRESS). Raw DescribeExport
  body keys for the filtered export: ['ClientToken', 'DestinationType', 'ExportArn', 'ExportFormat',
  'ExportStatus', 'ExportTime', 'FilterSpecification', 'S3Bucket', 'S3Prefix', 'S3SseAlgorithm', 'StartTime',
  'TableArn', 'TableId'] (FilterSpecification={"FilterExpression": "attribute_exists(pk)"}); the SDK-parsed
  description keys: ['ClientToken', 'ExportArn', 'ExportFormat', 'ExportStatus', 'ExportTime', 'S3Bucket',
  'S3Prefix', 'S3SseAlgorithm', 'StartTime', 'TableArn', 'TableId']. Every live DescribeExport body also
  carries DestinationType='S3', another member missing from botocore 1.42.97; the SDK silently drops both, so
  a controller generated from this model can neither express a filter nor see that an adopted export was
  filtered.
  - ACK: runtime-gap, custom_field · ops: ExportTableToPointInTime, DescribeExport · fields:
    FilterSpecification, DestinationType
  - repro: Register a before-call hook that adds FilterSpecification to the JSON body; call
    ExportTableToPointInTime; DescribeExport and read the raw response body
  - handling: not handled in the controller (as of commit 34b85e6)
  - hypotheses: H-B-125 · evidence: export/error-taxonomy/sync-validation
  - notes: Model drift check: whether the live service already knows a member the pinned SDK model lacks.

## Handling gaps (bugs to file)

None recorded for this document's findings; see [service.md 'Handling gaps summary'](service.md#handling-gaps-summary) for the service-wide list.

## E2E timing

Values are seconds unless the key says otherwise; n = trials behind the numbers ('1 run' when the finding records none).

| finding | what | measurements | n |
| --- | --- | --- | --- |
| [DDB-EXPORT-011](#ddb-export-011) | Async S3 failures: wrong S3BucketOwner -> FAILED FailureCode=S3AccessDenied; deny-policy bucket -> FAILED FailureCode=S3AccessDenied... | owner_duration_s=null, deny_duration_s=null, xreg_duration_s=705.9, xreg-owner_duration_s=722.7, kmsbad_duration_s=null, kmsnok_duration_s=724.7, kmsali_duration_s=722.9, badbkt_duration_s=null, tok1_duration_s=723.6, nt-a_duration_s=725.5, delsrc_duration_s=null | 1 run |
| [DDB-EXPORT-016](#ddb-export-016) | FULL_EXPORT of an empty table: COMPLETED after ~152.7s (EndTime-StartTime); ExportStatus never PENDING... | e1_duration_s_from_timestamps=152.7, export_durations_s.e1=152.7, export_durations_s.foo=154.9, export_durations_s.foo-slash=152.6, export_durations_s.slash-foo=152.2, export_durations_s.ion=153.9, export_durations_s.inc=125.7, poll_interval_s=15 | 1 run |

## Open questions

- [DDB-EXPORT-023](#ddb-export-023) (unverified) - Doc claim C017 UNTESTABLE: export ClientToken 'valid for 8 h after the first
  request completed' - expiry needs an 8 h wait: VERDICT: UNTESTABLE - a 4.13 h-old token replayed with
  identical parameters returned TableNotFoundException 'Table not found:
  arn:aws:dynamodb:us-west-2:<ACCOUNT>:table/ackq-f7205c-rt-src': the source table had been d...
- [DDB-EXPORT-024](#ddb-export-024) (unverified) - Doc claim C018 UNTESTABLE: 'after 8 h the same client token is treated as a
  new request' - needs an 8 h wait; not observable here: VERDICT: UNTESTABLE - a 4.13 h-old token replayed
  with identical parameters returned TableNotFoundException 'Table not found:
  arn:aws:dynamodb:us-west-2:<ACCOUNT>:table/ackq-f7205c-rt-src': the source table had been d...
- [DDB-EXPORT-025](#ddb-export-025) (unverified) - Doc claim C019 UNTESTABLE: 'do not resubmit the same token for more than 8 h' -
  window expiry not observable inside a probe budget: VERDICT: UNTESTABLE - a 4.13 h-old token replayed with
  identical parameters returned TableNotFoundException 'Table not found:
  arn:aws:dynamodb:us-west-2:<ACCOUNT>:table/ackq-f7205c-rt-src': the source table had been d...

<!-- preserved:start id=open-questions -->
<!-- open questions and follow-up experiments; survives re-renders -->
<!-- preserved:end -->

## Appendix: low-impact and duplicate findings

| id | category | impact | status | title | related | duplicate_of |
| --- | --- | --- | --- | --- | --- | --- |
| <a id="ddb-export-018"></a>**DDB-EXPORT-018** | normalization | low | confirmed | S3Prefix is stored verbatim and joined with '/': 'foo/' -> foo/AWSDynamoDB/01791506414860-d4f1098e/...... | [DDB-IMPORT-002](import.md#ddb-import-002), [DDB-IMPORT-003](import.md#ddb-import-003), [DDB-IMPORT-004](import.md#ddb-import-004), [DDB-EXPORT-016](#ddb-export-016), [DDB-EXPORT-017](#ddb-export-017) | - |
| <a id="ddb-export-022"></a>**DDB-EXPORT-022** | response-fidelity | low | confirmed | Export of a 3-item table: ItemCount=3 BilledSizeBytes=0, manifest-summary.json layout; records persist after the source table is deleted | [DDB-EXPORT-013](#ddb-export-013), [DDB-TABLE-454](table-subresources.md#ddb-table-454), [DDB-EXPORT-020](#ddb-export-020), [DDB-EXPORT-021](#ddb-export-021), [DDB-EXPORT-016](#ddb-export-016), [DDB-EXPORT-009](#ddb-export-009), [DDB-EXPORT-011](#ddb-export-011), [DDB-EXPORT-012](#ddb-export-012), [DDB-IMPORT-008](import.md#ddb-import-008) | - |
| <a id="ddb-export-023"></a>**DDB-EXPORT-023** | other | low | unverified | Doc claim C017 UNTESTABLE: export ClientToken 'valid for 8 h after the first request completed' - expiry needs an 8 h wait | [DDB-EXPORT-009](#ddb-export-009), [DDB-EXPORT-010](#ddb-export-010) | - |
| <a id="ddb-export-024"></a>**DDB-EXPORT-024** | other | low | unverified | Doc claim C018 UNTESTABLE: 'after 8 h the same client token is treated as a new request' - needs an 8 h wait; not observable here | [DDB-EXPORT-009](#ddb-export-009), [DDB-EXPORT-010](#ddb-export-010) | - |
| <a id="ddb-export-025"></a>**DDB-EXPORT-025** | other | low | unverified | Doc claim C019 UNTESTABLE: 'do not resubmit the same token for more than 8 h' - window expiry not observable inside a probe budget | [DDB-EXPORT-009](#ddb-export-009), [DDB-EXPORT-010](#ddb-export-010) | - |
| <a id="ddb-export-026"></a>**DDB-EXPORT-026** | other | low | confirmed | Doc claim C020 TRUE: same ClientToken with changed parameters -> ExportConflictException; identical replay -> 200 with the same ExportArn | [DDB-EXPORT-009](#ddb-export-009), [DDB-EXPORT-010](#ddb-export-010) | - |

## Supplementary notes

<!-- preserved:start -->
<!-- preserved:end -->
