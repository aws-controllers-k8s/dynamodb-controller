<!-- generated: ack-api-quirks dynamodb 2026-10-09 model=2012-08-10 controller=34b85e6 -->
# DynamoDB: service facts and cross-cutting behaviors
_Model facts, resource inventory and scope verdicts, findings tagged `service-wide`, tag API, codegen notes, out-of-scope resources and the handling-gaps summary across all documents._
Generated from ack-api-quirks `services/dynamodb` (render date in the marker above); model 2012-08-10 (service/dynamodb v1.39.8); controller commit 34b85e6; evidence: `services/dynamodb/probes/<probe id>/` in the lab repo.

## Overview

<!-- preserved:start id=overview -->
DynamoDB's control plane is several loosely coupled backends behind one endpoint - the table backend, a tagging backend, the continuous-backups/backup family, TTL, Contributor Insights, Kinesis, resource-policy and an account-wide limiter - each with its own state visibility, consistency window and error vocabulary. The facts below hold for every resource and operation this controller touches; resource-specific behavior lives in the per-resource documents (table.md, table-throughput-billing.md, table-streams-encryption-class.md, table-indexes.md, table-subresources.md, table-replicas.md, table-global-tables.md, table-restore.md, backup.md, import.md, export.md) and is cited here with its document name.

### What every agent working on this controller must know
- UpdateTable is one logical change per call: DeletionProtection, SSE, TableClass, WarmThroughput, ReplicaUpdates and a GSI Create/Delete each 'must be the only operation in the request' (synchronous ValidationException, nothing applied, rejected calls atomic); only BillingMode-centred combinations pass (BillingMode + ProvisionedThroughput + GSI throughput Updates, BillingMode=PAY_PER_REQUEST + StreamSpecification or + OnDemandThroughput). 33 of 35 pairs were rejected on idle tables and only 2 of 33 combinations accepted on fresh ones - plan one mutation per reconcile and re-read before the next ([DDB-TABLE-382](table-streams-encryption-class.md#ddb-table-382), [DDB-TABLE-433](table-streams-encryption-class.md#ddb-table-433), table-streams-encryption-class.md; [DDB-TABLE-224](table-replicas.md#ddb-table-224), table-replicas.md).
- TableStatus=ACTIVE is not quiescent: SSE changes, WarmThroughput increases, GSI backfill (~16 min) and the ~1.6-1.8 s tag write lock all run with TableStatus ACTIVE and still make DeleteTable fail with ResourceInUseException. Gate on SSEDescription.Status, WarmThroughput.Status, IndexStatus and the error code, never on TableStatus alone ([DDB-TABLE-069](table-streams-encryption-class.md#ddb-table-069), table-streams-encryption-class.md; [DDB-TABLE-150](table-indexes.md#ddb-table-150), table-indexes.md; [DDB-TABLE-172](#ddb-table-172)).
- Every error is HTTP 400 (no operation returns 404) except a small deterministic HTTP 500 class, and every code is shared between transient and permanent conditions: ResourceInUseException is wait-for-state except 'Table already exists'; LimitExceededException is a tag lock, a state gate, a 1 h / 24 h / 30 d budget or an account quota; ThrottlingException is a rate limit or a 15 s per-table cooldown; ResourceNotFound/TableNotFound mean gone, still CREATING, or the wrong API family. ValidationException carries ~360 texts, 'almost all PERMANENT-SPEC'; the catalogue body names six transient families (TTL cooldown, DescribeTimeToLive while CREATING/DELETING, Insights and Kinesis state gates, replica SSE UPDATING, replica being added), more than the four its title counts. Classify by stable message prefix - texts are identical across regions once names and timestamps are removed - never by code alone ([DDB-TABLE-070](#ddb-table-070), [DDB-TABLE-442](#ddb-table-442), [DDB-TABLE-449](#ddb-table-449)).
- HTTP 500s are reproducible request-shape bugs, not transient faults: InternalFailure with an empty message for UpdateTable WarmThroughput={} / OnDemandThroughput={} or a WarmThroughput update on a ghost index, InternalServerError for HMAC/RSA KMS keys; the same {} members are silently dropped (200, no-op) when combined with any other member. Retrying is futile - surface them as terminal ([DDB-TABLE-448](#ddb-table-448), [DDB-TABLE-456](#ddb-table-456), [DDB-TABLE-457](#ddb-table-457)).
- Identity and budgets are keyed on the incarnation: TableArn is deterministic (arn:aws:dynamodb:<region>:<acct>:table/<name>) while TableId is a new UUID per create - the only way to tell a re-created table apart and what backups and per-table budgets hang on; every per-table cooldown (DeletionProtection and resource policy 15 s, TTL ~30 min, SSE 4 per 24 h, TableClass 2 per 30 days, provisioned decreases 4 per UTC day then one per hour) is invisible in DescribeTable and resets on a same-name re-create. Persist a per-field 'last changed' or parse the server's 'Please try again after <ts>', which is authoritative to within ~0.2 s ([DDB-TABLE-013](#ddb-table-013), [DDB-TABLE-377](#ddb-table-377), [DDB-TABLE-464](#ddb-table-464)).
- Not-found is not one signal: DescribeTable never 404s after CreateTable (0/10 creates, first 200 within 7-83 ms), but for most of the CREATING window the sub-resource APIs answer ResourceNotFoundException (TTL, Insights, tags, policy, Kinesis) or TableNotFoundException (continuous backups, backup); while DELETING each backend forgets the table at its own time before DescribeTable 404s, and TTL/Insights/Kinesis writes still return 200 in the first second. Treat a sub-resource not-found as 'wait for ACTIVE' whenever DescribeTable still succeeds ([DDB-TABLE-075](#ddb-table-075), [DDB-TABLE-115](#ddb-table-115), [DDB-TABLE-455](#ddb-table-455)).
- Documented per-second limits are largely not enforced (ListTagsOfResource 10/s: 40 sequential calls at ~159/s all 200), but two real limiters exist: an account-wide control-plane mutation limiter (effective UpdateTable/TagResource/DeleteTable: 3-5 of 10 concurrent calls throttled; reads, no-op UpdateTable, DeletionProtection and TTL writes exempt) and per-API, per-HTTP-connection 'Rate exceeded' buckets on Describe* (~4 back-to-back DescribeTimeToLive calls on one connection, refill ~2 s; fresh connections never). Share one client with retries and spread effective mutations across reconciles ([DDB-TABLE-384](#ddb-table-384), [DDB-TABLE-434](#ddb-table-434), [DDB-TABLE-403](#ddb-table-403)).
- Tag API: TagResource upserts, UntagResource of an absent key is a 200 no-op and 'aws:' keys are rejected; all tag validation (key/value length, empty or duplicate keys, Tags=[]) and the 50-tag limit are synchronous all-or-nothing ValidationException checked on the post-merge set, never LimitExceededException ([DDB-TABLE-105](#ddb-table-105), [DDB-TABLE-107](#ddb-table-107), [DDB-TABLE-108](#ddb-table-108)).
- Tag write lock: every effective tag write holds a ~1.6-1.8 s per-table lock; the next effective write is LimitExceededException 'Table tags are being updated' (15/15; SDK standard retries still lose 4/5) and DeleteTable is ResourceInUseException meanwhile, while an identical replay or a superset TagResource passes. The controller's tag sync (pkg/resource/table/hooks_tags.go:27-73) issues UntagResource for removed keys and then TagResource for added keys in one reconcile, so the second call usually fails - send ONE TagResource with the full desired set and requeue the removals ([DDB-TABLE-103](#ddb-table-103), [DDB-TABLE-173](#ddb-table-173); [DDB-TABLE-440](table.md#ddb-table-440), table.md).
- Eventual-consistency windows are 1-3 s and never zero: ListTagsOfResource serves the previous set for p50 1.9 s (max 3.0 s) after TagResource/UntagResource; GetResourcePolicy right after a Put returns PolicyNotFoundException or the OLD document and RevisionId (0/30 immediate reads saw the write); CreateBackup is ContinuousBackupsUnavailableException for ~3-6 s after ACTIVE. Never confirm a write with a read in the same reconcile ([DDB-TABLE-102](#ddb-table-102); [DDB-TABLE-248](table-policy-kinesis-autoscaling.md#ddb-table-248), table-subresources.md; [DDB-BACKUP-002](backup.md#ddb-backup-002), backup.md).
- Mutation responses are not the new state: UpdateTable echoes the OLD ProvisionedThroughput alongside TableStatus=UPDATING (TableClassSummary and the KMS key behave the same, see table-streams-encryption-class.md), no-op re-sends return UPDATING while DescribeTable is already ACTIVE, and UpdateTableReplicaAutoScaling echoes pre-update settings. Always re-Describe; never diff against the mutation response ([DDB-TABLE-060](table-throughput-billing.md#ddb-table-060), table-throughput-billing.md; [DDB-TABLE-177](table-streams-encryption-class.md#ddb-table-177), table-streams-encryption-class.md; [DDB-TABLE-238](table-replicas.md#ddb-table-238), table-replicas.md).
- Durations vary by one to two orders of magnitude, so never use fixed sleeps: CREATING is ~2-6 s for a plain table but ~16 s with GSIs; a GSI added via UpdateTable holds TableStatus UPDATING only 25-55 s and then backfills 7-16 min with the table ACTIVE; INACCESSIBLE_ENCRYPTION_CREDENTIALS appears 13-75 min after a KMS key is disabled. Poll the specific status field with backoff ([DDB-TABLE-387](table.md#ddb-table-387), table.md; [DDB-TABLE-148](table-indexes.md#ddb-table-148), table-indexes.md; [DDB-TABLE-331](table-streams-encryption-class.md#ddb-table-331), table-streams-encryption-class.md).

### Reading the error catalogues
The per-code catalogues (one entry per error code, [DDB-TABLE-442](#ddb-table-442) to [DDB-TABLE-447](#ddb-table-447)) classify every observed message text into four handling classes: TRANSIENT-RETRY (clears by itself; requeue with the stated wait), TRANSIENT-WAIT-FOR-STATE (clears when the named status changes; requeue after re-Describe), PERMANENT-SPEC (the user must change the spec or fix an external dependency; set a terminal condition) and PERMANENT-QUOTA (needs a quota increase or the stated 1 h / 24 h / 30 d window; terminal with the window in the message). Request-specific tokens (<TBL>, <ARN>, <TS>, <N>, <REGION>, <IDX>) mark the parts of a text that change per call; match on the stable prefix ([DDB-TABLE-449](#ddb-table-449)).

Entries below are generated from the lab findings; low-impact items are in the appendix, long notes under details/.
<!-- preserved:end -->

## Service facts

- API version: 2012-08-10 · SDK module: service/dynamodb v1.39.8 · model fingerprint: 7b77f18f72bf3a86
- controller: aws-controllers-k8s/dynamodb-controller @ 34b85e6
- regions probed: us-west-2, us-east-1
- probes: 128 · findings: 548 (515 canonical, 33 duplicates) · service-wide findings: 46

## Resource inventory

| noun | ack_inferred | create | read | update | delete | list | sub_resource_ops | scope verdict |
| --- | --- | --- | --- | --- | --- | --- | --- | --- |
| Backup | yes | CreateBackup | DescribeBackup | - | DeleteBackup | ListBackups | - | implement, implemented |
| ContinuousBackup | no | - | DescribeContinuousBackups | UpdateContinuousBackups | - | - | - | - |
| ContributorInsight | no | - | DescribeContributorInsights | UpdateContributorInsights | - | ListContributorInsights | - | - |
| Export | no | - | DescribeExport | - | - | ListExports | - | implement |
| GlobalTable | yes | CreateGlobalTable | DescribeGlobalTable | UpdateGlobalTable | - | ListGlobalTables | DescribeGlobalTableSettings, UpdateGlobalTableSettings | skip:deprecated, implemented |
| GlobalTableSetting | no | - | DescribeGlobalTableSettings | UpdateGlobalTableSettings | - | - | - | skip:deprecated |
| Import | no | - | DescribeImport | - | - | ListImports | - | field-on-parent |
| Item | no | - | GetItem | UpdateItem | DeleteItem | - | - | - |
| KinesisStreamingDestination | no | - | DescribeKinesisStreamingDestination | DisableKinesisStreamingDestination, EnableKinesisStreamingDestination, UpdateKinesisStreamingDestination | - | - | - | - |
| ResourcePolicy | no | - | GetResourcePolicy | - | DeleteResourcePolicy | - | - | - |
| Table | yes | CreateTable | DescribeTable | UpdateTable | DeleteTable | ListTables | DescribeTableReplicaAutoScaling, UpdateTableReplicaAutoScaling | field-on-parent, implement, implemented |
| TableReplicaAutoScaling | no | - | DescribeTableReplicaAutoScaling | UpdateTableReplicaAutoScaling | - | - | - | skip:no-crud |
| TimeToLive | no | - | DescribeTimeToLive | UpdateTimeToLive | - | - | - | - |

## Cross-cutting behaviors

Findings tagged `service-wide` (resource-specific findings live in the per-resource documents listed in
README.md).

### State machine

- <a id="ddb-table-101"></a>**DDB-TABLE-101** `async-state-machine` · impact high · handled · verified 2026-10-08
  **Tag APIs during CREATING: ResourceNotFoundException, then ResourceInUseException just before ACTIVE; tags unreadable until ACTIVE**
  CreateTable returns TableDescription.TableArn immediately, but TagResource, UntagResource and
  ListTagsOfResource with that ARN return ResourceNotFoundException (HTTP 400, "Requested resource not found:
  ResourceArn: arn:...:table/<name> not found") for ~5-6 s of the ~7 s CREATING window (8/9 attempts at 0.7 s
  spacing). In the last ~1 s before ACTIVE the table becomes known to the tagging backend: TagResource ->
  ResourceInUseException "Attempt to change a resource which is still in use: Table is being created: <name>",
  UntagResource -> 200, ListTagsOfResource -> 200 with an empty set. Tags supplied in CreateTable are likewise
  unreadable (ListTagsOfResource 404 x8) until the table is ACTIVE, at which point they appear together with
  the ACTIVE status (no further lag, 2 runs). TagResource issued the instant DescribeTable first reports
  ACTIVE succeeds (200). The CreateTable response does not echo Tags.
  - ACK: tags.custom-sync, synced.when, exceptions.404 · ops: CreateTable, TagResource, UntagResource,
    ListTagsOfResource · fields: Tags, ResourceArn
  - repro: CreateTable (with or without Tags); immediately and every 0.7 s call
    TagResource/UntagResource/ListTagsOfResource with TableDescription.TableArn until DescribeTable reports
    ACTIVE
  - measurements: creating_window_s=6.769, tag_api_404_until_s=5.3, create_tags_visible_s=4.448,
    active_at_s=4.448
  - handling: handled via `pkg/resource/table/hooks.go:182-186; pkg/resource/table/hooks_resource_policy.go:60-63; pkg/resource/table/hooks_tags.go:138-168; pkg/resource/table/hooks.go:549-553; test/e2e/tests/test_table.py:277-349; e953ae5; templates/hooks/table/sdk_create_post_set_output.go.tpl:1-4; bcd26e1`
  - related: [DDB-TABLE-006](table.md#ddb-table-006), [DDB-TABLE-063](table-throughput-billing.md#ddb-table-063), [DDB-TABLE-103](#ddb-table-103), [DDB-TABLE-381](table-throughput-billing.md#ddb-table-381), [DDB-TABLE-005](table.md#ddb-table-005), [DDB-TABLE-104](table.md#ddb-table-104),
    [DDB-TABLE-110](table.md#ddb-table-110), [DDB-TABLE-071](#ddb-table-071) · evidence: table/tags/state-gating-and-lag
  - notes: Refutes the ResourceInUseException-only part of H-T-061 and the "visible as soon as DescribeTable
    succeeds" part of H-T-051. A controller must not treat ResourceNotFoundException from the tag APIs as
    "table gone" while the table is CREATING, and must defer tag reconciliation (including read-back of...
  - full notes: [details/DDB-TABLE-101.md](details/DDB-TABLE-101.md)

- <a id="ddb-table-115"></a>**DDB-TABLE-115** `async-state-machine` · impact high · handled · verified 2026-10-08
  **CREATING: TTL/Insights updates -> ResourceNotFound then ResourceInUse/Validation; PITR/backup -> TableNotFound then CBUnavailableException**
  Plain table CREATING (6.5s): UpdateTimeToLive [('ResourceNotFoundException', 400, 'Requested resource not
  found: Table: ackq-34ad39-s1 not found'), ('ResourceInUseException', 400, 'Attempt to change a resource
  which is still in use: Table ackq-34ad39-s1 is being created')]; DescribeTimeToLive [('ValidationException',
  400, 'Cannot describe time to live while table is in CREATING state: Current table state is CREATING')];
  UpdateContinuousBackups [('TableNotFoundException', 400, 'Table not found: ackq-34ad39-s1'),
  ('ContinuousBackupsUnavailableException', 400, 'Backups are being enabled for the table: ackq-34ad39-s1.
  Please retry later')]; DescribeContinuousBackups [('TableNotFoundException', 400, 'Table not found:
  ackq-34ad39-s1'), ('OK', 200, '')]; UpdateContributorInsights [('ResourceNotFoundException', 400, 'Requested
  resource not found: Table: ackq-34ad39-s1 not found'), ('ValidationException', 400, 'Table or Index is not
  in a valid state to update Key Access Insights: TableStatus must be ACTIVE to enable ContributorIn')];
  DescribeContributorInsights [('ResourceNotFoundException', 400, 'Requested resource not found: Table:
  ackq-34ad39-s1 not found'), ('OK', 200, '')]; ListContributorInsights [('ResourceNotFoundException', 400,
  'Requested resource not found: Table: ackq-34ad39-s1 not found'), ('OK', 200, '')]; CreateBackup
  [('TableNotFoundException', 400, 'Table not found: ackq-34ad39-s1'),
  ('ContinuousBackupsUnavailableException', 400, 'Backups are being enabled for the table: ackq-34ad39-s1.
  Please retry later')]. First success after first ACTIVE (s): {'DescribeContinuousBackups': None,
  'DescribeContributorInsights': None, 'ListContributorInsights': None, 'DescribeTimeToLive': 0.0,
  'UpdateTimeToLive': 0.0, 'UpdateContributorInsights': 0.1, 'UpdateContinuousBackups': 2.6, 'CreateBackup':
  2.7}. Describe values while CREATING: ttl=None cb={'ContinuousBackupsStatus': 'DISABLED',
  'PointInTimeRecoveryDescription': {'PointInTimeRecoveryStatus': 'DISABLED'}}
  ci={'CREATING/DescribeContributorInsights': {'status': 'DISABLED', 'keys': ['ContributorInsightsStatus',
  'TableName']}}.
  - ACK: synced.when, requeue, terminal_codes, exceptions.404 · ops: UpdateTimeToLive, DescribeTimeToLive,
    UpdateContinuousBackups, DescribeContinuousBackups, UpdateContributorInsights,
    DescribeContributorInsights, CreateBackup
  - repro: CreateTable then call each op every 1.2s until success
  - measurements: create_to_active_s=6.5, first_ok_after_active_s.DescribeContinuousBackups=null,
    first_ok_after_active_s.DescribeContributorInsights=null,
    first_ok_after_active_s.ListContributorInsights=null, first_ok_after_active_s.DescribeTimeToLive=0.0,
    first_ok_after_active_s.UpdateTimeToLive=0.0, first_ok_after_active_s.UpdateContributorInsights=0.1,
    first_ok_after_active_s.UpdateContinuousBackups=2.6, first_ok_after_active_s.CreateBackup=2.7
  - handling: handled via `generator.yaml:104-109; pkg/resource/table/hooks.go:72-93; test/e2e/table.py:47-73; test/e2e/tests/test_table.py:351-386; generator.yaml:46-50; pkg/resource/table/hooks_continuous_backup.go:27-94; generator.yaml:78-83; pkg/resource/table/hooks.go:882-960; generator.yaml:84-87; pkg/resource/table/sdk.go:83-86; templates/hooks/table/sdk_create_post_set_output.go.tpl:1-4; bcd26e1`
  - related: [DDB-TABLE-114](#ddb-table-114), [DDB-TABLE-111](table-subresources.md#ddb-table-111), [DDB-TABLE-233](table-policy-kinesis-autoscaling.md#ddb-table-233), [DDB-TABLE-234](table-policy-kinesis-autoscaling.md#ddb-table-234), [DDB-TABLE-083](table-subresources.md#ddb-table-083), [DDB-TABLE-084](table-subresources.md#ddb-table-084),
    [DDB-TABLE-085](table-subresources.md#ddb-table-085) · evidence: table/state-machine/subresource-admissibility
  - notes: Hypotheses: H-S-010, H-S-015, H-S-103, H-S-004. Hypotheses: H-S-010 (partially: first
    ResourceNotFoundException 'Table: X not found' - identical text to a deleted table - then, ~2.6 s into
    CREATING, ResourceInUseException 'Attempt to change a resource which is still in use: Table X is being...
  - full notes: [details/DDB-TABLE-115.md](details/DDB-TABLE-115.md)

### Identity and lookup

- <a id="ddb-table-011"></a>**DDB-TABLE-011** `identity` · impact medium · unhandled (not handled in controller) · verified 2026-10-08
  **TableName accepts the table ARN on Describe/Update/Delete (OK/OK/OK); response TableName stays the bare name**
  Passing the full table ARN as TableName: {'DescribeTable(TableName=ARN)': 'OK', 'DescribeTable(TableName=ARN
  other region)': 'ValidationException', 'DescribeTable(TableName=ARN other account)':
  'AccessDeniedException', 'DescribeTable(TableName=index ARN)': 'ValidationException',
  'DescribeTable(TableName=stream ARN)': 'ValidationException', 'DescribeTable(TableName=TableId)':
  'ResourceNotFoundException', 'DescribeTable(TableName=ARN, via us-east-1 endpoint)': 'ValidationException',
  'DescribeTimeToLive(TableName=ARN)': 'OK', 'DescribeContinuousBackups(TableName=ARN)': 'OK',
  'DescribeContributorInsights(TableName=ARN)': 'OK', 'DescribeKinesisStreamingDestination(TableName=ARN)':
  'OK', 'ListTagsOfResource(ResourceArn=bare name)': 'ValidationException', 'TagResource(ResourceArn=bare
  name)': 'ValidationException', 'ListTagsOfResource(ResourceArn=ARN)': 'OK',
  'GetResourcePolicy(ResourceArn=bare name)': 'ValidationException', 'UpdateTable(TableName=ARN, DP=true)':
  'OK', 'PutItem(TableName=ARN)': 'OK', 'DeleteTable(TableName=ARN)': 'OK'}. DescribeTable response
  TableName=ackq-8034da-ida (bare name). Wrong-region ARN -> ValidationException '1 validation error detected:
  Invalid AWS region in 'arn:aws:dynamodb:us-east-1:<ACCOUNT>:table/ac'; wrong-account ARN ->
  AccessDeniedException 'Access is denied'; index ARN -> ValidationException; TableId as name ->
  ResourceNotFoundException. Reverse direction (bare name as ResourceArn): ListTagsOfResource ->
  ValidationException 'One or more parameter values were invalid: ARNs must start with 'arn:':
  ackq-8034da-ida'.
  - ACK: is_arn_primary_key, custom_find, compare.is_ignored+delta_pre_compare · ops: DescribeTable,
    UpdateTable, DeleteTable, ListTagsOfResource · fields: TableName, TableArn
  - repro: DescribeTable(TableName=arn:aws:dynamodb:<region>:<acct>:table/<name>)
  - handling: not handled in the controller (as of commit 34b85e6)
  - related: [DDB-TABLE-432](table-policy-kinesis-autoscaling.md#ddb-table-432) · evidence: table/identity/arn-as-name-list-pagination
  - notes: H-T-056 (contrarian) confirmed: a spec.tableName holding an ARN would 'work' against AWS while
    Describe returns the bare name -> perpetual diff.

- <a id="ddb-table-071"></a>**DDB-TABLE-071** `identity` · impact medium · handled · verified 2026-10-08
  **ListTagsOfResource/TagResource identifier shapes: malformed ARN, bare name, foreign account, other region, stream ARN**
  ListTagsOfResource ResourceArn='not-an-arn' -> ('ValidationException', "One or more parameter values were
  invalid: ARNs must start with 'arn:': not-an-arn"); bare table NAME of an existing table ->
  ('ValidationException', "One or more parameter values were invalid: ARNs must start with 'arn:':
  ackq-a10352-et"); TagResource bare name -> ValidationException; foreign-account ARN ->
  ('AccessDeniedException', 'Access is denied'); other-region ARN -> ValidationException; stream ARN ->
  ('ResourceNotFoundException', 'Requested resource not found: ResourceArn:
  arn:aws:dynamodb:us-west-2:<ACCOUNT>:table/ackq-a10352'); uppercase ARN -> ResourceNotFoundException.
  - ACK: is_arn_primary_key, terminal_codes · ops: ListTagsOfResource, TagResource · fields: ResourceArn
  - repro: ListTagsOfResource with each identifier form against an ACTIVE table
  - handling: handled via `pkg/resource/table/hooks.go:182-186; pkg/resource/table/hooks_resource_policy.go:60-63; pkg/resource/table/hooks_tags.go:138-168; pkg/resource/table/hooks.go:549-553`
  - related: [DDB-TABLE-101](#ddb-table-101), [DDB-TABLE-104](table.md#ddb-table-104), [DDB-TABLE-050](table-streams-encryption-class.md#ddb-table-050), [DDB-TABLE-362](table-streams-encryption-class.md#ddb-table-362), [DDB-TABLE-367](table-streams-encryption-class.md#ddb-table-367), [DDB-TABLE-378](table-streams-encryption-class.md#ddb-table-378),
    [DDB-TABLE-180](table-streams-encryption-class.md#ddb-table-180), [DDB-TABLE-004](table-streams-encryption-class.md#ddb-table-004), [DDB-TABLE-373](#ddb-table-373), [DDB-TABLE-013](#ddb-table-013), [DDB-TABLE-213](table-streams-encryption-class.md#ddb-table-213) · evidence:
    table/error-taxonomy/not-found-codes

### Errors

- <a id="ddb-table-070"></a>**DDB-TABLE-070** `error-code` · impact high · unhandled (not handled in controller) · verified 2026-10-08
  **Not-found is HTTP 400 ResourceNotFoundException for table ops; sub-resource ops use other codes**
  For a table name that does not exist, DescribeTable, UpdateTable (DeletionProtectionEnabled or
  ProvisionedThroughput), DeleteTable, DescribeTimeToLive, UpdateTimeToLive, DescribeContributorInsights,
  DescribeKinesisStreamingDestination and GetResourcePolicy return ResourceNotFoundException with HTTP 400 and
  message 'Requested resource not found: Table: <name> not found';
  TagResource/UntagResource/ListTagsOfResource with the corresponding ARN return ResourceNotFoundException
  'Requested resource not found: ResourceArn: <arn> not found'. No operation returned HTTP 404. UpdateTable
  with an invalid shape (WriteCapacityUnits=0) on a missing table returns ValidationException, i.e. request
  validation runs before the existence check. DescribeContinuousBackups and CreateBackup return
  TableNotFoundException 'Table not found: <name>' instead; DescribeTableReplicaAutoScaling returns
  ResourceNotFoundException with the message "Global table with name: '<name>' does not exist."
  - ACK: exceptions.404, terminal_codes · ops: DescribeTable, UpdateTable, DeleteTable, TagResource,
    UntagResource, ListTagsOfResource, DescribeContinuousBackups, DescribeTimeToLive,
    DescribeContributorInsights
  - repro: Each op with TableName/ARN of a table that never existed
  - handling: not handled in the controller (as of commit 34b85e6)
  - related: [DDB-TABLE-074](#ddb-table-074), [DDB-TABLE-014](table-policy-kinesis-autoscaling.md#ddb-table-014), [DDB-TABLE-098](#ddb-table-098), [DDB-TABLE-447](#ddb-table-447) · evidence:
    table/error-taxonomy/not-found-codes
  - notes: Confirms the HTTP-400 part of H-T-054 and the ValidationException-for-malformed-ARN part; the codes
    are not uniform across API families (see the TableNotFoundException finding). Validation-before-existence
    means a controller cannot infer 'table gone' from a failed UpdateTable without checking the...
  - full notes: [details/DDB-TABLE-070.md](details/DDB-TABLE-070.md)

- <a id="ddb-table-098"></a>**DDB-TABLE-098** `error-code` · impact high · unhandled (not handled in controller) · verified 2026-10-08
  **Missing table: TTL/Insights APIs -> ResourceNotFoundException; continuous-backups/backup/restore -> TableNotFoundException (HTTP 400)**
  Ops by error code for a nonexistent table name: {'ResourceNotFoundException': ['DescribeTable',
  'DescribeTimeToLive', 'UpdateTimeToLive', 'DescribeContributorInsights', 'UpdateContributorInsights',
  'ListContributorInsights', 'DescribeKinesisStreamingDestination', 'ListTagsOfResource',
  'DescribeTableReplicaAutoScaling', 'UpdateTable(DeletionProtection)', 'DeleteTable'],
  'TableNotFoundException': ['DescribeContinuousBackups', 'UpdateContinuousBackups', 'CreateBackup',
  'RestoreTableToPointInTime'], 'ParamValidationError': ['DescribeContributorInsights(index)'], None:
  ['ListBackups(TableName)']}. Messages: TTL 'Requested resource not found: Table: ackq-30dba0-missing not
  found'; CB 'Table not found: ackq-30dba0-missing'; CI 'Requested resource not found: Table:
  ackq-30dba0-missing not found'; restore 'Table not found: ackq-30dba0-missing'; CreateBackup 'Table not
  found: ackq-30dba0-missing'; ListBackups(TableName) -> OK.
  - ACK: exceptions.404, terminal_codes · ops: CreateBackup, DeleteTable, DescribeContinuousBackups,
    DescribeContributorInsights, DescribeContributorInsights(index), DescribeKinesisStreamingDestination,
    DescribeTable, DescribeTableReplicaAutoScaling, DescribeTimeToLive, ListBackups(TableName),
    ListContributorInsights, ListTagsOfResource, RestoreTableToPointInTime, UpdateContinuousBackups,
    UpdateContributorInsights, UpdateTable(DeletionProtection), UpdateTimeToLive
  - repro: call each API with a nonexistent TableName
  - handling: not handled in the controller (as of commit 34b85e6)
  - related: [DDB-TABLE-014](table-policy-kinesis-autoscaling.md#ddb-table-014), [DDB-TABLE-070](#ddb-table-070), [DDB-TABLE-074](#ddb-table-074), [DDB-TABLE-447](#ddb-table-447) · evidence:
    table/error-taxonomy/subresource-errors
  - notes: Hypotheses: H-S-004, H-S-023, H-S-124.

- <a id="ddb-table-118"></a>**DDB-TABLE-118** `error-code` · impact high · handled · verified 2026-10-08
  **DELETING table: Update TTL/PITR/Insights, CreateBackup, Restore and most Describes return 200 in the first ~1 s; DescribeTimeToLive rejects**
  While TableStatus=DELETING (observed in table/error-taxonomy/subresource-errors,
  table/sub-resources/pitr-lifecycle, table/sub-resources/insights-lifecycle): UpdateTimeToLive -> 200;
  UpdateContinuousBackups(disable) -> 200; UpdateContributorInsights -> 200 (DISABLING); CreateBackup -> 200
  (an AVAILABLE USER backup of the dying table is created); RestoreTableToPointInTime(UseLatestRestorableTime)
  -> 200 (a new table is created from the deleting source); DescribeContinuousBackups,
  DescribeContributorInsights, ListContributorInsights -> 200; DescribeTimeToLive -> ValidationException (HTTP 400)
  'Cannot describe time to live while table is in DELETING state: Current table state is DELETING'. Once the
  table is gone: DescribeTimeToLive/UpdateTimeToLive/Describe+Update+ListContributorInsights ->
  ResourceNotFoundException 'Requested resource not found: Table: X not found' (byte-identical to the
  CREATING-phase message);
  DescribeContinuousBackups/UpdateContinuousBackups/RestoreTableToPointInTime/CreateBackup ->
  TableNotFoundException 'Table not found: X'. All HTTP 400.
  - ACK: exceptions.404, deletable.when, terminal_codes · ops: DescribeTimeToLive, UpdateTimeToLive,
    DescribeContinuousBackups, UpdateContinuousBackups, DescribeContributorInsights,
    UpdateContributorInsights, ListContributorInsights
  - repro: DeleteTable; call each sub-resource API immediately and after the table is gone
  - handling: handled via `generator.yaml:84-87; pkg/resource/table/sdk.go:83-86`
  - related: [DDB-BACKUP-001](backup.md#ddb-backup-001), [DDB-BACKUP-007](backup.md#ddb-backup-007), [DDB-TABLE-100](table-restore.md#ddb-table-100), [DDB-TABLE-087](table-restore.md#ddb-table-087), [DDB-TABLE-454](table-subresources.md#ddb-table-454), [DDB-TABLE-455](#ddb-table-455),
    [DDB-TABLE-227](table-replicas.md#ddb-table-227), [DDB-TABLE-228](table-replicas.md#ddb-table-228) · evidence: table/error-taxonomy/subresource-errors,
    table/state-machine/subresource-admissibility, table/sub-resources/insights-lifecycle,
    table/sub-resources/pitr-lifecycle
  - notes: Hypotheses: H-S-004. Hypotheses: H-S-004. In this probe the DeleteTable itself was refused
    (deletion protection left on by a throttled UpdateTable), so the DELETING observations above come from the
    three sibling probes listed in evidence.
  - full notes: [details/DDB-TABLE-118.md](details/DDB-TABLE-118.md)

- <a id="ddb-table-442"></a>**DDB-TABLE-442** `error-code` · impact high · unhandled (not handled in controller) · verified 2026-10-09
  **ValidationException message catalogue: 4 of the ~360 texts are transient (TTL 30-min cooldown, CREATING/UPDATING state gates)...**
  ValidationException (HTTP 400) carries ~360 distinct message texts in this lab's evidence. Almost all are
  PERMANENT-SPEC, but a handful are transient and must NOT be surfaced as terminal: 'Time to live has been
  modified multiple times within a fixed interval' (TRANSIENT-RETRY, ~30 min, state invisible in
  DescribeTimeToLive), 'Cannot describe time to live while table is in CREATING state' / '...DELETING state'
  (WAIT-FOR-STATE TableStatus), 'Table or Index is not in a valid state to update Key Access Insights:
  TableStatus must be ACTIVE...' (WAIT-FOR-STATE), 'Table is not in a valid state to enable Kinesis Streaming
  Destination: ...must be DISABLED or ENABLE_FAILED...' / '...must be ACTIVE to perform DISABLE...'
  (WAIT-FOR-STATE DestinationStatus), 'Operation cannot be performed while replica server-side encryption
  status is in UPDATING state' and 'Create/Update/Delete of replica is not allowed while the replica is being
  added...' (WAIT-FOR-STATE ReplicaStatus), 'Replica cannot be deleted because it has acted as a source region
  for new replica(s) being added to the table in the last 24 hours' (PERMANENT-QUOTA 24 h). Every text starts
  with one of ~12 stable prefixes ('One or more parameter values were invalid: ', 'N validation error(s)
  detected: Value ... at ...', 'Invalid Request: ', 'KMS validation error: ', 'Failed to update settings for
  global table with name: ', 'Table is not in a valid state to ...', ...).
  - ACK: terminal_codes, requeue, custom_update · ops: UpdateTable, UpdateTimeToLive, DescribeTimeToLive,
    UpdateContributorInsights, EnableKinesisStreamingDestination, CreateTable, TagResource, PutResourcePolicy
    · fields: TimeToLiveSpecification, SSESpecification, StreamSpecification
  - repro: see probe.py (live) and catalogue.py (mined); e.g. UpdateTimeToLive enable then disable within 30
    min -> cooldown text
  - handling: not handled in the controller (as of commit 34b85e6)
  - related: [DDB-TABLE-312](table-replicas.md#ddb-table-312), [DDB-TABLE-311](table-replicas.md#ddb-table-311), [DDB-TABLE-266](table-replicas.md#ddb-table-266), [DDB-TABLE-267](table-replicas.md#ddb-table-267) · evidence:
    table/creative/error-message-regions
  - notes: Classes: RETRY = TRANSIENT-RETRY, clears by itself (typical wait given); WAIT =
    TRANSIENT-WAIT-FOR-STATE, clears when the named status changes; SPEC = PERMANENT-SPEC, user must change
    the spec / fix an external dependency; QUOTA = PERMANENT-QUOTA, needs a quota increase or the stated
    1h/24h/30d...
  - full notes: [details/DDB-TABLE-442.md](details/DDB-TABLE-442.md)

- <a id="ddb-table-443"></a>**DDB-TABLE-443** `error-code` · impact high · unhandled (not handled in controller) · verified 2026-10-09
  **LimitExceededException message catalogue: 3 transient texts (tag lock ~2 s, one online index, hourly decrease) vs 24h/30d/account quotas**
  LimitExceededException (HTTP 400) texts fall into four classes. TRANSIENT-RETRY: 'Subscriber limit exceeded:
  Table tags are being updated: <TBL>' (~1.5-3 s). TRANSIENT-WAIT-FOR-STATE: 'Subscriber limit exceeded: Only
  1 online index can be created or deleted simultaneously per table' (IndexStatus ACTIVE), 'Subscriber limit
  exceeded: Only 50 restore operations can be done simultaneously' (restores finish). PERMANENT-QUOTA with a
  stated wait: 'Provisioned throughput decreases are limited within a given UTC day... at most once every 3600
  seconds...' (1 h / 00:00 UTC), 'Encryption mode changes are limited in the 24h window ending at <TS>... once
  every 21600 seconds... Next changes can be made at <TS>.' (6 h), 'Updates to TableClass are limited to 2
  times in 30 day(s).' (30 d, also 'Limit exceeded for replica in <REGION>. Updates to TableClass...'), 'The
  requested ReadCapacityUnits, N, is above the per table maximum for the account in <REGION>. Per table
  maximum: 40000...', 'This request would have caused the ReadCapacityUnits limit to be exceeded for the
  account in <REGION>...', '...exceeds TableMaxReadCapacityUnits of the account in region <REGION>' (quota
  increase). PERMANENT-SPEC: 'Subscriber limit exceeded: Number of global secondary indexes exceeds per-table
  limit of 20'. Decrease/encryption texts embed the next allowed time; TableClass does not.
  - ACK: terminal_codes, requeue · ops: UpdateTable, TagResource, UntagResource, RestoreTableFromBackup,
    CreateTable · fields: TableClass, SSESpecification, ProvisionedThroughput, Tags
  - repro: TagResource then UntagResource immediately; three TableClass switches on one table; see
    catalogue.py
  - handling: not handled in the controller (as of commit 34b85e6)
  - related: [DDB-TABLE-171](table-throughput-billing.md#ddb-table-171), [DDB-TABLE-103](#ddb-table-103), [DDB-TABLE-283](table-streams-encryption-class.md#ddb-table-283), [DDB-TABLE-081](table-streams-encryption-class.md#ddb-table-081), [DDB-TABLE-282](table-restore.md#ddb-table-282) · evidence:
    table/creative/error-message-regions
  - notes: Classes: RETRY = TRANSIENT-RETRY, clears by itself (typical wait given); WAIT =
    TRANSIENT-WAIT-FOR-STATE, clears when the named status changes; SPEC = PERMANENT-SPEC, user must change
    the spec / fix an external dependency; QUOTA = PERMANENT-QUOTA, needs a quota increase or the stated
    1h/24h/30d...
  - full notes: [details/DDB-TABLE-443.md](details/DDB-TABLE-443.md)

- <a id="ddb-table-444"></a>**DDB-TABLE-444** `error-code` · impact high · unhandled (not handled in controller) · verified 2026-10-09
  **ResourceInUseException catalogue: 40+ texts, all transient except 'Table already exists'; global tables return a terse detail-free variant**
  ResourceInUseException (HTTP 400) is TRANSIENT-WAIT-FOR-STATE for every text except 'Table already exists:
  <TBL>' (CreateTable/ImportTable duplicate: PERMANENT, adopt or rename) and 'Global table with name: '<TBL>'
  already exists with replicas in regions: ...' (PERMANENT-SPEC). Regional-table texts are 'Attempt to change
  a resource which is still in use: <detail>' where <detail> names the blocking state ('Table is being
  created: <TBL>', 'Table is being deleted: <TBL>', 'Table: <TBL> is in the process of being updated.', 'Table
  IOPS are currently being updated...', 'Cannot delete table while indexes are being created, updated, or
  deleted.', 'Index creation is in resource allocation phase. Retry deletion during backfilling phase or when
  the index is active...', 'Table tags are being updated: <TBL>' (~2 s), 'Table is pending previous
  resource-based policy update: <TBL>' (~2 s), 'Server-Side Encryption is still being updated', ...).
  UpdateTimeToLive uses a different word order ('Table <TBL> is being created') and sometimes the bare prefix
  'Attempt to change a resource which is still in use'. Tables that are members of a global table (replicas
  present) return the detail-free 'The resource which you are attempting to change is in use.' for UpdateTable
  and DeleteTable (27 records, all in replica probes), so the blocking state cannot be read from the message
  there.
  - ACK: requeue, synced.when, deletable.when · ops: UpdateTable, DeleteTable, CreateTable, TagResource,
    PutResourcePolicy, UpdateTimeToLive
  - repro: UpdateTable/DeleteTable in each state; see catalogue.py (mined from state-machine, cross-region and
    dependencies probes)
  - handling: not handled in the controller (as of commit 34b85e6)
  - related: [DDB-TABLE-005](table.md#ddb-table-005), [DDB-TABLE-001](table.md#ddb-table-001), [DDB-TABLE-150](table-indexes.md#ddb-table-150), [DDB-TABLE-200](table-replicas.md#ddb-table-200), [DDB-TABLE-219](table-restore.md#ddb-table-219), [DDB-TABLE-275](table-restore.md#ddb-table-275),
    [DDB-TABLE-100](table-restore.md#ddb-table-100), [DDB-TABLE-217](table-restore.md#ddb-table-217), [DDB-IMPORT-019](import.md#ddb-import-019), [DDB-IMPORT-018](import.md#ddb-import-018) · evidence:
    table/creative/error-message-regions
  - notes: Classes: RETRY = TRANSIENT-RETRY, clears by itself (typical wait given); WAIT =
    TRANSIENT-WAIT-FOR-STATE, clears when the named status changes; SPEC = PERMANENT-SPEC, user must change
    the spec / fix an external dependency; QUOTA = PERMANENT-QUOTA, needs a quota increase or the stated
    1h/24h/30d...
  - full notes: [details/DDB-TABLE-444.md](details/DDB-TABLE-444.md)

- <a id="ddb-table-445"></a>**DDB-TABLE-445** `error-code` · impact high · unhandled (not handled in controller) · verified 2026-10-09
  **ThrottlingException catalogue: 'Rate exceeded' / account control-plane rate (back off) vs per-table 15 s cooldowns with embedded retry-after**
  Five ThrottlingException (HTTP 400) texts, all TRANSIENT-RETRY. Rate limits: 'Rate exceeded'
  (per-connection/per-API read buckets, retry in ~1 s) and 'The rate of control plane requests made by this
  account is too high' (account mutation limiter, ~1 s). Per-table cooldowns that are NOT rate limits:
  'Deletion protection setting for table <TBL> modified within the previous 15000 milliseconds. Please try
  again after <TS>' and 'Resource-based policy for table <TBL> modified within the previous 15000
  milliseconds. Please try again after <TS>.' (also '...for stream <label>...'). The cooldown texts carry an
  ISO-8601 retry-after timestamp with millisecond precision, so exact or suffix matching breaks on every
  occurrence while the prefix 'Deletion protection setting for table' / 'Resource-based policy for' is stable;
  the timestamp can be parsed to compute the requeue delay (<= 15 s).
  - ACK: requeue, terminal_codes · ops: UpdateTable, PutResourcePolicy, DeleteResourcePolicy,
    DescribeTimeToLive, TagResource, CreateTable, DeleteTable
  - repro: UpdateTable(DeletionProtectionEnabled) twice within 15 s; PutResourcePolicy twice within 15 s
  - handling: not handled in the controller (as of commit 34b85e6)
  - related: [DDB-TABLE-003](table-streams-encryption-class.md#ddb-table-003), [DDB-TABLE-206](table-policy-kinesis-autoscaling.md#ddb-table-206), [DDB-TABLE-053](#ddb-table-053), [DDB-TABLE-403](#ddb-table-403), [DDB-TABLE-247](table-policy-kinesis-autoscaling.md#ddb-table-247), [DDB-TABLE-347](table-policy-kinesis-autoscaling.md#ddb-table-347),
    [DDB-TABLE-464](#ddb-table-464), [DDB-TABLE-348](table-policy-kinesis-autoscaling.md#ddb-table-348), [DDB-TABLE-213](table-streams-encryption-class.md#ddb-table-213), [DDB-TABLE-205](table-policy-kinesis-autoscaling.md#ddb-table-205), [DDB-TABLE-099](#ddb-table-099), [DDB-TABLE-360](table-subresources.md#ddb-table-360), [DDB-TABLE-434](#ddb-table-434),
    [DDB-TABLE-435](table-streams-encryption-class.md#ddb-table-435), [DDB-TABLE-383](table-streams-encryption-class.md#ddb-table-383), [DDB-TABLE-117](table-streams-encryption-class.md#ddb-table-117), [DDB-TABLE-377](#ddb-table-377) · evidence:
    table/creative/error-message-regions
  - notes: Classes: RETRY = TRANSIENT-RETRY, clears by itself (typical wait given); WAIT =
    TRANSIENT-WAIT-FOR-STATE, clears when the named status changes; SPEC = PERMANENT-SPEC, user must change
    the spec / fix an external dependency; QUOTA = PERMANENT-QUOTA, needs a quota increase or the stated
    1h/24h/30d...
  - full notes: [details/DDB-TABLE-445.md](details/DDB-TABLE-445.md)

- <a id="ddb-table-446"></a>**DDB-TABLE-446** `error-code` · impact high · unhandled (not handled in controller) · verified 2026-10-09
  **ResourceNotFoundException catalogue: the word after 'not found:'...**
  ResourceNotFoundException (HTTP 400) has ~8 text families: 'Requested resource not found: Table: <TBL> not
  found' (table gone or CREATING: WAIT or re-create), 'Requested resource not found: ResourceArn: <ARN> not
  found' (tag APIs; also returned while the table is CREATING -> WAIT-FOR-STATE), 'Requested resource not
  found: Index <name> for table <TBL>' (UpdateTable GSI Update/Delete: PERMANENT-SPEC) vs 'Requested resource
  not found: Index: <name> not found for table: <TBL>' (Contributor Insights, also while the index is
  CREATING), 'Requested resource not found: Stream: <label> not found for Table: <TBL>' (policy APIs on a
  stream ARN), "Global table with name: '<TBL>' does not exist." (DescribeTableReplicaAutoScaling on a
  regional table: PERMANENT), 'Failed to update settings for global table with name: ... because a replica
  does not exist in regions / the global secondary indexes with names ... do not exist' (PERMANENT-SPEC),
  'Stream <TBL> under account <ACCT> not found.' (Kinesis, different API), bare 'Requested resource not found'
  (data plane). On global tables the same text may arrive wrapped as '... not found (Service:
  AmazonDynamoDBv2; Status Code: 400; Error Code: ResourceNotFoundException; Request ID: <52 chars>; Proxy:
  null)', i.e. with a per-request id, so only prefix matching is safe.
  - ACK: exceptions.404, requeue · ops: DescribeTable, UpdateTable, TagResource, ListTagsOfResource,
    DescribeContributorInsights, GetResourcePolicy, DescribeTableReplicaAutoScaling,
    UpdateTableReplicaAutoScaling
  - repro: DescribeTable missing; UpdateTable GSI Update IndexName=ghost; DescribeContributorInsights
    IndexName=ghost; DescribeTableReplicaAutoScaling on a regional table
  - handling: not handled in the controller (as of commit 34b85e6)
  - related: [DDB-TABLE-070](#ddb-table-070), [DDB-TABLE-161](table-subresources.md#ddb-table-161), [DDB-TABLE-098](#ddb-table-098), [DDB-TABLE-101](#ddb-table-101), [DDB-TABLE-187](table-replicas.md#ddb-table-187), [DDB-TABLE-232](table-replicas.md#ddb-table-232),
    [DDB-TABLE-254](table-replicas.md#ddb-table-254), [DDB-TABLE-231](table-global-tables.md#ddb-table-231), [DDB-TABLE-253](table-replicas.md#ddb-table-253) · evidence: table/creative/error-message-regions
  - notes: Classes: RETRY = TRANSIENT-RETRY, clears by itself (typical wait given); WAIT =
    TRANSIENT-WAIT-FOR-STATE, clears when the named status changes; SPEC = PERMANENT-SPEC, user must change
    the spec / fix an external dependency; QUOTA = PERMANENT-QUOTA, needs a quota increase or the stated
    1h/24h/30d...
  - full notes: [details/DDB-TABLE-446.md](details/DDB-TABLE-446.md)

- <a id="ddb-table-447"></a>**DDB-TABLE-447** `error-code` · impact medium · unhandled (not handled in controller) · verified 2026-10-09
  **TableNotFoundException catalogue: three texts ('Table not found: <name>' / ': <ARN>' / bare) from the backup, PITR and export families...**
  TableNotFoundException (HTTP 400) is used only by the continuous-backups/backup/restore/export family.
  Texts: 'Table not found: <TBL>' (name-based calls), 'Table not found: <ARN>' (ExportTableToPointInTime
  echoes the ARN), bare 'Table not found' (RestoreTableToPointInTime with SourceTableArn, us-east-1). It is
  PERMANENT (missing table) when the table does not exist, but TRANSIENT-WAIT-FOR-STATE when the table is
  CREATING (CreateBackup/UpdateContinuousBackups right after CreateTable) and when a restore target is still
  CREATING - the text is identical in both cases, so a reconciler must consult DescribeTable before giving up.
  - ACK: exceptions.404, requeue · ops: CreateBackup, DescribeContinuousBackups, UpdateContinuousBackups,
    RestoreTableToPointInTime, ExportTableToPointInTime
  - repro: DescribeContinuousBackups/CreateBackup on a missing name; CreateBackup right after CreateTable;
    ExportTableToPointInTime with a missing ARN
  - handling: not handled in the controller (as of commit 34b85e6)
  - related: [DDB-TABLE-014](table-policy-kinesis-autoscaling.md#ddb-table-014), [DDB-TABLE-074](#ddb-table-074), [DDB-BACKUP-001](backup.md#ddb-backup-001), [DDB-TABLE-217](table-restore.md#ddb-table-217), [DDB-TABLE-070](#ddb-table-070), [DDB-TABLE-098](#ddb-table-098),
    [DDB-TABLE-298](table-replicas.md#ddb-table-298), [DDB-IMPORT-001](import.md#ddb-import-001), [DDB-BACKUP-002](backup.md#ddb-backup-002), [DDB-EXPORT-014](export.md#ddb-export-014), [DDB-EXPORT-001](export.md#ddb-export-001), [DDB-EXPORT-002](export.md#ddb-export-002),
    [DDB-BACKUP-012](backup.md#ddb-backup-012), [DDB-TABLE-274](table-restore.md#ddb-table-274), [DDB-TABLE-273](table-restore.md#ddb-table-273), [DDB-TABLE-272](table-restore.md#ddb-table-272) · evidence:
    table/creative/error-message-regions
  - notes: Classes: RETRY = TRANSIENT-RETRY, clears by itself (typical wait given); WAIT =
    TRANSIENT-WAIT-FOR-STATE, clears when the named status changes; SPEC = PERMANENT-SPEC, user must change
    the spec / fix an external dependency; QUOTA = PERMANENT-QUOTA, needs a quota increase or the stated
    1h/24h/30d...
  - full notes: [details/DDB-TABLE-447.md](details/DDB-TABLE-447.md)

- <a id="ddb-table-448"></a>**DDB-TABLE-448** `error-code` · impact high · unhandled (not handled in controller) · verified 2026-10-09
  **HTTP 500 catalogue: InternalFailure (EMPTY message) / InternalServerError ('Internal server error', KMS text) are deterministic shape bugs**
  All HTTP 500 responses seen in 9562 error records are reproducible request-shape bugs, not transient faults:
  code 'InternalFailure' with an EMPTY message (UpdateTable WarmThroughput={} / OnDemandThroughput={} / GSI
  Update WarmThroughput on a ghost index; 1.5-1.8 s latency), code 'InternalServerError' with 'Internal server
  error' (ListGlobalTables RegionName=<bogus>), and 'InternalServerError' with 'KMS internal error:
  com.amazonaws.services.kms.model.AWSKMSException: EncryptionContext is supported only when creating a grant
  for a symmetric encryption KMS key. (Service: AWSKMS; Status Code: 400; Error Code: ValidationException;
  Request ID: <UUID>; Proxy: null)' (asymmetric CMK). A controller that retries 5xx forever never converges;
  the message is empty for the most common one, so only the (operation, request shape) identifies it.
  Validation order measured here: WarmThroughput={} on a MISSING table and combined with other members -> see
  live records (whether the 500 fires before ResourceNotFound / 'must be the only operation').
  - ACK: terminal_codes, requeue · ops: UpdateTable, CreateTable, ListGlobalTables · fields: WarmThroughput,
    OnDemandThroughput, KMSMasterKeyId
  - repro: UpdateTable TableName=<any> WarmThroughput={}; ListGlobalTables RegionName=bogus-region-1
  - handling: not handled in the controller (as of commit 34b85e6)
  - related: [DDB-TABLE-437](table-throughput-billing.md#ddb-table-437), [DDB-TABLE-178](table-throughput-billing.md#ddb-table-178), [DDB-TABLE-161](table-subresources.md#ddb-table-161), [DDB-TABLE-022](table-streams-encryption-class.md#ddb-table-022), [DDB-TABLE-456](#ddb-table-456), [DDB-TABLE-458](table-indexes.md#ddb-table-458) ·
    evidence: table/creative/error-message-regions
  - notes: Classes: RETRY = TRANSIENT-RETRY, clears by itself (typical wait given); WAIT =
    TRANSIENT-WAIT-FOR-STATE, clears when the named status changes; SPEC = PERMANENT-SPEC, user must change
    the spec / fix an external dependency; QUOTA = PERMANENT-QUOTA, needs a quota increase or the stated
    1h/24h/30d...
  - full notes: [details/DDB-TABLE-448.md](details/DDB-TABLE-448.md)

- <a id="ddb-table-449"></a>**DDB-TABLE-449** `error-code` · impact high · unhandled (not handled in controller) · verified 2026-10-09
  **Error texts are identical in us-west-2 and us-east-1 after removing resource names/timestamps (58/58 triggers)...**
  The same 58 error triggers were fired in us-west-2 and us-east-1: 58/58 returned the same code AND the same
  text after normalising table names/ARNs/timestamps (differences, if any, listed in notes). Request-specific
  noise found in otherwise stable texts: resource names and ARNs (most texts), ISO-8601 millisecond timestamps
  (DP and policy 15 s cooldowns, SSE 24h window, system-backup expiry, PITR window), 52-char request ids
  (Java-SDK suffix on global-table errors), region names (quota texts), and the UpdateTable 'At least one of
  ... is required' list, which omits whichever listed member the request already contained
  (MultiRegionConsistency disappears when it was sent) and names three members that are not in the public SDK
  model (MultiAccountReplicaReady, ReplicaTransitRoleArn, UpdateStreamEnabled). Safe strategy: match on a
  stable prefix (or a short invariant phrase), never on the full text or suffix.
  - ACK: terminal_codes, requeue · ops: UpdateTable, DescribeTable, TagResource, PutResourcePolicy,
    UpdateTimeToLive, CreateTable
  - repro: see probe.py: same trigger list against one table per region; compare after norm()
  - handling: not handled in the controller (as of commit 34b85e6)
  - related: [DDB-TABLE-015](table-indexes.md#ddb-table-015), [DDB-TABLE-047](table-throughput-billing.md#ddb-table-047), [DDB-TABLE-160](table-indexes.md#ddb-table-160), [DDB-TABLE-437](table-throughput-billing.md#ddb-table-437) · evidence:
    table/creative/error-message-regions
  - notes: Per-label comparison (code, http, raw_identical, same_after_norm, noise, latency) is in result.yaml
    region_compare; differences: {}. Required-list texts: {"us-west-2": {"only_name": "At least one of
    ProvisionedThroughput, BillingMode, UpdateStreamEnabled, GlobalSecondaryIndexUpdates,...
  - full notes: [details/DDB-TABLE-449.md](details/DDB-TABLE-449.md)

### Request validation

- <a id="ddb-table-107"></a>**DDB-TABLE-107** `request-validation` · impact medium · unhandled (not handled in controller) · verified 2026-10-08
  **Tag validation is synchronous, all-or-nothing and always ValidationException (length, empty key, dup keys, Tags=[] / TagKeys=[])**
  Server-side (client validation disabled): key of 128 chars -> 200, 129 -> ValidationException 'The Tag Key
  provided is invalid, Key: kkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkk'; value
  256 -> 200, 257 -> ValidationException 'The Tag Value provided is invalid, Value:
  vvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvv'; empty key -> ValidationException 'The
  Tag Key provided is invalid, Key: '; Tags=[] -> ValidationException 'Atleast one Tag needs to be provided as
  Input.'; TagKeys=[] -> ValidationException 'Atleast one Tag Key needs to be provided as Input.'; Tag without
  Value member -> ValidationException 'The Tag Value provided is invalid, Value: null'; duplicate keys in one
  request -> ValidationException 'Duplicate Tag Keys provided as input: Duplicate Tag Key found dup' (stored
  value: None); UntagResource with a 129-char key -> ValidationException 'The Tag Key provided is invalid,
  Key: kkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkk'; UntagResource with 51 keys
  -> ValidationException 'Number of Tags exceed the current limit for the provided ResourceArn'. Mixed request
  [good, 129-char key] -> ValidationException 'The Tag Key provided is invalid, Key:
  xxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxx' and the good tag was NOT applied;
  mixed [good, aws:zzz] -> ValidationException 'Tag Key cannot be prefixed with aws:, Key: aws:zzz', good tag
  NOT applied.
  - ACK: tags.custom-sync, terminal_codes · ops: TagResource, UntagResource · fields: Tags, TagKeys
  - repro: TagResource with each malformed payload against an ACTIVE table (boto3 parameter_validation=False)
  - handling: not handled in the controller (as of commit 34b85e6)
  - related: [DDB-TABLE-105](#ddb-table-105), [DDB-TABLE-106](#ddb-table-106), [DDB-TABLE-108](#ddb-table-108), [DDB-TABLE-109](#ddb-table-109), [DDB-TABLE-110](table.md#ddb-table-110), [DDB-TABLE-368](#ddb-table-368),
    [DDB-TABLE-372](#ddb-table-372) · evidence: table/tags/validation-upsert-limits
  - notes: Confirms H-T-133 (all-or-nothing, ValidationException) and H-T-137 (empty lists rejected: 'Atleast
    one Tag needs to be provided as Input.'). Note UntagResource with >50 keys is rejected with the tag-count
    message even though it removes tags. In every mixed request the valid tag was NOT applied.

- <a id="ddb-table-372"></a>**DDB-TABLE-372** `request-validation` · impact medium · unhandled (not handled in controller) · verified 2026-10-09
  **Tag charset is the AWS tag regex: letters/digits/spaces and + - = . _ : / @ only; emoji, comma, %, quotes, &, *, ;, ?, |, \, <>, ~ rejected**
  TagResource accepted values/keys with Cyrillic ('ключ'), Latin-1 ('café'), CJK ('表'), NBSP and ideographic
  space, and the value 'a+b-c=d.e_f:g/h@i'. Rejected with ValidationException 'The Tag Value provided is
  invalid, Value: ...' (or 'The Tag Key provided is invalid'): emoji '🔑' (key and value), '100%', 'a,b',
  'it''s "q"', '(x)[y]{z}', 'a&b', 'a*b', 'a;b', 'a?b', 'a|b', 'a\b', '<a>', '~a'. Each rejection fails the
  whole TagResource call (all-or-nothing, [DDB-TABLE-107](#ddb-table-107)). Letters of any script are fine; punctuation other
  than + - = . _ : / @ is not.
  - ACK: terminal_codes, tags.custom-sync, docs-only · ops: TagResource, CreateTable · fields: Tags
  - repro: TagResource one tag at a time with each character class (22 cases), 2.2 s apart to respect the
    per-table tag write lock.
  - handling: not handled in the controller (as of commit 34b85e6)
  - related: [DDB-TABLE-106](#ddb-table-106), [DDB-TABLE-107](#ddb-table-107), [DDB-TABLE-110](table.md#ddb-table-110), [DDB-TABLE-105](#ddb-table-105), [DDB-TABLE-108](#ddb-table-108), [DDB-TABLE-109](#ddb-table-109),
    [DDB-TABLE-368](#ddb-table-368) · evidence: table/creative/xs-kms-echo-tags, table/creative/xs-identity-tags-streams
  - notes: Extends [DDB-TABLE-106](#ddb-table-106) ('#' rejected, 'unicode accepted') with the full accept/reject list;
    comma-separated or percent values copied from Kubernetes labels/annotations are a terminal
    ValidationException for the whole tag set, and because CreateTable carries Tags inline ([DDB-TABLE-110](table.md#ddb-table-110)) a
    single bad...
  - full notes: [details/DDB-TABLE-372.md](details/DDB-TABLE-372.md)

- <a id="ddb-table-456"></a>**DDB-TABLE-456** `request-validation` · impact high · unhandled (not handled in controller) · verified 2026-10-09
  **UpdateTable: WarmThroughput={} / OnDemandThroughput={} is silently dropped (200, no-op) when any other member is sent; alone it is HTTP 500**
  On an idle PAY_PER_REQUEST table: UpdateTable {WarmThroughput:{}} alone -> HTTP 500 InternalFailure (known),
  but {WarmThroughput:{}, DeletionProtectionEnabled:false} -> 200, {WarmThroughput:{}, TableClass:STANDARD} ->
  200 and {OnDemandThroughput:{}, BillingMode:PAY_PER_REQUEST} -> 200 with no change to the table and WITHOUT
  the usual 'WarmThroughput must be the only operation in the request' rejection: the empty struct is treated
  as absent for composition and ignored. Combined with ProvisionedThroughput on a PPR table the PT rule fires
  first (ValidationException). At GSI level the same shapes are validated properly:
  GlobalSecondaryIndexUpdates[Update{IndexName, WarmThroughput:{}}] -> ValidationException 'One or more
  parameter values were invalid: WarmThroughput must have at least one of ReadUnitsPerSecond or
  WriteUnitsPerSecond specified for index: gsi1' (also for a ghost index and even for a MISSING table - this
  validation precedes the existence check), Update{IndexName, OnDemandThroughput:{}} and Update{IndexName}
  alone -> 'The only Updates for index: gsi1 when TableThroughputMode is PAY_PER_REQUEST can be to
  OnDemandThroughput, WarmThroughput'; GSI OnDemandThroughput {-1,-1} on a real index -> 200 (clears, like the
  table). CreateTable with GlobalSecondaryIndexes[].WarmThroughput={} or OnDemandThroughput={} -> 200, index
  created with default WarmThroughput 12000/4000 and no OnDemandThroughput.
  - ACK: custom_update, compare.nil_equals_zero_value, requeue · ops: UpdateTable, CreateTable · fields:
    WarmThroughput, OnDemandThroughput, GlobalSecondaryIndexUpdates
  - repro: UpdateTable TableName=X WarmThroughput={} DeletionProtectionEnabled=false (200) vs UpdateTable
    TableName=X WarmThroughput={} (500); UpdateTable GlobalSecondaryIndexUpdates=[{Update:{IndexName:gsi1,
    WarmThroughput:{}}}]
  - handling: not handled in the controller (as of commit 34b85e6)
  - related: [DDB-TABLE-437](table-throughput-billing.md#ddb-table-437), [DDB-TABLE-178](table-throughput-billing.md#ddb-table-178), [DDB-TABLE-161](table-subresources.md#ddb-table-161), [DDB-TABLE-433](table-streams-encryption-class.md#ddb-table-433), [DDB-TABLE-043](table-indexes.md#ddb-table-043), [DDB-TABLE-129](table-indexes.md#ddb-table-129),
    [DDB-TABLE-359](table-indexes.md#ddb-table-359), [DDB-TABLE-126](table-indexes.md#ddb-table-126), [DDB-TABLE-448](#ddb-table-448), [DDB-TABLE-458](table-indexes.md#ddb-table-458), [DDB-TABLE-128](table-indexes.md#ddb-table-128), [DDB-TABLE-152](table-indexes.md#ddb-table-152), [DDB-TABLE-133](table-indexes.md#ddb-table-133),
    [DDB-TABLE-135](table-indexes.md#ddb-table-135), [DDB-TABLE-154](table-throughput-billing.md#ddb-table-154), [DDB-TABLE-153](table-indexes.md#ddb-table-153), [DDB-TABLE-375](table-indexes.md#ddb-table-375), [DDB-TABLE-286](table-streams-encryption-class.md#ddb-table-286) · evidence:
    table/creative/degenerate-5xx-hunt
  - notes: Extends [DDB-TABLE-437](table-throughput-billing.md#ddb-table-437)/178 (the {} -> 500 case) and [DDB-TABLE-161](table-subresources.md#ddb-table-161) (ghost-GSI WarmThroughput 500,
    reproduced here with valid values: gsi_update_warm_ghost_valid_values -> 500 InternalFailure, while ghost +
    {} -> 400 ValidationException). Controller consequence: a reconciler that materialises an empty...
  - full notes: [details/DDB-TABLE-456.md](details/DDB-TABLE-456.md)

- <a id="ddb-table-457"></a>**DDB-TABLE-457** `request-validation` · impact medium · unhandled (not handled in controller) · verified 2026-10-09
  **Degenerate-input 4xx catalogue: 82 empty-struct/empty-string/zero/-1 shapes across 20 operations are ValidationException with 6 validator...**
  Of 98 schema-valid-but-degenerate requests reaching the service (100 sent, 2 refused client-side), 82 were
  HTTP 400 ValidationException, 3 AccessDeniedException (KMS alias/aws/s3), 2 TableInUseException (restore
  side effect, see the restore finding), 1 ResourceNotFoundException, 6 accepted, 4 HTTP 500 (HMAC key x3,
  ghost-GSI WarmThroughput). The 400 texts come in six validator styles that a message classifier must accept:
  (1) generic shape 'N validation error(s) detected: Value ... at '<camelCaseField>' failed to satisfy
  constraint: ...' (PITR {}, RecoveryPeriodInDays 0/-1, TTL {}/{Enabled}/AttributeName '', ExpectedRevisionId
  '', StreamArn '', Limit 0, BackupName '', TableName '', BillingModeOverride '', Provisioned/GSI/LSI
  overrides {} / [{}]); (2) hand-written per-field texts without the prefix: 'IndexName must be at least 3
  characters long and at most 255 characters long' (Contributor Insights), 'TableName must be at least 3
  characters long...' (legacy global-table APIs and RestoreTableFromBackup TargetTableName ''), 'Invalid
  Backup ARN', 'Invalid TableArn: Invalid Table ARN' (export with TableArn ''); (3) tag texts 'The Tag Key
  provided is invalid, Key: null' (Tags=[{}] / value-only), 'The Tag Value provided is invalid, Value: null'
  (key-only), 'The Tag Key provided is invalid, Key: ' (UntagResource ['']), 'Atleast one Tag Key needs to be
  provided as Input.'; (4) policy texts 'Invalid policy document: Syntax error at position (1,3)' for '{}',
  '...This policy contains invalid Json' for '' and '[]', '...This text appears not to be a policy' for
  'null', '...Missing required field Effect' for Statement [{}], '...Could not parse the policy: Statement is
  empty!' for Statement []; (5) 'One or more parameter values were invalid: <rule>' incl. 'Unknown
  ProjectionType: null' and the bare prefix with no detail for legacy UpdateGlobalTable ReplicaUpdates=[{}];
  (6) replica/witness texts: ReplicaUpdates Delete RegionName '' -> 'Region is not supported. The latest
  version of global tables are only supported in the following regions: [...]' (double space, 36 regions
  listed), GlobalTableWitnessUpdates [{}] / Create or Delete with RegionName '' -> counted as ABSENT ('At
  least one of ... is required'). Accepted degenerates: ListTables works normally; GSI-level
  WarmThroughput/OnDemandThroughput {} at CreateTable; OnDemandThroughput {-1,-1} on a GSI.
  - ACK: terminal_codes, none · ops: UpdateContinuousBackups, UpdateTimeToLive, TagResource, UntagResource,
    PutResourcePolicy, DeleteResourcePolicy, UpdateContributorInsights, DescribeContributorInsights,
    EnableKinesisStreamingDestination, DescribeBackup, ListTables, ListBackups, CreateBackup, DescribeTable,
    DescribeGlobalTable, UpdateGlobalTable, UpdateGlobalTableSettings, ExportTableToPointInTime, UpdateTable,
    CreateTable, RestoreTableFromBackup
  - repro: see probe.py groups G1-G4; every call is one request against an idle PAY_PER_REQUEST table (or a
    table with one GSI / one backup)
  - handling: not handled in the controller (as of commit 34b85e6)
  - related: [DDB-TABLE-437](table-throughput-billing.md#ddb-table-437), [DDB-TABLE-107](#ddb-table-107), [DDB-TABLE-356](table-policy-kinesis-autoscaling.md#ddb-table-356), [DDB-TABLE-223](table-replicas.md#ddb-table-223), [DDB-TABLE-463](table-restore.md#ddb-table-463), [DDB-TABLE-276](table-restore.md#ddb-table-276),
    [DDB-TABLE-461](table-streams-encryption-class.md#ddb-table-461), [DDB-TABLE-219](table-restore.md#ddb-table-219) · evidence: table/creative/degenerate-5xx-hunt
  - notes: Full per-request table (label -> code 'message'): create_backup_empty_name -> 400
    ValidationException '2 validation errors detected: Value '' at 'backupName' failed to satisfy constraint:
    Member must satisfy regular expression pattern: [a-zA-Z0-9_.-]+; Value '' at 'backupName' failed to
    satisfy...
  - full notes: [details/DDB-TABLE-457.md](details/DDB-TABLE-457.md)

### Response fidelity and consistency

- <a id="ddb-table-075"></a>**DDB-TABLE-075** `eventual-consistency` · impact high · handled · verified 2026-10-08
  **DescribeTable right after CreateTable never returned ResourceNotFoundException (0/10 creates, 100 ms polling); first 200 within 90 ms** (hypothesis refuted; behavior confirmed)
  Across 10 CreateTable calls (PAY_PER_REQUEST, single key, no indexes), DescribeTable polled every 100 ms for
  5 s from the moment the create response arrived never returned ResourceNotFoundException; the first
  DescribeTable succeeded 7-83 ms after the create response (p50 49 ms) and already reported
  TableStatus=CREATING. CREATING lasted 1.0-2.0 s (p50 2.0 s) for these tables. CreateTable's own response
  already carries TableStatus=CREATING and the TableArn.
  - ACK: requeue, exceptions.404 · ops: CreateTable, DescribeTable
  - repro: CreateTable; DescribeTable in a 100ms loop for 5s; repeat 10x
  - measurements: create_samples=10, rnf_polls=0, samples_with_rnf=0, first_describe_ok_p50_s=0.049,
    first_describe_ok_max_s=0.083, creating_p50_s=2.02, creating_max_s=2.02
  - handling: handled via `test/e2e/table.py:240-269; b323c3d`
  - related: [DDB-TABLE-076](table.md#ddb-table-076), [DDB-TABLE-181](table.md#ddb-table-181) · evidence: table/consistency-windows/create-delete-visibility
  - notes: Refutes H-T-050 at n=10 for simple tables: the documented 'DescribeTable immediately after
    CreateTable might return ResourceNotFoundException' window was not observable even at 100 ms resolution.
    Treating a 404 right after create as 'not yet' is still cheap insurance, but a controller does not need...
  - full notes: [details/DDB-TABLE-075.md](details/DDB-TABLE-075.md)

- <a id="ddb-table-102"></a>**DDB-TABLE-102** `eventual-consistency` · impact high · handled · verified 2026-10-08
  **TagResource/UntagResource read-after-write lag in ListTagsOfResource: p50 ~1.9 s, max 3.0 s; the previous set is returned meanwhile**
  On an ACTIVE table, after TagResource returned 200 the new key became visible in ListTagsOfResource (polled
  every 0.3 s) after min 1.58 / p50 1.88 / max 2.98 s (n=5); every trial returned the previous tag set at
  least once before the change showed (never an empty set, never an error). After UntagResource (200) the
  removed key disappeared after min 1.27 / p50 2.19 / max 2.44 s (n=5), again with stale reads in between. The
  same ~2 s lag applies to tags set on a table that just became ACTIVE (1.86 s).
  - ACK: tags.custom-sync, requeue · ops: TagResource, UntagResource, ListTagsOfResource
  - repro: TagResource {k:v}; poll ListTagsOfResource every 0.3 s until k present; UntagResource [k]; poll
    until absent; 5 trials each
  - measurements: tag_lag_p50_s=1.884, tag_lag_max_s=2.979, untag_lag_p50_s=2.192, untag_lag_max_s=2.439,
    stale_reads_tag=5, stale_reads_untag=5
  - handling: handled via `pkg/resource/table/hooks_tags.go:27-73; pkg/resource/table/hooks_tags.go:109-136; test/e2e/tests/test_table.py:277-349; e953ae5`
  - related: [DDB-TABLE-103](#ddb-table-103), [DDB-TABLE-105](#ddb-table-105), [DDB-TABLE-108](#ddb-table-108) · evidence: table/tags/state-gating-and-lag
  - notes: Confirms the lag part of H-T-051/H-T-132 (untag lag is not worse than tag lag). A reconciler that
    reads tags back immediately after writing will see the old set and must not re-issue the write (which
    would hit the per-table lock, see the LimitExceededException finding).

### Tags

- <a id="ddb-backup-013"></a>**DDB-BACKUP-013** `tag-semantics` · impact medium · handled · verified 2026-10-09
  **Backups are not taggable: TagResource/ListTagsOfResource/UntagResource with a BackupArn fail with ValidationException**
  TagResource, ListTagsOfResource and UntagResource with a backup ARN all returned ValidationException (HTTP
  400, 'One or more parameter values were invalid: Provided Arn is not a DynamoDB resource arn: <backup
  arn>'). DescribeBackup output has no Tags member and does not record the source table's tags.
  - ACK: tags.ignore, scope:field-on-parent · ops: TagResource, ListTagsOfResource, UntagResource · fields:
    ResourceArn
  - repro: CreateBackup -> TagResource(ResourceArn=<backup arn>)
  - handling: handled via `generator.yaml:148-149; 589cde3`
  - related: [DDB-BACKUP-009](backup.md#ddb-backup-009), [DDB-BACKUP-015](backup.md#ddb-backup-015) · hypotheses: H-B-015 · evidence: backup/identity/arn-list-filters
  - notes: H-B-015 confirmed: a Backup CRD must not expose spec.tags / the generic tag sync.

- <a id="ddb-table-105"></a>**DDB-TABLE-105** `tag-semantics` · impact medium · handled · verified 2026-10-08
  **TagResource upserts; UntagResource of an absent key is a 200 no-op; 'aws:' prefix (any case) rejected even on UntagResource**
  TagResource {k:v1} then {k:v2} both return 200 and ListTagsOfResource shows v2 (1.02 s later). UntagResource
  with a key that was never set -> 200; UntagResource mixing an absent and a present key -> 200 and the
  present key is removed (True). 'aws:'-prefixed key -> ValidationException 'Tag Key cannot be prefixed with
  aws:, Key: aws:foo'; 'AWS:foo' -> ValidationException 'Tag Key cannot be prefixed with aws:, Key: AWS:foo';
  'myaws:foo' -> 200; UntagResource ['aws:foo'] -> ValidationException 'Tag Key cannot be prefixed with aws:,
  Key: aws:foo'.
  - ACK: tags.custom-sync, tags.ignore · ops: TagResource, UntagResource · fields: Tags, TagKeys
  - repro: TagResource {k:v1}; TagResource {k:v2}; ListTagsOfResource; UntagResource [absent]; TagResource
    {aws:foo:x}
  - handling: handled via `pkg/resource/table/hooks_tags.go:27-73; pkg/resource/table/hooks_tags.go:109-136`
  - related: [DDB-TABLE-102](#ddb-table-102), [DDB-TABLE-103](#ddb-table-103), [DDB-TABLE-108](#ddb-table-108), [DDB-TABLE-106](#ddb-table-106), [DDB-TABLE-107](#ddb-table-107), [DDB-TABLE-109](#ddb-table-109),
    [DDB-TABLE-110](table.md#ddb-table-110), [DDB-TABLE-368](#ddb-table-368), [DDB-TABLE-372](#ddb-table-372) · evidence: table/tags/validation-upsert-limits
  - notes: Confirms H-T-062. Note the asymmetry: UntagResource of a nonexistent user key is silently accepted,
    but UntagResource of an aws:-prefixed key is a ValidationException, so a controller must filter aws:* keys
    from both the add and the remove set.

- <a id="ddb-table-106"></a>**DDB-TABLE-106** `tag-semantics` · impact medium · unhandled (not handled in controller) · verified 2026-10-08
  **Empty tag values round-trip as Value=''; keys are case-sensitive; Unicode letters and leading spaces accepted, '#' rejected**
  TagResource [{Env:''},{env:'x'}] -> 200; ListTagsOfResource then returns both keys (True) with Env's value
  ''. Allowed characters: key 'sp ace' value 'a b+c-d=e.f_g:h/i@j' -> 200; unicode key/value -> 200; '#' in
  key -> ValidationException 'The Tag Key provided is invalid, Key: bad#char'; '#' in value ->
  ValidationException 'The Tag Value provided is invalid, Value: bad#char'; leading space in key -> 200.
  - ACK: tags.custom-sync, compare.nil_equals_zero_value · ops: TagResource, ListTagsOfResource · fields: Tags
  - repro: TagResource [{Key:'Env',Value:''},{Key:'env',Value:'x'}]; ListTagsOfResource
  - handling: not handled in the controller (as of commit 34b85e6)
  - related: [DDB-TABLE-105](#ddb-table-105), [DDB-TABLE-107](#ddb-table-107), [DDB-TABLE-108](#ddb-table-108), [DDB-TABLE-109](#ddb-table-109), [DDB-TABLE-110](table.md#ddb-table-110), [DDB-TABLE-368](#ddb-table-368),
    [DDB-TABLE-372](#ddb-table-372) · evidence: table/tags/validation-upsert-limits
  - notes: Confirms H-T-063. Cyrillic key and value were accepted (the documented 'letters' are Unicode
    letters). A controller must not trim or lowercase keys, nor drop empty values.

### Delete semantics

- <a id="ddb-table-374"></a>**DDB-TABLE-374** `delete-semantics` · impact high · unhandled (not handled in controller) · verified 2026-10-09
  **DELETING timeline per API: changed policy Put ResourceInUse from +0.03 s; identical re-Put/CreateBackup 200 until ~1.6 s; backends 404 first**
  One table per API; DeleteTable, then the API every 200 ms with DescribeTable in the same slot. Format:
  outcome [TableStatus seen] from-to s x calls. PutResourcePolicy with a NEW document, no prior policy, trial
  1: gone at 6.03s; ResourceInUseException HTTP 400 [DELETING] 0.03-4.63s x24 | ResourceNotFoundException HTTP
  400 [DELETING] 4.83-5.83s x6 | ResourceNotFoundException HTTP 400 [ERR:ResourceNotFoundException] 6.03-7.03s
  x6. Trial 2: gone at 7.43s; ResourceInUseException HTTP 400 [DELETING] 0.03-6.03s x31 |
  ResourceNotFoundException HTTP 400 [DELETING] 6.23-7.23s x6 | ResourceNotFoundException HTTP 400
  [ERR:ResourceNotFoundException] 7.43-8.43s x6. New document over an existing policy: gone at 5.64s;
  ResourceInUseException HTTP 400 [DELETING] 0.04-4.24s x22 | ResourceNotFoundException HTTP 400 [DELETING]
  4.44-5.44s x6 | ResourceNotFoundException HTTP 400 [ERR:ResourceNotFoundException] 5.64-6.64s x6. IDENTICAL
  re-Put of the existing document: gone at 4.83s; OK [DELETING] 0.03-1.63s x9 | ResourceInUseException HTTP
  400 [DELETING] 1.83-4.63s x15 | ResourceNotFoundException HTTP 400 [ERR:ResourceNotFoundException]
  4.83-5.83s x6. CreateBackup: gone at 5.63s; OK [DELETING] 0.03-1.63s x9 | TableNotFoundException HTTP 400
  [DELETING] 1.83-5.43s x19 | TableNotFoundException HTTP 400 [ERR:ResourceNotFoundException] 5.63-6.63s x6.
  TagResource re-sending the existing key/value: gone at 5.03s; OK [DELETING] 0.03-1.03s x6 |
  ResourceNotFoundException HTTP 400 [DELETING] 1.23-4.83s x19 | ResourceNotFoundException HTTP 400
  [ERR:ResourceNotFoundException] 5.03-6.03s x6. TagResource with a changing value: gone at 5.83s;
  ResourceInUseException HTTP 400 [DELETING] 0.03-1.63s x9 | ResourceNotFoundException HTTP 400 [DELETING]
  1.83-5.63s x20 | ResourceNotFoundException HTTP 400 [ERR:ResourceNotFoundException] 5.83-6.83s x6. Messages:
  policy {'ResourceInUseException': 'Attempt to change a resource which is still in use: Table is being
  deleted: ackq-53f6bd-del-p1', 'ResourceNotFoundException': 'Requested resource not found: Table:
  ackq-53f6bd-del-p1 not found'}; backup {'TableNotFoundException': 'Table not found: ackq-53f6bd-del-b'};
  identical tag {'ResourceNotFoundException': 'Requested resource not found: ResourceArn:
  arn:aws:dynamodb:us-west-2:<ACCOUNT>:table/ackq-53f6bd-del-t not found'}; changing tag
  {'ResourceInUseException': 'Attempt to change a resource which is still in use: Table is being deleted:
  ackq-53f6bd-del-t2', 'ResourceNotFoundException': 'Requested resource not found: ResourceArn:
  arn:aws:dynamodb:us-west-2:<ACCOUNT>:table/ackq-53f6bd-del-t2 not found'}. After the tables were gone (first
  run): GetResourcePolicy on p1/p1b/p2 -> ResourceNotFoundException 'Requested resource not found: Table: X
  not found'; the 9 backups created in the first 1.6 s of B's DELETING were AVAILABLE and deletable
  (DeleteBackup 9x 200). The second run re-created the p1/p1b/p2 names: GetResourcePolicy then returned
  PolicyNotFoundException (no policy leaked from the deleted incarnation).
  - ACK: deletable.when, pre-delete-cleanup, requeue · ops: DeleteTable, PutResourcePolicy, CreateBackup,
    TagResource, DescribeTable, GetResourcePolicy
  - repro: PPR table; DeleteTable; loop every 200 ms: <API> + DescribeTable until ResourceNotFoundException +1
    s; one table per API variant (new/identical policy doc, CreateBackup, same/changing tag)
  - measurements: p1_resource_in_use_s=[0.03, 4.63], p1_rnf_from_s=4.83, p1_table_gone_s=6.03,
    p1b_resource_in_use_s=[0.03, 6.03], p1b_table_gone_s=7.43, p2_resource_in_use_s=[0.04, 4.24],
    p2_table_gone_s=5.64, p3_identical_put_ok_s=[0.03, 1.63], p3_table_gone_s=4.83, b_backup_ok_s=[0.03,
    1.63], b_table_not_found_from_s=1.83, b_table_gone_s=5.63, t_identical_tag_ok_s=[0.03, 1.03],
    t_rnf_from_s=1.23, t_table_gone_s=5.03, t2_changing_tag_ok_s=null, t2_resource_in_use_s=[0.03, 1.63],
    t2_table_gone_s=5.83, backups_created_during_deleting=9
  - handling: not handled in the controller (as of commit 34b85e6)
  - related: [DDB-TABLE-122](table-policy-kinesis-autoscaling.md#ddb-table-122), [DDB-TABLE-236](table-policy-kinesis-autoscaling.md#ddb-table-236), [DDB-TABLE-104](table.md#ddb-table-104), [DDB-TABLE-118](#ddb-table-118), [DDB-BACKUP-001](backup.md#ddb-backup-001), [DDB-BACKUP-007](backup.md#ddb-backup-007),
    [DDB-TABLE-207](table-policy-kinesis-autoscaling.md#ddb-table-207), [DDB-TABLE-097](table-subresources.md#ddb-table-097), [DDB-TABLE-453](table-subresources.md#ddb-table-453), [DDB-TABLE-088](table-subresources.md#ddb-table-088), [DDB-TABLE-235](table-policy-kinesis-autoscaling.md#ddb-table-235), [DDB-TABLE-342](table-subresources.md#ddb-table-342), [DDB-TABLE-214](table-policy-kinesis-autoscaling.md#ddb-table-214),
    [DDB-TABLE-271](table-policy-kinesis-autoscaling.md#ddb-table-271), [DDB-TABLE-436](table-policy-kinesis-autoscaling.md#ddb-table-436), [DDB-TABLE-377](#ddb-table-377) · hypotheses: H-S-120, H-B-005, H-T-064 · evidence:
    table/consistency-windows/deleting-admissibility-timeline
  - notes: Reconciles [DDB-TABLE-122](table-policy-kinesis-autoscaling.md#ddb-table-122) (Put 200 during DELETING) with [DDB-TABLE-236](table-policy-kinesis-autoscaling.md#ddb-table-236) (Put ->
    ResourceInUseException): a Put that CHANGES the policy is rejected with 'Table is being deleted' from the
    first 200 ms slot in 3/3 trials, while an IDENTICAL re-Put (RevisionId no-op) returns 200 for the first
    ~1.6 s (P3)...
  - full notes: [details/DDB-TABLE-374.md](details/DDB-TABLE-374.md)

- <a id="ddb-table-455"></a>**DDB-TABLE-455** `delete-semantics` · impact medium · unhandled (not handled in controller) · verified 2026-10-09
  **DELETING phase map: TTL/Insights/Kinesis Enable accept once in the first second, each backend forgets the table at its own time**
  One table per API, DeleteTable then the API every 200 ms (DescribeTable in the same slot; tables gone at
  3.8-5.8 s). UpdateTimeToLive(enable): 200 at +0.04 s, then ValidationException 'TimeToLive is already
  enabled' until the table is gone (+5.44 s), then ResourceNotFoundException.
  UpdateContributorInsights(ENABLE): 200 ENABLING from +0.03 to +1.43 s (8 calls), then ValidationException
  'Table or Index is not in a valid state to update Key Access Insights: TableStatus must be ACTIVE to enable
  ContributorInsights.' until gone, then ResourceNotFoundException; no CloudWatch insight rule existed at any
  of 7 checks over 60 s after deletion and ListContributorInsights had no ghost.
  EnableKinesisStreamingDestination on a table WITHOUT a destination: 200 ENABLING at +0.03 s only, then
  ValidationException '...must be DISABLED or ENABLE_FAILED to perform' (the just-accepted entry blocks
  re-enable) until gone, then ResourceNotFoundException; DescribeKinesisStreamingDestination after deletion ->
  ResourceNotFoundException. UpdateTable(DeletionProtection / stream / WarmThroughput): ResourceInUseException
  'Table is being deleted' for the whole DELETING window, then ResourceNotFoundException.
  UpdateTableReplicaAutoScaling (Min/Max only): ValidationException "Parameters 'ScalingPolicyUpdate' are
  required unless auto scaling is being disabled" both while DELETING and after the table is gone (shape
  validation precedes the existence check). Reads: ListTagsOfResource 200 until +1.23 s then
  ResourceNotFoundException; GetResourcePolicy 200 (old RevisionId) until +5.23 s, PolicyNotFoundException at
  +5.43 s (one slot), ResourceNotFoundException from +5.63 s; DescribeContinuousBackups 200 until +1.03 s then
  TableNotFoundException; DescribeContributorInsights 200 DISABLED until gone. Same-name re-create of the
  ttl/ins/pitr/kin tables: TTL DISABLED, insights DISABLED, PITR DISABLED, no Kinesis destination - nothing
  leaked.
  - ACK: deletable.when, pre-delete-cleanup, requeue, exceptions.404 · ops: UpdateTimeToLive,
    UpdateContributorInsights, EnableKinesisStreamingDestination, UpdateTable, UpdateTableReplicaAutoScaling,
    ListTagsOfResource, GetResourcePolicy, DescribeContinuousBackups, DescribeContributorInsights
  - repro: DeleteTable; call the API every 200 ms until 1 s after DescribeTable returns
    ResourceNotFoundException; then describe-insight-rules / re-create the name
  - measurements: ttl_ok_until_s=0.04, insights_ok_until_s=1.43, kinesis_ok_until_s=0.03,
    list_tags_ok_until_s=1.23, get_policy_ok_until_s=5.23, policy_not_found_at_s=5.43,
    describe_cb_ok_until_s=1.03, table_gone_s=[5.64, 5.43, 5.23, 3.83, 5.04, 5.83, 5.63]
  - handling: not handled in the controller (as of commit 34b85e6)
  - related: [DDB-TABLE-374](#ddb-table-374), [DDB-TABLE-118](#ddb-table-118), [DDB-TABLE-236](table-policy-kinesis-autoscaling.md#ddb-table-236), [DDB-TABLE-104](table.md#ddb-table-104), [DDB-TABLE-122](table-policy-kinesis-autoscaling.md#ddb-table-122), [DDB-TABLE-342](table-subresources.md#ddb-table-342),
    [DDB-BACKUP-001](backup.md#ddb-backup-001), [DDB-BACKUP-007](backup.md#ddb-backup-007), [DDB-TABLE-100](table-restore.md#ddb-table-100), [DDB-TABLE-087](table-restore.md#ddb-table-087), [DDB-TABLE-454](table-subresources.md#ddb-table-454), [DDB-TABLE-227](table-replicas.md#ddb-table-227), [DDB-TABLE-228](table-replicas.md#ddb-table-228)
    · evidence: table/creative/deleting-lasting-effects
  - notes: Extends [DDB-TABLE-374](#ddb-table-374) (policy/backup/tag) to the remaining APIs and [DDB-TABLE-236](table-policy-kinesis-autoscaling.md#ddb-table-236) (whose table had
    an ACTIVE destination, hence ValidationException) with the no-destination case where Enable is accepted.
    The transient PolicyNotFoundException just before the 404 could be misread by a policy...
  - full notes: [details/DDB-TABLE-455.md](details/DDB-TABLE-455.md)

### Quotas and rate limits

- <a id="ddb-table-053"></a>**DDB-TABLE-053** `quota-limit` · impact medium · unhandled (not handled in controller) · verified 2026-10-08
  **Account-level control-plane rate limit surfaces as ThrottlingException (HTTP 400) on UpdateTable at ~1 call/s across concurrent clients**
  During the OnDemandThroughput sequence two UpdateTable calls failed with ThrottlingException (HTTP 400):
  'The rate of control plane requests made by this account is too high' while this probe issued ~1
  UpdateTable/s and other probes ran in the same account. The same code is used for the per-table
  DeletionProtection cooldown, so ThrottlingException on UpdateTable has two distinct causes.
  - ACK: requeue, terminal_codes · ops: UpdateTable
  - repro: Issue UpdateTable calls at ~1/s while other control-plane traffic is active in the account
  - handling: not handled in the controller (as of commit 34b85e6)
  - related: [DDB-TABLE-003](table-streams-encryption-class.md#ddb-table-003), [DDB-TABLE-176](table-streams-encryption-class.md#ddb-table-176), [DDB-TABLE-017](table-streams-encryption-class.md#ddb-table-017), [DDB-TABLE-438](table-streams-encryption-class.md#ddb-table-438), [DDB-TABLE-130](#ddb-table-130), [DDB-TABLE-132](#ddb-table-132),
    [DDB-TABLE-441](table.md#ddb-table-441), [DDB-TABLE-131](table.md#ddb-table-131), [DDB-TABLE-051](table-throughput-billing.md#ddb-table-051), [DDB-TABLE-178](table-throughput-billing.md#ddb-table-178), [DDB-TABLE-182](table-throughput-billing.md#ddb-table-182) · evidence:
    table/mutation-matrix/stream-protection-throughput
  - notes: The two throttled calls (odt-partial-read-2000, odt-both-minus1-again) were re-run successfully in
    table/response-fidelity/create-update-response.
  - full notes: [details/DDB-TABLE-053.md](details/DDB-TABLE-053.md)

- <a id="ddb-table-099"></a>**DDB-TABLE-099** `quota-limit` · impact medium · handled · verified 2026-10-08
  **15 concurrent DescribeContinuousBackups/DescribeTimeToLive on fresh connections all pass; 'Rate exceeded' is per-connection, not shared load**
  Burst of 15 concurrent DescribeContinuousBackups with SDK retries disabled completed in 134 ms, all 15 HTTP
  200; a single call 2 s later also 200. Same for 15 concurrent DescribeTimeToLive (52 ms, 15x200). Earlier in
  the session, with several probes polling in the same account, a 1/s DescribeTimeToLive poll loop received
  ThrottlingException (HTTP 400) on single calls (table/mutation-matrix/ttl-updates, t3/t5 timelines), so the
  limit is account-wide and bursty rather than a hard per-caller 10/s.
  - ACK: requeue, runtime-gap · ops: DescribeContinuousBackups, DescribeTimeToLive
  - repro: 15 threads x 1 DescribeContinuousBackups with max_attempts=1
  - measurements: burst_span_ms=134, throttled_count_cb=0, throttled_count_ttl=0
  - handling: handled via `templates/hooks/table/sdk_read_one_post_set_output.go.tpl:57-72; bcd26e1`
  - related: [DDB-TABLE-360](table-subresources.md#ddb-table-360), [DDB-TABLE-403](#ddb-table-403), [DDB-TABLE-445](#ddb-table-445) · evidence: table/error-taxonomy/subresource-errors
  - notes: Contradiction with [DDB-TABLE-360](table-subresources.md#ddb-table-360): 099 says 15 concurrent DescribeTimeToLive all pass and
    ThrottlingException appears only under shared account load; 360 shows 30 sequential DescribeTimeToLive on
    one table throttle from call #4 ('Rate exceeded') while 30 concurrent pass. [DDB-TABLE-403](#ddb-table-403) reconciles:
    the...
  - full notes: [details/DDB-TABLE-099.md](details/DDB-TABLE-099.md)

- <a id="ddb-table-103"></a>**DDB-TABLE-103** `quota-limit` · impact high · SUSPECTED CONTROLLER BUG · verified 2026-10-08
  **Per-table tag write lock: 2nd TagResource/UntagResource within ~1-3 s -> LimitExceededException 'Table tags are being updated'**
  Any TagResource or UntagResource issued right after a successful TagResource/UntagResource on the same table
  is rejected with LimitExceededException (HTTP 400) 'Subscriber limit exceeded: Table tags are being updated:
  <name>' - 15/15 trials here (each trial even included botocore's one implicit retry ~1 s later, which also
  failed). Retried every 100 ms the second write was accepted after p50 1.6-2.4 s, max 3.4 s from the first
  call (tag->tag 1.1-3.1 s; tag->untag 2.0-3.4 s; untag->tag 1.4-2.4 s). Measured with retries fully disabled
  in table/tags/write-lock-characterization, the lock is held for ~1.6-1.8 s after the write returns and is
  released when ListTagsOfResource reflects the change. DeleteTable issued right after TagResource ->
  ResourceInUseException 'Attempt to change a resource which is still in use: Table tags are being updated:
  <name>', accepted after 1.7-1.8 s. UpdateTable (DeletionProtectionEnabled) right after TagResource is NOT
  blocked (200), and TagResource right after UpdateTable is not blocked either. The lock also explains 'lost'
  untag+retag sequences: the re-tag is rejected synchronously, leaving the key missing (5/5 trials).
  - ACK: tags.custom-sync, requeue, one-per-reconcile · ops: TagResource, UntagResource, DeleteTable,
    UpdateTable
  - repro: TagResource {a:1}; immediately TagResource {b:1} (boto3 Config retries max_attempts=1); repeat
    every 100 ms until 200. Also TagResource then DeleteTable.
  - measurements: lock_window_p50_s=1.58, lock_window_max_s=3.366, delete_after_tag_blocked_s=1.796,
    untag_then_retag_lost=5
  - handling: suspected controller bug - see Handling gaps
  - related: [DDB-TABLE-006](table.md#ddb-table-006), [DDB-TABLE-063](table-throughput-billing.md#ddb-table-063), [DDB-TABLE-381](table-throughput-billing.md#ddb-table-381), [DDB-TABLE-005](table.md#ddb-table-005), [DDB-TABLE-101](#ddb-table-101), [DDB-TABLE-104](table.md#ddb-table-104),
    [DDB-TABLE-102](#ddb-table-102), [DDB-TABLE-105](#ddb-table-105), [DDB-TABLE-108](#ddb-table-108), [DDB-TABLE-173](#ddb-table-173), [DDB-TABLE-440](table.md#ddb-table-440), [DDB-TABLE-441](table.md#ddb-table-441), [DDB-TABLE-130](#ddb-table-130) ·
    evidence: table/tags/state-gating-and-lag
  - notes: Not the documented 5/s account rate limit: it is per table and fires on the 2nd call. botocore and
    aws-sdk-go-v2 standard retry modes classify LimitExceededException as a throttle and retry with short
    jittered backoff, which does NOT reliably outlast the ~1.7 s lock (see the follow-up finding: 3...
  - full notes: [details/DDB-TABLE-103.md](details/DDB-TABLE-103.md)

- <a id="ddb-table-108"></a>**DDB-TABLE-108** `quota-limit` · impact medium · unhandled (not handled in controller) · verified 2026-10-08
  **50-tag limit is a ValidationException (not LimitExceededException), checked against the post-merge set; no NextToken at 50 tags**
  With 50 tags present: TagResource {k51} -> ValidationException 'Number of Tags exceed the current limit for
  the provided ResourceArn'. {k01:new,k51} (post-merge 51) -> ValidationException 'Number of Tags exceed the
  current limit for the provided ResourceArn'. {k01:new} alone -> 200. All 50 existing keys with new values in
  one call -> 200 (k01 became v01b). 50 new distinct keys in one call -> ValidationException 'Number of Tags
  exceed the current limit for the provided ResourceArn'; 51 tags in one call -> ValidationException 'Number
  of Tags exceed the current limit for the provided ResourceArn'. TagResource of a 51st key immediately
  (<100ms) after UntagResource of another key -> LimitExceededException 'Subscriber limit exceeded: Table tags
  are being updated: ackq-202220-tgv'; after the untag became visible -> 200. ListTagsOfResource at 50 tags
  returned 50 tags with NextToken present=False. 49 tags in one TagResource call were visible after 1.53 s.
  - ACK: tags.custom-sync, terminal_codes · ops: TagResource, ListTagsOfResource, UntagResource · fields:
    Tags, NextToken
  - repro: TagResource with 49 tags; then the listed single-call variants
  - measurements: fifty_tags_visible_lag_s=1.53
  - handling: not handled in the controller (as of commit 34b85e6)
  - related: [DDB-TABLE-102](#ddb-table-102), [DDB-TABLE-103](#ddb-table-103), [DDB-TABLE-105](#ddb-table-105), [DDB-TABLE-173](#ddb-table-173), [DDB-TABLE-440](table.md#ddb-table-440), [DDB-TABLE-441](table.md#ddb-table-441),
    [DDB-TABLE-130](#ddb-table-130), [DDB-TABLE-104](table.md#ddb-table-104), [DDB-TABLE-106](#ddb-table-106), [DDB-TABLE-107](#ddb-table-107), [DDB-TABLE-109](#ddb-table-109), [DDB-TABLE-110](table.md#ddb-table-110), [DDB-TABLE-368](#ddb-table-368),
    [DDB-TABLE-372](#ddb-table-372), [DDB-TABLE-073](table.md#ddb-table-073) · evidence: table/tags/validation-upsert-limits
  - notes: Refutes the LimitExceededException part of H-T-065/H-T-133/H-T-137: the count limit surfaces as
    ValidationException 'Number of Tags exceed the current limit for the provided ResourceArn' (HTTP 400).
    Confirms the post-merge check of H-T-137 (upserting existing keys at 50 tags is fine; any new key is...
  - full notes: [details/DDB-TABLE-108.md](details/DDB-TABLE-108.md)

- <a id="ddb-table-130"></a>**DDB-TABLE-130** `quota-limit` · impact high · unhandled (not handled in controller) · verified 2026-10-08
  **Tag API bursts: account rate limit surfaces as ThrottlingException 'rate of control plane requests ... too high'; ListTags 15/s fine**
  With SDK retries disabled, 15 concurrent TagResource calls over 10 ACTIVE tables (fired within 1.1 s)
  returned 10x 200, 1x ThrottlingException (HTTP 400, 'The rate of control plane requests made by this account
  is too high') on a table that received only one call, and 4x LimitExceededException 'Subscriber limit
  exceeded: Table tags are being updated: <name>' on the tables that received two calls (the per-table tag
  lock, not a rate limit). 15 concurrent UntagResource: 10x 200 + 5x LimitExceededException (all on doubly-hit
  tables), no ThrottlingException. 15 concurrent ListTagsOfResource (nominal limit 10/s): 15x 200 in 26 ms.
  Every rejected call succeeded when retried sequentially 1 s later. Successful burst calls had latencies of
  0.1-1.1 s.
  - ACK: requeue, tags.custom-sync, terminal_codes · ops: TagResource, ListTagsOfResource, UntagResource
  - repro: 10 ACTIVE tables; ThreadPoolExecutor(15) firing TagResource with distinct keys; repeat for
    ListTagsOfResource and UntagResource
  - measurements: tag_burst_rejected=5, list_burst_rejected=0, untag_burst_rejected=5, tag_burst_span_ms=1105,
    tag_burst_latency_max_ms=1095, throttling_exception_count=1, per_table_lock_count_tag=4,
    per_table_lock_count_untag=5
  - handling: not handled in the controller (as of commit 34b85e6)
  - related: [DDB-TABLE-053](#ddb-table-053), [DDB-TABLE-132](#ddb-table-132), [DDB-TABLE-441](table.md#ddb-table-441), [DDB-TABLE-131](table.md#ddb-table-131), [DDB-TABLE-051](table-throughput-billing.md#ddb-table-051), [DDB-TABLE-103](#ddb-table-103),
    [DDB-TABLE-173](#ddb-table-173), [DDB-TABLE-440](table.md#ddb-table-440), [DDB-TABLE-104](table.md#ddb-table-104), [DDB-TABLE-108](#ddb-table-108) · evidence: table/limits/rate-bursts
  - notes: Confirms the ThrottlingException-not-LimitExceededException part of H-T-131 for the account-wide
    control-plane rate; the message ('The rate of control plane requests made by this account is too high')
    differs from the text hypothesised. CAVEAT: this burst ran with botocore max_attempts=1, which...
  - full notes: [details/DDB-TABLE-130.md](details/DDB-TABLE-130.md)

- <a id="ddb-table-132"></a>**DDB-TABLE-132** `quota-limit` · impact medium · unhandled (not handled in controller) · verified 2026-10-08
  **10 concurrent CreateTable or DeleteTable (no indexes): 4/10 get ThrottlingException (account control-plane rate); 50 DescribeTable fine**
  With SDK retries fully disabled (botocore total_max_attempts=1), 10 concurrent CreateTable calls for
  single-key PAY_PER_REQUEST tables (span 185 ms) returned 6x 200 and 4x ThrottlingException (HTTP 400, 'The
  rate of control plane requests made by this account is too high'); 10 concurrent DeleteTable likewise 6x 200
  / 4x ThrottlingException. No LimitExceededException and no ResourceInUseException. With a single SDK retry
  (botocore max_attempts=1, which still retries once) all 10 creates and deletes succeed (first run: 3-4 of 10
  needed the retry). 50 concurrent DescribeTable: 50x 200, max latency 63 ms, in both runs. All 10 tables were
  ACTIVE 6.7-7.7 s after the burst and gone 5.7-6.8 s after the delete burst.
  - ACK: requeue, e2e-timing · ops: CreateTable, DeleteTable, DescribeTable
  - repro: ThreadPoolExecutor(10) CreateTable; ThreadPoolExecutor(50) DescribeTable; ThreadPoolExecutor(10)
    DeleteTable
  - measurements: create_burst_rejected_noretry=4, delete_burst_rejected_noretry=4, describe_burst_rejected=0,
    create_burst_retry_needed_run1=3, all_active_after_s=7.67, all_gone_after_s=5.7
  - handling: not handled in the controller (as of commit 34b85e6)
  - related: [DDB-TABLE-053](#ddb-table-053), [DDB-TABLE-130](#ddb-table-130), [DDB-TABLE-441](table.md#ddb-table-441), [DDB-TABLE-131](table.md#ddb-table-131), [DDB-TABLE-051](table-throughput-billing.md#ddb-table-051) · evidence:
    table/limits/rate-bursts
  - notes: Refines H-T-131: for index-less tables the constraint on concurrent CreateTable is the account-wide
    control-plane rate (ThrottlingException), not a LimitExceededException/serialization rule, and it is cheap
    to ride out with one retry. A controller starting with many Table CRs will see...
  - full notes: [details/DDB-TABLE-132.md](details/DDB-TABLE-132.md)

- <a id="ddb-table-172"></a>**DDB-TABLE-172** `quota-limit` · impact high · unhandled (not handled in controller) · verified 2026-10-09, re-verified
  **Tag write lock: only effective changes take it; held ~1.6-1.8 s until ListTags reflects the change; blocks DeleteTable, no other mutation**
  Measured with SDK retries fully disabled. A no-op UntagResource (absent key) and a no-op TagResource (same
  key and value) do NOT take the lock (an immediately following TagResource is accepted), but a real
  UntagResource does (an immediately following no-op UntagResource -> LimitExceededException 'Subscriber limit
  exceeded: Table tags are being updated'). ListTagsOfResource is never blocked. After a 1-tag TagResource the
  lock was held 1.58 s / 1.80 s, after a 45-tag TagResource 1.80 s (probe: no-op UntagResource every 200 ms);
  the new tags appeared in ListTagsOfResource at the same poll (1.58 s / 1.80 s) or one poll later (2.02 s),
  i.e. the lock releases when the change becomes readable. Mutations issued 50 ms after a TagResource were all
  accepted: UpdateTable (ProvisionedThroughput) 200, UpdateTimeToLive 200, UpdateContinuousBackups 200,
  CreateBackup 200; only DeleteTable is rejected (ResourceInUseException 'Attempt to change a resource which
  is still in use: Table tags are being updated', accepted after 1.7 s). Reverse direction: TagResource right
  after UpdateTable (table UPDATING) -> 200; TagResource right after UpdateTimeToLive -> 200.
  - ACK: tags.custom-sync, requeue, one-per-reconcile · ops: TagResource, UntagResource, UpdateTable,
    UpdateTimeToLive, UpdateContinuousBackups, CreateBackup, DeleteTable
  - repro: TagResource; every 200 ms UntagResource [absent-key] until 200 while polling ListTagsOfResource;
    TagResource then each other mutation after 50 ms
  - measurements: lock_1tag_s=1.577, lock_1tag_again_s=1.799, lock_45tags_s=1.795,
    release_minus_visible_max_s=0.221, delete_after_tag_accepted_s=1.725
  - handling: not handled in the controller (as of commit 34b85e6)
  - related: [DDB-TABLE-348](table-policy-kinesis-autoscaling.md#ddb-table-348), [DDB-TABLE-119](table-streams-encryption-class.md#ddb-table-119), [DDB-TABLE-210](table-policy-kinesis-autoscaling.md#ddb-table-210), [DDB-TABLE-432](table-policy-kinesis-autoscaling.md#ddb-table-432) · evidence:
    table/tags/write-lock-characterization, table/creative/reverify-set-b
  - notes: Follow-up to the per-table tag lock finding. Practical consequences: (1) the controller's tag diff
    must be applied as at most one tag mutation per ~2 s per table; (2) tag writes may be interleaved freely
    with UpdateTable/TTL/PITR/backup calls; (3) a finalizer must tolerate ResourceInUseException...
  - full notes: [details/DDB-TABLE-172.md](details/DDB-TABLE-172.md)

- <a id="ddb-table-173"></a>**DDB-TABLE-173** `quota-limit` · impact high · unhandled (not handled in controller) · verified 2026-10-08
  **With SDK standard retries (3 attempts) a back-to-back TagResource+UntagResource pair still fails 4/5 times (LimitExceededException ~1.7 s)**
  Using botocore standard retry mode with total_max_attempts=3 (same shape as aws-sdk-go-v2's default retryer,
  which also treats LimitExceededException as a throttle), TagResource immediately followed by UntagResource
  on the same table: the second call ended in LimitExceededException 'Table tags are being updated' in 4/5
  runs after exhausting its retries (wall-clock 1.55-1.75 s), and succeeded in 1/5 (after 2 retries, 0.94 s).
  In an earlier run with one extra attempt available the pair succeeded 4/5 times at 1.9-5.3 s wall-clock.
  Unlocked tag calls take 10-50 ms.
  - ACK: tags.custom-sync, requeue, e2e-timing · ops: TagResource, UntagResource
  - repro: boto3 Config(retries={'max_attempts': 3, 'mode': 'standard'}); TagResource then UntagResource
    back-to-back; 5 runs
  - measurements: second_call_wall_ms=[1550, 1736, 1748, 1738, 944], success=1, n=5
  - handling: not handled in the controller (as of commit 34b85e6)
  - related: [DDB-TABLE-103](#ddb-table-103), [DDB-TABLE-440](table.md#ddb-table-440), [DDB-TABLE-441](table.md#ddb-table-441), [DDB-TABLE-130](#ddb-table-130), [DDB-TABLE-104](table.md#ddb-table-104), [DDB-TABLE-108](#ddb-table-108) ·
    evidence: table/tags/write-lock-characterization
  - notes: Shows what a controller using default SDK retries actually experiences: the jittered exponential
    backoff of 3 attempts (~0.1-2 s total) is shorter than the ~1.7 s lock most of the time, so the second tag
    mutation of a reconcile fails outright and must be requeued; each reconcile that needs both an...
  - full notes: [details/DDB-TABLE-173.md](details/DDB-TABLE-173.md)

- <a id="ddb-table-377"></a>**DDB-TABLE-377** `quota-limit` · impact medium · unhandled (not handled in controller) · verified 2026-10-09
  **Per-table quotas/cooldowns die with the table: TableClass, SSE, decrease budget, TTL and DP cooldowns reset on same-name re-create**
  Five tables, each driven into its per-table limit, deleted (DELETING 4.6-6.2 s) and re-created under the
  SAME name (same TableArn, new TableId): TableClass: IA, STD, 3rd -> LimitExceededException 'Updates to
  TableClass are limited to 2 times in 30 day(s)'; re-created table: IA OK, STD OK, 3rd LimitExceeded again
  (fresh budget). SSE: 4 flips OK, 5th -> LimitExceededException 'Encryption mode changes are limited in the
  24h window'; re-created table: flip OK. Provisioned decreases: 4 OK, 5th -> LimitExceededException;
  re-created table shows NumberOfDecreasesToday=0 and a decrease is accepted. TTL: enable OK, immediate
  disable -> ValidationException 'Time to live has been modified multiple times within a fixed interval';
  re-created table accepted TTL enable 11.4 s after the old incarnation's change. DeletionProtection: toggle
  on the re-created table 12.0 s after the old incarnation's toggle -> 200 (no 15 s ThrottlingException).
  WarmThroughput high-water mark (10/10 on the old PROVISIONED table) reads 5/5 on the re-created 5/5 table.
  - ACK: e2e-timing, docs-only · ops: UpdateTable, UpdateTimeToLive, DeleteTable, CreateTable · fields:
    TableClass, SSESpecification, ProvisionedThroughput, TimeToLiveSpecification, DeletionProtectionEnabled,
    WarmThroughput
  - repro: CreateTable X; UpdateTable TableClass=IA; TableClass=STANDARD; TableClass=IA (LimitExceeded);
    DeleteTable X; wait 404; CreateTable X; UpdateTable TableClass=IA -> 200
  - measurements: ttl_recreate_gap_s=11.4, dp_recreate_gap_s=12.0, delete_to_404_s=5.1,
    recreate_to_active_s=6.1
  - handling: not handled in the controller (as of commit 34b85e6)
  - related: [DDB-TABLE-283](table-streams-encryption-class.md#ddb-table-283), [DDB-TABLE-142](table-streams-encryption-class.md#ddb-table-142), [DDB-TABLE-184](table-throughput-billing.md#ddb-table-184), [DDB-TABLE-143](table-subresources.md#ddb-table-143), [DDB-TABLE-003](table-streams-encryption-class.md#ddb-table-003), [DDB-TABLE-061](table-throughput-billing.md#ddb-table-061),
    [DDB-TABLE-013](#ddb-table-013), [DDB-TABLE-214](table-policy-kinesis-autoscaling.md#ddb-table-214), [DDB-TABLE-236](table-policy-kinesis-autoscaling.md#ddb-table-236), [DDB-TABLE-097](table-subresources.md#ddb-table-097), [DDB-TABLE-342](table-subresources.md#ddb-table-342), [DDB-TABLE-374](#ddb-table-374), [DDB-TABLE-271](table-policy-kinesis-autoscaling.md#ddb-table-271),
    [DDB-TABLE-436](table-policy-kinesis-autoscaling.md#ddb-table-436), [DDB-TABLE-144](table-subresources.md#ddb-table-144), [DDB-TABLE-145](table-subresources.md#ddb-table-145), [DDB-TABLE-146](table-subresources.md#ddb-table-146), [DDB-TABLE-147](table-subresources.md#ddb-table-147), [DDB-TABLE-114](#ddb-table-114), [DDB-TABLE-117](table-streams-encryption-class.md#ddb-table-117),
    [DDB-TABLE-464](#ddb-table-464), [DDB-TABLE-445](#ddb-table-445) · evidence: table/creative/name-keyed-cooldowns
  - notes: All budgets are keyed by the table incarnation (TableId), not by TableName/ARN. Consequences for a
    controller: (1) e2e tests that delete and re-create a table name back-to-back never inherit an exhausted
    quota; (2) the ONLY way out of a 30-day TableClass lock, a 31-min TTL cooldown or an exhausted...
  - full notes: [details/DDB-TABLE-377.md](details/DDB-TABLE-377.md)

- <a id="ddb-table-392"></a>**DDB-TABLE-392** `quota-limit` · impact medium · unhandled (not handled in controller) · verified 2026-10-09
  **Doc claim C013 PARTLY: DescribeContinuousBackups '10/s' - 15 concurrent calls all pass; throttling only appeared under shared account load**
  15 concurrent DescribeContinuousBackups (no retries) completed in 134 ms, all HTTP 200, likewise 15
  concurrent DescribeTimeToLive; yet 1/s poll loops in other probes received ThrottlingException (HTTP 400)
  while several probes polled the same account ([DDB-TABLE-099](#ddb-table-099)). The limit is account-wide and bursty rather
  than a hard per-caller 10/s.
  - ACK: requeue · ops: DescribeContinuousBackups
  - handling: not handled in the controller (as of commit 34b85e6)
  - related: [DDB-TABLE-099](#ddb-table-099) · evidence: table/error-taxonomy/subresource-errors, service/static/doc-claims-1
  - notes: VERDICT: PARTLY - no hard 10/s; an account-wide bursty bucket shared by all callers - a controller
    polling many tables must expect ThrottlingException

- <a id="ddb-table-403"></a>**DDB-TABLE-403** `quota-limit` · impact medium · handled · verified 2026-10-09
  **DescribeTimeToLive throttling is per HTTP connection: ~4 calls then 'Rate exceeded' on one connection; fresh connections never; refill ~2 s**
  Sequential, no retries. ttl.same_conn_nogap: 30 (120.5/s) -> {'OK': 4, 'ThrottlingException': 26}, first
  throttle at #4, pattern ....TTTTTTTTTTTTTTTTTTTTTTTTTT. After the throttled burst, polling the SAME
  connection every 0.5 s: first success after 2.06 s (pattern TTTT.TTT.TTT.TT). ttl.fresh_conn_nogap: 30
  (27.1/s) -> {'OK': 30}, first throttle at #30, pattern ...............................
  ttl.same_conn_10_per_s: 30 (9.2/s) -> {'OK': 5, 'ThrottlingException': 25}, first throttle at #4, pattern
  ....TTTTTTTTTTTTTTT.TTTTTTTTTT. ttl.same_conn_4_per_s: 20 (3.8/s) -> {'OK': 6, 'ThrottlingException': 14},
  first throttle at #4, pattern ....TTTTTTT.TTTTTTT.. Comparison on one connection, no gap:
  cb.same_conn_nogap: 30 (79.6/s) -> {'OK': 13, 'ThrottlingException': 17}, first throttle at #11, pattern
  ...........TTTTTTTTTTT..TTTTTT; tags.same_conn_nogap_100: 100 (184.2/s) -> {'OK': 100}, first throttle at
  #100, pattern
  ..................................................................................................... 30
  concurrent DescribeTimeToLive sharing one client/pool: {'OK': 30}.
  - ACK: requeue, e2e-timing · ops: DescribeTimeToLive, DescribeContinuousBackups, ListTagsOfResource
  - repro: one boto3 client (keep-alive): 30x DescribeTimeToLive back-to-back; then poll every 0.5 s; then 30x
    with a new client per call; then paced 10/s and 4/s
  - measurements: ttl_same_conn_ok_before_throttle=4, ttl_refill_first_ok_s=2.06, ttl_fresh_conn_throttled=0,
    ttl_10_per_s_throttled=25, ttl_4_per_s_throttled=14, cb_same_conn_throttled=17
  - handling: handled via `templates/hooks/table/sdk_read_one_post_set_output.go.tpl:57-72; bcd26e1`
  - related: [DDB-TABLE-099](#ddb-table-099), [DDB-TABLE-360](table-subresources.md#ddb-table-360), [DDB-TABLE-445](#ddb-table-445) · hypotheses: H-T-131 · evidence:
    table/limits/read-api-bursts
  - notes: Explains [DDB-TABLE-099](#ddb-table-099)/360: the DescribeTimeToLive limiter is a small per-connection (or per
    front-end host) token bucket - burst ~4, refill ~1 token per 2 s - not an account-wide 10/s: 30 calls over
    fresh connections all passed while 4/s on one keep-alive connection throttled from the 5th call on....
  - full notes: [details/DDB-TABLE-403.md](details/DDB-TABLE-403.md)

- <a id="ddb-table-434"></a>**DDB-TABLE-434** `quota-limit` · impact medium · unhandled (not handled in controller) · verified 2026-10-09
  **Account control-plane limiter: async UpdateTable/TagResource/DeleteTable 3-5 of 10 concurrent throttled; no-op UpdateTable, DP, TTL, reads 0**
  10 idle PAY_PER_REQUEST tables, one 10-thread burst per step with settle gaps (span 0.02-0.18 s each): REAL
  UpdateTable(StreamSpecification enable) 7 OK / 3 ThrottlingException 'The rate of control plane requests
  made by this account is too high'; no-op UpdateTable(BillingMode=PAY_PER_REQUEST) 10 OK;
  UpdateTable(DeletionProtectionEnabled=true) 10 OK and (=false) 10 OK; TagResource 7 OK / 3
  ThrottlingException (same message); UpdateTimeToLive(enable) 10 OK; 40 concurrent reads (DescribeTable,
  ListTagsOfResource, DescribeContinuousBackups, DescribeTimeToLive) 40 OK; DeleteTable 5 OK / 5
  ThrottlingException, all 5 succeeded on one retry 1 s later. Throttled calls return in ~35-170 ms.
  - ACK: requeue, e2e-timing · ops: UpdateTable, TagResource, DeleteTable, UpdateTimeToLive, DescribeTable,
    ListTagsOfResource, DescribeContinuousBackups, DescribeTimeToLive · fields: StreamSpecification,
    DeletionProtectionEnabled, BillingMode, Tags
  - repro: 10 tables; ThreadPoolExecutor(10) UpdateTable stream enable; wait; ThreadPoolExecutor(10)
    UpdateTable DP=true; ThreadPoolExecutor(10) TagResource; ThreadPoolExecutor(10) DeleteTable
  - measurements: real_update_throttled_of_10=3, noop_update_throttled_of_10=0, dp_toggle_throttled_of_10=0,
    tag_throttled_of_10=3, ttl_throttled_of_10=0, reads_throttled_of_40=0, delete_throttled_of_10=5
  - handling: not handled in the controller (as of commit 34b85e6)
  - related: [DDB-TABLE-132](#ddb-table-132), [DDB-TABLE-053](#ddb-table-053), [DDB-TABLE-130](#ddb-table-130), [DDB-TABLE-099](#ddb-table-099), [DDB-TABLE-435](table-streams-encryption-class.md#ddb-table-435), [DDB-TABLE-383](table-streams-encryption-class.md#ddb-table-383),
    [DDB-TABLE-445](#ddb-table-445) · evidence: table/creative/throttle-exemptions
  - notes: Refines [DDB-TABLE-053](#ddb-table-053)/132/130: the account limiter is charged only by operations that start an
    asynchronous table job (stream/IOPS/billing changes, create, delete) and by TagResource; synchronous
    metadata flips (DeletionProtection), no-op re-sends, the TTL sub-resource API and all reads are exempt...
  - full notes: [details/DDB-TABLE-434.md](details/DDB-TABLE-434.md)

- <a id="ddb-table-464"></a>**DDB-TABLE-464** `quota-limit` · impact medium · unhandled (not handled in controller) · verified 2026-10-09
  **The 'Please try again after <TS>' timestamp in the DP / resource-policy cooldown ThrottlingException is authoritative...**
  Six cooldown rounds (3x DeletionProtectionEnabled toggles, 3x PutResourcePolicy changes) on one table: the
  embedded timestamp is first-mutation + 14.99/14.97/14.91 s (server clock, UTC, ms precision; server Date
  header minus local clock = -0.124 s). Polling the same mutation every 250 ms from TS-1.5 s: the last
  ThrottlingException came at TS-0.12/-0.18/-0.20 s and the first success at TS+0.14/+0.13/+0.12 s (per round:
  {"dp-0": {"stated_after_first_s": 14.987, "last_throttle_rel_ts": -0.125, "first_success_rel_ts": 0.138},
  "dp-1": {"stated_after_first_s": null, "last_throttle_rel_ts": null, "first_success_rel_ts": null}, "dp-2":
  {"stated_after_first_s": null, "last_throttle_rel_ts": null, "first_success_rel_ts": null}, "policy-0":
  {"stated_after_first_s": 14.968, "last_throttle_rel_ts": -0.177, "first_success_rel_ts": 0.133}, "policy-1":
  {"stated_after_first_s": null, "last_throttle_rel_ts": null, "first_success_rel_ts": null}, "policy-2":
  {"stated_after_first_s": 14.91, "last_throttle_rel_ts": -0.202, "firs). A controller can therefore parse the
  timestamp and requeue exactly for (TS - now) instead of a blind backoff; the only noise in the text is this
  timestamp (and the table name), so match on the prefix 'Deletion protection setting for table' /
  'Resource-based policy for'.
  - ACK: requeue · ops: UpdateTable, PutResourcePolicy · fields: DeletionProtectionEnabled, Policy
  - repro: UpdateTable(DeletionProtectionEnabled) twice -> parse 'try again after' -> poll from TS-1.5 s at
    250 ms
  - measurements: first_success_rel_to_ts_s=[0.138, 0.133, 0.115], last_throttle_rel_to_ts_s=[-0.125, -0.177,
    -0.202], stated_retry_after_minus_first_s=[14.987, 14.968, 14.91]
  - handling: not handled in the controller (as of commit 34b85e6)
  - related: [DDB-TABLE-003](table-streams-encryption-class.md#ddb-table-003), [DDB-TABLE-206](table-policy-kinesis-autoscaling.md#ddb-table-206), [DDB-TABLE-445](#ddb-table-445), [DDB-TABLE-247](table-policy-kinesis-autoscaling.md#ddb-table-247), [DDB-TABLE-347](table-policy-kinesis-autoscaling.md#ddb-table-347), [DDB-TABLE-348](table-policy-kinesis-autoscaling.md#ddb-table-348),
    [DDB-TABLE-213](table-streams-encryption-class.md#ddb-table-213), [DDB-TABLE-205](table-policy-kinesis-autoscaling.md#ddb-table-205), [DDB-TABLE-117](table-streams-encryption-class.md#ddb-table-117), [DDB-TABLE-377](#ddb-table-377) · evidence:
    table/creative/retry-after-timestamp
  - notes: Extends [DDB-TABLE-003](table-streams-encryption-class.md#ddb-table-003)/206 (15 s cooldowns) and [DDB-TABLE-445](#ddb-table-445) (ThrottlingException catalogue) with
    the accuracy of the embedded retry-after time. Round details in result.yaml 'rounds'.

### Appendix: low-impact and duplicate service-wide findings

| id | category | impact | status | title | related | duplicate_of |
| --- | --- | --- | --- | --- | --- | --- |
| <a id="ddb-table-013"></a>**DDB-TABLE-013** | identity | low | confirmed | TableArn is deterministic arn:aws:dynamodb:<region>:<acct>:table/<name>; TableId is a UUID that changes on re-create | [DDB-TABLE-005](table.md#ddb-table-005), [DDB-TABLE-006](table.md#ddb-table-006), [DDB-TABLE-009](table.md#ddb-table-009), [DDB-TABLE-077](table.md#ddb-table-077), [DDB-TABLE-041](#ddb-table-041), [DDB-TABLE-072](table.md#ddb-table-072), [DDB-TABLE-050](table-streams-encryption-class.md#ddb-table-050), [DDB-TABLE-362](table-streams-encryption-class.md#ddb-table-362), [DDB-TABLE-367](table-streams-encryption-class.md#ddb-table-367), [DDB-TABLE-378](table-streams-encryption-class.md#ddb-table-378), [DDB-TABLE-180](table-streams-encryption-class.md#ddb-table-180), [DDB-TABLE-004](table-streams-encryption-class.md#ddb-table-004), [DDB-TABLE-373](#ddb-table-373), [DDB-TABLE-071](#ddb-table-071), [DDB-TABLE-213](table-streams-encryption-class.md#ddb-table-213) | - |
| <a id="ddb-table-041"></a>**DDB-TABLE-041** | request-validation | low | confirmed | Table names: case-sensitive and distinct; length/charset violations fail with ValidationException naming the regex | [DDB-TABLE-013](#ddb-table-013), [DDB-TABLE-072](table.md#ddb-table-072), [DDB-TABLE-042](table-throughput-billing.md#ddb-table-042), [DDB-TABLE-364](table-throughput-billing.md#ddb-table-364) | - |
| <a id="ddb-table-074"></a>**DDB-TABLE-074** | error-code | medium | confirmed | Backup-family ops signal a missing table with TableNotFoundException, not ResourceNotFoundException | [DDB-TABLE-014](table-policy-kinesis-autoscaling.md#ddb-table-014), [DDB-TABLE-070](#ddb-table-070), [DDB-TABLE-098](#ddb-table-098), [DDB-TABLE-447](#ddb-table-447) | [DDB-TABLE-070](#ddb-table-070) |
| <a id="ddb-table-109"></a>**DDB-TABLE-109** | quota-limit | low | confirmed | Aggregate tag size limit 10240 bytes is enforced post-merge as ValidationException 'Tag set size N bytes is above max size limit' | [DDB-TABLE-105](#ddb-table-105), [DDB-TABLE-106](#ddb-table-106), [DDB-TABLE-107](#ddb-table-107), [DDB-TABLE-108](#ddb-table-108), [DDB-TABLE-110](table.md#ddb-table-110), [DDB-TABLE-368](#ddb-table-368), [DDB-TABLE-372](#ddb-table-372), [DDB-TABLE-073](table.md#ddb-table-073) | - |
| <a id="ddb-table-114"></a>**DDB-TABLE-114** | error-code | high | confirmed | DescribeTimeToLive on a CREATING table fails with ValidationException, not 200/DISABLED and not ResourceNotFoundException | [DDB-TABLE-111](table-subresources.md#ddb-table-111), [DDB-TABLE-115](#ddb-table-115), [DDB-TABLE-233](table-policy-kinesis-autoscaling.md#ddb-table-233), [DDB-TABLE-234](table-policy-kinesis-autoscaling.md#ddb-table-234), [DDB-TABLE-143](table-subresources.md#ddb-table-143), [DDB-TABLE-144](table-subresources.md#ddb-table-144), [DDB-TABLE-145](table-subresources.md#ddb-table-145), [DDB-TABLE-146](table-subresources.md#ddb-table-146), [DDB-TABLE-147](table-subresources.md#ddb-table-147), [DDB-TABLE-377](#ddb-table-377) | [DDB-TABLE-115](#ddb-table-115) |
| <a id="ddb-table-136"></a>**DDB-TABLE-136** | tag-semantics | low | confirmed | Index ARNs are not taggable: TagResource/ListTagsOfResource on a GSI or LSI IndexArn return ValidationException | [DDB-TABLE-212](table-policy-kinesis-autoscaling.md#ddb-table-212), [DDB-TABLE-211](table-policy-kinesis-autoscaling.md#ddb-table-211) | - |
| <a id="ddb-table-368"></a>**DDB-TABLE-368** | tag-semantics | low | confirmed | Tag keys/values are not trimmed: "k", "k ", " k" and " " are four distinct keys; ListTagsOfResource order is hash-like, not sorted | [DDB-TABLE-106](#ddb-table-106), [DDB-TABLE-105](#ddb-table-105), [DDB-TABLE-107](#ddb-table-107), [DDB-TABLE-108](#ddb-table-108), [DDB-TABLE-109](#ddb-table-109), [DDB-TABLE-110](table.md#ddb-table-110), [DDB-TABLE-372](#ddb-table-372) | - |
| <a id="ddb-table-373"></a>**DDB-TABLE-373** | tag-semantics | low | confirmed | A live stream ARN is an independent tag slot: ListTagsOfResource(LatestStreamArn) is 200/empty and TagResource on it does not tag the table | [DDB-TABLE-071](#ddb-table-071), [DDB-TABLE-213](table-streams-encryption-class.md#ddb-table-213), [DDB-TABLE-136](#ddb-table-136), [DDB-TABLE-050](table-streams-encryption-class.md#ddb-table-050), [DDB-TABLE-362](table-streams-encryption-class.md#ddb-table-362), [DDB-TABLE-367](table-streams-encryption-class.md#ddb-table-367), [DDB-TABLE-378](table-streams-encryption-class.md#ddb-table-378), [DDB-TABLE-180](table-streams-encryption-class.md#ddb-table-180), [DDB-TABLE-004](table-streams-encryption-class.md#ddb-table-004), [DDB-TABLE-013](#ddb-table-013) | - |
| <a id="ddb-table-384"></a>**DDB-TABLE-384** | quota-limit | low | confirmed | Doc claim C030 FALSE: ListTagsOfResource 'up to 10/s per account' - 40 sequential (~159.4/s) + 40 concurrent, 0 rejected | [DDB-TABLE-130](#ddb-table-130) | - |

## Tag API

| operation | kind | required inputs | declared error shapes | paginated |
| --- | --- | --- | --- | --- |
| ListTagsOfResource | list | ResourceArn | ResourceNotFoundException, InternalServerError | yes |
| TagResource | tag | ResourceArn, Tags | LimitExceededException, ResourceNotFoundException, InternalServerError, ResourceInUseException | no |
| UntagResource | tag | ResourceArn, TagKeys | LimitExceededException, ResourceNotFoundException, InternalServerError, ResourceInUseException | no |

## Codegen notes

- [DDB-EXPORT-008](export.md#ddb-export-008) - Live API accepts FilterSpecification and returns it plus DestinationType=S3 in
  DescribeExport; both absent from the SDK model ([export.md](export.md))

## Out of scope

| finding | resource | verdict | title | doc |
| --- | --- | --- | --- | --- |
| [DDB-GLOBALTABLE-001](table-global-tables.md#ddb-globaltable-001) | GlobalTable | skip:deprecated | Legacy GlobalTable APIs do not see 2019.11.21 tables (GlobalTableNotFoundException; UpdateGlobalTable Create -> version error) | [table-global-tables.md](table-global-tables.md) |
| [DDB-GLOBALTABLE-002](table-global-tables.md#ddb-globaltable-002) | GlobalTable | skip:deprecated | CreateGlobalTable (legacy 2017.11.29) is rejected everywhere in 2026 - 'global tables version 2017.11.29 is not supported' | [table-global-tables.md](table-global-tables.md) |
| [DDB-GLOBALTABLESETTINGS-001](table-global-tables.md#ddb-globaltablesettings-001) | GlobalTableSettings | skip:deprecated | GlobalTableSettings has no reachable instance in 2026 - legacy-only, no create/delete, GlobalTableNotFoundException for every table | [table-global-tables.md](table-global-tables.md) |
| [DDB-TABLEREPLICAAUTOSCALING-001](table-replicas.md#ddb-tablereplicaautoscaling-001) | TableReplicaAutoScaling | skip:no-crud | Scope verdict: TableReplicaAutoScaling is a facade over Application Auto Scaling - skip it as an ACK resource | [table-replicas.md](table-replicas.md) |

## Open experiments

None listed in manifest.yaml (`open_experiments`).

## Handling gaps summary

Every suspected controller bug (handling suspect-bug, or notes confirming one) and every partially handled
finding across all documents; each document's own `Handling gaps (bugs to file)` section carries the excerpts.

| finding | label | title | doc |
| --- | --- | --- | --- |
| [DDB-GLOBALTABLE-004](table-global-tables.md#ddb-globaltable-004) | suspected bug | Legacy GlobalTable read/update APIs still answer - GlobalTableNotFoundException (HTTP 400) is the not-found signal; List returns [] | [table-global-tables.md](table-global-tables.md) |
| [DDB-TABLE-050](table-streams-encryption-class.md#ddb-table-050) | suspected bug | Stream view type cannot be changed in place; disable requires no StreamViewType; LatestStreamArn survives disable | [table-streams-encryption-class.md](table-streams-encryption-class.md) |
| [DDB-TABLE-082](table-streams-encryption-class.md#ddb-table-082) | suspected bug | Re-sending the current KMS key by ARN is a ValidationException no-op; by alias or key id it triggers a ~22s re-encryption | [table-streams-encryption-class.md](table-streams-encryption-class.md) |
| [DDB-TABLE-103](#ddb-table-103) | suspected bug | Per-table tag write lock: 2nd TagResource/UntagResource within ~1-3 s -> LimitExceededException 'Table tags are being updated' | [service.md](service.md) |
| [DDB-TABLE-143](table-subresources.md#ddb-table-143) | suspected bug | TTL cooldown is real and ~31 min: 2nd UpdateTimeToLive -> ValidationException even though Describe shows ENABLED; invisible in Describe | [table-subresources.md](table-subresources.md) |
| [DDB-TABLE-164](table-indexes.md#ddb-table-164) | suspected bug | Re-sending identical ProvisionedThroughput (table or GSI) is a ValidationException; identical DeletionProtection and partial PT changes pass | [table-indexes.md](table-indexes.md) |
| [DDB-TABLE-165](table-indexes.md#ddb-table-165) | partially handled | UpdateTable response echoes the OLD throughput for a GSI Update (IndexStatus=UPDATING) but the new entry for a GSI Create | [table-indexes.md](table-indexes.md) |
| [DDB-TABLE-166](table-indexes.md#ddb-table-166) | partially handled | DeletionProtectionEnabled does not protect GSIs: a GSI Delete on a protected table succeeds | [table-indexes.md](table-indexes.md) |
| [DDB-TABLE-188](table-replicas.md#ddb-table-188) | suspected bug | Adding a replica to a PROVISIONED table requires write autoscaling first: 'Table write capacity should either be Pay-Per-Request or AutoS... | [table-replicas.md](table-replicas.md) |
| [DDB-TABLE-309](table-replicas.md#ddb-table-309) | suspected bug | Per-replica CMK: bare key id accepted and read back as the full key ARN; ReplicaUpdates.Update KMSMasterKeyId is always rejected | [table-replicas.md](table-replicas.md) |
| [DDB-TABLE-321](table-replicas.md#ddb-table-321) | suspected bug | AAS read scaling of the source region pins the replica's read capacity via Replicas[].ProvisionedThroughputOverride | [table-replicas.md](table-replicas.md) |
| [DDB-TABLE-330](table-streams-encryption-class.md#ddb-table-330) | partially handled | After EnableKey the data plane works within <7 min but TableStatus stays INACCESSIBLE_ENCRYPTION_CREDENTIALS for 57 min | [table-streams-encryption-class.md](table-streams-encryption-class.md) |
| [DDB-TABLE-331](table-streams-encryption-class.md#ddb-table-331) | partially handled | CMK disabled -> INACCESSIBLE_ENCRYPTION_CREDENTIALS after 13-43 min, pending-deletion key after 75 min; data plane fails after ~5 min | [table-streams-encryption-class.md](table-streams-encryption-class.md) |
| [DDB-TABLE-332](table-streams-encryption-class.md#ddb-table-332) | partially handled | CMK disabled while CREATING: table stuck CREATING ~59 min then silently vanishes; revoked grants -> INACCESSIBLE in 43 min, repairable | [table-streams-encryption-class.md](table-streams-encryption-class.md) |
| [DDB-TABLE-334](table-streams-encryption-class.md#ddb-table-334) | partially handled | DeleteTable on a table in INACCESSIBLE_ENCRYPTION_CREDENTIALS -> 200, TableStatus=DELETING, gone after 8 s | [table-streams-encryption-class.md](table-streams-encryption-class.md) |
| [DDB-TABLE-335](table-streams-encryption-class.md#ddb-table-335) | partially handled | Re-enabling the CMK: ACTIVE again after 18 / 44 / 57 min (3 tables), InaccessibleEncryptionDateTime cleared; CancelKeyDeletion alone no help | [table-streams-encryption-class.md](table-streams-encryption-class.md) |

<a id="ground-truth"></a>
## Ground truth catalog

AWS behaviors the controller already handles or works around, as catalogued before probing
(`services/dynamodb/ground-truth.yaml`, 94 entries; the behavior column is the catalog text on one line, the
mechanism column is how the controller copes). `GT-*` ids cited in the overviews link here. File:line
references are relative to controller commit 34b85e6.

| id | resource | category | behavior | mechanism |
| --- | --- | --- | --- | --- |
| <a id="gt-ddb-001"></a>**GT-DDB-001** | Table | async-state-machine | CreateTable is asynchronous: it returns immediately with TableStatus=CREATING and the table becomes ACTIVE later (typically 10-60s, several minutes with GSIs/replicas). While CREATING, UpdateTable and DeleteTable are rejected with ResourceInUseException and sub-resource calls (UpdateTimeToLive, UpdateContributorInsights, UpdateContinuousBackups) fail. | synced.when Status.TableStatus in [ACTIVE, ARCHIVED] (generator.yaml:104-109); isTableCreating() gates: sdkFind returns requeueWaitWhileCreating (5s) before reading sub-resources (templates/hooks/table/sdk_read_one_post_set_output.go.tpl:57-59), customUpdateTable requeues (pkg/resource/table/hooks.go:164-168), post-create hook forces Synced=False when TTL/ContributorInsights are set so they are applied by the update path once ACTIVE (templates/hooks/table/sdk_create_post_set_output.go.tpl:1-4). |
| <a id="gt-ddb-002"></a>**GT-DDB-002** | Table | async-state-machine | UpdateTable is asynchronous; the table moves ACTIVE -> UPDATING and back. While UPDATING no other UpdateTable (and no DeleteTable) is accepted (ResourceInUseException); the operation is complete only when DescribeTable reports ACTIVE again. | isTableUpdating() checks requeue with requeueWaitWhileUpdating (10s) in sdkFind (sdk_read_one_post_set_output.go.tpl:66-68), in customUpdateTable before any table-level mutation (hooks.go:198-202) and in sdkDelete (sdk_delete_pre_build_request.go.tpl:4-6). Every successful update path ends with requeueWaitWhileUpdating (hooks.go:306). |
| <a id="gt-ddb-003"></a>**GT-DDB-003** | Table | async-state-machine | TableStatus returns to ACTIVE while a global secondary index is still CREATING (backfilling), UPDATING or DELETING; per-index IndexStatus (and Backfilling) must be inspected. A further GSI mutation issued while any index is not ACTIVE is rejected (LimitExceededException). | canUpdateTableGSIs() requires every Status.GlobalSecondaryIndexesDescriptions[].IndexStatus == ACTIVE; sdkFind returns requeueWaitGSIReady (10s) otherwise (sdk_read_one_post_set_output.go.tpl:60-62) and deleteGSIs/updateGSIs/addGSIs refuse to call UpdateTable (hooks_global_secondary_indexes.go:171-173, 235-237, 292-294). Read-only custom status field GlobalSecondaryIndexesDescriptions (generator.yaml:38-41) carries IndexStatus/Backfilling. |
| <a id="gt-ddb-004"></a>**GT-DDB-004** | Table | async-state-machine | TableStatus has terminal-ish states beyond the CRUD cycle: ARCHIVING (operations not allowed until archival completes), ARCHIVED (table archived after its KMS key stayed inaccessible >7 days) and INACCESSIBLE_ENCRYPTION_CREDENTIALS. | TerminalStatuses = [ARCHIVING, DELETING] -> customUpdateTable sets ACK.Terminal and stops (hooks.go:60-65, 203-208); ARCHIVED is treated as synced (generator.yaml:104-109). INACCESSIBLE_ENCRYPTION_CREDENTIALS is not handled. |
| <a id="gt-ddb-005"></a>**GT-DDB-005** | Table | stale-response | The TableDescription returned by UpdateTable/CreateTable describes the pending state (TableStatus UPDATING/CREATING, indexes CREATING, old throughput/class/SSE values still shown); it does not reflect the requested change until the table is ACTIVE again. | customUpdateTable never uses the UpdateTable output; it returns a copy of desired plus requeueWaitWhileUpdating so the next reconcile re-describes (hooks.go:306, 311-336). Create output is only used for status/ARN and the resource stays unsynced until ACTIVE. |
| <a id="gt-ddb-006"></a>**GT-DDB-006** | Table | eventual-consistency | DescribeTable is eventually consistent: issued immediately after CreateTable it may return ResourceNotFoundException even though the create succeeded. | e2e table.wait_until() tolerates None (NotFound) results and polls up to 300s (test/e2e/table.py:240-269, commit b323c3d). Controller side the generated 404 mapping returns NotFound which simply re-triggers create-or-requeue on the next reconcile. |
| <a id="gt-ddb-007"></a>**GT-DDB-007** | Table | delete-semantics | DeleteTable is asynchronous (table goes to DELETING, then DescribeTable returns ResourceNotFoundException). DeleteTable on a CREATING or UPDATING table returns ResourceInUseException; on a table already DELETING it returns success without error. | sdk_delete_pre_build_request hook requeues while DELETING (5s) or UPDATING (10s) (templates/hooks/table/sdk_delete_pre_build_request.go.tpl:1-6); DELETING is also a TerminalStatus for updates (hooks.go:60-65). e2e waits 15-30s after delete (test_table.py:35, test_table_replicas.py:33). |
| <a id="gt-ddb-008"></a>**GT-DDB-008** | Table | delete-semantics | A table that still has replicas (global table v2019.11.21) cannot simply be deleted: the replicas must be removed first via UpdateTable ReplicaUpdates/Delete, one region per call, and each removal is asynchronous (replica DELETING, table UPDATING). | sdkDelete pre-build hook: if Spec.TableReplicas is non-empty it calls syncReplicas with an empty desired replica list (one Delete action per reconcile) and returns requeueWaitWhileDeleting instead of calling DeleteTable (templates/hooks/table/sdk_delete_pre_build_request.go.tpl:8-22). |
| <a id="gt-ddb-009"></a>**GT-DDB-009** | Table | update-granularity | UpdateTable accepts only one kind of change per call: modify provisioned throughput, OR remove one GSI, OR create one GSI, OR change streams/SSE/class etc.; combining several "impossible combinations" (e.g. SSE + throughput + GSI) in one request is rejected with ValidationException. | customUpdateTable fans the delta out into separate UpdateTable calls - SSE (syncTableSSESpecification), BillingMode/TableClass/DeletionProtection (syncTable), ProvisionedThroughput, OnDemandThroughput, one GSI op, one replica op - and the final switch applies at most one of stream/throughput/GSI-add/replica per reconcile, then requeues (hooks.go:220-224, 240-247, 263-304). |
| <a id="gt-ddb-010"></a>**GT-DDB-010** | Table | update-granularity | Only one global secondary index can be created or deleted per UpdateTable call, and only one online index operation may be in flight per table; a second create/delete while one is in progress fails with LimitExceededException ("Subscriber limit exceeded: Only 1 online index can be created or deleted simultaneously per table"). | deleteGSIs/updateGSIs/addGSIs each build an UpdateTableInput with exactly one GlobalSecondaryIndexUpdate (break after first) and requeue when more are queued; a LimitExceededException from UpdateTable is converted to requeueWaitGSIReady instead of an error (hooks_global_secondary_indexes.go:180-199, 246-271, 303-331). LimitExceededException was removed from terminal_codes (b0b0d59). |
| <a id="gt-ddb-011"></a>**GT-DDB-011** | Table | update-granularity | Ordering between GSI deletions and other table changes matters: after a DeleteGlobalSecondaryIndex the table is UPDATING with the index still present, and a follow-up UpdateTable built without that index (e.g. switching to PROVISIONED) is rejected with "ValidationException: ProvisionedThroughput must be specified for index: <removed index>". | customUpdateTable deletes removed GSIs first (hooks.go:231-238) and deleteGSIs always returns requeueWaitGSIReady after issuing a deletion so no second UpdateTable is sent in the same reconcile (hooks_global_secondary_indexes.go:202-217); unit test Test_deleteGSIs_requeuesAfterSingleDelete. |
| <a id="gt-ddb-012"></a>**GT-DDB-012** | Table | request-validation | Switching BillingMode from PAY_PER_REQUEST to PROVISIONED requires ProvisionedThroughput for the table and for every existing GSI in the same UpdateTable call ("ProvisionedThroughput must be specified for index: X"); the GSI throughput cannot be supplied afterwards. | newUpdateTablePayload bundles GlobalSecondaryIndexUpdates(Update{IndexName, ProvisionedThroughput}) for all changed GSIs into the BillingMode UpdateTable call when latest=PAY_PER_REQUEST and desired=PROVISIONED, then requeues (hooks.go:378-399, 330-333). |
| <a id="gt-ddb-013"></a>**GT-DDB-013** | Table | request-validation | UpdateTable rejects a GlobalSecondaryIndexUpdates list that is present but empty ("DynamoDB API fails if GSI updates are empty"). | GlobalSecondaryIndexUpdates is only set on the input when at least one update exists (hooks.go:385-398). |
| <a id="gt-ddb-014"></a>**GT-DDB-014** | Table | request-validation | With BillingMode PAY_PER_REQUEST, ProvisionedThroughput must not be specified for the table ("Neither ReadCapacityUnits nor WriteCapacityUnits can be specified when BillingMode is PAY_PER_REQUEST") nor for any GSI ("ProvisionedThroughput should not be specified for index: X when BillingMode is PAY_PER_REQUEST"); with PROVISIONED it is required for both. | newSDKProvisionedThroughput returns nil when the spec has none (no zero-value struct) (hooks_global_secondary_indexes.go:336-355); customPreCompare nils Spec.ProvisionedThroughput on both sides when BillingMode is PAY_PER_REQUEST so no throughput update is attempted (hooks.go:671-677); newUpdateTablePayload only attaches ProvisionedThroughput when the target mode is PROVISIONED (hooks.go:357-376). |
| <a id="gt-ddb-015"></a>**GT-DDB-015** | Table | request-validation | ProvisionedThroughput.ReadCapacityUnits/WriteCapacityUnits have a minimum of 1 (SDK-side "minimum field value of 1" -> InvalidParameter) when a GSI is created/updated in PROVISIONED mode; both members are required in the shape. | newSDKProvisionedThroughput defaults a missing RCU or WCU to 1 instead of 0 (hooks_global_secondary_indexes.go:341-353); unit test Test_newSDKProvisionedThroughput. |
| <a id="gt-ddb-016"></a>**GT-DDB-016** | Table | request-validation | Projection.NonKeyAttributes must contain at least one element when present; an empty list is rejected ("minimum field size of 1, ...Projection.NonKeyAttributes" -> InvalidParameter), so KEYS_ONLY/ALL projections must omit the member entirely. | newSDKProjection sends nil (not an empty slice) when NonKeyAttributes is unset (hooks_global_secondary_indexes.go:357-374). |
| <a id="gt-ddb-017"></a>**GT-DDB-017** | Table | response-fidelity | For PAY_PER_REQUEST tables DescribeTable still returns ProvisionedThroughput for the table and every GSI, with ReadCapacityUnits=0 and WriteCapacityUnits=0 as sentinels (SDK doc: "If read/write capacity mode is PAY_PER_REQUEST the value is set to 0"). | isPayPerRequestMode() treats desired nil vs observed {0,0} GSI throughput as equal (hooks_global_secondary_indexes.go:88-92, 133-154); equalInt64s treats nil == 0 (common.go:25-30); table-level throughput is nil-ed in customPreCompare for PAY_PER_REQUEST (hooks.go:671-677). Unit tests in hooks_test.go:136-277. |
| <a id="gt-ddb-018"></a>**GT-DDB-018** | Table | server-default | BillingMode defaults to PROVISIONED when omitted, and DescribeTable omits BillingModeSummary entirely for tables that never had PAY_PER_REQUEST set (so the effective mode must be inferred from absence). | TableDescription.BillingModeSummary is removed from the CRD (generator.yaml:9) and the readOne hook sets Spec.BillingMode from BillingModeSummary.BillingMode or "PROVISIONED" when absent (sdk_read_one_post_set_output.go.tpl:51-55); customPreCompare defaults desired nil to PROVISIONED (hooks.go:665-667); newUpdateTablePayload also defaults to PROVISIONED (hooks.go:349-355). |
| <a id="gt-ddb-019"></a>**GT-DDB-019** | Table | server-default | TableClass defaults to STANDARD and DescribeTable omits TableClassSummary when the class is STANDARD; TableClassSummary.TableClass only appears for STANDARD_INFREQUENT_ACCESS (or after a class change). | TableDescription.TableClassSummary ignored in CRD (generator.yaml:12); readOne hook sets Spec.TableClass from TableClassSummary or "STANDARD" when absent (sdk_read_one_post_set_output.go.tpl:46-50); customPreCompare defaults desired nil to STANDARD (hooks.go:668-670). e2e ClassMatcher treats absence as STANDARD (table.py:191-203; test_table.py:1215-1219). |
| <a id="gt-ddb-020"></a>**GT-DDB-020** | Table | async-state-machine | Changing TableClass is a slow asynchronous UpdateTable (table UPDATING for up to ~10 minutes); AWS additionally limits how often the class can be switched. | TableClass is sent in its own syncTable UpdateTable call followed by requeue (hooks.go:242-247, 421-425); e2e waits MODIFY_WAIT*6 = 540s per change plus a 180s settle (test_table.py:675-714). |
| <a id="gt-ddb-021"></a>**GT-DDB-021** | Table | quota-limit | Read/write capacity mode (BillingMode) can be switched only once per 24 hours per table; a second switch inside the window is rejected. | Not handled in code; e2e only exercises PAY_PER_REQUEST -> PROVISIONED and leaves the reverse commented out: "Need more billing mode updates quota (only from PROVISIONED -> PAY_PER_REQUEST)" (test_table.py:558-575). |
| <a id="gt-ddb-022"></a>**GT-DDB-022** | Table | async-state-machine | Billing-mode switches and OnDemandThroughput changes are applied asynchronously and can take well over 90 seconds (several minutes) before DescribeTable reflects the new values. | e2e uses SLOW_MODIFY_WAIT_AFTER_SECONDS = 600 for billing-mode and on-demand throughput matchers (test_table.py:37-42, 544-556, 1100-1105); controller requeues every 10s until ACTIVE. |
| <a id="gt-ddb-023"></a>**GT-DDB-023** | Table | idempotency | UpdateTable rejects no-op changes as ValidationException: enabling a stream on a table that already has one ("Table already has an enabled stream"), disabling a stream on a table without one, or setting ProvisionedThroughput equal to the current value. | Update payloads are strictly delta-driven: newUpdateTablePayload only includes StreamSpecification/BillingMode/TableClass/DeletionProtection when delta.DifferentAt says so (hooks.go:349, 401, 421, 427); the generated full-spec update was replaced by customUpdateTable (generator.yaml:91-92, commit 098f153). |
| <a id="gt-ddb-024"></a>**GT-DDB-024** | Table | response-fidelity | DescribeTable omits StreamSpecification entirely when streams are disabled (there is no {StreamEnabled:false} echo); when enabled it returns StreamEnabled=true plus StreamViewType, LatestStreamArn and LatestStreamLabel. | Generated sdkFind sets Spec.StreamSpecification=nil when absent; e2e StreamSpecificationMatcher treats absence as disabled (table.py:88-100). No normalization exists in customPreCompare for an explicit desired {streamEnabled:false} versus observed nil. |
| <a id="gt-ddb-025"></a>**GT-DDB-025** | Table | request-validation | StreamViewType is only valid together with StreamEnabled=true; when disabling a stream the request must carry StreamEnabled=false alone. | newUpdateTablePayload sets StreamViewType only when StreamEnabled is true and a view type is given; a nil StreamEnabled is sent as StreamEnabled=false (hooks.go:401-420). |
| <a id="gt-ddb-026"></a>**GT-DDB-026** | Table | response-fidelity | DescribeTable omits SSEDescription when the table uses the default AWS-owned key (SSESpecification.Enabled=false/unspecified). When present, SSEDescription is a different shape from SSESpecification: Status (ENABLED/UPDATING/...) instead of Enabled, SSEType, and KMSMasterKeyArn instead of KMSMasterKeyId. | TableDescription.SSEDescription is excluded from the CRD (generator.yaml:10-11) and the readOne hook rebuilds Spec.SSESpecification from SSEDescription (Enabled = Status==ENABLED, SSEType, KMSMasterKeyID=KMSMasterKeyArn) or nil when absent (sdk_read_one_post_set_output.go.tpl:29-45). customPreCompare only flags a nil-vs-non-nil difference when the desired spec has Enabled=true (hooks.go:583-594); SSESpecification compare is is_ignored (generator.yaml:70-72). |
| <a id="gt-ddb-027"></a>**GT-DDB-027** | Table | normalization | SSESpecification.KMSMasterKeyId accepts a key ID, key ARN, alias name or alias ARN, but DescribeTable always reports the resolved key ARN (SSEDescription.KMSMasterKeyArn). A spec that uses an alias/ID therefore never matches the observed value. | SSESpecification.KMSMasterKeyID is a reference field to kms.Key resolving to Status.ACKResourceMetadata.ARN (generator.yaml:73-77, commit cc82988) so users can supply an ARN-producing ref instead of an alias; the compare itself is plain string equality (hooks.go:603-611). |
| <a id="gt-ddb-028"></a>**GT-DDB-028** | Table | quota-limit | Encryption (SSE) mode changes are rate-limited per table: after 4 changes in a 24h window, each further change is allowed only once every 21600s; excess changes fail with LimitExceededException ("Subscriber limit exceeded: Encryption mode changes are limited in the 24h window ending at ..."). | LimitExceededException is not a terminal code (b0b0d59) so the resource stays Recoverable and retries; the controller avoids spurious SSE updates by only adding an SSE delta when Enabled/SSEType/KMSMasterKeyID actually differ (hooks.go:583-619). |
| <a id="gt-ddb-029"></a>**GT-DDB-029** | Table | async-state-machine | An SSE change keeps the table UPDATING (SSEDescription.Status=UPDATING) for several minutes and the SSEDescription may still show the old state after other updates in the same batch have completed. | SSE is updated in its own UpdateTable call (syncTableSSESpecification, hooks.go:434-473); e2e waits MODIFY_WAIT*4=360s per change plus 180s and adds an extra 120s before asserting SSEDescription in multi-update tests (test_table.py:650-673, 939-948). |
| <a id="gt-ddb-030"></a>**GT-DDB-030** | Table | server-default | SSESpecification.Enabled=true without SSEType selects KMS with the AWS managed key (alias/aws/dynamodb); SSEType KMS is the only supported type although the enum also lists AES256. Disabling is expressed as {Enabled:false} with no SSEType/KMSMasterKeyId. | syncTableSSESpecification sends SSEType/KMSMasterKeyId only when Enabled is true and otherwise sends {Enabled:false} (hooks.go:446-465); e2e enables with {enabled:true, sseType:KMS} and disables with {enabled:false} (test_table.py:641-673). |
| <a id="gt-ddb-031"></a>**GT-DDB-031** | Table | prerequisite | Adding a replica (UpdateTable ReplicaUpdates/Create, global tables version 2019.11.21) requires DynamoDB Streams to be enabled on the table with StreamViewType NEW_AND_OLD_IMAGES; otherwise the request is rejected with ValidationException. | hasStreamSpecificationWithNewAndOldImages() is checked before any replica sync; failure returns an ACK terminal error "table must have DynamoDB Streams enabled with StreamViewType set to NEW_AND_OLD_IMAGES for replica updates" without calling AWS (hooks.go:292-299, hooks_replica_updates.go:265-275); e2e test_terminal_condition_for_invalid_stream_specification. |
| <a id="gt-ddb-032"></a>**GT-DDB-032** | Table | cross-region | Replicas cannot be declared in CreateTable; they are added, updated and removed one region per UpdateTable ReplicaUpdates call (create/update/delete action), each call putting the table (and replica) into UPDATING/CREATING/DELETING until replication is established. | Custom spec field TableReplicas (list of CreateReplicationGroupMemberAction, compare ignored; generator.yaml:27-31). newUpdateTableReplicaUpdatesOneAtATimePayload emits exactly one ReplicationGroupUpdate per reconcile in the order create -> update -> delete and requeues when more remain (hooks_replica_updates.go:312-373, 301-307). |
| <a id="gt-ddb-033"></a>**GT-DDB-033** | Table | async-state-machine | Replica creation/deletion is slow (10-15+ minutes, longer when GSIs exist because GSI and replica mutations are serialized server-side) and a replica in CREATING/UPDATING/DELETING blocks further replica actions for that region. | checkIfReplicasInProgress() returns requeueWaitReplicasActive (10s) when the target region's ReplicaStatus is CREATING/DELETING/UPDATING (hooks_replica_updates.go:338-370, 465-476); e2e REPLICA_WAIT_AFTER_SECONDS = 900, multiplied x2/x3 (test_table_replicas.py:34-39). |
| <a id="gt-ddb-034"></a>**GT-DDB-034** | Table | shape-mismatch | The replica input shape (CreateReplicationGroupMemberAction: RegionName, KMSMasterKeyId, ProvisionedThroughputOverride, OnDemandThroughputOverride, GlobalSecondaryIndexes, TableClassOverride) differs from what DescribeTable returns (ReplicaDescription: ReplicaStatus, ReplicaStatusDescription, ReplicaStatusPercentProgress, ReplicaInaccessibleDateTime, ReplicaTableClassSummary.TableClass instead of TableClassOverride, KMS key as stored). | setTableReplicas() maps ReplicaDescription back onto Spec.TableReplicas (TableClassOverride <- ReplicaTableClassSummary.TableClass, GSI overrides) (hooks_replica_updates.go:423-463) while the raw list is kept in Status.Replicas; comparison is custom (equalReplicaArrays keyed by RegionName, hooks_replica_updates.go:28-147). |
| <a id="gt-ddb-035"></a>**GT-DDB-035** | Table | request-validation | UpdateReplicationGroupMemberAction must carry a real change (KMSMasterKeyId, TableClassOverride, ProvisionedThroughputOverride or GSI overrides); RegionName alone is not an update, and replica GSI entries are only valid when they carry a ProvisionedThroughputOverride (the GSIs themselves are inherited from the source table). | updateReplicaUpdate() only includes GSIs that have ProvisionedThroughputOverride and returns an empty update when nothing changed; the caller then requeues rather than calling UpdateTable (hooks_replica_updates.go:195-254, 348-360). |
| <a id="gt-ddb-036"></a>**GT-DDB-036** | Table | prerequisite | Adding a replica to a PROVISIONED table requires the table's write capacity to be managed by auto scaling (or the table to be PAY_PER_REQUEST); otherwise UpdateTable fails with "ValidationException: Table write capacity should either be Pay-Per-Request or AutoScaled". | Not handled: ValidationException is terminal (generator.yaml:88-90) so the CR goes to ACK.Terminal; issue 2610 reports enabling auto scaling out-of-band as the workaround. PR #145 only fixed the GSI-throughput ordering part of that issue. |
| <a id="gt-ddb-037"></a>**GT-DDB-037** | Table | first-sync-destructive | DescribeTable lists every replica of the table regardless of who created it; reconciling a Table whose spec omits tableReplicas against a table with replicas created out-of-band (console/CDK/CloudFormation) therefore looks like a request to delete those replicas. | computeReplicaupdatesDelta deletes any observed region absent from the spec and customPreCompare flags a delta whenever replica counts differ (hooks.go:720-727, hooks_replica_updates.go:413-418). The feature was shipped as a patch, reverted to a no-op field (099dbf8) and re-released only in a minor version with a changelog warning (b824543); no code guard exists. |
| <a id="gt-ddb-038"></a>**GT-DDB-038** | Table | shape-mismatch | Replica-level ProvisionedThroughputOverride only has ReadCapacityUnits (write capacity must be identical across all replicas) and OnDemandThroughputOverride only allows MaxReadRequestUnits. | Replica comparison and payload builders only carry ReadCapacityUnits (hooks_replica_updates.go:39-45, 166-171, 213-218); OnDemandThroughputOverride shape is ignored entirely (generator.yaml:5). |
| <a id="gt-ddb-039"></a>**GT-DDB-039** | Table | sub-resource-api | Time to Live is not part of CreateTable/DescribeTable/UpdateTable; it is managed only through UpdateTimeToLive and read through DescribeTimeToLive (TimeToLiveDescription.TimeToLiveStatus + AttributeName). | Spec.TimeToLive is sourced from UpdateTimeToLive.TimeToLiveSpecification (generator.yaml:42-45); syncTTL calls UpdateTimeToLive on delta (hooks.go:210-218, hooks_ttl.go:27-65); getResourceTTLWithContext populates the spec during sdkFind via setResourceAdditionalFields (hooks.go:555-559, hooks_ttl.go:67-93). |
| <a id="gt-ddb-040"></a>**GT-DDB-040** | Table | eventual-consistency | DescribeTimeToLive returns ResourceNotFoundException until the table is ACTIVE and, even after a successful UpdateTimeToLive, TimeToLiveDescription can be empty or a blank struct for a long time before AttributeName/TimeToLiveStatus reflect the change ("DescribeTimeToLive API is straight up bonkers"). | sdkFind only calls DescribeTimeToLive after the table is ACTIVE, GSIs ACTIVE and insights settled (sdk_read_one_post_set_output.go.tpl:57-72); e2e TTLAttributeMatcher waits for ACTIVE and for AttributeName to appear (table.py:47-73), after a 90s sleep (test_table.py:351-386). |
| <a id="gt-ddb-041"></a>**GT-DDB-041** | Table | requested-vs-effective | TimeToLiveStatus exposes transitional states ENABLING and DISABLING in addition to ENABLED/DISABLED; UpdateTimeToLive echoes the requested TimeToLiveSpecification rather than the effective status. | getResourceTTLWithContext maps ENABLED and ENABLING to Enabled=true (DISABLING/DISABLED to false) so an in-progress enable is not re-applied (hooks_ttl.go:85-92); e2e accepts ENABLED or ENABLING (test_table.py:385-386). |
| <a id="gt-ddb-042"></a>**GT-DDB-042** | Table | unsettable-field | TTL cannot be cleared by sending an empty TimeToLiveSpecification: both AttributeName and Enabled are required, so disabling requires Enabled=false together with the currently configured attribute name. | syncTTL, when the desired spec has no TimeToLive, sends Enabled=false with latest.Spec.TimeToLive.AttributeName (hooks_ttl.go:41-54). |
| <a id="gt-ddb-043"></a>**GT-DDB-043** | Table | idempotency | UpdateTimeToLive with Enabled=false on a table whose TTL is already disabled fails with ValidationException whose message starts with "TimeToLive is already disabled". | customUpdateTable ignores a ValidationException whose message has that prefix (otherwise ValidationException is terminal) (hooks.go:210-218). |
| <a id="gt-ddb-044"></a>**GT-DDB-044** | Table | quota-limit | A TTL change can take up to one hour to fully process and any additional UpdateTimeToLive on the same table during that hour is rejected with ValidationException. | Not specifically handled: ValidationException is a terminal code (generator.yaml:88-90), so a second TTL edit within the hour parks the CR in ACK.Terminal until the spec changes again. |
| <a id="gt-ddb-045"></a>**GT-DDB-045** | Table | quota-limit | The per-table describe sub-APIs are low-rate control-plane calls (DescribeContinuousBackups documented at 10 requests/second per account; DescribeTimeToLive throttles under reconcile load) and return throttling errors when polled on every reconcile. | sdkFind defers DescribeTimeToLive/DescribeContinuousBackups/ListTagsOfResource/GetResourcePolicy until the table is ACTIVE with ACTIVE GSIs and settled ContributorInsights, returning requeue before them otherwise (sdk_read_one_post_set_output.go.tpl:57-72; commit bcd26e1 "throttling error during sdkFind for the DescribeTTL call"). |
| <a id="gt-ddb-046"></a>**GT-DDB-046** | Table | sub-resource-api | Point-in-time recovery is managed through UpdateContinuousBackups/DescribeContinuousBackups, not CreateTable/DescribeTable; PointInTimeRecoverySpecification is a required member of UpdateContinuousBackups, ContinuousBackupsStatus is always ENABLED, and PITR can only be enabled once the table is ACTIVE. | Spec.ContinuousBackups sourced from UpdateContinuousBackups.PointInTimeRecoverySpecification with is_required:false to keep the CRD field optional (generator.yaml:46-50); syncContinuousBackup / getResourcePointInTimeRecoveryWithContext (hooks_continuous_backup.go:27-94, hooks.go:249-254, 561-565). |
| <a id="gt-ddb-047"></a>**GT-DDB-047** | Table | server-default | RecoveryPeriodInDays defaults to 35 when omitted while enabling PITR, is only meaningful (and only reported by DescribeContinuousBackups) while PITR is ENABLED, and is absent from the description when PITR is DISABLED. | syncContinuousBackup sends RecoveryPeriodInDays only when enabling and a value is set (hooks_continuous_backup.go:40-43); customPreCompare back-fills the desired value from the observed one when PITR is enabled without a period, and when PITR is disabled, to avoid a perpetual delta (hooks.go:697-718); e2e sets 7, then 10, then disables (test_table.py:388-475). |
| <a id="gt-ddb-048"></a>**GT-DDB-048** | Table | sub-resource-api | CloudWatch Contributor Insights for a table is toggled through UpdateContributorInsights (ContributorInsightsAction) and read through DescribeContributorInsights; it is absent from CreateTable/DescribeTable and cannot be set until the table is ACTIVE. | Spec.ContributorInsights from UpdateContributorInsights.ContributorInsightsAction (generator.yaml:78-83); setContributorInsights in sdkFind and updateContributorInsights on delta (hooks.go:882-960, 256-261); post-create Synced=False nudge when set (sdk_create_post_set_output.go.tpl:1-4). |
| <a id="gt-ddb-049"></a>**GT-DDB-049** | Table | shape-mismatch | The write enum (ContributorInsightsAction: ENABLE/DISABLE) differs from the read enum (ContributorInsightsStatus: ENABLING/ENABLED/DISABLING/DISABLED/FAILED), so the submitted value never round-trips literally. | ensureContibutorInsight() accepts ENABLE/ENABLED/DISABLE/DISABLED and normalizes both sides to an action before comparing (hooks.go:733-744, 913-930); the observed status string is written into latest.Spec.ContributorInsights (hooks.go:905-908). |
| <a id="gt-ddb-050"></a>**GT-DDB-050** | Table | async-state-machine | Contributor Insights enable/disable is asynchronous: DescribeContributorInsights reports ENABLING or DISABLING for a while (and FAILED on error, e.g. CloudWatch rule limit or insufficient permissions) before settling on ENABLED/DISABLED. | isTableContributorInsightsUpdating() makes sdkFind requeue (10s) while ENABLING/DISABLING (hooks.go:140-147; sdk_read_one_post_set_output.go.tpl:63-68). |
| <a id="gt-ddb-051"></a>**GT-DDB-051** | Table | first-sync-destructive | DescribeContributorInsights always returns a status (DISABLED by default) for every table, so a naive compare of an unset spec against the observed value would act on tables whose insights were enabled out-of-band. | Deliberate no-op: setContributorInsights only populates Spec.ContributorInsights when the user set it, and customPreCompare only compares when desired is non-nil ("Making this field a no-op if user does not set it") (hooks.go:733-744, 905-908). This is the opposite of the TTL/PITR treatment ([GT-DDB-052](#gt-ddb-052) (controller hooks catalog entry)). |
| <a id="gt-ddb-052"></a>**GT-DDB-052** | Table | first-sync-destructive | DescribeTimeToLive, DescribeContinuousBackups and DescribeTable always report a value for TTL, PITR and DeletionProtectionEnabled (DISABLED/false by default), and DescribeTable/ ListTagsOfResource/GetResourcePolicy list GSIs, tags and policies regardless of origin; there is no "unmanaged" marker. | customPreCompare substitutes defaults for an unset desired value - TTL {Enabled:false}, PITR {PointInTimeRecoveryEnabled:false}, DeletionProtectionEnabled=false (hooks.go:686-696, 729-731) - and treats length mismatches of GSIs/tags/policy as deltas (hooks.go:641-651, 679-685, 745), so a spec that omits them disables/removes out-of-band configuration on first sync. |
| <a id="gt-ddb-053"></a>**GT-DDB-053** | Table | sub-resource-api | The resource-based policy is accepted inline by CreateTable (ResourcePolicy) but afterwards is managed only via PutResourcePolicy / GetResourcePolicy / DeleteResourcePolicy; DescribeTable never returns it. | Spec.ResourcePolicy sourced from PutResourcePolicy.Policy with compare ignored (generator.yaml:32-37); CreateTableInput.ResourcePolicy is sent at create (pkg/resource/table/sdk.go:1015-1017); syncResourcePolicy puts/deletes on delta and getResourcePolicyWithContext reads it in sdkFind (hooks_resource_policy.go:30-137, hooks.go:181-192, 567-573). |
| <a id="gt-ddb-054"></a>**GT-DDB-054** | Table | identity | PutResourcePolicy/GetResourcePolicy/DeleteResourcePolicy, TagResource/UntagResource and ListTagsOfResource are keyed by the resource ARN (ResourceArn), not by TableName, so they cannot be issued until the table ARN is known from CreateTable/DescribeTable output. | customUpdateTable skips the policy sync with requeueWaitWhileCreating while Status.ACKResourceMetadata.ARN is nil (hooks.go:182-186); put/delete/get return an error without ARN (hooks_resource_policy.go:60-63, 85-88, 118-120); tag sync reads the ARN from latest, which fixed an adoption panic (13b019e, hooks_tags.go:45-70). |
| <a id="gt-ddb-055"></a>**GT-DDB-055** | Table | error-code | GetResourcePolicy on a table without a policy returns PolicyNotFoundException (not an empty policy). DeleteResourcePolicy is idempotent and only returns PolicyNotFoundException when an ExpectedRevisionId is supplied. | getResourcePolicyWithContext maps PolicyNotFoundException to a nil policy (hooks_resource_policy.go:129-132); deleteResourcePolicy swallows PolicyNotFoundException as success (hooks_resource_policy.go:97-103); e2e get_resource_policy returns None on it (table.py:335-353). |
| <a id="gt-ddb-056"></a>**GT-DDB-056** | Table | eventual-consistency | Resource policies are eventually consistent: GetResourcePolicy right after PutResourcePolicy may return PolicyNotFoundException or the previous policy, right after DeleteResourcePolicy may still return the deleted policy, and right after CreateTable-with-policy may return ResourceNotFoundException or PolicyNotFoundException. PutResourcePolicy is idempotent (same document -> same RevisionId). | Not handled explicitly; the controller re-reads on each reconcile and re-Puts/deletes if the semantic compare still differs (idempotent). e2e waits for ACK.ResourceSynced and 90s before reading the policy ("Need to wait and use an arn to query", test_table.py:1145-1180). |
| <a id="gt-ddb-057"></a>**GT-DDB-057** | Table | normalization | The policy document returned by GetResourcePolicy is not byte-identical to what was submitted (whitespace, key ordering, scalar-vs-array forms of Action/Resource/Principal), so a string compare never settles. | compareResourcePolicyDocument unmarshals both documents with github.com/micahhausler/aws-iam-policy and uses reflect.DeepEqual on the parsed structures (hooks_resource_policy.go:139-177); unit tests cover whitespace differences (hooks_resource_policy_test.go:115-144); dependency bumped to v0.4.4 for parse failures (4bd6d06). |
| <a id="gt-ddb-058"></a>**GT-DDB-058** | Table | async-state-machine | Resource policy and tag mutations are accepted while the table is UPDATING (they are not gated on TableStatus like UpdateTable is). | customUpdateTable syncs tags and the resource policy before the UPDATING/terminal-status checks and returns early if nothing else changed (hooks.go:175-196; comment "ResourcePolicy can be updated independently of table state"). |
| <a id="gt-ddb-059"></a>**GT-DDB-059** | Table | read-gap | Tags are accepted by CreateTable but never returned by DescribeTable; they must be read with ListTagsOfResource (ARN keyed, paginated via NextToken, documented at 10 calls/second per account) and are returned in arbitrary order. | getResourceTagsPagesWithContext pages through ListTagsOfResource and stores the result in Spec.Tags during sdkFind (hooks_tags.go:138-168, hooks.go:549-553); Tags compare is is_ignored with order-insensitive equalTags (generator.yaml:62-64, hooks_tags.go:75-83). |
| <a id="gt-ddb-060"></a>**GT-DDB-060** | Table | tag-semantics | There is no tag-update API: TagResource upserts values for existing keys and UntagResource removes by key list; both are limited to 5 calls/second per account and are applied asynchronously. | computeTagsDelta yields added+updated tags (sent via one TagResource) and removed keys (one UntagResource) (hooks_tags.go:27-73, 109-136); unit test Test_computeTagsDelta (hooks_test.go:55-133). |
| <a id="gt-ddb-061"></a>**GT-DDB-061** | Table | eventual-consistency | Tags written with TagResource (or at CreateTable) take a few seconds to appear in ListTagsOfResource (the tagging backend is asynchronous). | e2e sleeps MODIFY_WAIT_AFTER_SECONDS (raised from 5s to 10s in e953ae5 because "it sometimes takes a little while for the tags to appear in the Tagris APIs", now 90s) before asserting tags (test_table.py:277-349). |
| <a id="gt-ddb-062"></a>**GT-DDB-062** | Table | normalization | DescribeTable returns AttributeDefinitions (and KeySchema, GSIs, LSIs) in its own order (attribute definitions sorted by name), not the order submitted in CreateTable. | AttributeDefinitions/KeySchema/GSIs/LSIs comparisons are is_ignored in generator.yaml (51-69) and replaced by order-insensitive set comparisons in customPreCompare (hooks.go:621-663, 749-788; hooks_global_secondary_indexes.go:41-80); unit test Test_newResourceDelta_customDeltaFunction_AttributeDefinitions. |
| <a id="gt-ddb-063"></a>**GT-DDB-063** | Table | request-validation | AttributeDefinitions is only meaningful in UpdateTable together with GlobalSecondaryIndexUpdates (it must include the key attributes of a new index); it cannot be changed on its own, and the description keeps attributes used only by indexes. Creating a GSI that reuses already-defined attributes needs no AttributeDefinitions change. | The full desired AttributeDefinitions list is attached to every GSI create/update/delete UpdateTable call (hooks_global_secondary_indexes.go:175-178, 241-244, 298-301); the GSI update path triggers on a GSI delta alone, not on an AttributeDefinitions delta (78ec5b0, issue 1920); e2e test_create_gsi_same_attributes (0aeffae). |
| <a id="gt-ddb-064"></a>**GT-DDB-064** | Table | immutable-field | The table KeySchema (partition/sort key) cannot be changed after CreateTable; UpdateTable has no KeySchema parameter. | KeySchema is_immutable (CEL rule self == oldSelf on the CRD) with custom order-insensitive compare (generator.yaml:55-58, hooks.go:621-627); runtime getImmutableFieldChanges hook removed in favour of CEL (664e61b). |
| <a id="gt-ddb-065"></a>**GT-DDB-065** | Table | immutable-field | LocalSecondaryIndexes can only be defined at CreateTable (max 5); they cannot be added, removed or modified by UpdateTable. DescribeTable returns them as LocalSecondaryIndexDescription (adds IndexArn, IndexSizeBytes, ItemCount). | LocalSecondaryIndexes is_immutable (CEL) with custom compare that ignores description-only fields (generator.yaml:66-69, hooks.go:653-663, 815-880). |
| <a id="gt-ddb-066"></a>**GT-DDB-066** | Table | immutable-field | An existing GSI's KeySchema and Projection are immutable; UpdateGlobalSecondaryIndexAction only accepts ProvisionedThroughput / OnDemandThroughput / WarmThroughput. Changing key schema or projection requires deleting and recreating the index (one op per call). | updateGSIs only sends IndexName + ProvisionedThroughput + OnDemandThroughput (hooks_global_secondary_indexes.go:246-254) although equalGlobalSecondaryIndexes also diffs Projection/KeySchema (hooks_global_secondary_indexes.go:112-129). |
| <a id="gt-ddb-067"></a>**GT-DDB-067** | Table | shape-mismatch | The GSI input shape (GlobalSecondaryIndex) and the DescribeTable output shape (GlobalSecondaryIndexDescription) differ: the description adds IndexStatus, Backfilling, IndexArn, ItemCount, IndexSizeBytes and reports ProvisionedThroughput as a description with NumberOfDecreasesToday/LastIncreaseDateTime. | Generated sdkFind maps the description onto Spec.GlobalSecondaryIndexes (dropping extras) and the readOne hook copies the status-only members into the custom read-only Status.GlobalSecondaryIndexesDescriptions (generator.yaml:38-41, sdk_read_one_post_set_output.go.tpl:1-28). |
| <a id="gt-ddb-068"></a>**GT-DDB-068** | Table | async-state-machine | Creating a GSI (backfill) on a live table takes from minutes to well over an hour; because only one index operation runs at a time, N index changes take N sequential backfills. | e2e GSI waits scale from MODIFY_WAIT*20 (30 min) to *60 (90 min) for update+add (test_table.py:724-729, 774-785, 864-876); controller requeues every 10s via requeueWaitGSIReady until IndexStatus ACTIVE. |
| <a id="gt-ddb-069"></a>**GT-DDB-069** | Table | server-default | DeletionProtectionEnabled defaults to false and is always present in DescribeTable output; while true, DeleteTable is rejected with ValidationException. | customPreCompare defaults an unset desired value to false (hooks.go:729-731); DeletionProtectionEnabled is updated in the syncTable UpdateTable call (hooks.go:242-247, 427-429). Delete rejection is not handled (ValidationException is terminal). |
| <a id="gt-ddb-070"></a>**GT-DDB-070** | Table | request-validation | OnDemandThroughput (MaxReadRequestUnits/MaxWriteRequestUnits, at least one required) is only valid for PAY_PER_REQUEST tables/indexes and is mutually exclusive with ProvisionedThroughput; GSI on-demand limits are set per index through the same Create/Update GSI actions. | OnDemandThroughput shape un-ignored and wired for table and GSIs in d503de1 (syncTableOnDemandThroughput separate UpdateTable call, hooks.go:283-286, 475-495; newSDKOnDemandThroughput, hooks_global_secondary_indexes.go:401-414); equalGlobalSecondaryIndexes compares it (hooks_global_secondary_indexes.go:101-111). e2e table_on_demand_throughput. |
| <a id="gt-ddb-071"></a>**GT-DDB-071** | Table | scope | WarmThroughput (table, GSI and replica; CreateTableInput.WarmThroughput, TableWarmThroughputDescription, GlobalSecondaryIndexWarmThroughputDescription) and OnDemandThroughputOverride exist in the API model but are pre-warming/replica-override knobs with their own description shapes. | Shapes ignored in generator.yaml (shape_names 3-6, field_paths 15) - deferred, not modeled in the CRD (0f98642). |
| <a id="gt-ddb-072"></a>**GT-DDB-072** | Table | scope | MultiRegionConsistency (EVENTUAL/STRONG) is a creation-time property of a global table (settable only in the UpdateTable call that creates replicas) and is reported in TableDescription. | TableDescription.MultiRegionConsistency excluded from the CRD "Deferring proper Spec/mutable implementation" (generator.yaml:13-14, e4a4d8c). |
| <a id="gt-ddb-073"></a>**GT-DDB-073** | Table | error-code | DynamoDB control-plane errors split into transient ones - LimitExceededException (per-table index/encryption limits and account-wide concurrent table operations), ResourceInUseException (table busy / name in use), InternalServerError - and request errors that will never succeed on retry: ValidationException and the SDK-side InvalidParameter. | terminal_codes = [InvalidParameter, ValidationException] (generator.yaml:88-90); the original list InternalServerError/LimitExceededException/ResourceInUseException (e786654) was replaced in b0b0d59 because those must be retried; LimitExceededException from GSI ops maps to requeue ([GT-DDB-010](#gt-ddb-010) (controller hooks catalog entry)). |
| <a id="gt-ddb-074"></a>**GT-DDB-074** | Table | codegen-artifact | Not an AWS behavior: the generated terminalAWSError() uses errors.As on smithy.APIError, so a hook that wraps the SDK error with fmt.Errorf("%v") hides a terminal ValidationException and the controller retries an invalid update forever (ACK.Recoverable instead of ACK.Terminal). | All wrap sites use %w (2cf76bb); unit tests Test_customUpdateTable_preservesTerminalAWSError / Test_customUpdateTable_setsTerminalCondition (hooks_test.go:592-700) and e2e test_terminal_condition_for_invalid_table_class (test_table.py:1182-1244). |
| <a id="gt-ddb-075"></a>**GT-DDB-075** | Table | error-code | A missing table is signalled by the generic code ResourceNotFoundException from DescribeTable/UpdateTable/DeleteTable (also from DescribeTimeToLive/DescribeContinuousBackups while the table is not yet visible). | exceptions.errors.404 = ResourceNotFoundException (generator.yaml:84-87) -> ackerr.NotFound in sdkFind (pkg/resource/table/sdk.go:83-86). |
| <a id="gt-ddb-076"></a>**GT-DDB-076** | Table | async-state-machine | Sub-resource settings (TTL, Contributor Insights, PITR, replicas, resource policy) cannot be applied in the same request as CreateTable (except ResourcePolicy and Tags) and fail while the table is CREATING; they must be applied by follow-up calls once ACTIVE. | sdk_create_post_set_output sets ACK.ResourceSynced=False when TTL or ContributorInsights are specified so the update path runs after creation (templates/hooks/table/sdk_create_post_set_output.go.tpl:1-4); PITR/replicas/policy are picked up by the normal delta on the next reconcile (bcd26e1 moved the calls out of sdkCreate because they "usually fail as the Table may still be in Creating status"). |
| <a id="gt-ddb-077"></a>**GT-DDB-077** | Table | async-state-machine | A table declared with several GSIs and replicas takes several minutes before its first reconcile completes (CreateTable with indexes, then replica creation serialized one per call), far longer than a plain table (~30s). | e2e waits up to 5 minutes for the CR to be consumed by the controller (default ~30s) and REPLICA_WAIT 900s for ACTIVE (test_table_replicas.py:182-222); CREATE_WAIT 30s before creating tables (test_table.py:34, 82). |
| <a id="gt-ddb-078"></a>**GT-DDB-078** | Table | cross-region | A table that has replicas reports GlobalTableVersion "2019.11.21" and its Replicas list in DescribeTable; this version is independent of (and incompatible with) the legacy CreateGlobalTable/DescribeGlobalTable resource for the same table name. | Table.Status.GlobalTableVersion and Status.Replicas are generated read-only fields; replicas are managed on the Table resource (TableReplicas) rather than via the GlobalTable CRD (755ecd8, issue 2077). |
| <a id="gt-ddb-079"></a>**GT-DDB-079** | GlobalTable | scope | CreateGlobalTable/UpdateGlobalTable/DescribeGlobalTable implement global tables version 2017.11.29 (Legacy). New accounts/regions reject CreateGlobalTable with "ValidationException: One or more parameter values were invalid: DynamoDB global tables version 2017.11.29 is not supported. We recommend using DynamoDB global tables version 2019.11.21". | GlobalTable CRD is still generated but its e2e suite is skipped with that error as the reason (test_global_table.py:82, commit 089de59); replicas are supported on Table.tableReplicas instead ([GT-DDB-032](#gt-ddb-032) (controller hooks catalog entry)). |
| <a id="gt-ddb-080"></a>**GT-DDB-080** | GlobalTable | prerequisite | CreateGlobalTable/UpdateGlobalTable do not create tables: every region in ReplicationGroup must already contain an empty table with the same name, same key schema, same provisioned/maximum write capacity and DynamoDB Streams enabled with NEW_AND_OLD_IMAGES; otherwise TableNotFoundException ("Table not found: Table: 'x' not found in region: 'y'") or ValidationException is returned. | No controller logic; e2e creates a streams-enabled Table with the same name first and documents "Global Tables must have the same name as dynamodb Tables" (test_global_table.py:41-79, 102-103, resources/global_table.yaml). |
| <a id="gt-ddb-081"></a>**GT-DDB-081** | GlobalTable | delete-semantics | There is no DeleteGlobalTable API; a legacy global table disappears when all replicas are removed with UpdateGlobalTable ReplicaUpdates [{Delete:{RegionName}}...]. | UpdateGlobalTable is declared as both the Update and the Delete operation (generator.yaml:17-21, 96235cf) and customSetDeleteInput fills ReplicaUpdates with one Delete per Spec.ReplicationGroup region (pkg/resource/global_table/custom_api.go:21-30, sdk.go:337-355). |
| <a id="gt-ddb-082"></a>**GT-DDB-082** | GlobalTable | request-validation | UpdateGlobalTable requires a non-empty ReplicaUpdates list (Create or Delete replica actions); it has no other mutable parameters, so any ReplicationGroup change must be expressed as add/remove actions. | The generated sdkUpdate builds UpdateGlobalTableInput with only GlobalTableName and no ReplicaUpdates (pkg/resource/global_table/sdk.go:320-333); no custom update hook exists. |
| <a id="gt-ddb-083"></a>**GT-DDB-083** | GlobalTable | error-code | Missing legacy global tables are signalled by GlobalTableNotFoundException from DescribeGlobalTable (not ResourceNotFoundException). | exceptions.errors.404 = GlobalTableNotFoundException (generator.yaml:126-129; pkg/resource/global_table/sdk.go:83-85). |
| <a id="gt-ddb-084"></a>**GT-DDB-084** | GlobalTable | async-state-machine | GlobalTableStatus moves through CREATING / UPDATING / DELETING before ACTIVE; replica additions are asynchronous. | synced.when Status.GlobalTableStatus in [ACTIVE] (generator.yaml:135-139, ce5980c). |
| <a id="gt-ddb-085"></a>**GT-DDB-085** | GlobalTable | shape-mismatch | ReplicationGroup is submitted as Replica{RegionName} but DescribeGlobalTable returns ReplicaDescription (RegionName plus ReplicaStatus, KMSMasterKeyId, GSIs, ...); the global table ARN (GlobalTableArn) is only available from the describe/create output. | Generated sdkFind copies only RegionName back into Spec.ReplicationGroup (pkg/resource/global_table/sdk.go:115-127). |
| <a id="gt-ddb-086"></a>**GT-DDB-086** | GlobalTable | tag-semantics | Legacy global tables have no taggable resource; TagResource/ListTagsOfResource apply to tables only. | tags.ignore true for GlobalTable (generator.yaml:133-134). |
| <a id="gt-ddb-087"></a>**GT-DDB-087** | Backup | identity | A backup is addressed only by its BackupArn (DescribeBackup/DeleteBackup take BackupArn; there is no lookup by BackupName/TableName other than ListBackups filtering); the ARN is returned by CreateBackup. | requiredFieldsMissingFromReadOneInput returns NotFound until Status.ACKResourceMetadata.ARN is set; describe/delete payloads use the ARN (pkg/resource/backup/sdk.go:140-158, 285-291). An unsupported primary_identifier_field_name override was removed (119b264). |
| <a id="gt-ddb-088"></a>**GT-DDB-088** | Backup | shape-mismatch | DescribeBackup nests the backup attributes two levels deep (BackupDescription.BackupDetails) next to SourceTableDetails/SourceTableFeatureDetails, whereas CreateBackup returns BackupDetails at the top level. | output_wrapper_field_path BackupDescription.BackupDetails for DescribeBackup (generator.yaml:22-23, 589cde3). |
| <a id="gt-ddb-089"></a>**GT-DDB-089** | Backup | async-state-machine | CreateBackup is asynchronous: BackupStatus is CREATING until the backup becomes AVAILABLE (usually within a minute or two), and deleted backups report DELETED. The API doc string says the states are "CREATING, ACTIVE, DELETED" but the enum/actual value is AVAILABLE. | synced.when Status.BackupStatus in [AVAILABLE, DELETED] (generator.yaml:150-155); sdkFind requeues while CREATING (templates/hooks/backup/sdk_read_one_post_set_output.go.tpl:1-3, pkg/resource/backup/hooks.go:30-35, 52-60); e2e waits up to 100s for AVAILABLE (test_backup.py:121-127). |
| <a id="gt-ddb-090"></a>**GT-DDB-090** | Backup | immutable-field | Backups have no update API; BackupName and TableName are fixed at CreateBackup. | Generated sdkUpdate returns a terminal NotImplemented error (pkg/resource/backup/sdk.go:251-258); TerminalStatuses for Backup is intentionally empty (pkg/resource/backup/hooks.go:24-28). |
| <a id="gt-ddb-091"></a>**GT-DDB-091** | Backup | error-code | A missing backup is BackupNotFoundException; related codes are BackupInUseException (delete while CREATING), TableNotFoundException / TableInUseException (source table missing or CREATING/DELETING) and ContinuousBackupsUnavailableException on CreateBackup. | exceptions.errors.404 = BackupNotFoundException (generator.yaml:141-144; pkg/resource/backup/sdk.go:82-84); no terminal codes configured for Backup (sdk.go:393-399), so the other codes are retried. |
| <a id="gt-ddb-092"></a>**GT-DDB-092** | Backup | prerequisite | CreateBackup requires the source table to exist and be ACTIVE (TableInUseException while CREATING/DELETING). | No controller gating; e2e fixture waits for the table to reach ACTIVE before creating the Backup CR (test_backup.py:37-75). |
| <a id="gt-ddb-093"></a>**GT-DDB-093** | Backup | delete-semantics | After DeleteBackup, DescribeBackup returns BackupNotFoundException within seconds (the backup may transiently report DELETED). | DELETED is included in synced.when (generator.yaml:150-155); e2e asserts non-existence 10s after delete (test_backup.py:35, 135-143). |
| <a id="gt-ddb-094"></a>**GT-DDB-094** | Backup | tag-semantics | Backup ARNs are not taggable (TagResource/ListTagsOfResource accept table ARNs only). | tags.ignore true for Backup (generator.yaml:148-149). |

## Supplementary notes

<!-- preserved:start -->
<!-- preserved:end -->
