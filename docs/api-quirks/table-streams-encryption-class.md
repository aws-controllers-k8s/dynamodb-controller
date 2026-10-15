<!-- generated: ack-api-quirks dynamodb 2026-10-09 model=2012-08-10 controller=34b85e6 -->
# Table streams, encryption (SSE/KMS), table class, deletion protection
_Round-trip, mutation rules, quotas, KMS key states and async phases for streams, encryption, table class and deletion protection._
Generated from ack-api-quirks `services/dynamodb` (render date in the marker above); model 2012-08-10 (service/dynamodb v1.39.8); controller commit 34b85e6; evidence: `services/dynamodb/probes/<probe id>/` in the lab repo.

## Overview

<!-- preserved:start id=overview -->
This document covers StreamSpecification, SSESpecification/KMS (including the INACCESSIBLE_ENCRYPTION_CREDENTIALS state), TableClass and DeletionProtectionEnabled - the UpdateTable fields that each 'must be the only operation in the request' - plus the admission matrix of the CREATING/UPDATING windows they create. The most surprising facts are that UpdateTable is one-logical-change-per-call (33 of 35 field pairs rejected on idle tables in [DDB-TABLE-382](#ddb-table-382); only 2 of 33 combinations accepted on fresh tables in [DDB-TABLE-433](#ddb-table-433)), that an SSE alias re-send is not a no-op but a ~22 s re-encryption that burns one of 4 daily SSE changes, and that a disabled CMK takes 13-75 min to surface as INACCESSIBLE_ENCRYPTION_CREDENTIALS while the data plane already fails after ~5 min ([DDB-TABLE-082](#ddb-table-082), [DDB-TABLE-331](#ddb-table-331)).

### Rules a reconciler must respect
- UpdateTable is single-concern: DeletionProtection, SSE, TableClass and WarmThroughput each 'must be the only operation in the request', OnDemandThroughput combines only with BillingMode, StreamSpecification + ProvisionedThroughput/BillingMode=PROVISIONED is 'cannot modify stream status while updating table IOPS'; only BillingMode=PAY_PER_REQUEST + Stream or + OnDemandThroughput combine; rejected calls apply nothing and the message names SSE > TableClass > DP > Warm; nothing in the controller serializes multi-field deltas ([DDB-TABLE-433](#ddb-table-433), [DDB-TABLE-382](#ddb-table-382)).
- Streams: StreamViewType cannot change in place and an identical re-send is also 'Table already has an enabled stream'; disable must omit StreamViewType and enable must carry one; after disable StreamSpecification is absent (same as never enabled) while LatestStreamArn/Label linger, and re-sending StreamEnabled=false is ValidationException 'Table has no stream to disable'; every re-enable mints a new LatestStreamArn (rotating it in status) and old streams stay listable as DISABLED even after DeleteTable ([DDB-TABLE-050](#ddb-table-050), [DDB-TABLE-029](#ddb-table-029), [DDB-TABLE-035](#ddb-table-035), [DDB-TABLE-362](#ddb-table-362), [DDB-TABLE-367](#ddb-table-367), [DDB-TABLE-378](#ddb-table-378)).
- SSE re-sends: only the exact KMSMasterKeyArn re-send (and Enabled:false on a default-encrypted table) is a rejected no-op; alias, key id, {Enabled:true} and SSEType-only re-sends return 200, re-encrypt for ~21-22 s and each counts toward 4 SSE changes per 24 h (then one per 6 h, LimitExceededException); an alias is resolved once at write time (repointing it does not move the table) - resolve to the ARN before diffing ([DDB-TABLE-082](#ddb-table-082), [DDB-TABLE-141](#ddb-table-141), [DDB-TABLE-078](#ddb-table-078), [DDB-TABLE-081](#ddb-table-081), [DDB-TABLE-018](#ddb-table-018)).
- Unusable KMS keys fail synchronously, leave nothing behind and do not consume the quota: disabled/pending-deletion/nonexistent/other-region/other-account -> ValidationException, kms:CreateGrant denied -> AccessDeniedException, asymmetric RSA_2048 (and HMAC) -> HTTP 500 InternalServerError that is deterministic, not transient - a 5xx-retry loop never escapes it ([DDB-TABLE-020](#ddb-table-020), [DDB-TABLE-021](#ddb-table-021), [DDB-TABLE-022](#ddb-table-022), [DDB-TABLE-023](#ddb-table-023), [DDB-TABLE-037](#ddb-table-037), [DDB-TABLE-139](#ddb-table-139), [DDB-TABLE-461](#ddb-table-461)).
- SSE shape: SSEDescription is absent for the AWS-owned default and after disable (never {Status:DISABLED}); KMSMasterKeyId requires SSEType=KMS; Enabled=false with SSEType/KMSMasterKeyId and SSEType=AES256 are rejected; SSESpecification={} is accepted as 'no SSE' and {SSEType:KMS} alone enables; a key change echoes the OLD KMSMasterKeyArn with Status=UPDATING for 8-9 s in both the response and DescribeTable; a multi-region key must be given as the local region's ARN, bare mrk-id or alias, never the other region's ARN ([DDB-TABLE-027](#ddb-table-027), [DDB-TABLE-031](#ddb-table-031), [DDB-TABLE-036](#ddb-table-036), [DDB-TABLE-044](#ddb-table-044), [DDB-TABLE-079](#ddb-table-079), [DDB-TABLE-371](#ddb-table-371), [DDB-TABLE-328](#ddb-table-328)).
- INACCESSIBLE_ENCRYPTION_CREDENTIALS: a disabled/pending-deletion CMK or revoked grants flip TableStatus 13-75 min later (SSEDescription.Status stays ENABLED, InaccessibleEncryptionDateTime = detection time) while the data plane fails ~5 min after DisableKey; EnableKey alone recovers to ACTIVE in 18-57 min (CancelKeyDeletion alone does not; reads work again long before the status flips); with grants revoked but the key enabled an UpdateTable to another CMK heals it in 30 s; in-state, reads, tags, PITR, policy and DP/stream updates work while TTL, backup, Insights and SSE changes fail; DeleteTable is accepted in-state; a CMK disabled while CREATING leaves the table CREATING ~59 min and then it vanishes with no error ([DDB-TABLE-331](#ddb-table-331), [DDB-TABLE-335](#ddb-table-335), [DDB-TABLE-330](#ddb-table-330), [DDB-TABLE-332](#ddb-table-332), [DDB-TABLE-333](#ddb-table-333), [DDB-TABLE-334](#ddb-table-334)).
- TableClass: 2 changes per 30 days per table (3rd is LimitExceededException, and a re-send of the current class is also rejected once spent, while TableClass=STANDARD on a never-set table is a free no-op that leaves TableClassSummary absent); absent summary = STANDARD; a re-send while UPDATING is ResourceInUseException; an SSE write during a TableClass switch is the one write that resets TableStatus to ACTIVE, and a following TableClass change is accepted (200) but silently lost ([DDB-TABLE-283](#ddb-table-283), [DDB-TABLE-365](#ddb-table-365), [DDB-TABLE-026](#ddb-table-026), [DDB-TABLE-052](#ddb-table-052), [DDB-TABLE-451](#ddb-table-451), [DDB-TABLE-450](#ddb-table-450)).
- DeletionProtection: synchronous (16 ms, TableStatus stays ACTIVE, always present in DescribeTable), a same-value re-send is 200, a second toggle within 15 s is ThrottlingException whose 'Please try again after <ts>' is authoritative but not parsed by the controller; DeleteTable on a protected table is ValidationException 'Disable deletion protection first' and is admitted in the same second as the DP=false call (the cooldown gates only DP toggles) ([DDB-TABLE-017](#ddb-table-017), [DDB-TABLE-030](#ddb-table-030), [DDB-TABLE-003](#ddb-table-003), [DDB-TABLE-016](#ddb-table-016), [DDB-TABLE-438](#ddb-table-438)).
- Admission is per field, not per state: while CREATING every mutation is ResourceInUseException; while TableStatus=UPDATING (stream, PT, billing, TableClass) DeleteTable is ResourceInUseException but DP (and Warm during a billing switch) are admitted and PT/ODT/stream are not, while TTL/PITR/policy/backup/tags pass; while SSEDescription.Status or WarmThroughput.Status is UPDATING with TableStatus ACTIVE, DeleteTable is still ResourceInUseException while DP, stream toggles and TableClass are admitted; six different write APIs fired within 0.12 s on an ACTIVE table all land ([DDB-TABLE-002](#ddb-table-002), [DDB-TABLE-369](#ddb-table-369), [DDB-TABLE-069](#ddb-table-069), [DDB-TABLE-120](#ddb-table-120), [DDB-TABLE-119](#ddb-table-119), [DDB-TABLE-286](#ddb-table-286), [DDB-TABLE-435](#ddb-table-435); [DDB-TABLE-001](table.md#ddb-table-001), table.md; [DDB-TABLE-063](table-throughput-billing.md#ddb-table-063), [DDB-TABLE-459](table-throughput-billing.md#ddb-table-459), table-throughput-billing.md).
- Responses are stale: no-op BillingMode/TableClass re-sends return TableStatus=UPDATING while DescribeTable is already ACTIVE, TableClass/PT responses echo the OLD values, the stream response shows the requested spec during UPDATING while TableClassSummary flips only with ACTIVE, and AttributeDefinitions re-typing a key next to another field is 200 and silently ignored - re-Describe, never persist the response ([DDB-TABLE-177](#ddb-table-177), [DDB-TABLE-284](#ddb-table-284), [DDB-TABLE-180](#ddb-table-180); [DDB-TABLE-156](table-throughput-billing.md#ddb-table-156), [DDB-TABLE-358](table-throughput-billing.md#ddb-table-358), table-throughput-billing.md).

### Timing you should expect
- Stream enable/disable: UPDATING 4.05-5.07 s (n>=4); DP toggle synchronous (16 ms) with a 15 s cooldown (a blocked toggle succeeded at 11.1 s); TableClass switch UPDATING 3.6-6.1 s (n=6; the 31.4 s of [DDB-TABLE-052](#ddb-table-052) is a coarse-polling upper bound) ([DDB-TABLE-050](#ddb-table-050), [DDB-TABLE-002](#ddb-table-002), [DDB-TABLE-018](#ddb-table-018), [DDB-TABLE-017](#ddb-table-017), [DDB-TABLE-003](#ddb-table-003), [DDB-TABLE-284](#ddb-table-284), [DDB-TABLE-365](#ddb-table-365)).
- SSE change (on, off, key change, alias re-send): SSEDescription.Status=UPDATING 21.3-23.3 s with TableStatus ACTIVE throughout; the old key ARN is echoed for 8-9 s ([DDB-TABLE-065](#ddb-table-065), [DDB-TABLE-079](#ddb-table-079), [DDB-TABLE-082](#ddb-table-082), [DDB-TABLE-140](#ddb-table-140), [DDB-TABLE-371](#ddb-table-371)).
- Windows that gate admission: billing switch UPDATING 71.9 s on a fresh table and 2.0 s on a re-flipped one ([DDB-TABLE-369](#ddb-table-369)); PT change 1.0-2.0 s and WarmThroughput job 441-514 s (n=5) ([DDB-TABLE-058](table-throughput-billing.md#ddb-table-058), [DDB-TABLE-459](table-throughput-billing.md#ddb-table-459), table-throughput-billing.md).
- INACCESSIBLE_ENCRYPTION_CREDENTIALS: onset 12.6-75.1 min after DisableKey/RevokeGrant/ScheduleKeyDeletion (5 tables, 30 s polling; data plane failed at 5.5 min); recovery 18.1-56.8 min after EnableKey (3 tables; reads worked within 7 min); CREATING with a disabled key vanished at 59.3 min; DeleteTable in-state gone after 8 s; after DeleteTable the stream is DISABLING at once and DISABLED at +6.1 s ([DDB-TABLE-331](#ddb-table-331), [DDB-TABLE-335](#ddb-table-335), [DDB-TABLE-330](#ddb-table-330), [DDB-TABLE-332](#ddb-table-332), [DDB-TABLE-334](#ddb-table-334), [DDB-TABLE-386](#ddb-table-386)).

### Known handling gaps in the controller
- The hooks catalog assumes an absent StreamSpecification simply means disabled and has no normalization of a desired {streamEnabled:false} against observed nil ([GT-DDB-024](service.md#gt-ddb-024) (controller hooks catalog entry)); confirmed: the stream compare/update path (pkg/resource/table/hooks.go:338-432, 401-420; sdk.go:369-380) re-sends StreamEnabled=false on every reconcile and each re-send is the terminal ValidationException 'Table has no stream to disable' - nil observed must compare equal to {StreamEnabled:false} ([DDB-TABLE-050](#ddb-table-050)).
- The hooks catalog assumes kmsMasterKeyID arrives as an ARN through a kms.Key reference and compares it with plain string equality in customPreCompare (pkg/resource/table/hooks.go:603-611; [GT-DDB-027](service.md#gt-ddb-027) (controller hooks catalog entry)); confirmed: a literal alias or key id never equals the observed KMSMasterKeyArn, so each reconcile re-encrypts (~22 s) and burns one of 4 daily SSE changes, locking the CR out with LimitExceededException within 4 reconciles ([DDB-TABLE-082](#ddb-table-082)).
- INACCESSIBLE_ENCRYPTION_CREDENTIALS is only partially handled: TerminalStatuses covers ARCHIVING/DELETING (pkg/resource/table/hooks.go:60-65, 203-208) but not this status, whose onset (13-75 min) and recovery (18-57 min) lag the key state and whose only early signal is the data plane - a reconciler can only requeue with a long backoff, must not persist InaccessibleEncryptionDateTime as permanent, and must report a table that vanishes while CREATING as a KMS failure instead of waiting for it ([DDB-TABLE-331](#ddb-table-331), [DDB-TABLE-335](#ddb-table-335), [DDB-TABLE-330](#ddb-table-330), [DDB-TABLE-332](#ddb-table-332), [DDB-TABLE-334](#ddb-table-334)).

### Where to look next
- Billing/PT/Warm/OnDemand rules and the decrease budget ([DDB-TABLE-055](table-throughput-billing.md#ddb-table-055), [DDB-TABLE-059](table-throughput-billing.md#ddb-table-059), table-throughput-billing.md); the billing-switch admission window [DDB-TABLE-369](#ddb-table-369) is rendered here because its title names streams. The stream is immutable once replicas exist and every replica needs a regional CMK ([DDB-TABLE-222](table-replicas.md#ddb-table-222), [DDB-TABLE-313](table-replicas.md#ddb-table-313), table-replicas.md); KeySchema/LSIs are structurally immutable ([DDB-TABLE-357](table-indexes.md#ddb-table-357), table-indexes.md).
- 5xx catalogue and ThrottlingException retry-after parsing ([DDB-TABLE-448](service.md#ddb-table-448), [DDB-TABLE-445](service.md#ddb-table-445), [DDB-TABLE-464](service.md#ddb-table-464), service.md). Evidence: services/dynamodb/probes/table/{mutation-matrix,round-trip,weird-inputs,state-machine,response-fidelity,creative}/.

Entries below are generated from the lab findings; low-impact items are in the appendix, long notes under details/.
<!-- preserved:end -->

## At a glance

- canonical findings: 68 (high 42 / medium 21 / low 5); duplicates folded into the appendix: 6
- handling: handled 19 · partial 5 · tracked 4 · unhandled 38 · suspect-bug 2 · n-a 0 (tracked = handled/partial whose reference is an open GitHub issue; counted as not handled)
- re-verified: 4 · last_verified: 2026-10-08..2026-10-09 · model: 2012-08-10 (service/dynamodb v1.39.8)
- categories: async-state-machine 21, error-code 7, delete-semantics 6, quota-limit 5, request-validation 5,
  update-granularity 5, idempotency 4, stale-response 4, identity 3, response-fidelity 3, eventual-consistency
  1, normalization 1, requested-vs-effective 1, server-default 1, sub-resource-api 1
- medium-impact entries are compacted in this document (header, title, first sentence, link) because its full
  form exceeds 1500 lines (render.yaml `size_warn_lines`); each links to its full entry under `details/`.
  High-impact entries are never compacted.

## Operations

| operation | kind | required inputs | declared error shapes | paginated |
| --- | --- | --- | --- | --- |
| CreateBackup | create | TableName, BackupName | TableNotFoundException, TableInUseException, ContinuousBackupsUnavailableException, BackupInUseException, LimitExceededException, InternalServerError | no |
| CreateTable | create | TableName | ResourceInUseException, LimitExceededException, InternalServerError | no |
| DeleteResourcePolicy | delete | ResourceArn | ResourceNotFoundException, InternalServerError, PolicyNotFoundException, ResourceInUseException, LimitExceededException | no |
| DeleteTable | delete | TableName | ResourceInUseException, ResourceNotFoundException, LimitExceededException, InternalServerError | no |
| DescribeBackup | read | BackupArn | BackupNotFoundException, InternalServerError | no |
| DescribeContributorInsights | read | TableName | ResourceNotFoundException, InternalServerError | no |
| DescribeTable | read | TableName | ResourceNotFoundException, InternalServerError | no |
| GetItem | read | TableName, Key | ProvisionedThroughputExceededException, ResourceNotFoundException, RequestLimitExceeded, InternalServerError, ThrottlingException | no |
| GetResourcePolicy | read | ResourceArn | ResourceNotFoundException, InternalServerError, PolicyNotFoundException | no |
| ListTagsOfResource | list | ResourceArn | ResourceNotFoundException, InternalServerError | yes |
| PutItem | create | TableName, Item | ConditionalCheckFailedException, ProvisionedThroughputExceededException, ResourceNotFoundException, ItemCollectionSizeLimitExceededException, TransactionConflictException, RequestLimitExceeded, InternalServerError, ReplicatedWriteConflictException, ThrottlingException | no |
| PutResourcePolicy | create | ResourceArn, Policy | ResourceNotFoundException, InternalServerError, LimitExceededException, PolicyNotFoundException, ResourceInUseException | no |
| RestoreTableFromBackup | create | TargetTableName, BackupArn | TableAlreadyExistsException, TableInUseException, BackupNotFoundException, BackupInUseException, LimitExceededException, InternalServerError | no |
| RestoreTableToPointInTime | create | TargetTableName | TableAlreadyExistsException, TableNotFoundException, TableInUseException, LimitExceededException, InvalidRestoreTimeException, PointInTimeRecoveryUnavailableException, InternalServerError | no |
| TagResource | tag | ResourceArn, Tags | LimitExceededException, ResourceNotFoundException, InternalServerError, ResourceInUseException | no |
| UntagResource | tag | ResourceArn, TagKeys | LimitExceededException, ResourceNotFoundException, InternalServerError, ResourceInUseException | no |
| UpdateContinuousBackups | update | TableName, PointInTimeRecoverySpecification | TableNotFoundException, ContinuousBackupsUnavailableException, InternalServerError | no |
| UpdateContributorInsights | update | TableName, ContributorInsightsAction | ResourceNotFoundException, InternalServerError | no |
| UpdateTable | update | TableName | ResourceInUseException, ResourceNotFoundException, LimitExceededException, InternalServerError | no |
| UpdateTimeToLive | update | TableName, TimeToLiveSpecification | ResourceInUseException, ResourceNotFoundException, LimitExceededException, InternalServerError | no |

Also referenced by findings but not in the dynamodb model (other services or annotated variants):
DescribeStream, ListStreams

## State machine

- **IndexStatus**: CREATING, UPDATING, DELETING, ACTIVE (transitional: CREATING, UPDATING, DELETING)
- **ReplicaStatus**: CREATING, CREATION_FAILED, UPDATING, DELETING, ACTIVE, REGION_DISABLED,
  INACCESSIBLE_ENCRYPTION_CREDENTIALS, ARCHIVING, ARCHIVED, REPLICATION_NOT_AUTHORIZED (transitional:
  CREATING, UPDATING, DELETING, ARCHIVING)
- **SSEStatus**: ENABLING, ENABLED, DISABLING, DISABLED, UPDATING (transitional: ENABLING, DISABLING,
  UPDATING)
- **TableStatus**: CREATING, UPDATING, DELETING, ACTIVE, INACCESSIBLE_ENCRYPTION_CREDENTIALS, ARCHIVING,
  ARCHIVED, REPLICATION_NOT_AUTHORIZED (transitional: CREATING, UPDATING, DELETING, ARCHIVING)
- **WitnessStatus**: CREATING, DELETING, ACTIVE (transitional: CREATING, DELETING)

- <a id="ddb-table-002"></a>**DDB-TABLE-002** `async-state-machine` · impact high · unhandled (not handled in controller) · verified 2026-10-08
  **While UPDATING (stream enable), DeleteTable=ResourceInUseException/400 but UpdateTable(DeletionProtectionEnabled)=OK(200); UPDATING ~4.71s**
  After UpdateTable(StreamSpecification enable) the table is UPDATING for 4.71 s. In that window DeleteTable
  -> ResourceInUseException/400, UpdateTable(DeletionProtectionEnabled) -> OK(200), re-sending the identical
  StreamSpecification -> ResourceInUseException/400, duplicate CreateTable -> ResourceInUseException/400.
  Message e.g. 'Attempt to change a resource which is still in use: Cannot delete table while stream is being
  enabled/disabled.'.
  - ACK: requeue, deletable.when, updateable.when · ops: UpdateTable, DeleteTable
  - repro: UpdateTable(StreamSpecification{true,NEW_IMAGE}) then immediately DeleteTable / UpdateTable(DP)
  - measurements: updating_duration_s=4.71
  - handling: not handled in the controller (as of commit 34b85e6)
  - related: [DDB-TABLE-001](table.md#ddb-table-001), [DDB-TABLE-370](table-throughput-billing.md#ddb-table-370), [DDB-TABLE-369](#ddb-table-369), [DDB-TABLE-052](#ddb-table-052), [DDB-TABLE-285](#ddb-table-285), [DDB-TABLE-120](#ddb-table-120),
    [DDB-TABLE-065](#ddb-table-065), [DDB-TABLE-459](table-throughput-billing.md#ddb-table-459), [DDB-TABLE-054](#ddb-table-054), [DDB-TABLE-010](table.md#ddb-table-010), [DDB-TABLE-069](#ddb-table-069), [DDB-TABLE-066](table-throughput-billing.md#ddb-table-066), [DDB-TABLE-284](#ddb-table-284),
    [DDB-TABLE-334](#ddb-table-334), [DDB-TABLE-050](#ddb-table-050), [DDB-TABLE-035](#ddb-table-035), [DDB-TABLE-018](#ddb-table-018), [DDB-TABLE-029](#ddb-table-029), [DDB-TABLE-040](table-throughput-billing.md#ddb-table-040), [DDB-TABLE-036](#ddb-table-036) ·
    evidence: table/state-machine/admissibility-matrix
  - notes: Hypotheses: H-T-002, H-T-001. H-T-002 UPDATING half confirmed for Delete; H-T-001's 'UpdateTable
    rejected while UPDATING' is REFUTED for the DeletionProtectionEnabled field (admitted, 200, response still
    says UPDATING) - admissibility is per-field, not per-state.

- <a id="ddb-table-017"></a>**DDB-TABLE-017** `async-state-machine` · impact medium · handled · verified 2026-10-08
  **UpdateTable(DeletionProtectionEnabled) is synchronous: response TableStatus=ACTIVE, DP=False;
  DescribeTable immediately agrees (False)** - On a quiet ACTIVE table
  UpdateTable(DeletionProtectionEnabled=false) returned HTTP 200 with TableDescription.TableStatus=ACTIVE and
  DeletionProtectionEnabled=False; DescribeTable immediately afterwards: TableStatus=AC... - see:
  [details/DDB-TABLE-017.md](details/DDB-TABLE-017.md)

- <a id="ddb-table-052"></a>**DDB-TABLE-052** `async-state-machine` · impact medium · handled · verified 2026-10-08
  **TableClass STANDARD -> STANDARD_INFREQUENT_ACCESS: re-send while UPDATING -> ResourceInUse, DP admitted;
  31.4 s is a polling upper bound** - UpdateTable TableClass=STANDARD_INFREQUENT_ACCESS on an empty PPR table
  -> OK (TableStatus=UPDATING). - see: [details/DDB-TABLE-052.md](details/DDB-TABLE-052.md)

- <a id="ddb-table-065"></a>**DDB-TABLE-065** `async-state-machine` · impact medium · handled · verified 2026-10-08
  **SSE switch to KMS: TableStatus stays ACTIVE, SSEDescription.Status=UPDATING ~22s; Delete/SSE change
  rejected meanwhile** - UpdateTable(SSESpecification{Enabled:true,SSEType:KMS}) response: TableStatus=ACTIVE,
  SSEDescription={'Status': 'UPDATING'}. - see: [details/DDB-TABLE-065.md](details/DDB-TABLE-065.md)

- <a id="ddb-table-079"></a>**DDB-TABLE-079** `async-state-machine` · impact high · tracked in GitHub issue (not handled) · verified 2026-10-08
  **SSE CMK -> disabled: SSEDescription disappears after ~22s (TableStatus stays ACTIVE); later changes hit the 4-per-24h quota**
  From a CMK table, UpdateTable SSESpecification{Enabled:false} -> 200 with TableStatus=ACTIVE and
  SSEDescription{Status:UPDATING,...old key}; DescribeTable showed SSEDescription.Status=UPDATING for 22.3s
  (TableStatus never left ACTIVE), then SSEDescription was ABSENT (not {Status:DISABLED}). Enabled:false again
  -> ValidationException 'One or more parameter values were invalid: Table is already encrypted by default'.
  Every further SSE change on this table (Enabled:true, Enabled:true+SSEType:KMS, alias/aws/dynamodb, CMK ARN,
  bad keys) failed with LimitExceededException because 4 encryption changes had already been made in the 24h
  window (see the quota finding); those transitions are re-tested on fresh tables in
  table/mutation-matrix/sse-kms-quota.
  - ACK: custom_update, requeue, compare.is_ignored+delta_pre_compare · ops: UpdateTable, DescribeTable ·
    fields: SSESpecification, SSEDescription.Status, SSEDescription.KMSMasterKeyArn
  - repro: PPR table with CMK; UpdateTable SSESpecification Enabled:false; Enabled:true; Enabled:true again;
    KMSMasterKeyId=alias/aws/dynamodb; CMK ARN
  - measurements: disable_sse_updating_s=22.26
  - handling: tracked in https://github.com/aws-controllers-k8s/community/issues/2136 (not handled) · code refs: `pkg/resource/table/hooks.go:338-432; generator.yaml:91-92; generator.yaml:10-11; generator.yaml:70-72; pkg/resource/table/hooks.go:583-619; pkg/resource/table/hooks.go:434-473; test/e2e/tests/test_table.py:630-673; pkg/resource/table/hooks.go:446-465; test/e2e/tests/test_table.py:641-673`
  - related: [DDB-TABLE-082](#ddb-table-082), [DDB-TABLE-078](#ddb-table-078), [DDB-TABLE-141](#ddb-table-141), [DDB-TABLE-140](#ddb-table-140), [DDB-TABLE-371](#ddb-table-371), [DDB-TABLE-065](#ddb-table-065),
    [DDB-TABLE-081](#ddb-table-081), [DDB-TABLE-142](#ddb-table-142), [DDB-TABLE-120](#ddb-table-120), [DDB-TABLE-027](#ddb-table-027), [DDB-TABLE-036](#ddb-table-036), [DDB-TABLE-044](#ddb-table-044), [DDB-TABLE-031](#ddb-table-031) ·
    evidence: table/mutation-matrix/sse-kms
  - notes: Hypotheses: H-T-112, H-T-034. H-T-034 confirmed for the disable case: 'SSE disabled' and 'SSE never
    set' are both SSEDescription absent. H-T-112 AWS-managed re-send could not be tested on this table
    (quota); see sse-kms-quota.

- <a id="ddb-table-117"></a>**DDB-TABLE-117** `async-state-machine` · impact medium · unhandled (not handled in controller) · verified 2026-10-08
  **DP toggle never shows UPDATING (stays ACTIVE); stream enable gives a ~7 s UPDATING window in which all
  TTL/PITR/Insights calls are admitted** - UpdateTable(DeletionProtectionEnabled=true) response
  TableStatus=ACTIVE, timeline [{'value': 'ACTIVE', 'from_s': 0.01, 'to_s': None, 'duration_s': None}]; ops
  right after (table ACTIVE->ACTIVE): {'DescribeTimeToLive':... - see:
  [details/DDB-TABLE-117.md](details/DDB-TABLE-117.md)

- <a id="ddb-table-119"></a>**DDB-TABLE-119** `async-state-machine` · impact high · unhandled (not handled in controller) · verified 2026-10-08
  **While TableStatus=UPDATING (stream toggle): Delete/TableClass/OnDemandThroughput rejected, DP/SSE/Backup/TTL/PITR/policy/Warm admitted**
  Each op fired right after UpdateTable(StreamSpecification toggle) returned TableStatus=UPDATING (fresh ~4s
  window per op): {'delete': 'ResourceInUseException', 'dp_true': 'OK(UPDATING)', 'tableclass_ia':
  'ResourceInUseException', 'sse_toggle': 'OK(ACTIVE)', 'create_backup': 'OK', 'ttl_enable': 'OK',
  'pitr_enable': 'OK', 'put_resource_policy': 'OK', 'ondemand_throughput': 'ResourceInUseException',
  'warm_increase': 'OK(UPDATING)'}. Rejections: {'delete': 'Attempt to change a resource which is still in
  use: Cannot delete table while stream is being enabled/disabled.', 'tableclass_ia': "Attempt to change a
  resource which is still in use: Can't update table class when stream status is being updated. Table:",
  'ondemand_throughput': 'Attempt to change a resource which is still in use: OnDemandThroughput cannot be
  updated while stream status update is i'}.
  - ACK: updateable.when, requeue, synced.when · ops: UpdateTable, DeleteTable, CreateBackup,
    UpdateTimeToLive, UpdateContinuousBackups, PutResourcePolicy · fields: DeletionProtectionEnabled,
    TableClass, SSESpecification, OnDemandThroughput, WarmThroughput, StreamSpecification
  - repro: UpdateTable(StreamSpecification toggle); immediately issue the op; fresh toggle per op
  - handling: not handled in the controller (as of commit 34b85e6)
  - related: [DDB-TABLE-117](#ddb-table-117), [DDB-TABLE-234](table-policy-kinesis-autoscaling.md#ddb-table-234), [DDB-TABLE-287](#ddb-table-287), [DDB-TABLE-450](#ddb-table-450), [DDB-TABLE-460](table-policy-kinesis-autoscaling.md#ddb-table-460), [DDB-TABLE-435](#ddb-table-435),
    [DDB-TABLE-121](table-throughput-billing.md#ddb-table-121), [DDB-TABLE-172](service.md#ddb-table-172), [DDB-TABLE-348](table-policy-kinesis-autoscaling.md#ddb-table-348), [DDB-TABLE-210](table-policy-kinesis-autoscaling.md#ddb-table-210), [DDB-TABLE-432](table-policy-kinesis-autoscaling.md#ddb-table-432) · evidence:
    table/state-machine/field-admissibility-while-updating
  - notes: Refines H-T-001: UPDATING is not a blanket ResourceInUseException. Rejection messages are
    field-specific ('Can't update table class when stream s...', 'OnDemandThroughput cannot be updated w...',
    'Cannot delete table while stream is being enabled/disabled'). An SSE switch, a WarmThroughput increase...
  - full notes: [details/DDB-TABLE-119.md](details/DDB-TABLE-119.md)

- <a id="ddb-table-120"></a>**DDB-TABLE-120** `async-state-machine` · impact high · handled · verified 2026-10-08
  **While SSEDescription.Status=UPDATING (TableStatus ACTIVE): Delete rejected; DP, stream toggle and TableClass change admitted**
  Ops fired right after UpdateTable(SSESpecification toggle) (TableStatus stays ACTIVE,
  SSEDescription.Status=UPDATING ~20s): {'delete': 'ResourceInUseException', 'dp_false': 'OK(ACTIVE)',
  'stream_toggle': 'OK(UPDATING)', 'tableclass_standard': 'OK(UPDATING)'}. Rejections: {'delete': 'Attempt to
  change a resource which is still in use: Table: ackq-8d9518-fadm is in the process of being updated.'}. The
  4th SSE toggle within ~9 minutes failed: LimitExceededException 'Subscriber limit exceeded: Encryption mode
  changes are limited in the 24h window ending at <ts>. After the first 4 change, each subsequent change in
  the same window can be performe[d]...'.
  - ACK: synced.when, deletable.when, requeue, terminal_codes · ops: UpdateTable, DeleteTable · fields:
    SSESpecification, SSEDescription.Status, DeletionProtectionEnabled, StreamSpecification, TableClass
  - repro: UpdateTable(SSESpecification{Enabled:true,SSEType:KMS}); immediately DeleteTable /
    UpdateTable(other field); repeat toggles >4 times in 24h
  - handling: handled via `pkg/resource/table/hooks.go:434-473; test/e2e/tests/test_table.py:630-673`
  - related: [DDB-TABLE-283](#ddb-table-283), [DDB-TABLE-365](#ddb-table-365), [DDB-TABLE-052](#ddb-table-052), [DDB-TABLE-284](#ddb-table-284), [DDB-TABLE-285](#ddb-table-285), [DDB-TABLE-451](#ddb-table-451),
    [DDB-TABLE-287](#ddb-table-287), [DDB-TABLE-180](#ddb-table-180), [DDB-TABLE-001](table.md#ddb-table-001), [DDB-TABLE-002](#ddb-table-002), [DDB-TABLE-370](table-throughput-billing.md#ddb-table-370), [DDB-TABLE-369](#ddb-table-369), [DDB-TABLE-065](#ddb-table-065),
    [DDB-TABLE-459](table-throughput-billing.md#ddb-table-459), [DDB-TABLE-054](#ddb-table-054), [DDB-TABLE-010](table.md#ddb-table-010), [DDB-TABLE-069](#ddb-table-069), [DDB-TABLE-066](table-throughput-billing.md#ddb-table-066), [DDB-TABLE-334](#ddb-table-334), [DDB-TABLE-082](#ddb-table-082),
    [DDB-TABLE-078](#ddb-table-078), [DDB-TABLE-141](#ddb-table-141), [DDB-TABLE-140](#ddb-table-140), [DDB-TABLE-371](#ddb-table-371), [DDB-TABLE-079](#ddb-table-079), [DDB-TABLE-081](#ddb-table-081), [DDB-TABLE-142](#ddb-table-142) ·
    evidence: table/state-machine/field-admissibility-while-updating
  - notes: Two findings in one window: (1) TableStatus=ACTIVE is not 'quiet' - a stream toggle started during
    an SSE update made both UPDATING at once; (2) SSE mode flips are quota-limited to 4 per rolling 24h per
    table (LimitExceededException, not ValidationException), so a controller flapping SSE settings...
  - full notes: [details/DDB-TABLE-120.md](details/DDB-TABLE-120.md)

- <a id="ddb-table-140"></a>**DDB-TABLE-140** `async-state-machine` · impact high · handled · verified 2026-10-08
  **SSE transitions CMK->CMK, CMK->AWS managed, AWS managed->off->on, off->CMK: phases, durations and which re-sends are no-ops**
  T1 (CMK K1): K1->K2 by ARN -> OK (resp TableStatus=ACTIVE, resp SSE={"Status": "UPDATING", "SSEType": "KMS",
  "KMSMasterKeyArn": "arn:aws:kms:us-west-2:<ACCOUNT>:key/8963cda5-0fac-4af1-8635-0af67d16fe14"}; 22.31s via
  ACTIVE/UPDATING/0af67d16fe14->ACTIVE/UPDATING/07e049320145->ACTIVE/ENABLED/07e049320145; after={"Status":
  "ENABLED", "SSEType": "KMS", "KMSMasterKeyArn":
  "arn:aws:kms:us-west-2:<ACCOUNT>:key/5494d1e8-65e0-4fdc-9814-07e049320145"}). ->Enabled:true (AWS managed)
  -> OK (resp TableStatus=ACTIVE, resp SSE={"Status": "UPDATING", "SSEType": "KMS", "KMSMasterKeyArn":
  "arn:aws:kms:us-west-2:<ACCOUNT>:key/5494d1e8-65e0-4fdc-9814-07e049320145"}; 21.29s via
  ACTIVE/UPDATING/07e049320145->ACTIVE/UPDATING/14f2ee58148a->ACTIVE/ENABLED/14f2ee58148a; after={"Status":
  "ENABLED", "SSEType": "KMS", "KMSMasterKeyArn":
  "arn:aws:kms:us-west-2:<ACCOUNT>:key/301f0bb6-edf0-476a-9525-14f2ee58148a"}). Enabled:true again -> OK (resp
  TableStatus=ACTIVE, resp SSE={"Status": "UPDATING", "SSEType": "KMS", "KMSMasterKeyArn":
  "arn:aws:kms:us-west-2:<ACCOUNT>:key/301f0bb6-edf0-476a-9525-14f2ee58148a"}; 21.31s via
  ACTIVE/UPDATING/14f2ee58148a->ACTIVE/ENABLED/14f2ee58148a; after={"Status": "ENABLED", "SSEType": "KMS",
  "KMSMasterKeyArn": "arn:aws:kms:us-west-2:<ACCOUNT>:key/301f0bb6-edf0-476a-9525-14f2ee58148a"}).
  alias/aws/dynamodb explicit -> OK (resp TableStatus=ACTIVE, resp SSE={"Status": "UPDATING", "SSEType":
  "KMS", "KMSMasterKeyArn": "arn:aws:kms:us-west-2:<ACCOUNT>:key/301f0bb6-edf0-476a-9525-14f2ee58148a"};
  21.31s via ACTIVE/UPDATING/14f2ee58148a->ACTIVE/ENABLED/14f2ee58148a; after={"Status": "ENABLED", "SSEType":
  "KMS", "KMSMasterKeyArn": "arn:aws:kms:us-west-2:<ACCOUNT>:key/301f0bb6-edf0-476a-9525-14f2ee58148a"}).
  Enabled:false -> OK (resp TableStatus=ACTIVE, resp SSE={"Status": "UPDATING", "SSEType": "KMS",
  "KMSMasterKeyArn": "arn:aws:kms:us-west-2:<ACCOUNT>:key/301f0bb6-edf0-476a-9525-14f2ee58148a"}; 21.3s via
  ACTIVE/UPDATING/14f2ee58148a->ACTIVE/UPDATING/->ACTIVE/<absent>/None; after="<absent>"). Enabled:true after
  disable -> LimitExceededException: 'Subscriber limit exceeded: Encryption mode changes are limited in the
  24h window ending at 2026-10-09T23:23:37.938Z. After the first 4 change, each subsequent change in the same
  window can be performed at most once every 21600 seconds. Number of updates today: 5. Last change at
  2026-10-08T23:25:04.5'. T3 (never encrypted with a specified key): Enabled:true -> OK (resp
  TableStatus=ACTIVE, resp SSE={"Status": "UPDATING"}; 22.32s via
  ACTIVE/UPDATING/->ACTIVE/UPDATING/14f2ee58148a->ACTIVE/ENABLED/14f2ee58148a; after={"Status": "ENABLED",
  "SSEType": "KMS", "KMSMasterKeyArn":
  "arn:aws:kms:us-west-2:<ACCOUNT>:key/301f0bb6-edf0-476a-9525-14f2ee58148a"}). Enabled:false -> OK (resp
  TableStatus=ACTIVE, resp SSE={"Status": "UPDATING", "SSEType": "KMS", "KMSMasterKeyArn":
  "arn:aws:kms:us-west-2:<ACCOUNT>:key/301f0bb6-edf0-476a-9525-14f2ee58148a"}; 22.32s via
  ACTIVE/UPDATING/14f2ee58148a->ACTIVE/UPDATING/->ACTIVE/<absent>/None; after="<absent>"). CMK K1 -> OK (resp
  TableStatus=ACTIVE, resp SSE={"Status": "UPDATING"}; 22.33s via
  ACTIVE/UPDATING/->ACTIVE/UPDATING/0af67d16fe14->ACTIVE/ENABLED/0af67d16fe14; after={"Status": "ENABLED",
  "SSEType": "KMS", "KMSMasterKeyArn":
  "arn:aws:kms:us-west-2:<ACCOUNT>:key/8963cda5-0fac-4af1-8635-0af67d16fe14"}). Enabled:false -> OK (resp
  TableStatus=ACTIVE, resp SSE={"Status": "UPDATING", "SSEType": "KMS", "KMSMasterKeyArn":
  "arn:aws:kms:us-west-2:<ACCOUNT>:key/8963cda5-0fac-4af1-8635-0af67d16fe14"}; 23.35s via
  ACTIVE/UPDATING/0af67d16fe14->ACTIVE/UPDATING/->ACTIVE/<absent>/None; after="<absent>").
  - ACK: requeue, synced.when, custom_update · ops: UpdateTable, DescribeTable · fields: SSESpecification,
    SSEDescription.Status, SSEDescription.KMSMasterKeyArn
  - repro: see behavior; poll DescribeTable at 1s after each UpdateTable
  - measurements: create-t1=8.12, create-t2=0.01, create-t3=0.01, t1-k1-to-k2-arn=22.31,
    t1-k2-to-aws-managed=21.29, t1-aws-managed-resend=21.31, t1-aws-alias-explicit=21.31, t1-disable=21.3,
    t2-resend-enabled-true=21.31, t2-enabled-true-type-kms=21.31, t2-aws-alias-explicit=21.32,
    t2-disable=21.3, t3-enable-aws-managed=22.32, t3-disable=22.32, t3-cmk-k1=22.33, t3-disable-2=23.35
  - handling: handled via `pkg/resource/table/hooks.go:434-473; test/e2e/tests/test_table.py:630-673`
  - related: [DDB-TABLE-060](table-throughput-billing.md#ddb-table-060), [DDB-TABLE-370](table-throughput-billing.md#ddb-table-370), [DDB-TABLE-284](#ddb-table-284), [DDB-TABLE-179](table-throughput-billing.md#ddb-table-179), [DDB-TABLE-066](table-throughput-billing.md#ddb-table-066), [DDB-TABLE-371](#ddb-table-371),
    [DDB-TABLE-064](table-throughput-billing.md#ddb-table-064), [DDB-TABLE-183](table-throughput-billing.md#ddb-table-183), [DDB-TABLE-180](#ddb-table-180), [DDB-TABLE-177](#ddb-table-177), [DDB-TABLE-156](table-throughput-billing.md#ddb-table-156), [DDB-TABLE-082](#ddb-table-082), [DDB-TABLE-078](#ddb-table-078),
    [DDB-TABLE-141](#ddb-table-141), [DDB-TABLE-065](#ddb-table-065), [DDB-TABLE-079](#ddb-table-079), [DDB-TABLE-081](#ddb-table-081), [DDB-TABLE-142](#ddb-table-142), [DDB-TABLE-120](#ddb-table-120), [DDB-TABLE-027](#ddb-table-027),
    [DDB-TABLE-036](#ddb-table-036), [DDB-TABLE-044](#ddb-table-044), [DDB-TABLE-031](#ddb-table-031) · evidence: table/mutation-matrix/sse-kms-quota
  - notes: Hypotheses: H-T-112, H-T-034. Phase shapes: CMK->CMK: SSE Status UPDATING with the OLD key ARN,
    then UPDATING with the NEW key ARN, then ENABLED (~22s). -> AWS owned (Enabled:false): UPDATING old key ->
    {Status:UPDATING} without SSEType/KMSMasterKeyArn -> SSEDescription absent (~21s). AWS owned ->...
  - full notes: [details/DDB-TABLE-140.md](details/DDB-TABLE-140.md)

- <a id="ddb-table-284"></a>**DDB-TABLE-284** `async-state-machine` · impact medium · handled · verified 2026-10-09
  **TableClass switch: UPDATING ~3.6-4.1 s; TableClassSummary flips with ACTIVE; UpdateTable echoes the OLD
  summary; Delete refused until ACTIVE** - 0.5 s DescribeTable polling on empty PPR tables. - see:
  [details/DDB-TABLE-284.md](details/DDB-TABLE-284.md)

- <a id="ddb-table-285"></a>**DDB-TABLE-285** `async-state-machine` · impact medium · unhandled (not handled in controller) · verified 2026-10-09
  **DeletionProtection write during a TableClass switch does NOT reset TableStatus; attempt-1 anomaly traced
  to the SSE write** - UpdateTable(TableClass=IA) then UpdateTable(DeletionProtectionEnabled=true) at +0.05 s
  -> OK (response TableStatus=UPDATING). - see: [details/DDB-TABLE-285.md](details/DDB-TABLE-285.md)

- <a id="ddb-table-286"></a>**DDB-TABLE-286** `async-state-machine` · impact high · unhandled (not handled in controller) · verified 2026-10-09
  **While a TableClass switch is UPDATING: stream/OnDemandThroughput/TableClass ResourceInUse; SSE, DP, tags, TTL, PITR, backup, policy OK**
  Ops fired 0-0.66 s after UpdateTable(TableClass=IA) returned TableStatus=UPDATING (DescribeTable view just
  before each op: ["['ACTIVE', None, None]", "['UPDATING', None, None]"]): {'tableclass_resend_same':
  'ResourceInUseException', 'tableclass_reverse': 'ResourceInUseException', 'stream_enable':
  'ResourceInUseException', 'ondemand_throughput': 'ResourceInUseException', 'sse_aws_managed': 'OK(ACTIVE)',
  'tag_resource': 'OK', 'ttl_enable': 'OK', 'pitr_enable': 'OK', 'create_backup': 'OK', 'put_resource_policy':
  'OK', 'dp_true': 'OK(ACTIVE)'}. Rejection messages: {'tableclass_resend_same': "Attempt to change a resource
  which is still in use: Can't update table class when a table class update is in progress. Table:
  ackq-61b715-tc-ops TableClassUpdateInProgress: STANDARD_INFREQUENT_ACCESS", 'tableclass_reverse': "Attempt
  to change a resource which is still in use: Can't update table class when a table class update is in
  progress. Table: ackq-61b715-tc-ops TableClassUpdateInProgress: STANDARD_INFREQUENT_ACCESS",
  'stream_enable': "Attempt to change a resource which is still in use: Can't change stream status when a
  table class update is in progress. Table: ackq-61b715-tc-ops TableClassUpdateInProgress:
  STANDARD_INFREQUENT_ACCESS", 'ondemand_throughput': 'Attempt to change a resource which is still in use:
  OnDemandThroughput cannot be updated while TableClass update is in progress. TableClassUpdateInProgress:
  STANDARD_INFREQUENT_ACCESS'}. DescribeTable after the switch settled: {'DeletionProtectionEnabled': True,
  'StreamSpecification': None, 'SSEDescription': {'Status': 'ENABLED', 'SSEType': 'KMS', 'KMSMasterKeyArn':
  'arn:aws:kms:us-west-2:<ACCOUNT>:key/301f0bb6-edf0-476a-9525-14f2ee58148a'}, 'OnDemandThroughput': None,
  'GSIs': [], 'TableClassSummary': {'TableClass': 'STANDARD_INFREQUENT_ACCESS', 'LastUpdateDateTime':
  '2026-10-09 00:55:02.345000+00:00'}}. Switch timing with the ops interleaved: UPDATING 0 s, class flipped at
  3.59 s.
  - ACK: updateable.when, deletable.when, requeue, synced.when · ops: UpdateTable, DeleteTable, TagResource,
    UpdateTimeToLive, UpdateContinuousBackups, CreateBackup, PutResourcePolicy · fields: TableClass,
    DeletionProtectionEnabled, StreamSpecification, SSESpecification, OnDemandThroughput,
    GlobalSecondaryIndexUpdates
  - repro: UpdateTable(TableClass=STANDARD_INFREQUENT_ACCESS) on a PPR table; immediately issue each op once
  - handling: not handled in the controller (as of commit 34b85e6)
  - related: [DDB-TABLE-166](table-indexes.md#ddb-table-166), [DDB-TABLE-163](table-subresources.md#ddb-table-163), [DDB-TABLE-376](table-indexes.md#ddb-table-376), [DDB-TABLE-458](table-indexes.md#ddb-table-458), [DDB-TABLE-175](table-indexes.md#ddb-table-175), [DDB-TABLE-174](table-indexes.md#ddb-table-174),
    [DDB-TABLE-382](#ddb-table-382), [DDB-TABLE-462](table-indexes.md#ddb-table-462), [DDB-TABLE-287](#ddb-table-287), [DDB-TABLE-450](#ddb-table-450), [DDB-TABLE-135](table-indexes.md#ddb-table-135), [DDB-TABLE-154](table-throughput-billing.md#ddb-table-154), [DDB-TABLE-456](service.md#ddb-table-456),
    [DDB-TABLE-128](table-indexes.md#ddb-table-128), [DDB-TABLE-153](table-indexes.md#ddb-table-153), [DDB-TABLE-375](table-indexes.md#ddb-table-375), [DDB-TABLE-164](table-indexes.md#ddb-table-164), [DDB-TABLE-155](table-indexes.md#ddb-table-155) · hypotheses: H-T-001, H-T-002 ·
    evidence: table/state-machine/table-class-switch
  - notes: TableClass flavour of the per-field UPDATING admissibility matrix (wave 1 covered the stream-toggle
    and SSE flavours; wave 1 also showed GSI Create is ResourceInUse during a TableClass switch, so it was not
    re-attempted here). CAUTION: the SSE write at +0.2 s was accepted AND flipped TableStatus to...
  - full notes: [details/DDB-TABLE-286.md](details/DDB-TABLE-286.md)

- <a id="ddb-table-287"></a>**DDB-TABLE-287** `async-state-machine` · impact high · unhandled (not handled in controller) · verified 2026-10-09
  **SSE write during a TableClass switch flips TableStatus to ACTIVE at once; a 2nd TableClass change is then accepted but silently lost**
  Table 'sse': UpdateTable(TableClass=IA) -> UPDATING; UpdateTable(SSESpecification{Enabled:true,SSEType:KMS})
  at +0.16 s -> OK with TableStatus=ACTIVE; DescribeTable at +0.17 s: ACTIVE, TableClassSummary absent,
  SSEDescription.Status=UPDATING. UpdateTable(TableClass=STANDARD) at +0.19 s -> OK (response UPDATING)
  although the IA job was still running; UpdateTable(OnDemandThroughput) at +0.22 s -> OK (normally
  ResourceInUseException during a TableClass switch). TableClassSummary showed IA at +9.1 s
  (LastUpdateDateTime 01:00:38) and never STANDARD: the accepted STANDARD request was silently dropped. A
  further TableClass=STANDARD request afterwards -> OK and applied (so the dropped request did not count
  against the 2-per-30-days quota). Controls on sibling tables: TTL write / PITR write / no write ->
  TableStatus stayed UPDATING, every TableClass re-send was ResourceInUseException ('Can't update table class
  when a table class update is in progress') until the class flipped at 4.6/4.7/4.0 s; in the no-write control
  TableClassSummary showed the new class ~0.2 s before TableStatus returned to ACTIVE.
  - ACK: one-per-reconcile, synced.when, requeue, custom_update · ops: UpdateTable, UpdateTimeToLive,
    UpdateContinuousBackups, DescribeTable · fields: TableClass, SSESpecification, TableStatus,
    TableClassSummary
  - repro: UpdateTable(TableClass=IA); UpdateTable(SSESpecification{Enabled:true,SSEType:KMS}) 0.1 s later;
    DescribeTable every 0.2 s; UpdateTable(TableClass=STANDARD) every 0.5 s
  - handling: not handled in the controller (as of commit 34b85e6)
  - related: [DDB-TABLE-283](#ddb-table-283), [DDB-TABLE-365](#ddb-table-365), [DDB-TABLE-052](#ddb-table-052), [DDB-TABLE-284](#ddb-table-284), [DDB-TABLE-285](#ddb-table-285), [DDB-TABLE-451](#ddb-table-451),
    [DDB-TABLE-180](#ddb-table-180), [DDB-TABLE-120](#ddb-table-120), [DDB-TABLE-286](#ddb-table-286), [DDB-TABLE-458](table-indexes.md#ddb-table-458), [DDB-TABLE-462](table-indexes.md#ddb-table-462), [DDB-TABLE-175](table-indexes.md#ddb-table-175), [DDB-TABLE-450](#ddb-table-450),
    [DDB-TABLE-117](#ddb-table-117), [DDB-TABLE-119](#ddb-table-119), [DDB-TABLE-234](table-policy-kinesis-autoscaling.md#ddb-table-234), [DDB-TABLE-460](table-policy-kinesis-autoscaling.md#ddb-table-460), [DDB-TABLE-435](#ddb-table-435), [DDB-TABLE-121](table-throughput-billing.md#ddb-table-121) · hypotheses:
    H-T-001, H-T-013 · evidence: table/creative/tableclass-sse-race
  - notes: Follow-up of table/state-machine/table-class-switch attempt 1 (five TableClass changes accepted in
    14 s after a DP+SSE combo). The SSE path (which keeps TableStatus=ACTIVE by design) overwrites the
    table-level UPDATING marker set by the asynchronous TableClass job. A controller that sends TableClass...
  - full notes: [details/DDB-TABLE-287.md](details/DDB-TABLE-287.md)

- <a id="ddb-table-325"></a>**DDB-TABLE-325** `async-state-machine` · impact high · unhandled (not handled in controller) · verified 2026-10-09
  **While INACCESSIBLE_ENCRYPTION_CREDENTIALS, UpdateTable BillingMode/TableClass/OnDemand/Warm/stream/DP are all accepted and applied**
  Target: CMK-encrypted PPR table whose key had been disabled ~65 min earlier
  (TableStatus=INACCESSIBLE_ENCRYPTION_CREDENTIALS, SSEDescription.Status=ENABLED; KMS key verified Disabled
  throughout). UpdateTable with one field at a time: WarmThroughput 12001/4001 -> OK (status stays
  INACCESSIBLE); OnDemandThroughput -> OK; TableClass=STANDARD_INFREQUENT_ACCESS -> OK, TableStatus=UPDATING
  for 4 s then back to INACCESSIBLE_ENCRYPTION_CREDENTIALS (not ACTIVE); BillingMode=PROVISIONED 1/1 -> OK,
  UPDATING 78.5 s then INACCESSIBLE again; StreamSpecification disable -> OK, UPDATING 4 s;
  DeletionProtectionEnabled=false -> OK. DescribeTable afterwards shows every change applied (PROVISIONED, IA,
  stream absent, DP false). Only KMS-dependent calls fail in this state (SSESpecification changes,
  UpdateTimeToLive, CreateBackup, UpdateContributorInsights, GetItem/PutItem -> ValidationException 'KMS key
  disabled error: ...DisabledException', see table/state-machine/kms-inaccessible-lifecycle).
  - ACK: updateable.when, synced.when, terminal_codes · ops: UpdateTable, DescribeTable · fields: BillingMode,
    TableClass, OnDemandThroughput, WarmThroughput, StreamSpecification, DeletionProtectionEnabled
  - repro: CMK table; kms DisableKey; wait for INACCESSIBLE_ENCRYPTION_CREDENTIALS; UpdateTable with each
    field alone
  - handling: not handled in the controller (as of commit 34b85e6)
  - related: [DDB-TABLE-333](#ddb-table-333) · hypotheses: H-T-103 · evidence: table/mutation-matrix/inaccessible-update-table
  - notes: REFUTES H-T-103 (every UpdateTable rejected while INACCESSIBLE): only SSESpecification changes are
    refused; all other table-level updates go through, each with a transient UPDATING that returns to
    INACCESSIBLE_ENCRYPTION_CREDENTIALS rather than ACTIVE. A controller must therefore not use...
  - full notes: [details/DDB-TABLE-325.md](details/DDB-TABLE-325.md)

- <a id="ddb-table-331"></a>**DDB-TABLE-331** `async-state-machine` · impact high · partially handled · verified 2026-10-09
  **CMK disabled -> INACCESSIBLE_ENCRYPTION_CREDENTIALS after 13-43 min, pending-deletion key after 75 min; data plane fails after ~5 min**
  Five PPR tables on four CMKs, DescribeTable every 30 s. Minutes from the KMS action to the first poll
  showing TableStatus=INACCESSIBLE_ENCRYPTION_CREDENTIALS: DisableKey, zero traffic ('quiet'): 12.6;
  DisableKey, GetItem every 30 s ('traf'): 41.7; sibling table on the same disabled key, no traffic ('del'):
  36.2; RevokeGrant on the table's grants, key Enabled ('grants'): 43.2; ScheduleKeyDeletion(7 days) ('pend'):
  75.1 (InaccessibleEncryptionDateTime 01:34:36 for an action at 00:19:32). The two tables sharing one key
  flipped 5.5 min apart, so detection is per table, not per key, and traffic does not accelerate it. In every
  case the sequence was ACTIVE -> INACCESSIBLE_ENCRYPTION_CREDENTIALS with SSEDescription.Status still
  ENABLED, KMSMasterKeyArn unchanged and SSEDescription.InaccessibleEncryptionDateTime added (its value is
  20-30 s before the first poll that showed the status, i.e. it is the detection time, not the KMS action
  time); ArchivalSummary absent. Data plane on 'traf': GetItem kept succeeding for 5.5 min after DisableKey
  (cached data key), then failed on every call (120/120) with ValidationException 'KMS key disabled error:
  com.amazonaws.services.kms.model.DisabledException: arn:aws:kms:...:key/... is disabled. (Service: AWSKMS;
  Status Code: 400; Error Code: DisabledException; ...)' while TableStatus stayed ACTIVE for another 36 min.
  - ACK: synced.when, requeue, terminal_codes · ops: DescribeTable, GetItem · fields: TableStatus,
    SSEDescription.Status, SSEDescription.InaccessibleEncryptionDateTime
  - repro: CreateTable(SSESpecification KMS CMK) -> ACTIVE -> kms DisableKey | ScheduleKeyDeletion(7d) ->
    DescribeTable every 30s
  - measurements: detect_quiet_s=754.6, detect_traf_s=2504.5, detect_del_s=2172.7, detect_grants_s=2595.0,
    detect_pend_s=4504, get_item_first_failure_s=332.2
  - handling: partially handled via `pkg/resource/table/hooks.go:60-65; pkg/resource/table/hooks.go:203-208` - see Handling gaps
  - related: [DDB-TABLE-330](#ddb-table-330), [DDB-TABLE-335](#ddb-table-335), [DDB-TABLE-332](#ddb-table-332), [DDB-TABLE-334](#ddb-table-334) · hypotheses: H-T-101, H-T-102,
    H-T-105 · evidence: table/state-machine/kms-inaccessible-lifecycle
  - notes: H-T-101 first half: confirmed for the status/InaccessibleEncryptionDateTime shape, but the '<30
    min' bound is refuted (13-75 min, apparently a slow per-table periodic check). H-T-102: the only reliable
    signal that the key is unusable is the data plane (fails within ~5 min) - TableStatus lags by up...
  - full notes: [details/DDB-TABLE-331.md](details/DDB-TABLE-331.md)

- <a id="ddb-table-332"></a>**DDB-TABLE-332** `async-state-machine` · impact medium · partially handled · verified 2026-10-09
  **CMK disabled while CREATING: table stuck CREATING ~59 min then silently vanishes; revoked grants ->
  INACCESSIBLE in 43 min, repairable** - 'creating': CreateTable(CMK) returned TableStatus=CREATING;
  DisableKey ~0.3 s later. - see: [details/DDB-TABLE-332.md](details/DDB-TABLE-332.md)

- <a id="ddb-table-333"></a>**DDB-TABLE-333** `async-state-machine` · impact high · unhandled (not handled in controller) · verified 2026-10-09
  **INACCESSIBLE_ENCRYPTION_CREDENTIALS: reads, tags, PITR, policy, DP/stream updates OK; data plane, TTL, backup, insights, SSE changes fail**
  Table in INACCESSIBLE_ENCRYPTION_CREDENTIALS (ListTables still lists it). OK: ListTagsOfResource,
  DescribeTimeToLive, DescribeContinuousBackups, DescribeKinesisStreamingDestination,
  DescribeContributorInsights, ListBackups, GetResourcePolicy (PolicyNotFoundException as for any table
  without a policy), TagResource (UntagResource 0.1 s later -> LimitExceededException 'Table tags are being
  updated' = the usual tag write lock), UpdateContinuousBackups(PITR on), PutResourcePolicy,
  UpdateTable(DeletionProtectionEnabled=true), UpdateTable(StreamSpecification enable) -> TableStatus UPDATING
  10 s then back to INACCESSIBLE_ENCRYPTION_CREDENTIALS (not ACTIVE). Rejected with ValidationException 'KMS
  key disabled error: com.amazonaws.services.kms.model.DisabledException: <key arn> is disabled. (Service:
  AWSKMS; Status Code: 400; Error Code: DisabledException ...)': GetItem (consistent and eventually
  consistent), PutItem, UpdateTimeToLive, CreateBackup, UpdateContributorInsights, and every SSESpecification
  change (to another enabled CMK, to the AWS managed key alias/aws/dynamodb, to the AWS owned key
  Enabled=false) - the error names the OLD disabled key; re-sending the same key -> ValidationException 'Table
  is already encrypted with given KMSMasterKey'. BillingMode/TableClass/OnDemandThroughput/WarmThroughput were
  ResourceInUseException here only because the stream enable was still in flight; re-tested alone they are all
  accepted (table/mutation-matrix/inaccessible-update-table).
  - ACK: synced.when, updateable.when, terminal_codes, tags.custom-sync, requeue · ops: DescribeTable,
    ListTagsOfResource, TagResource, UntagResource, UpdateTable, UpdateTimeToLive, UpdateContinuousBackups,
    CreateBackup, PutResourcePolicy, UpdateContributorInsights, GetItem, PutItem · fields: TableStatus,
    SSESpecification, DeletionProtectionEnabled, StreamSpecification, BillingMode, TableClass,
    OnDemandThroughput
  - repro: CMK table -> kms DisableKey -> wait for INACCESSIBLE_ENCRYPTION_CREDENTIALS -> issue each
    read/mutation once
  - handling: not handled in the controller (as of commit 34b85e6)
  - related: [DDB-TABLE-325](#ddb-table-325) · hypotheses: H-T-102, H-T-103, H-T-136 · evidence:
    table/state-machine/kms-inaccessible-lifecycle
  - notes: H-T-102 confirmed (reads work, data plane fails with a ValidationException carrying the KMS
    DisabledException text - no dedicated code). H-T-103 REFUTED: only SSE changes (and KMS-touching
    sub-resources TTL/backup/insights) are refused; DP, streams, PITR, tags, policy, billing, class,
    throughput...
  - full notes: [details/DDB-TABLE-333.md](details/DDB-TABLE-333.md)

- <a id="ddb-table-335"></a>**DDB-TABLE-335** `async-state-machine` · impact high · partially handled · verified 2026-10-09
  **Re-enabling the CMK: ACTIVE again after 18 / 44 / 57 min (3 tables), InaccessibleEncryptionDateTime cleared; CancelKeyDeletion alone no help**
  EnableKey on the disabled CMKs (30 s polling): 'quiet' ACTIVE after 1087.7 s (18.1 min); 'traf' after 3410 s
  (56.8 min, measured by table/consistency-windows/inaccessible-recovery-lag because it exceeded this probe's
  45-min recovery budget); 'pend' (ScheduleKeyDeletion'd key): CancelKeyDeletion leaves KeyState=Disabled and
  the table stayed INACCESSIBLE_ENCRYPTION_CREDENTIALS for the following 45.3 min; EnableKey then -> ACTIVE
  after 2654.2 s (44.2 min). In every case the sequence was INACCESSIBLE_ENCRYPTION_CREDENTIALS -> ACTIVE in
  one step with SSEDescription.InaccessibleEncryptionDateTime removed in the same poll (cleared, not frozen);
  SSEDescription afterwards is {Status: ENABLED, SSEType: KMS, KMSMasterKeyArn: <same key>}. No DynamoDB API
  call was needed.
  - ACK: synced.when, requeue, is_read_only · ops: DescribeTable · fields: TableStatus,
    SSEDescription.InaccessibleEncryptionDateTime
  - repro: INACCESSIBLE table -> kms EnableKey (or CancelKeyDeletion then EnableKey) -> DescribeTable every
    30s
  - measurements: recover_quiet_s=1087.7, recover_traf_s=3410, recover_pend_after_enable_s=2654.2,
    pend_cancel_only_no_recovery_observed_min=45.3
  - handling: partially handled via `pkg/resource/table/hooks.go:60-65; pkg/resource/table/hooks.go:203-208` - see Handling gaps
  - related: [DDB-TABLE-331](#ddb-table-331), [DDB-TABLE-330](#ddb-table-330), [DDB-TABLE-332](#ddb-table-332), [DDB-TABLE-334](#ddb-table-334) · hypotheses: H-T-105 · evidence:
    table/state-machine/kms-inaccessible-lifecycle
  - notes: H-T-105 recovery half confirmed except for the '<30 min' bound: 18-57 min observed, i.e. the same
    slow periodic check as detection. A controller can only requeue; InaccessibleEncryptionDateTime must not
    be persisted as a permanent field. The 'archival clock restarts' half is untested.
  - full notes: [details/DDB-TABLE-335.md](details/DDB-TABLE-335.md)

- <a id="ddb-table-336"></a>**DDB-TABLE-336** `async-state-machine` · impact medium · unhandled (not handled in controller) · verified 2026-10-09 · status: unverified
  **UNTESTED: 7-day archival path (ARCHIVING/ARCHIVED, ArchivalSummary, system backup) after
  INACCESSIBLE_ENCRYPTION_CREDENTIALS** - Not exercised (needs the key to stay unusable for >7 days). - see:
  [details/DDB-TABLE-336.md](details/DDB-TABLE-336.md)

- <a id="ddb-table-369"></a>**DDB-TABLE-369** `async-state-machine` · impact high · handled · verified 2026-10-09
  **Billing-mode switch UPDATING: PT/OnDemand/stream/Delete -> ResourceInUse, DP and Warm admitted; reverse switch 200 but lost (71.9 s / 2.0 s)**
  FRESH PAY_PER_REQUEST table, first switch to PROVISIONED 1/1: trigger 200 OK (TableStatus=UPDATING);
  UPDATING 71.9 s (timeline (TableStatus, BillingModeSummary, RCU): [(('UPDATING', 'PROVISIONED', 1), 71.87),
  (('ACTIVE', 'PROVISIONED', 1), None)]). Fired right after: billing_switch_back_ppr -> 200 OK
  (TableStatus=UPDATING) @+0.06s; pt_change_2 -> ResourceInUseException (HTTP 400) 'Attempt to change a
  resource which is still in use: Table IOPS are currently being updated. Table: ackq-90be14-upd2f' @+0.4s;
  warm_increase -> 200 OK (TableStatus=UPDATING) @+0.77s; odt_change -> ResourceInUseException (HTTP 400)
  'Attempt to change a resource which is still in use: OnDemandThroughput cannot be updated while BillingMode
  update is in progress' @+1.1s; stream_enable -> ResourceInUseException (HTTP 400) 'Attempt to change a
  resource which is still in use: Can't enable or disable stream while table IOPS are being updated. Table:
  ackq-90be14-upd2f' @+1.43s; delete_table -> ResourceInUseException (HTTP 400) 'Attempt to change a resource
  which is still in use: Table: ackq-90be14-upd2f is in the process of being updated.' @+1.75s; dp_toggle ->
  200 OK (TableStatus=UPDATING) @+2.09s. Mid-window: billing_switch_back_ppr -> 200 OK (TableStatus=UPDATING)
  @+8.5s; pt_change_2 -> ResourceInUseException (HTTP 400) 'Attempt to change a resource which is still in
  use: Table IOPS are currently being updated. Table: ackq-90be14-upd2f' @+8.83s; warm_increase -> 200 OK
  (TableStatus=UPDATING) @+9.19s; odt_change -> ResourceInUseException (HTTP 400) 'Attempt to change a
  resource which is still in use: OnDemandThroughput cannot be updated while BillingMode update is in
  progress' @+9.52s; stream_enable -> ResourceInUseException (HTTP 400) 'Attempt to change a resource which is
  still in use: Can't enable or disable stream while table IOPS are being updated. Table: ackq-90be14-upd2f'
  @+9.85s; delete_table -> ResourceInUseException (HTTP 400) 'Attempt to change a resource which is still in
  use: Table: ackq-90be14-upd2f is in the process of being updated.' @+10.19s; dp_toggle -> skipped (dp
  cooldown). State after: {'status': 'ACTIVE', 'billing': 'PROVISIONED', 'rcu': 1, 'wcu': 1, 'warm': (12000,
  4000, 'UPDATING'), 'odt': None, 'dp': True}. FRESH table back to PAY_PER_REQUEST: trigger 200 OK
  (TableStatus=UPDATING); UPDATING 2.0 s ([(('UPDATING', 'PAY_PER_REQUEST', 0), 2.02), (('ACTIVE',
  'PAY_PER_REQUEST', 0), None)]). Right after: billing_switch_back_provisioned -> ResourceInUseException (HTTP 400)
  'Attempt to change a resource which is still in use: Table IOPS are currently being updated. Table:
  ackq-90be14-upd2f' @+0.06s; billing_resend_ppr -> ResourceInUseException (HTTP 400) 'Attempt to change a
  resource which is still in use: Table IOPS are currently being updated. Table: ackq-90be14-upd2f' @+0.39s;
  pt_change_7 -> ResourceInUseException (HTTP 400) 'Attempt to change a resource which is still in use: Table
  IOPS are currently being updated. Table: ackq-90be14-upd2f' @+0.72s; odt_change -> ValidationException (HTTP 400)
  'One or more parameter values were invalid: MaxReadRequestUnits for OnDemandThroughput cannot be specified
  when the table BillingMode is PROVISIONED' @+1.05s; warm_increase -> 200 OK (TableStatus=UPDATING) @+1.41s;
  delete_table -> ResourceInUseException (HTTP 400) 'Attempt to change a resource which is still in use:
  Table: ackq-90be14-upd2f is in the process of being updated.' @+1.73s; dp_toggle -> 200 OK
  (TableStatus=UPDATING) @+2.07s. Mid-window: . State after: {'status': 'ACTIVE', 'billing':
  'PAY_PER_REQUEST', 'rcu': 0, 'wcu': 0, 'warm': (12000, 4000, 'UPDATING'), 'odt': None, 'dp': False}. Same
  table re-flipped later (PPR->PROVISIONED again): UPDATING only 2.0 s; t0 round: billing_switch_back_ppr ->
  200 OK (TableStatus=UPDATING) @+0.07s; pt_change_2 -> ResourceInUseException (HTTP 400) 'Attempt to change a
  resource which is still in use: Table IOPS are currently being updated. Table: ackq-90be14-upd2' @+0.4s;
  warm_increase -> 200 OK (TableStatu [truncated in evidence]
  - ACK: updateable.when, deletable.when, requeue, synced.when · ops: UpdateTable, DeleteTable, DescribeTable
    · fields: BillingMode, ProvisionedThroughput, WarmThroughput, OnDemandThroughput,
    DeletionProtectionEnabled, StreamSpecification
  - repro: PPR table; UpdateTable(BillingMode=PROVISIONED, PT 1/1); immediately
    UpdateTable(BillingMode=PAY_PER_REQUEST) / PT 2/2 / WarmThroughput+1000 / OnDemandThroughput / stream
    enable / DeleteTable / DP toggle; repeat at +8 s; then the reverse switch
  - measurements: fresh_ppr_to_provisioned_updating_s=71.9, fresh_provisioned_to_ppr_updating_s=2.0,
    reflip_ppr_to_provisioned_updating_s=2.0, reflip_provisioned_to_ppr_updating_s=0
  - handling: handled via `pkg/resource/table/hooks.go:81-84; pkg/resource/table/hooks.go:198-202`
  - related: [DDB-TABLE-063](table-throughput-billing.md#ddb-table-063), [DDB-TABLE-064](table-throughput-billing.md#ddb-table-064), [DDB-TABLE-119](#ddb-table-119), [DDB-TABLE-002](#ddb-table-002), [DDB-TABLE-156](table-throughput-billing.md#ddb-table-156), [DDB-TABLE-056](table-throughput-billing.md#ddb-table-056),
    [DDB-TABLE-062](table-throughput-billing.md#ddb-table-062), [DDB-TABLE-067](table-throughput-billing.md#ddb-table-067), [DDB-TABLE-183](table-throughput-billing.md#ddb-table-183), [DDB-TABLE-452](table-throughput-billing.md#ddb-table-452), [DDB-TABLE-055](table-throughput-billing.md#ddb-table-055), [DDB-TABLE-433](#ddb-table-433), [DDB-TABLE-057](table-throughput-billing.md#ddb-table-057),
    [DDB-TABLE-001](table.md#ddb-table-001), [DDB-TABLE-370](table-throughput-billing.md#ddb-table-370), [DDB-TABLE-052](#ddb-table-052), [DDB-TABLE-285](#ddb-table-285), [DDB-TABLE-120](#ddb-table-120), [DDB-TABLE-065](#ddb-table-065), [DDB-TABLE-459](table-throughput-billing.md#ddb-table-459),
    [DDB-TABLE-054](#ddb-table-054), [DDB-TABLE-010](table.md#ddb-table-010), [DDB-TABLE-069](#ddb-table-069), [DDB-TABLE-066](table-throughput-billing.md#ddb-table-066), [DDB-TABLE-284](#ddb-table-284), [DDB-TABLE-334](#ddb-table-334) · hypotheses:
    H-T-001, H-T-002 · evidence: table/state-machine/updating-second-mutation
  - notes: Confirms H-T-002 (DeleteTable during UPDATING -> ResourceInUseException 'is in the process of being
    updated') and qualifies H-T-001: during a billing-mode UPDATING window the rejection is per-field, not
    blanket - ProvisionedThroughput/OnDemandThroughput/stream are ResourceInUse ('Table IOPS are...
  - full notes: [details/DDB-TABLE-369.md](details/DDB-TABLE-369.md)

- <a id="ddb-table-450"></a>**DDB-TABLE-450** `async-state-machine` · impact high · unhandled (not handled in controller) · verified 2026-10-09
  **Clobber matrix (3 jobs x 8 writes): only the SSE write resets TableStatus, only in a TableClass switch; Warm/DP/tag/TTL/PITR/policy never do**
  One fresh PAY_PER_REQUEST table per cell; job J at t0, write W at +0.1 s, reverse job J' at +1.0 s,
  DescribeTable every 0.25 s. TableClass switch (-> IA; indicator TableClassSummary): sse: W=OK (resp
  TableStatus ACTIVE), first ACTIVE 0.26s, job done 3.1s, J' LOST; dp: W=OK (resp TableStatus UPDATING), first
  ACTIVE 4.39s, job done 4.39s, J' rejected:ResourceInUseException; warm: W=OK (resp TableStatus UPDATING),
  first ACTIVE 3.62s, job done 3.36s, J' rejected:ResourceInUseException; odt: W=ResourceInUseException, first
  ACTIVE 3.9s, job done 3.9s, J' rejected:ResourceInUseException; tag: W=OK, first ACTIVE 4.4s, job done 4.4s,
  J' rejected:ResourceInUseException; ttl: W=OK, first ACTIVE 4.39s, job done 4.39s, J'
  rejected:ResourceInUseException; pitr: W=OK, first ACTIVE 3.09s, job done 3.09s, J'
  rejected:ResourceInUseException; policy: W=OK, first ACTIVE 3.35s, job done 3.35s, J'
  rejected:ResourceInUseException; none (control): UPDATING for the whole 45 s watch, TableClassSummary=IA
  appeared 46 s after the request (switch durations vary 3-46 s), J' rejected:ResourceInUseException. Stream
  enable (indicator DescribeStream.StreamStatus): sse: W=OK (resp TableStatus ACTIVE), first ACTIVE 4.23s, job
  done 4.23s, J' rejected:ResourceInUseException; dp: W=OK (resp TableStatus UPDATING), first ACTIVE 3.97s,
  job done 3.97s, J' rejected:ResourceInUseException; warm: W=OK (resp TableStatus UPDATING), first ACTIVE
  4.52s, job done 4.52s, J' rejected:ResourceInUseException; odt: W=ResourceInUseException, first ACTIVE 4.5s,
  job done 4.5s, J' rejected:ResourceInUseException; tag: W=OK, first ACTIVE 5.31s, job done 5.31s, J'
  rejected:ResourceInUseException; ttl: W=OK, first ACTIVE 3.69s, job done 3.69s, J'
  rejected:ResourceInUseException; pitr: W=OK, first ACTIVE 3.73s, job done 3.73s, J'
  rejected:ResourceInUseException; policy: W=OK, first ACTIVE 4.51s, job done 4.51s, J'
  rejected:ResourceInUseException; none: W=-, first ACTIVE 3.16s, job done 3.16s, J'
  rejected:ResourceInUseException. Billing switch PPR->PROVISIONED 1/1 (indicator BillingModeSummary, which
  flips to PROVISIONED at +0.1 s already): sse: W=OK at +1.28 s after one ThrottlingException retry (resp
  TableStatus ACTIVE), first ACTIVE 61.43s, J' LOST; dp: W=OK (resp TableStatus UPDATING), first ACTIVE
  86.73s, job done 0.13s, J' LOST; warm: W=OK (resp TableStatus UPDATING), first ACTIVE 107.98s, job done
  0.17s, J' LOST; odt: W=ResourceInUseException, first ACTIVE 64.47s, job done 0.13s, J' LOST; tag: W=OK,
  first ACTIVE 90.76s, job done 0.24s, J' LOST; ttl: W=OK, first ACTIVE 108.05s, job done 0.17s, J' LOST;
  pitr: W=OK, first ACTIVE 115.09s, job done 0.17s, J' LOST; policy: W=OK, first ACTIVE 160.63s, job done
  0.2s, J' LOST; none: W=-, first ACTIVE 118.1s, job done 0.11s, J' LOST. Write admissibility:
  OnDemandThroughput is ResourceInUseException during all three jobs ('OnDemandThroughput cannot be updated
  while BillingMode update is in progress' / TableClass / stream variants); SSE, DP, WarmThroughput,
  TagResource, TTL, PITR and PutResourcePolicy are accepted during all three. Only the SSE write's own
  UpdateTable response says TableStatus=ACTIVE; DP and Warm responses echo UPDATING. Only in the TableClass
  cell did DescribeTable follow the SSE response (ACTIVE at +0.26 s with the class still pending,
  [DDB-TABLE-287](#ddb-table-287)); during the stream enable the SSE response said ACTIVE but DescribeTable kept UPDATING and
  the stream-disable J' was ResourceInUseException. Every bl cell including the no-write control shows J'
  (BillingMode=PAY_PER_REQUEST) accepted with 200 and lost - that is a property of the billing switch itself,
  see table/creative/billing-reversal-noop, not a clobber.
  - ACK: synced.when, one-per-reconcile, requeue, custom_update · ops: UpdateTable, TagResource,
    UpdateTimeToLive, UpdateContinuousBackups, PutResourcePolicy, DescribeTable · fields: TableStatus,
    TableClass, StreamSpecification, BillingMode, SSESpecification, WarmThroughput, DeletionProtectionEnabled
  - repro: UpdateTable(J); W at +0.1 s; UpdateTable(reverse of J) at +1.0 s; DescribeTable every 0.25 s until
    quiescent
  - measurements: cells=27, premature_active_cells=1, tc_updating_s_control=46, st_updating_s=[4.23, 3.97,
    4.52, 4.5, 5.31, 3.69, 3.73, 4.51, 3.16], bl_first_active_s=[61.43, 86.73, 107.98, 64.47, 90.76, 108.05,
    115.09, 160.63, 118.1]
  - handling: not handled in the controller (as of commit 34b85e6)
  - related: [DDB-TABLE-287](#ddb-table-287), [DDB-TABLE-285](#ddb-table-285), [DDB-TABLE-286](#ddb-table-286), [DDB-TABLE-119](#ddb-table-119), [DDB-TABLE-458](table-indexes.md#ddb-table-458), [DDB-TABLE-462](table-indexes.md#ddb-table-462),
    [DDB-TABLE-175](table-indexes.md#ddb-table-175), [DDB-TABLE-117](#ddb-table-117), [DDB-TABLE-234](table-policy-kinesis-autoscaling.md#ddb-table-234), [DDB-TABLE-460](table-policy-kinesis-autoscaling.md#ddb-table-460), [DDB-TABLE-435](#ddb-table-435), [DDB-TABLE-121](table-throughput-billing.md#ddb-table-121) · evidence:
    table/creative/clobber-matrix
  - notes: Generalizes [DDB-TABLE-287](#ddb-table-287) across jobs and writes: the TableStatus reset is specific to the SSE
    write path x TableClass job. The WarmThroughput increase is admitted during TableClass and stream jobs
    (not listed in [DDB-TABLE-286](#ddb-table-286)) and does not reset the status. The 9 'LOST' bl cells are explained by...
  - full notes: [details/DDB-TABLE-450.md](details/DDB-TABLE-450.md)

## Identity and lookup

- <a id="ddb-table-362"></a>**DDB-TABLE-362** `identity` · impact medium · unhandled (not handled in controller) · verified 2026-10-09
  **Re-enabling a stream mints a NEW LatestStreamArn/Label; the response already carries it and the old ARN is
  gone from DescribeTable** - Enable {true, NEW_IMAGE} -> LatestStreamArn .../stream/2026-10-09T04:48:48.824. -
  see: [details/DDB-TABLE-362.md](details/DDB-TABLE-362.md)

- <a id="ddb-table-378"></a>**DDB-TABLE-378** `identity` · impact medium · unhandled (not handled in controller) · verified 2026-10-09
  **Each stream enable mints a new LatestStreamArn; ListStreams(TableName) still lists DISABLED streams of
  flaps and deleted incarnations** - Table re-created under the same name with StreamSpecification enabled:
  LatestStreamArn differs from the old incarnation's (label = enable timestamp),
  dynamodbstreams:DescribeStream on the old ARN still returns 200 wit... - see:
  [details/DDB-TABLE-378.md](details/DDB-TABLE-378.md)

## Idempotency

- <a id="ddb-table-018"></a>**DDB-TABLE-018** `idempotency` · impact high · unhandled (not handled in controller) · verified 2026-10-08
  **No-op UpdateTable re-sends are per-field: DP/BillingMode/TableClass 200; Stream and SSE Enabled:false re-sends ValidationException**
  Outcome of UpdateTable re-sending the current value, field by field (OK(status) or error code):
  {'dp_true_resend_immediate': 'OK(ACTIVE)', 'dp_false_resend_immediate': 'ThrottlingException',
  'dp_false_resend_after_cooldown': 'OK(ACTIVE)', 'billing_ppr_resend': 'OK(UPDATING)',
  'tableclass_standard_resend': 'OK(UPDATING)', 'tableclass_ia_resend': 'OK(UPDATING)',
  'sse_enabled_false_resend': 'ValidationException', 'stream_disabled_resend_when_no_stream':
  'ValidationException', 'stream_resend_identical': 'ValidationException',
  'stream_disable_resend_after_disabled': 'ValidationException', 'ondemand_throughput_resend_same':
  'ThrottlingException', 'provisioned_throughput_on_ppr_table': 'ValidationException',
  'stream_change_viewtype_while_enabled': 'ValidationException', 'stream_enabled_true_without_viewtype':
  'ValidationException'}. Error messages: {'dp_false_resend_immediate': 'Deletion protection setting for table
  ackq-ba1bb7-err modified within the previous 15000 milliseconds. Please ', 'sse_enabled_false_resend': 'One
  or more parameter values were invalid: Table is already encrypted by default',
  'stream_disabled_resend_when_no_stream': 'Table has no stream to disable: TableName: ackq-ba1bb7-err',
  'stream_resend_identical': 'Table already has an enabled stream: TableName: ackq-ba1bb7-err',
  'stream_disable_resend_after_disabled': 'Table has no stream to disable: TableName: ackq-ba1bb7-err',
  'ondemand_throughput_resend_same': 'The rate of control plane requests made by this account is too high',
  'provisioned_throughput_on_ppr_table': 'One or more parameter values were invalid: Neither ReadCapacityUnits
  nor WriteCapacityUnits can be specified w', 'stream_change_viewtype_while_enabled': 'Table already has an
  enabled stream: TableName: ackq-ba1bb7-err', 'stream_enabled_true_without_viewtype': 'One or more parameter
  values were invalid: If stream is being enabled then UpdateViewType is required'}.
  - ACK: custom_update, compare.is_ignored+delta_pre_compare, one-per-reconcile · ops: UpdateTable · fields:
    DeletionProtectionEnabled, BillingMode, TableClass, SSESpecification, StreamSpecification,
    OnDemandThroughput
  - repro: On an ACTIVE table call UpdateTable with each field set to its current value
  - measurements: stream_enable_updating_s=4.05, stream_disable_updating_s=5.07, tableclass_updating_s=6.08
  - handling: not handled in the controller (as of commit 34b85e6)
  - related: [DDB-TABLE-177](#ddb-table-177), [DDB-TABLE-019](#ddb-table-019), [DDB-TABLE-156](table-throughput-billing.md#ddb-table-156), [DDB-TABLE-057](table-throughput-billing.md#ddb-table-057), [DDB-TABLE-365](#ddb-table-365), [DDB-TABLE-183](table-throughput-billing.md#ddb-table-183),
    [DDB-TABLE-358](table-throughput-billing.md#ddb-table-358), [DDB-TABLE-283](#ddb-table-283), [DDB-TABLE-024](table-throughput-billing.md#ddb-table-024), [DDB-TABLE-038](table-throughput-billing.md#ddb-table-038), [DDB-TABLE-039](table-throughput-billing.md#ddb-table-039), [DDB-TABLE-056](table-throughput-billing.md#ddb-table-056), [DDB-TABLE-059](table-throughput-billing.md#ddb-table-059),
    [DDB-TABLE-433](#ddb-table-433), [DDB-TABLE-050](#ddb-table-050), [DDB-TABLE-035](#ddb-table-035), [DDB-TABLE-002](#ddb-table-002), [DDB-TABLE-029](#ddb-table-029), [DDB-TABLE-040](table-throughput-billing.md#ddb-table-040), [DDB-TABLE-036](#ddb-table-036),
    [DDB-TABLE-052](#ddb-table-052), [DDB-TABLE-284](#ddb-table-284), [DDB-TABLE-180](#ddb-table-180), [DDB-TABLE-141](#ddb-table-141), [DDB-TABLE-082](#ddb-table-082), [DDB-TABLE-078](#ddb-table-078), [DDB-TABLE-065](#ddb-table-065) ·
    evidence: table/error-taxonomy/missing-table-noop-update-dp
  - notes: H-T-069: compare DP (accepted?) vs TableClass/BillingMode/Stream (rejected?). The controller cannot
    rely on a uniform no-op rule. OnDemandThroughput re-send result inconclusive: hit account-level
    ThrottlingException 'The rate of control plane requests made by this account is too high' (shared...
  - full notes: [details/DDB-TABLE-018.md](details/DDB-TABLE-018.md)

- <a id="ddb-table-082"></a>**DDB-TABLE-082** `idempotency` · impact high · SUSPECTED CONTROLLER BUG · verified 2026-10-08
  **Re-sending the current KMS key by ARN is a ValidationException no-op; by alias or key id it triggers a ~22s re-encryption**
  Table encrypted with CMK K1 (created via alias). UpdateTable
  SSESpecification{Enabled:true,SSEType:KMS,KMSMasterKeyId:<K1 ARN>} -> ValidationException 'One or more
  parameter values were invalid: Table is already encrypted with given KMSMasterKeyId. Use KMSMasterKeyId
  parameter if you want to change Master Key'. The same with KMSMasterKeyId=<alias> or <key id> -> 200,
  response SSEDescription.Status=UPDATING (TableStatus ACTIVE), DescribeTable SSE Status UPDATING for 21-22s,
  final KMSMasterKeyArn unchanged. Each such re-send counts against the 4-per-24h encryption-change quota.
  - ACK: custom_update, compare.is_ignored+delta_pre_compare, references · ops: UpdateTable, DescribeTable ·
    fields: SSESpecification.KMSMasterKeyId, SSEDescription.KMSMasterKeyArn, SSEDescription.Status
  - repro: CMK table; UpdateTable SSESpecification with the same key as ARN / alias / key id; DescribeTable at
    1s
  - measurements: resend_alias_sse_updating_s=22.27, resend_keyid_sse_updating_s=21.26
  - handling: suspected controller bug - see Handling gaps
  - related: [DDB-TABLE-078](#ddb-table-078), [DDB-TABLE-141](#ddb-table-141), [DDB-TABLE-140](#ddb-table-140), [DDB-TABLE-371](#ddb-table-371), [DDB-TABLE-065](#ddb-table-065), [DDB-TABLE-079](#ddb-table-079),
    [DDB-TABLE-081](#ddb-table-081), [DDB-TABLE-142](#ddb-table-142), [DDB-TABLE-120](#ddb-table-120), [DDB-TABLE-018](#ddb-table-018) · evidence: table/mutation-matrix/sse-kms
  - notes: Only the ARN form is compared against SSEDescription.KMSMasterKeyArn server-side; a controller
    should resolve alias/id to the ARN (kms:DescribeKey) before deciding whether to send SSESpecification.
  - full notes: [details/DDB-TABLE-082.md](details/DDB-TABLE-082.md)

- <a id="ddb-table-141"></a>**DDB-TABLE-141** `idempotency` · impact high · tracked in GitHub issue (not handled) · verified 2026-10-08
  **AWS-managed-key table: re-sending Enabled:true/SSEType:KMS/alias/aws/dynamodb re-encrypts (~21s) and burns quota; only ARN is a no-op** (hypothesis refuted; behavior confirmed)
  T2 created with SSESpecification{Enabled:true} (KMSMasterKeyArn = alias/aws/dynamodb key: True). Re-send
  {Enabled:true} -> OK (resp TableStatus=ACTIVE, resp SSE={"Status": "UPDATING", "SSEType": "KMS",
  "KMSMasterKeyArn": "arn:aws:kms:us-west-2:<ACCOUNT>:key/301f0bb6-edf0-476a-9525-14f2ee58148a"}; 21.31s via
  ACTIVE/UPDATING/14f2ee58148a->ACTIVE/ENABLED/14f2ee58148a; after={"Status": "ENABLED", "SSEType": "KMS",
  "KMSMasterKeyArn": "arn:aws:kms:us-west-2:<ACCOUNT>:key/301f0bb6-edf0-476a-9525-14f2ee58148a"}).
  {Enabled:true,SSEType:KMS} -> OK (resp TableStatus=ACTIVE, resp SSE={"Status": "UPDATING", "SSEType": "KMS",
  "KMSMasterKeyArn": "arn:aws:kms:us-west-2:<ACCOUNT>:key/301f0bb6-edf0-476a-9525-14f2ee58148a"}; 21.31s via
  ACTIVE/UPDATING/14f2ee58148a->ACTIVE/ENABLED/14f2ee58148a; after={"Status": "ENABLED", "SSEType": "KMS",
  "KMSMasterKeyArn": "arn:aws:kms:us-west-2:<ACCOUNT>:key/301f0bb6-edf0-476a-9525-14f2ee58148a"}).
  {..,KMSMasterKeyId:alias/aws/dynamodb} -> OK (resp TableStatus=ACTIVE, resp SSE={"Status": "UPDATING",
  "SSEType": "KMS", "KMSMasterKeyArn":
  "arn:aws:kms:us-west-2:<ACCOUNT>:key/301f0bb6-edf0-476a-9525-14f2ee58148a"}; 21.32s via
  ACTIVE/UPDATING/14f2ee58148a->ACTIVE/ENABLED/14f2ee58148a; after={"Status": "ENABLED", "SSEType": "KMS",
  "KMSMasterKeyArn": "arn:aws:kms:us-west-2:<ACCOUNT>:key/301f0bb6-edf0-476a-9525-14f2ee58148a"}).
  {..,KMSMasterKeyId:<aws key ARN>} -> ValidationException: 'One or more parameter values were invalid: Table
  is already encrypted with given KMSMasterKeyId. Use KMSMasterKeyId parameter if you want to change Master
  Key'. Enabled:false -> OK (resp TableStatus=ACTIVE, resp SSE={"Status": "UPDATING", "SSEType": "KMS",
  "KMSMasterKeyArn": "arn:aws:kms:us-west-2:<ACCOUNT>:key/301f0bb6-edf0-476a-9525-14f2ee58148a"}; 21.3s via
  ACTIVE/UPDATING/14f2ee58148a->ACTIVE/UPDATING/->ACTIVE/<absent>/None; after="<absent>"). Enabled:true ->
  LimitExceededException: 'Subscriber limit exceeded: Encryption mode changes are limited in the 24h window
  ending at 2026-10-09T23:25:27.800Z. After the first 4 change, each subsequent change in the same window can
  be performed at most once every 21600 seconds. Number of updates today: 4. Last change at
  2026-10-08T23:26:32.1'. -> CMK K1 -> LimitExceededException: 'Subscriber limit exceeded: Encryption mode
  changes are limited in the 24h window ending at 2026-10-09T23:25:27.800Z. After the first 4 change, each
  subsequent change in the same window can be performed at most once every 21600 seconds. Number of updates
  today: 4. Last change at 2026-10-08T23:26:32.1'.
  - ACK: compare.is_ignored+delta_pre_compare, custom_update · ops: UpdateTable, DescribeTable · fields:
    SSESpecification.Enabled, SSESpecification.SSEType, SSESpecification.KMSMasterKeyId
  - repro: CreateTable SSESpecification{Enabled:true}; UpdateTable with each equivalent AWS-managed spelling
  - handling: tracked in https://github.com/aws-controllers-k8s/community/issues/2136 (not handled) · code refs: `generator.yaml:73-77; pkg/resource/table/hooks.go:603-611; pkg/resource/table/hooks.go:583-619; pkg/resource/table/hooks.go:446-465; test/e2e/tests/test_table.py:641-673`
  - related: [DDB-TABLE-082](#ddb-table-082), [DDB-TABLE-078](#ddb-table-078), [DDB-TABLE-140](#ddb-table-140), [DDB-TABLE-371](#ddb-table-371), [DDB-TABLE-065](#ddb-table-065), [DDB-TABLE-079](#ddb-table-079),
    [DDB-TABLE-081](#ddb-table-081), [DDB-TABLE-142](#ddb-table-142), [DDB-TABLE-120](#ddb-table-120), [DDB-TABLE-018](#ddb-table-018) · evidence: table/mutation-matrix/sse-kms-quota
  - notes: Hypotheses: H-T-112. REFUTED for the re-send claim: UpdateTable SSESpecification{Enabled:true} on a
    table already using the AWS managed key returns 200 but starts a ~21s SSEDescription.Status=UPDATING cycle
    and counts as one of the 4 encryption changes allowed per 24h; the same for...
  - full notes: [details/DDB-TABLE-141.md](details/DDB-TABLE-141.md)

- <a id="ddb-table-383"></a>**DDB-TABLE-383** `idempotency` · impact medium · unhandled (not handled in controller) · verified 2026-10-09
  **No-op UpdateTable re-sends are free: 20 same-value BillingMode/DP/TableClass calls in 0.4 s -> all 200, no
  throttle/UPDATING/quota/cooldown** - PAY_PER_REQUEST table. - see:
  [details/DDB-TABLE-383.md](details/DDB-TABLE-383.md)

## Errors

- <a id="ddb-table-020"></a>**DDB-TABLE-020** `error-code` · impact high · unhandled (not handled in controller) · verified 2026-10-08
  **CreateTable with unusable KMS key is rejected synchronously (ValidationException, embedded KMS text), no table left behind**
  CreateTable(SSESpecification{Enabled,KMS,KMSMasterKeyId=<key>}) outcomes by key state: {'disabled':
  'ValidationException', 'pending_deletion': 'ValidationException', 'asymmetric_rsa2048':
  'InternalServerError', 'deny_creategrant': 'AccessDeniedException', 'other_region_arn':
  'ValidationException', 'nonexistent_keyid': 'ValidationException', 'nonexistent_alias':
  'ValidationException', 'disabled_by_alias': 'ValidationException'}. Messages: {'disabled': 'KMS key disabled
  error: com.amazonaws.services.kms.model.DisabledException:
  arn:aws:kms:us-west-2:<ACCOUNT>:key/1cb8c9d5-9941-4339-a86b-e80c2159783', 'pending_deletion': 'KMS
  validation error: com.amazonaws.services.kms.model.KMSInvalidStateException:
  arn:aws:kms:us-west-2:<ACCOUNT>:key/1ed86bdb-3db8-481c-922b-402e98', 'asymmetric_rsa2048': 'KMS internal
  error: com.amazonaws.services.kms.model.AWSKMSException: EncryptionContext is supported only when creating a
  grant for a symmetric encryp', 'deny_creategrant': 'KMS key access denied error:
  com.amazonaws.services.kms.model.AWSKMSException: User: arn:aws:sts::<ACCOUNT>:assumed-role/<PRINCIPAL> is',
  'other_region_arn': 'KMS validation error: com.amazonaws.services.kms.model.NotFoundException: Invalid arn
  us-east-1 (Service: AWSKMS; Status Code: 400; Error Code: NotFou', 'nonexistent_keyid': "KMS validation
  error: com.amazonaws.services.kms.model.NotFoundException: Key
  'arn:aws:kms:us-west-2:<ACCOUNT>:key/c9bfce6a-71e3-40d0-bfc5-f594a60e", 'nonexistent_alias': 'KMS validation
  error: com.amazonaws.services.kms.model.NotFoundException: Alias
  arn:aws:kms:us-west-2:<ACCOUNT>:alias/ackq-f1d9f4-nope is not found', 'disabled_by_alias': 'KMS key disabled
  error: com.amazonaws.services.kms.model.DisabledException:
  arn:aws:kms:us-west-2:<ACCOUNT>:key/1cb8c9d5-9941-4339-a86b-e80c2159783'}. DescribeTable right after each
  rejection: {'disabled': 'ERR:ResourceNotFoundException', 'pending_deletion':
  'ERR:ResourceNotFoundException', 'asymmetric_rsa2048': 'ERR:ResourceNotFoundException', 'deny_creategrant':
  'ERR:ResourceNotFoundException', 'other_region_arn': 'ERR:ResourceNotFoundException', 'nonexistent_keyid':
  'ERR:ResourceNotFoundException', 'nonexistent_alias': 'ERR:ResourceNotFoundException', 'disabled_by_alias':
  'ERR:ResourceNotFoundException'}.
  - ACK: terminal_codes, custom_create · ops: CreateTable · fields: SSESpecification.KMSMasterKeyId
  - repro: CreateTable with KMSMasterKeyId of a disabled / pending-deletion / RSA / CreateGrant-denied /
    nonexistent key
  - handling: not handled in the controller (as of commit 34b85e6)
  - related: [DDB-TABLE-080](#ddb-table-080), [DDB-TABLE-021](#ddb-table-021), [DDB-TABLE-139](#ddb-table-139), [DDB-TABLE-022](#ddb-table-022), [DDB-TABLE-023](#ddb-table-023), [DDB-TABLE-037](#ddb-table-037) ·
    evidence: table/error-taxonomy/kms-key-states
  - notes: H-T-111 CreateTable half. Causes are distinguishable only by message substring if the code is the
    same. H-T-111 PARTIALLY REFUTED: disabled/pending-deletion/nonexistent/other-region keys ->
    ValidationException as predicted, but an asymmetric key -> InternalServerError (HTTP 500) and a...
  - full notes: [details/DDB-TABLE-020.md](details/DDB-TABLE-020.md)

- <a id="ddb-table-021"></a>**DDB-TABLE-021** `error-code` · impact high · unhandled (not handled in controller) · verified 2026-10-08
  **UpdateTable with unusable KMS key is rejected synchronously; table stays ACTIVE with SSEDescription unchanged**
  UpdateTable(SSESpecification{Enabled,KMS,KMSMasterKeyId=<key>}) on an ACTIVE default-encrypted table:
  {'disabled': 'ValidationException', 'pending_deletion': 'ValidationException', 'asymmetric_rsa2048':
  'InternalServerError', 'deny_creategrant': 'AccessDeniedException', 'other_region_arn':
  'ValidationException', 'nonexistent_keyid': 'ValidationException', 'nonexistent_alias':
  'ValidationException', 'disabled_by_alias': 'ValidationException'}. Messages: {'disabled': 'KMS key disabled
  error: com.amazonaws.services.kms.model.DisabledException:
  arn:aws:kms:us-west-2:<ACCOUNT>:key/1cb8c9d5-9941-4339-a86b-e80c2159783', 'pending_deletion': 'KMS
  validation error: com.amazonaws.services.kms.model.KMSInvalidStateException:
  arn:aws:kms:us-west-2:<ACCOUNT>:key/1ed86bdb-3db8-481c-922b-402e98', 'asymmetric_rsa2048': 'KMS internal
  error: com.amazonaws.services.kms.model.AWSKMSException: EncryptionContext is supported only when creating a
  grant for a symmetric encryp', 'deny_creategrant': 'KMS key access denied error:
  com.amazonaws.services.kms.model.AWSKMSException: User: arn:aws:sts::<ACCOUNT>:assumed-role/<PRINCIPAL> is',
  'other_region_arn': 'KMS validation error: com.amazonaws.services.kms.model.NotFoundException: Invalid arn
  us-east-1 (Service: AWSKMS; Status Code: 400; Error Code: NotFou', 'nonexistent_keyid': "KMS validation
  error: com.amazonaws.services.kms.model.NotFoundException: Key
  'arn:aws:kms:us-west-2:<ACCOUNT>:key/c9bfce6a-71e3-40d0-bfc5-f594a60e", 'nonexistent_alias': 'KMS validation
  error: com.amazonaws.services.kms.model.NotFoundException: Alias
  arn:aws:kms:us-west-2:<ACCOUNT>:alias/ackq-f1d9f4-nope is not found', 'disabled_by_alias': 'KMS key disabled
  error: com.amazonaws.services.kms.model.DisabledException:
  arn:aws:kms:us-west-2:<ACCOUNT>:key/1cb8c9d5-9941-4339-a86b-e80c2159783'}. TableStatus after each:
  {'disabled': 'ACTIVE', 'pending_deletion': 'ACTIVE', 'asymmetric_rsa2048': 'ACTIVE', 'deny_creategrant':
  'ACTIVE', 'other_region_arn': 'ACTIVE', 'nonexistent_keyid': 'ACTIVE', 'nonexistent_alias': 'ACTIVE',
  'disabled_by_alias': 'ACTIVE'}. SSEDescription before/after: {'before': 'ABSENT', 'after': 'ABSENT'}.
  - ACK: terminal_codes, custom_update · ops: UpdateTable · fields: SSESpecification.KMSMasterKeyId
  - repro: UpdateTable(SSESpecification{Enabled:true,SSEType:KMS,KMSMasterKeyId=<bad key>}) on ACTIVE table
  - handling: not handled in the controller (as of commit 34b85e6)
  - related: [DDB-TABLE-020](#ddb-table-020), [DDB-TABLE-080](#ddb-table-080), [DDB-TABLE-139](#ddb-table-139), [DDB-TABLE-022](#ddb-table-022), [DDB-TABLE-023](#ddb-table-023), [DDB-TABLE-037](#ddb-table-037) ·
    evidence: table/error-taxonomy/kms-key-states
  - notes: H-T-111 UpdateTable half. H-T-111 PARTIALLY REFUTED:
    disabled/pending-deletion/nonexistent/other-region keys -> ValidationException as predicted, but an
    asymmetric key -> InternalServerError (HTTP 500) and a CreateGrant-denied key -> AccessDeniedException
    (HTTP 400).

- <a id="ddb-table-022"></a>**DDB-TABLE-022** `error-code` · impact high · unhandled (not handled in controller) · verified 2026-10-08
  **Asymmetric (RSA_2048) KMSMasterKeyId makes CreateTable/UpdateTable fail with InternalServerError HTTP 500 (looks transient, is permanent)**
  CreateTable and UpdateTable with SSESpecification.KMSMasterKeyId pointing at an RSA_2048 key return
  InternalServerError (HTTP 500) after ~1.8s: 'KMS internal error:
  com.amazonaws.services.kms.model.AWSKMSException: EncryptionContext is supported only when creating a grant
  for a symmetric encryption KMS key. (Service: AWSKMS; Status Code: 400; Error Code: ValidationException;
  Request ID: 0a42f385-87e3-4'. No table is created (DescribeTable -> ResourceNotFoundException) and the
  existing table stays ACTIVE with default encryption. The same wrong-key class with a
  disabled/pending-deletion/nonexistent key returns ValidationException (400) instead.
  - ACK: terminal_codes, requeue · ops: CreateTable, UpdateTable · fields: SSESpecification.KMSMasterKeyId
  - repro: aws kms create-key --key-spec RSA_2048 --key-usage ENCRYPT_DECRYPT; CreateTable ...
    SSESpecification={Enabled:true,SSEType:KMS,KMSMasterKeyId:<key>}
  - measurements: create_latency_ms=1849, update_latency_ms=1731
  - handling: not handled in the controller (as of commit 34b85e6)
  - related: [DDB-TABLE-020](#ddb-table-020), [DDB-TABLE-080](#ddb-table-080), [DDB-TABLE-021](#ddb-table-021), [DDB-TABLE-139](#ddb-table-139), [DDB-TABLE-023](#ddb-table-023), [DDB-TABLE-037](#ddb-table-037) ·
    evidence: table/error-taxonomy/kms-key-states
  - notes: A controller that treats 5xx as retryable will retry this user error forever; the KMS text
    'EncryptionContext is supported only when creating a grant for a symmetric encryption KMS key' is the only
    signal.

- <a id="ddb-table-023"></a>**DDB-TABLE-023** `error-code` · impact medium · unhandled (not handled in controller) · verified 2026-10-08
  **KMS key policy denying kms:CreateGrant -> AccessDeniedException (400) from CreateTable/UpdateTable, naming
  the caller's STS session** - With a key policy containing Deny kms:CreateGrant,
  CreateTable/UpdateTable(SSESpecification KMS <key>) fail synchronously with AccessDeniedException HTTP 400:
  'KMS key access denied error: com.amazonaws.services.kms.m... - see:
  [details/DDB-TABLE-023.md](details/DDB-TABLE-023.md)

- <a id="ddb-table-037"></a>**DDB-TABLE-037** `error-code` · impact medium · unhandled (not handled in controller) · verified 2026-10-08
  **CreateTable with a non-existent KMS alias or another account's key ARN: codes and messages** -
  KMSMasterKeyId=alias/ackq-does-not-exist -> ValidationException: 'KMS validation error:
  com.amazonaws.services.kms.model.NotFoundException: Alias
  arn:aws:kms:us-west-2:<ACCOUNT>:alias/ackq-does-not-exist is not found. - see:
  [details/DDB-TABLE-037.md](details/DDB-TABLE-037.md)

- <a id="ddb-table-139"></a>**DDB-TABLE-139** `error-code` · impact high · tracked in GitHub issue (not handled) · verified 2026-10-08
  **UpdateTable with an unusable KMS key is rejected synchronously; table and SSEDescription unchanged; rejections do not consume the SSE quota**
  On a CMK table: disabled key -> ValidationException: 'KMS key disabled error:
  com.amazonaws.services.kms.model.DisabledException:
  arn:aws:kms:us-west-2:<ACCOUNT>:key/16530a9a-7be6-4b9e-b402-c933603f3327 is disabled. (Service: AWSKMS;
  Status Code: 400; Error Code: DisabledException; Request ID: 99ee1c40-8909-463f-a014-28a0c1a0df16; Proxy:
  null)'. PendingDeletion key -> ValidationException: 'KMS validation error:
  com.amazonaws.services.kms.model.KMSInvalidStateException:
  arn:aws:kms:us-west-2:<ACCOUNT>:key/7ae3758e-f42e-4c34-b7e8-559326908365 is pending deletion. (Service:
  AWSKMS; Status Code: 400; Error Code: KMSInvalidStateException; Request ID:
  3ce81ce8-c4b2-4832-9320-5ef3cef65bde'. Asymmetric RSA_2048 key -> InternalServerError: 'KMS internal error:
  com.amazonaws.services.kms.model.AWSKMSException: EncryptionContext is supported only when creating a grant
  for a symmetric encryption KMS key. (Service: AWSKMS; Status Code: 400; Error Code: ValidationException;
  Request ID: 748a9dcd-a5bc-45b6-a13c-806d80b7d169; Proxy: null)'. Other-account key ARN ->
  AccessDeniedException: 'KMS key access denied error: com.amazonaws.services.kms.model.AWSKMSException: User:
  arn:aws:sts::<ACCOUNT>:assumed-role/<PRINCIPAL> is not authorized to perform: kms:DescribeKey on this
  resource because the resource does not exist in this Region, no resource-based policies allow acce'. Missing
  alias -> ValidationException: 'KMS validation error: com.amazonaws.services.kms.model.NotFoundException:
  Alias arn:aws:kms:us-west-2:<ACCOUNT>:alias/ackq-does-not-exist-8def75 is not found. (Service: AWSKMS;
  Status Code: 400; Error Code: NotFoundException; Request ID: 38ce56e8-29c6-4a00-a42b-38e4d4171de3; Proxy:
  null)'. State afterwards: {"TableStatus": "ACTIVE", "SSE": {"Status": "ENABLED", "SSEType": "KMS",
  "KMSMasterKeyArn": "arn:aws:kms:us-west-2:<ACCOUNT>:key/8963cda5-0fac-4af1-8635-0af67d16fe14"}}. The next
  real change (K1->K2) then succeeded: OK (resp TableStatus=ACTIVE, resp SSE={"Status": "UPDATING",.
  - ACK: terminal_codes, requeue · ops: UpdateTable, DescribeTable · fields: SSESpecification.KMSMasterKeyId,
    SSEDescription
  - repro: CMK table; UpdateTable SSESpecification KMSMasterKeyId = disabled / PendingDeletion / RSA_2048 /
    foreign / unknown alias
  - handling: tracked in https://github.com/aws-controllers-k8s/community/issues/2136 (not handled) · code refs: `pkg/resource/table/hooks.go:583-619`
  - related: [DDB-TABLE-020](#ddb-table-020), [DDB-TABLE-080](#ddb-table-080), [DDB-TABLE-021](#ddb-table-021), [DDB-TABLE-022](#ddb-table-022), [DDB-TABLE-023](#ddb-table-023), [DDB-TABLE-037](#ddb-table-037) ·
    evidence: table/mutation-matrix/sse-kms-quota
  - notes: Hypotheses: H-T-111. UpdateTable half.

- <a id="ddb-table-461"></a>**DDB-TABLE-461** `error-code` · impact high · unhandled (not handled in controller) · verified 2026-10-09
  **Unusable KMS key taxonomy by key type: HMAC and RSA keys -> HTTP 500 on Create/Update/RestoreFromBackup/RestoreToPointInTime...**
  The wire code for an unusable KMSMasterKeyId depends on WHY the key is unusable, not on the operation: a
  symmetric-encryption-incapable key (HMAC_256 GENERATE_VERIFY_MAC, like the known RSA_2048 case) -> HTTP 500
  InternalServerError 'KMS internal error: com.amazonaws.services.kms.model.AWSKMSException: EncryptionContext
  is supported only when creating a grant for a symmetric encryption KMS key. (Service: AWSKMS; Status Code:
  400; Error Code: ValidationException; Request ID: <uuid>; Proxy: null)' on CreateTable, UpdateTable,
  RestoreTableFromBackup AND RestoreTableToPointInTime (SSESpecificationOverride), deterministic (6/6, 30-200
  ms); another service's AWS-managed key ('alias/aws/s3') -> HTTP 400 AccessDeniedException 'KMS key access
  denied error: ...AWSKMSException: User: <caller ARN> is not authorized to perform: kms:CreateGrant on
  resource: <key ARN> because no resource-based policy allows the kms:CreateGrant action (...)'; a disabled
  key -> 400 ValidationException 'KMS key disabled error: ...DisabledException: <key ARN> is disabled...'; a
  nonexistent key ARN -> 400 ValidationException 'KMS validation error: ...NotFoundException: Key '<arn>' does
  not exist...'. In every case the target/new table does NOT exist afterwards (checked at +0 s, +1 s and in a
  later sweep) and an existing table keeps its SSEDescription; an immediate retry of the 500 returns the same
  500 and a corrected retry to the same target name is accepted (200). All three codes are PERMANENT-SPEC for
  the controller; the 500 and the AccessDenied would be misclassified as 'transient' / 'IAM problem' by a
  code-based mapper - the stable substrings are 'EncryptionContext is supported only when creating a grant for
  a symmetric encryption KMS key', 'kms:CreateGrant', 'is disabled', 'does not exist'. Every text carries a
  per-request KMS request id (uuid).
  - ACK: terminal_codes, requeue · ops: CreateTable, UpdateTable, RestoreTableFromBackup,
    RestoreTableToPointInTime · fields: SSESpecification.KMSMasterKeyId,
    SSESpecificationOverride.KMSMasterKeyId
  - repro: CreateKey KeySpec=HMAC_256 KeyUsage=GENERATE_VERIFY_MAC; RestoreTableFromBackup
    SSESpecificationOverride={Enabled:true,SSEType:KMS,KMSMasterKeyId:<hmac arn>} -> 500; UpdateTable
    SSESpecification KMSMasterKeyId=alias/aws/s3 -> AccessDenied
  - handling: not handled in the controller (as of commit 34b85e6)
  - related: [DDB-TABLE-022](#ddb-table-022), [DDB-TABLE-080](#ddb-table-080), [DDB-TABLE-023](#ddb-table-023), [DDB-TABLE-037](#ddb-table-037), [DDB-TABLE-463](table-restore.md#ddb-table-463), [DDB-TABLE-457](service.md#ddb-table-457),
    [DDB-TABLE-276](table-restore.md#ddb-table-276), [DDB-TABLE-219](table-restore.md#ddb-table-219) · evidence: table/creative/restore-5xx-side-effect,
    table/creative/degenerate-5xx-hunt
  - notes: Extends [DDB-TABLE-022](#ddb-table-022)/080 (RSA_2048 -> 500 on Create/Update) to HMAC keys and to both restore
    paths, and adds the AccessDenied class for foreign AWS-managed keys. Rows (this probe): rst_bk_hmac -> 500
    InternalServerError 63ms 'KMS internal error: com.amazonaws.services.kms.model.AWSKMSException:...
  - full notes: [details/DDB-TABLE-461.md](details/DDB-TABLE-461.md)

## Request validation

- <a id="ddb-table-031"></a>**DDB-TABLE-031** `request-validation` · impact high · unhandled (not handled in controller) · verified 2026-10-08
  **SSESpecification.KMSMasterKeyId without SSEType=KMS is rejected by CreateTable**
  CreateTable with SSESpecification{Enabled:true, KMSMasterKeyId:<alias|key id>} and no SSEType fails with
  ValidationException (HTTP 400): 'One or more parameter values were invalid: SSEType KMS is required if
  KMSMasterKeyId is specified'. The same key id/alias/ARN with SSEType=KMS is accepted
  (sse-keyid/sse-labalias/sse-keyarn shapes).
  - ACK: custom_create, custom_update, docs-only · ops: CreateTable · fields: SSESpecification.SSEType,
    SSESpecification.KMSMasterKeyId
  - repro: CreateTable SSESpecification={Enabled:true, KMSMasterKeyId:'alias/aws/dynamodb'} (no SSEType)
  - handling: not handled in the controller (as of commit 34b85e6)
  - related: [DDB-TABLE-027](#ddb-table-027), [DDB-TABLE-036](#ddb-table-036), [DDB-TABLE-044](#ddb-table-044), [DDB-TABLE-079](#ddb-table-079), [DDB-TABLE-140](#ddb-table-140) · evidence:
    table/round-trip/full-fields
  - notes: Hypotheses: H-T-027, H-T-112. A controller that lets users set only kmsMasterKeyID must inject
    SSEType=KMS itself.

- <a id="ddb-table-035"></a>**DDB-TABLE-035** `request-validation` · impact high · handled · verified 2026-10-08
  **CreateTable StreamSpecification: StreamEnabled=false with StreamViewType and StreamEnabled=true without it are both rejected**
  {StreamEnabled:false, StreamViewType:NEW_IMAGE} -> ValidationException: 'One or more parameter values were
  invalid: Table is being created with a stream disabled, UpdateViewType should not be specified'.
  {StreamEnabled:true} (no view type) -> ValidationException: 'One or more parameter values were invalid:
  Table is being created with a stream enabled, UpdateViewType is required'. {StreamViewType only} ->
  ValidationException: '1 validation error detected: Value null at 'streamSpecification.streamEnabled' failed
  to satisfy constraint: Member must not be null'. {} -> ValidationException: '1 validation error detected:
  Value null at 'streamSpecification.streamEnabled' failed to satisfy constraint: Member must not be null'.
  lowercase view type -> ValidationException: '1 validation error detected: Value 'new_image' at
  'streamSpecification.streamViewType' failed to satisfy constraint: Member must satisfy enum value set:
  [OLD_IMAGE, KEYS_ONLY, NEW_AND_OLD_IMAGES, NEW_IMAGE]'.
  - ACK: custom_update, custom_create · ops: CreateTable · fields: StreamSpecification.StreamEnabled,
    StreamSpecification.StreamViewType
  - repro: CreateTable with the listed StreamSpecification shapes (parameter validation disabled client-side)
  - handling: handled via `pkg/resource/table/hooks.go:401-420; b0b0d59`
  - related: [DDB-TABLE-050](#ddb-table-050), [DDB-TABLE-018](#ddb-table-018), [DDB-TABLE-002](#ddb-table-002), [DDB-TABLE-029](#ddb-table-029), [DDB-TABLE-040](table-throughput-billing.md#ddb-table-040), [DDB-TABLE-036](#ddb-table-036) ·
    evidence: table/weird-inputs/create-validation
  - notes: Hypotheses: H-T-025. Create side of H-T-025; the UpdateTable side is in
    table/mutation-matrix/stream-protection-throughput.

- <a id="ddb-table-036"></a>**DDB-TABLE-036** `request-validation` · impact high · handled · verified 2026-10-08
  **CreateTable SSESpecification: Enabled=false with SSEType/KMSMasterKeyId, and SSEType=AES256, are rejected (exact messages)**
  {Enabled:false, SSEType:KMS} -> ValidationException: 'One or more parameter values were invalid: SSEType can
  not be specified if Enabled is false'. {Enabled:false, KMSMasterKeyId} -> ValidationException: 'One or more
  parameter values were invalid: KMSMasterKeyId can not be specified if Enabled is false'. {Enabled:false,
  SSEType:KMS, KMSMasterKeyId} -> ValidationException: 'One or more parameter values were invalid: SSEType can
  not be specified if Enabled is false'. {Enabled:true, SSEType:AES256} -> ValidationException: 'One or more
  parameter values were invalid: SSEType AES256 is not supported'. {Enabled:false, SSEType:AES256} ->
  ValidationException: 'One or more parameter values were invalid: SSEType can not be specified if Enabled is
  false'. {} -> ACCEPTED (table created). {SSEType:KMS} without Enabled -> ACCEPTED. {SSEType:KMS,
  KMSMasterKeyId} without Enabled -> ACCEPTED. lowercase 'kms' -> ValidationException: '1 validation error
  detected: Value 'kms' at 'sSESpecification.sSEType' failed to satisfy constraint: Member must satisfy enum
  value set: [AES256, KMS]'.
  - ACK: custom_create, custom_update · ops: CreateTable · fields: SSESpecification.Enabled,
    SSESpecification.SSEType, SSESpecification.KMSMasterKeyId
  - repro: CreateTable with the listed SSESpecification shapes
  - handling: handled via `pkg/resource/table/hooks.go:446-465; test/e2e/tests/test_table.py:641-673`
  - related: [DDB-TABLE-027](#ddb-table-027), [DDB-TABLE-044](#ddb-table-044), [DDB-TABLE-031](#ddb-table-031), [DDB-TABLE-079](#ddb-table-079), [DDB-TABLE-140](#ddb-table-140), [DDB-TABLE-050](#ddb-table-050),
    [DDB-TABLE-035](#ddb-table-035), [DDB-TABLE-018](#ddb-table-018), [DDB-TABLE-002](#ddb-table-002), [DDB-TABLE-029](#ddb-table-029), [DDB-TABLE-040](table-throughput-billing.md#ddb-table-040) · evidence:
    table/weird-inputs/create-validation
  - notes: Hypotheses: H-T-027.

- <a id="ddb-table-044"></a>**DDB-TABLE-044** `request-validation` · impact medium · handled · verified 2026-10-08
  **SSESpecification without the Enabled member is accepted: {} -> no SSEDescription,
  {SSEType:KMS[,KMSMasterKeyId]} -> KMS encryption enabled** - CreateTable SSESpecification={} -> OK (Describe
  SSEDescription: "<absent>"). - see: [details/DDB-TABLE-044.md](details/DDB-TABLE-044.md)

- <a id="ddb-table-328"></a>**DDB-TABLE-328** `request-validation` · impact medium · unhandled (not handled in controller) · verified 2026-10-09
  **Multi-region KMS key: the other region's ARN of the same MRK is rejected (Invalid arn); bare mrk-id/local
  ARN/alias accepted** - MRK primary in us-west-2 (KeyId mrk-67c1526d2d8940a3947df29d0358c35a), replica in
  us-east-1 with the identical KeyId (replica key Enabled 60 s after ReplicateKey). - see:
  [details/DDB-TABLE-328.md](details/DDB-TABLE-328.md)

## Update granularity and ordering

- <a id="ddb-table-050"></a>**DDB-TABLE-050** `update-granularity` · impact high · SUSPECTED CONTROLLER BUG · verified 2026-10-09, re-verified
  **Stream view type cannot be changed in place; disable requires no StreamViewType; LatestStreamArn survives disable**
  Enable {true,NEW_IMAGE} -> OK (TableStatus=UPDATING), UPDATING 4.05s. Re-send same -> ValidationException:
  'Table already has an enabled stream: TableName: ackq-8c14eb-mm-a'. Change to NEW_AND_OLD_IMAGES while
  enabled -> ValidationException: 'Table already has an enabled stream: TableName: ackq-8c14eb-mm-a'. {true}
  without view type -> ValidationException: 'One or more parameter values were invalid: If stream is being
  enabled then UpdateViewType is required'. Disable with {false,KEYS_ONLY} -> ValidationException: 'One or
  more parameter values were invalid: If stream is being disabled, then UpdateViewType must not be specified'.
  Disable {false} -> OK (TableStatus=UPDATING), UPDATING 5.07s; DescribeTable afterwards:
  {"StreamSpecification": "<absent>", "LatestStreamArn":
  "arn:aws:dynamodb:us-west-2:<ACCOUNT>:table/ackq-8c14eb-mm-a/stream/2026-10-08T23:07:31.275",
  "LatestStreamLabel": "2026-10-08T23:07:31.275"}. Disable again -> ValidationException: 'Table has no stream
  to disable: TableName: ackq-8c14eb-mm-a'. Re-enable -> OK (TableStatus=UPDATING), UPDATING 5.06s; new
  LatestStreamArn differs: True. Old stream DescribeStream: {"ok": true, "code": null, "status": "DISABLED"}.
  - ACK: custom_update, requeue, compare.is_ignored+delta_pre_compare · ops: UpdateTable, DescribeTable ·
    fields: StreamSpecification.StreamEnabled, StreamSpecification.StreamViewType, LatestStreamArn,
    LatestStreamLabel
  - repro: PPR table; UpdateTable stream enable NEW_IMAGE; re-send; change view type; disable with view type;
    disable; disable again; re-enable NEW_AND_OLD_IMAGES
  - measurements: stream_enable_updating_s=4.05, stream_disable_updating_s=5.07,
    stream_reenable_updating_s=5.06
  - handling: suspected controller bug - see Handling gaps
  - related: [DDB-TABLE-035](#ddb-table-035), [DDB-TABLE-018](#ddb-table-018), [DDB-TABLE-002](#ddb-table-002), [DDB-TABLE-029](#ddb-table-029), [DDB-TABLE-040](table-throughput-billing.md#ddb-table-040), [DDB-TABLE-036](#ddb-table-036),
    [DDB-TABLE-362](#ddb-table-362), [DDB-TABLE-367](#ddb-table-367), [DDB-TABLE-378](#ddb-table-378), [DDB-TABLE-180](#ddb-table-180), [DDB-TABLE-004](#ddb-table-004), [DDB-TABLE-373](service.md#ddb-table-373), [DDB-TABLE-013](service.md#ddb-table-013),
    [DDB-TABLE-071](service.md#ddb-table-071), [DDB-TABLE-213](#ddb-table-213) · evidence: table/mutation-matrix/stream-protection-throughput,
    table/creative/reverify-set-b
  - notes: Hypotheses: H-T-025, H-T-026, H-T-036.
  - full notes: [details/DDB-TABLE-050.md](details/DDB-TABLE-050.md)

- <a id="ddb-table-092"></a>**DDB-TABLE-092** `update-granularity` · impact high · unhandled (not handled in controller) · verified 2026-10-08
  **ContributorInsightsMode changes via ENABLE (re-ENABLING); omitting mode reverts to ACCESSED_AND_THROTTLED_KEYS; DISABLE with mode accepted**
  Initial ENABLE (no mode) -> mode ACCESSED_AND_THROTTLED_KEYS, rules
  ['DynamoDBContributorInsights-PKC-ackq-244c78-i1-1791500895719',
  'DynamoDBContributorInsights-PKT-ackq-244c78-i1-1791500895719']. ENABLE mode=THROTTLED_KEYS on ENABLED table
  -> 200 OK status=ENABLING, transition [{'value': 'ENABLING', 'from_s': 0.01, 'to_s': 1.02, 'duration_s':
  1.01}, {'value': 'ENABLED', 'from_s': 1.02, 'to_s': None, 'duration_s': None}], rules after
  ['DynamoDBContributorInsights-PKT-ackq-244c78-i1-1791500895719']. ENABLE (no mode) afterwards -> 200 OK
  status=ENABLING, Describe mode=ACCESSED_AND_THROTTLED_KEYS, transition [{'value': 'ENABLING', 'from_s':
  0.01, 'to_s': 1.03, 'duration_s': 1.02}, {'value': 'ENABLED', 'from_s': 1.03, 'to_s': None, 'duration_s':
  None}]. ENABLE mode=ACCESSED_AND_THROTTLED_KEYS -> 200 OK status=ENABLED, transition [{'value': 'ENABLED',
  'from_s': 0.01, 'to_s': None, 'duration_s': None}]. DISABLE with mode=THROTTLED_KEYS -> 200 OK
  status=DISABLING.
  - ACK: custom_update, late_initialize, compare.is_ignored+delta_pre_compare · ops:
    UpdateContributorInsights, DescribeContributorInsights · fields: ContributorInsightsMode,
    ContributorInsightsRuleList
  - repro: ENABLE; ENABLE mode=THROTTLED_KEYS; ENABLE; ENABLE mode=ACCESSED_AND_THROTTLED_KEYS; DISABLE
    mode=THROTTLED_KEYS
  - handling: not handled in the controller (as of commit 34b85e6)
  - related: [DDB-TABLE-089](table-subresources.md#ddb-table-089), [DDB-TABLE-090](table-subresources.md#ddb-table-090), [DDB-TABLE-091](table-subresources.md#ddb-table-091), [DDB-TABLE-341](table-subresources.md#ddb-table-341), [DDB-TABLE-339](table-subresources.md#ddb-table-339), [DDB-TABLE-343](table-subresources.md#ddb-table-343),
    [DDB-TABLE-340](table-subresources.md#ddb-table-340) · evidence: table/sub-resources/insights-lifecycle
  - notes: Hypotheses: H-S-021. H-S-021 partially confirmed: a mode change is done with ENABLE on an
    already-ENABLED resource and re-transitions through ENABLING (~1 s), rule list shrinks 2->1 (hash-only)
    under THROTTLED_KEYS. REFUTED clauses: ENABLE without a mode does NOT keep THROTTLED_KEYS - it reverts
    to...
  - full notes: [details/DDB-TABLE-092.md](details/DDB-TABLE-092.md)

- <a id="ddb-table-382"></a>**DDB-TABLE-382** `update-granularity` · impact high · unhandled (not handled in controller) · verified 2026-10-09
  **UpdateTable is single-concern: 33/35 pairs of PT/ODT, Stream, DP, SSE, TableClass, Warm rejected on an idle table; only BillingMode combines**
  Idle ACTIVE tables (PROVISIONED 10/10 and PAY_PER_REQUEST), every pair sent with both members valid on their
  own: 33 of 35 pairs fail synchronously with ValidationException and nothing is applied. Messages:
  'DeletionProtection modification must be the only operation in the request' (DP + anything), 'Server-Side
  Encryption modification must be the only operation in the request' (SSE + anything), 'TableClass
  modification must be the only operation in the request', 'WarmThroughput must be the only operation in the
  request', 'OnDemandThroughput can only be combined with BillingMode in an UpdateTable request' (ODT +
  Stream/Warm) and - for ProvisionedThroughput + StreamSpecification and for BillingMode=PROVISIONED(+PT) +
  StreamSpecification - 'You cannot modify stream status while updating table IOPS' although no IOPS update is
  running; PT + GlobalSecondaryIndexUpdates.Delete likewise -> 'You cannot create or delete index while
  updating table IOPS'. Accepted pairs: BillingMode=PAY_PER_REQUEST (switch from PROVISIONED, or no-op
  re-send) + StreamSpecification (both applied; switch UPDATING 147 s), OnDemandThroughput +
  BillingMode=PAY_PER_REQUEST (no-op), and the known BillingMode/PT/GSI-Update family.
  - ACK: one-per-reconcile, custom_update, requeue · ops: UpdateTable · fields: ProvisionedThroughput,
    OnDemandThroughput, StreamSpecification, DeletionProtectionEnabled, SSESpecification, TableClass,
    WarmThroughput, BillingMode
  - repro: CreateTable PROVISIONED 10/10 -> ACTIVE -> UpdateTable with {ProvisionedThroughput 11/11 +
    StreamSpecification enable}; repeat for each pair (toggle values derived from DescribeTable)
  - measurements: pairs_tested=35, pairs_rejected=33, switch_to_ppr_plus_stream_updating_s=147.5
  - handling: not handled in the controller (as of commit 34b85e6)
  - related: [DDB-TABLE-156](table-throughput-billing.md#ddb-table-156), [DDB-TABLE-174](table-indexes.md#ddb-table-174), [DDB-TABLE-224](table-replicas.md#ddb-table-224), [DDB-TABLE-433](#ddb-table-433), [DDB-TABLE-175](table-indexes.md#ddb-table-175), [DDB-TABLE-163](table-subresources.md#ddb-table-163),
    [DDB-TABLE-286](#ddb-table-286), [DDB-TABLE-159](table-indexes.md#ddb-table-159), [DDB-TABLE-152](table-indexes.md#ddb-table-152), [DDB-TABLE-380](table-indexes.md#ddb-table-380) · evidence: table/creative/update-pair-matrix,
    table/creative/update-atomicity
  - notes: Extends [DDB-TABLE-174](table-indexes.md#ddb-table-174) (GSI Create + X) and [DDB-TABLE-156](table-throughput-billing.md#ddb-table-156) (BillingMode + DP) to the full pairwise
    matrix of non-index members: a controller that sends "everything that differs" in one UpdateTable can
    never succeed when two concerns differ; it must issue one call per concern (DP, SSE, TableClass,...
  - full notes: [details/DDB-TABLE-382.md](details/DDB-TABLE-382.md)

- <a id="ddb-table-433"></a>**DDB-TABLE-433** `update-granularity` · impact high · unhandled (not handled in controller) · verified 2026-10-09
  **UpdateTable is one-logical-change-per-call: DP, SSE, TableClass, WarmThroughput each 'must be the only operation'; most pairs rejected**
  33 combinations of plain UpdateTable fields sent in ONE call on fresh tables, all answered synchronously
  (7-9 ms) and nothing partially applied. Rejected with ValidationException 'One or more parameter values were
  invalid: <X> modification must be the only operation in the request' for X = Server-Side Encryption,
  TableClass, DeletionProtection and 'WarmThroughput must be the only operation in the request' whenever
  SSESpecification / TableClass / DeletionProtectionEnabled / WarmThroughput is paired with anything
  (DP+STREAM, DP+ODT, DP+BillingMode, DP+PT, SSE+*, CLASS+*, WARM+*). OnDemandThroughput: 'OnDemandThroughput
  can only be combined with BillingMode in an UpdateTable request' (STREAM+ODT, WARM+ODT); StreamSpecification +
  ProvisionedThroughput or + BillingMode=PROVISIONED: 'You cannot modify stream status while updating table
  IOPS'. The only accepted pairs: BillingMode=PAY_PER_REQUEST + OnDemandThroughput (200, both applied) and
  BillingMode=PAY_PER_REQUEST + StreamSpecification (200, both applied), on a PROVISIONED table. Precedence of
  the exclusivity message when several exclusive fields are present: SSE > TableClass > DeletionProtection >
  WarmThroughput (e.g. DP+SSE names SSE, DP+CLASS names TableClass, WARM+DP names DP).
  - ACK: one-per-reconcile, custom_update, requeue · ops: UpdateTable · fields: DeletionProtectionEnabled,
    SSESpecification, TableClass, WarmThroughput, OnDemandThroughput, StreamSpecification, BillingMode,
    ProvisionedThroughput
  - repro: Fresh PPR (or PROVISIONED 1/1) table per combination; UpdateTable with the two (or 3-5) fields
    together; DescribeTable at T+0 to confirm nothing applied; for accepted calls wait ACTIVE and verify both
    changes.
  - measurements: combinations_tested=33, accepted=2, ppr_plus_odt_updating_s=42.3,
    ppr_plus_stream_updating_s=16.2, billing_to_provisioned_alone_updating_s=60.5
  - handling: not handled in the controller (as of commit 34b85e6)
  - related: [DDB-TABLE-156](table-throughput-billing.md#ddb-table-156), [DDB-TABLE-174](table-indexes.md#ddb-table-174), [DDB-TABLE-199](table-replicas.md#ddb-table-199), [DDB-TABLE-224](table-replicas.md#ddb-table-224), [DDB-TABLE-152](table-indexes.md#ddb-table-152), [DDB-TABLE-056](table-throughput-billing.md#ddb-table-056),
    [DDB-TABLE-062](table-throughput-billing.md#ddb-table-062), [DDB-TABLE-064](table-throughput-billing.md#ddb-table-064), [DDB-TABLE-067](table-throughput-billing.md#ddb-table-067), [DDB-TABLE-183](table-throughput-billing.md#ddb-table-183), [DDB-TABLE-369](#ddb-table-369), [DDB-TABLE-452](table-throughput-billing.md#ddb-table-452), [DDB-TABLE-055](table-throughput-billing.md#ddb-table-055),
    [DDB-TABLE-024](table-throughput-billing.md#ddb-table-024), [DDB-TABLE-038](table-throughput-billing.md#ddb-table-038), [DDB-TABLE-039](table-throughput-billing.md#ddb-table-039), [DDB-TABLE-057](table-throughput-billing.md#ddb-table-057), [DDB-TABLE-059](table-throughput-billing.md#ddb-table-059), [DDB-TABLE-018](#ddb-table-018), [DDB-TABLE-358](table-throughput-billing.md#ddb-table-358),
    [DDB-TABLE-162](table-indexes.md#ddb-table-162), [DDB-TABLE-127](table-indexes.md#ddb-table-127), [DDB-TABLE-382](#ddb-table-382) · evidence: table/creative/xs-update-combos,
    table/creative/xs-update-combos-2
  - notes: Generalizes [DDB-TABLE-156](table-throughput-billing.md#ddb-table-156) (DP + BillingMode) and 174/199/224 (GSI Create, ReplicaUpdates) to ALL
    plain fields: a generated sdkUpdate that copies every changed spec field into one UpdateTable request
    fails whenever two of {deletionProtection, sse, tableClass, warmThroughput, stream,...
  - full notes: [details/DDB-TABLE-433.md](details/DDB-TABLE-433.md)

- <a id="ddb-table-435"></a>**DDB-TABLE-435** `update-granularity` · impact medium · unhandled (not handled in controller) · verified 2026-10-09
  **Fan-out of UpdateTable(stream)+TTL+PITR+Tag+Policy+Insights on ONE table at the same instant: no per-table
  conflict, all settings land** - Six different write APIs against the same fresh table fired within 0.12 s:
  UpdateTable(StreamSpecification enable) 200, UpdateTimeToLive(enable) 200, UpdateContinuousBackups(PITR on)
  200, PutResourcePolicy 200, Update... - see: [details/DDB-TABLE-435.md](details/DDB-TABLE-435.md)

## Field behavior (defaults, normalization, shapes, immutability)

- <a id="ddb-table-078"></a>**DDB-TABLE-078** `normalization` · impact high · unhandled (not handled in controller) · verified 2026-10-08
  **KMS alias resolved once at Create/UpdateTable: repointing the alias does not move the table; re-sending the alias migrates it**
  CreateTable KMSMasterKeyId=alias -> SSEDescription.KMSMasterKeyArn = K1 ARN. Re-sending the same alias -> OK
  (response TableStatus=ACTIVE, response SSE={"Status": "UPDATING", "SSEType": "KMS", "KMSMasterKeyArn":
  "arn:aws:kms:us-west-2:<ACCOUNT>:key/f481fbde-3c2b-4538-b2da-34a4fbb96282"}; settled in 22.27s via
  ACTIVE/UPDATING->ACTIVE/ENABLED; after={"Status": "ENABLED", "SSEType": "KMS", "KMSMasterKeyArn":
  "arn:aws:kms:us-west-2:<ACCOUNT>:key/f481fbde-3c2b-4538-b2da-34a4fbb96282"}); same key as ARN ->
  ValidationException: 'One or more parameter values were invalid: Table is already encrypted with given
  KMSMasterKeyId. Use KMSMasterKeyId parameter if you want to change Master Key'; as key id -> OK (response
  TableStatus=ACTIVE, response SSE={"Status": "UPDATING", "SSEType": "KMS", "KMSMasterKeyArn":
  "arn:aws:kms:us-west-2:<ACCOUNT>:key/f481fbde-3c2b-4538-b2da-34a4fbb96282"}; settled in 21.26s via
  ACTIVE/UPDATING->ACTIVE/ENABLED; after={"Status": "ENABLED", "SSEType": "KMS", "KMSMasterKeyArn":
  "arn:aws:kms:us-west-2:<ACCOUNT>:key/f481fbde-3c2b-4538-b2da-34a4fbb96282"}). After kms:UpdateAlias to K2,
  DescribeTable watched for 150s: followed alias = False. Re-sending UpdateTable
  SSESpecification{Enabled:true,SSEType:KMS,KMSMasterKeyId:alias} -> OK (response TableStatus=ACTIVE, response
  SSE={"Status": "UPDATING", "SSEType": "KMS", "KMSMasterKeyArn":
  "arn:aws:kms:us-west-2:<ACCOUNT>:key/f481fbde-3c2b-4538-b2da-34a4fbb96282"}; settled in 22.27s via
  ACTIVE/UPDATING->ACTIVE/UPDATING->ACTIVE/ENABLED; after={"Status": "ENABLED", "SSEType": "KMS",
  "KMSMasterKeyArn": "arn:aws:kms:us-west-2:<ACCOUNT>:key/3ae36f3d-c39c-4ca2-ad5c-65b410eaa7ef"}). Final key
  is K2: True.
  - ACK: custom_update, compare.is_ignored+delta_pre_compare, references · ops: CreateTable, UpdateTable,
    DescribeTable · fields: SSESpecification.KMSMasterKeyId, SSEDescription.KMSMasterKeyArn
  - repro: CreateTable with KMSMasterKeyId=alias/x -> K1; kms update-alias alias/x -> K2; DescribeTable (still
    K1); UpdateTable with the same alias
  - measurements: alias_drift_watch_s=150, resend_alias_settle_s=22.27
  - handling: not handled in the controller (as of commit 34b85e6)
  - related: [DDB-TABLE-082](#ddb-table-082), [DDB-TABLE-141](#ddb-table-141), [DDB-TABLE-140](#ddb-table-140), [DDB-TABLE-371](#ddb-table-371), [DDB-TABLE-065](#ddb-table-065), [DDB-TABLE-079](#ddb-table-079),
    [DDB-TABLE-081](#ddb-table-081), [DDB-TABLE-142](#ddb-table-142), [DDB-TABLE-120](#ddb-table-120), [DDB-TABLE-018](#ddb-table-018) · evidence: table/mutation-matrix/sse-kms
  - notes: Hypotheses: H-T-109.
  - full notes: [details/DDB-TABLE-078.md](details/DDB-TABLE-078.md)

- <a id="ddb-table-180"></a>**DDB-TABLE-180** `requested-vs-effective` · impact medium · handled · verified 2026-10-08
  **UpdateTable response and DescribeTable show requested StreamSpecification/LatestStreamArn during UPDATING;
  TableClassSummary only after** - Stream enable -> OK response={"TableStatus": "UPDATING",
  "StreamSpecification": {"StreamEnabled": true, "StreamViewType": "KEYS_ONLY"}, "LatestStreamArn":
  "arn:aws:dynamodb:us-west-2:<ACCOUNT>:table/ackq-2e2def-rf-e/s... - see:
  [details/DDB-TABLE-180.md](details/DDB-TABLE-180.md)

## Response fidelity and consistency

- <a id="ddb-table-026"></a>**DDB-TABLE-026** `response-fidelity` · impact high · handled · verified 2026-10-09, re-verified
  **TableClassSummary presence in DescribeTable depends on whether TableClass was sent at create**
  TableClassSummary present by create shape: {"min": false, "ppr": false, "prov-explicit": true, "full-prov":
  true, "full-ppr": true, "sse-false": false, "sse-type-only": false, "sse-awsalias": false, "sse-keyid":
  false, "sse-keyarn": false, "sse-labalias": false, "stream-new": false, "stream-old": false, "prov-warm":
  false}. Raw: {"min": "<absent>", "ppr": "<absent>", "prov-explicit": {"TableClass": "STANDARD"},
  "full-prov": {"TableClass": "STANDARD_INFREQUENT_ACCESS"}, "full-ppr": {"TableClass": "STANDARD"},
  "sse-false": "<absent>", "sse-type-only": "<absent>", "sse-awsalias": "<absent>", "sse-keyid": "<absent>",
  "sse-keyarn": "<absent>", "sse-labalias": "<absent>", "stream-new": "<absent>", "stream-old": "<absent>",
  "prov-warm": "<absent>"}.
  - ACK: compare.is_ignored+delta_pre_compare, late_initialize · ops: CreateTable, DescribeTable · fields:
    TableClass, TableClassSummary
  - repro: CreateTable without TableClass; CreateTable TableClass=STANDARD; CreateTable
    TableClass=STANDARD_INFREQUENT_ACCESS; DescribeTable each
  - handling: handled via `generator.yaml:12; templates/hooks/table/sdk_read_one_post_set_output.go.tpl:46-50`
  - related: [DDB-TABLE-177](#ddb-table-177), [DDB-TABLE-365](#ddb-table-365), [DDB-TABLE-284](#ddb-table-284), [DDB-TABLE-180](#ddb-table-180), [DDB-TABLE-019](#ddb-table-019) · evidence:
    table/round-trip/full-fields, table/creative/reverify-set-b
  - notes: Hypotheses: H-T-033. See behavior for the exact per-shape presence; 'min' and 'ppr' sent no
    TableClass, 'prov-explicit'/'full-ppr' sent STANDARD explicitly, 'full-prov' sent
    STANDARD_INFREQUENT_ACCESS.

- <a id="ddb-table-027"></a>**DDB-TABLE-027** `response-fidelity` · impact high · handled · verified 2026-10-09, re-verified
  **SSEDescription shape by SSESpecification sent at create (absent / Enabled=false / Enabled=true / CMK)**
  DescribeTable SSEDescription per create shape: {"min": "<absent>", "ppr": "<absent>", "prov-explicit":
  "<absent>", "full-prov": {"Status": "ENABLED", "SSEType": "KMS", "KMSMasterKeyArn":
  "arn:aws:kms:us-west-2:<ACCOUNT>:key/0e89ef16-bbf9-456f-be05-8000c64aa7d3"}, "full-ppr": {"Status":
  "ENABLED", "SSEType": "KMS", "KMSMasterKeyArn":
  "arn:aws:kms:us-west-2:<ACCOUNT>:key/301f0bb6-edf0-476a-9525-14f2ee58148a"}, "sse-false": "<absent>",
  "sse-type-only": {"Status": "ENABLED", "SSEType": "KMS", "KMSMasterKeyArn":
  "arn:aws:kms:us-west-2:<ACCOUNT>:key/301f0bb6-edf0-476a-9525-14f2ee58148a"}, "sse-awsalias": {"Status":
  "ENABLED", "SSEType": "KMS", "KMSMasterKeyArn":
  "arn:aws:kms:us-west-2:<ACCOUNT>:key/301f0bb6-edf0-476a-9525-14f2ee58148a"}, "sse-keyid": {"Status":
  "ENABLED", "SSEType": "KMS", "KMSMasterKeyArn":
  "arn:aws:kms:us-west-2:<ACCOUNT>:key/0e89ef16-bbf9-456f-be05-8000c64aa7d3"}, "sse-keyarn": {"Status":
  "ENABLED", "SSET. KMSMasterKeyArn equals the account's alias/aws/dynamodb key ARN for: {"full-ppr": true,
  "sse-type-only": true, "sse-awsalias": true}; equals the lab CMK ARN for: {"full-prov": true, "sse-keyid":
  true, "sse-keyarn": true, "sse-labalias": true}.
  - ACK: compare.is_ignored+delta_pre_compare, custom_update · ops: CreateTable, DescribeTable · fields:
    SSESpecification, SSEDescription
  - repro: CreateTable with no SSESpecification / Enabled:false / Enabled:true / Enabled:true+SSEType:KMS /
    KMSMasterKeyId as alias, key id, key ARN, alias/aws/dynamodb; DescribeTable each
  - handling: handled via `generator.yaml:10-11; generator.yaml:70-72; generator.yaml:73-77; pkg/resource/table/hooks.go:603-611`
  - related: [DDB-TABLE-036](#ddb-table-036), [DDB-TABLE-044](#ddb-table-044), [DDB-TABLE-031](#ddb-table-031), [DDB-TABLE-079](#ddb-table-079), [DDB-TABLE-140](#ddb-table-140) · evidence:
    table/round-trip/full-fields, table/creative/reverify-set-a1
  - notes: Hypotheses: H-T-034, H-T-112. Covers both hypotheses' create-side claims; the UpdateTable re-send
    no-op claim of H-T-112 is in table/mutation-matrix/sse-kms.

- <a id="ddb-table-029"></a>**DDB-TABLE-029** `response-fidelity` · impact medium · handled · verified 2026-10-08
  **StreamSpecification round-trip: absent when never enabled; {StreamEnabled:false} sent at create ->
  observed shape recorded** - DescribeTable StreamSpecification per shape: {"min": "<absent>", "ppr":
  "<absent>", "prov-explicit": "<absent>", "full-prov": {"StreamEnabled": true, "StreamViewType":
  "NEW_AND_OLD_IMAGES"}, "full-ppr": {"StreamEnable... - see:
  [details/DDB-TABLE-029.md](details/DDB-TABLE-029.md)

- <a id="ddb-table-177"></a>**DDB-TABLE-177** `stale-response` · impact high · handled · verified 2026-10-08
  **No-op UpdateTable calls (TableClass=STANDARD when unset, same BillingMode) return TableStatus=UPDATING but DescribeTable stays ACTIVE**
  TableClass=STANDARD on a table with no TableClassSummary -> OK response={"TableStatus": "UPDATING",
  "TableClassSummary": "<absent>", "BillingModeSummary": {"BillingMode": "PAY_PER_REQUEST",
  "LastUpdateToPayPerRequestDateTime": "2026-10-08 23:33:00.069000+00:00"}}; describe
  transitions={"timed_out": true, "transitions": [{"at_s": 0.01, "value": {"TableStatus": "ACTIVE",
  "TableClassSummary": "<absent>", "BillingModeSummary": {"BillingMode": "PAY_PER_REQUEST",
  "LastUpdateToPayPerRequestDateTime": "2026-10-08 23:33:00.069000+00:00"}}}]}. BillingMode=PAY_PER_REQUEST
  (same) -> OK response={"TableStatus": "UPDATING", "TableClassSummary": "<absent>", "BillingModeSummary":
  {"BillingMode": "PAY_PER_REQUEST", "LastUpdateToPayPerRequestDateTime": "2026-10-08
  23:33:00.069000+00:00"}}; describe transitions={"timed_out": true, "transitions": [{"at_s": 0.01, "value":
  {"TableStatus": "ACTIVE", "TableClassSummary": "<absent>", "BillingModeSummary": {"BillingMode":
  "PAY_PER_REQUEST". TableClass=STANDARD again -> OK response={"TableStatus": "UPDATING", "TableClassSummary":
  "<absent>", "BillingModeSummary": {"BillingMode": "PAY_PER_REQUEST", "LastUpdateToPayPerRequestDateTime":
  "2026-10-08 23:33:00.069000+00:00"}}; describe transitions={"timed_out": true, "transitions": [{"at_s":
  0.01, "value": {"TableStatus". TableClassSummary afterwards: {"TableClassSummary": "<absent>"}.
  - ACK: compare.is_ignored+delta_pre_compare, synced.when, requeue · ops: UpdateTable, DescribeTable ·
    fields: TableClass, TableClassSummary, BillingMode, TableStatus
  - repro: PPR table never given a TableClass; UpdateTable TableClass=STANDARD; DescribeTable at 0.5s
    intervals
  - handling: handled via `generator.yaml:12; templates/hooks/table/sdk_read_one_post_set_output.go.tpl:46-50`
  - related: [DDB-TABLE-019](#ddb-table-019), [DDB-TABLE-156](table-throughput-billing.md#ddb-table-156), [DDB-TABLE-057](table-throughput-billing.md#ddb-table-057), [DDB-TABLE-018](#ddb-table-018), [DDB-TABLE-365](#ddb-table-365), [DDB-TABLE-183](table-throughput-billing.md#ddb-table-183),
    [DDB-TABLE-358](table-throughput-billing.md#ddb-table-358), [DDB-TABLE-283](#ddb-table-283), [DDB-TABLE-026](#ddb-table-026), [DDB-TABLE-284](#ddb-table-284), [DDB-TABLE-180](#ddb-table-180), [DDB-TABLE-060](table-throughput-billing.md#ddb-table-060), [DDB-TABLE-370](table-throughput-billing.md#ddb-table-370),
    [DDB-TABLE-179](table-throughput-billing.md#ddb-table-179), [DDB-TABLE-066](table-throughput-billing.md#ddb-table-066), [DDB-TABLE-371](#ddb-table-371), [DDB-TABLE-140](#ddb-table-140), [DDB-TABLE-064](table-throughput-billing.md#ddb-table-064), [DDB-TABLE-452](table-throughput-billing.md#ddb-table-452) · evidence:
    table/response-fidelity/create-update-response
  - notes: UpdateTable's TableStatus is not a reliable signal that an asynchronous update started; and setting
    TableClass=STANDARD never makes TableClassSummary appear, so a spec of STANDARD diffs forever against nil.
  - full notes: [details/DDB-TABLE-177.md](details/DDB-TABLE-177.md)

- <a id="ddb-table-330"></a>**DDB-TABLE-330** `eventual-consistency` · impact medium · partially handled · verified 2026-10-09
  **After EnableKey the data plane works within <7 min but TableStatus stays
  INACCESSIBLE_ENCRYPTION_CREDENTIALS for 57 min** - Table 'traf' of
  table/state-machine/kms-inaccessible-lifecycle: CMK disabled 00:19:32, TableStatus flagged
  INACCESSIBLE_ENCRYPTION_CREDENTIALS at 01:00:55, key re-enabled 01:35:08 UTC. - see:
  [details/DDB-TABLE-330.md](details/DDB-TABLE-330.md)

- <a id="ddb-table-361"></a>**DDB-TABLE-361** `stale-response` · impact medium · unhandled (not handled in controller) · verified 2026-10-09
  **UpdateTable response-echo matrix: SSE change answers with a Status-only stub, OnDemandThroughput is
  synchronous (new values)** - Which UpdateTable mutations echo the requested value in the response and in
  DescribeTable at T+0 (new cells from this probe, known cells from related ids): OnDemandThroughput set
  {100,100} and change {200,200} -> resp... - see: [details/DDB-TABLE-361.md](details/DDB-TABLE-361.md)

- <a id="ddb-table-371"></a>**DDB-TABLE-371** `stale-response` · impact high · unhandled (not handled in controller) · verified 2026-10-09
  **KMS key change echoes the OLD KMSMasterKeyArn with Status=UPDATING for ~8-9 s in the UpdateTable response and DescribeTable**
  Table encrypted with the AWS-managed key. UpdateTable SSESpecification{Enabled:true, SSEType:KMS,
  KMSMasterKeyId:<CMK ARN>} -> 200, TableStatus=ACTIVE, response SSEDescription = {Status: UPDATING, SSEType:
  KMS, KMSMasterKeyArn: <OLD aws-managed ARN>}; DescribeTable keeps showing the OLD key with Status=UPDATING
  for 9.1 s, then the NEW key with Status=UPDATING for 14.2 s, then {ENABLED, KMS, <CMK>} at 23.3 s. The
  reverse change (KMSMasterKeyId=alias/aws/dynamodb) behaves identically: old CMK echoed for 8.1 s, new key
  UPDATING for 14.2 s, ENABLED at 22.3 s. By contrast CreateTable with SSESpecification returns {ENABLED, KMS,
  <ARN>} immediately in the create response.
  - ACK: synced.when, compare.is_ignored+delta_pre_compare, requeue · ops: UpdateTable, DescribeTable ·
    fields: SSESpecification.KMSMasterKeyId, SSEDescription.KMSMasterKeyArn, SSEDescription.Status
  - repro: CreateTable SSESpecification{Enabled:true,SSEType:KMS} -> wait ENABLED -> UpdateTable with a CMK
    ARN; record the response SSEDescription and poll DescribeTable at 1/s until Status=ENABLED with the new
    ARN; repeat back to alias/aws/dynamodb.
  - measurements: to_cmk_old_key_echo_s=9.1, to_cmk_new_key_updating_s=14.2, to_cmk_total_s=23.3,
    to_aws_managed_old_key_echo_s=8.1, to_aws_managed_total_s=22.3
  - handling: not handled in the controller (as of commit 34b85e6)
  - related: [DDB-TABLE-065](#ddb-table-065), [DDB-TABLE-079](#ddb-table-079), [DDB-TABLE-082](#ddb-table-082), [DDB-TABLE-141](#ddb-table-141), [DDB-TABLE-078](#ddb-table-078), [DDB-TABLE-060](table-throughput-billing.md#ddb-table-060),
    [DDB-TABLE-370](table-throughput-billing.md#ddb-table-370), [DDB-TABLE-284](#ddb-table-284), [DDB-TABLE-179](table-throughput-billing.md#ddb-table-179), [DDB-TABLE-066](table-throughput-billing.md#ddb-table-066), [DDB-TABLE-140](#ddb-table-140), [DDB-TABLE-064](table-throughput-billing.md#ddb-table-064), [DDB-TABLE-183](table-throughput-billing.md#ddb-table-183),
    [DDB-TABLE-180](#ddb-table-180), [DDB-TABLE-177](#ddb-table-177), [DDB-TABLE-156](table-throughput-billing.md#ddb-table-156), [DDB-TABLE-081](#ddb-table-081), [DDB-TABLE-142](#ddb-table-142), [DDB-TABLE-120](#ddb-table-120) · evidence:
    table/creative/xs-kms-echo-tags
  - notes: Direct DynamoDB counterpart of the ElastiCache 'Modify response echoes the old Durability' seed. A
    reconciler that reads back SSEDescription.KMSMasterKeyArn right after UpdateTable (or within ~9 s) sees
    the previous key and would compute a delta and re-issue the change; the re-issue is rejected with...
  - full notes: [details/DDB-TABLE-371.md](details/DDB-TABLE-371.md)

- <a id="ddb-table-451"></a>**DDB-TABLE-451** `stale-response` · impact high · unhandled (not handled in controller) · verified 2026-10-09, re-verified
  **SSE write during a TableClass->IA switch: TableClass=STANDARD at +1 s is 200 but dropped; class ends IA, one LastUpdateDateTime (cf. 287)**
  Table ackq-649535-cl-tc-sse: J at t0 (response UPDATING); W=sse at +0.26 s -> OK (response TableStatus
  ACTIVE); DescribeTable at +0.26 s: {"status": "ACTIVE", "tc": null, "tc_ts": null, "bm": "PAY_PER_REQUEST",
  "rcu": 0, "stream": null, "stream_arn_tail": null, "sse": "UPDATING", "warm": "ACTIVE", "dp": false, "odt":
  null}. J' at +1.02 s -> OK (response TableStatus UPDATING). Dense timeline (changes only): [{"t_s": 0.26,
  "status": "ACTIVE", "tc": null, "tc_ts": null, "bm": "PAY_PER_REQUEST", "rcu": 0, "stream": null,
  "stream_arn_tail": null, "sse": "UPDATING", "warm": "ACTIVE", "dp": false, "odt": null}, {"t_s": 3.1,
  "status": "ACTIVE", "tc": "STANDARD_INFREQUENT_ACCESS", "tc_ts": "2026-10-09 05:47:15.322000+00:00", "bm":
  "PAY_PER_REQUEST", "rcu": 0, "stream": null, "stream_arn_tail": null, "sse": "UPDATING", "warm": "ACTIVE",
  "dp": false, "odt": null}, {"t_s": 21.92, "status": "ACTIVE", "tc": "STANDARD_INFREQUENT_ACCESS", "tc_ts":
  "2026-10-09 05:47:15.322000+00:00", "bm": "PAY_PER_REQUEST", "rcu": 0, "stream": null, "stream_arn_tail":
  null, "sse": "ENABLED", "warm": "ACTIVE", "dp": false, "odt": null}]. Final view: {"t_s": 21.92, "status":
  "ACTIVE", "tc": "STANDARD_INFREQUENT_ACCESS", "tc_ts": "2026-10-09 05:47:15.322000+00:00", "bm":
  "PAY_PER_REQUEST", "rcu": 0, "stream": null, "stream_arn_tail": null, "sse": "ENABLED", "warm": "ACTIVE",
  "dp": false, "odt": null} - the job's target is in effect, J' left no trace.
  - ACK: synced.when, requeue, custom_update · ops: UpdateTable, DescribeTable · fields: TableStatus,
    TableClass
  - repro: UpdateTable(tc); sse write at +0.1 s; UpdateTable(reverse) at +1 s; watch DescribeTable
  - handling: not handled in the controller (as of commit 34b85e6)
  - related: [DDB-TABLE-287](#ddb-table-287), [DDB-TABLE-283](#ddb-table-283), [DDB-TABLE-365](#ddb-table-365), [DDB-TABLE-052](#ddb-table-052), [DDB-TABLE-284](#ddb-table-284), [DDB-TABLE-285](#ddb-table-285),
    [DDB-TABLE-180](#ddb-table-180), [DDB-TABLE-120](#ddb-table-120) · evidence: table/creative/clobber-matrix, table/creative/reverify-set-b
  - notes: Replication of [DDB-TABLE-287](#ddb-table-287) in a fresh run (tc-sse cell of the clobber matrix). TableClassSummary
    showed exactly one update (IA); the accepted STANDARD request never produced a second LastUpdateDateTime.

## Sub-resources

- <a id="ddb-table-213"></a>**DDB-TABLE-213** `sub-resource-api` · impact medium · unhandled (not handled in controller) · verified 2026-10-09
  **The stream ARN is an independent policy slot (own RevisionId, own 15s window); Get(table ARN) ignores it
  -> PolicyNotFoundException** - PutResourcePolicy(ResourceArn=LatestStreamArn, Resource=stream ARN, stream
  actions) -> 200 rev S, readable via Get(stream ARN) after 1.54s. - see:
  [details/DDB-TABLE-213.md](details/DDB-TABLE-213.md)

## Delete semantics

- <a id="ddb-table-016"></a>**DDB-TABLE-016** `delete-semantics` · impact high · handled · verified 2026-10-08
  **DeleteTable with DeletionProtectionEnabled=true -> ValidationException (HTTP 400); table stays ACTIVE**
  DeleteTable on a protected table fails with ValidationException HTTP 400: 'Resource cannot be deleted as it
  is currently protected against deletion. Disable deletion protection first.'. The table remains ACTIVE.
  After UpdateTable(DeletionProtectionEnabled=false) DeleteTable succeeds (OK).
  - ACK: custom_delete, terminal_codes, pre-delete-cleanup · ops: DeleteTable, UpdateTable · fields:
    DeletionProtectionEnabled
  - repro: CreateTable(DeletionProtectionEnabled=true) -> DeleteTable
  - handling: handled via `pkg/resource/table/hooks.go:729-731; pkg/resource/table/hooks.go:427-429; generator.yaml:88-90; pkg/resource/table/sdk.go:1234-1250`
  - related: [DDB-TABLE-034](#ddb-table-034), [DDB-TABLE-438](#ddb-table-438), [DDB-TABLE-030](#ddb-table-030) · evidence:
    table/error-taxonomy/missing-table-noop-update-dp
  - notes: Same ValidationException code as malformed requests; only the message identifies deletion
    protection. Retrying forever is pointless until the user flips the flag.

- <a id="ddb-table-069"></a>**DDB-TABLE-069** `delete-semantics` · impact high · handled · verified 2026-10-08
  **TableStatus=ACTIVE does not mean deletable: DeleteTable is ResourceInUseException during SSE and WarmThroughput updates**
  While SSEDescription.Status=UPDATING (TableStatus=ACTIVE) and while WarmThroughput.Status=UPDATING
  (TableStatus=ACTIVE), DeleteTable fails with ResourceInUseException HTTP 400 'Attempt to change a resource
  which is still in use: Table: <name> is in the process of being updated.' - the same message as for a
  genuine TableStatus=UPDATING.
  - ACK: deletable.when, requeue · ops: DeleteTable, DescribeTable · fields: SSEDescription.Status,
    WarmThroughput.Status, TableStatus
  - repro: UpdateTable(SSESpecification KMS) or UpdateTable(WarmThroughput increase); DescribeTable shows
    ACTIVE; DeleteTable
  - handling: handled via `pkg/resource/table/hooks.go:434-473; test/e2e/tests/test_table.py:630-673`
  - related: [DDB-TABLE-179](table-throughput-billing.md#ddb-table-179), [DDB-TABLE-066](table-throughput-billing.md#ddb-table-066), [DDB-TABLE-459](table-throughput-billing.md#ddb-table-459), [DDB-TABLE-049](table-throughput-billing.md#ddb-table-049), [DDB-TABLE-061](table-throughput-billing.md#ddb-table-061), [DDB-TABLE-059](table-throughput-billing.md#ddb-table-059),
    [DDB-TABLE-121](table-throughput-billing.md#ddb-table-121), [DDB-TABLE-120](#ddb-table-120), [DDB-TABLE-065](#ddb-table-065), [DDB-TABLE-002](#ddb-table-002), [DDB-TABLE-284](#ddb-table-284), [DDB-TABLE-369](#ddb-table-369), [DDB-TABLE-370](table-throughput-billing.md#ddb-table-370),
    [DDB-TABLE-001](table.md#ddb-table-001), [DDB-TABLE-334](#ddb-table-334) · evidence: table/state-machine/billing-sse-warm-throughput
  - notes: Finalizer must treat ResourceInUseException as 'requeue' regardless of the TableStatus it just
    read.

- <a id="ddb-table-334"></a>**DDB-TABLE-334** `delete-semantics` · impact high · partially handled · verified 2026-10-09
  **DeleteTable on a table in INACCESSIBLE_ENCRYPTION_CREDENTIALS -> 200, TableStatus=DELETING, gone after 8 s**
  State before: {'status': 'INACCESSIBLE_ENCRYPTION_CREDENTIALS', 'sse_status': 'ENABLED', 'inaccessible_dt':
  '2026-10-09 00:55:39.143000+00:00', 'archival': None}. DeleteTable: {'ok': True, 'code': None,
  'http_status': 200, 'full_message': None}. Response TableStatus=DELETING, SSEDescription={'Status':
  'ENABLED', 'SSEType': 'KMS', 'KMSMasterKeyArn':
  'arn:aws:kms:us-west-2:<ACCOUNT>:key/74e12eb3-f7c5-41b7-a8c9-901ca9fdc45b',
  'InaccessibleEncryptionDateTime': '2026-10-09 00:55:39.143000+00:00'}. Removal timeline: [{'value':
  'DELETING', 'from_s': 0.01, 'to_s': 8.08, 'duration_s': 8.07}, {'value': 'ERR:ResourceNotFoundException',
  'from_s': 8.08, 'to_s': None, 'duration_s': None}]. The response echoes SSEDescription with
  InaccessibleEncryptionDateTime still present.
  - ACK: deletable.when, custom_delete · ops: DeleteTable, DescribeTable · fields: TableStatus
  - repro: CMK table, kms DisableKey, wait for INACCESSIBLE_ENCRYPTION_CREDENTIALS, DeleteTable, poll
    DescribeTable until ResourceNotFoundException
  - handling: partially handled via `pkg/resource/table/hooks.go:60-65; pkg/resource/table/hooks.go:203-208` - see Handling gaps
  - related: [DDB-TABLE-069](#ddb-table-069), [DDB-TABLE-120](#ddb-table-120), [DDB-TABLE-065](#ddb-table-065), [DDB-TABLE-066](table-throughput-billing.md#ddb-table-066), [DDB-TABLE-002](#ddb-table-002), [DDB-TABLE-284](#ddb-table-284),
    [DDB-TABLE-369](#ddb-table-369), [DDB-TABLE-370](table-throughput-billing.md#ddb-table-370), [DDB-TABLE-001](table.md#ddb-table-001), [DDB-TABLE-331](#ddb-table-331), [DDB-TABLE-330](#ddb-table-330), [DDB-TABLE-335](#ddb-table-335), [DDB-TABLE-332](#ddb-table-332) ·
    hypotheses: H-T-104 · evidence: table/state-machine/kms-inaccessible-lifecycle
  - notes: H-T-104 first half confirmed. The ARCHIVING/ARCHIVED halves are untested (7-day path).
  - full notes: [details/DDB-TABLE-334.md](details/DDB-TABLE-334.md)

- <a id="ddb-table-431"></a>**DDB-TABLE-431** `delete-semantics` · impact high · unhandled (not handled in controller) · verified 2026-10-09
  **Resource-policy Deny on dynamodb:DeleteTable blocks the owner's DeleteTable (AccessDenied, beats DP) and outlives its removal by ~226 s**
  PutResourcePolicy {Deny, Principal:'*', Action:dynamodb:DeleteTable} on an ACTIVE table (same account, Admin
  role): DeleteTable -> AccessDeniedException (HTTP 400) 'User: arn:aws:sts::<acct>:assumed-role/<PRINCIPAL>
  is not authorized to perform: dynamodb:DeleteTable on resource: <arn> with an explicit deny in a
  resource-based policy', enforced 0-2 s after Put. With DeletionProtectionEnabled=true AND the deny, the
  AccessDeniedException is returned (not the DP ValidationException); turning DP off (UpdateTable allowed)
  changes nothing. DescribeTable shows no hint (DeletionProtectionEnabled=false, TableStatus=ACTIVE).
  DeleteResourcePolicy within 15 s of the Put -> ThrottlingException 'Resource-based policy for table <name>
  modified within the previous 15000 milliseconds'; once removed (GetResourcePolicy 200/PolicyNotFound at
  once) the caller's DeleteTable kept failing with AccessDeniedException for 226.4 s (46 attempts at 5 s)
  before the first 200.
  - ACK: pre-delete-cleanup, terminal_codes, requeue · ops: DeleteTable, PutResourcePolicy,
    DeleteResourcePolicy, UpdateTable · fields: ResourcePolicy, DeletionProtectionEnabled
  - repro: CreateTable (DP=true); PutResourcePolicy Deny dynamodb:DeleteTable Principal *; DeleteTable ->
    AccessDenied; UpdateTable DP=false; DeleteTable -> AccessDenied; DeleteResourcePolicy; DeleteTable every 5
    s
  - measurements: deny_enforced_after_put_s=2.0, delete_allowed_after_policy_removal_s=226.4,
    delete_allowed_after_policy_removal_attempts=46
  - handling: not handled in the controller (as of commit 34b85e6)
  - related: [DDB-TABLE-324](table-policy-kinesis-autoscaling.md#ddb-table-324), [DDB-TABLE-270](table-policy-kinesis-autoscaling.md#ddb-table-270), [DDB-TABLE-016](#ddb-table-016), [DDB-TABLE-206](table-policy-kinesis-autoscaling.md#ddb-table-206), [DDB-TABLE-210](table-policy-kinesis-autoscaling.md#ddb-table-210), [DDB-TABLE-432](table-policy-kinesis-autoscaling.md#ddb-table-432) ·
    evidence: table/creative/policy-denies-finalizer
  - notes: A user-managed (or spec-managed) resourcePolicy is a second, invisible deletion protection: the
    finalizer's DeleteTable returns AccessDeniedException, which a controller should treat as
    terminal-with-message rather than requeue blindly. Recovery needs DeleteResourcePolicy (possible as long
    as the...
  - full notes: [details/DDB-TABLE-431.md](details/DDB-TABLE-431.md)

## Quotas and rate limits

- <a id="ddb-table-003"></a>**DDB-TABLE-003** `quota-limit` · impact high · unhandled (not handled in controller) · verified 2026-10-08
  **Flipping DeletionProtectionEnabled twice within 15s -> ThrottlingException (400) per-table cooldown with retry-after**
  UpdateTable(DeletionProtectionEnabled=false) issued 2.3s after a successful
  UpdateTable(DeletionProtectionEnabled=true) failed with ThrottlingException: 'Deletion protection setting
  for table ackq-65ed52-adm modified within the previous 15000 milliseconds. Please try again after
  2026-10-08T22:57:03.204Z'. The call succeeded after 11.1 s. This is a per-table cooldown, not an account
  rate limit, and it is returned even though the table was ACTIVE.
  - ACK: requeue, custom_update · ops: UpdateTable · fields: DeletionProtectionEnabled
  - repro: UpdateTable(DP=true); within 15s UpdateTable(DP=false)
  - measurements: cooldown_s=15, succeeded_after_s=11.1
  - handling: not handled in the controller (as of commit 34b85e6)
  - related: [DDB-TABLE-176](#ddb-table-176), [DDB-TABLE-017](#ddb-table-017), [DDB-TABLE-438](#ddb-table-438), [DDB-TABLE-053](service.md#ddb-table-053) · evidence:
    table/state-machine/admissibility-matrix
  - notes: A user flipping deletionProtectionEnabled back quickly (or a controller correcting drift) will see
    a throttle code that generic retry logic handles, but the message carries an explicit retry-after
    timestamp that could be used to requeue precisely.

- <a id="ddb-table-054"></a>**DDB-TABLE-054** `quota-limit` · impact high · unhandled (not handled in controller) · verified 2026-10-08
  **DeletionProtectionEnabled: synchronous (TableStatus stays ACTIVE) but a second toggle within 15s fails with ThrottlingException**
  UpdateTable(DeletionProtectionEnabled=true) returned 200 with TableStatus=ACTIVE and DescribeTable
  immediately showed true (no UPDATING). Re-sending true, and sending false, within 15s both failed with
  ThrottlingException (HTTP 400): 'Deletion protection setting for table <name> modified within the previous
  15000 milliseconds. Please try again after <ISO timestamp>'. The toggle was also accepted while the table
  was UPDATING for a TableClass change.
  - ACK: requeue, terminal_codes, updateable.when · ops: UpdateTable · fields: DeletionProtectionEnabled
  - repro: UpdateTable DeletionProtectionEnabled=true; immediately UpdateTable DeletionProtectionEnabled=false
  - measurements: cooldown_s=15
  - handling: not handled in the controller (as of commit 34b85e6)
  - related: [DDB-TABLE-001](table.md#ddb-table-001), [DDB-TABLE-002](#ddb-table-002), [DDB-TABLE-370](table-throughput-billing.md#ddb-table-370), [DDB-TABLE-369](#ddb-table-369), [DDB-TABLE-052](#ddb-table-052), [DDB-TABLE-285](#ddb-table-285),
    [DDB-TABLE-120](#ddb-table-120), [DDB-TABLE-065](#ddb-table-065), [DDB-TABLE-459](table-throughput-billing.md#ddb-table-459), [DDB-TABLE-010](table.md#ddb-table-010) · evidence:
    table/mutation-matrix/stream-protection-throughput
  - notes: Characterised further (retry timing) in table/response-fidelity/create-update-response.

- <a id="ddb-table-081"></a>**DDB-TABLE-081** `quota-limit` · impact high · tracked in GitHub issue (not handled) · verified 2026-10-08
  **SSE/KMS changes are quota-limited per table: 4 per 24h window, then one per 6h; excess fails with LimitExceededException (HTTP 400)**
  After four SSESpecification changes on one table within minutes (three no-op re-sends of the same key by
  alias/key-id plus one Enabled:false), every further UpdateTable with SSESpecification failed with
  LimitExceededException (HTTP 400): 'Subscriber limit exceeded: Encryption mode changes are limited in the
  24h window ending at 2026-10-09T23:13:00.402Z. After the first 4 change, each subsequent change in the same
  window can be performed at most once every 21600 seconds. Number of updates today: 4. Last change at <ts>'.
  The window started at the first change (table creation time + ~0), not at midnight. Requests rejected by
  this quota include ones that would otherwise be ValidationException (bad keys), so the quota check runs
  before key validation.
  - ACK: requeue, terminal_codes, custom_update, compare.is_ignored+delta_pre_compare · ops: UpdateTable ·
    fields: SSESpecification, SSESpecification.KMSMasterKeyId
  - repro: CMK table; UpdateTable SSESpecification with the same key as alias x3 (each triggers a change) +
    Enabled:false; then any further SSESpecification update
  - measurements: changes_before_limit=4, window_h=24, subsequent_min_interval_s=21600
  - handling: tracked in https://github.com/aws-controllers-k8s/community/issues/2136 (not handled) · code refs: `generator.yaml:73-77; pkg/resource/table/hooks.go:603-611; pkg/resource/table/hooks.go:583-619`
  - related: [DDB-TABLE-142](#ddb-table-142), [DDB-TABLE-082](#ddb-table-082), [DDB-TABLE-078](#ddb-table-078), [DDB-TABLE-141](#ddb-table-141), [DDB-TABLE-140](#ddb-table-140), [DDB-TABLE-371](#ddb-table-371),
    [DDB-TABLE-065](#ddb-table-065), [DDB-TABLE-079](#ddb-table-079), [DDB-TABLE-120](#ddb-table-120) · evidence: table/mutation-matrix/sse-kms
  - notes: A controller that re-sends KMSMasterKeyId as an alias or key id on each reconcile burns the quota
    in four reconciles and is then locked out for up to 24h.
  - full notes: [details/DDB-TABLE-081.md](details/DDB-TABLE-081.md)

- <a id="ddb-table-283"></a>**DDB-TABLE-283** `quota-limit` · impact high · handled · verified 2026-10-09
  **TableClass changes are limited to 2 per 30 days per table (3rd -> LimitExceededException); no-op re-sends also rejected once spent**
  Fresh PPR table: change 1 (STD->IA) OK, change 2 (IA->STD) OK, change 3 -> LimitExceededException HTTP 400
  'Subscriber limit exceeded: Updates to TableClass are limited to 2 times in 30 day(s).' (table 'clean');
  same on table 'ops' (LimitExceededException). No-op re-send of the current class BEFORE the quota is spent:
  OK (TableStatus in response UPDATING; it did not count: the 2nd real change afterwards was OK). No-op
  re-send AFTER the quota is spent: LimitExceededException 'Subscriber limit exceeded: Updates to TableClass
  are limited to 2 times in 30 day(s).'. Attempt 1 of this probe (evidence.attempt1.jsonl) got 5 TableClass
  changes accepted within 14 s on one table when a DeletionProtection update was interleaved - see the race
  finding.
  - ACK: terminal_codes, requeue, custom_update, compare.is_ignored+delta_pre_compare · ops: UpdateTable ·
    fields: TableClass, TableClassSummary
  - repro: PPR table; UpdateTable TableClass=STANDARD_INFREQUENT_ACCESS; wait; TableClass=STANDARD; wait;
    TableClass=STANDARD_INFREQUENT_ACCESS -> LimitExceededException
  - handling: handled via `pkg/resource/table/hooks.go:421-425; test/e2e/tests/test_table.py:675-714`
  - related: [DDB-TABLE-177](#ddb-table-177), [DDB-TABLE-019](#ddb-table-019), [DDB-TABLE-156](table-throughput-billing.md#ddb-table-156), [DDB-TABLE-057](table-throughput-billing.md#ddb-table-057), [DDB-TABLE-018](#ddb-table-018), [DDB-TABLE-365](#ddb-table-365),
    [DDB-TABLE-183](table-throughput-billing.md#ddb-table-183), [DDB-TABLE-358](table-throughput-billing.md#ddb-table-358), [DDB-TABLE-052](#ddb-table-052), [DDB-TABLE-284](#ddb-table-284), [DDB-TABLE-285](#ddb-table-285), [DDB-TABLE-451](#ddb-table-451), [DDB-TABLE-287](#ddb-table-287),
    [DDB-TABLE-180](#ddb-table-180), [DDB-TABLE-120](#ddb-table-120) · evidence: table/state-machine/table-class-switch
  - notes: A controller that flip-flops TableClass (or a user reverting a change) locks the field for a month;
    LimitExceededException here is terminal for ~30 days, not a transient throttle. Wave-1 noted no-op
    TableClass re-sends return TableStatus=UPDATING; whether they consume the quota is answered above.

## Handling gaps (bugs to file)

- [DDB-TABLE-050](#ddb-table-050) - Stream view type cannot be changed in place; disable requires no StreamViewType;
  LatestStreamArn survives disable (suspected bug)
  Suspected controller bug confirmed by evidence: AWS side of the suspicion verified: after disabling,
  DescribeTable omits StreamSpecification entirely (observed nil) and a repeated StreamEnabled=false is
  rejected with ValidationException 'Table has no stream to disable' (050); never-enabled tables also report
  no StreamSpecification (029). A spec with streamEnabled=false therefore diffs against nil and each re-send
  is a terminal ValidationException - a pre-compare normalization (nil observed == {StreamEnabled:false}) is
  needed.
  - handling_ref: `pkg/resource/table/hooks.go:338-432; generator.yaml:91-92; test/e2e/table.py:88-100;
    pkg/resource/table/sdk.go:369-380; pkg/resource/table/hooks.go:401-420; b0b0d59`
- [DDB-TABLE-082](#ddb-table-082) - Re-sending the current KMS key by ARN is a ValidationException no-op; by alias or key id it
  triggers a ~22s re-encryption (suspected bug)
  Suspected controller bug confirmed by evidence: Re-sending the current key by alias or key id is accepted
  and triggers a ~22 s re-encryption that counts toward the 4-per-24h SSE quota; only the ARN form is a
  rejected no-op (082). Same on AWS-managed-key tables: re-sending
  {Enabled:true[,SSEType:KMS][,alias/aws/dynamodb]} re-encrypts and burns quota (141); after 4 such re-sends
  every SSE change is LimitExceededException for 6 h (081, 142). A perpetual alias/ID delta escalates to a
  quota lockout within 4 reconciles, exactly as suspected. The same hazard applies to a spec with enabled:true
  and no key if the compare includes the observed KMSMasterKeyArn-derived kmsMasterKeyID.
  - handling_ref: `pkg/resource/table/hooks.go:338-432; generator.yaml:91-92; generator.yaml:73-77;
    pkg/resource/table/hooks.go:603-611`
- [DDB-TABLE-330](#ddb-table-330) - After EnableKey the data plane works within <7 min but TableStatus stays
  INACCESSIBLE_ENCRYPTION_CREDENTIALS for 57 min (partially handled)
  H-T-105 recovery half: TableStatus/InaccessibleEncryptionDateTime are refreshed by a slow periodic check
  (12-80 min observed for both directions in this session), not by data-plane or control-plane activity; a
  controller should treat INACCESSIBLE_ENCRYPTION_CREDENTIALS as 'requeue with a long backoff' and must not
  infer anything about the key from a successful GetItem. H-T-102: a table can be fully readable while still
  flagged.
  - handling_ref: `pkg/resource/table/hooks.go:60-65; pkg/resource/table/hooks.go:203-208`
- [DDB-TABLE-331](#ddb-table-331) - CMK disabled -> INACCESSIBLE_ENCRYPTION_CREDENTIALS after 13-43 min, pending-deletion key
  after 75 min; data plane fails after ~5 min (partially handled)
  H-T-101 first half: confirmed for the status/InaccessibleEncryptionDateTime shape, but the '<30 min' bound
  is refuted (13-75 min, apparently a slow per-table periodic check). H-T-102: the only reliable signal that
  the key is unusable is the data plane (fails within ~5 min) - TableStatus lags by up to 70 min and
  SSEDescription.Status never leaves ENABLED. H-T-105 pending-deletion variant: behaves like a disabled key
  but was the slowest to be detected.
  - handling_ref: `pkg/resource/table/hooks.go:60-65; pkg/resource/table/hooks.go:203-208`
- [DDB-TABLE-332](#ddb-table-332) - CMK disabled while CREATING: table stuck CREATING ~59 min then silently vanishes; revoked
  grants -> INACCESSIBLE in 43 min, repairable (partially handled)
  H-T-148: refuted in detail - the table neither reaches ACTIVE nor INACCESSIBLE; it is garbage-collected
  after ~1 h, so a controller waiting for ACTIVE after CreateTable must also exit on ResourceNotFoundException
  (the table it created is gone) and report the KMS cause. H-T-143: confirmed that revoking the grants flips
  the table within ~45 min; its contrarian 'never recoverable' branch is refuted: because the old key itself
  is still usable, UpdateTable to another CMK re-encrypts and heals the table in 30 s (contrast: with the old
  key DISABLED the same call is ValidationException 'KMS key disabled error', see the op-matrix finding).
  - handling_ref: `pkg/resource/table/hooks.go:60-65; pkg/resource/table/hooks.go:203-208`
- [DDB-TABLE-334](#ddb-table-334) - DeleteTable on a table in INACCESSIBLE_ENCRYPTION_CREDENTIALS -> 200, TableStatus=DELETING,
  gone after 8 s (partially handled)
  H-T-104 first half confirmed. The ARCHIVING/ARCHIVED halves are untested (7-day path).
  - handling_ref: `pkg/resource/table/hooks.go:60-65; pkg/resource/table/hooks.go:203-208`
- [DDB-TABLE-335](#ddb-table-335) - Re-enabling the CMK: ACTIVE again after 18 / 44 / 57 min (3 tables),
  InaccessibleEncryptionDateTime cleared; CancelKeyDeletion alone no help (partially handled)
  H-T-105 recovery half confirmed except for the '<30 min' bound: 18-57 min observed, i.e. the same slow
  periodic check as detection. A controller can only requeue; InaccessibleEncryptionDateTime must not be
  persisted as a permanent field. The 'archival clock restarts' half is untested.
  - handling_ref: `pkg/resource/table/hooks.go:60-65; pkg/resource/table/hooks.go:203-208`

## E2E timing

Values are seconds unless the key says otherwise; n = trials behind the numbers ('1 run' when the finding records none).

| finding | what | measurements | n |
| --- | --- | --- | --- |
| [DDB-TABLE-002](#ddb-table-002) | While UPDATING (stream enable), DeleteTable=ResourceInUseException/400 but UpdateTable(DeletionProtectionEnabled)=OK(200); UPDATING ~4.71s | updating_duration_s=4.71 | 1 run |
| [DDB-TABLE-003](#ddb-table-003) | Flipping DeletionProtectionEnabled twice within 15s -> ThrottlingException (400) per-table cooldown with retry-after | cooldown_s=15, succeeded_after_s=11.1 | 1 run |
| [DDB-TABLE-018](#ddb-table-018) | No-op UpdateTable re-sends are per-field: DP/BillingMode/TableClass 200; Stream and SSE Enabled:false re-sends ValidationException | stream_enable_updating_s=4.05, stream_disable_updating_s=5.07, tableclass_updating_s=6.08 | 1 run |
| [DDB-TABLE-022](#ddb-table-022) | Asymmetric (RSA_2048) KMSMasterKeyId makes CreateTable/UpdateTable fail with InternalServerError HTTP 500 (looks transient, is permanent) | create_latency_ms=1849, update_latency_ms=1731 | 1 run |
| [DDB-TABLE-050](#ddb-table-050) | Stream view type cannot be changed in place; disable requires no StreamViewType; LatestStreamArn survives disable | stream_enable_updating_s=4.05, stream_disable_updating_s=5.07, stream_reenable_updating_s=5.06 | 1 run |
| [DDB-TABLE-052](#ddb-table-052) | TableClass STANDARD -> STANDARD_INFREQUENT_ACCESS: re-send while UPDATING -> ResourceInUse, DP admitted; 31.4 s is a polling upper bound | tableclass_updating_s=31.4 | 1 run |
| [DDB-TABLE-054](#ddb-table-054) | DeletionProtectionEnabled: synchronous (TableStatus stays ACTIVE) but a second toggle within 15s fails with ThrottlingException | cooldown_s=15 | 1 run |
| [DDB-TABLE-078](#ddb-table-078) | KMS alias resolved once at Create/UpdateTable: repointing the alias does not move the table; re-sending the alias migrates it | alias_drift_watch_s=150, resend_alias_settle_s=22.27 | 1 run |
| [DDB-TABLE-079](#ddb-table-079) | SSE CMK -> disabled: SSEDescription disappears after ~22s (TableStatus stays ACTIVE); later changes hit the 4-per-24h quota | disable_sse_updating_s=22.26 | 1 run |
| [DDB-TABLE-081](#ddb-table-081) | SSE/KMS changes are quota-limited per table: 4 per 24h window, then one per 6h; excess fails with LimitExceededException (HTTP 400) | changes_before_limit=4, window_h=24, subsequent_min_interval_s=21600 | 1 run |
| [DDB-TABLE-082](#ddb-table-082) | Re-sending the current KMS key by ARN is a ValidationException no-op; by alias or key id it triggers a ~22s re-encryption | resend_alias_sse_updating_s=22.27, resend_keyid_sse_updating_s=21.26 | 1 run |
| [DDB-TABLE-140](#ddb-table-140) | SSE transitions CMK->CMK, CMK->AWS managed, AWS managed->off->on, off->CMK: phases, durations and which re-sends are no-ops | create-t1=8.12, create-t2=0.01, create-t3=0.01, t1-k1-to-k2-arn=22.31, t1-k2-to-aws-managed=21.29, t1-aws-managed-resend=21.31, t1-aws-alias-explicit=21.31, t1-disable=21.3, t2-resend-enabled-true=21.31, t2-enabled-true-type-kms=21.31, t2-aws-alias-explicit=21.32, t2-disable=21.3, t3-enable-aws-managed=22.32, t3-disable=22.32, t3-cmk-k1=22.33, t3-disable-2=23.35 | 1 run |
| [DDB-TABLE-180](#ddb-table-180) | UpdateTable response and DescribeTable show requested StreamSpecification/LatestStreamArn during UPDATING; TableClassSummary only after | stream_enable_updating_s=5.13, tableclass_updating_s=6.07 | 1 run |
| [DDB-TABLE-213](#ddb-table-213) | The stream ARN is an independent policy slot (own RevisionId, own 15s window); Get(table ARN) ignores it -> PolicyNotFoundException | stream_second_write_first_success_s=15.56, stream_delete_attempts=15 | 1 run |
| [DDB-TABLE-284](#ddb-table-284) | TableClass switch: UPDATING ~3.6-4.1 s; TableClassSummary flips with ACTIVE; UpdateTable echoes the OLD summary; Delete refused until ACTIVE | switch_std_to_ia_updating_s=4.09, switch_ia_to_std_updating_s=3.58, switch_with_sse_interleaved_class_flipped_s=3.59, delete_admitted_after_s=5.19, delete_gone_after_s=5.07 | 1 run |
| [DDB-TABLE-330](#ddb-table-330) | After EnableKey the data plane works within <7 min but TableStatus stays INACCESSIBLE_ENCRYPTION_CREDENTIALS for 57 min | status_active_after_enable_s=3410, first_observed_get_item_ok_after_enable_s=407 | 1 run |
| [DDB-TABLE-331](#ddb-table-331) | CMK disabled -> INACCESSIBLE_ENCRYPTION_CREDENTIALS after 13-43 min, pending-deletion key after 75 min; data plane fails after ~5 min | detect_quiet_s=754.6, detect_traf_s=2504.5, detect_del_s=2172.7, detect_grants_s=2595.0, detect_pend_s=4504, get_item_first_failure_s=332.2 | 1 run |
| [DDB-TABLE-332](#ddb-table-332) | CMK disabled while CREATING: table stuck CREATING ~59 min then silently vanishes; revoked grants -> INACCESSIBLE in 43 min, repairable | creating_vanished_after_s=3560.3, grants_detect_s=2595.0, grants_recover_via_sse_switch_s=30.4 | 1 run |
| [DDB-TABLE-335](#ddb-table-335) | Re-enabling the CMK: ACTIVE again after 18 / 44 / 57 min (3 tables), InaccessibleEncryptionDateTime cleared; CancelKeyDeletion alone no help | recover_quiet_s=1087.7, recover_traf_s=3410, recover_pend_after_enable_s=2654.2, pend_cancel_only_no_recovery_observed_min=45.3 | 1 run |
| [DDB-TABLE-361](#ddb-table-361) | UpdateTable response-echo matrix: SSE change answers with a Status-only stub, OnDemandThroughput is synchronous (new values) | sse_enable_stub_s=9.1, sse_enable_total_s=22.3, sse_disable_old_key_echo_s=8.1, sse_disable_stub_s=13.2, sse_disable_total_s=21.3, odt_updating_window_s=0 | 1 run |
| [DDB-TABLE-362](#ddb-table-362) | Re-enabling a stream mints a NEW LatestStreamArn/Label; the response already carries it and the old ARN is gone from DescribeTable | enable_updating_s=3.0, disable_updating_s=4.1, reenable_updating_s=4.1 | 1 run |
| [DDB-TABLE-365](#ddb-table-365) | TableClass=STANDARD re-send on a table that never set TableClass is a free no-op: no TableClassSummary appears, no 30-day budget spent | ia_updating_s=6.1, standard_updating_s=4.0 | 1 run |
| [DDB-TABLE-369](#ddb-table-369) | Billing-mode switch UPDATING: PT/OnDemand/stream/Delete -> ResourceInUse, DP and Warm admitted; reverse switch 200 but lost (71.9 s / 2.0 s) | fresh_ppr_to_provisioned_updating_s=71.9, fresh_provisioned_to_ppr_updating_s=2.0, reflip_ppr_to_provisioned_updating_s=2.0, reflip_provisioned_to_ppr_updating_s=0 | 1 run |
| [DDB-TABLE-371](#ddb-table-371) | KMS key change echoes the OLD KMSMasterKeyArn with Status=UPDATING for ~8-9 s in the UpdateTable response and DescribeTable | to_cmk_old_key_echo_s=9.1, to_cmk_new_key_updating_s=14.2, to_cmk_total_s=23.3, to_aws_managed_old_key_echo_s=8.1, to_aws_managed_total_s=22.3 | 1 run |
| [DDB-TABLE-378](#ddb-table-378) | Each stream enable mints a new LatestStreamArn; ListStreams(TableName) still lists DISABLED streams of flaps and deleted incarnations | streams_listed_after_recreate=2, streams_listed_after_one_flap=3 | 1 run |
| [DDB-TABLE-382](#ddb-table-382) | UpdateTable is single-concern: 33/35 pairs of PT/ODT, Stream, DP, SSE, TableClass, Warm rejected on an idle table; only BillingMode combines | pairs_tested=35, pairs_rejected=33, switch_to_ppr_plus_stream_updating_s=147.5 | 1 run |
| [DDB-TABLE-383](#ddb-table-383) | No-op UpdateTable re-sends are free: 20 same-value BillingMode/DP/TableClass calls in 0.4 s -> all 200, no throttle/UPDATING/quota/cooldown | noop_updatetable_burst_calls=20, noop_updatetable_burst_span_s=0.4, noop_updatetable_throttled=0, pitr_burst_calls=20, pitr_burst_span_s=0.2, pitr_burst_throttled=9 | 1 run |
| [DDB-TABLE-386](#ddb-table-386) | Doc claim C010 PARTLY: after DeleteTable the stream is DISABLING at once, DISABLED at +6.09 s, still readable; 24 h deletion unverifiable | stream_disabled_after_delete_s=6.09, oldest_orphan_stream_h=6.0 | 1 run |
| [DDB-TABLE-431](#ddb-table-431) | Resource-policy Deny on dynamodb:DeleteTable blocks the owner's DeleteTable (AccessDenied, beats DP) and outlives its removal by ~226 s | deny_enforced_after_put_s=2.0, delete_allowed_after_policy_removal_s=226.4, delete_allowed_after_policy_removal_attempts=46 | 1 run |
| [DDB-TABLE-433](#ddb-table-433) | UpdateTable is one-logical-change-per-call: DP, SSE, TableClass, WarmThroughput each 'must be the only operation'; most pairs rejected | combinations_tested=33, accepted=2, ppr_plus_odt_updating_s=42.3, ppr_plus_stream_updating_s=16.2, billing_to_provisioned_alone_updating_s=60.5 | 1 run |
| [DDB-TABLE-435](#ddb-table-435) | Fan-out of UpdateTable(stream)+TTL+PITR+Tag+Policy+Insights on ONE table at the same instant: no per-table conflict, all settings land | burst_span_s=0.12, ok=5, throttled=1 | 1 run |
| [DDB-TABLE-450](#ddb-table-450) | Clobber matrix (3 jobs x 8 writes): only the SSE write resets TableStatus, only in a TableClass switch; Warm/DP/tag/TTL/PITR/policy never do | cells=27, premature_active_cells=1, tc_updating_s_control=46, st_updating_s=[4.23, 3.97, 4.52, 4.5, 5.31, 3.69, 3.73, 4.51, 3.16], bl_first_active_s=[61.43, 86.73, 107.98, 64.47, 90.76, 108.05, 115.09, 160.63, 118.1] | 1 run |

## Open questions

- [DDB-TABLE-336](#ddb-table-336) (unverified) - UNTESTED: 7-day archival path (ARCHIVING/ARCHIVED, ArchivalSummary, system
  backup) after INACCESSIBLE_ENCRYPTION_CREDENTIALS: Untested in this session (budget).

<!-- preserved:start id=open-questions -->
<!-- open questions and follow-up experiments; survives re-renders -->
<!-- preserved:end -->

## Appendix: low-impact and duplicate findings

| id | category | impact | status | title | related | duplicate_of |
| --- | --- | --- | --- | --- | --- | --- |
| <a id="ddb-table-004"></a>**DDB-TABLE-004** | async-state-machine | medium | confirmed | UpdateTable(StreamSpecification enable) response: TableStatus=UPDATING, LatestStreamArn present=True; UPDATING ~4.71s | [DDB-TABLE-050](#ddb-table-050), [DDB-TABLE-362](#ddb-table-362), [DDB-TABLE-367](#ddb-table-367), [DDB-TABLE-378](#ddb-table-378), [DDB-TABLE-180](#ddb-table-180), [DDB-TABLE-373](service.md#ddb-table-373), [DDB-TABLE-013](service.md#ddb-table-013), [DDB-TABLE-071](service.md#ddb-table-071), [DDB-TABLE-213](#ddb-table-213) | [DDB-TABLE-180](#ddb-table-180) |
| <a id="ddb-table-019"></a>**DDB-TABLE-019** | stale-response | low | confirmed | No-op UpdateTable(BillingMode/TableClass same value) returns TableStatus=UPDATING in the response but DescribeTable is ACTIVE at once | [DDB-TABLE-177](#ddb-table-177), [DDB-TABLE-156](table-throughput-billing.md#ddb-table-156), [DDB-TABLE-057](table-throughput-billing.md#ddb-table-057), [DDB-TABLE-018](#ddb-table-018), [DDB-TABLE-365](#ddb-table-365), [DDB-TABLE-183](table-throughput-billing.md#ddb-table-183), [DDB-TABLE-358](table-throughput-billing.md#ddb-table-358), [DDB-TABLE-283](#ddb-table-283), [DDB-TABLE-026](#ddb-table-026), [DDB-TABLE-284](#ddb-table-284), [DDB-TABLE-180](#ddb-table-180), [DDB-TABLE-452](table-throughput-billing.md#ddb-table-452) | [DDB-TABLE-177](#ddb-table-177) |
| <a id="ddb-table-030"></a>**DDB-TABLE-030** | server-default | low | confirmed | DeletionProtectionEnabled is always present in DescribeTable for every create shape | [DDB-TABLE-016](#ddb-table-016), [DDB-TABLE-034](#ddb-table-034), [DDB-TABLE-438](#ddb-table-438) | - |
| <a id="ddb-table-034"></a>**DDB-TABLE-034** | delete-semantics | medium | confirmed | DeleteTable on a table with DeletionProtectionEnabled=true fails with ValidationException | [DDB-TABLE-016](#ddb-table-016), [DDB-TABLE-438](#ddb-table-438), [DDB-TABLE-030](#ddb-table-030) | [DDB-TABLE-016](#ddb-table-016) |
| <a id="ddb-table-080"></a>**DDB-TABLE-080** | error-code | high | confirmed | CreateTable with disabled/pending-deletion KMS key -> ValidationException; asymmetric key -> HTTP 500; no table left behind | [DDB-TABLE-020](#ddb-table-020), [DDB-TABLE-021](#ddb-table-021), [DDB-TABLE-139](#ddb-table-139), [DDB-TABLE-022](#ddb-table-022), [DDB-TABLE-023](#ddb-table-023), [DDB-TABLE-037](#ddb-table-037) | [DDB-TABLE-020](#ddb-table-020) |
| <a id="ddb-table-142"></a>**DDB-TABLE-142** | quota-limit | high | confirmed | SSE change quota from a clean count: the 5th SSESpecification change within 24h on one table -> LimitExceededException | [DDB-TABLE-082](#ddb-table-082), [DDB-TABLE-078](#ddb-table-078), [DDB-TABLE-141](#ddb-table-141), [DDB-TABLE-140](#ddb-table-140), [DDB-TABLE-371](#ddb-table-371), [DDB-TABLE-065](#ddb-table-065), [DDB-TABLE-079](#ddb-table-079), [DDB-TABLE-081](#ddb-table-081), [DDB-TABLE-120](#ddb-table-120) | [DDB-TABLE-081](#ddb-table-081) |
| <a id="ddb-table-176"></a>**DDB-TABLE-176** | quota-limit | high | confirmed | DeletionProtectionEnabled toggles are synchronous but rate-limited: a second toggle within 15s fails with ThrottlingException (HTTP 400) | [DDB-TABLE-003](#ddb-table-003), [DDB-TABLE-017](#ddb-table-017), [DDB-TABLE-438](#ddb-table-438), [DDB-TABLE-053](service.md#ddb-table-053) | [DDB-TABLE-003](#ddb-table-003) |
| <a id="ddb-table-365"></a>**DDB-TABLE-365** | quota-limit | low | confirmed | TableClass=STANDARD re-send on a table that never set TableClass is a free no-op: no TableClassSummary appears, no 30-day budget spent | [DDB-TABLE-177](#ddb-table-177), [DDB-TABLE-283](#ddb-table-283), [DDB-TABLE-026](#ddb-table-026), [DDB-TABLE-284](#ddb-table-284), [DDB-TABLE-019](#ddb-table-019), [DDB-TABLE-156](table-throughput-billing.md#ddb-table-156), [DDB-TABLE-057](table-throughput-billing.md#ddb-table-057), [DDB-TABLE-018](#ddb-table-018), [DDB-TABLE-183](table-throughput-billing.md#ddb-table-183), [DDB-TABLE-358](table-throughput-billing.md#ddb-table-358), [DDB-TABLE-180](#ddb-table-180), [DDB-TABLE-052](#ddb-table-052), [DDB-TABLE-285](#ddb-table-285), [DDB-TABLE-451](#ddb-table-451), [DDB-TABLE-287](#ddb-table-287), [DDB-TABLE-120](#ddb-table-120) | - |
| <a id="ddb-table-367"></a>**DDB-TABLE-367** | identity | low | confirmed | Disabled streams linger: ListStreams(TableName) lists every past stream and DescribeStream serves them (DISABLED) even after DeleteTable | [DDB-TABLE-050](#ddb-table-050), [DDB-TABLE-362](#ddb-table-362), [DDB-TABLE-378](#ddb-table-378), [DDB-TABLE-180](#ddb-table-180), [DDB-TABLE-004](#ddb-table-004), [DDB-TABLE-373](service.md#ddb-table-373), [DDB-TABLE-013](service.md#ddb-table-013), [DDB-TABLE-071](service.md#ddb-table-071), [DDB-TABLE-213](#ddb-table-213) | - |
| <a id="ddb-table-386"></a>**DDB-TABLE-386** | delete-semantics | low | confirmed | Doc claim C010 PARTLY: after DeleteTable the stream is DISABLING at once, DISABLED at +6.09 s, still readable; 24 h deletion unverifiable | [DDB-TABLE-050](#ddb-table-050) | - |
| <a id="ddb-table-438"></a>**DDB-TABLE-438** | delete-semantics | low | confirmed | DeleteTable admitted in the same second as UpdateTable(DeletionProtectionEnabled=false); the 15 s DP cooldown gates only further DP toggles | [DDB-TABLE-003](#ddb-table-003), [DDB-TABLE-016](#ddb-table-016), [DDB-TABLE-176](#ddb-table-176), [DDB-TABLE-017](#ddb-table-017), [DDB-TABLE-053](service.md#ddb-table-053), [DDB-TABLE-034](#ddb-table-034), [DDB-TABLE-030](#ddb-table-030) | - |

## Supplementary notes

<!-- preserved:start -->
<!-- preserved:end -->
