<!-- generated: ack-api-quirks dynamodb 2026-10-09 model=2012-08-10 controller=34b85e6 -->
# Table (lifecycle, identity, errors, tags, limits)
_State machine, identity and idempotency, error taxonomy, delete semantics, tags, account limits; the catch-all for Table findings not covered by the other Table documents._
Generated from ack-api-quirks `services/dynamodb` (render date in the marker above); model 2012-08-10 (service/dynamodb v1.39.8); controller commit 34b85e6; evidence: `services/dynamodb/probes/<probe id>/` in the lab repo.

## Overview

<!-- preserved:start id=overview -->
This document is the Table catch-all: the lifecycle, identity and tag facts not claimed by the other Table documents - the CREATING/DELETING windows as DescribeTable, ListTables and the tag APIs see them, DeleteTable idempotency, re-create identity, ListTables/ListTagsOfResource pagination and tag write admission. The most surprising facts are that the tagging backend forgets a table from +1.6 s into DELETING, ~4.4 s before DescribeTable 404s at +6.0 s, that a deleted name can be re-created the instant DescribeTable first returns 404 (same ARN, new TableId), and that the ~1.5 s tag write lock admits a replay or a superset but rejects every other write ([DDB-TABLE-104](#ddb-table-104), [DDB-TABLE-009](#ddb-table-009), [DDB-TABLE-440](#ddb-table-440)).

### Rules a reconciler must respect
- CreateTable is asynchronous (CREATING -> ACTIVE in 2-6 s for a no-index PAY_PER_REQUEST table); DescribeTable never returned ResourceNotFoundException after the create response (0/10 at 100 ms, first 200 within 7-83 ms) and ListTables lists the name within 58-93 ms - no grace period is needed, a 404-as-not-yet guard is only cheap insurance; while CREATING, UpdateTable and DeleteTable are ResourceInUseException ([DDB-TABLE-387](#ddb-table-387), [DDB-TABLE-394](#ddb-table-394), [DDB-TABLE-076](#ddb-table-076), [DDB-TABLE-001](#ddb-table-001)).
- DeleteTable returns 200 with TableStatus=DELETING; a second DeleteTable while DELETING is ResourceInUseException 'Table is being deleted' and after the table is gone ResourceNotFoundException - both mean progress, not failure; UpdateTable in that window is ResourceInUseException too, and a duplicate CreateTable is ResourceInUseException in every state ('Table already exists' except while CREATING), regardless of parameters ([DDB-TABLE-006](#ddb-table-006), [DDB-TABLE-008](#ddb-table-008), [DDB-TABLE-005](#ddb-table-005), [DDB-TABLE-010](#ddb-table-010)).
- DescribeTable and ListTables converge: ListTables drops a deleted name between 0.79 s before and 0.44 s after the first DescribeTable 404 and neither read path flaps afterwards; a DELETING table is still listed; the 404 is terminal ([DDB-TABLE-076](#ddb-table-076), [DDB-TABLE-012](#ddb-table-012)).
- Re-create: CreateTable with the same name is accepted ~40 ms after the first 404 (5/5, no ResourceInUseException) and the new table is ACTIVE in ~6 s; TableArn is identical while TableId changes, so the ARN alone cannot detect a delete+recreate behind the controller's back - the controller does not compare TableId ([DDB-TABLE-009](#ddb-table-009), [DDB-TABLE-077](#ddb-table-077)).
- Identifier inputs: DescribeTable accepts the full table ARN as TableName (a missing table is RNF naming the extracted name) but the ListTables cursor does not (regex ValidationException); names are case-sensitive; ListTables orders byte-wise (upper before lower), omits LastEvaluatedTableName exactly on the last item, accepts a deleted or never-existing ExclusiveStartTableName and caps Limit at 100 ([DDB-TABLE-072](#ddb-table-072), [DDB-TABLE-012](#ddb-table-012), [DDB-TABLE-379](#ddb-table-379), [DDB-TABLE-400](#ddb-table-400)).
- Tags across the lifecycle: tags given to CreateTable are visible the moment the table is ACTIVE, while an invalid or >50-tag set (aws: prefix, 129-char key, '#', duplicates) fails the whole CreateTable with ValidationException and creates nothing (Tags=[] is accepted); tag ops are admitted while UPDATING; after DeleteTable TagResource is ResourceInUseException at +0.9 s and all tag APIs are ResourceNotFoundException from +1.6 s while DescribeTable still says DELETING until +6.0 s ([DDB-TABLE-110](#ddb-table-110), [DDB-TABLE-104](#ddb-table-104)).
- The controller's hooks catalog records that customUpdateTable syncs tags and the resource policy before the UPDATING/terminal-status checks (pkg/resource/table/hooks.go:175-196; [GT-DDB-058](service.md#gt-ddb-058) (controller hooks catalog entry)): right for UPDATING, where tag ops are admitted, but a tag-API ResourceNotFoundException during DELETING precedes the DescribeTable 404 by ~4.4 s and must not be read as 'table gone' - a finalizer should stop tag reconciliation once deletion starts ([DDB-TABLE-104](#ddb-table-104)).
- Tag write admission: an effective TagResource/UntagResource holds a ~1.5 s per-table lock during which only a TagResource that contains the in-flight pairs (identical replay or full superset) passes; any other write, including UntagResource of an absent key, is LimitExceededException 'Table tags are being updated' until ~1.52 s - write the complete desired set in ONE TagResource and never follow it with an UntagResource in the same reconcile ([DDB-TABLE-440](#ddb-table-440)).
- No-op tag writes are free: identical TagResource and UntagResource of absent keys never take the lock or the account limiter (20 sequential in 0.26 s and 10 concurrent all 200) while 10 concurrent effective writes hit the account ThrottlingException 3/10 - a level-triggered re-send of the identical set is harmless, real changes must be batched ([DDB-TABLE-441](#ddb-table-441)).
- Tag reads lag writes: TagResource/UntagResource are asynchronous and ListTagsOfResource serves the previous set for min 1.58 / p50 1.9 / max 3.0 s (removals 1.27-2.44 s), never an empty set and never an error; an invalid NextToken is ignored and returns the first page ([DDB-TABLE-406](#ddb-table-406), [DDB-TABLE-407](#ddb-table-407), [DDB-TABLE-408](#ddb-table-408), [DDB-TABLE-409](#ddb-table-409), [DDB-TABLE-073](#ddb-table-073)).
- DescribeLimits reports quotas, not headroom (account 80000/80000 RCU/WCU, per table 40000/40000); 5 sequential calls in 30 s pass while truly concurrent calls get ThrottlingException 'Rate exceeded' ([DDB-TABLE-131](#ddb-table-131)).

### Timing you should expect
- CreateTable -> ACTIVE: 6.08-6.09 s for a no-index PAY_PER_REQUEST table (n=2); first DescribeTable 200 within 7-83 ms (p50 49 ms, n=10); ListTables lists the name after 58-93 ms (5/5) ([DDB-TABLE-009](#ddb-table-009), [DDB-TABLE-387](#ddb-table-387), [DDB-TABLE-394](#ddb-table-394), [DDB-TABLE-076](#ddb-table-076)).
- DeleteTable -> first ResourceNotFoundException: 4.0-5.9 s (p50 5.1 s, n=10; 5.74-6.0 s in single runs); ListTables drops the name within +-0.8 s of that 404 ([DDB-TABLE-076](#ddb-table-076), [DDB-TABLE-006](#ddb-table-006), [DDB-TABLE-008](#ddb-table-008), [DDB-TABLE-104](#ddb-table-104)).
- Tag backend after DeleteTable: ResourceInUseException at +0.9 s, ResourceNotFoundException from +1.6 s, DescribeTable 404 at +6.0 s ([DDB-TABLE-104](#ddb-table-104)); tag write lock 1.52 s for a rejected shape ([DDB-TABLE-440](#ddb-table-440)); read-after-write 1.58-2.98 s add / 1.27-2.44 s remove (n=5 each) ([DDB-TABLE-406](#ddb-table-406), [DDB-TABLE-408](#ddb-table-408)).
- Re-create accepted 0.04 s after the first 404 ([DDB-TABLE-009](#ddb-table-009)); 20 identical TagResource calls in 0.26 s all 200 ([DDB-TABLE-441](#ddb-table-441)).

### Known handling gaps in the controller
- No finding rendered in this document is stored as suspect-bug or partial. The tag-sync ordering bug lives in service.md: pkg/resource/table/hooks_tags.go:27-73 issues UntagResource for removed keys and then TagResource for added keys in one reconcile, and the ~1.6-1.8 s per-table lock rejects the second call with LimitExceededException 'Table tags are being updated' (15/15 trials; the SDK standard retryer outlasts the lock only ~1/5), so a mixed add+remove delta errors on most reconciles and DeleteTable within ~2 s of a tag write is ResourceInUseException ([DDB-TABLE-103](service.md#ddb-table-103), service.md).

### Where to look next
- The per-table tag lock and the account control-plane limiter ([DDB-TABLE-103](service.md#ddb-table-103), [DDB-TABLE-053](service.md#ddb-table-053), [DDB-TABLE-132](service.md#ddb-table-132), service.md), tag validation ([DDB-TABLE-105](service.md#ddb-table-105) to [DDB-TABLE-109](service.md#ddb-table-109), [DDB-TABLE-368](service.md#ddb-table-368), [DDB-TABLE-372](service.md#ddb-table-372), service.md), identity ([DDB-TABLE-013](service.md#ddb-table-013), [DDB-TABLE-041](service.md#ddb-table-041), [DDB-TABLE-071](service.md#ddb-table-071), service.md) and the error catalogues ([DDB-TABLE-442](service.md#ddb-table-442) to [DDB-TABLE-449](service.md#ddb-table-449), service.md); the UPDATING admission matrix, DeletionProtection and its 15 s cooldown ([DDB-TABLE-002](table-streams-encryption-class.md#ddb-table-002), [DDB-TABLE-003](table-streams-encryption-class.md#ddb-table-003), [DDB-TABLE-016](table-streams-encryption-class.md#ddb-table-016), [DDB-TABLE-438](table-streams-encryption-class.md#ddb-table-438), table-streams-encryption-class.md).
- DeleteTable during a throughput/billing UPDATING window and the provisioned-decrease budget ([DDB-TABLE-063](table-throughput-billing.md#ddb-table-063), [DDB-TABLE-184](table-throughput-billing.md#ddb-table-184), [DDB-TABLE-326](table-throughput-billing.md#ddb-table-326), table-throughput-billing.md); GSI phases that block DeleteTable ([DDB-TABLE-150](table-indexes.md#ddb-table-150), table-indexes.md); the DELETING phase map for sub-resources and the SYSTEM backup created when a PITR table is deleted ([DDB-TABLE-167](table-subresources.md#ddb-table-167), table-subresources.md). Evidence: services/dynamodb/probes/table/{state-machine,idempotency,identity,tags,limits,consistency-windows,creative}/.

Entries below are generated from the lab findings; low-impact items are in the appendix, long notes under details/.
<!-- preserved:end -->

## At a glance

- canonical findings: 24 (high 4 / medium 6 / low 14); duplicates folded into the appendix: 3
- handling: handled 7 · partial 0 · tracked 0 · unhandled 15 · suspect-bug 0 · n-a 2 (tracked = handled/partial whose reference is an open GitHub issue; counted as not handled)
- re-verified: 1 · last_verified: 2026-10-08..2026-10-09 · model: 2012-08-10 (service/dynamodb v1.39.8)
- categories: other 6, idempotency 4, request-validation 3, async-state-machine 2, delete-semantics 2,
  eventual-consistency 2, quota-limit 2, identity 1, normalization 1, response-fidelity 1

## Operations

| operation | kind | required inputs | declared error shapes | paginated |
| --- | --- | --- | --- | --- |
| CreateTable | create | TableName | ResourceInUseException, LimitExceededException, InternalServerError | no |
| DeleteTable | delete | TableName | ResourceInUseException, ResourceNotFoundException, LimitExceededException, InternalServerError | no |
| DescribeLimits | read | - | InternalServerError | no |
| DescribeTable | read | TableName | ResourceNotFoundException, InternalServerError | no |
| ImportTable | create | S3BucketSource, InputFormat, TableCreationParameters | ResourceInUseException, LimitExceededException, ImportConflictException | no |
| ListTables | list | - | InternalServerError | yes |
| ListTagsOfResource | list | ResourceArn | ResourceNotFoundException, InternalServerError | yes |
| TagResource | tag | ResourceArn, Tags | LimitExceededException, ResourceNotFoundException, InternalServerError, ResourceInUseException | no |
| UntagResource | tag | ResourceArn, TagKeys | LimitExceededException, ResourceNotFoundException, InternalServerError, ResourceInUseException | no |
| UpdateTable | update | TableName | ResourceInUseException, ResourceNotFoundException, LimitExceededException, InternalServerError | no |

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

- <a id="ddb-table-001"></a>**DDB-TABLE-001** `async-state-machine` · impact high · handled · verified 2026-10-08
  **While CREATING, UpdateTable and DeleteTable are rejected with ResourceInUseException (HTTP 400); no-index PPR table ACTIVE in ~6.16s**
  During TableStatus=CREATING: UpdateTable(DeletionProtectionEnabled)=ResourceInUseException/400,
  UpdateTable(StreamSpecification)=ResourceInUseException/400, DeleteTable=ResourceInUseException/400,
  duplicate CreateTable=ResourceInUseException/400; message e.g. 'Attempt to change a resource which is still
  in use: Table is being created: ackq-65ed52-adm'. The table stays CREATING and reaches ACTIVE after 6.16 s.
  DescribeTable immediately after CreateTable returned: {'ok': True, 'code': None, 'status': 'CREATING'}.
  - ACK: requeue, synced.when, updateable.when, deletable.when · ops: CreateTable, UpdateTable, DeleteTable,
    DescribeTable
  - repro: CreateTable (PAY_PER_REQUEST, 1 key) then immediately UpdateTable/DeleteTable; poll DescribeTable
    1/s
  - measurements: creating_duration_s=6.16, create_latency_ms=72
  - handling: handled via `generator.yaml:104-109; pkg/resource/table/hooks.go:72-93`
  - related: [DDB-TABLE-002](table-streams-encryption-class.md#ddb-table-002), [DDB-TABLE-370](table-throughput-billing.md#ddb-table-370), [DDB-TABLE-369](table-streams-encryption-class.md#ddb-table-369), [DDB-TABLE-052](table-streams-encryption-class.md#ddb-table-052), [DDB-TABLE-285](table-streams-encryption-class.md#ddb-table-285), [DDB-TABLE-120](table-streams-encryption-class.md#ddb-table-120),
    [DDB-TABLE-065](table-streams-encryption-class.md#ddb-table-065), [DDB-TABLE-459](table-throughput-billing.md#ddb-table-459), [DDB-TABLE-054](table-streams-encryption-class.md#ddb-table-054), [DDB-TABLE-010](#ddb-table-010), [DDB-TABLE-069](table-streams-encryption-class.md#ddb-table-069), [DDB-TABLE-066](table-throughput-billing.md#ddb-table-066), [DDB-TABLE-284](table-streams-encryption-class.md#ddb-table-284),
    [DDB-TABLE-334](table-streams-encryption-class.md#ddb-table-334) · evidence: table/state-machine/admissibility-matrix
  - notes: Hypotheses: H-T-001, H-T-002. H-T-001/H-T-002 CREATING half. 400 is NOT terminal here; the
    reconciler must requeue.

- <a id="ddb-table-104"></a>**DDB-TABLE-104** `async-state-machine` · impact medium · handled · verified 2026-10-08
  **DELETING: tagging backend forgets the table ~1 s after DeleteTable, ~5 s before DescribeTable 404s; UPDATING admits tag ops**
  While UPDATING (provisioned throughput increase, 2.2 s window): TagResource at +0.05 s -> 200,
  ListTagsOfResource -> 200; the following UntagResource/TagResource -> LimitExceededException "Table tags are
  being updated" (the per-table lock, not the table state). After DeleteTable (200, TableStatus DELETING for
  6.0 s): at +0.9 s TagResource -> ResourceInUseException "Attempt to change a resource which is still in use:
  Table is being deleted: <name>" while ListTagsOfResource still returned the full tag set (200); from +1.6 s
  onward, while DescribeTable still reported DELETING, TagResource, UntagResource and ListTagsOfResource all
  returned ResourceNotFoundException ("Requested resource not found: ResourceArn: ... not found"). After the
  DescribeTable 404 (+6.0 s) all three stayed ResourceNotFoundException for the 15 s observed (no 200 ever
  reappeared, tag set not readable via the ARN of a deleted table).
  - ACK: tags.custom-sync, synced.when, deletable.when · ops: TagResource, UntagResource, ListTagsOfResource,
    DeleteTable, UpdateTable
  - repro: UpdateTable ProvisionedThroughput 1/1->2/2 then tag ops every 0.3 s while UPDATING; DeleteTable
    then tag/untag/list every 0.7 s until 15 s after DescribeTable 404
  - measurements: updating_window_s=2.203, deleting_window_s=6.003, tag_api_404_after_delete_s=1.6,
    list_tags_200_after_404_s=0
  - handling: handled via `pkg/resource/table/hooks.go:175-196; 888fee6`
  - related: [DDB-TABLE-006](#ddb-table-006), [DDB-TABLE-008](#ddb-table-008), [DDB-TABLE-076](#ddb-table-076), [DDB-TABLE-063](table-throughput-billing.md#ddb-table-063), [DDB-TABLE-103](service.md#ddb-table-103), [DDB-TABLE-381](table-throughput-billing.md#ddb-table-381),
    [DDB-TABLE-005](#ddb-table-005), [DDB-TABLE-101](service.md#ddb-table-101), [DDB-TABLE-110](#ddb-table-110), [DDB-TABLE-173](service.md#ddb-table-173), [DDB-TABLE-440](#ddb-table-440), [DDB-TABLE-441](#ddb-table-441), [DDB-TABLE-130](service.md#ddb-table-130),
    [DDB-TABLE-108](service.md#ddb-table-108), [DDB-TABLE-071](service.md#ddb-table-071), [DDB-TABLE-184](table-throughput-billing.md#ddb-table-184) · evidence: table/tags/state-gating-and-lag
  - notes: Partially confirms H-T-064: ListTagsOfResource does keep serving the tag set for the first second
    of DELETING, but it switches to ResourceNotFoundException long before DescribeTable does, so a finalizer
    must not use tag-API 404s as the "table is gone" signal and should skip tag reconciliation as...
  - full notes: [details/DDB-TABLE-104.md](details/DDB-TABLE-104.md)

## Field matrix

C = accepted by the create input (CreateTable), U = by the update input, R = present in the read output.

| leaf | C | U | R | type |
| --- | --- | --- | --- | --- |
| ArchivalBackupArn | - | - | x | string |
| ArchivalDateTime | - | - | x | timestamp |
| ArchivalReason | - | - | x | string |
| ArchivalSummary | - | - | x | struct:ArchivalSummary |
| AttributeDefinitions | x | x | x | list<struct:AttributeDefinition> |
| AttributeName | x | x | x | string |
| AttributeType | x | x | x | enum:ScalarAttributeType |
| Backfilling | - | - | x | boolean |
| BillingMode | x | x | x | enum:BillingMode |
| BillingModeSummary | - | - | x | struct:BillingModeSummary |
| Create | - | x | - | struct:CreateGlobalSecondaryIndexAction |
| CreationDateTime | - | - | x | timestamp |
| Delete | - | x | - | struct:DeleteGlobalSecondaryIndexAction |
| DeletionProtectionEnabled | x | x | x | boolean |
| Enabled | x | x | - | boolean |
| GlobalSecondaryIndexUpdates | - | x | - | list<struct:GlobalSecondaryIndexUpdate> |
| GlobalSecondaryIndexes | x | x | x | list<struct:GlobalSecondaryIndex> |
| GlobalTableSettingsReplicationMode | x | x | x | enum:GlobalTableSettingsReplicationMode |
| GlobalTableSourceArn | x | - | - | string |
| GlobalTableVersion | - | - | x | string |
| GlobalTableWitnessUpdates | - | x | - | list<struct:GlobalTableWitnessGroupUpdate> |
| GlobalTableWitnesses | - | - | x | list<struct:GlobalTableWitnessDescription> |
| InaccessibleEncryptionDateTime | - | - | x | timestamp |
| IndexArn | - | - | x | string |
| IndexName | x | x | x | string |
| IndexSizeBytes | - | - | x | long |
| IndexStatus | - | - | x | enum:IndexStatus |
| ItemCount | - | - | x | long |
| KMSMasterKeyArn | - | - | x | string |
| KMSMasterKeyId | x | x | x | string |
| Key | x | - | - | string |
| KeySchema | x | x | x | list<struct:KeySchemaElement> |
| KeyType | x | x | x | enum:KeyType |
| LastDecreaseDateTime | - | - | x | timestamp |
| LastIncreaseDateTime | - | - | x | timestamp |
| LastUpdateDateTime | - | - | x | timestamp |
| LastUpdateToPayPerRequestDateTime | - | - | x | timestamp |
| LatestStreamArn | - | - | x | string |
| LatestStreamLabel | - | - | x | string |
| LocalSecondaryIndexes | x | - | x | list<struct:LocalSecondaryIndex> |
| MaxReadRequestUnits | x | x | x | long |
| MaxWriteRequestUnits | x | x | x | long |
| MultiRegionConsistency | - | x | x | enum:MultiRegionConsistency |
| NonKeyAttributes | x | x | x | list<string> |
| NumberOfDecreasesToday | - | - | x | long |
| OnDemandThroughput | x | x | x | struct:OnDemandThroughput |
| OnDemandThroughputOverride | - | x | x | struct:OnDemandThroughputOverride |
| Projection | x | x | x | struct:Projection |
| ProjectionType | x | x | x | enum:ProjectionType |
| ProvisionedThroughput | x | x | x | struct:ProvisionedThroughput |
| ProvisionedThroughputOverride | - | x | x | struct:ProvisionedThroughputOverride |
| ReadCapacityUnits | x | x | x | long |
| ReadUnitsPerSecond | x | x | x | long |
| RegionName | - | x | x | string |
| ReplicaArn | - | - | x | string |
| ReplicaInaccessibleDateTime | - | - | x | timestamp |
| ReplicaStatus | - | - | x | enum:ReplicaStatus |
| ReplicaStatusDescription | - | - | x | string |
| ReplicaStatusPercentProgress | - | - | x | string |
| ReplicaTableClassSummary | - | - | x | struct:TableClassSummary |
| ReplicaUpdates | - | x | - | list<struct:ReplicationGroupUpdate> |
| Replicas | - | - | x | list<struct:ReplicaDescription> |
| ResourcePolicy | x | - | - | string |
| RestoreDateTime | - | - | x | timestamp |
| RestoreInProgress | - | - | x | boolean |
| RestoreSummary | - | - | x | struct:RestoreSummary |
| SSEDescription | - | - | x | struct:SSEDescription |
| SSESpecification | x | x | - | struct:SSESpecification |
| SSEType | x | x | x | enum:SSEType |
| SourceBackupArn | - | - | x | string |
| SourceTableArn | - | - | x | string |
| Status | - | - | x | enum:IndexStatus |
| StreamEnabled | x | x | x | boolean |
| StreamSpecification | x | x | x | struct:StreamSpecification |
| StreamViewType | x | x | x | enum:StreamViewType |
| Table | - | - | x | struct:TableDescription |
| TableArn | - | - | x | string |
| TableClass | x | x | x | enum:TableClass |
| TableClassOverride | - | x | - | enum:TableClass |
| TableClassSummary | - | - | x | struct:TableClassSummary |
| TableId | - | - | x | string |
| TableName | x | x | x | string |
| TableSizeBytes | - | - | x | long |
| TableStatus | - | - | x | enum:TableStatus |
| Tags | x | - | - | list<struct:Tag> |
| Update | - | x | - | struct:UpdateGlobalSecondaryIndexAction |
| Value | x | - | - | string |
| WarmThroughput | x | x | x | struct:WarmThroughput |
| WitnessStatus | - | - | x | enum:WitnessStatus |
| WriteCapacityUnits | x | x | x | long |
| WriteUnitsPerSecond | x | x | x | long |

## Idempotency

- <a id="ddb-table-005"></a>**DDB-TABLE-005** `idempotency` · impact high · handled · verified 2026-10-09, re-verified
  **Duplicate CreateTable -> ResourceInUseException in CREATING/ACTIVE/UPDATING/DELETING; message 'Table already exists' except CREATING**
  CreateTable with an existing name (same params / different key type / invalid KeySchema) by state:
  {'CREATING': {'create_dup_same': 'ResourceInUseException/400', 'create_dup_diff_keytype':
  'ResourceInUseException/400', 'create_dup_invalid_keyschema': 'ValidationException/400'}, 'ACTIVE':
  {'create_dup_same': 'ResourceInUseException/400', 'create_dup_diff_keytype': 'ResourceInUseException/400',
  'create_dup_invalid_keyschema': 'ValidationException/400'}, 'UPDATING': {'create_dup_same':
  'ResourceInUseException/400', 'create_dup_diff_keytype': 'ResourceInUseException/400',
  'create_dup_invalid_keyschema': 'ValidationException/400'}, 'DELETING': {'create_dup_same':
  'ResourceInUseException/400', 'create_dup_diff_keytype': 'ResourceInUseException/400',
  'create_dup_invalid_keyschema': 'ValidationException/400'}}. Messages: {'CREATING': 'Attempt to change a
  resource which is still in use: Table is being created: ackq-65ed52-adm', 'ACTIVE': 'Table already exists:
  ackq-65ed52-adm', 'DELETING': 'Table already exists: ackq-65ed52-adm'}. There is no TableAlreadyExists code;
  the caller must DescribeTable to learn the state.
  - ACK: custom_create, custom_find, requeue · ops: CreateTable
  - repro: CreateTable twice (second while CREATING, third after ACTIVE, fourth right after DeleteTable)
  - handling: handled via `pkg/resource/table/hooks.go:81-84; pkg/resource/table/hooks.go:198-202; generator.yaml:88-90; pkg/resource/table/sdk.go:1234-1250`
  - related: [DDB-TABLE-006](#ddb-table-006), [DDB-TABLE-009](#ddb-table-009), [DDB-TABLE-077](#ddb-table-077), [DDB-TABLE-013](service.md#ddb-table-013), [DDB-TABLE-063](table-throughput-billing.md#ddb-table-063), [DDB-TABLE-103](service.md#ddb-table-103),
    [DDB-TABLE-381](table-throughput-billing.md#ddb-table-381), [DDB-TABLE-101](service.md#ddb-table-101), [DDB-TABLE-104](#ddb-table-104) · evidence: table/state-machine/admissibility-matrix,
    table/creative/reverify-set-a2
  - notes: Hypotheses: H-T-053. Whether an invalid payload against an existing name yields ValidationException
    or ResourceInUseException shows validation order.

- <a id="ddb-table-008"></a>**DDB-TABLE-008** `idempotency` · impact high · handled · verified 2026-10-08
  **DeleteTable while DELETING -> ResourceInUseException; after the table is gone -> ResourceNotFoundException**
  Repeated DeleteTable calls during the DELETING window (every ~0.5s, 12 calls) returned: [(0.0,
  'ResourceInUseException'), (0.53, 'ResourceInUseException'), (1.06, 'ResourceInUseException'), (1.59,
  'ResourceInUseException'), (2.13, 'ResourceInUseException'), (2.66, 'ResourceInUseException'), (3.19,
  'ResourceInUseException'), (3.72, 'ResourceInUseException'), (4.26, 'ResourceInUseException'), (4.79,
  'ResourceInUseException'), (5.33, 'ResourceInUseException'), (5.86, 'ResourceNotFoundException')]. Distinct
  outcomes: [[False, 'ResourceInUseException', None], [False, 'ResourceNotFoundException', None]]. DeleteTable
  once DescribeTable returns ResourceNotFoundException -> ResourceNotFoundException (HTTP 400) 'Requested
  resource not found: Table: ackq-7cb2b8-idem not found'.
  - ACK: custom_delete, requeue, exceptions.404 · ops: DeleteTable
  - repro: DeleteTable; immediately DeleteTable again repeatedly until ResourceNotFoundException
  - handling: handled via `templates/hooks/table/sdk_delete_pre_build_request.go.tpl:1-6; pkg/resource/table/hooks.go:60-65`
  - related: [DDB-TABLE-006](#ddb-table-006), [DDB-TABLE-076](#ddb-table-076), [DDB-TABLE-104](#ddb-table-104) · evidence:
    table/idempotency/dup-create-double-delete
  - notes: Hypotheses: H-T-008. H-T-008 refuted/partial: see distinct outcomes.

- <a id="ddb-table-010"></a>**DDB-TABLE-010** `idempotency` · impact medium · unhandled (not handled in controller) · verified 2026-10-08
  **Duplicate CreateTable on ACTIVE table -> ResourceInUseException regardless of params; names are case-sensitive**
  CreateTable(existing name, identical params) -> ResourceInUseException HTTP 400 'Table already exists:
  ackq-7cb2b8-idem'; with a different BillingMode -> ResourceInUseException 'Table already exists:
  ackq-7cb2b8-idem'; with the name upper-cased -> OK (distinct table; names are case-sensitive). DescribeTable
  immediately after CreateTable (10 calls, 100ms apart) returned: ['CREATING', 'CREATING', 'CREATING',
  'CREATING', 'CREATING', 'CREATING', 'CREATING', 'CREATING', 'CREATING', 'CREATING']; ListTables immediately
  after create: True.
  - ACK: custom_create, custom_find · ops: CreateTable, DescribeTable
  - repro: CreateTable twice with same name (same and different params)
  - handling: not handled in the controller (as of commit 34b85e6)
  - related: [DDB-TABLE-001](#ddb-table-001), [DDB-TABLE-002](table-streams-encryption-class.md#ddb-table-002), [DDB-TABLE-370](table-throughput-billing.md#ddb-table-370), [DDB-TABLE-369](table-streams-encryption-class.md#ddb-table-369), [DDB-TABLE-052](table-streams-encryption-class.md#ddb-table-052), [DDB-TABLE-285](table-streams-encryption-class.md#ddb-table-285),
    [DDB-TABLE-120](table-streams-encryption-class.md#ddb-table-120), [DDB-TABLE-065](table-streams-encryption-class.md#ddb-table-065), [DDB-TABLE-459](table-throughput-billing.md#ddb-table-459), [DDB-TABLE-054](table-streams-encryption-class.md#ddb-table-054) · evidence:
    table/idempotency/dup-create-double-delete
  - notes: Hypotheses: H-T-053. No idempotency token on CreateTable; identical re-sends are errors, not
    no-ops.

## Request validation

- <a id="ddb-table-110"></a>**DDB-TABLE-110** `request-validation` · impact medium · handled · verified 2026-10-08
  **CreateTable with invalid or >50 tags fails with ValidationException and creates nothing; CreateTable Tags=[] is accepted**
  CreateTable Tags=[{aws:foo}] -> ValidationException 'Tag Key cannot be prefixed with aws:, Key: aws:foo';
  key 129 chars -> ValidationException 'The Tag Key provided is invalid, Key:
  xxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxx'; 51 tags -> ValidationException 'Number of
  Tags exceed the current limit for the provided ResourceArn'; duplicate keys -> ValidationException (stored:
  None); Tags=[] -> 200; '#' in key -> ValidationException; 50 tags -> 200 (50 tags visible 0.01 s after
  ACTIVE). DescribeTable 1s after each rejected create -> {'aws_prefix': 'ResourceNotFoundException',
  'key_129': 'ResourceNotFoundException', '51_tags': 'ResourceNotFoundException', 'dup_keys':
  'ResourceNotFoundException', 'hash_char': 'ResourceNotFoundException'}.
  - ACK: terminal_codes, tags.custom-sync · ops: CreateTable · fields: Tags
  - repro: CreateTable with each Tags payload (boto3 parameter_validation=False); DescribeTable after 1s
  - handling: handled via `templates/hooks/table/sdk_create_post_set_output.go.tpl:1-4; bcd26e1`
  - related: [DDB-TABLE-101](service.md#ddb-table-101), [DDB-TABLE-104](#ddb-table-104), [DDB-TABLE-105](service.md#ddb-table-105), [DDB-TABLE-106](service.md#ddb-table-106), [DDB-TABLE-107](service.md#ddb-table-107), [DDB-TABLE-108](service.md#ddb-table-108),
    [DDB-TABLE-109](service.md#ddb-table-109), [DDB-TABLE-368](service.md#ddb-table-368), [DDB-TABLE-372](service.md#ddb-table-372) · evidence: table/tags/validation-upsert-limits
  - notes: Confirms the no-orphan part of H-T-133 and refutes its LimitExceededException claim for 51 tags
    (ValidationException). Asymmetry: an empty Tags list is accepted by CreateTable but rejected by
    TagResource ('Atleast one Tag needs to be provided as Input.'). 50 tags on CreateTable are visible the...
  - full notes: [details/DDB-TABLE-110.md](details/DDB-TABLE-110.md)

## Response fidelity and consistency

- <a id="ddb-table-076"></a>**DDB-TABLE-076** `eventual-consistency` · impact medium · handled · verified 2026-10-08
  **ListTables lists a new table within 0.1 s of CreateTable and drops a deleted one within +-0.8 s of DescribeTable's first 404**
  ListTables (polled every 0.5 s with full pagination) already contained the new name on the first poll, 58-93
  ms after CreateTable returned (5/5 trials, never absent first). After DeleteTable (response
  TableStatus=DELETING), DescribeTable kept returning DELETING and switched to ResourceNotFoundException after
  4.0-5.9 s (p50 5.1 s, n=10); ListTables dropped the name 4.0-6.0 s after DeleteTable, i.e. between 0.79 s
  before and 0.44 s after the first DescribeTable 404 (mean +0.06 s). In 0/5 trials was the name still listed
  at the moment of the first 404. After the first 404, DescribeTable was polled for 10 more seconds at 200 ms
  and never flapped back to 200; ListTables presence never flapped either.
  - ACK: requeue, deletable.when, custom_find · ops: ListTables, DeleteTable, DescribeTable
  - repro: CreateTable/DeleteTable with ListTables + DescribeTable polling at 0.5s / 0.25s
  - measurements: list_lag_create_max_s=0.093, list_gone_after_delete_max_s=6.046,
    list_gone_minus_describe_404_mean_s=0.058, deleting_window_p50_s=5.097, deleting_window_max_s=5.882,
    flaps_after_404=0, list_tables_flaps=0
  - handling: handled via `test/e2e/table.py:240-269; b323c3d; templates/hooks/table/sdk_delete_pre_build_request.go.tpl:1-6; pkg/resource/table/hooks.go:60-65`
  - related: [DDB-TABLE-007](#ddb-table-007), [DDB-TABLE-006](#ddb-table-006), [DDB-TABLE-008](#ddb-table-008), [DDB-TABLE-104](#ddb-table-104), [DDB-TABLE-075](service.md#ddb-table-075), [DDB-TABLE-181](#ddb-table-181) ·
    evidence: table/consistency-windows/create-delete-visibility
  - notes: Confirms the ~5 s DELETING window of H-T-007 but refutes the 'ListTables still includes the name
    after the 404' part: the two read paths converge within a second of each other and ListTables may even
    drop the name first. 404 is terminal (no flapping observed in 10 deletions).

## Delete semantics

- <a id="ddb-table-006"></a>**DDB-TABLE-006** `delete-semantics` · impact high · handled · verified 2026-10-08
  **DeleteTable response TableStatus=DELETING; DELETING lasts ~5.74s; DeleteTable again while DELETING -> ResourceInUseException/400**
  DeleteTable on an ACTIVE empty table returns HTTP 200 with TableDescription.TableStatus=DELETING.
  DescribeTable reports DELETING for 5.74 s, then ResourceNotFoundException. During DELETING: DeleteTable
  again -> ResourceInUseException/400 (message 'Attempt to change a resource which is still in use: Table is
  being deleted: ackq-65ed52-adm'), UpdateTable -> ResourceInUseException/400, duplicate CreateTable ->
  ResourceInUseException/400.
  - ACK: deletable.when, requeue, exceptions.404 · ops: DeleteTable, DescribeTable
  - repro: DeleteTable; immediately DeleteTable again; poll DescribeTable 2/s until RNF
  - measurements: deleting_duration_s=5.74
  - handling: handled via `templates/hooks/table/sdk_delete_pre_build_request.go.tpl:1-6; pkg/resource/table/hooks.go:60-65; generator.yaml:88-90; pkg/resource/table/sdk.go:1234-1250`
  - related: [DDB-TABLE-005](#ddb-table-005), [DDB-TABLE-009](#ddb-table-009), [DDB-TABLE-077](#ddb-table-077), [DDB-TABLE-013](service.md#ddb-table-013), [DDB-TABLE-008](#ddb-table-008), [DDB-TABLE-076](#ddb-table-076),
    [DDB-TABLE-104](#ddb-table-104), [DDB-TABLE-063](table-throughput-billing.md#ddb-table-063), [DDB-TABLE-103](service.md#ddb-table-103), [DDB-TABLE-381](table-throughput-billing.md#ddb-table-381), [DDB-TABLE-101](service.md#ddb-table-101) · evidence:
    table/state-machine/admissibility-matrix
  - notes: Hypotheses: H-T-008, H-T-002.

- <a id="ddb-table-009"></a>**DDB-TABLE-009** `delete-semantics` · impact medium · unhandled (not handled in controller) · verified 2026-10-08
  **CreateTable with the same name right after DescribeTable first returns ResourceNotFound: accepted immediately**
  Re-create attempts (t_s since first RNF, outcome): [(0.04, 'OK')]. Re-created table identity:
  {'TableId_new': 'a225901b-431e-4a8a-a584-e9b8d88d010f', 'TableId_orig':
  '5313d8ea-44e5-4e0e-b247-25a19a96f6a2', 'TableId_changed': True, 'TableArn_same': True}. The new table
  reached ACTIVE after 6.08 s.
  - ACK: custom_create, requeue, is_arn_primary_key · ops: CreateTable, DescribeTable · fields: TableId,
    TableArn
  - repro: DeleteTable; poll DescribeTable until RNF; immediately CreateTable same name
  - measurements: recreate_accepted_after_s=0.04
  - handling: not handled in the controller (as of commit 34b85e6)
  - related: [DDB-TABLE-077](#ddb-table-077), [DDB-TABLE-005](#ddb-table-005), [DDB-TABLE-006](#ddb-table-006), [DDB-TABLE-013](service.md#ddb-table-013) · evidence:
    table/idempotency/dup-create-double-delete
  - notes: TableArn is deterministic (same for the re-created table) while TableId changes: ARN alone cannot
    detect a delete+recreate.

## Quotas and rate limits

- <a id="ddb-table-440"></a>**DDB-TABLE-440** `quota-limit` · impact medium · unhandled (not handled in controller) · verified 2026-10-09
  **Tag write lock (~1.5 s) admits only a TagResource that includes the in-flight tags (replay or full set); other Tag/Untag -> LimitExceeded**
  Pairs of back-to-back calls on one table (second call within ms of an effective TagResource): identical
  replay {A} after {A} -> 200; {A} after {B} (A already committed, i.e. a no-op against the committed set) ->
  LimitExceededException 'Subscriber limit exceeded: Table tags are being updated: <name>'; {A,B,C} after {C}
  -> 200; the full desired set {base..., A, B, C, D} after {D} -> 200; UntagResource of an absent key after
  {E} -> LimitExceededException; {F:y} after {F:x} -> LimitExceededException; ListTagsOfResource -> 200.
  Polling the rejected shape at 0.2 s after an effective write: LimitExceededException until 1.52 s, then 200.
  - ACK: tags.custom-sync, requeue · ops: TagResource, UntagResource, ListTagsOfResource · fields: Tags
  - repro: TagResource {B}; immediately TagResource {A} (A already present) -> LimitExceededException; vs
    TagResource {C}; immediately TagResource {A,B,C} -> 200
  - measurements: lock_duration_for_rejected_request_s=1.52, trials_per_shape=1
  - handling: not handled in the controller (as of commit 34b85e6)
  - related: [DDB-TABLE-103](service.md#ddb-table-103), [DDB-TABLE-172](service.md#ddb-table-172), [DDB-TABLE-173](service.md#ddb-table-173), [DDB-TABLE-441](#ddb-table-441), [DDB-TABLE-130](service.md#ddb-table-130), [DDB-TABLE-104](#ddb-table-104),
    [DDB-TABLE-108](service.md#ddb-table-108) · evidence: table/creative/noop-tag-storm
  - notes: Refines [DDB-TABLE-172](service.md#ddb-table-172) ('only effective changes take the lock'): the lock is taken by the effective
    write, but the admission test for the NEXT call is whether it contains the in-flight request's key/value
    pairs, not whether it is a no-op against the committed set. For a controller this means: write...
  - full notes: [details/DDB-TABLE-440.md](details/DDB-TABLE-440.md)

## Handling gaps (bugs to file)

None recorded for this document's findings; see [service.md 'Handling gaps summary'](service.md#handling-gaps-summary) for the service-wide list.

## E2E timing

Values are seconds unless the key says otherwise; n = trials behind the numbers ('1 run' when the finding records none).

| finding | what | measurements | n |
| --- | --- | --- | --- |
| [DDB-TABLE-001](#ddb-table-001) | While CREATING, UpdateTable and DeleteTable are rejected with ResourceInUseException (HTTP 400); no-index PPR table ACTIVE in ~6.16s | creating_duration_s=6.16, create_latency_ms=72 | 1 run |
| [DDB-TABLE-006](#ddb-table-006) | DeleteTable response TableStatus=DELETING; DELETING lasts ~5.74s; DeleteTable again while DELETING -> ResourceInUseException/400 | deleting_duration_s=5.74 | 1 run |
| [DDB-TABLE-009](#ddb-table-009) | CreateTable with the same name right after DescribeTable first returns ResourceNotFound: accepted immediately | recreate_accepted_after_s=0.04 | 1 run |
| [DDB-TABLE-076](#ddb-table-076) | ListTables lists a new table within 0.1 s of CreateTable and drops a deleted one within +-0.8 s of DescribeTable's first 404 | list_lag_create_max_s=0.093, list_gone_after_delete_max_s=6.046, list_gone_minus_describe_404_mean_s=0.058, deleting_window_p50_s=5.097, deleting_window_max_s=5.882, flaps_after_404=0, list_tables_flaps=0 | 1 run |
| [DDB-TABLE-104](#ddb-table-104) | DELETING: tagging backend forgets the table ~1 s after DeleteTable, ~5 s before DescribeTable 404s; UPDATING admits tag ops | updating_window_s=2.203, deleting_window_s=6.003, tag_api_404_after_delete_s=1.6, list_tags_200_after_404_s=0 | 1 run |
| [DDB-TABLE-440](#ddb-table-440) | Tag write lock (~1.5 s) admits only a TagResource that includes the in-flight tags (replay or full set); other Tag/Untag -> LimitExceeded | lock_duration_for_rejected_request_s=1.52, trials_per_shape=1 | 1 run |
| [DDB-TABLE-441](#ddb-table-441) | No-op tag writes are free: 10 concurrent identical TagResource/UntagResource(absent) and 20 sequential replays -> all 200, no limiter/lock | noop_tag_throttled_of_10=0, effective_tag_throttled_of_10=3, sequential_noop_x20_span_s=0.26 | 1 run |

## Open questions

<!-- preserved:start id=open-questions -->
<!-- open questions and follow-up experiments; survives re-renders -->
<!-- preserved:end -->

## Appendix: low-impact and duplicate findings

| id | category | impact | status | title | related | duplicate_of |
| --- | --- | --- | --- | --- | --- | --- |
| <a id="ddb-table-007"></a>**DDB-TABLE-007** | delete-semantics | medium | confirmed | After DeleteTable, DescribeTable shows DELETING for ~5.86s and ListTables drops the name at ~5.86s | - | [DDB-TABLE-076](#ddb-table-076) |
| <a id="ddb-table-012"></a>**DDB-TABLE-012** | response-fidelity | low | confirmed | ListTables(Limit=1): final page omits LastEvaluatedTableName; next call returns []; order is byte-wise (upper before lower) | [DDB-TABLE-181](#ddb-table-181), [DDB-TABLE-379](#ddb-table-379), [DDB-TABLE-072](#ddb-table-072) | - |
| <a id="ddb-table-072"></a>**DDB-TABLE-072** | identity | low | confirmed | DescribeTable accepts the full table ARN as TableName (missing table -> RNF naming the extracted name); ListTables cursor does not | [DDB-TABLE-012](#ddb-table-012), [DDB-TABLE-181](#ddb-table-181), [DDB-TABLE-379](#ddb-table-379), [DDB-TABLE-013](service.md#ddb-table-013), [DDB-TABLE-041](service.md#ddb-table-041) | - |
| <a id="ddb-table-073"></a>**DDB-TABLE-073** | request-validation | low | confirmed | ListTagsOfResource ignores an invalid NextToken and returns the first page (200) | [DDB-TABLE-108](service.md#ddb-table-108), [DDB-TABLE-109](service.md#ddb-table-109) | - |
| <a id="ddb-table-077"></a>**DDB-TABLE-077** | delete-semantics | medium | confirmed (hypothesis refuted) | A just-deleted table name can be re-created the instant DescribeTable returns 404 (5/5, no ResourceInUseException) | [DDB-TABLE-005](#ddb-table-005), [DDB-TABLE-006](#ddb-table-006), [DDB-TABLE-009](#ddb-table-009), [DDB-TABLE-013](service.md#ddb-table-013) | [DDB-TABLE-009](#ddb-table-009) |
| <a id="ddb-table-131"></a>**DDB-TABLE-131** | quota-limit | low | confirmed (hypothesis refuted) | DescribeLimits: 5 sequential calls in 30 s all succeed; only a concurrent burst gets ThrottlingException 'Rate exceeded'; values 80k/40k | [DDB-TABLE-053](service.md#ddb-table-053), [DDB-TABLE-130](service.md#ddb-table-130), [DDB-TABLE-132](service.md#ddb-table-132), [DDB-TABLE-441](#ddb-table-441), [DDB-TABLE-051](table-throughput-billing.md#ddb-table-051) | - |
| <a id="ddb-table-181"></a>**DDB-TABLE-181** | identity | low | confirmed (hypothesis refuted) | ListTables Limit=1 pagination: LastEvaluatedTableName is omitted on the final item; bytewise-sorted; read-after-create window | [DDB-TABLE-075](service.md#ddb-table-075), [DDB-TABLE-076](#ddb-table-076), [DDB-TABLE-012](#ddb-table-012), [DDB-TABLE-379](#ddb-table-379), [DDB-TABLE-072](#ddb-table-072) | [DDB-TABLE-012](#ddb-table-012) |
| <a id="ddb-table-366"></a>**DDB-TABLE-366** | normalization | low | confirmed | INCLUDE projections are stored verbatim: key attributes are accepted and not pruned; duplicates are a synchronous ValidationException | [DDB-TABLE-125](table-indexes.md#ddb-table-125), [DDB-TABLE-126](table-indexes.md#ddb-table-126), [DDB-TABLE-041](service.md#ddb-table-041), [DDB-TABLE-129](table-indexes.md#ddb-table-129), [DDB-TABLE-169](table-indexes.md#ddb-table-169), [DDB-TABLE-043](table-indexes.md#ddb-table-043), [DDB-TABLE-123](table-indexes.md#ddb-table-123), [DDB-TABLE-124](table-indexes.md#ddb-table-124), [DDB-TABLE-127](table-indexes.md#ddb-table-127) | - |
| <a id="ddb-table-379"></a>**DDB-TABLE-379** | request-validation | low | confirmed | ListTables accepts a deleted or never-existing name as ExclusiveStartTableName (200, cursor still works); only charset/length are validated | [DDB-TABLE-012](#ddb-table-012), [DDB-TABLE-181](#ddb-table-181), [DDB-TABLE-072](#ddb-table-072) | - |
| <a id="ddb-table-387"></a>**DDB-TABLE-387** | other | low | confirmed | Doc claim C004 TRUE: CreateTable is asynchronous - response TableStatus=CREATING, ACTIVE ~2-6 s later for a no-index PPR table | [DDB-TABLE-001](#ddb-table-001), [DDB-TABLE-075](service.md#ddb-table-075) | - |
| <a id="ddb-table-394"></a>**DDB-TABLE-394** | eventual-consistency | low | confirmed | Doc claim C016 FALSE: 'DescribeTable right after CreateTable might return ResourceNotFoundException' - never observed (0/10 at 100 ms) | [DDB-TABLE-075](service.md#ddb-table-075), [DDB-TABLE-001](#ddb-table-001), [DDB-TABLE-076](#ddb-table-076) | - |
| <a id="ddb-table-400"></a>**DDB-TABLE-400** | other | low | confirmed | Doc claim C029 TRUE: ListTables Limit caps the page - Limit=1 returns one name per page; LastEvaluatedTableName omitted on the final item | [DDB-TABLE-012](#ddb-table-012), [DDB-TABLE-181](#ddb-table-181) | - |
| <a id="ddb-table-406"></a>**DDB-TABLE-406** | other | low | confirmed | Doc claim C037 TRUE: TagResource is an asynchronous operation | [DDB-TABLE-102](service.md#ddb-table-102), [DDB-TABLE-172](service.md#ddb-table-172), [DDB-TABLE-103](service.md#ddb-table-103) | - |
| <a id="ddb-table-407"></a>**DDB-TABLE-407** | other | low | confirmed | Doc claim C038 TRUE: ListTagsOfResource is eventually consistent; tag metadata may not be available right after TagResource | [DDB-TABLE-102](service.md#ddb-table-102) | - |
| <a id="ddb-table-408"></a>**DDB-TABLE-408** | other | low | confirmed | Doc claim C039 TRUE: Application and removal of tags via TagResource/UntagResource is eventually consistent | [DDB-TABLE-102](service.md#ddb-table-102), [DDB-TABLE-172](service.md#ddb-table-172) | - |
| <a id="ddb-table-409"></a>**DDB-TABLE-409** | other | low | confirmed | Doc claim C040 TRUE: UntagResource is an asynchronous operation | [DDB-TABLE-102](service.md#ddb-table-102), [DDB-TABLE-172](service.md#ddb-table-172) | - |
| <a id="ddb-table-441"></a>**DDB-TABLE-441** | idempotency | low | confirmed | No-op tag writes are free: 10 concurrent identical TagResource/UntagResource(absent) and 20 sequential replays -> all 200, no limiter/lock | [DDB-TABLE-434](service.md#ddb-table-434), [DDB-TABLE-383](table-streams-encryption-class.md#ddb-table-383), [DDB-TABLE-130](service.md#ddb-table-130), [DDB-TABLE-172](service.md#ddb-table-172), [DDB-TABLE-053](service.md#ddb-table-053), [DDB-TABLE-132](service.md#ddb-table-132), [DDB-TABLE-131](#ddb-table-131), [DDB-TABLE-051](table-throughput-billing.md#ddb-table-051), [DDB-TABLE-103](service.md#ddb-table-103), [DDB-TABLE-173](service.md#ddb-table-173), [DDB-TABLE-440](#ddb-table-440), [DDB-TABLE-104](#ddb-table-104), [DDB-TABLE-108](service.md#ddb-table-108) | - |

## Supplementary notes

<!-- preserved:start -->
<!-- preserved:end -->
