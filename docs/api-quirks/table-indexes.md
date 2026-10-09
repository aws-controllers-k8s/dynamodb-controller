<!-- generated: ack-api-quirks dynamodb 2026-10-09 model=2012-08-10 controller=34b85e6 -->
# Table secondary indexes
_GSI/LSI create/update/delete granularity, backfill state machine, validation, throughput interplay._
Generated from ack-api-quirks `services/dynamodb` (render date in the marker above); model 2012-08-10 (service/dynamodb v1.39.8); controller commit 34b85e6; evidence: `services/dynamodb/probes/<probe id>/` in the lab repo.

## Overview

<!-- preserved:start id=overview -->
This document covers GSI/LSI validation at CreateTable, the granularity and admission rules of GlobalSecondaryIndexUpdates, the GSI create/backfill/delete state machine and how index throughput interacts with table billing. The most surprising facts are that a GSI added via UpdateTable keeps TableStatus=UPDATING for only 24-56 s and then the table reads ACTIVE while the index backfills 8-17 min - with DeleteTable rejected the whole time - and that LimitExceededException is one code for retryable and permanent conditions ([DDB-TABLE-148](#ddb-table-148), [DDB-TABLE-150](#ddb-table-150); [DDB-TABLE-171](table-throughput-billing.md#ddb-table-171), table-throughput-billing.md).

### Rules a reconciler must respect
- One GSI Create or Delete per UpdateTable and per table at a time: two Creates/Deletes in one call, or a Create/Delete while another index is CREATING or DELETING, is LimitExceededException 'Only 1 online index can be created or deleted simultaneously per table' (retryable); several Update actions in one call are accepted ([DDB-TABLE-159](#ddb-table-159)).
- A GSI Create cannot share a call with ProvisionedThroughput, StreamSpecification, SSE, TableClass, DeletionProtection or WarmThroughput (ValidationException); only BillingMode or a GSI Update may accompany it; a Create issued right after an accepted PT/stream/TableClass change is ResourceInUseException until that change settles ([DDB-TABLE-174](#ddb-table-174), [DDB-TABLE-175](#ddb-table-175); [DDB-TABLE-382](table-streams-encryption-class.md#ddb-table-382), table-streams-encryption-class.md).
- Re-sending identical ProvisionedThroughput for the table or a GSI is ValidationException 'will not change' (also BillingMode=PROVISIONED + unchanged PT); diff per index before emitting an Update action - partial changes pass ([DDB-TABLE-164](#ddb-table-164)).
- KeySchema, Projection and LSIs are immutable: UpdateTable has no KeySchema/LSI member, GlobalSecondaryIndexUpdates rejects LSI names, and a projection or key change is Delete then Create (same name accepted once the entry is gone; 507 s to ACTIVE on an empty table); DeletionProtection does not protect GSIs, and the controller treats neither LSI changes nor GSI key/projection changes as immutable ([DDB-TABLE-357](#ddb-table-357), [DDB-TABLE-166](#ddb-table-166); [DDB-TABLE-137](table-subresources.md#ddb-table-137), table-subresources.md).
- DeleteTable is ResourceInUseException 'Cannot delete table while indexes are being created, updated, or deleted' while ANY GSI is CREATING/UPDATING/DELETING, even with TableStatus=ACTIVE (~950 attempts over ~16 min): gate delete on every IndexStatus - the controller's delete path does not ([DDB-TABLE-150](#ddb-table-150)).
- Admission is phase-dependent: during resource allocation (TableStatus=UPDATING, Backfilling=false) table PT/stream/OnDemandThroughput and a Delete of the new index are ResourceInUseException and WarmThroughput is HTTP 500; once Backfilling=true (TableStatus=ACTIVE) table PT, GSI Delete, DP, TTL and tags are admitted; while a GSI is DELETING a billing switch or table PT is ResourceInUseException ([DDB-TABLE-149](#ddb-table-149), [DDB-TABLE-458](#ddb-table-458), [DDB-TABLE-375](#ddb-table-375), [DDB-TABLE-376](#ddb-table-376); [DDB-TABLE-163](table-subresources.md#ddb-table-163), table-subresources.md).
- Classify LimitExceededException by message: 'Only 1 online index...' and 'decreases are limited...' clear on their own; the 21st GSI, per-table/per-index capacity caps, OnDemand/Warm caps and the account RCU limit are permanent; the 21st GSI is ValidationException at CreateTable but LimitExceededException via UpdateTable; the controller does not split the code by message ([DDB-TABLE-169](#ddb-table-169), [DDB-TABLE-170](#ddb-table-170); [DDB-TABLE-154](table-throughput-billing.md#ddb-table-154), table-throughput-billing.md).
- An unknown IndexName is ResourceNotFoundException whose message reads 'Index <name> for table' (a missing table reads 'Table: <name> not found'); valid entries in the same call are not applied; WarmThroughput on a ghost index is HTTP 500 InternalFailure; rejected multi-member calls are atomic ([DDB-TABLE-380](#ddb-table-380); [DDB-TABLE-161](table-subresources.md#ddb-table-161), table-subresources.md).
- CreateTable validation: AttributeDefinitions must equal the exact set of key attributes (checked before every other GSI rule, so an extra attribute masks other errors); INCLUDE needs 1-20 NonKeyAttributes, ALL/KEYS_ONLY forbid them, ProjectionType has no default; GlobalSecondaryIndexes=[] and LocalSecondaryIndexes=[] are rejected (send nil); GSI keys accept up to 4 HASH + 4 RANGE elements ([DDB-TABLE-127](#ddb-table-127), [DDB-TABLE-126](#ddb-table-126), [DDB-TABLE-043](#ddb-table-043), [DDB-TABLE-123](#ddb-table-123), [DDB-TABLE-129](#ddb-table-129)).
- UpdateTable AttributeDefinitions is consumed only by a GSI Create (the new key attribute alone suffices); unused, stale or type-conflicting entries elsewhere are silently ignored and a GSI delete auto-prunes AttributeDefinitions, so compare must ignore desired attributes no key uses or the prune leaves a permanent delta ([DDB-TABLE-162](#ddb-table-162), [DDB-TABLE-151](#ddb-table-151); [DDB-TABLE-358](table-throughput-billing.md#ddb-table-358), table-throughput-billing.md).
- PAY_PER_REQUEST -> PROVISIONED requires table ProvisionedThroughput AND an Update for every GSI in the same call (nothing is auto-assigned); a billing switch drops OnDemandThroughput from table and GSIs; table and each GSI have separate 4-per-day decrease budgets and a coupled table+GSI decrease is rejected atomically ([DDB-TABLE-152](#ddb-table-152), [DDB-TABLE-153](#ddb-table-153), [DDB-TABLE-157](#ddb-table-157), [DDB-TABLE-128](#ddb-table-128)).
- DescribeTable echoes: PAY_PER_REQUEST GSIs carry ProvisionedThroughput 0/0 and WarmThroughput 12000/4000 without being set; AttributeDefinitions and NonKeyAttributes come back sorted by name while index lists keep submitted order; a GSI Update response echoes the OLD throughput, a Create echoes the new entry; index ARNs are not taggable ([DDB-TABLE-133](#ddb-table-133), [DDB-TABLE-134](#ddb-table-134), [DDB-TABLE-125](#ddb-table-125), [DDB-TABLE-165](#ddb-table-165); [DDB-TABLE-136](service.md#ddb-table-136), service.md).

### Timing you should expect
- GSI add via UpdateTable on an empty or 1-item table: TableStatus UPDATING 24.3-56.3 s (8+ runs), then the index is ACTIVE after 506-539 s in most runs (n>=7) but 990-1003 s in others (n=4, PAY_PER_REQUEST; [DDB-TABLE-458](#ddb-table-458) saw the slow cells on tables that got an SSE or WarmThroughput write during allocation) ([DDB-TABLE-148](#ddb-table-148), [DDB-TABLE-458](#ddb-table-458), [DDB-TABLE-174](#ddb-table-174), [DDB-TABLE-166](#ddb-table-166)).
- CreateTable with GSIs: table and all indexes flip ACTIVE together after 14-22 s (2 GSIs 16.2 s; 20 GSIs 20-22 s); 3 and 5 concurrent GSI-bearing creates all ACTIVE in 16.3-16.5 s, no serialization ([DDB-TABLE-148](#ddb-table-148), [DDB-TABLE-169](#ddb-table-169), [DDB-TABLE-168](#ddb-table-168), [DDB-TABLE-388](#ddb-table-388)).
- GSI Delete: IndexStatus DELETING 2.6-5.1 s with TableStatus UPDATING ([DDB-TABLE-148](#ddb-table-148), [DDB-TABLE-149](#ddb-table-149), [DDB-TABLE-166](#ddb-table-166), [DDB-TABLE-375](#ddb-table-375), [DDB-TABLE-376](#ddb-table-376)); GSI throughput Update: IndexStatus UPDATING 2.0-4.0 s with TableStatus ACTIVE ([DDB-TABLE-148](#ddb-table-148), [DDB-TABLE-159](#ddb-table-159), [DDB-TABLE-165](#ddb-table-165)).
- GSI WarmThroughput increase: IndexStatus stays ACTIVE, WarmThroughput.Status UPDATING ~6.5 min; billing switch PAY_PER_REQUEST -> PROVISIONED with a GSI: UPDATING 82.7-112.5 s ([DDB-TABLE-155](#ddb-table-155), [DDB-TABLE-152](#ddb-table-152), [DDB-TABLE-174](#ddb-table-174)).
- LimitExceededException rejections take 0.8-2.9 s versus ~10 ms for ValidationException ([DDB-TABLE-171](table-throughput-billing.md#ddb-table-171), [DDB-TABLE-159](#ddb-table-159), [DDB-TABLE-169](#ddb-table-169)).

### Known handling gaps in the controller
- The hooks catalog records that updateGSIs sends only IndexName plus throughput members although equalGlobalSecondaryIndexes also diffs Projection/KeySchema (pkg/resource/table/hooks_global_secondary_indexes.go:84-131, 246-258; [GT-DDB-066](service.md#gt-ddb-066) (controller hooks catalog entry)); confirmed: a changed Projection/KeySchema is classified as 'updated' and emits a throughput-only UpdateGlobalSecondaryIndexAction every reconcile, which DynamoDB rejects with 'will not change' when PT is unchanged, so the delta persists and the index is never recreated - the working path is Delete, wait for the entry to vanish, then Create ([DDB-TABLE-164](#ddb-table-164), [DDB-TABLE-166](#ddb-table-166)).
- The same path (pkg/resource/table/hooks_global_secondary_indexes.go:84-131, 246-258) is stored as partial for the response shape: a GSI throughput Update echoes the OLD throughput with IndexStatus=UPDATING while a Create echoes the new entry, so the UpdateTable response must not be persisted as observed state - re-Describe 2-4 s later ([DDB-TABLE-165](#ddb-table-165)).

### Where to look next
- Table-level BillingMode/PT/Warm/OnDemand rules and the LimitExceededException catalogue for index/throughput operations ([DDB-TABLE-171](table-throughput-billing.md#ddb-table-171), table-throughput-billing.md); the UpdateTable single-concern matrix ([DDB-TABLE-433](table-streams-encryption-class.md#ddb-table-433), table-streams-encryption-class.md); the ThrottlingException and HTTP 500 catalogues ([DDB-TABLE-445](service.md#ddb-table-445), [DDB-TABLE-448](service.md#ddb-table-448), [DDB-TABLE-456](service.md#ddb-table-456), service.md); Contributor Insights is per table AND per GSI ([DDB-TABLE-094](table-subresources.md#ddb-table-094), table-subresources.md); GSI fan-out and autoscaling prerequisites on global tables (table-replicas.md).
- Evidence: services/dynamodb/probes/table/{weird-inputs,round-trip,state-machine,mutation-matrix,limits,creative}/ (gsi-*, index-* probes).

Entries below are generated from the lab findings; low-impact items are in the appendix, long notes under details/.
<!-- preserved:end -->

## At a glance

- canonical findings: 48 (high 21 / medium 20 / low 7); duplicates folded into the appendix: 2
- handling: handled 21 · partial 2 · tracked 0 · unhandled 23 · suspect-bug 1 · n-a 1 (tracked = handled/partial whose reference is an open GitHub issue; counted as not handled)
- re-verified: 1 · last_verified: 2026-10-08..2026-10-09 · model: 2012-08-10 (service/dynamodb v1.39.8)
- categories: request-validation 12, async-state-machine 7, other 6, quota-limit 4, update-granularity 4,
  error-code 2, normalization 2, requested-vs-effective 2, stale-response 2, delete-semantics 1,
  first-sync-destructive 1, idempotency 1, immutable-field 1, response-fidelity 1, server-default 1,
  shape-mismatch 1

## Operations

| operation | kind | required inputs | declared error shapes | paginated |
| --- | --- | --- | --- | --- |
| CreateTable | create | TableName | ResourceInUseException, LimitExceededException, InternalServerError | no |
| DeleteTable | delete | TableName | ResourceInUseException, ResourceNotFoundException, LimitExceededException, InternalServerError | no |
| DescribeLimits | read | - | InternalServerError | no |
| DescribeTable | read | TableName | ResourceNotFoundException, InternalServerError | no |
| RestoreTableFromBackup | create | TargetTableName, BackupArn | TableAlreadyExistsException, TableInUseException, BackupNotFoundException, BackupInUseException, LimitExceededException, InternalServerError | no |
| TagResource | tag | ResourceArn, Tags | LimitExceededException, ResourceNotFoundException, InternalServerError, ResourceInUseException | no |
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

- <a id="ddb-table-135"></a>**DDB-TABLE-135** `async-state-machine` · impact medium · handled · verified 2026-10-08
  **GSI OnDemandThroughput is independent of the table cap and only flips IndexStatus (not TableStatus) to UPDATING**
  On a PAY_PER_REQUEST table with no table-level OnDemandThroughput, a GSI created with OnDemandThroughput
  {50,50} reports it while Table.OnDemandThroughput stays absent; UpdateTable
  GlobalSecondaryIndexUpdates[Update OnDemandThroughput {5000,5000}] (above a table cap of 1000) returns 200
  with IndexStatus=UPDATING and TableStatus=ACTIVE, DescribeTable already shows the new values, and the index
  is ACTIVE again after ~2 s. Re-sending the identical GSI OnDemandThroughput returns 200 (no 'will not
  change' error). A second GSI OnDemandThroughput update issued while the first is in flight fails with
  ResourceInUseException 'Index is being updated. Table: X Index: idx-c'. Table-level OnDemandThroughput
  changes are synchronous: the UpdateTable response shows the new values with TableStatus=ACTIVE.
  - ACK: synced.when, requeue · ops: UpdateTable, DescribeTable · fields:
    GlobalSecondaryIndexUpdates.Update.OnDemandThroughput, OnDemandThroughput,
    GlobalSecondaryIndexes.IndexStatus
  - repro: PAY_PER_REQUEST table w/ GSI; UpdateTable GSI Update OnDemandThroughput {5000,5000}; DescribeTable;
    repeat immediately
  - measurements: gsi_odt_updating_s=2.0
  - handling: handled via `pkg/resource/table/hooks_global_secondary_indexes.go:30-39; templates/hooks/table/sdk_read_one_post_set_output.go.tpl:1-28; pkg/resource/table/hooks_global_secondary_indexes.go:84-131; pkg/resource/table/hooks_global_secondary_indexes.go:246-258; pkg/resource/table/hooks.go:475-495; pkg/resource/table/hooks_global_secondary_indexes.go:401-414`
  - related: [DDB-TABLE-150](#ddb-table-150), [DDB-TABLE-376](#ddb-table-376), [DDB-TABLE-159](#ddb-table-159), [DDB-TABLE-163](table-subresources.md#ddb-table-163), [DDB-TABLE-170](#ddb-table-170), [DDB-TABLE-154](table-throughput-billing.md#ddb-table-154),
    [DDB-TABLE-456](service.md#ddb-table-456), [DDB-TABLE-128](#ddb-table-128), [DDB-TABLE-153](#ddb-table-153), [DDB-TABLE-375](#ddb-table-375), [DDB-TABLE-458](#ddb-table-458), [DDB-TABLE-286](table-streams-encryption-class.md#ddb-table-286), [DDB-TABLE-164](#ddb-table-164),
    [DDB-TABLE-155](#ddb-table-155) · evidence: table/round-trip/gsi-lsi-describe
  - notes: Confirms H-T-122 (no cross-validation against the table cap) and the synchronous table-level part
    of H-T-123.

- <a id="ddb-table-148"></a>**DDB-TABLE-148** `async-state-machine` · impact high · handled · verified 2026-10-08
  **GSI added via UpdateTable: TableStatus UPDATING only ~25-55s (resource allocation), then ACTIVE while the index backfills 7-16 min**
  After UpdateTable GlobalSecondaryIndexUpdates[Create] on an EMPTY table the response shows
  TableStatus=UPDATING and the new index IndexStatus=CREATING, Backfilling=false. That state lasts 24-55 s
  (resource allocation); then TableStatus returns to ACTIVE while the index stays CREATING with
  Backfilling=true for the remaining 7-16 minutes (measured 537 s and 507 s on a PROVISIONED table, ~16.5 min
  on a PAY_PER_REQUEST table). CreateTable with GSIs behaves differently: TableStatus=CREATING with all
  indexes CREATING, and everything flips to ACTIVE together after 14-20 s. A GSI delete makes
  TableStatus=UPDATING / IndexStatus=DELETING for 5-6 s; a GSI throughput Update makes only
  IndexStatus=UPDATING (~2 s) with TableStatus=ACTIVE.
  - ACK: synced.when, requeue, e2e-timing · ops: UpdateTable, DescribeTable, CreateTable · fields:
    TableStatus, GlobalSecondaryIndexes.IndexStatus, GlobalSecondaryIndexes.Backfilling
  - repro: ACTIVE empty table; UpdateTable Create GSI; DescribeTable every 1s until IndexStatus=ACTIVE
  - measurements: resource_allocation_s_min=24.3, resource_allocation_s_max=55.0,
    gsi_create_total_s_provisioned=537.5, gsi_create_total_s_ppr_approx=990, create_table_with_2_gsis_s=16.2,
    gsi_delete_s=5.1, gsi_throughput_update_s=2.0
  - handling: handled via `pkg/resource/table/hooks_global_secondary_indexes.go:30-39; templates/hooks/table/sdk_read_one_post_set_output.go.tpl:1-28; test/e2e/tests/test_table.py:724-729; test/e2e/tests/test_table.py:864-876`
  - related: [DDB-TABLE-138](#ddb-table-138), [DDB-TABLE-149](#ddb-table-149), [DDB-TABLE-163](table-subresources.md#ddb-table-163), [DDB-TABLE-458](#ddb-table-458), [DDB-TABLE-116](table-subresources.md#ddb-table-116), [DDB-TABLE-165](#ddb-table-165),
    [DDB-TABLE-168](#ddb-table-168), [DDB-TABLE-169](#ddb-table-169), [DDB-TABLE-123](#ddb-table-123), [DDB-TABLE-134](#ddb-table-134) · evidence: table/state-machine/gsi-lifecycle
  - notes: Confirms H-T-003 (a reconciler gating on TableStatus alone sees an idle table for ~90% of a GSI
    build) and H-T-014's 'all indexes ACTIVE when TableStatus flips' for CreateTable. Readiness = TableStatus
    ACTIVE AND every IndexStatus ACTIVE.
  - full notes: [details/DDB-TABLE-148.md](details/DDB-TABLE-148.md)

- <a id="ddb-table-149"></a>**DDB-TABLE-149** `async-state-machine` · impact medium · unhandled (not handled in controller) · verified 2026-10-08
  **Deleting a GSI that is CREATING is rejected (ResourceInUseException) during resource allocation and accepted once Backfilling=true**
  While IndexStatus=CREATING and Backfilling=false, UpdateTable GlobalSecondaryIndexUpdates[Delete] fails with
  ResourceInUseException 'Attempt to change a resource which is still in use: Index creation is in resource
  allocation phase. Retry deletion during backfilling phase or when the index is active. Table: X Index: gsi3'
  (28 attempts over 53 s). The first attempt after Backfilling flipped to true (t=55 s, TableStatus already
  ACTIVE) succeeded; the response showed TableStatus=UPDATING and IndexStatus=DELETING, and the index entry
  disappeared 5-6 s later. The attribute used only by that index is pruned from AttributeDefinitions once the
  index is gone.
  - ACK: requeue, deletable.when · ops: UpdateTable, DescribeTable · fields:
    GlobalSecondaryIndexUpdates.Delete, GlobalSecondaryIndexes.Backfilling
  - repro: UpdateTable Create gsi3; loop: UpdateTable Delete gsi3 every 2s until 200
  - measurements: rejected_window_s=55, delete_to_gone_s=5.1
  - handling: not handled in the controller (as of commit 34b85e6)
  - related: [DDB-TABLE-148](#ddb-table-148), [DDB-TABLE-138](#ddb-table-138), [DDB-TABLE-163](table-subresources.md#ddb-table-163), [DDB-TABLE-458](#ddb-table-458), [DDB-TABLE-116](table-subresources.md#ddb-table-116), [DDB-TABLE-165](#ddb-table-165),
    [DDB-TABLE-151](#ddb-table-151), [DDB-TABLE-166](#ddb-table-166) · evidence: table/state-machine/gsi-lifecycle
  - notes: Confirms H-T-142. A spec rollback of a just-added GSI must requeue on ResourceInUseException until
    Backfilling=true.

- <a id="ddb-table-155"></a>**DDB-TABLE-155** `async-state-machine` · impact medium · handled · verified 2026-10-08
  **GSI WarmThroughput: decrease rejected, same value accepted, increase keeps IndexStatus ACTIVE but Status UPDATING ~6.5 min**
  GlobalSecondaryIndexUpdates[Update WarmThroughput {100,100}] on a 12000/4000 GSI -> ValidationException
  'Requested ReadUnitsPerSecond for WarmThroughput for index gsi1 is lower than current WarmThroughput,
  decreasing WarmThroughput is not supported' (table variant: '...for table is lower than current
  WarmThroughput...'). Re-sending the current 12000/4000 -> 200 (no-op). {12001,4001} -> 200; the response
  still shows the old 12000/4000 with Status=UPDATING, IndexStatus stays ACTIVE and TableStatus ACTIVE;
  DescribeTable shows Status=UPDATING for 388 s before the new values appear with Status=ACTIVE. A read-only
  increase {ReadUnitsPerSecond: 12002} keeps the write value. Re-sending the identical warm value after it is
  applied -> 200.
  - ACK: synced.when, requeue, terminal_codes · ops: UpdateTable, DescribeTable · fields:
    GlobalSecondaryIndexUpdates.Update.WarmThroughput, GlobalSecondaryIndexes.WarmThroughput.Status,
    WarmThroughput
  - repro: PPR table with GSI; UpdateTable GSI Update WarmThroughput {12001,4001}; DescribeTable every 1s
  - measurements: gsi_warm_status_updating_s=388.4
  - handling: handled via `pkg/resource/table/hooks_global_secondary_indexes.go:84-131; pkg/resource/table/hooks_global_secondary_indexes.go:246-258; generator.yaml:1-6; generator.yaml:15`
  - related: [DDB-TABLE-134](#ddb-table-134), [DDB-TABLE-138](#ddb-table-138), [DDB-TABLE-153](#ddb-table-153), [DDB-TABLE-128](#ddb-table-128), [DDB-TABLE-154](table-throughput-billing.md#ddb-table-154), [DDB-TABLE-164](#ddb-table-164),
    [DDB-TABLE-135](#ddb-table-135), [DDB-TABLE-375](#ddb-table-375), [DDB-TABLE-286](table-streams-encryption-class.md#ddb-table-286), [DDB-TABLE-165](#ddb-table-165), [DDB-TABLE-361](table-streams-encryption-class.md#ddb-table-361), [DDB-TABLE-462](#ddb-table-462), [DDB-TABLE-458](#ddb-table-458) ·
    evidence: table/mutation-matrix/gsi-billing-throughput
  - notes: Confirms H-T-038 (GSI path via GlobalSecondaryIndexUpdates) and the independent
    WarmThroughput.Status of H-T-070; readiness must include WarmThroughput.Status when the spec sets warm
    throughput.

- <a id="ddb-table-175"></a>**DDB-TABLE-175** `async-state-machine` · impact medium · unhandled (not handled in controller) · verified 2026-10-08
  **GSI Create admitted during SSE/DeletionProtection/Warm updates, ResourceInUse while IOPS, stream or TableClass changes run**
  Immediately after an accepted stand-alone change, UpdateTable GlobalSecondaryIndexUpdates[Create] returned:
  after ProvisionedThroughput -> ResourceInUseException "Can't create or delete indexes while table IOPS are
  being updated. Table: X"; after StreamSpecification -> "Can't create or delete an index when stream status
  is being updated."; after TableClass -> "Can't create or delete an index when a table class update is in
  progress."; after SSESpecification (SSEDescription.Status=UPDATING), DeletionProtectionEnabled and
  WarmThroughput -> 200 (index created while the other change completed). The TableClass change alone briefly
  showed the existing GSI as IndexStatus=UPDATING (~5 s) with TableStatus=ACTIVE; a GSI Delete alone showed
  TableStatus=UPDATING for ~5 s.
  - ACK: updateable.when, requeue · ops: UpdateTable, DescribeTable · fields:
    GlobalSecondaryIndexUpdates.Create, TableStatus, SSEDescription.Status, TableClassSummary
  - repro: UpdateTable TableClass=STANDARD_INFREQUENT_ACCESS; immediately UpdateTable
    GlobalSecondaryIndexUpdates=[Create gsi2]
  - measurements: table_class_gsi_updating_s=5.1, stream_enable_table_updating_s=5.1,
    gsi_delete_table_updating_s=5.1
  - handling: not handled in the controller (as of commit 34b85e6)
  - related: [DDB-TABLE-174](#ddb-table-174), [DDB-TABLE-382](table-streams-encryption-class.md#ddb-table-382), [DDB-TABLE-163](table-subresources.md#ddb-table-163), [DDB-TABLE-286](table-streams-encryption-class.md#ddb-table-286), [DDB-TABLE-458](#ddb-table-458), [DDB-TABLE-462](#ddb-table-462),
    [DDB-TABLE-287](table-streams-encryption-class.md#ddb-table-287), [DDB-TABLE-450](table-streams-encryption-class.md#ddb-table-450) · evidence: table/mutation-matrix/gsi-combined-updates
  - notes: ResourceInUseException here is a pure 'retry later' signal; the three message variants name the
    blocking operation.

- <a id="ddb-table-375"></a>**DDB-TABLE-375** `async-state-machine` · impact high · handled · verified 2026-10-09
  **GSI DELETING on a PPR table (~2.6 s): BillingMode=PROVISIONED (+/- dying index) -> ResourceInUse IOPS/'Index is being deleted'; ODT refused**
  PAY_PER_REQUEST table with GSI gsi1: UpdateTable Delete gsi1 -> 200 OK (TableStatus=UPDATING,
  gsi1=DELETING). Fired right after: BillingMode=PROVISIONED + table PT 1/1 (no gsi1 entry) ->
  ResourceInUseException (HTTP 400) 'Attempt to change a resource which is still in use: Can't change table
  IOPS when an index is being deleted. Table: ackq-fbb48c-gsiracep Indexes: [gsi1]' @+0.07s [gsi1 was
  DELETING]; the same + GlobalSecondaryIndexUpdates[Update gsi1 1/1] -> ResourceInUseException (HTTP 400)
  'Attempt to change a resource which is still in use: Index is being deleted. Table: ackq-fbb48c-gsiracep
  Index: gsi1' @+0.09s [gsi1 was DELETING]; BillingMode=PAY_PER_REQUEST re-send -> 200 OK
  (TableStatus=UPDATING, gsi1=ACTIVE) @+0.12s [gsi1 was DELETING]; OnDemandThroughput change ->
  ResourceInUseException (HTTP 400) 'Attempt to change a resource which is still in use: OnDemandThroughput
  cannot be updated while index deletion is in progress for indexes: [gsi1]' @+0.15s [gsi1 was DELETING].
  Timeline (TableStatus, gsi1 IndexStatus) at 0.5 s: [(('UPDATING', 'DELETING'), 2.56), (('ACTIVE', 'GONE'),
  None)] (gsi1 DELETING for 2.6 s). Once gsi1 is gone: BillingMode=PROVISIONED + PT 1/1 -> 200 OK
  (TableStatus=UPDATING, gsi1=None) [gsi1 was GONE] (settle [(('UPDATING', 'GONE'), 87.09), (('ACTIVE',
  'GONE'), None)]); Update of the vanished gsi1 -> ResourceNotFoundException (HTTP 400) 'Requested resource
  not found: Index gsi1 for table ackq-fbb48c-gsiracep' [gsi1 was GONE].
  - ACK: updateable.when, requeue, custom_update, one-per-reconcile · ops: UpdateTable, DescribeTable ·
    fields: BillingMode, ProvisionedThroughput, GlobalSecondaryIndexUpdates
  - repro: PPR table + GSI gsi1 ACTIVE: UpdateTable(Delete gsi1); immediately
    UpdateTable(BillingMode=PROVISIONED, PT 1/1) and the same with Update gsi1; repeat after gsi1 disappears
  - measurements: t1_gsi_deleting_s=2.6
  - handling: handled via `pkg/resource/table/hooks.go:226-238; pkg/resource/table/hooks_global_secondary_indexes.go:202-217`
  - related: [DDB-TABLE-152](#ddb-table-152), [DDB-TABLE-163](table-subresources.md#ddb-table-163), [DDB-TABLE-149](#ddb-table-149), [DDB-TABLE-376](#ddb-table-376), [DDB-TABLE-462](#ddb-table-462), [DDB-TABLE-153](#ddb-table-153),
    [DDB-TABLE-164](#ddb-table-164), [DDB-TABLE-128](#ddb-table-128), [DDB-TABLE-135](#ddb-table-135), [DDB-TABLE-154](table-throughput-billing.md#ddb-table-154), [DDB-TABLE-456](service.md#ddb-table-456), [DDB-TABLE-458](#ddb-table-458), [DDB-TABLE-286](table-streams-encryption-class.md#ddb-table-286),
    [DDB-TABLE-155](#ddb-table-155), [DDB-TABLE-165](#ddb-table-165), [DDB-TABLE-361](table-streams-encryption-class.md#ddb-table-361) · hypotheses: H-T-023 · evidence:
    table/state-machine/gsi-delete-billing-race
  - notes: Qualifies H-T-023 / [DDB-TABLE-152](#ddb-table-152): while the index is still listed as DELETING the state check wins
    over the per-index throughput validation - the controller gets ResourceInUseException ("Can't change table
    IOPS when an index is being deleted ... Indexes: [gsi1]"), never 'ProvisionedThroughput must...
  - full notes: [details/DDB-TABLE-375.md](details/DDB-TABLE-375.md)

- <a id="ddb-table-376"></a>**DDB-TABLE-376** `async-state-machine` · impact high · handled · verified 2026-10-09
  **GSI DELETING on a PROVISIONED table (~4.6 s): billing switch / table PT -> ResourceInUse (IOPS); DeleteTable refused; DP admitted**
  PROVISIONED 1/1 table with GSI gsi1 1/1: UpdateTable Delete gsi1 -> 200 OK (TableStatus=UPDATING,
  gsi1=DELETING). Fired right after: BillingMode=PAY_PER_REQUEST -> ResourceInUseException (HTTP 400) 'Attempt
  to change a resource which is still in use: Can't change table IOPS when an index is being deleted. Table:
  ackq-fbb48c-gsiracev Indexes: [gsi1]' @+0.06s [gsi1 was DELETING]; table PT 2/2 -> ResourceInUseException
  (HTTP 400) 'Attempt to change a resource which is still in use: Can't change table IOPS when an index is
  being deleted. Table: ackq-fbb48c-gsiracev Indexes: [gsi1]' @+0.09s [gsi1 was DELETING];
  BillingMode=PROVISIONED + PT 2/2 (no gsi1 entry) -> ResourceInUseException (HTTP 400) 'Attempt to change a
  resource which is still in use: Can't change table IOPS when an index is being deleted. Table:
  ackq-fbb48c-gsiracev Indexes: [gsi1]' @+0.11s [gsi1 was DELETING]; PT 2/2 + Update gsi1 2/2 ->
  ResourceInUseException (HTTP 400) 'Attempt to change a resource which is still in use: Index is being
  deleted. Table: ackq-fbb48c-gsiracev Index: gsi1' @+0.14s [gsi1 was DELETING]; DeleteTable ->
  ResourceInUseException (HTTP 400) 'Attempt to change a resource which is still in use: Cannot delete table
  while indexes are being created, updated, or deleted.' @+0.16s [gsi1 was DELETING];
  DeletionProtectionEnabled=true -> 200 OK (TableStatus=UPDATING, gsi1=DELETING) @+0.19s [gsi1 was DELETING].
  Timeline (TableStatus, gsi1 IndexStatus) at 0.5 s: [(('UPDATING', 'DELETING'), 4.62), (('ACTIVE', 'GONE'),
  None)]. Once gsi1 is gone: PT 2/2 -> 200 OK (TableStatus=UPDATING, gsi1=None) [gsi1 was GONE] (settle
  [(('UPDATING', 'GONE'), 1.01), (('ACTIVE', 'GONE'), None)]); BillingMode=PAY_PER_REQUEST -> 200 OK
  (TableStatus=UPDATING, gsi1=None) [gsi1 was GONE] (settle [(('UPDATING', 'GONE'), 107.36), (('ACTIVE',
  'GONE'), None)]).
  - ACK: updateable.when, deletable.when, requeue, one-per-reconcile · ops: UpdateTable, DeleteTable,
    DescribeTable · fields: BillingMode, ProvisionedThroughput, GlobalSecondaryIndexUpdates,
    DeletionProtectionEnabled
  - repro: PROVISIONED table + GSI gsi1: UpdateTable(Delete gsi1); immediately
    UpdateTable(BillingMode=PAY_PER_REQUEST) / PT 2/2 / DeleteTable / DP=true; repeat after gsi1 disappears
  - measurements: t2_gsi_deleting_s=4.6
  - handling: handled via `pkg/resource/table/hooks.go:226-238; pkg/resource/table/hooks_global_secondary_indexes.go:202-217`
  - related: [DDB-TABLE-163](table-subresources.md#ddb-table-163), [DDB-TABLE-150](#ddb-table-150), [DDB-TABLE-152](#ddb-table-152), [DDB-TABLE-159](#ddb-table-159), [DDB-TABLE-135](#ddb-table-135), [DDB-TABLE-375](#ddb-table-375),
    [DDB-TABLE-462](#ddb-table-462), [DDB-TABLE-166](#ddb-table-166), [DDB-TABLE-458](#ddb-table-458), [DDB-TABLE-286](table-streams-encryption-class.md#ddb-table-286), [DDB-TABLE-153](#ddb-table-153), [DDB-TABLE-164](#ddb-table-164), [DDB-TABLE-128](#ddb-table-128) ·
    hypotheses: H-T-023 · evidence: table/state-machine/gsi-delete-billing-race
  - notes: A GSI delete occupies the table for the whole IndexStatus=DELETING window (TableStatus=UPDATING for
    the same 4.6 s here). Table-level IOPS changes (PT, billing switch) and DeleteTable are
    ResourceInUseException with index-specific messages; DeletionProtectionEnabled is admitted. Right after
    the...
  - full notes: [details/DDB-TABLE-376.md](details/DDB-TABLE-376.md)

## Idempotency

- <a id="ddb-table-164"></a>**DDB-TABLE-164** `idempotency` · impact high · SUSPECTED CONTROLLER BUG · verified 2026-10-08
  **Re-sending identical ProvisionedThroughput (table or GSI) is a ValidationException; identical DeletionProtection and partial PT changes pass**
  UpdateTable GlobalSecondaryIndexUpdates[Update gsi1 2/2] when gsi1 is already 2/2 -> ValidationException
  'The provisioned throughput for the index gsi1 will not change. The requested value equals the current
  value. Current ReadCapacityUnits provisioned for index gsi1: 2. Requested ReadCapacityUnits: 2. Current
  WriteCapacityUnits ...'. Table ProvisionedThroughput identical -> 'The provisioned throughput for the table
  will not change. ...'. BillingMode=PROVISIONED alone on a PROVISIONED table -> 'ProvisionedThroughput must
  be specified when BillingMode is PROVISIONED'; BillingMode=PROVISIONED + identical throughput -> the 'will
  not change' error. Changing only WriteCapacityUnits (2/2 -> 2/3) is accepted. DeletionProtectionEnabled=true
  re-sent on a protected table -> 200.
  - ACK: custom_update, compare.is_ignored+delta_pre_compare · ops: UpdateTable · fields:
    ProvisionedThroughput, GlobalSecondaryIndexUpdates.Update.ProvisionedThroughput, BillingMode,
    DeletionProtectionEnabled
  - repro: PROVISIONED table 1/1 with gsi1 2/2; UpdateTable GlobalSecondaryIndexUpdates=[Update gsi1 2/2]
  - handling: suspected controller bug - see Handling gaps
  - related: [DDB-TABLE-058](table-throughput-billing.md#ddb-table-058), [DDB-TABLE-060](table-throughput-billing.md#ddb-table-060), [DDB-TABLE-370](table-throughput-billing.md#ddb-table-370), [DDB-TABLE-057](table-throughput-billing.md#ddb-table-057), [DDB-TABLE-061](table-throughput-billing.md#ddb-table-061), [DDB-TABLE-063](table-throughput-billing.md#ddb-table-063),
    [DDB-TABLE-152](#ddb-table-152), [DDB-TABLE-153](#ddb-table-153), [DDB-TABLE-375](#ddb-table-375), [DDB-TABLE-376](#ddb-table-376), [DDB-TABLE-128](#ddb-table-128), [DDB-TABLE-135](#ddb-table-135), [DDB-TABLE-154](table-throughput-billing.md#ddb-table-154),
    [DDB-TABLE-155](#ddb-table-155), [DDB-TABLE-286](table-streams-encryption-class.md#ddb-table-286) · evidence: table/mutation-matrix/gsi-update-granularity
  - notes: Confirms H-T-030; the controller must diff per index before emitting Update actions.
  - full notes: [details/DDB-TABLE-164.md](details/DDB-TABLE-164.md)

## Errors

- <a id="ddb-table-169"></a>**DDB-TABLE-169** `error-code` · impact medium · unhandled (not handled in controller) · verified 2026-10-08
  **21st GSI: ValidationException at CreateTable but LimitExceededException via UpdateTable; a 20-GSI table creates in ~20s**
  CreateTable with 21 GSIs -> ValidationException 'One or more parameter values were invalid:
  GlobalSecondaryIndex count exceeds the per-table limit of 20' (12 ms). CreateTable with 20 GSIs succeeds
  (all 20 indexes ACTIVE after 20-22 s). UpdateTable GlobalSecondaryIndexUpdates[Create] of a 21st index ->
  LimitExceededException 'Subscriber limit exceeded: Number of global secondary indexes exceeds per-table
  limit of 20' (1.8 s); the table stays ACTIVE.
  - ACK: terminal_codes · ops: CreateTable, UpdateTable · fields: GlobalSecondaryIndexes,
    GlobalSecondaryIndexUpdates.Create
  - repro: CreateTable with 20 GSIs; wait ACTIVE; UpdateTable Create g20
  - measurements: create_20_gsis_s=22.3
  - handling: not handled in the controller (as of commit 34b85e6)
  - related: [DDB-TABLE-168](#ddb-table-168), [DDB-TABLE-148](#ddb-table-148), [DDB-TABLE-123](#ddb-table-123), [DDB-TABLE-116](table-subresources.md#ddb-table-116), [DDB-TABLE-134](#ddb-table-134), [DDB-TABLE-171](table-throughput-billing.md#ddb-table-171),
    [DDB-TABLE-159](#ddb-table-159), [DDB-TABLE-157](#ddb-table-157), [DDB-TABLE-154](table-throughput-billing.md#ddb-table-154), [DDB-TABLE-170](#ddb-table-170), [DDB-TABLE-129](#ddb-table-129), [DDB-TABLE-043](#ddb-table-043), [DDB-TABLE-366](table.md#ddb-table-366) ·
    evidence: table/limits/gsi-concurrency-quotas
  - notes: Confirms H-T-049. This LimitExceededException is terminal (quota), unlike the 'Only 1 online
    index...' variant.

- <a id="ddb-table-458"></a>**DDB-TABLE-458** `error-code` · impact medium · unhandled (not handled in controller) · verified 2026-10-09
  **GSI-add resource allocation (UPDATING 27-56 s): SSE/DP/tag admitted, TableStatus not reset; WarmThroughput -> HTTP 500 'system maintenance'**
  1-item PAY_PER_REQUEST tables, UpdateTable(GSI Create) at t0 (response TableStatus=UPDATING,
  IndexStatus=CREATING, Backfilling=false), one write at +0.1 s, UpdateTable(StreamSpecification enable) at
  +1.0 s, DescribeTable every 0.25 s. Writes: SSESpecification KMS -> 200 with TableStatus=ACTIVE in its own
  response but DescribeTable kept UPDATING (no clobber; SSEDescription ENABLED at +22 s while still UPDATING);
  DeletionProtection -> 200 (response UPDATING); TagResource -> 200; OnDemandThroughput ->
  ResourceInUseException 'OnDemandThroughput cannot be updated while index creation is in progress for
  indexes: [gsi1]'; WarmThroughput 13000/5000 -> InternalServerError (HTTP 500) 'Table is under system
  maintenance, please try again later'. The stream enable at +1.0 s was ResourceInUseException "Can't change
  stream status when an index is being created" in all 6 cells. TableStatus returned to ACTIVE after 27-56 s
  with IndexStatus CREATING/Backfilling=true; the index became ACTIVE after 510-539 s in four cells and after
  1001-1003 s in the two cells that had received the SSE change or the (rejected) WarmThroughput call.
  - ACK: terminal_codes, requeue, synced.when · ops: UpdateTable, TagResource, DescribeTable · fields:
    GlobalSecondaryIndexUpdates, WarmThroughput, SSESpecification, OnDemandThroughput, StreamSpecification,
    TableStatus
  - repro: UpdateTable(GSI Create) on a 1-item table; UpdateTable(WarmThroughput 13000/5000) 0.1 s later ->
    HTTP 500; UpdateTable(SSESpecification) -> 200 but TableStatus stays UPDATING
  - measurements: updating_s=[27.16, 27.18, 28.16, 38.07, 42.31, 56.25], index_active_s=[510.59, 512.39,
    524.71, 538.62, 1001.31, 1003.3]
  - handling: not handled in the controller (as of commit 34b85e6)
  - related: [DDB-TABLE-148](#ddb-table-148), [DDB-TABLE-163](table-subresources.md#ddb-table-163), [DDB-TABLE-175](#ddb-table-175), [DDB-TABLE-450](table-streams-encryption-class.md#ddb-table-450), [DDB-TABLE-161](table-subresources.md#ddb-table-161), [DDB-TABLE-138](#ddb-table-138),
    [DDB-TABLE-149](#ddb-table-149), [DDB-TABLE-116](table-subresources.md#ddb-table-116), [DDB-TABLE-165](#ddb-table-165), [DDB-TABLE-166](#ddb-table-166), [DDB-TABLE-376](#ddb-table-376), [DDB-TABLE-286](table-streams-encryption-class.md#ddb-table-286), [DDB-TABLE-462](#ddb-table-462),
    [DDB-TABLE-287](table-streams-encryption-class.md#ddb-table-287), [DDB-TABLE-448](service.md#ddb-table-448), [DDB-TABLE-456](service.md#ddb-table-456), [DDB-TABLE-135](#ddb-table-135), [DDB-TABLE-154](table-throughput-billing.md#ddb-table-154), [DDB-TABLE-128](#ddb-table-128), [DDB-TABLE-153](#ddb-table-153),
    [DDB-TABLE-375](#ddb-table-375), [DDB-TABLE-361](table-streams-encryption-class.md#ddb-table-361), [DDB-TABLE-155](#ddb-table-155) · evidence: table/creative/clobber-gsi-warm
  - notes: The SSE path does not clobber the GSI job's UPDATING marker (it does clobber a TableClass switch,
    [DDB-TABLE-287](table-streams-encryption-class.md#ddb-table-287)/450). The HTTP 500 for a WarmThroughput change during index resource allocation is a state
    conflict reported as a server error; a controller treating 5xx as retryable will simply retry,...
  - full notes: [details/DDB-TABLE-458.md](details/DDB-TABLE-458.md)

## Request validation

- <a id="ddb-table-015"></a>**DDB-TABLE-015** `request-validation` · impact high · unhandled (not handled in controller) · verified 2026-10-08
  **UpdateTable with only TableName -> ValidationException 'At least one of ... is required'; GlobalSecondaryIndexUpdates=[] counts as absent**
  UpdateTable(TableName) on an ACTIVE table: ValidationException 'At least one of ProvisionedThroughput,
  BillingMode, UpdateStreamEnabled, GlobalSecondaryIndexUpdates, SSESpecification, ReplicaUpdates,
  MultiAccountReplicaReady, ReplicaTransitRoleArn, MultiRegionConsistency, DeletionProtectionEnabled,
  OnDemandThroughput, WarmThroughput or TableClass is required'. UpdateTable(TableName,
  GlobalSecondaryIndexUpdates=[]): ValidationException 'At least one of ProvisionedThroughput, BillingMode,
  UpdateStreamEnabled, GlobalSecondaryIndexUpdates, SSESpecification, ReplicaUpdates,
  MultiAccountReplicaReady, ReplicaTransitRoleArn, MultiRegionConsistency, DeletionProtectionEnabled,
  OnDemandThroughput, WarmThroughput or TableClass is required'. UpdateTable(TableName, ReplicaUpdates=[]):
  ParamValidationError 'Parameter validation failed:
  Invalid length for parameter ReplicaUpdates, value: 0, valid min length: 1'. On a missing table
  UpdateTable(TableName) -> ValidationException (existence vs validation order).
  - ACK: custom_update, compare.is_ignored+delta_pre_compare · ops: UpdateTable
  - repro: UpdateTable(TableName=<active table>) with no other fields
  - handling: not handled in the controller (as of commit 34b85e6)
  - related: [DDB-TABLE-160](#ddb-table-160), [DDB-TABLE-046](#ddb-table-046), [DDB-TABLE-047](table-throughput-billing.md#ddb-table-047), [DDB-TABLE-437](table-throughput-billing.md#ddb-table-437), [DDB-TABLE-449](service.md#ddb-table-449) · evidence:
    table/error-taxonomy/missing-table-noop-update-dp
  - notes: H-T-029 confirmed.
  - full notes: [details/DDB-TABLE-015.md](details/DDB-TABLE-015.md)

- <a id="ddb-table-043"></a>**DDB-TABLE-043** `request-validation` · impact medium · handled · verified 2026-10-08
  **Empty GlobalSecondaryIndexes / LocalSecondaryIndexes / Tags lists at CreateTable: ValidationException / ValidationException / accepted**
  GlobalSecondaryIndexes=[] -> ValidationException: 'One or more parameter values were invalid: List of
  GlobalSecondaryIndexes is empty'. LocalSecondaryIndexes=[] -> ValidationException: 'One or more parameter
  values were invalid: List of LocalSecondaryIndexes is empty'. Tags=[] -> OK.
  - ACK: custom_create, compare.nil_equals_zero_value · ops: CreateTable · fields: GlobalSecondaryIndexes,
    LocalSecondaryIndexes, Tags
  - repro: CreateTable PAY_PER_REQUEST with GlobalSecondaryIndexes=[] (and separately
    LocalSecondaryIndexes=[], Tags=[])
  - handling: handled via `pkg/resource/table/hooks.go:385-398; 5bbfe82`
  - related: [DDB-TABLE-129](#ddb-table-129), [DDB-TABLE-359](#ddb-table-359), [DDB-TABLE-456](service.md#ddb-table-456), [DDB-TABLE-126](#ddb-table-126), [DDB-TABLE-169](#ddb-table-169), [DDB-TABLE-366](table.md#ddb-table-366) ·
    evidence: table/weird-inputs/create-validation
  - notes: Relevant for Go controllers that serialize nil vs empty slices differently.

- <a id="ddb-table-123"></a>**DDB-TABLE-123** `request-validation` · impact high · unhandled (not handled in controller) · verified 2026-10-08
  **GSI KeySchema accepts up to 4 HASH + 4 RANGE attributes (multi-attribute keys); table and LSI keys stay at 2**
  CreateTable with a GSI KeySchema of [HASH a, HASH b], [HASH a, RANGE b, RANGE c] and even [4x HASH, 4x
  RANGE] (8 elements) succeeds and DescribeTable echoes the 8 elements in submitted order. 5 HASH ->
  ValidationException 'The KeySchema exceeds the maximum allowed number of HASH key attributes'; 5 RANGE ->
  '...maximum allowed number of RANGE key attributes'; [HASH, RANGE, HASH] -> 'All HASH key attributes must
  precede RANGE key attributes in the KeySchema'; duplicate attribute -> 'The KeySchema contains multiple key
  attributes with the same name'. The table KeySchema and an LSI KeySchema with 3 elements are rejected with
  'Member must have length less than or equal to 2'.
  - ACK: docs-only, custom_field · ops: CreateTable, DescribeTable · fields: GlobalSecondaryIndexes.KeySchema,
    KeySchema, LocalSecondaryIndexes.KeySchema
  - repro: CreateTable PAY_PER_REQUEST, AttributeDefinitions for all 10 attrs, GSI KeySchema [h1..h4 HASH,
    r1..r4 RANGE] -> 200, ACTIVE in 20s
  - measurements: create_active_s_8_element_gsi=20.2, create_active_s_3_element_gsi=16.2
  - handling: not handled in the controller (as of commit 34b85e6)
  - related: [DDB-TABLE-168](#ddb-table-168), [DDB-TABLE-148](#ddb-table-148), [DDB-TABLE-169](#ddb-table-169), [DDB-TABLE-116](table-subresources.md#ddb-table-116), [DDB-TABLE-134](#ddb-table-134), [DDB-TABLE-124](#ddb-table-124),
    [DDB-TABLE-125](#ddb-table-125), [DDB-TABLE-126](#ddb-table-126), [DDB-TABLE-127](#ddb-table-127), [DDB-TABLE-366](table.md#ddb-table-366) · evidence:
    table/weird-inputs/gsi-request-validation
  - notes: Refutes H-T-124: the API-reference sentence 'up to 4 partition keys and up to 4 sort keys' is
    enforced literally. A CRD validation of maxItems=2 / exactly one HASH on
    spec.globalSecondaryIndexes[].keySchema would reject valid tables; the KeySchema list type must stay
    open-ended for GSIs only.

- <a id="ddb-table-124"></a>**DDB-TABLE-124** `request-validation` · impact medium · handled · verified 2026-10-08
  **KeySchema element order is validated, not normalized: RANGE listed before HASH is rejected for table and GSI keys**
  CreateTable with table KeySchema [{sk RANGE},{pk HASH}] fails with ValidationException 'Invalid KeySchema:
  The first KeySchemaElement is not a HASH key type'; the same message is returned for a GSI KeySchema [{b
  RANGE},{a HASH}] and for a RANGE-only GSI key. Two HASH elements on the table key -> 'Invalid KeySchema: The
  second KeySchemaElement is not a RANGE key type'. DescribeTable returns KeySchema HASH first only because
  that is the only accepted input order.
  - ACK: terminal_codes · ops: CreateTable · fields: KeySchema, GlobalSecondaryIndexes.KeySchema
  - repro: CreateTable KeySchema=[{AttributeName:sk,KeyType:RANGE},{AttributeName:pk,KeyType:HASH}]
  - handling: handled via `generator.yaml:51-69; pkg/resource/table/hooks.go:621-663`
  - related: [DDB-TABLE-123](#ddb-table-123), [DDB-TABLE-125](#ddb-table-125), [DDB-TABLE-126](#ddb-table-126), [DDB-TABLE-127](#ddb-table-127), [DDB-TABLE-366](table.md#ddb-table-366) · evidence:
    table/weird-inputs/gsi-request-validation
  - notes: Partially refutes H-T-041 (KeySchema is not reordered by the service; the order is an input
    constraint).

- <a id="ddb-table-126"></a>**DDB-TABLE-126** `request-validation` · impact high · handled · verified 2026-10-08
  **Projection rules: INCLUDE needs 1-20 NonKeyAttributes, ALL/KEYS_ONLY forbid them, ProjectionType has no default**
  INCLUDE with NonKeyAttributes=[] -> ValidationException "Value '[]' at '...projection.nonKeyAttributes'
  failed to satisfy constraint: Member must have length greater than or equal to 1" (the SDK also rejects it
  client-side); INCLUDE without NonKeyAttributes -> 'ProjectionType is INCLUDE, but NonKeyAttributes is not
  specified'; KEYS_ONLY or ALL with NonKeyAttributes -> 'ProjectionType is KEYS_ONLY, but NonKeyAttributes is
  specified' (resp. ALL); duplicate -> 'Duplicate element in NonKeyAttributes: x'; 21 entries -> 'Member must
  have length less than or equal to 20'; Projection {} -> 'Unknown ProjectionType: null'; Projection omitted
  -> "Value null at '...projection' failed to satisfy constraint: Member must not be null"; lowercase 'all' ->
  enum error. NonKeyAttributes naming the table key or the index's own key attribute is accepted.
  - ACK: terminal_codes, compare.nil_equals_zero_value · ops: CreateTable · fields:
    GlobalSecondaryIndexes.Projection, LocalSecondaryIndexes.Projection, Projection.NonKeyAttributes,
    Projection.ProjectionType
  - repro: CreateTable with GSI Projection {ProjectionType: INCLUDE, NonKeyAttributes: []} (SDK validation
    disabled)
  - handling: handled via `pkg/resource/table/hooks_global_secondary_indexes.go:357-374; b52e89f; generator.yaml:88-90; pkg/resource/table/sdk.go:1234-1250`
  - related: [DDB-TABLE-043](#ddb-table-043), [DDB-TABLE-129](#ddb-table-129), [DDB-TABLE-359](#ddb-table-359), [DDB-TABLE-456](service.md#ddb-table-456), [DDB-TABLE-123](#ddb-table-123), [DDB-TABLE-124](#ddb-table-124),
    [DDB-TABLE-125](#ddb-table-125), [DDB-TABLE-127](#ddb-table-127), [DDB-TABLE-366](table.md#ddb-table-366) · evidence: table/weird-inputs/gsi-request-validation
  - notes: Confirms H-T-028 and the ProjectionType part of H-T-127. Codegen must omit NonKeyAttributes (nil,
    not []) unless INCLUDE.

- <a id="ddb-table-127"></a>**DDB-TABLE-127** `request-validation` · impact high · unhandled (not handled in controller) · verified 2026-10-08
  **CreateTable requires AttributeDefinitions to exactly equal the set of key attributes, and checks it before other GSI rules**
  An attribute defined but used by no key schema fails: without indexes 'Number of attributes in KeySchema
  does not exactly match number of attributes defined in AttributeDefinitions'; with indexes 'Some
  AttributeDefinitions are not used. AttributeDefinitions: [sk, a, b, pk, c], keys used: [a, sk, pk]'. A key
  attribute without a definition fails with 'Some index key attributes are not defined in
  AttributeDefinitions. Keys: [q], AttributeDefinitions: [sk, pk]'. The unused-attribute check runs before
  Projection/throughput/name checks, so an over-declared AttributeDefinitions masks every other validation
  error with the same message. Duplicate names -> 'Attribute Name is duplicated: pk' (also when the two
  entries differ in type).
  - ACK: terminal_codes, custom_create · ops: CreateTable · fields: AttributeDefinitions, KeySchema,
    GlobalSecondaryIndexes.KeySchema
  - repro: CreateTable AttributeDefinitions [pk, extra], KeySchema [pk HASH]
  - handling: not handled in the controller (as of commit 34b85e6)
  - related: [DDB-TABLE-433](table-streams-encryption-class.md#ddb-table-433), [DDB-TABLE-156](table-throughput-billing.md#ddb-table-156), [DDB-TABLE-358](table-throughput-billing.md#ddb-table-358), [DDB-TABLE-056](table-throughput-billing.md#ddb-table-056), [DDB-TABLE-174](#ddb-table-174), [DDB-TABLE-199](table-replicas.md#ddb-table-199),
    [DDB-TABLE-224](table-replicas.md#ddb-table-224), [DDB-TABLE-162](#ddb-table-162), [DDB-TABLE-123](#ddb-table-123), [DDB-TABLE-124](#ddb-table-124), [DDB-TABLE-125](#ddb-table-125), [DDB-TABLE-126](#ddb-table-126), [DDB-TABLE-366](table.md#ddb-table-366),
    [DDB-TABLE-151](#ddb-table-151), [DDB-TABLE-357](#ddb-table-357), [DDB-TABLE-359](#ddb-table-359) · evidence: table/weird-inputs/gsi-request-validation
  - notes: Create-side confirmation of H-T-020; the UpdateTable side behaves differently (see
    table/mutation-matrix/gsi-update-granularity).

- <a id="ddb-table-128"></a>**DDB-TABLE-128** `request-validation` · impact high · handled · verified 2026-10-08
  **CreateTable throughput blocks are gated by BillingMode at table and GSI level; zero capacity and OnDemandThroughput on PROVISIONED rejected**
  PAY_PER_REQUEST + table ProvisionedThroughput 1/1 -> 'Neither ReadCapacityUnits nor WriteCapacityUnits can
  be specified when BillingMode is PAY_PER_REQUEST'; + GSI ProvisionedThroughput -> 'ProvisionedThroughput
  should not be specified for index: g-a when BillingMode is PAY_PER_REQUEST'. Any 0 capacity (table or GSI,
  either mode) -> "Value '0' at '...readCapacityUnits' failed to satisfy constraint: Member must have value
  greater than or equal to 1" (the SDK rejects it client-side first). PROVISIONED GSI without throughput ->
  'ProvisionedThroughput is not specified for index: g-a'; PROVISIONED (or BillingMode omitted) without table
  throughput -> 'ReadCapacityUnits and WriteCapacityUnits must both be specified when BillingMode is
  PROVISIONED'. OnDemandThroughput on a PROVISIONED table -> 'MaxReadRequestUnits for OnDemandThroughput
  cannot be specified when table BillingMode is PROVISIONED.' (GSI variant: '...for OnDemandThroughput for
  index: g-a cannot be specified...'). GSI OnDemandThroughput 0 or -1 at create -> 'Requested
  MaxReadRequestUnits for OnDemandThroughput for index : g-a is outside of valid range'; GSI WarmThroughput
  below 12000/4000 -> 'Requested ReadUnitsPerSecond for WarmThroughput for index g-a is lower than initial
  throughput for OnDemand'. Read-only OnDemandThroughput or WarmThroughput on a GSI is accepted.
  - ACK: compare.nil_equals_zero_value, terminal_codes · ops: CreateTable · fields: BillingMode,
    ProvisionedThroughput, GlobalSecondaryIndexes.ProvisionedThroughput, OnDemandThroughput,
    GlobalSecondaryIndexes.OnDemandThroughput, GlobalSecondaryIndexes.WarmThroughput
  - repro: CreateTable BillingMode=PAY_PER_REQUEST with GSI ProvisionedThroughput {1,1}; CreateTable
    PROVISIONED with GSI OnDemandThroughput {10,10}
  - handling: handled via `pkg/resource/table/hooks.go:665-677; pkg/resource/table/hooks.go:357-376; pkg/resource/table/hooks_global_secondary_indexes.go:336-355; pkg/resource/table/hooks_global_secondary_indexes_test.go:13-76; pkg/resource/table/hooks.go:475-495; pkg/resource/table/hooks_global_secondary_indexes.go:401-414`
  - related: [DDB-TABLE-152](#ddb-table-152), [DDB-TABLE-153](#ddb-table-153), [DDB-TABLE-375](#ddb-table-375), [DDB-TABLE-376](#ddb-table-376), [DDB-TABLE-164](#ddb-table-164), [DDB-TABLE-456](service.md#ddb-table-456),
    [DDB-TABLE-133](#ddb-table-133), [DDB-TABLE-134](#ddb-table-134), [DDB-TABLE-138](#ddb-table-138), [DDB-TABLE-155](#ddb-table-155), [DDB-TABLE-154](table-throughput-billing.md#ddb-table-154), [DDB-TABLE-135](#ddb-table-135), [DDB-TABLE-458](#ddb-table-458),
    [DDB-TABLE-286](table-streams-encryption-class.md#ddb-table-286) · evidence: table/weird-inputs/gsi-request-validation
  - notes: Confirms H-T-021, H-T-022 and H-T-040 (create side). Go zero-valued structs must be treated as
    unset.

- <a id="ddb-table-129"></a>**DDB-TABLE-129** `request-validation` · impact medium · handled · verified 2026-10-08
  **Index count and shape limits at CreateTable are synchronous ValidationExceptions (21 GSIs, 6 LSIs, empty lists, LSI key rules)**
  21 GSIs -> ValidationException 'GlobalSecondaryIndex count exceeds the per-table limit of 20'; 6 LSIs ->
  'Number of LocalSecondaryIndexes exceeds per-table limit of 5'; GlobalSecondaryIndexes=[] -> 'List of
  GlobalSecondaryIndexes is empty' (same for LocalSecondaryIndexes). LSI on a table without a sort key ->
  'Table KeySchema does not have a range key, which is required when specifying a LocalSecondaryIndex'; LSI
  hash != table hash -> 'Index KeySchema does not have the same leading hash key as table KeySchema for index:
  l-a. index hash key: a, table hash key: pk'; LSI without RANGE -> 'Index KeySchema does not have a range key
  for index: l-a'. Duplicate names across GSIs or between a GSI and an LSI -> 'Duplicate index name: dup';
  IndexName < 3 chars -> length constraint; invalid chars -> 'Member must satisfy regular expression pattern:
  [a-zA-Z0-9_.-]+'. Accepted: an LSI whose RANGE equals the table sort key, a GSI identical to the table key,
  and a GSI named exactly like the table.
  - ACK: terminal_codes · ops: CreateTable · fields: GlobalSecondaryIndexes, LocalSecondaryIndexes, IndexName
  - repro: CreateTable PAY_PER_REQUEST with 21 single-attribute GSIs
  - handling: handled via `generator.yaml:66-69; pkg/resource/table/hooks.go:815-880`
  - related: [DDB-TABLE-043](#ddb-table-043), [DDB-TABLE-359](#ddb-table-359), [DDB-TABLE-456](service.md#ddb-table-456), [DDB-TABLE-126](#ddb-table-126), [DDB-TABLE-169](#ddb-table-169), [DDB-TABLE-366](table.md#ddb-table-366) ·
    evidence: table/weird-inputs/gsi-request-validation
  - notes: Create-side half of H-T-049 confirmed (ValidationException). An empty GSI list must be omitted, not
    sent.

- <a id="ddb-table-160"></a>**DDB-TABLE-160** `request-validation` · impact medium · handled · verified 2026-10-08
  **Per-index and per-entry action rules in GlobalSecondaryIndexUpdates are ValidationExceptions; empty list is rejected**
  [Delete gsi1, Create gsi1] (same name), [Update gsi2, Update gsi2] or [Delete gsi2, Update gsi2] ->
  ValidationException 'One or more parameter values were invalid: Only one global secondary index update per
  index is allowed simultaneously. Index: gsi1'. One entry carrying both Update and Delete -> 'Only one global
  secondary index action is allowed per GlobalSecondaryIndexUpdate object'; an empty entry {} -> 'One of
  GlobalSecondaryIndexUpdate.Update, GlobalSecondaryIndexUpdate.Create, GlobalSecondaryIndexUpdate.Delete must
  not be null'. GlobalSecondaryIndexUpdates=[] alone -> 'At least one of ProvisionedThroughput, BillingMode,
  UpdateStreamEnabled, GlobalSecondaryIndexUpdates, SSESpecification, ReplicaUpdates, ... is required' (same
  as an UpdateTable with no change); GlobalSecondaryIndexUpdates=[] together with DeletionProtectionEnabled ->
  'List of GlobalSecondaryIndexUpdates is empty'.
  - ACK: custom_update, terminal_codes · ops: UpdateTable · fields: GlobalSecondaryIndexUpdates
  - repro: UpdateTable GlobalSecondaryIndexUpdates=[] DeletionProtectionEnabled=true
  - handling: handled via `pkg/resource/table/hooks.go:220-304; test/e2e/tests/test_table.py:878-952`
  - related: [DDB-TABLE-015](#ddb-table-015), [DDB-TABLE-047](table-throughput-billing.md#ddb-table-047), [DDB-TABLE-437](table-throughput-billing.md#ddb-table-437), [DDB-TABLE-449](service.md#ddb-table-449) · evidence:
    table/mutation-matrix/gsi-update-granularity
  - notes: Confirms H-T-018 (never send an empty list) and the same-name Delete+Create part of H-T-067 (must
    be two calls, the second after the index entry is gone).
  - full notes: [details/DDB-TABLE-160.md](details/DDB-TABLE-160.md)

- <a id="ddb-table-162"></a>**DDB-TABLE-162** `request-validation` · impact high · handled · verified 2026-10-08
  **UpdateTable AttributeDefinitions is only used for GSI Create; unused, stale or type-conflicting entries are silently ignored elsewhere**
  GSI Create without AttributeDefinitions -> ValidationException 'AttributeDefinitions is not specified for
  index: gsi3'. AttributeDefinitions containing only the new key attribute is accepted (no need to resend
  existing ones). AttributeDefinitions with an extra unused attribute, with an attribute left over from a
  deleted index, or with the table key declared as N while it is S, sent together with a GSI Update, a GSI
  Delete, DeletionProtectionEnabled or a ProvisionedThroughput change, are all accepted (200) and
  DescribeTable AttributeDefinitions is unchanged afterwards. A GSI Create with a stale extra attribute in the
  list is also accepted. AttributeDefinitions as the only parameter -> the generic 'At least one of
  ProvisionedThroughput, BillingMode, ...' ValidationException.
  - ACK: custom_update, compare.is_ignored+delta_pre_compare · ops: UpdateTable · fields:
    AttributeDefinitions, GlobalSecondaryIndexUpdates.Create
  - repro: UpdateTable AttributeDefinitions=[{pk,N},{a,S},{b,S}] GlobalSecondaryIndexUpdates=[Update gsi1 PT
    3/3] on a table whose pk is S -> 200
  - handling: handled via `pkg/resource/table/hooks_global_secondary_indexes.go:175-178; pkg/resource/table/hooks_global_secondary_indexes.go:298-301`
  - related: [DDB-TABLE-433](table-streams-encryption-class.md#ddb-table-433), [DDB-TABLE-156](table-throughput-billing.md#ddb-table-156), [DDB-TABLE-358](table-throughput-billing.md#ddb-table-358), [DDB-TABLE-056](table-throughput-billing.md#ddb-table-056), [DDB-TABLE-174](#ddb-table-174), [DDB-TABLE-199](table-replicas.md#ddb-table-199),
    [DDB-TABLE-224](table-replicas.md#ddb-table-224), [DDB-TABLE-127](#ddb-table-127), [DDB-TABLE-151](#ddb-table-151), [DDB-TABLE-357](#ddb-table-357), [DDB-TABLE-359](#ddb-table-359) · evidence:
    table/mutation-matrix/gsi-update-granularity
  - notes: Confirms H-T-019 (must send the new attribute; only-new is enough). Refutes H-T-020 and H-T-128 for
    UpdateTable (CreateTable enforces the exact match, UpdateTable does not) and refutes H-T-066's expectation
    of a ValidationException: a changed key type passes through silently, so the controller...
  - full notes: [details/DDB-TABLE-162.md](details/DDB-TABLE-162.md)

- <a id="ddb-table-280"></a>**DDB-TABLE-280** `request-validation` · impact high · unhandled (not handled in controller) · verified 2026-10-09
  **Index overrides must match a source index by KeySchema+Projection but may RENAME it; throughput overrides follow the effective billing mode**
  GlobalSecondaryIndexOverride=[{IndexName:'gsi-new', same KeySchema/Projection as gsi1, 5/5}] and
  LocalSecondaryIndexOverride=[{IndexName:'lsi-new', same keys/projection as lsi1}] were accepted (200) and
  the restored tables carried indexes named gsi-new / lsi-new (the original names were gone). Overrides with a
  different KeySchema or Projection -> ValidationException 'Index <n> does not match a secondary index that
  existed in the source table and cannot be created during the restore operation'. A GSI override without
  ProvisionedThroughput on a PROVISIONED restore -> 'Must specify provisioned throughput for index gsi1'; with
  ProvisionedThroughput when BillingModeOverride=PAY_PER_REQUEST -> 'Cannot override ProvisionedThroughput if
  BillingMode is overridden to PAY_PER_REQUEST for index gsi1'; OnDemandThroughputOverride on a PROVISIONED
  backup -> 'Cannot override MaxReadRequestUnits for OnDemandThroughput unless BillingModeOverride is
  PAY_PER_REQUEST'; ProvisionedThroughputOverride 0/0 is rejected client-side (min 1).
  - ACK: custom_create, terminal_codes · ops: RestoreTableFromBackup · fields: GlobalSecondaryIndexOverride,
    LocalSecondaryIndexOverride, ProvisionedThroughputOverride, OnDemandThroughputOverride,
    BillingModeOverride
  - repro: RestoreTableFromBackup with each override variant on a PROVISIONED backup that has gsi1 (gk) and
    lsi1 (pk+lsk)
  - handling: not handled in the controller (as of commit 34b85e6)
  - related: [DDB-TABLE-273](table-restore.md#ddb-table-273), [DDB-TABLE-279](table-restore.md#ddb-table-279), [DDB-TABLE-281](#ddb-table-281), [DDB-BACKUP-021](backup.md#ddb-backup-021), [DDB-BACKUP-017](backup.md#ddb-backup-017) · hypotheses:
    H-B-138, H-B-104 · evidence: table/round-trip/restore-overrides
  - notes: Refines H-B-138: 'cannot create new indexes' is enforced on keys/projection, not names - a Table
    spec whose GSI names differ from the backup's is accepted and silently yields renamed indexes. H-B-104(c)
    confirmed (OnDemandThroughputOverride on a PROVISIONED backup is a synchronous...
  - full notes: [details/DDB-TABLE-280.md](details/DDB-TABLE-280.md)

- <a id="ddb-table-359"></a>**DDB-TABLE-359** `request-validation` · impact high · handled · verified 2026-10-09
  **GlobalSecondaryIndexUpdates=[]: alone (or with AttributeDefinitions) counts as absent; with any effective change -> 'List ... is empty'**
  UpdateTable(GlobalSecondaryIndexUpdates=[]) alone -> ValidationException (HTTP 400) 'At least one of
  ProvisionedThroughput, BillingMode, ... or TableClass is required'. With DeletionProtectionEnabled toggle ->
  ValidationException (HTTP 400) 'One or more parameter values were invalid: List of
  GlobalSecondaryIndexUpdates is empty'. With BillingMode=PAY_PER_REQUEST re-send -> ValidationException (HTTP 400)
  'One or more parameter values were invalid: List of GlobalSecondaryIndexUpdates is empty'. With
  AttributeDefinitions=[pk S] -> ValidationException (HTTP 400) 'At least one of ProvisionedThroughput,
  BillingMode, ... or TableClass is required'. With OnDemandThroughput -> ValidationException (HTTP 400) 'One
  or more parameter values were invalid: List of GlobalSecondaryIndexUpdates is empty'.
  DeletionProtectionEnabled before/after the DP-carrier call: True/True; OnDemandThroughput after the series:
  None.
  - ACK: custom_update · ops: UpdateTable · fields: GlobalSecondaryIndexUpdates
  - repro: UpdateTable(TableName, GlobalSecondaryIndexUpdates=[]); UpdateTable(TableName,
    GlobalSecondaryIndexUpdates=[], DeletionProtectionEnabled=true)
  - handling: handled via `pkg/resource/table/hooks.go:385-398; 5bbfe82`
  - related: [DDB-TABLE-015](#ddb-table-015), [DDB-TABLE-160](#ddb-table-160), [DDB-TABLE-043](#ddb-table-043), [DDB-TABLE-129](#ddb-table-129), [DDB-TABLE-456](service.md#ddb-table-456), [DDB-TABLE-126](#ddb-table-126),
    [DDB-TABLE-162](#ddb-table-162), [DDB-TABLE-127](#ddb-table-127), [DDB-TABLE-151](#ddb-table-151), [DDB-TABLE-357](#ddb-table-357) · hypotheses: H-T-018 · evidence:
    table/mutation-matrix/schema-immutability
  - notes: Resolves [DDB-TABLE-015](#ddb-table-015) vs [DDB-TABLE-160](#ddb-table-160): both are right - the empty list is treated as absent only
    for the 'at least one parameter' check; once another parameter makes the request valid, the empty list
    itself is rejected and the carrier change is NOT applied. Never serialize an empty...
  - full notes: [details/DDB-TABLE-359.md](details/DDB-TABLE-359.md)

## Update granularity and ordering

- <a id="ddb-table-152"></a>**DDB-TABLE-152** `update-granularity` · impact high · handled · verified 2026-10-08
  **PAY_PER_REQUEST -> PROVISIONED requires table ProvisionedThroughput AND a throughput Update for every GSI in the same UpdateTable**
  On a PAY_PER_REQUEST table with one GSI: UpdateTable BillingMode=PROVISIONED alone -> ValidationException
  'ProvisionedThroughput must be specified when BillingMode is PROVISIONED' (also with only a GSI Update);
  BillingMode=PROVISIONED + table ProvisionedThroughput 50/50 -> 'ProvisionedThroughput must be specified for
  index: gsi1'; adding GlobalSecondaryIndexUpdates[Update gsi1 50/50] -> 200, TableStatus=UPDATING 83 s
  (IndexStatus UPDATING for the first 6 s). Nothing is auto-assigned. Under PAY_PER_REQUEST, table
  ProvisionedThroughput alone -> 'Neither ReadCapacityUnits nor WriteCapacityUnits can be specified when
  BillingMode is PAY_PER_REQUEST' and a GSI ProvisionedThroughput Update -> 'The only Updates for index: gsi1
  when TableThroughputMode is PAY_PER_REQUEST can be to OnDemandThroughput, WarmThroughput'. After the switch
  to PROVISIONED, a GSI Create without ProvisionedThroughput -> 'Both ReadCapacityUnits and WriteCapacityUnits
  must be specified for index: gsi-nopt'.
  - ACK: custom_update, one-per-reconcile · ops: UpdateTable · fields: BillingMode, ProvisionedThroughput,
    GlobalSecondaryIndexUpdates.Update.ProvisionedThroughput
  - repro: PAY_PER_REQUEST table with GSI; UpdateTable BillingMode=PROVISIONED ProvisionedThroughput={50,50}
  - measurements: switch_to_provisioned_updating_s=82.7, gsi_updating_s=6.1
  - handling: handled via `pkg/resource/table/hooks.go:240-247; pkg/resource/table/hooks.go:378-399; pkg/resource/table/hooks.go:665-677; pkg/resource/table/hooks.go:357-376`
  - related: [DDB-TABLE-163](table-subresources.md#ddb-table-163), [DDB-TABLE-375](#ddb-table-375), [DDB-TABLE-376](#ddb-table-376), [DDB-TABLE-462](#ddb-table-462), [DDB-TABLE-382](table-streams-encryption-class.md#ddb-table-382), [DDB-TABLE-174](#ddb-table-174),
    [DDB-TABLE-159](#ddb-table-159), [DDB-TABLE-380](#ddb-table-380), [DDB-TABLE-153](#ddb-table-153), [DDB-TABLE-164](#ddb-table-164), [DDB-TABLE-128](#ddb-table-128), [DDB-TABLE-456](service.md#ddb-table-456), [DDB-TABLE-133](#ddb-table-133) ·
    evidence: table/mutation-matrix/gsi-billing-throughput
  - notes: Confirms H-T-023; refutes H-T-024 (no auto-assigned GSI capacity, the request is rejected instead).

- <a id="ddb-table-159"></a>**DDB-TABLE-159** `update-granularity` · impact high · handled · verified 2026-10-08
  **One GSI Create/Delete per UpdateTable and per table at a time; violations are LimitExceededException, not ValidationException**
  GlobalSecondaryIndexUpdates with [Create g3, Create g4], [Create g3, Delete g1] or [Delete g1, Delete g2] in
  one call fail with LimitExceededException 'Subscriber limit exceeded: Only 1 online index can be created or
  deleted simultaneously per table' (HTTP 400, 1.7-2.8 s latency). The identical code and message are returned
  for a Create or Delete of another index while one index is CREATING (both phases) or DELETING. Two Update
  actions on two different indexes in one call are accepted (each index UPDATING ~2 s, TableStatus stays
  ACTIVE), and a GSI Update of another index is accepted while an index is CREATING or DELETING.
  - ACK: one-per-reconcile, requeue, terminal_codes · ops: UpdateTable · fields: GlobalSecondaryIndexUpdates
  - repro: ACTIVE table with gsi1,gsi2; UpdateTable GlobalSecondaryIndexUpdates=[Create gsi3, Create gsi4]
    (+AttributeDefinitions)
  - measurements: limit_exceeded_latency_ms=2403, two_updates_settle_s=4.0
  - handling: handled via `pkg/resource/table/hooks_global_secondary_indexes.go:30-39; templates/hooks/table/sdk_read_one_post_set_output.go.tpl:1-28; pkg/resource/table/hooks_global_secondary_indexes.go:180-199; pkg/resource/table/hooks_global_secondary_indexes.go:246-275; pkg/resource/table/hooks.go:226-238; pkg/resource/table/hooks_global_secondary_indexes.go:202-217`
  - related: [DDB-TABLE-150](#ddb-table-150), [DDB-TABLE-376](#ddb-table-376), [DDB-TABLE-163](table-subresources.md#ddb-table-163), [DDB-TABLE-135](#ddb-table-135), [DDB-TABLE-382](table-streams-encryption-class.md#ddb-table-382), [DDB-TABLE-174](#ddb-table-174),
    [DDB-TABLE-152](#ddb-table-152), [DDB-TABLE-380](#ddb-table-380), [DDB-TABLE-171](table-throughput-billing.md#ddb-table-171), [DDB-TABLE-157](#ddb-table-157), [DDB-TABLE-154](table-throughput-billing.md#ddb-table-154), [DDB-TABLE-170](#ddb-table-170), [DDB-TABLE-169](#ddb-table-169) ·
    evidence: table/mutation-matrix/gsi-update-granularity
  - notes: H-T-004 confirmed; H-T-016 partially: the multi-action rejection is LimitExceededException (same
    code as the retryable 'one at a time' and the terminal capacity/decrease quotas), so the controller must
    match the message 'Only 1 online index can be created or deleted simultaneously' and treat it as...
  - full notes: [details/DDB-TABLE-159.md](details/DDB-TABLE-159.md)

- <a id="ddb-table-174"></a>**DDB-TABLE-174** `update-granularity` · impact high · handled · verified 2026-10-08
  **GSI Create cannot share an UpdateTable with PT/stream/SSE/TableClass/DeletionProtection/Warm changes; OK with BillingMode or GSI Update**
  On idle PROVISIONED tables, UpdateTable {GlobalSecondaryIndexUpdates:[Create gsi2] + X} fails with
  ValidationException for X = ProvisionedThroughput ('You cannot create or delete index while updating table
  IOPS'), StreamSpecification ('You cannot create or delete index while changing stream status'),
  DeletionProtectionEnabled ('DeletionProtection modification must be the only operation in the request'),
  SSESpecification ('Server-Side Encryption modification must be the only operation in the request'),
  TableClass ('TableClass modification must be the only operation in the request') and WarmThroughput ('Create
  global secondary index cannot be specified when updating WarmThroughput'); [Create gsi2, Delete gsi1] ->
  LimitExceededException 'Only 1 online index can be created or deleted simultaneously per table'. Accepted:
  [Create gsi2, Update gsi1 throughput] (both applied) and Create gsi2 + BillingMode=PAY_PER_REQUEST (table
  UPDATING 112 s for the switch while the index built; both applied). Each X alone was accepted on its own
  table.
  - ACK: one-per-reconcile, custom_update · ops: UpdateTable · fields: GlobalSecondaryIndexUpdates.Create,
    ProvisionedThroughput, StreamSpecification, SSESpecification, TableClass, DeletionProtectionEnabled,
    WarmThroughput, BillingMode
  - repro: UpdateTable TableName=T AttributeDefinitions=[pk,a] GlobalSecondaryIndexUpdates=[{Create gsi2}]
    DeletionProtectionEnabled=true
  - measurements: billing_switch_with_create_table_updating_s=112.5, gsi_create_total_s_min=506,
    gsi_create_total_s_max=996.6
  - handling: handled via `pkg/resource/table/hooks.go:220-304; test/e2e/tests/test_table.py:878-952; test/e2e/tests/test_table.py:724-729; test/e2e/tests/test_table.py:864-876`
  - related: [DDB-TABLE-433](table-streams-encryption-class.md#ddb-table-433), [DDB-TABLE-156](table-throughput-billing.md#ddb-table-156), [DDB-TABLE-358](table-throughput-billing.md#ddb-table-358), [DDB-TABLE-056](table-throughput-billing.md#ddb-table-056), [DDB-TABLE-199](table-replicas.md#ddb-table-199), [DDB-TABLE-224](table-replicas.md#ddb-table-224),
    [DDB-TABLE-162](#ddb-table-162), [DDB-TABLE-127](#ddb-table-127), [DDB-TABLE-175](#ddb-table-175), [DDB-TABLE-382](table-streams-encryption-class.md#ddb-table-382), [DDB-TABLE-163](table-subresources.md#ddb-table-163), [DDB-TABLE-286](table-streams-encryption-class.md#ddb-table-286), [DDB-TABLE-159](#ddb-table-159),
    [DDB-TABLE-152](#ddb-table-152), [DDB-TABLE-380](#ddb-table-380) · evidence: table/mutation-matrix/gsi-combined-updates
  - notes: H-T-017 partially confirmed: table ProvisionedThroughput is rejected as predicted, but
    DeletionProtectionEnabled and StreamSpecification are NOT accepted alongside a Create, while a BillingMode
    switch IS. A reconciler must emit the GSI Create alone (or with GSI Updates / a billing switch) and
    defer...
  - full notes: [details/DDB-TABLE-174.md](details/DDB-TABLE-174.md)

## Field behavior (defaults, normalization, shapes, immutability)

- <a id="ddb-table-125"></a>**DDB-TABLE-125** `normalization` · impact medium · handled · verified 2026-10-08
  **DescribeTable sorts AttributeDefinitions and Projection.NonKeyAttributes by name; GSI/LSI lists keep submitted order**
  AttributeDefinitions sent as [z, a, m, sk, pk] are returned as [a, m, pk, sk, z] (already in the CreateTable
  response); INCLUDE NonKeyAttributes sent as [zz, aa, mm, sk] come back as [aa, mm, sk, zz].
  GlobalSecondaryIndexes sent as [g-z, g-a] (and [idx-c, idx-a]) are returned in the submitted order, not
  sorted; GSI KeySchema keeps the submitted HASH-then-RANGE order. Order was stable across repeated
  DescribeTable calls, but a CreateTable response once listed the two GSIs in reverse order while CREATING
  (table/mutation-matrix/gsi-update-granularity).
  - ACK: compare.is_ignored+delta_pre_compare · ops: CreateTable, DescribeTable · fields:
    AttributeDefinitions, GlobalSecondaryIndexes, Projection.NonKeyAttributes
  - repro: CreateTable with AttributeDefinitions [z,a,m,sk,pk], GSI INCLUDE NonKeyAttributes [zz,aa,mm,sk];
    DescribeTable
  - handling: handled via `generator.yaml:51-69; pkg/resource/table/hooks.go:621-663`
  - related: [DDB-TABLE-123](#ddb-table-123), [DDB-TABLE-124](#ddb-table-124), [DDB-TABLE-126](#ddb-table-126), [DDB-TABLE-127](#ddb-table-127), [DDB-TABLE-366](table.md#ddb-table-366) · evidence:
    table/weird-inputs/gsi-request-validation
  - notes: Confirms the NonKeyAttributes part of H-T-127; the AttributeDefinitions part of H-T-041; refutes
    GSI-sorted-by-name.

- <a id="ddb-table-134"></a>**DDB-TABLE-134** `server-default` · impact medium · handled · verified 2026-10-08
  **WarmThroughput is always reported on tables and GSIs; PROVISIONED values mirror the provisioned RCU/WCU and never drop**
  PAY_PER_REQUEST: table and every GSI report WarmThroughput {ReadUnitsPerSecond: 12000, WriteUnitsPerSecond:
  4000, Status: ACTIVE} without ever being set. PROVISIONED table 5/5: table WarmThroughput is {5, 5, ACTIVE}
  (not 12000/4000); GSI with 1/1 reports {1, 1}; GSI with 13000/4500 reports {13000, 4500}. After decreasing
  that GSI to 1/1 its WarmThroughput stayed {13000, 4500} (high-water mark). A GSI created with explicit
  WarmThroughput 13000/5000 on a PAY_PER_REQUEST table reports exactly that, with Status UPDATING while
  IndexStatus=CREATING (CreateTable response: WarmThroughput absent; first DescribeTable: Status UPDATING;
  ACTIVE together with the index).
  - ACK: compare.is_ignored+delta_pre_compare, is_read_only · ops: CreateTable, DescribeTable · fields:
    WarmThroughput, GlobalSecondaryIndexes.WarmThroughput
  - repro: CreateTable PROVISIONED 5/5 with GSI 13000/4500 and GSI 1/1; DescribeTable; UpdateTable GSI to 1/1;
    DescribeTable
  - measurements: gsi_decrease_updating_s=6.1
  - handling: handled via `generator.yaml:1-6; generator.yaml:15`
  - related: [DDB-TABLE-168](#ddb-table-168), [DDB-TABLE-148](#ddb-table-148), [DDB-TABLE-169](#ddb-table-169), [DDB-TABLE-123](#ddb-table-123), [DDB-TABLE-116](table-subresources.md#ddb-table-116), [DDB-TABLE-138](#ddb-table-138),
    [DDB-TABLE-155](#ddb-table-155), [DDB-TABLE-153](#ddb-table-153), [DDB-TABLE-128](#ddb-table-128), [DDB-TABLE-154](table-throughput-billing.md#ddb-table-154), [DDB-TABLE-363](table-throughput-billing.md#ddb-table-363) · evidence:
    table/round-trip/gsi-lsi-describe
  - notes: Partially refutes H-T-037/H-T-118: a PROVISIONED table does NOT report max(12000, provisioned); it
    reports the provisioned values themselves (5/5). Confirms the never-drops rule and the per-index block.
    Values recorded in run.log of attempt 1 (b_table_warm, b_gsi_warm, b_gsi_warm_after_decrease).

- <a id="ddb-table-138"></a>**DDB-TABLE-138** `shape-mismatch` · impact medium · handled · verified 2026-10-08
  **GlobalSecondaryIndexDescription adds IndexArn/IndexStatus/Backfilling/counters/Warm+OnDemand blocks; Backfilling is tri-state**
  Compared with the GlobalSecondaryIndex input, DescribeTable adds IndexArn
  (arn:aws:dynamodb:REGION:ACCOUNT:table/T/index/I), IndexStatus, IndexSizeBytes, ItemCount (both 0 on fresh
  indexes), ProvisionedThroughput.NumberOfDecreasesToday (+LastIncrease/DecreaseDateTime after changes),
  WarmThroughput (always) and OnDemandThroughput (only if set). Backfilling is absent for GSIs created by
  CreateTable (while CREATING and when ACTIVE), absent on ACTIVE/UPDATING/ DELETING indexes, and present only
  for GSIs added via UpdateTable while IndexStatus=CREATING: false during resource allocation, true during
  backfill. The CreateTable response omits WarmThroughput on the indexes (first DescribeTable has it).
  - ACK: is_read_only, ignore.field_paths · ops: DescribeTable, CreateTable, UpdateTable · fields:
    GlobalSecondaryIndexes.Backfilling, GlobalSecondaryIndexes.IndexArn, GlobalSecondaryIndexes.IndexStatus,
    GlobalSecondaryIndexes.IndexSizeBytes, GlobalSecondaryIndexes.ItemCount
  - repro: DescribeTable after CreateTable with GSIs; UpdateTable Create GSI then DescribeTable every 1s
  - handling: handled via `generator.yaml:104-109; pkg/resource/table/hooks.go:72-93; generator.yaml:38-41; templates/hooks/table/sdk_read_one_post_set_output.go.tpl:1-28; generator.yaml:1-6; generator.yaml:15`
  - related: [DDB-TABLE-148](#ddb-table-148), [DDB-TABLE-149](#ddb-table-149), [DDB-TABLE-163](table-subresources.md#ddb-table-163), [DDB-TABLE-458](#ddb-table-458), [DDB-TABLE-116](table-subresources.md#ddb-table-116), [DDB-TABLE-165](#ddb-table-165),
    [DDB-TABLE-134](#ddb-table-134), [DDB-TABLE-155](#ddb-table-155), [DDB-TABLE-153](#ddb-table-153), [DDB-TABLE-128](#ddb-table-128), [DDB-TABLE-154](table-throughput-billing.md#ddb-table-154), [DDB-TABLE-363](table-throughput-billing.md#ddb-table-363) · evidence:
    table/round-trip/gsi-lsi-describe
  - notes: Confirms H-T-043 and the tri-state part of H-T-142; H-T-126 only partially (fresh counters are 0;
    no long-run refresh observed).

- <a id="ddb-table-151"></a>**DDB-TABLE-151** `normalization` · impact medium · unhandled (not handled in controller) · verified 2026-10-08
  **AttributeDefinitions is auto-pruned when a GSI delete removes the last key usage of an attribute**
  A table with AttributeDefinitions [a, b, c, pk] whose gsi3 keyed on 'c' is deleted reports
  AttributeDefinitions [a, b, pk] once the index is gone (no request mentioned AttributeDefinitions). The same
  happened for every GSI delete observed (attribute 'a' after deleting gsi1, 'd' after deleting gsi4). Lists
  are returned sorted by AttributeName.
  - ACK: compare.is_ignored+delta_pre_compare, custom_update · ops: UpdateTable, DescribeTable · fields:
    AttributeDefinitions
  - repro: Table with GSI on attr c; UpdateTable Delete GSI (no AttributeDefinitions); DescribeTable after the
    index is gone
  - handling: not handled in the controller (as of commit 34b85e6)
  - related: [DDB-TABLE-149](#ddb-table-149), [DDB-TABLE-166](#ddb-table-166), [DDB-TABLE-162](#ddb-table-162), [DDB-TABLE-127](#ddb-table-127), [DDB-TABLE-357](#ddb-table-357), [DDB-TABLE-359](#ddb-table-359) ·
    evidence: table/state-machine/gsi-lifecycle
  - notes: A spec that still lists the attribute will show a diff against observed AttributeDefinitions; see
    the update-side AttributeDefinitions finding for why that diff is harmless.

- <a id="ddb-table-153"></a>**DDB-TABLE-153** `requested-vs-effective` · impact medium · unhandled (not handled in controller) · verified 2026-10-08
  **Switching BillingMode drops OnDemandThroughput from table and GSIs; a switch is also accepted again within minutes (no 24h lockout seen)**
  A PAY_PER_REQUEST table with table OnDemandThroughput {-1,-1} and GSI OnDemandThroughput {6000,5000}
  switched to PROVISIONED: DescribeTable afterwards has no OnDemandThroughput on the table or the GSI (blocks
  absent, not zeroed), while GSI WarmThroughput (12002/4001) survived. UpdateTable OnDemandThroughput on the
  now-PROVISIONED table -> ValidationException 'MaxReadRequestUnits for OnDemandThroughput cannot be specified
  when the table BillingMode is PROVISIONED' (GSI: '...for index : gsi1 cannot be specified...'). Switching
  back to PAY_PER_REQUEST 80 s after the switch to PROVISIONED was accepted (200, TableStatus=UPDATING), and
  two further switches minutes later were accepted too (4 switches within 15 minutes on one table); the switch
  to PAY_PER_REQUEST incremented the GSI's NumberOfDecreasesToday to 1 (table counter stayed 0).
  - ACK: compare.is_ignored+delta_pre_compare, custom_update · ops: UpdateTable, DescribeTable · fields:
    BillingMode, OnDemandThroughput, GlobalSecondaryIndexes.OnDemandThroughput,
    ProvisionedThroughput.NumberOfDecreasesToday
  - repro: PPR table with ODT on table+GSI; UpdateTable BillingMode=PROVISIONED (+PT+GSI PT); DescribeTable;
    UpdateTable BillingMode=PAY_PER_REQUEST
  - handling: not handled in the controller (as of commit 34b85e6)
  - related: [DDB-TABLE-158](#ddb-table-158), [DDB-TABLE-157](#ddb-table-157), [DDB-TABLE-133](#ddb-table-133), [DDB-TABLE-152](#ddb-table-152), [DDB-TABLE-375](#ddb-table-375), [DDB-TABLE-376](#ddb-table-376),
    [DDB-TABLE-164](#ddb-table-164), [DDB-TABLE-128](#ddb-table-128), [DDB-TABLE-134](#ddb-table-134), [DDB-TABLE-138](#ddb-table-138), [DDB-TABLE-155](#ddb-table-155), [DDB-TABLE-154](table-throughput-billing.md#ddb-table-154), [DDB-TABLE-135](#ddb-table-135),
    [DDB-TABLE-456](service.md#ddb-table-456), [DDB-TABLE-458](#ddb-table-458), [DDB-TABLE-286](table-streams-encryption-class.md#ddb-table-286) · evidence: table/mutation-matrix/gsi-billing-throughput
  - notes: Confirms the drop part of H-T-123 and H-T-040 (update side). The documented 'once per 24 hours'
    billing-mode limit was not enforced for this fresh table (see also table/limits/gsi-decrease-budget);
    confidence medium because the rule may apply only to older tables or be enforced asynchronously.

- <a id="ddb-table-281"></a>**DDB-TABLE-281** `requested-vs-effective` · impact high · unhandled (not handled in controller) · verified 2026-10-09
  **ProvisionedThroughputOverride applies to the base table only (GSIs keep recorded throughput); PAY_PER_REQUEST override flips GSIs**
  From a PROVISIONED 5/5 backup with gsi1 5/5: ProvisionedThroughputOverride 7/7 -> table 7/7 but gsi1 5/5;
  GlobalSecondaryIndexOverride=[gsi1 8/8] -> gsi1 8/8, table 5/5; BillingModeOverride=PAY_PER_REQUEST +
  OnDemandThroughputOverride 50/50 -> BillingModeSummary PAY_PER_REQUEST, table ProvisionedThroughput 0/0 with
  OnDemandThroughput 50/50, gsi1 ProvisionedThroughput 0/0 without listing the GSI. Items and GSI projections
  were restored (Scan count 1, Query on gsi1 count 1).
  - ACK: custom_create, post-create-nudge · ops: RestoreTableFromBackup, DescribeTable · fields:
    ProvisionedThroughputOverride, GlobalSecondaryIndexOverride, BillingModeOverride,
    OnDemandThroughputOverride
  - repro: restore the same GSI backup four ways and DescribeTable each
  - handling: not handled in the controller (as of commit 34b85e6)
  - related: [DDB-TABLE-273](table-restore.md#ddb-table-273), [DDB-TABLE-280](#ddb-table-280), [DDB-TABLE-279](table-restore.md#ddb-table-279), [DDB-BACKUP-021](backup.md#ddb-backup-021), [DDB-BACKUP-017](backup.md#ddb-backup-017) · hypotheses:
    H-B-138, H-B-104 · evidence: table/round-trip/restore-overrides
  - notes: H-B-138 confirmed; H-B-104(b) confirmed (explicit OnDemandThroughputOverride replaces recorded
    maxima). To converge a provisioned Table spec with GSI throughput the controller must either pass
    GlobalSecondaryIndexOverride with every GSI's throughput or follow up with UpdateTable.

- <a id="ddb-table-357"></a>**DDB-TABLE-357** `immutable-field` · impact high · handled · verified 2026-10-09
  **KeySchema and LSIs are structurally immutable: no UpdateTable member (SDK ParamValidationError); unknown wire members are ignored**
  The UpdateTable input shape has no KeySchema or LocalSecondaryIndexes member (create-only members:
  ['GlobalSecondaryIndexes', 'GlobalTableSourceArn', 'KeySchema', 'LocalSecondaryIndexes', 'ResourcePolicy',
  'Tags']); boto3 rejects them client-side with ParamValidationError ('Unknown parameter in input:
  "KeySchema", must be one of: AttributeDefinitions, TableName, BillingMode, ...') and no request is sent.
  Injected into the raw JSON body as the only change, KeySchema -> ValidationException (HTTP 400) 'At least
  one of ProvisionedThroughput, BillingMode, ... or TableClass is required'; LocalSecondaryIndexes ->
  ValidationException (HTTP 400) 'At least one of ProvisionedThroughput, BillingMode, ... or TableClass is
  required'; a bogus member -> ValidationException (HTTP 400) 'At least one of ProvisionedThroughput,
  BillingMode, ... or TableClass is required'; KeySchema as a non-list -> ValidationException (HTTP 400) 'At
  least one of ProvisionedThroughput, BillingMode, ... or TableClass is required'. Injected next to
  DeletionProtectionEnabled=true -> 200 OK (TableStatus=ACTIVE) and DescribeTable KeySchema/LSIs
  unchanged=True.
  - ACK: is_immutable, custom_update · ops: UpdateTable, DescribeTable · fields: KeySchema,
    LocalSecondaryIndexes, AttributeDefinitions
  - repro: ACTIVE table (pk HASH, sk RANGE, 1 LSI): boto3 update_table(KeySchema=...) -> ParamValidationError;
    inject {'KeySchema': [...]} into the serialized body via a before-call hook -> see response
  - handling: handled via `generator.yaml:55-58; pkg/resource/table/hooks.go:621-627; generator.yaml:66-69; pkg/resource/table/hooks.go:815-880`
  - related: [DDB-TABLE-137](table-subresources.md#ddb-table-137), [DDB-TABLE-162](#ddb-table-162), [DDB-TABLE-127](#ddb-table-127), [DDB-TABLE-151](#ddb-table-151), [DDB-TABLE-359](#ddb-table-359), [DDB-TABLE-133](#ddb-table-133),
    [DDB-TABLE-094](table-subresources.md#ddb-table-094) · hypotheses: H-T-066, H-T-045 · evidence: table/mutation-matrix/schema-immutability
  - notes: Confirms H-T-066/H-T-045 structural part: a KeySchema or LSI diff can only be reconciled by
    recreate; the controller must detect it itself (terminal condition) because the API offers no call that
    would even fail for it. LocalSecondaryIndexDescription keys: ['IndexArn', 'IndexName',
    'IndexSizeBytes',...
  - full notes: [details/DDB-TABLE-357.md](details/DDB-TABLE-357.md)

## Response fidelity and consistency

- <a id="ddb-table-133"></a>**DDB-TABLE-133** `response-fidelity` · impact high · handled · verified 2026-10-08
  **PAY_PER_REQUEST tables report ProvisionedThroughput {0,0,NumberOfDecreasesToday:0} on the table and on every GSI**
  DescribeTable on a PAY_PER_REQUEST table returns Table.ProvisionedThroughput = {NumberOfDecreasesToday: 0,
  ReadCapacityUnits: 0, WriteCapacityUnits: 0} and the same block on each GlobalSecondaryIndexDescription,
  although neither was sent. LocalSecondaryIndexDescription has no throughput block at all. Sending those 0
  values back is rejected (see request-validation findings).
  - ACK: compare.nil_equals_zero_value, compare.is_ignored+delta_pre_compare · ops: DescribeTable · fields:
    ProvisionedThroughput, GlobalSecondaryIndexes.ProvisionedThroughput
  - repro: CreateTable PAY_PER_REQUEST with one GSI; DescribeTable
  - handling: handled via `pkg/resource/table/hooks_global_secondary_indexes.go:133-154; pkg/resource/table/common.go:25-37; generator.yaml:66-69; pkg/resource/table/hooks.go:815-880`
  - related: [DDB-TABLE-157](#ddb-table-157), [DDB-TABLE-153](#ddb-table-153), [DDB-TABLE-128](#ddb-table-128), [DDB-TABLE-152](#ddb-table-152), [DDB-TABLE-456](service.md#ddb-table-456), [DDB-TABLE-137](table-subresources.md#ddb-table-137),
    [DDB-TABLE-357](#ddb-table-357), [DDB-TABLE-094](table-subresources.md#ddb-table-094) · evidence: table/round-trip/gsi-lsi-describe
  - notes: Confirms H-T-032.

- <a id="ddb-table-165"></a>**DDB-TABLE-165** `stale-response` · impact medium · partially handled · verified 2026-10-08
  **UpdateTable response echoes the OLD throughput for a GSI Update (IndexStatus=UPDATING) but the new entry for a GSI Create**
  UpdateTable [Update gsi1 1/1 -> 2/2, Update gsi2 1/1 -> 2/2] returned TableStatus=ACTIVE and both indexes
  with IndexStatus=UPDATING but ProvisionedThroughput still 1/1 (and 2/2 -> 2/3 returned 2/2); DescribeTable
  2-4 s later shows 2/2 with LastIncreaseDateTime. For a GSI Create the response already contains the new
  index with IndexStatus=CREATING, Backfilling=false, its ProvisionedThroughput, IndexArn, IndexSizeBytes=0
  and ItemCount=0, identical to an immediate DescribeTable; for a GSI Delete the response shows
  IndexStatus=DELETING. In a PAY_PER_REQUEST->PROVISIONED switch the response did show the new 50/50 for table
  and GSI.
  - ACK: synced.when, requeue · ops: UpdateTable, DescribeTable · fields:
    GlobalSecondaryIndexes.ProvisionedThroughput, GlobalSecondaryIndexes.IndexStatus
  - repro: UpdateTable GlobalSecondaryIndexUpdates=[Update gsi1 PT 2/2]; compare TableDescription with
    DescribeTable
  - handling: partially handled via `pkg/resource/table/hooks_global_secondary_indexes.go:84-131; pkg/resource/table/hooks_global_secondary_indexes.go:246-258` - see Handling gaps
  - related: [DDB-TABLE-148](#ddb-table-148), [DDB-TABLE-138](#ddb-table-138), [DDB-TABLE-149](#ddb-table-149), [DDB-TABLE-163](table-subresources.md#ddb-table-163), [DDB-TABLE-458](#ddb-table-458), [DDB-TABLE-116](table-subresources.md#ddb-table-116),
    [DDB-TABLE-361](table-streams-encryption-class.md#ddb-table-361), [DDB-TABLE-375](#ddb-table-375), [DDB-TABLE-462](#ddb-table-462), [DDB-TABLE-155](#ddb-table-155) · evidence:
    table/mutation-matrix/gsi-update-granularity
  - notes: Confirms H-T-060 for GSI throughput updates: do not persist the UpdateTable response as observed
    state.
  - full notes: [details/DDB-TABLE-165.md](details/DDB-TABLE-165.md)

- <a id="ddb-table-462"></a>**DDB-TABLE-462** `stale-response` · impact medium · unhandled (not handled in controller) · verified 2026-10-09
  **SSE write does not clobber throughput changes, GSI deletes or PROV->PPR switches: its response says ACTIVE, DescribeTable keeps UPDATING**
  PROVISIONED 1/1 tables, UpdateTable(SSESpecification Enabled/KMS) 0.1 s after the job, DescribeTable every
  0.2 s. pt (ProvisionedThroughput 2/2, ~1.5 s): pt: job -> UPDATING; SSE at +0.2 s -> 200 (response
  TableStatus ACTIVE); DescribeTable right after: UPDATING; follow-up at +0.61 s -> ResourceInUseException
  'Attempt to change a resource which is still in use: Table IOPS are currently being updated. Tab'; job
  indicator settled at +1.45 s; final {'status': 'ACTIVE', 'rcu': 2, 'bm': None, 'gsi': None, 'sse':
  'ENABLED'}. gd (GSI Delete on a PROVISIONED table): gd: job -> UPDATING; SSE at +0.2 s -> 200 (response
  TableStatus ACTIVE); DescribeTable right after: UPDATING; follow-up at +1.02 s -> ResourceInUseException
  'Attempt to change a resource which is still in use: Can't change table IOPS when an index is be'; job
  indicator settled at +3.34 s; final {'status': 'ACTIVE', 'rcu': 1, 'bm': None, 'gsi': None, 'sse':
  'ENABLED'}. b2 (BillingMode PAY_PER_REQUEST, ~107 s): b2: job -> UPDATING; SSE at +0.2 s -> 200 (response
  TableStatus ACTIVE); DescribeTable right after: UPDATING; follow-up at +1.02 s -> ResourceInUseException
  'Attempt to change a resource which is still in use: Table IOPS are currently being updated. Tab'; job
  indicator settled at +107.15 s; final {'status': 'ACTIVE', 'rcu': 0, 'bm': 'PAY_PER_REQUEST', 'gsi': None,
  'sse': 'ENABLED'}. In every cell the SSE write's own UpdateTable response carried TableStatus=ACTIVE while
  DescribeTable stayed UPDATING and the follow-up was refused; the SSE job itself ran to ENABLED alongside the
  table job.
  - ACK: synced.when, one-per-reconcile, requeue · ops: UpdateTable, DescribeTable · fields: SSESpecification,
    TableStatus, ProvisionedThroughput, GlobalSecondaryIndexUpdates, BillingMode
  - repro: UpdateTable(job); UpdateTable(SSESpecification KMS) at +0.1 s; follow-up UpdateTable at +0.6/+1.0
    s; DescribeTable every 0.2 s
  - handling: not handled in the controller (as of commit 34b85e6)
  - related: [DDB-TABLE-450](table-streams-encryption-class.md#ddb-table-450), [DDB-TABLE-287](table-streams-encryption-class.md#ddb-table-287), [DDB-TABLE-370](table-throughput-billing.md#ddb-table-370), [DDB-TABLE-376](#ddb-table-376), [DDB-TABLE-452](table-throughput-billing.md#ddb-table-452), [DDB-TABLE-163](table-subresources.md#ddb-table-163),
    [DDB-TABLE-375](#ddb-table-375), [DDB-TABLE-152](#ddb-table-152), [DDB-TABLE-286](table-streams-encryption-class.md#ddb-table-286), [DDB-TABLE-458](#ddb-table-458), [DDB-TABLE-175](#ddb-table-175), [DDB-TABLE-165](#ddb-table-165), [DDB-TABLE-361](table-streams-encryption-class.md#ddb-table-361),
    [DDB-TABLE-155](#ddb-table-155) · evidence: table/creative/clobber-iops-sse
  - notes: Closes the SSE column of the clobber matrix: with table/creative/clobber-matrix (stream enable,
    PPR->PROV) and clobber-gsi-warm (GSI add) the premature-ACTIVE reset of [DDB-TABLE-287](table-streams-encryption-class.md#ddb-table-287) is confined to the
    TableClass switch. The SSE response's TableStatus=ACTIVE is a stale-response hazard everywhere (it...
  - full notes: [details/DDB-TABLE-462.md](details/DDB-TABLE-462.md)

## Delete semantics

- <a id="ddb-table-150"></a>**DDB-TABLE-150** `delete-semantics` · impact high · unhandled (not handled in controller) · verified 2026-10-08
  **DeleteTable is rejected with ResourceInUseException while any GSI is CREATING/UPDATING/DELETING, even when TableStatus=ACTIVE**
  While a GSI added via UpdateTable was CREATING (both during resource allocation with TableStatus=UPDATING
  and during the ~16-minute backfill with TableStatus=ACTIVE and Backfilling=true) every DeleteTable call
  (~950 attempts, 1/s) failed with ResourceInUseException 'Attempt to change a resource which is still in use:
  Cannot delete table while indexes are being created, updated, or deleted.' The same message was returned
  while a GSI OnDemandThroughput update had the index in UPDATING for ~2 s
  (table/round-trip/gsi-lsi-describe). DeleteTable succeeded as soon as the index was ACTIVE; the table was
  gone 10 s later.
  - ACK: deletable.when, requeue · ops: DeleteTable, DescribeTable · fields:
    GlobalSecondaryIndexes.IndexStatus, TableStatus
  - repro: UpdateTable Create GSI; DeleteTable while IndexStatus=CREATING
  - measurements: rejected_window_s=990, delete_to_404_s=10.1
  - handling: not handled in the controller (as of commit 34b85e6)
  - related: [DDB-TABLE-376](#ddb-table-376), [DDB-TABLE-159](#ddb-table-159), [DDB-TABLE-163](table-subresources.md#ddb-table-163), [DDB-TABLE-135](#ddb-table-135) · evidence:
    table/state-machine/gsi-lifecycle
  - notes: Refutes H-T-006. Finalizer logic must check every IndexStatus (not TableStatus) before DeleteTable
    and treat ResourceInUseException as retryable.

## Quotas and rate limits

- <a id="ddb-table-157"></a>**DDB-TABLE-157** `quota-limit` · impact medium · unhandled (not handled in controller) · verified 2026-10-08
  **Table and GSI provisioned-throughput decrease budgets are separate counters; a coupled table+GSI decrease is rejected atomically**
  Fresh PROVISIONED table 50/50 with gsi1 50/50. Table decreases 50->40/40, 40/40->30/60 (RCU down, WCU up),
  ->20/20, ->10/10 all succeed (TableStatus UPDATING ~2 s each) and
  Table.ProvisionedThroughput.NumberOfDecreasesToday goes 1,2,3,4 (RCU+WCU in one call = one decrease; RCU
  down with WCU up also = one decrease); the GSI counter stays 0. A 5th call decreasing the table (5/5) AND
  gsi1 (40/40) fails with LimitExceededException 'Subscriber limit exceeded: Provisioned throughput decreases
  are limited within a given UTC day. After the first 4 decreases, each subsequent decrease in the same UTC
  day can be performed at most once every 3600 seconds. Number of decreases today: 4. Last decrease at ...'
  and the GSI is untouched (still 50/50, counter 0). A GSI-only decrease right after succeeds (GSI counter 1)
  and three more GSI decreases succeed until the GSI's own 5th is rejected with the same message. Table
  increases always succeed; a table RCU-down/WCU-up call with an exhausted budget is rejected like a decrease.
  - ACK: terminal_codes, requeue, custom_update · ops: UpdateTable, DescribeTable · fields:
    ProvisionedThroughput, GlobalSecondaryIndexUpdates.Update.ProvisionedThroughput,
    ProvisionedThroughput.NumberOfDecreasesToday
  - repro: PROVISIONED 50/50 + GSI 50/50; 4 table decreases; UpdateTable {PT 5/5, GSI Update 40/40}
  - measurements: decrease_updating_s=2.1, table_decreases_before_limit=4, gsi_decreases_before_limit=4
  - handling: not handled in the controller (as of commit 34b85e6)
  - related: [DDB-TABLE-171](table-throughput-billing.md#ddb-table-171), [DDB-TABLE-159](#ddb-table-159), [DDB-TABLE-154](table-throughput-billing.md#ddb-table-154), [DDB-TABLE-170](#ddb-table-170), [DDB-TABLE-169](#ddb-table-169), [DDB-TABLE-153](#ddb-table-153),
    [DDB-TABLE-133](#ddb-table-133) · evidence: table/limits/gsi-decrease-budget
  - notes: Confirms H-T-114. This LimitExceededException is retryable only after the hour; it shares the code
    with the 'Only 1 online index...' and capacity-cap variants, so message matching ('decreases are limited')
    is required.

- <a id="ddb-table-168"></a>**DDB-TABLE-168** `quota-limit` · impact high · unhandled (not handled in controller) · verified 2026-10-09, re-verified
  **Concurrent CreateTable calls with GSIs are no longer serialized: 3 and 5 simultaneous creates all return 200 and reach ACTIVE in ~16s**
  Three CreateTable calls (each PAY_PER_REQUEST with one GSI) fired from threads within 3 ms all returned HTTP
  200 (latencies 49-154 ms) and all three tables were ACTIVE with their index ACTIVE 16.3 s later; the same
  for five simultaneous calls (latencies 43-920 ms, all ACTIVE after 16.5 s). No LimitExceededException about
  'only one table with secondary indexes in CREATING' was observed. Two earlier probes also created
  GSI-bearing tables back to back (9 tables within 2 s) without rejection.
  - ACK: none · ops: CreateTable, DescribeTable · fields: GlobalSecondaryIndexes
  - repro: threading: 5x CreateTable(PAY_PER_REQUEST, 1 GSI) at once; poll DescribeTable
  - measurements: concurrent_3_all_active_s=16.3, concurrent_5_all_active_s=16.5, max_create_latency_ms=920
  - handling: not handled in the controller (as of commit 34b85e6)
  - related: [DDB-TABLE-148](#ddb-table-148), [DDB-TABLE-169](#ddb-table-169), [DDB-TABLE-123](#ddb-table-123), [DDB-TABLE-116](table-subresources.md#ddb-table-116), [DDB-TABLE-134](#ddb-table-134) · evidence:
    table/limits/gsi-concurrency-quotas, table/creative/reverify-set-a1
  - notes: Confirms H-T-138 and refutes H-T-015: a controller does not need to serialize GSI-bearing table
    creations (the live docs say up to 250 such requests at a time). The lab's dynamodb:index-table-creating
    lock is therefore a precaution, not an API requirement.

- <a id="ddb-table-170"></a>**DDB-TABLE-170** `quota-limit` · impact medium · unhandled (not handled in controller) · verified 2026-10-08
  **Per-table and per-index capacity caps are checked separately (table 39900 + GSI 200 accepted); over-cap requests are LimitExceededException**
  With TableMaxReadCapacityUnits=40000: CreateTable RCU 40001 -> LimitExceededException 'The requested
  ReadCapacityUnits, 40001, is above the per table maximum for the account in us-west-2. Per table maximum: 40000.
  Refer to the Amazon DynamoDB Developer Guide for current limits and how to request higher limits.' (1.4 s).
  A GSI with RCU 40001 -> 'The requested ReadCapacityUnits for index gsi1, 40001, is above the per index
  maximum for the account in us-west-2. Per table maximum: 40000. ...'. CreateTable with table RCU 39900 + GSI
  RCU 200 (sum 40100) is ACCEPTED, as is table 1 + GSI 40000 and adding a 100-RCU GSI to a table already at
  40000, so the per-table cap does not aggregate index capacity. UpdateTable to 40001 on an ACTIVE 40000 table
  -> the same per-table message, table stays ACTIVE. When the account total would exceed
  AccountMaxReadCapacityUnits the message changes to 'This request would have caused the ReadCapacityUnits
  limit to be exceeded for the account in us-west-2. Current ReadCapacityUnits reserved by the account: 40136.
  Limit: 80000. Requested: 40001'.
  - ACK: terminal_codes · ops: CreateTable, UpdateTable, DescribeLimits · fields: ProvisionedThroughput,
    GlobalSecondaryIndexes.ProvisionedThroughput
  - repro: DescribeLimits; CreateTable PROVISIONED RCU=TableMax+1; CreateTable RCU=TableMax-100 with GSI RCU
    200
  - handling: not handled in the controller (as of commit 34b85e6)
  - related: [DDB-TABLE-171](table-throughput-billing.md#ddb-table-171), [DDB-TABLE-159](#ddb-table-159), [DDB-TABLE-157](#ddb-table-157), [DDB-TABLE-154](table-throughput-billing.md#ddb-table-154), [DDB-TABLE-169](#ddb-table-169), [DDB-TABLE-135](#ddb-table-135) ·
    evidence: table/limits/gsi-concurrency-quotas
  - notes: Partially refutes H-T-116 (the cap does not include GSIs; message says 'per table maximum' not
    'Subscriber limit exceeded'). Attempt 1 left the 39900-RCU table running unregistered for ~2.5 min before
    manual deletion.

- <a id="ddb-table-388"></a>**DDB-TABLE-388** `quota-limit` · impact medium · unhandled (not handled in controller) · verified 2026-10-09
  **Doc claim C005 FALSE: 'only one table with secondary indexes CREATING at a time' is not enforced - 3 and 5 concurrent GSI creates all 200**
  Three and five CreateTable calls, each with one GSI, fired within milliseconds all returned HTTP 200 and
  were ACTIVE ~16 s later; no LimitExceededException about 'only one table with secondary indexes in CREATING'
  ([DDB-TABLE-168](#ddb-table-168); nine GSI tables within 2 s elsewhere). Seven concurrent restores with GSI+LSI were admitted
  too ([DDB-TABLE-282](table-restore.md#ddb-table-282)).
  - ACK: none · ops: CreateTable · fields: GlobalSecondaryIndexes
  - handling: not handled in the controller (as of commit 34b85e6)
  - related: [DDB-TABLE-168](#ddb-table-168), [DDB-TABLE-282](table-restore.md#ddb-table-282) · evidence: table/limits/gsi-concurrency-quotas,
    table/round-trip/restore-overrides, service/static/doc-claims-1
  - notes: VERDICT: FALSE - the model doc is stale (live docs allow up to 250 such requests); a controller
    need not serialize GSI-bearing table creations

## Adoption and first-sync hazards

- <a id="ddb-table-166"></a>**DDB-TABLE-166** `first-sync-destructive` · impact high · partially handled · verified 2026-10-08
  **DeletionProtectionEnabled does not protect GSIs: a GSI Delete on a protected table succeeds**
  On a table with DeletionProtectionEnabled=true, UpdateTable GlobalSecondaryIndexUpdates[Delete gsi1] returns
  200, the index goes DELETING and is gone 5 s later; its attribute is pruned from AttributeDefinitions.
  Nothing in the API gates index deletion. Re-creating an index with the same name (different Projection, ALL)
  succeeded once the entry had disappeared and took 507 s to become ACTIVE on the empty table.
  - ACK: custom_update, one-per-reconcile · ops: UpdateTable · fields: DeletionProtectionEnabled,
    GlobalSecondaryIndexUpdates.Delete
  - repro: Table DeletionProtectionEnabled=true with gsi1; UpdateTable GlobalSecondaryIndexUpdates=[Delete
    gsi1]
  - measurements: gsi_delete_s=4.8, gsi_recreate_s=507.0
  - handling: partially handled via `pkg/resource/table/hooks_global_secondary_indexes.go:84-131; pkg/resource/table/hooks_global_secondary_indexes.go:246-258` - see Handling gaps
  - related: [DDB-TABLE-151](#ddb-table-151), [DDB-TABLE-149](#ddb-table-149), [DDB-TABLE-163](table-subresources.md#ddb-table-163), [DDB-TABLE-376](#ddb-table-376), [DDB-TABLE-458](#ddb-table-458), [DDB-TABLE-286](table-streams-encryption-class.md#ddb-table-286) ·
    evidence: table/mutation-matrix/gsi-update-granularity
  - notes: Confirms H-T-068 and H-T-067: adoption logic must treat an empty spec.globalSecondaryIndexes as
    'unspecified' or it will drop indexes.
  - full notes: [details/DDB-TABLE-166.md](details/DDB-TABLE-166.md)

## Handling gaps (bugs to file)

- [DDB-TABLE-164](#ddb-table-164) - Re-sending identical ProvisionedThroughput (table or GSI) is a ValidationException;
  identical DeletionProtection and partial PT changes pass (suspected bug)
  Suspected controller bug confirmed by evidence: The fallback the controller takes for a changed
  Projection/KeySchema - a throughput-only UpdateGlobalSecondaryIndexAction - is not a harmless no-op: when
  the PT is unchanged DynamoDB rejects it with ValidationException 'The provisioned throughput for the index X
  will not change' ([DDB-TABLE-164](#ddb-table-164)), so the delta persists and every reconcile errors; under PPR a PT Update is
  rejected too ('The only Updates for index ... can be to OnDemandThroughput, WarmThroughput', [DDB-TABLE-152](#ddb-table-152)).
  The correct path - delete then recreate under the same name - works once the entry is gone ([DDB-TABLE-166](#ddb-table-166),
  507 s on an empty table). The immutability itself is model-level (the action has no Projection/KeySchema
  members) and was not separately probed.
  - handling_ref: `pkg/resource/table/hooks_global_secondary_indexes.go:84-131;
    pkg/resource/table/hooks_global_secondary_indexes.go:246-258`
- [DDB-TABLE-165](#ddb-table-165) - UpdateTable response echoes the OLD throughput for a GSI Update (IndexStatus=UPDATING) but
  the new entry for a GSI Create (partially handled)
  Confirms H-T-060 for GSI throughput updates: do not persist the UpdateTable response as observed state.
  - handling_ref: `pkg/resource/table/hooks_global_secondary_indexes.go:84-131;
    pkg/resource/table/hooks_global_secondary_indexes.go:246-258`
- [DDB-TABLE-166](#ddb-table-166) - DeletionProtectionEnabled does not protect GSIs: a GSI Delete on a protected table succeeds
  (partially handled)
  Confirms H-T-068 and H-T-067: adoption logic must treat an empty spec.globalSecondaryIndexes as
  'unspecified' or it will drop indexes.
  - handling_ref: `pkg/resource/table/hooks_global_secondary_indexes.go:84-131;
    pkg/resource/table/hooks_global_secondary_indexes.go:246-258`

## E2E timing

Values are seconds unless the key says otherwise; n = trials behind the numbers ('1 run' when the finding records none).

| finding | what | measurements | n |
| --- | --- | --- | --- |
| [DDB-TABLE-123](#ddb-table-123) | GSI KeySchema accepts up to 4 HASH + 4 RANGE attributes (multi-attribute keys); table and LSI keys stay at 2 | create_active_s_8_element_gsi=20.2, create_active_s_3_element_gsi=16.2 | 1 run |
| [DDB-TABLE-134](#ddb-table-134) | WarmThroughput is always reported on tables and GSIs; PROVISIONED values mirror the provisioned RCU/WCU and never drop | gsi_decrease_updating_s=6.1 | 1 run |
| [DDB-TABLE-135](#ddb-table-135) | GSI OnDemandThroughput is independent of the table cap and only flips IndexStatus (not TableStatus) to UPDATING | gsi_odt_updating_s=2.0 | 1 run |
| [DDB-TABLE-148](#ddb-table-148) | GSI added via UpdateTable: TableStatus UPDATING only ~25-55s (resource allocation), then ACTIVE while the index backfills 7-16 min | resource_allocation_s_min=24.3, resource_allocation_s_max=55.0, gsi_create_total_s_provisioned=537.5, gsi_create_total_s_ppr_approx=990, create_table_with_2_gsis_s=16.2, gsi_delete_s=5.1, gsi_throughput_update_s=2.0 | 1 run |
| [DDB-TABLE-149](#ddb-table-149) | Deleting a GSI that is CREATING is rejected (ResourceInUseException) during resource allocation and accepted once Backfilling=true | rejected_window_s=55, delete_to_gone_s=5.1 | 1 run |
| [DDB-TABLE-150](#ddb-table-150) | DeleteTable is rejected with ResourceInUseException while any GSI is CREATING/UPDATING/DELETING, even when TableStatus=ACTIVE | rejected_window_s=990, delete_to_404_s=10.1 | 1 run |
| [DDB-TABLE-152](#ddb-table-152) | PAY_PER_REQUEST -> PROVISIONED requires table ProvisionedThroughput AND a throughput Update for every GSI in the same UpdateTable | switch_to_provisioned_updating_s=82.7, gsi_updating_s=6.1 | 1 run |
| [DDB-TABLE-155](#ddb-table-155) | GSI WarmThroughput: decrease rejected, same value accepted, increase keeps IndexStatus ACTIVE but Status UPDATING ~6.5 min | gsi_warm_status_updating_s=388.4 | 1 run |
| [DDB-TABLE-157](#ddb-table-157) | Table and GSI provisioned-throughput decrease budgets are separate counters; a coupled table+GSI decrease is rejected atomically | decrease_updating_s=2.1, table_decreases_before_limit=4, gsi_decreases_before_limit=4 | 1 run |
| [DDB-TABLE-159](#ddb-table-159) | One GSI Create/Delete per UpdateTable and per table at a time; violations are LimitExceededException, not ValidationException | limit_exceeded_latency_ms=2403, two_updates_settle_s=4.0 | 1 run |
| [DDB-TABLE-166](#ddb-table-166) | DeletionProtectionEnabled does not protect GSIs: a GSI Delete on a protected table succeeds | gsi_delete_s=4.8, gsi_recreate_s=507.0 | 1 run |
| [DDB-TABLE-168](#ddb-table-168) | Concurrent CreateTable calls with GSIs are no longer serialized: 3 and 5 simultaneous creates all return 200 and reach ACTIVE in ~16s | concurrent_3_all_active_s=16.3, concurrent_5_all_active_s=16.5, max_create_latency_ms=920 | 1 run |
| [DDB-TABLE-169](#ddb-table-169) | 21st GSI: ValidationException at CreateTable but LimitExceededException via UpdateTable; a 20-GSI table creates in ~20s | create_20_gsis_s=22.3 | 1 run |
| [DDB-TABLE-174](#ddb-table-174) | GSI Create cannot share an UpdateTable with PT/stream/SSE/TableClass/DeletionProtection/Warm changes; OK with BillingMode or GSI Update | billing_switch_with_create_table_updating_s=112.5, gsi_create_total_s_min=506, gsi_create_total_s_max=996.6 | 1 run |
| [DDB-TABLE-175](#ddb-table-175) | GSI Create admitted during SSE/DeletionProtection/Warm updates, ResourceInUse while IOPS, stream or TableClass changes run | table_class_gsi_updating_s=5.1, stream_enable_table_updating_s=5.1, gsi_delete_table_updating_s=5.1 | 1 run |
| [DDB-TABLE-375](#ddb-table-375) | GSI DELETING on a PPR table (~2.6 s): BillingMode=PROVISIONED (+/- dying index) -> ResourceInUse IOPS/'Index is being deleted'; ODT refused | t1_gsi_deleting_s=2.6 | 1 run |
| [DDB-TABLE-376](#ddb-table-376) | GSI DELETING on a PROVISIONED table (~4.6 s): billing switch / table PT -> ResourceInUse (IOPS); DeleteTable refused; DP admitted | t2_gsi_deleting_s=4.6 | 1 run |
| [DDB-TABLE-417](#ddb-table-417) | Doc claim C050 TRUE: UpdateTable GSI Create supports up to 4 partition and up to 4 sort keys | gsi44_create_timeline=[{"duration_s":24.18,"from_s":0.01,"to_s":24.19,"value":{"backfilling":false,"index":"CREATING","table":"UPDATING"}}, {"duration_s":null,"from_s":24.19,"to_s":null,"value":{"backfilling":true,"index":"CREATING","table":"ACTIVE"}}], gsi44_delete_timeline=[{"duration_s":6.06,"from_s":0.02,"to_s":6.08,"value":{"backfilling":null,"index":"DELETING","table":"UPDATING"}}, {"duration_s":null,"from_s":6.08,"to_s":null,"value":{"backfilling":null,"index":"GONE","table":"ACTIVE"}}], gsi44_delete_attempts=[OK] | 1 run |
| [DDB-TABLE-458](#ddb-table-458) | GSI-add resource allocation (UPDATING 27-56 s): SSE/DP/tag admitted, TableStatus not reset; WarmThroughput -> HTTP 500 'system maintenance' | updating_s=[27.16, 27.18, 28.16, 38.07, 42.31, 56.25], index_active_s=[510.59, 512.39, 524.71, 538.62, 1001.31, 1003.3] | 1 run |

## Open questions

<!-- preserved:start id=open-questions -->
<!-- open questions and follow-up experiments; survives re-renders -->
<!-- preserved:end -->

## Appendix: low-impact and duplicate findings

| id | category | impact | status | title | related | duplicate_of |
| --- | --- | --- | --- | --- | --- | --- |
| <a id="ddb-table-046"></a>**DDB-TABLE-046** | request-validation | high | confirmed | UpdateTable with only TableName is rejected with ValidationException | - | [DDB-TABLE-015](#ddb-table-015) |
| <a id="ddb-table-158"></a>**DDB-TABLE-158** | quota-limit | medium | confirmed | BillingMode can be flipped repeatedly within minutes: PPR->PROVISIONED->PPR->PROVISIONED->PPR all accepted on one table | - | [DDB-TABLE-153](#ddb-table-153) |
| <a id="ddb-table-380"></a>**DDB-TABLE-380** | update-granularity | low | confirmed | Rejected multi-member UpdateTable calls are atomic: ghost-GSI RNF, stream re-enable or 5th decrease leave valid members unapplied | [DDB-TABLE-161](table-subresources.md#ddb-table-161), [DDB-TABLE-382](table-streams-encryption-class.md#ddb-table-382), [DDB-TABLE-174](#ddb-table-174), [DDB-TABLE-159](#ddb-table-159), [DDB-TABLE-152](#ddb-table-152), [DDB-TABLE-094](table-subresources.md#ddb-table-094), [DDB-TABLE-137](table-subresources.md#ddb-table-137), [DDB-TABLE-116](table-subresources.md#ddb-table-116) | - |
| <a id="ddb-table-389"></a>**DDB-TABLE-389** | other | low | confirmed | Doc claim C006 TRUE: a GSI KeySchema accepts up to 4 HASH and 4 RANGE attributes; the 5th of either kind is a ValidationException | [DDB-TABLE-123](#ddb-table-123) | - |
| <a id="ddb-table-413"></a>**DDB-TABLE-413** | other | low | confirmed | Doc claim C045 TRUE: Only one GSI can be created or deleted per UpdateTable operation | [DDB-TABLE-159](#ddb-table-159), [DDB-TABLE-174](#ddb-table-174) | - |
| <a id="ddb-table-417"></a>**DDB-TABLE-417** | other | low | confirmed | Doc claim C050 TRUE: UpdateTable GSI Create supports up to 4 partition and up to 4 sort keys | [DDB-TABLE-123](#ddb-table-123) | - |
| <a id="ddb-table-418"></a>**DDB-TABLE-418** | other | low | confirmed | Doc claim C051 TRUE: CreateGlobalSecondaryIndexAction.OnDemandThroughput sets the new GSI's read/write maxima | [DDB-TABLE-135](#ddb-table-135), [DDB-TABLE-165](#ddb-table-165) | - |
| <a id="ddb-table-419"></a>**DDB-TABLE-419** | other | low | confirmed | Doc claim C052 TRUE: GlobalSecondaryIndex.OnDemandThroughput (CreateTable) sets the GSI's read/write maxima | [DDB-TABLE-135](#ddb-table-135), [DDB-TABLE-154](table-throughput-billing.md#ddb-table-154) | - |
| <a id="ddb-table-430"></a>**DDB-TABLE-430** | other | low | confirmed | Doc claim C063 TRUE: UpdateGlobalSecondaryIndexAction.OnDemandThroughput updates the GSI's read/write maxima | [DDB-TABLE-135](#ddb-table-135), [DDB-TABLE-154](table-throughput-billing.md#ddb-table-154), [DDB-TABLE-165](#ddb-table-165) | - |

## Supplementary notes

<!-- preserved:start -->
<!-- preserved:end -->
