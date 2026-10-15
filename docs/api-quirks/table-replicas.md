<!-- generated: ack-api-quirks dynamodb 2026-10-09 model=2012-08-10 controller=34b85e6 -->
# Table replicas (ReplicaUpdates, version 2019.11.21)
_Replica create/update/delete rules, prerequisites, settings replication across regions, per-region views, autoscaling coupling, timings._
Generated from ack-api-quirks `services/dynamodb` (render date in the marker above); model 2012-08-10 (service/dynamodb v1.39.8); controller commit 34b85e6; evidence: `services/dynamodb/probes/<probe id>/` in the lab repo.

## Overview

<!-- preserved:start id=overview -->
Replicas are the 2019.11.21 global-table mechanism: a Table's `tableReplicas` fans out into `UpdateTable ReplicaUpdates` calls that must be the only member of the request, and control is symmetric - any member region can add, update or remove replicas ([DDB-TABLE-224](#ddb-table-224), [DDB-TABLE-297](#ddb-table-297)). TableReplicaAutoScaling is a facade over Application Auto Scaling (skip verdict), yet AAS state gates replica creation on PROVISIONED tables and rewrites provisioned throughput out-of-band, so it still shapes what a Table reconciler may assume ([DDB-TABLEREPLICAAUTOSCALING-001](#ddb-tablereplicaautoscaling-001), [DDB-TABLE-249](#ddb-table-249); [DDB-TABLE-314](table-policy-kinesis-autoscaling.md#ddb-table-314), table-subresources.md).

### Rules a reconciler must respect
- There is no stream prerequisite: ReplicaUpdates.Create succeeds on a table with no stream (DynamoDB enables NEW_AND_OLD_IMAGES itself, kept after the last replica leaves) and on KEYS_ONLY/NEW_IMAGE/OLD_IMAGE streams unchanged, a disabled stream is re-enabled with a NEW LatestStreamLabel; the controller's pre-check hasStreamSpecificationWithNewAndOldImages (pkg/resource/table/hooks.go:292-299; [GT-DDB-031](service.md#gt-ddb-031) (controller hooks catalog entry)) therefore rejects specs AWS accepts; once a replica exists the stream is immutable in every region and even an identical re-send is ValidationException, until the moment Replicas[] empties ([DDB-TABLE-305](#ddb-table-305), [DDB-TABLE-221](#ddb-table-221), [DDB-TABLE-222](#ddb-table-222)).
- ReplicaUpdates must be the only operation in the call (validated before any state check); EVENTUAL groups take exactly one Create or Delete per call ('more than one create or delete replica actions not allowed'; two identical Creates for one region are de-duplicated), a second action while one is in flight is ResourceInUseException, a Create may be issued at any member's endpoint (that region becomes the 'source') and Replicas[] is creation order as seen per region - diff by RegionName as a set; an Update needs an action field (RegionName-only is 'no actions specified') and a same-value TableClassOverride is accepted at a 34-58 s UPDATING cycle; a TableClass change on the base is applied to every replica and spends each replica's 2-per-30-days budget ([DDB-TABLE-224](#ddb-table-224), [DDB-TABLE-265](#ddb-table-265), [DDB-TABLE-294](#ddb-table-294)).
- The error taxonomy is message-based, never the legacy ReplicaAlreadyExists/ReplicaNotFound codes: existing member or foreign same-name table -> 'already existed as tables'; Delete/Update of a non-member or not-yet-listed entry -> 'not part of the global table ... [region]'; local region -> 'Cannot add or delete the local region through ReplicaUpdates'; invalid/uppercase region -> list of supported regions; disabled opt-in region -> 'region is disabled'; empty RegionName -> AccessDeniedException; an unknown GSI name in a Create override is silently accepted; nothing in the controller classifies these texts ([DDB-TABLE-203](#ddb-table-203), [DDB-TABLE-223](#ddb-table-223), [DDB-TABLE-307](#ddb-table-307)).
- Create/Delete lifecycle: the Create response shows Replicas=[] and the entry appears ~6 s later, the replica region is describable ~10 s in; a CREATING replica answers only DescribeTable (other regional calls are ResourceNotFound/TableNotFound, DeleteTable ResourceInUseException, no cancel); a Delete deletes the regional table with the source UPDATING ~36 s then ACTIVE while the replica is DELETING - Synced must read Replicas[].ReplicaStatus, not TableStatus (the controller derives it from TableStatus); while a replica is CREATING or DELETING any other UpdateTable (second replica action, DP, TableClass, cancel) is ResourceInUseException and stream disable is 'Disabling Stream is not allowed for a Global Table replica', while tags, TTL, PITR, policy, insights and CreateBackup are admitted ([DDB-TABLE-226](#ddb-table-226), [DDB-TABLE-298](#ddb-table-298), [DDB-TABLE-204](#ddb-table-204), [DDB-TABLE-227](#ddb-table-227), [DDB-TABLE-228](#ddb-table-228)).
- PROVISIONED prerequisites: a Create needs an AAS write target AND policy on the table and on every GSI ('Table/GSI write capacity should either be Pay-Per-Request or AutoScaled'); a bare target is not enough, UpdateTableReplicaAutoScaling cannot create them (RNF 'Global table ... does not exist' on a regional table, also after the last replica is removed; it rejects PAY_PER_REQUEST groups except AutoScalingDisabled), the first retry after registering may be HTTP 500, DynamoDB mirrors the targets into the replica region, and afterwards any un-autoscaled GSI blocks every GSI UpdateTable ([DDB-TABLE-249](#ddb-table-249), [DDB-TABLE-288](#ddb-table-288), [DDB-TABLE-187](#ddb-table-187), [DDB-TABLE-190](#ddb-table-190)).
- Settings replication is group-wide for TTL (from any region), GSI add/delete (also when issued at the replica endpoint; the base returns ACTIVE while replica indexes still backfill), SSE type (KMS on the base fans out, the key itself is regional and reported as a full ARN per replica) and manual ProvisionedThroughput (in ANY region, no override materialized, so out-of-band changes in a replica region read as drift on the base); tags, PITR, resource policy, Contributor Insights and DeletionProtection stay regional (a DP toggle on any member holds the base UPDATING 34-39 s) - apply regional settings per region with a regional client ([DDB-TABLE-230](#ddb-table-230), [DDB-TABLE-296](#ddb-table-296), [DDB-TABLE-295](#ddb-table-295), [DDB-TABLE-297](#ddb-table-297), [DDB-TABLE-251](#ddb-table-251)).
- Overrides are the per-region view: ProvisionedThroughputOverride (RCU only) and OnDemandThroughputOverride (read cap only) are absent when inherited, cleared by sending the base value, accepted again on a same-value re-send (~35 s UPDATING) and never echoed in the UpdateTable response; replica GSI overrides merge per index (unnamed indexes keep theirs); each region labels the OTHER region's cap as its 'override' - treat absent == equal-to-base and read only the region you manage ([DDB-TABLE-250](#ddb-table-250), [DDB-TABLE-252](#ddb-table-252), [DDB-TABLE-422](#ddb-table-422)).
- Delete leaf-first: DeleteTable on the source, and Delete of a region that sourced another replica <24 h ago, fail with 'acted as a source region ... last 24 hours' until the sourced replica is gone (retry-later, never terminal - but the one-Delete-per-reconcile finalizer in templates/hooks/table/sdk_delete_pre_build_request.go.tpl:8-22 ([GT-DDB-008](service.md#gt-ddb-008) (controller hooks catalog entry)) hits it as a ValidationException the generator mapping treats as terminal); a replica with its own DeletionProtection refuses both ReplicaUpdates.Delete and a regional DeleteTable; a regional DeleteTable on an unprotected replica is allowed and shows on the base as DELETING then a missing entry (pure drift) ([DDB-TABLE-266](#ddb-table-266), [DDB-TABLE-267](#ddb-table-267), [DDB-TABLE-297](#ddb-table-297)).
- 'one or more replicas already existed as tables' covers a foreign same-name table, a duplicate Create and a replica still DELETING (~20 s after its Replicas[] entry vanished): DescribeTable in the target region tells them apart and re-adding is admitted only once that region returns ResourceNotFoundException ([DDB-TABLE-225](#ddb-table-225), [DDB-TABLE-308](#ddb-table-308)).
- The UpdateTable 200 is not durable: a Create aborted asynchronously (KMS key disabled seconds later) leaves no Replicas entry and no CREATION_FAILED - re-read Replicas[] after ACTIVE and re-add a missing region; an already-disabled key is rejected synchronously with the misleading 'replica server-side encryption status is in UPDATING state' text; in a genuine CREATING window (~11 min for a 1-item table) every other UpdateTable incl. a cancelling Delete is ResourceInUseException and UpdateTimeToLive is blocked ([DDB-TABLE-310](#ddb-table-310), [DDB-TABLE-312](#ddb-table-312); [DDB-TABLE-306](table-policy-kinesis-autoscaling.md#ddb-table-306), table-subresources.md).
- KMS: on a CMK table every Create must carry a KMSMasterKeyId resolvable in the replica region (a bare id or mrk-id is resolved there and read back as the full regional ARN; the other region's ARN of a multi-region key is 'Invalid arn'), all replicas share one key type (no alias/aws/dynamodb on a CMK group), an AWS-owned-key table rejects any per-replica key, and ReplicaUpdates.Update{KMSMasterKeyId} is rejected unconditionally - rotate via SSESpecification in the replica region; a disabled replica key surfaces only as replica-region data-plane errors and a hidden SSE state that blocks new Creates, ReplicaStatus turns INACCESSIBLE_ENCRYPTION_CREDENTIALS only after ~80 min with the source still writable, and the broken replica can be dropped regionally once DP is off ([DDB-TABLE-313](#ddb-table-313), [DDB-TABLE-309](#ddb-table-309), [DDB-TABLE-295](#ddb-table-295), [DDB-TABLE-311](#ddb-table-311), [DDB-TABLE-329](#ddb-table-329), [DDB-TABLE-327](#ddb-table-327); [DDB-TABLE-328](table-streams-encryption-class.md#ddb-table-328), table-streams-encryption-class.md).
- Autoscaling coupling: AAS is per-region state (write targets/policies/alarms mirrored on Create, orphaned by replica removal and DeleteTable, not re-armed by a same-name re-create); a target without a policy reads as AutoScalingDisabled:true yet enforces its MinCapacity; raising Min triggers an AAS UpdateTable within ~4 s while a manual value below Min is never re-enforced; source read scaling pins the replica through a server-materialized ProvisionedThroughputOverride; PolicyName is the identity (a new name replaces); DescribeTable carries no autoscaling indicator - treat provisionedThroughput as unmanaged when a target exists ([DDB-TABLE-194](#ddb-table-194), [DDB-TABLE-292](#ddb-table-292), [DDB-TABLE-321](#ddb-table-321); [DDB-TABLE-195](table-policy-kinesis-autoscaling.md#ddb-table-195), [DDB-TABLE-315](table-policy-kinesis-autoscaling.md#ddb-table-315), [DDB-TABLE-240](table-policy-kinesis-autoscaling.md#ddb-table-240), [DDB-TABLE-193](table-policy-kinesis-autoscaling.md#ddb-table-193), table-subresources.md).

### Timing you should expect
- Replica Create on an empty table: 14.4-49 s to ACTIVE (n=7: [DDB-TABLE-305](#ddb-table-305) x4, [DDB-TABLE-309](#ddb-table-309), [DDB-TABLE-249](#ddb-table-249), [DDB-TABLE-308](#ddb-table-308); entry visible at ~6 s, [DDB-TABLE-226](#ddb-table-226)), 161.5 s with an OnDemandThroughputOverride (source UPDATING with no entry for 149 s, [DDB-TABLE-422](#ddb-table-422)); with a single item 686 s, the base flipping ACTIVE -> UPDATING -> ACTIVE meanwhile ([DDB-TABLE-306](table-policy-kinesis-autoscaling.md#ddb-table-306)).
- Replica Delete: source UPDATING 31-47 s, then ACTIVE with the replica DELETING; entry gone after 121-244 s (n=5: [DDB-TABLE-222](#ddb-table-222), [DDB-TABLE-308](#ddb-table-308), [DDB-TABLE-297](#ddb-table-297), [DDB-TABLE-267](#ddb-table-267), [DDB-TABLE-422](#ddb-table-422); 36 s / 88 s / 125 s in [DDB-TABLE-204](#ddb-table-204)); the replica-region table 404s ~10-35 s after the entry vanishes and only then is a re-add admitted ([DDB-TABLE-308](#ddb-table-308), [DDB-TABLE-297](#ddb-table-297), [DDB-TABLE-228](#ddb-table-228)).
- Fan-out: override set/clear ~35-38 s ([DDB-TABLE-250](#ddb-table-250), [DDB-TABLE-422](#ddb-table-422)); same-value TableClassOverride 34-58 s ([DDB-TABLE-294](#ddb-table-294)); DP toggle with a replica 34-39 s ([DDB-TABLE-230](#ddb-table-230), [DDB-TABLE-297](#ddb-table-297)); PT propagation 38-39 s ([DDB-TABLE-251](#ddb-table-251)); GSI add with a replica 541-575 s vs ~1 min regional, GSI delete ~38 s ([DDB-TABLE-296](#ddb-table-296)); replica INACCESSIBLE detected 4783 s after DisableKey ([DDB-TABLE-329](#ddb-table-329)).
- Autoscaling: a raised Min changes RCU at 4.4 s and settles at 33 s ([DDB-TABLE-314](table-policy-kinesis-autoscaling.md#ddb-table-314)); Describe reflects an UpdateTableReplicaAutoScaling in 0.43-0.63 s while the response is stale ([DDB-TABLE-238](#ddb-table-238)); a manual value below Min stayed for 726 s ([DDB-TABLE-315](table-policy-kinesis-autoscaling.md#ddb-table-315)); a disabled replica CMK produced no INACCESSIBLE status within 25 min ([DDB-TABLE-311](#ddb-table-311)).

### Known handling gaps in the controller
- The hooks catalog records that the AAS prerequisite cannot be satisfied by the controller and that ValidationException is terminal (generator.yaml:88-90; tracked in community issue 2610; [GT-DDB-036](service.md#gt-ddb-036) (controller hooks catalog entry)); confirmed: 'Table write capacity should either be Pay-Per-Request or AutoScaled' wedges a PROVISIONED CR although registering AAS targets+policies out of band makes the next attempt succeed, so the text must be requeued, not terminal ([DDB-TABLE-188](#ddb-table-188), a duplicate entry whose canonical is cited under PROVISIONED prerequisites above).
- The hooks catalog records that setTableReplicas maps ReplicaDescription back onto the spec and equalReplicaArrays compares by plain equality (pkg/resource/table/hooks_replica_updates.go:423-463, 28-147; [GT-DDB-034](service.md#gt-ddb-034) (controller hooks catalog entry)); confirmed for KMS: a bare key id or alias in the spec is read back as the full ARN, and the ReplicaUpdates.Update{KMSMasterKeyId} the perpetual delta triggers is rejected unconditionally - terminal, not merely unsynced ([DDB-TABLE-309](#ddb-table-309)).
- The hooks catalog records that updateReplicaUpdate returns an empty update when no expressible change exists and the caller requeues (pkg/resource/table/hooks_replica_updates.go:39-45, 166-171; [GT-DDB-035](service.md#gt-ddb-035) (controller hooks catalog entry)); confirmed: AAS read scaling materializes a Replicas[].ProvisionedThroughputOverride nobody set and DescribeTable echoes every GSI without overrides, so the hook requeues with requeueWaitReplicasActive forever, and an expressible same-value Update churns ~35 s ([DDB-TABLE-321](#ddb-table-321)).

### Where to look next
- MRSC/STRONG groups and witnesses (two Creates in one call is the STRONG exception), the group's Describe shape and the legacy GlobalTable verdicts ([DDB-TABLE-258](table-global-tables.md#ddb-table-258), [DDB-TABLE-229](table-global-tables.md#ddb-table-229), [DDB-TABLE-231](table-global-tables.md#ddb-table-231), table-global-tables.md); AAS target semantics and GSI autoscaling auto-registration ([DDB-TABLE-322](table-policy-kinesis-autoscaling.md#ddb-table-322), table-subresources.md); stream/SSE rules on regional tables (table-streams-encryption-class.md); GSI rules (table-indexes.md). Scope: TableReplicaAutoScaling is skip ([DDB-TABLEREPLICAAUTOSCALING-001](#ddb-tablereplicaautoscaling-001)).
- Controller: pkg/resource/table/hooks_replica_updates.go, templates/hooks/table/sdk_delete_pre_build_request.go.tpl, generator.yaml terminal codes. Evidence: services/dynamodb/probes/table/{cross-region,dependencies,mutation-matrix,state-machine,sub-resources}/ (replica-*, autoscaling-*).

Entries below are generated from the lab findings; low-impact items are in the appendix, long notes under details/.
<!-- preserved:end -->

## At a glance

- canonical findings: 51 (high 36 / medium 12 / low 3); duplicates folded into the appendix: 6
- handling: handled 12 · partial 0 · tracked 1 · unhandled 35 · suspect-bug 3 · n-a 0 (tracked = handled/partial whose reference is an open GitHub issue; counted as not handled)
- re-verified: 0 · last_verified: 2026-10-09 · model: 2012-08-10 (service/dynamodb v1.39.8)
- categories: cross-region 8, async-state-machine 7, prerequisite 6, delete-semantics 5, error-code 5,
  normalization 3, other 3, update-granularity 3, eventual-consistency 1, identity 1, immutable-field 1,
  read-gap 1, request-validation 1, requested-vs-effective 1, response-fidelity 1, scope 1, shape-mismatch 1,
  stale-response 1, unsettable-field 1

## Operations

| operation | kind | required inputs | declared error shapes | paginated |
| --- | --- | --- | --- | --- |
| CreateBackup | create | TableName, BackupName | TableNotFoundException, TableInUseException, ContinuousBackupsUnavailableException, BackupInUseException, LimitExceededException, InternalServerError | no |
| CreateTable | create | TableName | ResourceInUseException, LimitExceededException, InternalServerError | no |
| DeleteTable | delete | TableName | ResourceInUseException, ResourceNotFoundException, LimitExceededException, InternalServerError | no |
| DescribeTable | read | TableName | ResourceNotFoundException, InternalServerError | no |
| DescribeTableReplicaAutoScaling | read | TableName | ResourceNotFoundException, InternalServerError | no |
| GetItem | read | TableName, Key | ProvisionedThroughputExceededException, ResourceNotFoundException, RequestLimitExceeded, InternalServerError, ThrottlingException | no |
| PutItem | create | TableName, Item | ConditionalCheckFailedException, ProvisionedThroughputExceededException, ResourceNotFoundException, ItemCollectionSizeLimitExceededException, TransactionConflictException, RequestLimitExceeded, InternalServerError, ReplicatedWriteConflictException, ThrottlingException | no |
| PutResourcePolicy | create | ResourceArn, Policy | ResourceNotFoundException, InternalServerError, LimitExceededException, PolicyNotFoundException, ResourceInUseException | no |
| TagResource | tag | ResourceArn, Tags | LimitExceededException, ResourceNotFoundException, InternalServerError, ResourceInUseException | no |
| UpdateContinuousBackups | update | TableName, PointInTimeRecoverySpecification | TableNotFoundException, ContinuousBackupsUnavailableException, InternalServerError | no |
| UpdateContributorInsights | update | TableName, ContributorInsightsAction | ResourceNotFoundException, InternalServerError | no |
| UpdateTable | update | TableName | ResourceInUseException, ResourceNotFoundException, LimitExceededException, InternalServerError | no |
| UpdateTableReplicaAutoScaling | update | TableName | ResourceNotFoundException, ResourceInUseException, LimitExceededException, InternalServerError | no |
| UpdateTimeToLive | update | TableName, TimeToLiveSpecification | ResourceInUseException, ResourceNotFoundException, LimitExceededException, InternalServerError | no |

Also referenced by findings but not in the dynamodb model (other services or annotated variants):
DescribeScalableTargets, PutScalingPolicy, RegisterScalableTarget

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

- <a id="ddb-table-226"></a>**DDB-TABLE-226** `async-state-machine` · impact high · handled · verified 2026-10-09
  **Replica Create on an EMPTY table completes in ~20-30s; the UpdateTable response shows Replicas=[] (entry appears ~6s later)**
  UpdateTable ReplicaUpdates=[Create us-east-1] on an empty PPR table returned after 2.0s with
  TableStatus=UPDATING, GlobalTableVersion=2019.11.21 and Replicas=[] (empty list, no CREATING entry yet).
  Polling both regions every 3s: A=UPDATING with Replicas=[] for 6.4s; then A Replicas=[us-east-1 CREATING]
  while us-east-1 DescribeTable still returned ResourceNotFoundException for 3.4s more; us-east-1 appeared at
  10.1s as TableStatus=CREATING with Replicas=[us-west-2 ACTIVE] and GlobalTableVersion already set; both
  regions ACTIVE at 20.9s (total 28s incl. the call). ReplicaStatusPercentProgress was never present. A later
  re-add of the same region took 23s. Non-empty tables behave very differently (see
  table/dependencies/replica-prerequisites and mutation-matrix/replica-overrides: with a single item the
  replica stays CREATING for many minutes while the base returns to ACTIVE).
  - ACK: synced.when, requeue, e2e-timing · ops: UpdateTable, DescribeTable · fields: ReplicaUpdates,
    Replicas, TableStatus
  - repro: PPR table with NEW_AND_OLD_IMAGES stream -> UpdateTable ReplicaUpdates=[Create us-east-1] -> poll
    DescribeTable in both regions every 3s
  - measurements: create_total_s=28.0, update_table_latency_ms=2046, replicas_entry_appears_s=6.7,
    replica_region_visible_s=10.1, readd_total_s=23.0
  - handling: handled via `generator.yaml:27-31; pkg/resource/table/hooks_replica_updates.go:277-373; pkg/resource/table/hooks_replica_updates.go:465-476; pkg/resource/table/hooks.go:89-92; test/e2e/tests/test_table_replicas.py:200-206; test/e2e/tests/test_table.py:34`
  - related: [DDB-TABLE-306](table-policy-kinesis-autoscaling.md#ddb-table-306), [DDB-TABLE-265](#ddb-table-265), [DDB-TABLE-308](#ddb-table-308), [DDB-TABLE-309](#ddb-table-309), [DDB-TABLE-329](#ddb-table-329), [DDB-TABLE-187](#ddb-table-187),
    [DDB-TABLE-305](#ddb-table-305), [DDB-TABLE-249](#ddb-table-249), [DDB-TABLE-258](table-global-tables.md#ddb-table-258), [DDB-TABLE-204](#ddb-table-204), [DDB-TABLE-261](table-global-tables.md#ddb-table-261), [DDB-TABLE-250](#ddb-table-250), [DDB-TABLE-294](#ddb-table-294),
    [DDB-TABLE-296](#ddb-table-296), [DDB-TABLE-221](#ddb-table-221) · hypotheses: H-R-005, H-R-008 · evidence:
    table/state-machine/replica-create-timeline
  - notes: Qualifies H-R-005 (durations are seconds for an empty table; the Replicas entry lags the 200 by ~6s
    so a controller reading back immediately sees no replica at all) and H-R-008 (replica region NotFound
    window ~10s; A's Replicas[] is the earlier signal by ~3s).
  - full notes: [details/DDB-TABLE-226.md](details/DDB-TABLE-226.md)

- <a id="ddb-table-227"></a>**DDB-TABLE-227** `async-state-machine` · impact high · unhandled (not handled in controller) · verified 2026-10-09
  **Admissibility while a replica is CREATING: which UpdateTable / tag / TTL / PITR / policy / backup calls succeed**
  Issued 3-7s after ReplicaUpdates.Create (A: TableStatus=UPDATING, Replicas still []): UpdateTable Create
  eu-west-1 / duplicate Create us-east-1 / DeletionProtectionEnabled / TableClass -> ResourceInUseException
  'The resource which you are attempting to change is in use.' (HTTP 400). UpdateTable Update{us-east-1
  TableClassOverride} and ReplicaUpdates Delete us-east-1 -> ValidationException 'Update global table
  operation failed because one or more replicas were not part of the global table. Please retry the request
  without these replicas: ...' (the entry did not exist yet). StreamSpecification{StreamEnabled:false} ->
  ValidationException 'Disabling Stream is not allowed for a Global Table replica.' TagResource,
  UpdateTimeToLive, UpdateContinuousBackups(PITR), PutResourcePolicy, UpdateContributorInsights, CreateBackup
  and DescribeTableReplicaAutoScaling all succeeded (200) during the window.
  - ACK: updateable.when, requeue, custom_update · ops: UpdateTable, TagResource, UpdateTimeToLive,
    UpdateContinuousBackups, PutResourcePolicy, UpdateContributorInsights, CreateBackup,
    DescribeTableReplicaAutoScaling
  - repro: UpdateTable Create replica; within 10s issue each op; record code
  - handling: not handled in the controller (as of commit 34b85e6)
  - related: [DDB-BACKUP-001](backup.md#ddb-backup-001), [DDB-BACKUP-007](backup.md#ddb-backup-007), [DDB-TABLE-100](table-restore.md#ddb-table-100), [DDB-TABLE-118](service.md#ddb-table-118), [DDB-TABLE-087](table-restore.md#ddb-table-087), [DDB-TABLE-454](table-subresources.md#ddb-table-454),
    [DDB-TABLE-455](service.md#ddb-table-455), [DDB-TABLE-228](#ddb-table-228) · hypotheses: H-R-006, H-R-026, H-R-024 · evidence:
    table/state-machine/replica-create-timeline
  - notes: Confirms H-R-006 for the UpdateTable half (ResourceInUseException for any other UpdateTable
    mutation, including a second replica action) and for the sub-resource half
    (tags/TTL/PITR/policy/insights/backup are admitted). H-R-026 (cancel during CREATING) could not be
    exercised here because Replicas[]...
  - full notes: [details/DDB-TABLE-227.md](details/DDB-TABLE-227.md)

- <a id="ddb-table-228"></a>**DDB-TABLE-228** `async-state-machine` · impact high · unhandled (not handled in controller) · verified 2026-10-09
  **Admissibility while a replica is DELETING, and A-entry-gone vs B-ResourceNotFound ordering**
  Issued 3-7s after ReplicaUpdates.Delete (A: UPDATING, Replicas=[us-east-1 ACTIVE]): Create eu-west-1 /
  duplicate Delete / DeletionProtectionEnabled -> ResourceInUseException; Create us-east-1 (re-add while
  deleting) -> ValidationException 'because one or more replicas already existed as tables'; TagResource,
  UpdateContinuousBackups, DeleteResourcePolicy, UpdateContributorInsights, DescribeTableReplicaAutoScaling ->
  200; UpdateTimeToLive -> ValidationException 'Time to live has been modified multiple times within a fixed
  interval' (TTL rate limit, not state). Timeline: A UPDATING[us-east-1 ACTIVE] + B UPDATING for 46s -> A
  ACTIVE[us-east-1 DELETING] + B DELETING for 53s -> A UPDATING[us-east-1 DELETING] for 108s -> A ACTIVE with
  Replicas gone at 208s while B was still DELETING (Replicas=[] and GlobalTableVersion already dropped in B)
  -> B ResourceNotFoundException at 242s. Re-add after B was gone: 200 immediately. The second delete (after
  re-add) took 97s with no second UPDATING phase.
  - ACK: deletable.when, requeue, updateable.when · ops: UpdateTable, DescribeTable · fields: ReplicaUpdates,
    Replicas
  - repro: UpdateTable ReplicaUpdates=[Delete us-east-1]; poll both regions; re-issue Create as soon as
    Replicas is empty
  - measurements: delete_total_s=242.3, a_entry_gone_s=207.8, b_notfound_s=242.3, readd_delete_total_s=96.8
  - handling: not handled in the controller (as of commit 34b85e6)
  - related: [DDB-BACKUP-001](backup.md#ddb-backup-001), [DDB-BACKUP-007](backup.md#ddb-backup-007), [DDB-TABLE-100](table-restore.md#ddb-table-100), [DDB-TABLE-118](service.md#ddb-table-118), [DDB-TABLE-087](table-restore.md#ddb-table-087), [DDB-TABLE-454](table-subresources.md#ddb-table-454),
    [DDB-TABLE-455](service.md#ddb-table-455), [DDB-TABLE-227](#ddb-table-227) · hypotheses: H-R-006, H-R-026 · evidence:
    table/state-machine/replica-create-timeline
  - notes: Confirms H-R-006 for DELETING. Qualifies H-R-026: A's Replicas[] empties ~35s before the replica
    table disappears; a Create issued in that gap is rejected with the 'already existed as tables'
    ValidationException (observed via the re-add-while-deleting op); the exact gap behaviour is measured in...
  - full notes: [details/DDB-TABLE-228.md](details/DDB-TABLE-228.md)

- <a id="ddb-table-298"></a>**DDB-TABLE-298** `async-state-machine` · impact high · unhandled (not handled in controller) · verified 2026-10-09
  **A CREATING replica is visible to DescribeTable only (other regional calls -> NotFound); DeleteTable on it -> ResourceInUseException**
  1-item source table; the us-east-1 replica table became visible 68s after Create as TableStatus=CREATING
  with Replicas=[us-west-2 ACTIVE], GlobalTableVersion set, no StreamSpecification/LatestStreamArn yet, its
  own TableId. While CREATING, from the us-east-1 endpoint: DescribeTimeToLive / ListTagsOfResource /
  UpdateTable(DP) / TagResource / UpdateTimeToLive / PutItem / GetItem -> ResourceNotFoundException
  ('Requested resource not found: Table: <name> not found'); DescribeContinuousBackups /
  UpdateContinuousBackups -> TableNotFoundException ('Table not found: <name>');
  DescribeTableReplicaAutoScaling -> 200; ReplicaUpdates Create eu-west-1 -> ValidationException
  'Create/Update/Delete of replica is not allowed while the replica is being added to table with name: <name>
  in region ...'; ReplicaUpdates Delete us-west-2 (the source) -> ValidationException 'Replica cannot be
  deleted because it has acted as a source region for new replica(s) being added to the table in the last 24
  hours.'; DeleteTable on the CREATING replica -> ResourceInUseException 'Attempt to change a resource which
  is still in use: Table: <name> is being used.' PutItem on the base during CREATING -> 200 and the item was
  present in the replica afterwards. Base: ACTIVE[us-east-1 CREATING] 175s -> UPDATING[CREATING] 309s -> both
  ACTIVE at 494s (+68s). Replica ItemCount reported 0 (stale) although items exist.
  - ACK: updateable.when, deletable.when, custom_find, requeue · ops: DescribeTable, UpdateTable, DeleteTable,
    TagResource, UpdateTimeToLive, UpdateContinuousBackups, PutItem, GetItem · fields: Replicas, TableStatus,
    DeletionProtectionEnabled
  - repro: 1-item PPR table -> UpdateTable Create us-east-1 -> wait until us-east-1 DescribeTable succeeds ->
    issue each op against us-east-1 -> poll both regions
  - measurements: replica_visible_after_s=67.9, one_item_create_total_s=562.2
  - handling: not handled in the controller (as of commit 34b85e6)
  - related: [DDB-BACKUP-001](backup.md#ddb-backup-001), [DDB-TABLE-217](table-restore.md#ddb-table-217), [DDB-IMPORT-001](import.md#ddb-import-001), [DDB-TABLE-447](service.md#ddb-table-447), [DDB-TABLE-277](table-restore.md#ddb-table-277), [DDB-IMPORT-006](import.md#ddb-import-006) ·
    hypotheses: H-R-006, H-R-007, H-R-009, H-R-026 · evidence: table/state-machine/replica-creating-side-ops
  - notes: Qualifies H-R-006/H-R-007: during CREATING the replica region's DescribeTable works but
    sub-resource APIs disagree on the not-found code (ResourceNotFoundException vs TableNotFoundException for
    continuous backups). H-R-009/H-R-026: the creation cannot be cancelled from either side...
  - full notes: [details/DDB-TABLE-298.md](details/DDB-TABLE-298.md)

- <a id="ddb-table-310"></a>**DDB-TABLE-310** `async-state-machine` · impact high · unhandled (not handled in controller) · verified 2026-10-09
  **Replica Create aborted asynchronously (KMS key disabled seconds after Create) leaves no trace: no Replicas entry, no CREATION_FAILED**
  Create eu-west-1 with an ENABLED eu-west-1 CMK -> 200 (5.1s; response Replicas=[us-east-1 ACTIVE] only). KMS
  DisableKey 7.7s later while A was UPDATING with no eu-west-1 entry yet. A returned to ACTIVE at ~12s with
  Replicas=[us-east-1 ACTIVE] only; eu-west-1 DescribeTable -> ResourceNotFoundException immediately and still
  60s later; no CREATION_FAILED entry, no ReplicaStatusDescription anywhere. Attempt 1 of this probe (run
  c0ae7d, evidence.jsonl) reproduced the same for us-east-1: DisableKey 5s after Create -> UPDATING[] ->
  ACTIVE[] at 6.5s, table never created. By contrast a Create with an ALREADY disabled key is rejected
  synchronously: ValidationException 'Operation cannot be performed while replica server-side encryption
  status is in UPDATING state. Please retry the request after the status is updated to ENABLED.'
  - ACK: requeue, synced.when, terminal_codes · ops: UpdateTable, DescribeTable · fields: ReplicaUpdates,
    Replicas, TableStatus
  - repro: UpdateTable ReplicaUpdates=[Create eu-west-1 KMSMasterKeyId=<enabled eu-west-1 CMK>]; 2s later kms
    disable-key; poll DescribeTable
  - measurements: abort_total_s=11.8
  - handling: not handled in the controller (as of commit 34b85e6)
  - related: [DDB-TABLE-313](#ddb-table-313), [DDB-TABLE-309](#ddb-table-309), [DDB-TABLE-295](#ddb-table-295), [DDB-TABLE-328](table-streams-encryption-class.md#ddb-table-328), [DDB-TABLE-312](#ddb-table-312), [DDB-TABLE-311](#ddb-table-311),
    [DDB-TABLE-329](#ddb-table-329), [DDB-TABLE-327](#ddb-table-327) · hypotheses: H-R-023 · evidence: table/state-machine/replica-kms-lifecycle
  - notes: Refutes H-R-023 for this failure mode: the UpdateTable 200 is not a durable acknowledgement and no
    CREATION_FAILED state is surfaced. A controller must re-read Replicas[] after the table returns to ACTIVE
    and treat a missing entry as 'add again' (retryable), not as success.

- <a id="ddb-table-311"></a>**DDB-TABLE-311** `async-state-machine` · impact medium · unhandled (not handled in controller) · verified 2026-10-09
  **Replica CMK disabled: INACCESSIBLE status lags (none within 25 min; ~80 min in 329); control plane works, replica-region data plane fails**
  KMS DisableKey on the us-east-1 replica's CMK; both regions polled every 30s for 1500s: TableStatus and
  ReplicaStatus stayed ACTIVE everywhere, SSEDescription.Status stayed ENABLED, no
  ReplicaInaccessibleDateTime. Meanwhile: PutItem in us-west-2 -> 200; PutItem/GetItem in us-east-1 ->
  ValidationException 'KMS key disabled error: ... DisabledException: <key arn> is disabled'; ReplicaUpdates
  Update{TableClassOverride} on that replica -> 200; DeletionProtection on base and on the replica -> 200;
  TagResource -> 200; Create eu-west-1 -> ValidationException 'Operation cannot be performed while replica
  server-side encryption status is in UPDATING state...'; re-Create us-east-1 -> 'already existed as tables'.
  After EnableKey everything was ACTIVE at once (nothing to recover).
  - ACK: synced.when, requeue, terminal_codes · ops: UpdateTable, DescribeTable, PutItem, GetItem, TagResource
    · fields: Replicas.ReplicaStatus, Replicas.ReplicaInaccessibleDateTime, TableStatus
  - repro: replica with its own CMK ACTIVE -> kms disable-key in the replica region -> poll DescribeTable in
    both regions every 30s (bounded 1500s)
  - measurements: inaccessible_watch_s=1500, inaccessible_detect_s=null
  - handling: not handled in the controller (as of commit 34b85e6)
  - related: [DDB-TABLE-297](#ddb-table-297), [DDB-TABLE-327](#ddb-table-327), [DDB-TABLE-263](table-global-tables.md#ddb-table-263), [DDB-TABLE-329](#ddb-table-329), [DDB-TABLE-313](#ddb-table-313), [DDB-TABLE-309](#ddb-table-309),
    [DDB-TABLE-295](#ddb-table-295), [DDB-TABLE-328](table-streams-encryption-class.md#ddb-table-328), [DDB-TABLE-312](#ddb-table-312), [DDB-TABLE-310](#ddb-table-310), [DDB-TABLE-442](service.md#ddb-table-442), [DDB-TABLE-266](#ddb-table-266), [DDB-TABLE-267](#ddb-table-267) ·
    hypotheses: H-R-034 · evidence: table/state-machine/replica-kms-lifecycle
  - notes: Partially refutes H-R-034: within 25 minutes the replica never reported
    INACCESSIBLE_ENCRYPTION_CREDENTIALS (AWS documents a detection window; it is longer than 25 min or only
    applies to the table's own region). The only signals are data-plane ValidationExceptions in the replica
    region and a hidden...
  - full notes: [details/DDB-TABLE-311.md](details/DDB-TABLE-311.md)

- <a id="ddb-table-312"></a>**DDB-TABLE-312** `async-state-machine` · impact high · unhandled (not handled in controller) · verified 2026-10-09
  **Replica Create with an already-disabled replica-region KMS key is rejected synchronously (SSE 'UPDATING state' message)**
  Base table encrypted with a us-west-2 CMK; a us-east-1 CMK created and disabled beforehand. UpdateTable
  ReplicaUpdates=[Create us-east-1 KMSMasterKeyId=<disabled key ARN>] -> ValidationException (HTTP 400, 2.1s)
  'Operation cannot be performed while replica server-side encryption status is in UPDATING state. Please
  retry the request after the status is updated to ENABLED.' No replica entry is created; the table stays
  ACTIVE. The message suggests a transient state although the cause (disabled key) is permanent until
  EnableKey.
  - ACK: terminal_codes, requeue, custom_update · ops: UpdateTable, DescribeTable · fields:
    ReplicaUpdates.Create.KMSMasterKeyId, Replicas.ReplicaStatus, Replicas.ReplicaStatusDescription
  - repro: KMS CreateKey in us-east-1 + DisableKey; table with CMK SSE in us-west-2; UpdateTable
    ReplicaUpdates=[Create us-east-1 KMSMasterKeyId=<disabled key>]
  - handling: not handled in the controller (as of commit 34b85e6)
  - related: [DDB-TABLE-313](#ddb-table-313), [DDB-TABLE-309](#ddb-table-309), [DDB-TABLE-295](#ddb-table-295), [DDB-TABLE-328](table-streams-encryption-class.md#ddb-table-328), [DDB-TABLE-310](#ddb-table-310), [DDB-TABLE-311](#ddb-table-311),
    [DDB-TABLE-329](#ddb-table-329), [DDB-TABLE-327](#ddb-table-327), [DDB-TABLE-442](service.md#ddb-table-442), [DDB-TABLE-266](#ddb-table-266), [DDB-TABLE-267](#ddb-table-267) · hypotheses: H-R-023, H-R-016 ·
    evidence: table/state-machine/replica-creation-failed
  - notes: Qualifies H-R-023: with a key that is already disabled there is no asynchronous CREATION_FAILED
    path - the check is synchronous but the error text is misleading ('retry after ... ENABLED'). The async
    case (key disabled right after Create) is in table/state-machine/replica-kms-lifecycle (silent...
  - full notes: [details/DDB-TABLE-312.md](details/DDB-TABLE-312.md)

## Identity and lookup

- <a id="ddb-table-225"></a>**DDB-TABLE-225** `identity` · impact high · unhandled (not handled in controller) · verified 2026-10-09
  **ReplicaUpdates.Create is rejected when a standalone table with the same name already exists in the target region**
  With a non-replica table of the same name ACTIVE in us-east-1, UpdateTable ReplicaUpdates=[Create us-east-1]
  fails synchronously (~0.6s) with ValidationException 'Failed to create a the new replica of table with name:
  <name> because one or more replicas already existed as tables.' The same code/message is returned while that
  table is DELETING (so the controller cannot distinguish 'leftover table' from 'replica being torn down').
  The existing table is never adopted; the message does not name the region. The identical message is returned
  for a duplicate Create of an existing replica (after ACTIVE, and while the replica is DELETING).
  - ACK: terminal_codes, requeue · ops: UpdateTable, CreateTable · fields: ReplicaUpdates
  - repro: CreateTable X in us-east-1; CreateTable X (streams) in us-west-2; UpdateTable X
    ReplicaUpdates=[Create us-east-1]
  - handling: not handled in the controller (as of commit 34b85e6)
  - related: [DDB-TABLE-203](#ddb-table-203), [DDB-TABLE-223](#ddb-table-223), [DDB-TABLE-307](#ddb-table-307), [DDB-TABLE-308](#ddb-table-308), [DDB-TABLE-294](#ddb-table-294), [DDB-TABLE-437](table-throughput-billing.md#ddb-table-437) ·
    hypotheses: H-R-025 · evidence: table/state-machine/replica-create-timeline
  - notes: Confirms H-R-025. The same message covers three situations (foreign same-name table, replica
    already present, replica still DELETING in its region); a controller must DescribeTable in the target
    region to tell them apart and treat it as retryable only in the DELETING case.
  - full notes: [details/DDB-TABLE-225.md](details/DDB-TABLE-225.md)

## Errors

- <a id="ddb-table-190"></a>**DDB-TABLE-190** `error-code` · impact high · unhandled (not handled in controller) · verified 2026-10-09
  **UpdateTableReplicaAutoScaling on a PAY_PER_REQUEST global table: ValidationException**
  write Min 1 Max 10 Target 70 -> {"operation": "update_table_replica_auto_scaling", "ok": false, "code":
  "ValidationException", "http_status": 400, "message": "Failed to update global table with name
  ‘ackq-0ad4a5-as-ppr‘. Replicas 'US-EAST-1', 'US-WEST-2' BillingMode is PayPerRequest. You must convert
  table's BillingMode to PROVISIONED to set parameters:
  'GlobalTableProvisionedWriteCapacityAutoScalingSettingsUpdate'.", "latency_ms": 459, "client_side": false};
  AutoScalingDisabled=true only -> {"operation": "update_table_replica_auto_scaling", "ok": true, "code":
  null, "http_status": 200, "message": null, "latency_ms": 1035, "client_side": false}; ReplicaUpdates read
  update -> {"operation": "update_table_replica_auto_scaling", "ok": false, "code": "ValidationException",
  "http_status": 400, "message": "Failed to update global table with name ‘ackq-0ad4a5-as-ppr‘. Replicas
  'US-EAST-1', 'US-WEST-2' BillingMode is PayPerRequest. You must convert table's BillingMode to PROVISIONED
  to set parameters: 'ReplicaProvisionedReadCapacityAutoScalingSettingsUpdate'.", "latency_ms": 671,
  "client_side": false}. AAS targets afterwards: []. Describe on the PPR global table: {"operation":
  "describe_table_replica_auto_scaling", "ok": true, "code": null, "http_status": 200, "message": null,
  "latency_ms": 453, "client_side": false}.
  - ACK: terminal_codes, custom_update · ops: UpdateTableReplicaAutoScaling · fields: BillingMode,
    ProvisionedWriteCapacityAutoScalingUpdate
  - repro: PAY_PER_REQUEST table with a replica -> UpdateTableReplicaAutoScaling
  - handling: not handled in the controller (as of commit 34b85e6)
  - related: [DDB-TABLE-317](table-policy-kinesis-autoscaling.md#ddb-table-317), [DDB-TABLE-232](#ddb-table-232), [DDB-TABLE-189](table-policy-kinesis-autoscaling.md#ddb-table-189) · hypotheses: H-S-040 · evidence:
    table/sub-resources/replica-autoscaling-facade

- <a id="ddb-table-203"></a>**DDB-TABLE-203** `error-code` · impact medium · unhandled (not handled in controller) · verified 2026-10-09
  **ReplicaUpdates duplicate taxonomy: Create existing / Delete non-member / Create local region are all ValidationException**
  ReplicaUpdates=[Create <existing member>] -> ValidationException "Failed to create a the new replica of
  table with name: '<name>' because one or more replicas already existed as tables." (sic); [Delete
  ca-central-1] (non-member) -> ValidationException "Update global table operation failed because one or more
  replicas were not part of the global table. Please retry the request without these replicas:
  [ca-central-1]."; [Create us-west-2] (the table's own region) -> ValidationException "Cannot add or delete
  the local region through ReplicaUpdates. Use CreateTable, DeleteTable, or UpdateTable as required." None of
  these use the ReplicaAlreadyExistsException / ReplicaNotFoundException codes declared for the legacy API.
  - ACK: terminal_codes, custom_update · ops: UpdateTable · fields: ReplicaUpdates
  - repro: EVENTUAL group -> UpdateTable ReplicaUpdates=[Create <member>]; [Delete <non-member>]; [Create
    <local region>]
  - handling: not handled in the controller (as of commit 34b85e6)
  - related: [DDB-TABLE-223](#ddb-table-223), [DDB-TABLE-307](#ddb-table-307), [DDB-TABLE-225](#ddb-table-225), [DDB-TABLE-308](#ddb-table-308), [DDB-TABLE-294](#ddb-table-294), [DDB-TABLE-437](table-throughput-billing.md#ddb-table-437) ·
    hypotheses: H-R-001, H-R-042 · evidence: table/cross-region/mrec-replica-updates
  - notes: A controller diffing spec.replicas against status must pre-filter the local region and
    already-present members or it will hit terminal ValidationExceptions; message text (not code) is the only
    way to tell these apart.
  - full notes: [details/DDB-TABLE-203.md](details/DDB-TABLE-203.md)

- <a id="ddb-table-223"></a>**DDB-TABLE-223** `error-code` · impact high · unhandled (not handled in controller) · verified 2026-10-09
  **ReplicaUpdates error taxonomy: self/invalid/opt-in region, duplicate Create, no-op Update, mixed actions, Delete from the replica endpoint**
  Table with ACTIVE replica us-east-1. create_already_replica -> ValidationException: Failed to create a the
  new replica of table with name: ‘ackq-c42a0c-rsv-none’ because one or more replicas already existed as
  tables. | self_region -> ValidationException: Cannot add or delete the local region through ReplicaUpdates.
  Use CreateTable, DeleteTable, or UpdateTable as required. | delete_self_region -> ValidationException:
  Cannot add or delete the local region through ReplicaUpdates. Use CreateTable, DeleteTable, or UpdateTable
  as required. | invalid_region -> ValidationException: Region us-fake-1 is not supported. The latest version
  of global tables are only supported in the following regions: [ap-south-2, ap-south-1, eu-south-1,
  eu-south-2, me-ce | uppercase_region -> ValidationException: Region EU-WEST-1 is not supported. The latest
  version of global tables are only supported in the following regions: [ap-south-2, ap-south-1, eu-south-1,
  eu-south-2, me-ce | optin_region_af_south_1 -> ValidationException: Failed to access the region:
  ‘af-south-1’. User is missing the permissions since the region is disabled. | empty_region ->
  AccessDeniedException: User: arn:aws:sts::<ACCOUNT>:assumed-role/<PRINCIPAL> is not authorized to perform:
  dynamodb:Scan on resource: arn:aws:dynamodb::<ACCOUNT>:table/ackq-c42a0c-r | update_region_only_no_change ->
  ValidationException: There are no actions specified in the Replica Update Action of the request. |
  empty_action -> ValidationException: There are no actions specified in the Replica Update Action of the
  request. | empty_replica_updates -> ParamValidationError: Parameter validation failed:
  Invalid length for parameter ReplicaUpdates, value: 0, valid min length: 1 | create_and_delete_same_element
  -> ValidationException: Update table operation with more than one type of replica actions not allowed. |
  update_non_replica -> ValidationException: Update global table operation failed because one or more replicas
  were not part of the global table. Please retry the request without these replicas: [eu-west-1]. |
  update_ppr_provisioned_override -> ValidationException: Neither ReadCapacityUnits nor WriteCapacityUnits can
  be specified when BillingMode is PAY_PER_REQUEST | update_same_table_class_STANDARD -> 200 OK |
  ppr_table_provisioned_override -> 200 OK | duplicate_create_same_region_twice -> 200 OK | delete_non_replica
  -> 200 OK | delete_base_region_from_replica_side_dryrun_invalid -> 200 OK | unknown_gsi_override ->
  ValidationException: Failed to create a the new replica of table with name: ‘ackq-c42a0c-rsv-none’ because
  one or more replicas already existed as tables. | create_from_replica_side_for_third_region_INVALID ->
  ValidationException: Region us-fake-1 is not supported. The latest version of global tables are only
  supported in the following regions: [ap-south-2, ap-south-1, eu-south-1, eu-south-2, me-ce
  - ACK: terminal_codes, custom_update · ops: UpdateTable · fields: ReplicaUpdates
  - repro: table with ACTIVE replica us-east-1 -> UpdateTable ReplicaUpdates with each malformed/duplicate
    action
  - handling: not handled in the controller (as of commit 34b85e6)
  - related: [DDB-TABLE-224](#ddb-table-224), [DDB-TABLE-265](#ddb-table-265), [DDB-TABLE-258](table-global-tables.md#ddb-table-258), [DDB-TABLE-257](table-global-tables.md#ddb-table-257), [DDB-TABLE-203](#ddb-table-203), [DDB-TABLE-307](#ddb-table-307),
    [DDB-TABLE-225](#ddb-table-225), [DDB-TABLE-308](#ddb-table-308), [DDB-TABLE-294](#ddb-table-294), [DDB-TABLE-437](table-throughput-billing.md#ddb-table-437), [DDB-TABLE-250](#ddb-table-250), [DDB-TABLE-252](#ddb-table-252), [DDB-TABLE-251](#ddb-table-251),
    [DDB-TABLE-321](#ddb-table-321), [DDB-TABLE-229](table-global-tables.md#ddb-table-229) · hypotheses: H-R-024, H-R-014, H-R-012 · evidence:
    table/error-taxonomy/replica-sync-validation
  - notes: CAVEAT: the sequence became entangled - 'ppr_table_provisioned_override' (Create eu-west-1 with
    ProvisionedThroughputOverride on a PAY_PER_REQUEST table) was ACCEPTED and made eu-west-1 a real replica,
    so 'delete_non_replica' actually deleted that replica (200), 'duplicate_create_same_region_twice'...
  - full notes: [details/DDB-TABLE-223.md](details/DDB-TABLE-223.md)

- <a id="ddb-table-254"></a>**DDB-TABLE-254** `error-code` · impact medium · unhandled (not handled in controller) · verified 2026-10-09
  **DescribeTableReplicaAutoScaling on a missing table: ResourceNotFoundException HTTP 400 'Global table with name: 'ackq-a9cc82-nope' does n...**
  Describe missing -> ["ResourceNotFoundException", 400, "Global table with name: 'ackq-a9cc82-nope' does not
  exist."]; Update missing -> ["ResourceNotFoundException", 400, "Global table with name: 'ackq-a9cc82-nope'
  does not exist."]; Describe with the table ARN as TableName -> ["200", 200, ""]; Update with the ARN ->
  ["200", 200, ""].
  - ACK: exceptions.404, is_arn_primary_key · ops: DescribeTableReplicaAutoScaling,
    UpdateTableReplicaAutoScaling · fields: TableName
  - repro: DescribeTableReplicaAutoScaling TableName=<missing>; TableName=<table ARN>
  - handling: not handled in the controller (as of commit 34b85e6)
  - related: [DDB-TABLE-187](#ddb-table-187), [DDB-TABLE-232](#ddb-table-232), [DDB-TABLE-231](table-global-tables.md#ddb-table-231), [DDB-TABLE-253](#ddb-table-253), [DDB-TABLE-446](service.md#ddb-table-446) · hypotheses: H-S-040 ·
    evidence: table/error-taxonomy/autoscaling-validation
  - notes: TableName accepts the table ARN for both Describe and Update (TableName is modeled as TableArn).
    Missing table and existing-regional-table both yield ResourceNotFoundException HTTP 400 'Global table with
    name: ... does not exist.'

- <a id="ddb-table-307"></a>**DDB-TABLE-307** `error-code` · impact high · unhandled (not handled in controller) · verified 2026-10-09
  **ReplicaUpdates: unknown GSI name in a Create override is silently accepted; Delete/Update of a never-replica region -> ValidationException**
  Create eu-west-1 with GlobalSecondaryIndexes=[{IndexName:'nope'}] on a table without GSIs -> 200 (HTTP 200,
  4.1s) and the replica was created normally - the unknown index override is ignored, not validated.
  ReplicaUpdates=[Delete ca-central-1] (never a replica) and [Update ca-central-1 TableClassOverride] ->
  ValidationException 'Update global table operation failed because one or more replicas were not part of the
  global table. Please retry the request without these replicas: [ca-central-1].' (not
  ResourceNotFoundException). The remaining two checks in this run (alias key on an AWS-owned table;
  ProvisionedThroughputOverride on a PPR Create) were confounded by the eu-west-1 table still DELETING
  ('already existed as tables'); the PPR override was accepted (200) in
  table/error-taxonomy/replica-sync-validation and the alias case is rejected with 'KMSMasterKeyId must be
  specified for each replica' in table/mutation-matrix/replica-overrides.
  - ACK: terminal_codes, custom_update, compare.is_ignored+delta_pre_compare · ops: UpdateTable, DescribeTable
    · fields: ReplicaUpdates, Replicas.ProvisionedThroughputOverride
  - repro: UpdateTable ReplicaUpdates with each malformed action against a table with one ACTIVE replica
  - handling: not handled in the controller (as of commit 34b85e6)
  - related: [DDB-TABLE-203](#ddb-table-203), [DDB-TABLE-223](#ddb-table-223), [DDB-TABLE-225](#ddb-table-225), [DDB-TABLE-308](#ddb-table-308), [DDB-TABLE-294](#ddb-table-294), [DDB-TABLE-437](table-throughput-billing.md#ddb-table-437),
    [DDB-TABLE-250](#ddb-table-250), [DDB-TABLE-252](#ddb-table-252), [DDB-TABLE-251](#ddb-table-251), [DDB-TABLE-321](#ddb-table-321), [DDB-TABLE-229](table-global-tables.md#ddb-table-229) · hypotheses: H-R-014, H-R-024 ·
    evidence: table/dependencies/replica-prerequisites
  - notes: Refutes the GSI half of H-R-014 (no ValidationException for an unknown index) and the Create half
    for ProvisionedThroughputOverride on PAY_PER_REQUEST (accepted). Confirms H-R-024 for non-replica
    Delete/Update (ValidationException with the region list). A controller cannot rely on server validation...
  - full notes: [details/DDB-TABLE-307.md](details/DDB-TABLE-307.md)

## Request validation

- <a id="ddb-table-253"></a>**DDB-TABLE-253** `request-validation` · impact high · unhandled (not handled in controller) · verified 2026-10-09
  **UpdateTableReplicaAutoScaling validation: AAS errors pass through verbatim (TargetValue 10-90, not 20-90); negative cooldown accepted**
  One call per case (code, http, message): {"update_missing": ["ResourceNotFoundException", 400, "Global table
  with name: 'ackq-a9cc82-nope' does not exist."], "min_gt_max": ["ValidationException", 400, "Maximum
  capacity cannot be less than minimum capacity (Service: AWSApplicationAutoScaling; Status Code: 400; Error
  Code: ValidationException; Request ID: 933f88a0-18d1-4878-85a4-6ca0f7db48ae; Proxy: n"], "min_eq_max":
  ["200", 200, ""], "min_zero": ["ParamValidationError", null, "Parameter validation failed:\nInvalid value
  for parameter ReplicaUpdates[0].ReplicaProvisionedReadCapacityAutoScalingUpdate.MinimumUnits, value: 0,
  valid min value: 1"], "min_negative": ["ParamValidationError", null, "Parameter validation failed:\nInvalid
  value for parameter ReplicaUpdates[0].ReplicaProvisionedReadCapacityAutoScalingUpdate.MinimumUnits, value:
  -1, valid min value: 1"], "target_19_9": ["200", 200, ""], "target_90_1": ["ValidationException", 400, "For
  predefined metric type DynamoDBReadCapacityUtilization, target value must be between '10.0' and '90.0', but
  was '90.1'. (Service: AWSApplicationAutoScaling; Status Code: 400; Error Code: Validatio"], "target_0":
  ["ValidationException", 400, "For target tracking scaling, target value must be between '8.51592E-109' and
  '1.174271E108', but was '0.0'. (Service: AWSApplicationAutoScaling; Status Code: 400; Error Code:
  ValidationException; Requ"], "target_100": ["ValidationException", 400, "For predefined metric type
  DynamoDBReadCapacityUtilization, target value must be between '10.0' and '90.0', but was '100.0'. (Service:
  AWSApplicationAutoScaling; Status Code: 400; Error Code: Validati"], "target_missing":
  ["ParamValidationError", null, "Parameter validation failed:\nMissing required parameter in
  ReplicaUpdates[0].ReplicaProvisionedReadCapacityAutoScalingUpdate.ScalingPolicyUpdate.TargetTrackingScalingPolicyConfiguration:
  \"TargetValue\""], "policy_without_config": ["ParamValidationError", null, "Parameter validation
  failed:\nMissing required parameter in
  ReplicaUpdates[0].ReplicaProvisionedReadCapacityAutoScalingUpdate.ScalingPolicyUpdate:
  \"TargetTrackingScalingPolicyConfiguration\""], "negative_scale_in_cooldown": ["200", 200, ""],
  "huge_cooldown": ["200", 200, ""], "min_max_without_policy_fresh": ["ValidationException", 400, "Failed to
  update settings for global table with name: ‘ackq-a9cc82-as-err’: Parameters 'ScalingPolicyUpdate' are
  required unless auto scaling is being disabled."], "policy_without_min_max_fresh": ["ValidationException",
  400, "Failed to update settings for global table with name: ‘ackq-a9cc82-as-err’: Parameters 'MaximumUnits',
  'MinimumUnits' are required unless auto scaling is being disabled."], "min_only_fresh":
  ["ValidationException", 400, "Failed to update settings for global table with name: ‘ackq-a9cc82-as-err’:
  Parameters 'MaximumUnits' are required unless auto scaling is being disabled."], "empty_struct":
  ["ValidationException", 400, "Failed to update settings for global table with name: ‘ackq-a9cc82-as-err’:
  Parameters 'MaximumUnits', 'ScalingPolicyUpdate', 'MinimumUnits' are required unless auto scaling is being
  disabled."], "disabled_true_never_enabled": ["200", 200, ""], "disabled_false_never_enabled":
  ["ValidationException", 400, "Failed to update settings for global table with name: ‘ackq-a9cc82-as-err’:
  Parameters 'MaximumUnits', 'ScalingPolicyUpdate', 'MinimumUnits' are required unless auto scaling is being
  disabled."], "disabled_false_full_fresh": ["200", 200, ""], "empty_replica_updates":
  ["ParamValidationError", null, "Parameter validation failed:\nInvalid length for parameter ReplicaUpdates,
  value: 0, valid min length: 1"], "replica_updates_region_only": ["ValidationException", 400, "Failed to
  update settings for global table with name: ‘ackq-a9cc82-as-err’ because at least one update parameter must
  be specified for region: ‘us-west-2’."], "foreign_region_eu_west_1": ["ResourceNotFou [truncated in
  evidence]
  - ACK: terminal_codes, custom_update · ops: UpdateTableReplicaAutoScaling · fields: MinimumUnits,
    MaximumUnits, TargetValue, ScaleInCooldown, ScaleOutCooldown, ReplicaUpdates.RegionName,
    GlobalSecondaryIndexUpdates.IndexName, TableName
  - repro: 2019.11.21 PROVISIONED global table; send each invalid/partial UpdateTableReplicaAutoScaling shape
    against the never-configured read dimension
  - handling: not handled in the controller (as of commit 34b85e6)
  - related: [DDB-TABLE-187](#ddb-table-187), [DDB-TABLE-232](#ddb-table-232), [DDB-TABLE-254](#ddb-table-254), [DDB-TABLE-231](table-global-tables.md#ddb-table-231), [DDB-TABLE-446](service.md#ddb-table-446), [DDB-TABLE-241](table-policy-kinesis-autoscaling.md#ddb-table-241),
    [DDB-TABLE-242](table-policy-kinesis-autoscaling.md#ddb-table-242), [DDB-TABLE-243](table-policy-kinesis-autoscaling.md#ddb-table-243), [DDB-TABLE-255](#ddb-table-255) · hypotheses: H-R-111, H-R-108 · evidence:
    table/error-taxonomy/autoscaling-validation
  - notes: H-R-111 partially confirmed/partially refuted: min>max -> ValidationException whose text is the AAS
    error ('Maximum capacity cannot be less than minimum capacity (Service: AWSApplicationAutoScaling; ...)');
    TargetValue 19.9 ACCEPTED, 90.1/100 rejected with 'target value must be between 10.0 and...
  - full notes: [details/DDB-TABLE-253.md](details/DDB-TABLE-253.md)

## Update granularity and ordering

- <a id="ddb-table-187"></a>**DDB-TABLE-187** `prerequisite` · impact high · unhandled (not handled in controller) · verified 2026-10-09
  **DescribeTableReplicaAutoScaling/UpdateTableReplicaAutoScaling fail on a regional table: RNF 'Global table ... does not exist'**
  On an ACTIVE single-region PROVISIONED table (streams on, never had a replica)
  DescribeTableReplicaAutoScaling -> HTTP 400 ResourceNotFoundException 'Global table with name:
  'ackq-0ad4a5-as-prov' does not exist.'; UpdateTableReplicaAutoScaling (write) -> ResourceNotFoundException,
  (read via ReplicaUpdates own region) -> ResourceNotFoundException; same on a regional PAY_PER_REQUEST table
  (ResourceNotFoundException); same with an AAS write target already registered (ResourceNotFoundException).
  Right after UpdateTable(ReplicaUpdates Create) -> 200; polling at 5 s: [{"value":
  "UPDATING|rep:-|v:2019.11.21||ras:200:ACTIVE", "from_s": 0.26, "to_s": 5.83, "duration_s": 5.57}, {"value":
  "UPDATING|rep:CREATING|v:2019.11.21||ras:200:ACTIVE", "from_s": 5.83, "to_s": 17.09, "duration_s": 11.26},
  {"value": "ACTIVE|rep:ACTIVE|v:2019.11.21||ras:200:ACTIVE", "from_s": 17.09, "to_s": null, "duration_s":
  null}]. After removing the only replica (GlobalTableVersion <absent>, Replicas key present False) Describe
  -> ResourceNotFoundException, Update -> ResourceNotFoundException.
  - ACK: exceptions.404, terminal_codes, scope:skip · ops: DescribeTableReplicaAutoScaling,
    UpdateTableReplicaAutoScaling · fields: GlobalTableVersion, Replicas
  - repro: CreateTable (streams on, no replicas) -> DescribeTableReplicaAutoScaling (RNF) -> UpdateTable add
    replica -> poll -> remove replica -> Describe again
  - handling: not handled in the controller (as of commit 34b85e6)
  - related: [DDB-TABLE-232](#ddb-table-232), [DDB-TABLE-254](#ddb-table-254), [DDB-TABLE-231](table-global-tables.md#ddb-table-231), [DDB-TABLE-253](#ddb-table-253), [DDB-TABLE-446](service.md#ddb-table-446), [DDB-TABLE-306](table-policy-kinesis-autoscaling.md#ddb-table-306),
    [DDB-TABLE-323](table-policy-kinesis-autoscaling.md#ddb-table-323), [DDB-TABLE-226](#ddb-table-226), [DDB-TABLE-265](#ddb-table-265), [DDB-TABLE-308](#ddb-table-308), [DDB-TABLE-309](#ddb-table-309), [DDB-TABLE-329](#ddb-table-329), [DDB-TABLE-305](#ddb-table-305),
    [DDB-TABLE-249](#ddb-table-249), [DDB-TABLE-258](table-global-tables.md#ddb-table-258) · hypotheses: H-S-040 · evidence:
    table/sub-resources/replica-autoscaling-facade
  - notes: REFUTES the single-region part of H-S-040: the API is gated on the table being a 2019.11.21 global
    table. The RNF uses the same code as 'table missing' (ResourceNotFoundException, HTTP 400) and the same
    message template; missing-table message: 'Global table with name: 'ackq-0ad4a5-missing' does not...
  - full notes: [details/DDB-TABLE-187.md](details/DDB-TABLE-187.md)

- <a id="ddb-table-188"></a>**DDB-TABLE-188** `prerequisite` · impact high · SUSPECTED CONTROLLER BUG · verified 2026-10-09
  **Adding a replica to a PROVISIONED table requires write autoscaling first: 'Table write capacity should either be Pay-Per-Request or AutoS...**
  UpdateTable(ReplicaUpdates=[Create us-east-1]) on a PROVISIONED 1/1 regional table -> ValidationException
  'Table write capacity should either be Pay-Per-Request or AutoScaled.'. With only an AAS scalable target (no
  policy) on dynamodb:table:WriteCapacityUnits -> ValidationException. With target + target-tracking policy -> 200.
  UpdateTableReplicaAutoScaling cannot be used to satisfy this because it is itself rejected on a regional
  table, so the write autoscaling must be created through Application Auto Scaling (or the table must be
  PAY_PER_REQUEST: add replica -> 200).
  - ACK: custom_update, references, scope:skip · ops: UpdateTable, RegisterScalableTarget, PutScalingPolicy ·
    fields: ReplicaUpdates, BillingMode, ProvisionedThroughput
  - repro: CreateTable PROVISIONED + streams -> UpdateTable ReplicaUpdates Create -> ValidationException ->
    application-autoscaling register-scalable-target + put-scaling-policy -> retry
  - handling: suspected controller bug - see Handling gaps · tracked in https://github.com/aws-controllers-k8s/community/issues/2610 (not handled) · code refs: `5bbfe82`
  - related: [DDB-TABLE-249](#ddb-table-249) · hypotheses: H-R-033, H-R-131 · evidence:
    table/sub-resources/replica-autoscaling-facade
  - notes: Suspected controller bug confirmed by evidence: Suspicion confirmed: the prerequisite is
    server-enforced (188/249), cannot be met through the DynamoDB API (UpdateTableReplicaAutoScaling -> RNF on
    a regional table, 187/188; a bare AAS target without a policy is insufficient, 188) and also covers...
  - full notes: [details/DDB-TABLE-188.md](details/DDB-TABLE-188.md)

- <a id="ddb-table-221"></a>**DDB-TABLE-221** `prerequisite` · impact high · handled · verified 2026-10-09
  **ReplicaUpdates.Create on a table WITHOUT streams is accepted; DynamoDB silently enables a NEW_AND_OLD_IMAGES stream**
  Four PAY_PER_REQUEST tables created with no StreamSpecification: UpdateTable ReplicaUpdates=[Create
  us-east-1] returned 200 on all of them (latency 2-11s); the UpdateTable response already carries
  StreamSpecification{StreamEnabled:true, StreamViewType:NEW_AND_OLD_IMAGES}, LatestStreamArn and
  GlobalTableVersion=2019.11.21 while Replicas is absent/empty. The replica region table also reports a
  NEW_AND_OLD_IMAGES stream with its own LatestStreamArn. The stream is NOT removed when the last replica is
  deleted (StreamSpecification stays enabled; see D.final_regional_view: StreamSpecification absent in the
  regional view but LatestStreamArn/LatestStreamLabel still present after replica removal + stream disable).
  - ACK: compare.is_ignored+delta_pre_compare, custom_update, late_initialize · ops: UpdateTable,
    DescribeTable · fields: ReplicaUpdates, StreamSpecification, LatestStreamArn
  - repro: CreateTable PPR without StreamSpecification -> UpdateTable ReplicaUpdates=[Create us-east-1] ->
    DescribeTable
  - handling: handled via `pkg/resource/table/hooks.go:292-299; pkg/resource/table/hooks_replica_updates.go:265-275`
  - related: [DDB-TABLE-305](#ddb-table-305), [DDB-TABLE-222](#ddb-table-222), [DDB-TABLE-231](table-global-tables.md#ddb-table-231), [DDB-TABLE-204](#ddb-table-204), [DDB-TABLE-297](#ddb-table-297), [DDB-TABLE-224](#ddb-table-224),
    [DDB-TABLE-261](table-global-tables.md#ddb-table-261), [DDB-TABLE-250](#ddb-table-250), [DDB-TABLE-294](#ddb-table-294), [DDB-TABLE-296](#ddb-table-296), [DDB-TABLE-226](#ddb-table-226), [DDB-TABLE-309](#ddb-table-309) · hypotheses:
    H-R-002, H-R-003 · evidence: table/error-taxonomy/replica-sync-validation
  - notes: Refutes H-R-002 (no synchronous stream prerequisite), confirms contrarian H-R-003. The
    KEYS_ONLY/NEW_IMAGE/OLD_IMAGE scenarios in this run were invalid (script bug: tables were created without
    the stream) and are re-tested in table/dependencies/replica-prerequisites. Controller impact: a spec
    with...
  - full notes: [details/DDB-TABLE-221.md](details/DDB-TABLE-221.md)

- <a id="ddb-table-224"></a>**DDB-TABLE-224** `update-granularity` · impact high · handled · verified 2026-10-09
  **ReplicaUpdates must be the only operation in an UpdateTable call; any other field -> ValidationException**
  One fresh PPR table per combo; UpdateTable ReplicaUpdates=[Create us-east-1] plus StreamSpecification /
  BillingMode+ProvisionedThroughput / GlobalSecondaryIndexUpdates / SSESpecification / TableClass /
  DeletionProtectionEnabled each fail synchronously (6-9ms, HTTP 400) with ValidationException 'One or more
  parameter values were invalid: Replica modification must be the only operation in the request'. The same
  message is returned for [Create us-east-1, Create eu-west-1] + DeletionProtectionEnabled. Two actions of
  different types in one element -> 'Update table operation with more than one type of replica actions not
  allowed'. Two Create actions for the SAME region in one call ([Create eu-west-1, Create eu-west-1]) were
  accepted (200) and created one replica (section B).
  - ACK: one-per-reconcile, custom_update · ops: UpdateTable · fields: ReplicaUpdates, StreamSpecification,
    BillingMode, GlobalSecondaryIndexUpdates, SSESpecification, TableClass, DeletionProtectionEnabled
  - repro: UpdateTable TableName=T ReplicaUpdates=[{Create:{RegionName:us-east-1}}] <other mutation>
  - handling: handled via `pkg/resource/table/hooks.go:220-304; test/e2e/tests/test_table.py:878-952`
  - related: [DDB-TABLE-433](table-streams-encryption-class.md#ddb-table-433), [DDB-TABLE-156](table-throughput-billing.md#ddb-table-156), [DDB-TABLE-358](table-throughput-billing.md#ddb-table-358), [DDB-TABLE-056](table-throughput-billing.md#ddb-table-056), [DDB-TABLE-174](table-indexes.md#ddb-table-174), [DDB-TABLE-199](#ddb-table-199),
    [DDB-TABLE-162](table-indexes.md#ddb-table-162), [DDB-TABLE-127](table-indexes.md#ddb-table-127), [DDB-TABLE-265](#ddb-table-265), [DDB-TABLE-258](table-global-tables.md#ddb-table-258), [DDB-TABLE-223](#ddb-table-223), [DDB-TABLE-257](table-global-tables.md#ddb-table-257), [DDB-TABLE-221](#ddb-table-221),
    [DDB-TABLE-305](#ddb-table-305), [DDB-TABLE-222](#ddb-table-222), [DDB-TABLE-231](table-global-tables.md#ddb-table-231), [DDB-TABLE-204](#ddb-table-204), [DDB-TABLE-297](#ddb-table-297) · hypotheses: H-R-006 ·
    evidence: table/error-taxonomy/replica-sync-validation
  - notes: Qualifies H-R-006: the one-operation rule is enforced per call, independent of table state
    (validation happens before the ResourceInUse check). Two Creates for DIFFERENT regions in one call is
    tested in table/cross-region/multi-replica-delete-semantics.

- <a id="ddb-table-239"></a>**DDB-TABLE-239** `prerequisite` · impact medium · unhandled (not handled in controller) · verified 2026-10-09
  **AutoScalingRoleArn other than the SLR is rejected: DynamoDB calls AAS as AWSServiceRoleForDynamoDBReplication, which lacks iam:PassRole**
  Nonexistent role ARN -> {"operation": "update_table_replica_auto_scaling", "ok": false, "code":
  "ValidationException", "http_status": 400, "message": "User: arn:aws:sts::<ACCOUNT>:assumed-role/<PRINCIPAL>
  is not authorized to perform: iam:PassRole on resource: arn:aws:iam::<ACCOUNT>:role/<PRINCIPAL> because no
  identity-based policy allows the iam:PassRole action (Service: AWSApplicat", "latency_ms": 994,
  "client_side": false}. Explicit service-linked role ARN -> {"operation":
  "update_table_replica_auto_scaling", "ok": true, "code": null, "http_status": 200, "message": null,
  "latency_ms": 1138, "client_side": false}. Existing non-SLR role (Admin) -> {"operation":
  "update_table_replica_auto_scaling", "ok": false, "code": "ValidationException", "http_status": 400,
  "message": "User: arn:aws:sts::<ACCOUNT>:assumed-role/<PRINCIPAL> is not authorized to perform: iam:PassRole
  on resource: arn:aws:iam::<ACCOUNT>:role/<PRINCIPAL> because no identity-based policy allows the
  iam:PassRole action (Service: AWSApplicationAutoScaling; St", "latency_ms": 693, "client_side": false}.
  Describe role afterwards: arn:aws:iam::<ACCOUNT>:role/<PRINCIPAL> AAS target RoleARN:
  arn:aws:iam::<ACCOUNT>:role/<PRINCIPAL>
  - ACK: is_read_only, docs-only · ops: UpdateTableReplicaAutoScaling · fields: AutoScalingRoleArn
  - repro: UpdateTableReplicaAutoScaling read update with AutoScalingRoleArn variants
  - handling: not handled in the controller (as of commit 34b85e6)
  - related: [DDB-TABLE-237](table-policy-kinesis-autoscaling.md#ddb-table-237), [DDB-TABLE-240](table-policy-kinesis-autoscaling.md#ddb-table-240), [DDB-TABLE-191](table-policy-kinesis-autoscaling.md#ddb-table-191), [DDB-TABLE-189](table-policy-kinesis-autoscaling.md#ddb-table-189), [DDB-TABLE-196](table-policy-kinesis-autoscaling.md#ddb-table-196) · hypotheses: H-S-041 ·
    evidence: table/round-trip/autoscaling-settings
  - notes: The error text names the principal 'arn:aws:sts::<ACCOUNT>:assumed-role/<PRINCIPAL>' and the AAS
    service - the DynamoDB facade performs the Application Auto Scaling calls under DynamoDB's own
    service-linked role, not the caller's identity (qualifies H-R-105). AutoScalingRoleArn is effectively...
  - full notes: [details/DDB-TABLE-239.md](details/DDB-TABLE-239.md)

- <a id="ddb-table-255"></a>**DDB-TABLE-255** `update-granularity` · impact medium · unhandled (not handled in controller) · verified 2026-10-09
  **Multi-dimension autoscaling updates in one call: read+write -> 200, both replicas -> 200, duplicate region -> ValidationException**
  read+write in one call -> ["200", 200, ""]; read for both replicas in one call -> ["200", 200, ""] (after:
  {"read_a": {"MinimumUnits": 1, "MaximumUnits": 10, "AutoScalingRoleArn":
  "arn:aws:iam::<ACCOUNT>:role/<PRINCIPAL>", "ScalingPolicies": [{"PolicyName":
  "DynamoDBReadCapacityUtilization:table/a); same region twice -> ["ValidationException", 400, "Failed to
  update settings for global table with name: ‘ackq-a9cc82-as-err’ because the regions: ‘[us-west-2]’ were
  specified more than once."]; ReplicaUpdates=[] -> ["ParamValidationError", null, "Parameter validation
  failed:\nInvalid length for parameter ReplicaUpdates, value: 0, valid min length: 1"]; RegionName only ->
  ["ValidationException", 400, "Failed to update settings for global table with name: ‘ackq-a9cc82-as-err’
  because at least one update parameter must be specified for region: ‘us-west-2’."]; TableName only ->
  ["ValidationException", 400, "Failed to update settings for global table with name: ‘ackq-a9cc82-as-err’
  because at least one update parameter must be specified."]; GlobalSecondaryIndexUpdates=[] ->
  ["ParamValidationError", null, "Parameter validation failed:\nInvalid length for parameter
  GlobalSecondaryIndexUpdates, value: 0, valid min length: 1"].
  - ACK: custom_update, one-per-reconcile · ops: UpdateTableReplicaAutoScaling · fields: ReplicaUpdates,
    ProvisionedWriteCapacityAutoScalingUpdate, GlobalSecondaryIndexUpdates
  - repro: UpdateTableReplicaAutoScaling with several dimensions/replicas in one request
  - handling: not handled in the controller (as of commit 34b85e6)
  - related: [DDB-TABLE-241](table-policy-kinesis-autoscaling.md#ddb-table-241), [DDB-TABLE-253](#ddb-table-253), [DDB-TABLE-242](table-policy-kinesis-autoscaling.md#ddb-table-242), [DDB-TABLE-243](table-policy-kinesis-autoscaling.md#ddb-table-243) · hypotheses: H-R-111 · evidence:
    table/error-taxonomy/autoscaling-validation

- <a id="ddb-table-265"></a>**DDB-TABLE-265** `update-granularity` · impact high · handled · verified 2026-10-09
  **One Create/Delete replica action per UpdateTable call; a Create may be issued at any member's endpoint; Replicas[] order differs per region**
  ReplicaUpdates=[Create us-east-1, Create eu-west-1] in one call -> ValidationException 'Update table
  operation with more than one create or delete replica actions not allowed' (same message for two Delete
  actions). Sequential: Create us-east-1 -> 200; Create eu-west-1 2s later (table UPDATING, replica CREATING)
  -> ResourceInUseException. Once ACTIVE, Create eu-west-1 issued at the us-east-1 endpoint -> 200 (control is
  symmetric; the endpoint region becomes the 'source' of the new replica, see the 24h-source finding).
  3-region views: Replicas[] order is creation order as seen from each region and differs per region -
  us-west-2: [us-east-1, eu-west-1]; us-east-1: [us-west-2, eu-west-1]; eu-west-1: [us-west-2, us-east-1];
  DescribeTableReplicaAutoScaling from the base lists [eu-west-1, us-east-1, us-west-2] (alphabetical, home
  included). Each region has its own TableId/CreationDateTime; GlobalTableVersion 2019.11.21 everywhere; the
  single item was readable in all three regions.
  - ACK: one-per-reconcile, custom_update, compare.is_ignored+delta_pre_compare · ops: UpdateTable,
    DescribeTable · fields: ReplicaUpdates, Replicas
  - repro: UpdateTable ReplicaUpdates=[{Create:us-east-1},{Create:eu-west-1}]; DescribeTable in all three
    regions
  - measurements: us_east_1_create_total_s_1_item=660.0, eu_west_1_create_total_s_1_item=580.0
  - handling: handled via `generator.yaml:104-109; pkg/resource/table/hooks.go:72-93; generator.yaml:27-31; pkg/resource/table/hooks_replica_updates.go:277-373; pkg/resource/table/hooks_replica_updates.go:465-476; pkg/resource/table/hooks.go:89-92; pkg/resource/table/hooks.go:720-727; pkg/resource/table/hooks_replica_updates.go:413-418; test/e2e/tests/test_table_replicas.py:200-206; test/e2e/tests/test_table.py:34`
  - related: [DDB-TABLE-200](#ddb-table-200), [DDB-TABLE-224](#ddb-table-224), [DDB-TABLE-258](table-global-tables.md#ddb-table-258), [DDB-TABLE-223](#ddb-table-223), [DDB-TABLE-257](table-global-tables.md#ddb-table-257), [DDB-TABLE-226](#ddb-table-226),
    [DDB-TABLE-306](table-policy-kinesis-autoscaling.md#ddb-table-306), [DDB-TABLE-308](#ddb-table-308), [DDB-TABLE-309](#ddb-table-309), [DDB-TABLE-329](#ddb-table-329), [DDB-TABLE-187](#ddb-table-187), [DDB-TABLE-305](#ddb-table-305), [DDB-TABLE-249](#ddb-table-249),
    [DDB-TABLE-266](#ddb-table-266), [DDB-TABLE-262](#ddb-table-262), [DDB-TABLE-267](#ddb-table-267), [DDB-TABLE-297](#ddb-table-297), [DDB-TABLE-260](table-global-tables.md#ddb-table-260), [DDB-TABLE-296](#ddb-table-296), [DDB-TABLE-251](#ddb-table-251),
    [DDB-TABLE-294](#ddb-table-294), [DDB-TABLE-295](#ddb-table-295), [DDB-TABLE-322](table-policy-kinesis-autoscaling.md#ddb-table-322), [DDB-TABLE-232](#ddb-table-232), [DDB-TABLE-189](table-policy-kinesis-autoscaling.md#ddb-table-189), [DDB-TABLE-229](table-global-tables.md#ddb-table-229) · hypotheses:
    H-R-006, H-R-007, H-R-031 · evidence: table/cross-region/multi-replica-delete-semantics
  - notes: Confirms the one-action-per-call rule (H-R-006 vocabulary) and H-R-031 (order differs per region ->
    diff Replicas by RegionName as a set). Two IDENTICAL Create actions for the same region in one call are
    de-duplicated and accepted (table/error-taxonomy/replica-sync-validation). Both replica...
  - full notes: [details/DDB-TABLE-265.md](details/DDB-TABLE-265.md)

- <a id="ddb-table-288"></a>**DDB-TABLE-288** `prerequisite` · impact high · tracked in GitHub issue (not handled) · verified 2026-10-09
  **Adding a replica to a PROVISIONED table with a GSI: table write autoscaling only -> ValidationException; with GSI write autoscaling -> 200**
  UpdateTable(ReplicaUpdates Create) with AAS write autoscaling on the table only -> {"operation":
  "update_table", "ok": false, "code": "ValidationException", "http_status": 400, "message": "GSI write
  capacity should either be Pay-Per-Request or AutoScaled.", "latency_ms": 1011, "client_side": false}. After
  also registering table/<name>/index/gsi0 dynamodb:index:WriteCapacityUnits + policy -> {"operation":
  "update_table", "ok": true, "code": null, "http_status": 200, "message": null, "latency_ms": 2577,
  "client_side": false}. AAS targets copied to us-east-1 after the replica became ACTIVE: [["/", "table:W", 1,
  10], ["/index/gsi0", "index:W", 1, 10]].
  - ACK: custom_update, references · ops: UpdateTable, RegisterScalableTarget · fields: ReplicaUpdates,
    GlobalSecondaryIndexes.ProvisionedThroughput
  - repro: PROVISIONED table + GSI -> AAS write autoscaling on table -> UpdateTable add replica -> add GSI
    write autoscaling -> retry
  - handling: tracked in https://github.com/aws-controllers-k8s/community/issues/2610 (not handled) · code refs: `5bbfe82`
  - related: [DDB-TABLE-249](#ddb-table-249), [DDB-TABLE-291](table-policy-kinesis-autoscaling.md#ddb-table-291), [DDB-TABLE-322](table-policy-kinesis-autoscaling.md#ddb-table-322), [DDB-TABLE-290](table-policy-kinesis-autoscaling.md#ddb-table-290), [DDB-TABLE-194](#ddb-table-194) · hypotheses: H-R-108,
    H-R-131 · evidence: table/dependencies/autoscaling-gsi-and-orphans
  - notes: Every PROVISIONED write dimension (table AND each GSI) must have an AAS target+policy before
    UpdateTable(ReplicaUpdates Create) is accepted: 'GSI write capacity should either be Pay-Per-Request or
    AutoScaled.' After the replica is ACTIVE the write targets+policies exist in the replica region too...
  - full notes: [details/DDB-TABLE-288.md](details/DDB-TABLE-288.md)

- <a id="ddb-table-305"></a>**DDB-TABLE-305** `prerequisite` · impact high · handled · verified 2026-10-09
  **No stream view-type prerequisite: replicas are created on KEYS_ONLY/NEW_IMAGE/OLD_IMAGE streams unchanged; a disabled stream is re-enabled**
  Tables created with StreamSpecification KEYS_ONLY, NEW_IMAGE and OLD_IMAGE: UpdateTable
  ReplicaUpdates=[Create us-east-1] -> 200 on all three (~2s), replica ACTIVE after 14-18s, and the
  StreamViewType is left unchanged in the base AND copied as-is to the replica (us-east-1 reports the same
  KEYS_ONLY/NEW_IMAGE/OLD_IMAGE spec with its own LatestStreamArn). A table whose stream was enabled then
  disabled (StreamSpecification absent, stale LatestStreamLabel still present): Create -> 200 and the response
  already shows StreamSpecification{true, NEW_AND_OLD_IMAGES} with a NEW LatestStreamLabel; the replica gets
  NEW_AND_OLD_IMAGES too (ACTIVE after 34s).
  - ACK: custom_update, compare.is_ignored+delta_pre_compare, terminal_codes · ops: UpdateTable, DescribeTable
    · fields: ReplicaUpdates, StreamSpecification.StreamViewType
  - repro: CreateTable with StreamSpecification{KEYS_ONLY} -> UpdateTable ReplicaUpdates=[Create us-east-1] ->
    DescribeTable
  - measurements: replica_active_s_keys_only=14.4, replica_active_s_new_image=14.6,
    replica_active_s_old_image=17.8, replica_active_s_disabled_stream=33.6
  - handling: handled via `pkg/resource/table/hooks.go:292-299; pkg/resource/table/hooks_replica_updates.go:265-275; pkg/resource/table/hooks_replica_updates.go:465-476; pkg/resource/table/hooks.go:89-92`
  - related: [DDB-TABLE-226](#ddb-table-226), [DDB-TABLE-306](table-policy-kinesis-autoscaling.md#ddb-table-306), [DDB-TABLE-265](#ddb-table-265), [DDB-TABLE-308](#ddb-table-308), [DDB-TABLE-309](#ddb-table-309), [DDB-TABLE-329](#ddb-table-329),
    [DDB-TABLE-187](#ddb-table-187), [DDB-TABLE-249](#ddb-table-249), [DDB-TABLE-258](table-global-tables.md#ddb-table-258), [DDB-TABLE-221](#ddb-table-221), [DDB-TABLE-222](#ddb-table-222), [DDB-TABLE-231](table-global-tables.md#ddb-table-231), [DDB-TABLE-204](#ddb-table-204),
    [DDB-TABLE-297](#ddb-table-297), [DDB-TABLE-224](#ddb-table-224) · hypotheses: H-R-002, H-R-003 · evidence:
    table/dependencies/replica-prerequisites
  - notes: Refutes H-R-002 completely (no synchronous stream prerequisite of any kind) and confirms contrarian
    H-R-003 only for the no-stream/disabled case (server enables NEW_AND_OLD_IMAGES). Combined with H-R-004
    (stream immutable while replicas exist) a spec with replicas + a KEYS_ONLY stream is stable,...
  - full notes: [details/DDB-TABLE-305.md](details/DDB-TABLE-305.md)

## Field behavior (defaults, normalization, shapes, immutability)

- <a id="ddb-table-222"></a>**DDB-TABLE-222** `immutable-field` · impact high · unhandled (not handled in controller) · verified 2026-10-09
  **StreamSpecification is immutable while the table has replicas (disable / view-type change rejected in both regions)**
  With an ACTIVE replica: {'disable_with_replica': 'ValidationException: One or more parameter values were
  invalid: Disabling Stream is not allowed for a Global Table replica.', 'keys_only_with_replica':
  'ValidationException: Table already has an enabled stream: TableName: ackq-c42a0c-rsv-none',
  'resend_new_and_old_with_replica': 'ValidationException: Table already has an enabled stream: TableName:
  ackq-c42a0c-rsv-none', 'disable_on_replica_table_in_B': 'ValidationException: One or more parameter values
  were invalid: Disabling Stream is not allowed for a Global Table replica.'} After ReplicaUpdates.Delete, the
  moment A's Replicas[] became empty (B still 'DELETING[]'): StreamSpecification{StreamEnabled:false} -> OK
  (200). Replica removal timeline: [('UPDATING[us-east-1=ACTIVE]', 38.16), ('ACTIVE[us-east-1=DELETING]',
  203.08), ('UPDATING[us-east-1=DELETING]', 3.02), ('ACTIVE[]', None)].
  - ACK: custom_update, terminal_codes, is_immutable · ops: UpdateTable · fields: StreamSpecification
  - repro: table with replica -> UpdateTable StreamSpecification{StreamEnabled:false}
  - measurements: replica_delete_until_entry_gone_s=244.41
  - handling: not handled in the controller (as of commit 34b85e6)
  - related: [DDB-TABLE-204](#ddb-table-204), [DDB-TABLE-293](#ddb-table-293), [DDB-TABLE-308](#ddb-table-308), [DDB-TABLE-297](#ddb-table-297), [DDB-TABLE-267](#ddb-table-267), [DDB-TABLE-261](table-global-tables.md#ddb-table-261),
    [DDB-TABLE-231](table-global-tables.md#ddb-table-231), [DDB-TABLE-221](#ddb-table-221), [DDB-TABLE-305](#ddb-table-305), [DDB-TABLE-224](#ddb-table-224) · hypotheses: H-R-004 · evidence:
    table/error-taxonomy/replica-sync-validation
  - notes: Confirms H-R-004. Re-sending the identical NEW_AND_OLD_IMAGES spec is rejected with 'Table already
    has an enabled stream' (not a no-op), so a controller must not re-send StreamSpecification when it is
    already enabled.

- <a id="ddb-table-232"></a>**DDB-TABLE-232** `shape-mismatch` · impact medium · unhandled (not handled in controller) · verified 2026-10-09
  **DescribeTableReplicaAutoScaling on a PPR table with 0/1 replicas and during replica CREATING: entry count and members**
  Regional table (0 replicas): DescribeTableReplicaAutoScaling -> ResourceNotFoundException 'Global table with
  name: <name> does not exist.' (HTTP 400). With one ACTIVE replica, called from either region: 2 entries
  ordered [us-east-1, us-west-2] (replica first, home region second - alphabetical, not home-first), each with
  GlobalSecondaryIndexes=[],
  ReplicaProvisionedRead/WriteCapacityAutoScalingSettings={AutoScalingDisabled:true, ScalingPolicies:[]} even
  though the table is PAY_PER_REQUEST, and ReplicaStatus. During CREATING (sampled ~13s after Create) the
  us-east-1 entry was already present with ReplicaStatus=CREATING and the SAME members (settings not absent).
  - ACK: scope:field-on-parent, custom_find · ops: DescribeTableReplicaAutoScaling · fields: Replicas
  - repro: DescribeTableReplicaAutoScaling before/during/after replica creation
  - handling: not handled in the controller (as of commit 34b85e6)
  - related: [DDB-TABLE-187](#ddb-table-187), [DDB-TABLE-254](#ddb-table-254), [DDB-TABLE-231](table-global-tables.md#ddb-table-231), [DDB-TABLE-253](#ddb-table-253), [DDB-TABLE-446](service.md#ddb-table-446), [DDB-TABLE-306](table-policy-kinesis-autoscaling.md#ddb-table-306),
    [DDB-TABLE-323](table-policy-kinesis-autoscaling.md#ddb-table-323), [DDB-TABLE-189](table-policy-kinesis-autoscaling.md#ddb-table-189), [DDB-TABLE-195](table-policy-kinesis-autoscaling.md#ddb-table-195), [DDB-TABLE-243](table-policy-kinesis-autoscaling.md#ddb-table-243), [DDB-TABLE-317](table-policy-kinesis-autoscaling.md#ddb-table-317), [DDB-TABLE-289](#ddb-table-289), [DDB-TABLE-237](table-policy-kinesis-autoscaling.md#ddb-table-237),
    [DDB-TABLE-190](#ddb-table-190), [DDB-TABLE-265](#ddb-table-265), [DDB-TABLE-229](table-global-tables.md#ddb-table-229) · hypotheses: H-R-101, H-R-050 · evidence:
    table/state-machine/replica-create-timeline
  - notes: Refutes H-R-050's first clause (the API is NotFound on a plain regional table) and H-R-101's 'home
    listed first' and 'settings absent while CREATING' clauses; confirms N+1 entries. DescribeTable.Replicas
    excludes the home region, so joining the two lists by position is wrong.

- <a id="ddb-table-250"></a>**DDB-TABLE-250** `read-gap` · impact high · handled · verified 2026-10-09
  **Replica ProvisionedThroughputOverride: absent when inherited; re-send accepted (UPDATING); base value clears it; ODT override rejected**
  After Create without overrides: A.Replicas[us-east-1].ProvisionedThroughputOverride is ABSENT while the
  us-east-1 table reports ProvisionedThroughput 5/5 (inherited).
  Update{OnDemandThroughputOverride:{MaxReadRequestUnits:100}} on the PROVISIONED table -> ValidationException
  'MaxReadRequestUnits for OnDemandThroughput cannot be specified when the table BillingMode is PROVISIONED'.
  Update{ProvisionedThroughputOverride:{ReadCapacityUnits:10}} -> 200; the UpdateTable response still shows
  the override as absent; after ~38s (A UPDATING 34s, then B UPDATING 3s more) A shows {ReadCapacityUnits:10}
  and us-east-1 reports RCU 10 / WCU 5. Re-sending the identical override -> 200 and another UPDATING cycle
  (not rejected as a no-op). Sending ReadCapacityUnits=5 (equal to the table's RCU) -> 200 and the override
  field becomes ABSENT again (B back to 5/5): the override is cleared by setting it equal to the base value.
  - ACK: compare.is_ignored+delta_pre_compare, compare.nil_equals_zero_value, custom_update · ops:
    UpdateTable, DescribeTable · fields: ReplicaUpdates.Update.ProvisionedThroughputOverride,
    Replicas.ProvisionedThroughputOverride, OnDemandThroughputOverride
  - repro: PROVISIONED table + replica -> DescribeTable both regions -> UpdateTable
    ReplicaUpdates=[{Update:{RegionName, ProvisionedThroughputOverride:{ReadCapacityUnits:10}}}]
  - handling: handled via `pkg/resource/table/hooks_replica_updates.go:423-463; pkg/resource/table/hooks_replica_updates.go:28-147; pkg/resource/table/hooks_replica_updates.go:39-45; pkg/resource/table/hooks_replica_updates.go:166-171; generator.yaml:1-6; generator.yaml:15`
  - related: [DDB-TABLE-252](#ddb-table-252), [DDB-TABLE-251](#ddb-table-251), [DDB-TABLE-321](#ddb-table-321), [DDB-TABLE-294](#ddb-table-294), [DDB-TABLE-229](table-global-tables.md#ddb-table-229), [DDB-TABLE-223](#ddb-table-223),
    [DDB-TABLE-307](#ddb-table-307), [DDB-TABLE-204](#ddb-table-204), [DDB-TABLE-261](table-global-tables.md#ddb-table-261), [DDB-TABLE-296](#ddb-table-296), [DDB-TABLE-226](#ddb-table-226), [DDB-TABLE-309](#ddb-table-309), [DDB-TABLE-221](#ddb-table-221) ·
    hypotheses: H-R-013, H-R-014, H-R-012 · evidence: table/cross-region/provisioned-replica
  - notes: Confirms H-R-013 (nil override = inherit; only RCU is overridable). Refutes H-R-012 for this field
    (same-value Update is accepted and costs an UPDATING cycle) and the 'cannot clear' part of H-R-032 at
    table level. Response fidelity: the UpdateTable response does not echo the new override.

- <a id="ddb-table-252"></a>**DDB-TABLE-252** `unsettable-field` · impact medium · handled · verified 2026-10-09
  **Replica GSI ProvisionedThroughputOverride: unnamed indexes keep their overrides; sending the base value clears an override**
  Update{GlobalSecondaryIndexes:[gsi1 RCU 6, gsi2 RCU 7]} -> 200; A.Replicas[].GlobalSecondaryIndexes shows
  both overrides and us-east-1's GSIs report RCU 6/7 (WCU 5 inherited). Update{[gsi1 RCU 8]} only -> gsi1
  override 8, gsi2 override 7 kept (partial merge). Update{[gsi1 RCU 5]} (= table GSI RCU) -> gsi1's
  ProvisionedThroughputOverride disappears (entry keeps only IndexName + WarmThroughput) while gsi2 keeps 7.
  Update{[{IndexName:gsi1}]} with no override -> ValidationException 'There are no actions specified in the
  Replica Update Action of the request.' Replica GSI entries without an override contain only IndexName and
  WarmThroughput.
  - ACK: compare.is_ignored+delta_pre_compare, custom_update · ops: UpdateTable, DescribeTable · fields:
    ReplicaUpdates.Update.GlobalSecondaryIndexes,
    Replicas.GlobalSecondaryIndexes.ProvisionedThroughputOverride
  - repro: PROVISIONED table with gsi1/gsi2 + replica -> UpdateTable ReplicaUpdates Update with
    GlobalSecondaryIndexes overrides
  - handling: handled via `pkg/resource/table/hooks_replica_updates.go:423-463; pkg/resource/table/hooks_replica_updates.go:28-147; pkg/resource/table/hooks_replica_updates.go:195-254; pkg/resource/table/hooks_replica_updates.go:348-360; pkg/resource/table/hooks_replica_updates.go:39-45; pkg/resource/table/hooks_replica_updates.go:166-171`
  - related: [DDB-TABLE-250](#ddb-table-250), [DDB-TABLE-251](#ddb-table-251), [DDB-TABLE-321](#ddb-table-321), [DDB-TABLE-294](#ddb-table-294), [DDB-TABLE-229](table-global-tables.md#ddb-table-229), [DDB-TABLE-223](#ddb-table-223),
    [DDB-TABLE-307](#ddb-table-307) · hypotheses: H-R-032 · evidence: table/cross-region/provisioned-replica
  - notes: Confirms the partial-merge half of H-R-032, refutes the 'no way to clear' half: equal-to-base
    clears the override (same as the table-level override).

- <a id="ddb-table-256"></a>**DDB-TABLE-256** `requested-vs-effective` · impact medium · unhandled (not handled in controller) · verified 2026-10-09
  **MinimumUnits above the table-level read quota (40000.0): UpdateTableReplicaAutoScaling -> 200; AAS activity Failed**
  Quota {"name": "Table-level read throughput limit", "code": "L-CF0CBE56", "value": 40000.0}. Update read Min
  50000 Max 60000 -> {"operation": "update_table_replica_auto_scaling", "ok": true, "code": null,
  "http_status": 200, "message": null, "latency_ms": 1048, "client_side": false}. Describe read settings
  after: {"MinimumUnits": 50000, "MaximumUnits": 60000, "AutoScalingRoleArn":
  "arn:aws:iam::<ACCOUNT>:role/<PRINCIPAL>", "ScalingPolicies": [{"PolicyName":
  "DynamoDBReadCapacityUtilization:table/ackq-a9cc82-as-err", "TargetTrackingScalingPolicyConfiguration":
  {"TargetValue": 70.0}}]}; AAS targets [["ReadCapacityUnits", 50000, 60000], ["WriteCapacityUnits", 1, 12]].
  Scaling activities within 4 min: [["Failed", "Setting read capacity units to 50000.", "Failed to set read
  capacity units to 50000. Reason: The requested ReadCapacityUnits, 50000, is above the per table maximum for
  the account in us-west-2. Per table maximum: 40000. Refer to the Amazon DynamoDB Developer Guide for current
  limits and how to request higher limits. (Service: AmazonDynamoD", "2026-10-09T00:42:20.357000+00:00"],
  ["InProgress", "Setting read capacity units to 5.", "Successfully set read capacity units to 5. Waiting for
  change to be fulfilled by dynamodb.", "2026-10-09T00:41:48.793000+00:00"]]. DescribeTable RCU samples: [[2,
  "ACTIVE", 5]]. Revert -> 200.
  - ACK: docs-only, scope:skip · ops: UpdateTableReplicaAutoScaling · fields: MinimumUnits, MaximumUnits
  - repro: service-quotas list-service-quotas dynamodb -> UpdateTableReplicaAutoScaling read Min=quota+10000
    -> describe-scaling-activities
  - handling: not handled in the controller (as of commit 34b85e6)
  - related: [DDB-TABLE-314](table-policy-kinesis-autoscaling.md#ddb-table-314), [DDB-TABLE-195](table-policy-kinesis-autoscaling.md#ddb-table-195), [DDB-TABLE-315](table-policy-kinesis-autoscaling.md#ddb-table-315), [DDB-TABLE-321](#ddb-table-321) · hypotheses: H-R-112 · evidence:
    table/error-taxonomy/autoscaling-validation
  - notes: H-R-112 confirmed: DynamoDB accepted Min 50000 > quota 40000, Describe shows Min 50000 as
    effective, and the failure is only visible in application-autoscaling describe-scaling-activities
    (StatusCode Failed, message quoting DynamoDB's per-table maximum). Table stayed at RCU 5.

- <a id="ddb-table-294"></a>**DDB-TABLE-294** `normalization` · impact high · handled · verified 2026-10-09
  **ReplicaUpdates.Update: RegionName-only rejected; same-value TableClassOverride accepted (UPDATING ~35s); class reported per replica**
  Update{RegionName only} -> ValidationException 'There are no actions specified in the Replica Update Action
  of the request.' Update{TableClassOverride:STANDARD} while the replica is STANDARD -> 200, table UPDATING
  for 34s (same-value Update is not a free no-op). Update{TableClassOverride:STANDARD_INFREQUENT_ACCESS} ->
  200; after 34s A.Replicas[us-east-1].ReplicaTableClassSummary.TableClass=STANDARD_INFREQUENT_ACCESS and
  us-east-1's own TableClassSummary=STANDARD_INFREQUENT_ACCESS, while A's TableClassSummary stays absent
  (STANDARD) and B's Replicas[us-west-2].ReplicaTableClassSummary stays absent. The UpdateTable response does
  not yet show the new class. Re-sending IA -> 200 and 58s UPDATING. UpdateTable TableClass=STANDARD issued
  directly in us-east-1 -> 200; A then shows ReplicaTableClassSummary.TableClass=STANDARD explicitly (with
  LastUpdateDateTime) - i.e. default STANDARD is omitted but an explicitly set STANDARD is reported.
  UpdateTable TableClass=IA on the BASE with a replica -> LimitExceededException 'Limit exceeded for replica
  in us-east-1. Updates to TableClass are limited to 2 times in 30 day(s)': a base TableClass change is
  applied to every replica.
  - ACK: compare.is_ignored+delta_pre_compare, compare.nil_equals_zero_value, custom_update, terminal_codes ·
    ops: UpdateTable, DescribeTable · fields: ReplicaUpdates.Update.TableClassOverride,
    Replicas.ReplicaTableClassSummary, TableClassSummary, TableClass
  - repro: table with ACTIVE replica -> UpdateTable ReplicaUpdates=[{Update:{RegionName, TableClassOverride}}]
    -> DescribeTable both regions
  - handling: handled via `pkg/resource/table/hooks_replica_updates.go:423-463; pkg/resource/table/hooks_replica_updates.go:28-147; pkg/resource/table/hooks_replica_updates.go:195-254; pkg/resource/table/hooks_replica_updates.go:348-360`
  - related: [DDB-TABLE-203](#ddb-table-203), [DDB-TABLE-223](#ddb-table-223), [DDB-TABLE-307](#ddb-table-307), [DDB-TABLE-225](#ddb-table-225), [DDB-TABLE-308](#ddb-table-308), [DDB-TABLE-437](table-throughput-billing.md#ddb-table-437),
    [DDB-TABLE-250](#ddb-table-250), [DDB-TABLE-252](#ddb-table-252), [DDB-TABLE-251](#ddb-table-251), [DDB-TABLE-321](#ddb-table-321), [DDB-TABLE-229](table-global-tables.md#ddb-table-229), [DDB-TABLE-296](#ddb-table-296), [DDB-TABLE-265](#ddb-table-265),
    [DDB-TABLE-295](#ddb-table-295), [DDB-TABLE-297](#ddb-table-297), [DDB-TABLE-322](table-policy-kinesis-autoscaling.md#ddb-table-322), [DDB-TABLE-204](#ddb-table-204), [DDB-TABLE-261](table-global-tables.md#ddb-table-261), [DDB-TABLE-226](#ddb-table-226), [DDB-TABLE-309](#ddb-table-309),
    [DDB-TABLE-221](#ddb-table-221) · hypotheses: H-R-012, H-R-015 · evidence: table/mutation-matrix/replica-overrides
  - notes: Qualifies H-R-012 (no-op Update IS rejected only when no action field is present; a same-value
    override is accepted and costs an UPDATING cycle) and H-R-015 (ReplicaTableClassSummary absent for default
    STANDARD, present once set explicitly - nil vs STANDARD must compare equal). The 2-per-30-days...
  - full notes: [details/DDB-TABLE-294.md](details/DDB-TABLE-294.md)

- <a id="ddb-table-309"></a>**DDB-TABLE-309** `normalization` · impact high · SUSPECTED CONTROLLER BUG · verified 2026-10-09
  **Per-replica CMK: bare key id accepted and read back as the full key ARN; ReplicaUpdates.Update KMSMasterKeyId is always rejected**
  Base encrypted with a us-west-2 CMK. Create us-east-1 with KMSMasterKeyId='<bare us-east-1 key id>' -> 200
  (response Replicas=[]); both ACTIVE after 41s (A UPDATING[] for 23s before the CREATING entry appeared).
  A.Replicas[us-east-1].KMSMasterKeyId = 'arn:aws:kms:us-east-1:<acct>:key/<id>' (full ARN); us-east-1
  SSEDescription.KMSMasterKeyArn = the same ARN; the replica's view of the base lists
  Replicas[us-west-2].KMSMasterKeyId = the us-west-2 CMK ARN. ReplicaUpdates=[Update{us-east-1,
  KMSMasterKeyId:<same ARN>}], [... <same bare id>] and [... 'alias/aws/dynamodb'] ALL fail with
  ValidationException 'One or more parameter values were invalid: KMSMasterKeyId must be specified for each
  replica.' (the same text an AWS-owned-key table returns, see table/mutation-matrix/replica-overrides).
  UpdateTable SSESpecification{Enabled:true, SSEType:KMS} (AWS managed, no key) on the base -> 200 with
  SSEDescription.Status=UPDATING in both regions; an SSESpecification change issued directly in us-east-1
  right after -> ResourceInUseException 'Server-Side Encryption is still being updated'. Earlier
  (table/state-machine/replica-creation-failed): Create without KMSMasterKeyId on a CMK table -> the same
  'must be specified for each replica' error; Create with 'alias/aws/dynamodb' on a CMK table -> 'All replica
  keys must either be Customer Managed CMK or AWS Managed CMK.'; Create with a us-west-2 key ARN/id for
  us-east-1 -> 'KMS validation error for region us-east-1: ... NotFoundException: Invalid arn us-west-2' /
  "Key 'arn:aws:kms:us-east-1:...:key/<id>' does not exist" (a bare id is resolved in the REPLICA region).
  - ACK: compare.is_ignored+delta_pre_compare, references, custom_update · ops: UpdateTable, DescribeTable ·
    fields: ReplicaUpdates.Create.KMSMasterKeyId, ReplicaUpdates.Update.KMSMasterKeyId,
    Replicas.KMSMasterKeyId, SSEDescription.KMSMasterKeyArn
  - repro: table with CMK SSE -> UpdateTable ReplicaUpdates=[Create us-east-1 KMSMasterKeyId=<bare id>] ->
    DescribeTable both regions -> Update variants
  - measurements: create_total_s=40.8
  - handling: suspected controller bug - see Handling gaps
  - related: [DDB-TABLE-226](#ddb-table-226), [DDB-TABLE-306](table-policy-kinesis-autoscaling.md#ddb-table-306), [DDB-TABLE-265](#ddb-table-265), [DDB-TABLE-308](#ddb-table-308), [DDB-TABLE-329](#ddb-table-329), [DDB-TABLE-187](#ddb-table-187),
    [DDB-TABLE-305](#ddb-table-305), [DDB-TABLE-249](#ddb-table-249), [DDB-TABLE-258](table-global-tables.md#ddb-table-258), [DDB-TABLE-313](#ddb-table-313), [DDB-TABLE-295](#ddb-table-295), [DDB-TABLE-328](table-streams-encryption-class.md#ddb-table-328), [DDB-TABLE-312](#ddb-table-312),
    [DDB-TABLE-310](#ddb-table-310), [DDB-TABLE-311](#ddb-table-311), [DDB-TABLE-327](#ddb-table-327), [DDB-TABLE-204](#ddb-table-204), [DDB-TABLE-261](table-global-tables.md#ddb-table-261), [DDB-TABLE-250](#ddb-table-250), [DDB-TABLE-294](#ddb-table-294),
    [DDB-TABLE-296](#ddb-table-296), [DDB-TABLE-221](#ddb-table-221) · hypotheses: H-R-016, H-R-012 · evidence:
    table/state-machine/replica-kms-lifecycle
  - notes: Confirms H-R-016: keys must resolve in the replica region, a bare id is accepted and normalised to
    an ARN (spec never string-equals status), all replicas must share the key TYPE (all CMK or all AWS
    managed). Refutes the assumption that the replica key can be rotated through ReplicaUpdates.Update -...
  - full notes: [details/DDB-TABLE-309.md](details/DDB-TABLE-309.md)

- <a id="ddb-table-313"></a>**DDB-TABLE-313** `normalization` · impact high · unhandled (not handled in controller) · verified 2026-10-09
  **CMK-encrypted table: every replica Create must carry a KMSMasterKeyId resolvable in the replica region; no mixing CMK and AWS-managed keys**
  Base with a us-west-2 CMK. Create us-east-1 WITHOUT KMSMasterKeyId -> ValidationException 'One or more
  parameter values were invalid: KMSMasterKeyId must be specified for each replica.' Create eu-west-1 with
  KMSMasterKeyId='alias/aws/dynamodb' -> ValidationException 'One or more parameter values were invalid: All
  replica keys must either be Customer Managed CMK or AWS Managed CMK.' Create us-east-1 with the us-west-2
  key ARN -> ValidationException 'KMS validation error for region us-east-1:
  com.amazonaws.services.kms.model.NotFoundException: Invalid arn us-west-2 ...'; with the bare us-west-2 key
  id -> ValidationException "KMS validation error for region us-east-1: ... Key
  'arn:aws:kms:us-east-1:<acct>:key/<id>' does not exist" (bare ids are resolved in the replica region). All
  synchronous, 1.7-3.6s latency.
  - ACK: compare.is_ignored+delta_pre_compare, references, custom_update · ops: UpdateTable, DescribeTable ·
    fields: ReplicaUpdates.Create.KMSMasterKeyId, Replicas.KMSMasterKeyId, SSEDescription.KMSMasterKeyArn
  - repro: table with SSE CMK -> UpdateTable ReplicaUpdates=[Create us-east-1] / [Create eu-west-1
    KMSMasterKeyId=alias/aws/dynamodb] -> DescribeTable everywhere
  - measurements: default_key_create_total_s=null
  - handling: not handled in the controller (as of commit 34b85e6)
  - related: [DDB-TABLE-309](#ddb-table-309), [DDB-TABLE-295](#ddb-table-295), [DDB-TABLE-328](table-streams-encryption-class.md#ddb-table-328), [DDB-TABLE-312](#ddb-table-312), [DDB-TABLE-310](#ddb-table-310), [DDB-TABLE-311](#ddb-table-311),
    [DDB-TABLE-329](#ddb-table-329), [DDB-TABLE-327](#ddb-table-327) · hypotheses: H-R-016 · evidence: table/state-machine/replica-creation-failed
  - notes: Confirms H-R-016 (per-region key resolution; alias 'alias/aws/dynamodb' is NOT accepted on a CMK
    group, so there is no AWS-managed default for a CMK table's replicas). For an AWS-owned-key table the
    replica Create needs no key and any KMSMasterKeyId is rejected...
  - full notes: [details/DDB-TABLE-313.md](details/DDB-TABLE-313.md)

## Response fidelity and consistency

- <a id="ddb-table-238"></a>**DDB-TABLE-238** `stale-response` · impact medium · unhandled (not handled in controller) · verified 2026-10-09
  **UpdateTableReplicaAutoScaling response echoes PRE-update settings; Describe reflects the change after 0.43s**
  Response of the read update carried read settings {"AutoScalingDisabled": true, "ScalingPolicies": []}
  (previous state); Describe at 1/s first differed from the previous state at t=0.43s (samples [0.43]).
  Custom-policy update converged in 0.63s; disable converged in 0.6s.
  - ACK: requeue, docs-only · ops: UpdateTableReplicaAutoScaling, DescribeTableReplicaAutoScaling
  - repro: UpdateTableReplicaAutoScaling -> compare response TableAutoScalingDescription with Describe polled
    at 1/s
  - measurements: converge_minimal_s=0.43, converge_custom_s=0.63, converge_disable_s=0.6
  - handling: not handled in the controller (as of commit 34b85e6)
  - related: [DDB-TABLE-192](#ddb-table-192), [DDB-TABLE-198](table-policy-kinesis-autoscaling.md#ddb-table-198), [DDB-TABLE-242](table-policy-kinesis-autoscaling.md#ddb-table-242), [DDB-TABLE-314](table-policy-kinesis-autoscaling.md#ddb-table-314) · hypotheses: H-S-042 · evidence:
    table/round-trip/autoscaling-settings

- <a id="ddb-table-289"></a>**DDB-TABLE-289** `response-fidelity` · impact medium · unhandled (not handled in controller) · verified 2026-10-09
  **GSI entries in DescribeTableReplicaAutoScaling: keys ['IndexName', 'IndexStatus', 'ProvisionedReadCapacityAutoScalingSettings', 'Provisio...**
  Replicas[].GlobalSecondaryIndexes[] entry for gsi0 in us-west-2: {"IndexName": "gsi0", "IndexStatus":
  "ACTIVE", "ProvisionedReadCapacityAutoScalingSettings": {"AutoScalingDisabled": true, "ScalingPolicies":
  []}, "ProvisionedWriteCapacityAutoScalingSettings": {"MinimumUnits": 1, "MaximumUnits": 10,
  "AutoScalingRoleArn": "arn:aws:iam::<ACCOUNT>:role/<PRINCIPAL> in us-east-1: {"IndexName": "gsi0",
  "IndexStatus": "ACTIVE", "ProvisionedReadCapacityAutoScalingSettings": {"AutoScalingDisabled": true,
  "ScalingPolicies": []}, "ProvisionedWriteCapacityAutoScalingSettings": {"MinimumUnits": 1, "MaximumUnits":
  10, "AutoScalingRoleArn": "arn:aws:iam::<ACCOUNT>:role/<PRINCIPAL> After enabling all four dimensions:
  {"IndexName": "gsi0", "IndexStatus": "ACTIVE", "ProvisionedReadCapacityAutoScalingSettings":
  {"MinimumUnits": 1, "MaximumUnits": 10, "AutoScalingRoleArn": "arn:aws:iam::<ACCOUNT>:role/<PRINCIPAL>",
  "ScalingPolicies": [{"PolicyName": "DynamoDBReadCapacityUtilization:table/ackq-cf966e-as-gsi/index/gsi0",
  "TargetTrackingScalingPolicyConfiguration": {"TargetValue": 70.0}}]}, "ProvisionedWrite; AAS targets A
  [["/", "table:R", 1, 10], ["/", "table:W", 1, 10], ["/index/gsi0", "index:R", 1, 10], ["/index/gsi0",
  "index:W", 1, 12]], B [["/", "table:W", 1, 10], ["/index/gsi0", "index:W", 1, 12]]; policies [["/", "R",
  "DynamoDBReadCapacityUtilization:table/ac"], ["/", "W", "ackq-cf966e-ackq-cf966e-as-gsi-Units"],
  ["/index/gsi0", "R", "DynamoDBReadCapacityUtilization:table/ac"], ["/index/gsi0", "W",
  "DynamoDBWriteCapacityUtilization:table/a"]]; alarms per kind {"-AlarmHigh": 2, "-AlarmLow": 2,
  "-ProvisionedCapacityHigh": 2, "-ProvisionedCapacityLow": 2, "/index/gsi0-AlarmHigh": 2,
  "/index/gsi0-AlarmLow": 2, "/index/gsi0-ProvisionedCapacityHigh": 2, "/index/gsi0-ProvisionedCapacityLow":
  2}.
  - ACK: compare.nil_equals_zero_value, scope:skip · ops: DescribeTableReplicaAutoScaling,
    UpdateTableReplicaAutoScaling · fields: GlobalSecondaryIndexes, GlobalSecondaryIndexUpdates,
    ReplicaGlobalSecondaryIndexUpdates
  - repro: global PROVISIONED table with GSI -> DescribeTableReplicaAutoScaling -> enable table read, GSI read
    (ReplicaUpdates) and GSI write (GlobalSecondaryIndexUpdates)
  - handling: not handled in the controller (as of commit 34b85e6)
  - related: [DDB-TABLE-189](table-policy-kinesis-autoscaling.md#ddb-table-189), [DDB-TABLE-195](table-policy-kinesis-autoscaling.md#ddb-table-195), [DDB-TABLE-243](table-policy-kinesis-autoscaling.md#ddb-table-243), [DDB-TABLE-232](#ddb-table-232), [DDB-TABLE-317](table-policy-kinesis-autoscaling.md#ddb-table-317), [DDB-TABLE-237](table-policy-kinesis-autoscaling.md#ddb-table-237) ·
    hypotheses: H-R-102, H-R-108 · evidence: table/dependencies/autoscaling-gsi-and-orphans
  - notes: H-R-102 for GSIs: never-configured GSI read renders as {AutoScalingDisabled:true,
    ScalingPolicies:[]} exactly like the table level; GSI entries carry IndexStatus. GSI write updates go
    through GlobalSecondaryIndexUpdates (all replicas), GSI read through...
  - full notes: [details/DDB-TABLE-289.md](details/DDB-TABLE-289.md)

- <a id="ddb-table-308"></a>**DDB-TABLE-308** `eventual-consistency` · impact high · unhandled (not handled in controller) · verified 2026-10-09
  **Re-adding a replica after its Replicas[] entry vanishes is rejected until the replica-region table is gone (~20s gap)**
  ReplicaUpdates.Delete us-east-1 on a table that had held one item (items deleted beforehand): A's Replicas[]
  entry vanished at 184s (A UPDATING 46s -> ACTIVE[DELETING] 135s -> UPDATING 2s -> ACTIVE[]) while us-east-1
  was still 'DELETING' with Replicas=[]. Create us-east-1 re-issued at +0.7s, +6.4s, +12.1s ->
  ValidationException 'Failed to create a the new replica ... because one or more replicas already existed as
  tables.'; accepted at +19.4s, the first attempt after us-east-1 returned ResourceNotFoundException. The
  re-creation took 16.5s (both ACTIVE), i.e. the 'empty table' fast path is decided by actual content, not by
  the stale ItemCount (still 0 throughout).
  - ACK: requeue, custom_update, terminal_codes · ops: UpdateTable, DescribeTable · fields: ReplicaUpdates,
    Replicas
  - repro: UpdateTable Delete replica -> poll A until Replicas empty -> UpdateTable Create same region every
    5s
  - measurements: a_entry_gone_s=184.0, b_notfound_gap_s=19.4, recreate_total_s=16.5
  - handling: not handled in the controller (as of commit 34b85e6)
  - related: [DDB-TABLE-203](#ddb-table-203), [DDB-TABLE-223](#ddb-table-223), [DDB-TABLE-307](#ddb-table-307), [DDB-TABLE-225](#ddb-table-225), [DDB-TABLE-294](#ddb-table-294), [DDB-TABLE-437](table-throughput-billing.md#ddb-table-437),
    [DDB-TABLE-226](#ddb-table-226), [DDB-TABLE-306](table-policy-kinesis-autoscaling.md#ddb-table-306), [DDB-TABLE-265](#ddb-table-265), [DDB-TABLE-309](#ddb-table-309), [DDB-TABLE-329](#ddb-table-329), [DDB-TABLE-187](#ddb-table-187), [DDB-TABLE-305](#ddb-table-305),
    [DDB-TABLE-249](#ddb-table-249), [DDB-TABLE-258](table-global-tables.md#ddb-table-258), [DDB-TABLE-204](#ddb-table-204), [DDB-TABLE-293](#ddb-table-293), [DDB-TABLE-222](#ddb-table-222), [DDB-TABLE-297](#ddb-table-297), [DDB-TABLE-267](#ddb-table-267),
    [DDB-TABLE-261](table-global-tables.md#ddb-table-261), [DDB-TABLE-231](table-global-tables.md#ddb-table-231) · hypotheses: H-R-026 · evidence: table/dependencies/replica-prerequisites
  - notes: Confirms H-R-026 part 2 with the exact code (ValidationException, not ResourceInUseException): the
    readiness gate for re-adding a region is DescribeTable in that region returning ResourceNotFoundException.
    Deleting a replica of a (formerly) non-empty table took ~3 min vs ~1.5 min for an empty one.

## Delete semantics

- <a id="ddb-table-204"></a>**DDB-TABLE-204** `delete-semantics` · impact high · handled · verified 2026-10-09
  **ReplicaUpdates.Delete deletes the regional table; source stays UPDATING ~36 s then replica DELETING ~88 s; response echoes ACTIVE**
  UpdateTable ReplicaUpdates=[Delete us-east-1] returned 200 with TableStatus=UPDATING but the echoed Replicas
  entry still said ReplicaStatus=ACTIVE. DescribeTable then showed TableStatus=UPDATING with the replica
  ACTIVE for 36.3 s, then TableStatus=ACTIVE with the replica DELETING for 88.2 s, then no Replicas key (124.7
  s total). The us-east-1 table itself was still DELETING at that point (DeleteTable there ->
  ResourceInUseException "Table is being deleted") and disappeared shortly after. The source table lost
  GlobalTableVersion and kept its stream.
  - ACK: requeue, synced.when, e2e-timing · ops: UpdateTable, DescribeTable, DeleteTable · fields:
    ReplicaUpdates, Replicas, GlobalTableVersion
  - repro: EVENTUAL group -> UpdateTable ReplicaUpdates=[Delete us-east-1] -> poll DescribeTable (both
    regions)
  - measurements: replica_delete_total_s=124.67, source_updating_s=36.3, replica_deleting_s=88.2
  - handling: handled via `templates/hooks/table/sdk_delete_pre_build_request.go.tpl:8-22; pkg/resource/table/hooks_replica_updates.go:256-263; generator.yaml:27-31; pkg/resource/table/hooks_replica_updates.go:277-373`
  - related: [DDB-TABLE-293](#ddb-table-293), [DDB-TABLE-308](#ddb-table-308), [DDB-TABLE-222](#ddb-table-222), [DDB-TABLE-297](#ddb-table-297), [DDB-TABLE-267](#ddb-table-267), [DDB-TABLE-261](table-global-tables.md#ddb-table-261),
    [DDB-TABLE-231](table-global-tables.md#ddb-table-231), [DDB-TABLE-221](#ddb-table-221), [DDB-TABLE-305](#ddb-table-305), [DDB-TABLE-224](#ddb-table-224), [DDB-TABLE-250](#ddb-table-250), [DDB-TABLE-294](#ddb-table-294), [DDB-TABLE-296](#ddb-table-296),
    [DDB-TABLE-226](#ddb-table-226), [DDB-TABLE-309](#ddb-table-309) · hypotheses: H-R-001, H-R-041 · evidence:
    table/cross-region/mrec-replica-updates
  - notes: Contrast for H-R-041 (legacy delete would have left the regional table; untestable in 2026). The
    'TableStatus ACTIVE while a replica is DELETING' window means Synced must look at
    Replicas[].ReplicaStatus, not TableStatus alone.

- <a id="ddb-table-266"></a>**DDB-TABLE-266** `delete-semantics` · impact high · handled · verified 2026-10-09
  **DeleteTable on a table with replicas is rejected while a replica it sourced (<24h) exists; the stated reason is the 24h source-region rule**
  3-region group (us-east-1 added from us-west-2 at T+0, eu-west-1 added from the us-east-1 endpoint at
  T+11min). DeleteTable in us-west-2 while both replicas were ACTIVE -> ValidationException (HTTP 400)
  'Replica cannot be deleted because it has acted as a source region for new replica(s) being added to the
  table in the last 24 hours.' The same message was returned 13 min later while the last remaining replica
  (us-east-1, sourced from us-west-2) was DELETING. Base-table deletion only succeeded once Replicas[] was
  empty. DeleteTable issued in us-east-1 (a replica, while itself DELETING) -> ResourceInUseException 'The
  resource which you are attempting to change is in use.'
  - ACK: custom_delete, pre-delete-cleanup, deletable.when, requeue · ops: DeleteTable · fields: Replicas
  - repro: 3-region table -> DeleteTable in the base region
  - handling: handled via `templates/hooks/table/sdk_delete_pre_build_request.go.tpl:8-22; pkg/resource/table/hooks_replica_updates.go:256-263`
  - related: [DDB-TABLE-262](#ddb-table-262), [DDB-TABLE-267](#ddb-table-267), [DDB-TABLE-265](#ddb-table-265), [DDB-TABLE-297](#ddb-table-297), [DDB-TABLE-260](table-global-tables.md#ddb-table-260), [DDB-TABLE-442](service.md#ddb-table-442),
    [DDB-TABLE-312](#ddb-table-312), [DDB-TABLE-311](#ddb-table-311) · hypotheses: H-R-010, H-R-011 · evidence:
    table/cross-region/multi-replica-delete-semantics
  - notes: Confirms H-R-011 in practice for any group whose replicas were added from the base within the last
    24h (i.e. the normal controller flow); refutes H-R-010 for that window. Whether DeleteTable on a base
    succeeds (and orphans replicas) once every replica it sourced is older than 24h remains untested...
  - full notes: [details/DDB-TABLE-266.md](details/DDB-TABLE-266.md)

- <a id="ddb-table-267"></a>**DDB-TABLE-267** `delete-semantics` · impact high · handled · verified 2026-10-09
  **Source-region rule: a replica that was the endpoint/source of another replica added <24h ago cannot be removed until that replica is gone**
  With eu-west-1 (added from the us-east-1 endpoint) ACTIVE: ReplicaUpdates=[Delete us-east-1] from us-west-2
  -> ValidationException 'Replica cannot be deleted because it has acted as a source region for new replica(s)
  being added to the table in the last 24 hours.' ReplicaUpdates=[Delete eu-west-1] -> 200 (gone after ~2 min:
  A UPDATING 31s -> eu-west-1 DELETING ~65s -> ResourceNotFoundException at 121s). 13 minutes later, with
  eu-west-1 gone, Delete us-east-1 -> 200 (accepted). While that delete ran: ReplicaUpdates=[Delete us-west-2]
  issued at us-east-1 -> ResourceInUseException; DeleteTable in us-east-1 -> ResourceInUseException;
  DeleteTable in us-west-2 -> the 24h-source ValidationException (us-east-1 was sourced from us-west-2 <24h
  ago and still existed). Same message seen for ReplicaUpdates.Delete of the base region issued from a replica
  while that replica was still CREATING (table/state-machine/replica-creating-side-ops).
  - ACK: custom_delete, pre-delete-cleanup, terminal_codes, requeue · ops: UpdateTable, DeleteTable · fields:
    ReplicaUpdates.Delete, Replicas
  - repro: Create replica B from A; Create replica C from B's endpoint; try to remove B (or A) within 24h
  - measurements: eu_west_1_delete_total_s=121.0
  - handling: handled via `templates/hooks/table/sdk_delete_pre_build_request.go.tpl:8-22; pkg/resource/table/hooks_replica_updates.go:256-263`
  - related: [DDB-TABLE-204](#ddb-table-204), [DDB-TABLE-293](#ddb-table-293), [DDB-TABLE-308](#ddb-table-308), [DDB-TABLE-222](#ddb-table-222), [DDB-TABLE-297](#ddb-table-297), [DDB-TABLE-261](table-global-tables.md#ddb-table-261),
    [DDB-TABLE-231](table-global-tables.md#ddb-table-231), [DDB-TABLE-266](#ddb-table-266), [DDB-TABLE-262](#ddb-table-262), [DDB-TABLE-265](#ddb-table-265), [DDB-TABLE-260](table-global-tables.md#ddb-table-260), [DDB-TABLE-442](service.md#ddb-table-442), [DDB-TABLE-312](#ddb-table-312),
    [DDB-TABLE-311](#ddb-table-311) · hypotheses: H-R-053, H-R-010, H-R-011, H-R-006 · evidence:
    table/cross-region/multi-replica-delete-semantics
  - notes: Qualifies H-R-053: deleting every replica in one call is impossible (one action per call) AND the
    order matters - replicas must be removed leaf-first (most recently added / those not used as a source
    first); a Delete rejected with the 24h-source message becomes acceptable as soon as the replicas it...
  - full notes: [details/DDB-TABLE-267.md](details/DDB-TABLE-267.md)

- <a id="ddb-table-292"></a>**DDB-TABLE-292** `delete-semantics` · impact high · unhandled (not handled in controller) · verified 2026-10-09
  **DeleteTable (and replica removal) orphan AAS targets/policies/alarms; recreating the name does not re-arm them within 4 min**
  Before delete: {"targets_a": [["/", "table:R", 3, 10], ["/", "table:W", 1, 10], ["/index/gsi0", "index:R",
  1, 10]], "targets_b": [["/", "table:W", 1, 10]], "policies_a": [["/", "R",
  "DynamoDBReadCapacityUtilization:table/ac"], ["/", "W", "DynamoDBWriteCapacityUtilization:table/a"],
  ["/index/gsi0", "R", "DynamoDBReadCapacityUtilization:table/ac"]], "alarms": {"-AlarmHigh": 2, "-AlarmLow":
  2, "-ProvisionedCapacityHigh": 2, "-ProvisionedCapacityLow": 2, "/index/gsi0-AlarmHigh": 1,
  "/index/gsi0-AlarmLow": 1, "/ind. After removing the replica: {"targets_a": [["/", "table:R", 3, 10], ["/",
  "table:W", 1, 10], ["/index/gsi0", "index:R", 1, 10]], "targets_b": [["/", "table:W", 1, 10]], "alarms":
  {"-AlarmHigh": 2, "-AlarmLow": 2, "-ProvisionedCapacityHigh": 2, "-ProvisionedCapacityLow": 2,
  "/index/gsi0-AlarmHigh": 1, "/index/gsi0-AlarmLow": 1, "/index/gsi0-ProvisionedCapacityHigh": 1,
  "/index/gsi0-ProvisionedCapacityLow": 1}}. After DeleteTable (RNF reached) samples: [{"t_s": 0, "targets_a":
  [["/", "table:R", 3, 10], ["/", "table:W", 1, 10], ["/index/gsi0", "index:R", 1, 10]], "policies_a": [["/",
  "R", "DynamoDBReadCapacityUtilization:table/ac"], ["/", "W", "DynamoDBWriteCapacityUtilization:table/a"],
  ["/index/gsi0", "R", "DynamoDBReadCapacityUtilization:table/ac"]], "alarms": {"-AlarmHigh": 2, "-AlarmLow":
  2, "-ProvisionedCapacityHigh": 2, "-ProvisionedCapacityLow": 2, "/index/gsi0-AlarmHigh": 1,
  "/index/gsi0-AlarmLow": 1, "/index/gsi0-ProvisionedCapacityHigh": 1, "/index/gsi0-ProvisionedCapacityLow":
  1}}, {"t_s": 30, "targets_a": [["/", "table:R", 3, 10], ["/", "table:W", 1, 10], ["/index/gsi0", "index:R",
  1, 10]], "policies_a": [["/", "R", "DynamoDBReadCapacityUtilization:table/ac"], ["/", "W",
  "DynamoDBWriteCapacityUtilization:table/a"], ["/index/gsi0", "R",
  "DynamoDBReadCapacityUtilization:table/ac"]], "alarms": {"-AlarmHigh": 2, "-AlarmLow": 2,. Recreating the
  same table name (PROVISIONED 1/1, regional): RCU transitions over 4 min [{"t_s": 0.0, "status": "ACTIVE",
  "rcu": 1}]; read scaling activities [["2026-10-09T00:58:20.225000+00:00", "Successful", "Setting read
  capacity units to 3.", "Successfully set read capacity units to 3. Change successfully fulfilled by
  dynamodb."]]; DescribeTableReplicaAutoScaling on the recreated regional table -> ResourceNotFoundException.
  - ACK: pre-delete-cleanup, scope:skip · ops: DeleteTable, CreateTable, DescribeScalableTargets · fields:
    ProvisionedThroughput
  - repro: autoscaled table -> remove replica -> DeleteTable -> describe-scalable-targets at 0/30/90 s ->
    CreateTable same name -> poll ProvisionedThroughput
  - handling: not handled in the controller (as of commit 34b85e6)
  - related: [DDB-TABLE-293](#ddb-table-293), [DDB-TABLE-242](table-policy-kinesis-autoscaling.md#ddb-table-242), [DDB-TABLE-198](table-policy-kinesis-autoscaling.md#ddb-table-198), [DDB-TABLE-290](table-policy-kinesis-autoscaling.md#ddb-table-290), [DDB-TABLE-323](table-policy-kinesis-autoscaling.md#ddb-table-323), [DDB-TABLE-243](table-policy-kinesis-autoscaling.md#ddb-table-243),
    [DDB-TABLEREPLICAAUTOSCALING-001](#ddb-tablereplicaautoscaling-001), [DDB-TABLE-193](table-policy-kinesis-autoscaling.md#ddb-table-193), [DDB-TABLE-197](table-policy-kinesis-autoscaling.md#ddb-table-197), [DDB-TABLE-196](table-policy-kinesis-autoscaling.md#ddb-table-196), [DDB-TABLE-189](table-policy-kinesis-autoscaling.md#ddb-table-189), [DDB-TABLE-195](table-policy-kinesis-autoscaling.md#ddb-table-195)
    · hypotheses: H-R-131 · evidence: table/dependencies/autoscaling-gsi-and-orphans
  - notes: H-R-131 row 3 confirmed: 90 s after the table was gone all 3 targets, 3 policies and 12 alarms
    still existed in us-west-2, and the replica region kept the table write target after the replica was
    removed (targets_b [["/", "table:W", 1, 10]]). Recreating the same table name (regional, RCU 1) with an...
  - full notes: [details/DDB-TABLE-292.md](details/DDB-TABLE-292.md)

- <a id="ddb-table-297"></a>**DDB-TABLE-297** `delete-semantics` · impact high · unhandled (not handled in controller) · verified 2026-10-09
  **Deleting a replica: DeletionProtection on the replica blocks ReplicaUpdates.Delete; DeleteTable in the replica region removes the replica**
  With DeletionProtectionEnabled=true set directly on the us-east-1 replica table (which made the BASE go
  UPDATING for 34s; A.DeletionProtectionEnabled stays false): ReplicaUpdates=[Delete us-east-1] from the base
  -> ValidationException 'Cannot delete table <name> in region us-east-1 because it has deletion protection
  enabled. Disable deletion protection first.'; DeleteTable in us-east-1 -> same message. After disabling DP
  in us-east-1: DeleteTable in us-east-1 -> 200 (response TableStatus=DELETING, Replicas=[us-west-2 ACTIVE]);
  base: UPDATING 31s -> ACTIVE[us-east-1 DELETING] 140s -> UPDATING 3s -> ACTIVE with Replicas gone at 178s;
  us-east-1 ResourceNotFoundException at 190s. StreamSpecification{StreamEnabled:false} right after the entry
  vanished -> 200, but DeleteTable 11ms later -> ResourceInUseException 'Cannot delete table while stream is
  being enabled/disabled.' Settings touched from the replica side: TagResource (regional), UpdateTimeToLive
  attr change (propagated to the base within 60s - TTL is group-wide from any region), PITR (regional),
  DeletionProtection (regional).
  - ACK: custom_delete, pre-delete-cleanup, terminal_codes · ops: UpdateTable, DeleteTable, DescribeTable ·
    fields: ReplicaUpdates.Delete, DeletionProtectionEnabled, Replicas
  - repro: UpdateTable DeletionProtectionEnabled=true in us-east-1 on the replica; UpdateTable
    ReplicaUpdates=[Delete us-east-1] in us-west-2; DeleteTable in us-east-1
  - measurements: replica_direct_delete_total_s=190.4, b_dp_toggle_base_updating_s=34.1
  - handling: not handled in the controller (as of commit 34b85e6)
  - related: [DDB-TABLE-204](#ddb-table-204), [DDB-TABLE-293](#ddb-table-293), [DDB-TABLE-308](#ddb-table-308), [DDB-TABLE-222](#ddb-table-222), [DDB-TABLE-267](#ddb-table-267), [DDB-TABLE-261](table-global-tables.md#ddb-table-261),
    [DDB-TABLE-231](table-global-tables.md#ddb-table-231), [DDB-TABLE-266](#ddb-table-266), [DDB-TABLE-262](#ddb-table-262), [DDB-TABLE-265](#ddb-table-265), [DDB-TABLE-260](table-global-tables.md#ddb-table-260), [DDB-TABLE-327](#ddb-table-327), [DDB-TABLE-263](table-global-tables.md#ddb-table-263),
    [DDB-TABLE-329](#ddb-table-329), [DDB-TABLE-311](#ddb-table-311), [DDB-TABLE-221](#ddb-table-221), [DDB-TABLE-305](#ddb-table-305), [DDB-TABLE-224](#ddb-table-224), [DDB-TABLE-296](#ddb-table-296), [DDB-TABLE-251](#ddb-table-251),
    [DDB-TABLE-294](#ddb-table-294), [DDB-TABLE-295](#ddb-table-295), [DDB-TABLE-322](table-policy-kinesis-autoscaling.md#ddb-table-322) · hypotheses: H-R-027, H-R-009, H-R-004, H-R-028 · evidence:
    table/mutation-matrix/replica-overrides
  - notes: Confirms H-R-027 (per-region deletion protection guards ReplicaUpdates.Delete; message names the
    region) and H-R-009 (out-of-band DeleteTable on a replica is allowed when that replica was not a source;
    the base sees DELETING then a missing entry - pure drift). Any UpdateTable on any member flips...
  - full notes: [details/DDB-TABLE-297.md](details/DDB-TABLE-297.md)

## Cross-region

- <a id="ddb-table-194"></a>**DDB-TABLE-194** `cross-region` · impact high · unhandled (not handled in controller) · verified 2026-10-09
  **Write autoscaling is per-region AAS state: after replica create region B has 1 target(s); table-level write update touched B: [{"dim": "W...**
  Source table had AAS write target Min 1 Max 10 + custom policy (target 55) in us-west-2. After the replica
  became ACTIVE, AAS in us-east-1: targets [{"dim": "WriteCapacityUnits", "min": 1, "max": 10, "role":
  "AWSServiceRoleForApplicationAutoScaling_DynamoDBTable"}], policies [{"dim": "WriteCapacityUnits", "name":
  "ackq-0ad4a5-custom-write-policy", "type": "TargetTrackingScaling", "target": 55.0}], alarms
  ['TargetTracking-table/ackq-0ad4a5-as-prov-AlarmHigh-febf442e-53ee-4f10-8dd7-4965da6c2924',
  'TargetTracking-table/ackq-0ad4a5-as-prov-ProvisionedCapacityHigh-db57f3ca-04b3-48f6-bf88-9512af99f459',
  'TargetTracking-table/ackq-0ad4a5-as-prov-ProvisionedCapacityLow-ef66dddb-e85d-44e9-8f36-1512119b441d'].
  Describe write settings under the us-east-1 replica: {"MinimumUnits": 1, "MaximumUnits": 10,
  "AutoScalingRoleArn": "arn:aws:iam::<ACCOUNT>:role/<PRINCIPAL>", "ScalingPolicies": [{"PolicyName":
  "ackq-0ad4a5-custom-write-policy", "TargetTrackingS. After
  UpdateTableReplicaAutoScaling(ProvisionedWriteCapacityAutoScalingUpdate Min 1 Max 12): Describe write a/b
  identical True; AAS targets A [{"dim": "ReadCapacityUnits", "min": 1, "max": 10, "role":
  "AWSServiceRoleForApplicationAutoScaling_DynamoDBTable"}, {"dim": "WriteCapacityUnits", "min": 1, "max": 10,
  "role": "AWSServiceRoleForApplicationAutoScaling_DynamoDBTable"}], B [{"dim": "WriteCapacityUnits", "min":
  1, "max": 10, "role": "AWSServiceRoleForApplicationAutoScaling_DynamoDBTable"}]; policies A [{"dim":
  "WriteCapacityUnits", "name": "ackq-0ad4a5-custom-write-policy", "type": "TargetTrackingScaling", "target":
  55.0}, {"dim": "ReadCapacityUnits", "name": "DynamoDBReadCapacityUtilization:table/ackq-0ad4a5-as-prov",
  "type": "TargetTrackingScaling", "target": 70.0}], B [{"dim": "WriteCapacityUnits", "name":
  "ackq-0ad4a5-custom-write-policy", "type": "TargetTrackingScaling", "target": 55.0}]. Read update via
  ReplicaUpdates[us-west-2] left B read settings {"AutoScalingDisabled": true, "ScalingPolicies": []}.
  - ACK: scope:skip, docs-only · ops: UpdateTable, UpdateTableReplicaAutoScaling · fields: ReplicaUpdates,
    ProvisionedWriteCapacityAutoScalingUpdate, ReplicaProvisionedWriteCapacityAutoScalingSettings
  - repro: AAS write autoscaling in A -> add replica B -> describe-scalable-targets in B ->
    UpdateTableReplicaAutoScaling write -> describe-scalable-targets A and B
  - handling: not handled in the controller (as of commit 34b85e6)
  - related: [DDB-TABLE-249](#ddb-table-249), [DDB-TABLE-288](#ddb-table-288), [DDB-TABLE-291](table-policy-kinesis-autoscaling.md#ddb-table-291), [DDB-TABLE-322](table-policy-kinesis-autoscaling.md#ddb-table-322), [DDB-TABLE-290](table-policy-kinesis-autoscaling.md#ddb-table-290) · hypotheses: H-R-033,
    H-S-039, H-R-131 · evidence: table/sub-resources/replica-autoscaling-facade

- <a id="ddb-table-230"></a>**DDB-TABLE-230** `cross-region` · impact high · unhandled (not handled in controller) · verified 2026-10-09
  **Which table settings replicate to the replica region (tags, TTL, PITR, resource policy, insights, deletion protection, table class)**
  Right after the replica became ACTIVE, us-east-1 showed: TTL ENABLED (set on A during CREATING -> replicated
  at creation), tags [] (A had 3 tags), PITR DISABLED (A ENABLED), resource policy PolicyNotFoundException (A
  had one), ContributorInsights DISABLED (A ENABLED), DeletionProtectionEnabled false, stream
  NEW_AND_OLD_IMAGES (own ARN). Then set on A: a new tag, PITR (already on), resource policy, insights,
  DeletionProtectionEnabled=true (UPDATING for 39s). After 240s of polling (every 10s) NONE of tags / PITR /
  policy / insights / deletion protection had appeared in us-east-1. Only TimeToLive is group-wide (and the
  re-created replica inherited it again).
  - ACK: tags.custom-sync, custom_update, compare.is_ignored+delta_pre_compare · ops: TagResource,
    UpdateTimeToLive, UpdateContinuousBackups, PutResourcePolicy, UpdateContributorInsights, UpdateTable ·
    fields: Tags, TimeToLiveSpecification, PointInTimeRecoverySpecification, DeletionProtectionEnabled,
    ResourcePolicy
  - repro: Set each setting on the base table, read the equivalent in the replica region
  - measurements: dp_toggle_updating_s_with_replica=39.3, propagation_watch_s=240
  - handling: not handled in the controller (as of commit 34b85e6)
  - related: [DDB-TABLE-215](table-restore.md#ddb-table-215), [DDB-BACKUP-009](backup.md#ddb-backup-009), [DDB-BACKUP-021](backup.md#ddb-backup-021), [DDB-BACKUP-004](backup.md#ddb-backup-004), [DDB-TABLE-218](table-restore.md#ddb-table-218) · hypotheses:
    H-R-028 · evidence: table/state-machine/replica-create-timeline
  - notes: Confirms H-R-028: tags, PITR, resource policy, Contributor Insights and DeletionProtection are
    regional and must be applied per replica region with a regional client; TTL is replicated. Enabling
    DeletionProtection on a table with a replica keeps TableStatus UPDATING for ~40s (vs seconds on a...
  - full notes: [details/DDB-TABLE-230.md](details/DDB-TABLE-230.md)

- <a id="ddb-table-251"></a>**DDB-TABLE-251** `cross-region` · impact high · handled · verified 2026-10-09
  **Manual ProvisionedThroughput changes in any region replicate group-wide (no override materialized); per-region RCU only via overrides**
  UpdateTable ProvisionedThroughput RCU5/WCU10 on the base -> 200; after 38s the us-east-1 table also reports
  5/10 (A UPDATING 34s, B UPDATING 3s longer). UpdateTable issued directly in us-east-1 with RCU 7 / WCU 10 ->
  200, and the BASE table's RCU became 7 as well (A.Replicas[].ProvisionedThroughputOverride stays absent).
  UpdateTable in us-east-1 WCU 20 -> 200 and the base's WCU became 20. BillingMode=PAY_PER_REQUEST on the base
  with a replica -> 200; both regions switched (GSIs UPDATING for ~5 min). The replica entry shows
  GlobalTableSettingsReplicationMode=ENABLED_WITH_OVERRIDES.
  - ACK: compare.is_ignored+delta_pre_compare, custom_update, requeue, annotation-shadow-state · ops:
    UpdateTable, DescribeTable · fields: ProvisionedThroughput, Replicas.ProvisionedThroughputOverride,
    BillingMode
  - repro: PROVISIONED table + replica: UpdateTable ProvisionedThroughput in each region; DescribeTable both
    regions
  - measurements: wcu_propagation_total_s=39.1
  - handling: handled via `pkg/resource/table/hooks_replica_updates.go:39-45; pkg/resource/table/hooks_replica_updates.go:166-171`
  - related: [DDB-TABLE-048](table-global-tables.md#ddb-table-048), [DDB-TABLE-320](table-global-tables.md#ddb-table-320), [DDB-TABLE-231](table-global-tables.md#ddb-table-231), [DDB-TABLE-201](table-global-tables.md#ddb-table-201), [DDB-TABLE-229](table-global-tables.md#ddb-table-229), [DDB-TABLE-250](#ddb-table-250),
    [DDB-TABLE-252](#ddb-table-252), [DDB-TABLE-321](#ddb-table-321), [DDB-TABLE-294](#ddb-table-294), [DDB-TABLE-223](#ddb-table-223), [DDB-TABLE-307](#ddb-table-307), [DDB-TABLE-296](#ddb-table-296), [DDB-TABLE-265](#ddb-table-265),
    [DDB-TABLE-295](#ddb-table-295), [DDB-TABLE-297](#ddb-table-297), [DDB-TABLE-322](table-policy-kinesis-autoscaling.md#ddb-table-322), [DDB-TABLE-314](table-policy-kinesis-autoscaling.md#ddb-table-314) · hypotheses: H-R-018 · evidence:
    table/cross-region/provisioned-replica
  - notes: Refutes the 'read-side is regional' half of H-R-018: with the default settings-replication mode a
    regional RCU change does NOT materialise as a ProvisionedThroughputOverride in the other region, it is
    replicated into the other region's own ProvisionedThroughput. A controller comparing its spec...
  - full notes: [details/DDB-TABLE-251.md](details/DDB-TABLE-251.md)

- <a id="ddb-table-295"></a>**DDB-TABLE-295** `cross-region` · impact medium · unhandled (not handled in controller) · verified 2026-10-09
  **Per-replica KMSMasterKeyId rejected on an AWS-owned-key table; SSESpecification KMS on the base fans out to the replica (regional key)**
  Base with default (AWS-owned) encryption and an ACTIVE replica: ReplicaUpdates=[Update{us-east-1,
  KMSMasterKeyId:'alias/aws/dynamodb'}] -> ValidationException 'One or more parameter values were invalid:
  KMSMasterKeyId must be specified for each replica.' UpdateTable SSESpecification{Enabled:true, SSEType:KMS}
  on the base -> 200; TableStatus stayed ACTIVE while SSEDescription.Status=UPDATING in BOTH regions;
  afterwards A.Replicas[us-east-1].KMSMasterKeyId = arn:aws:kms:us-east-1:...:key/<aws-managed key of
  us-east-1> and B.Replicas[us-west-2].KMSMasterKeyId = the us-west-2 AWS managed key ARN. Encryption type is
  group-wide; the key itself is regional and reported as a full ARN per replica.
  - ACK: custom_update, compare.is_ignored+delta_pre_compare · ops: UpdateTable, DescribeTable · fields:
    ReplicaUpdates.Update.KMSMasterKeyId, Replicas.KMSMasterKeyId, SSESpecification, SSEDescription
  - repro: UpdateTable ReplicaUpdates=[{Update:{RegionName:us-east-1, KMSMasterKeyId:alias/aws/dynamodb}}];
    UpdateTable SSESpecification{Enabled:true,SSEType:KMS}
  - handling: not handled in the controller (as of commit 34b85e6)
  - related: [DDB-TABLE-296](#ddb-table-296), [DDB-TABLE-251](#ddb-table-251), [DDB-TABLE-265](#ddb-table-265), [DDB-TABLE-294](#ddb-table-294), [DDB-TABLE-297](#ddb-table-297), [DDB-TABLE-322](table-policy-kinesis-autoscaling.md#ddb-table-322),
    [DDB-TABLE-313](#ddb-table-313), [DDB-TABLE-309](#ddb-table-309), [DDB-TABLE-328](table-streams-encryption-class.md#ddb-table-328), [DDB-TABLE-312](#ddb-table-312), [DDB-TABLE-310](#ddb-table-310), [DDB-TABLE-311](#ddb-table-311), [DDB-TABLE-329](#ddb-table-329),
    [DDB-TABLE-327](#ddb-table-327) · hypotheses: H-R-016 · evidence: table/mutation-matrix/replica-overrides
  - notes: Partially covers H-R-016: SSE type changes on the base fan out to replicas; the per-replica key is
    readable as an ARN only (read-gap for alias inputs). CMK-per-replica rules are in
    table/state-machine/replica-kms-lifecycle and replica-creation-failed. Note the misleading message text
    ('must be...
  - full notes: [details/DDB-TABLE-295.md](details/DDB-TABLE-295.md)

- <a id="ddb-table-296"></a>**DDB-TABLE-296** `cross-region` · impact high · unhandled (not handled in controller) · verified 2026-10-09
  **GSI add/delete with a replica fans out to every region; a GSI add issued at the replica endpoint is accepted and propagates back**
  UpdateTable GlobalSecondaryIndexUpdates=[Create gsi1] on the base (1-item table) -> 200 (response: GSIs=[],
  Replicas unchanged). Timeline: both regions TableStatus UPDATING for ~31s; gsi1 appears CREATING in
  us-east-1 at 31s and in A.Replicas[].GlobalSecondaryIndexes (IndexName + WarmThroughput only) at 58s; base
  back to ACTIVE at ~89s while both indexes kept backfilling; us-east-1's gsi1 ACTIVE at ~545s, base's at
  575s. During the GSI create: ReplicaUpdates Update/Create -> ResourceInUseException. UpdateTable Create gsi2
  issued at the us-east-1 endpoint -> 200 and gsi2 appeared in the base (542s total). UpdateTable Delete gsi1
  on the base -> 200, removed from both regions in ~38s; AttributeDefinitions converge in both regions.
  - ACK: custom_update, synced.when, compare.is_ignored+delta_pre_compare · ops: UpdateTable, DescribeTable ·
    fields: GlobalSecondaryIndexUpdates, GlobalSecondaryIndexes, Replicas.GlobalSecondaryIndexes
  - repro: table with replica -> UpdateTable GlobalSecondaryIndexUpdates Create -> poll both regions
  - measurements: gsi1_total_s=575.4, gsi2_from_replica_total_s=541.5, gsi_delete_total_s=38.0
  - handling: not handled in the controller (as of commit 34b85e6)
  - related: [DDB-TABLE-306](table-policy-kinesis-autoscaling.md#ddb-table-306), [DDB-TABLE-200](#ddb-table-200), [DDB-TABLE-263](table-global-tables.md#ddb-table-263), [DDB-TABLE-315](table-policy-kinesis-autoscaling.md#ddb-table-315), [DDB-TABLE-327](#ddb-table-327), [DDB-TABLE-314](table-policy-kinesis-autoscaling.md#ddb-table-314),
    [DDB-TABLE-251](#ddb-table-251), [DDB-TABLE-265](#ddb-table-265), [DDB-TABLE-294](#ddb-table-294), [DDB-TABLE-295](#ddb-table-295), [DDB-TABLE-297](#ddb-table-297), [DDB-TABLE-322](table-policy-kinesis-autoscaling.md#ddb-table-322), [DDB-TABLE-204](#ddb-table-204),
    [DDB-TABLE-261](table-global-tables.md#ddb-table-261), [DDB-TABLE-250](#ddb-table-250), [DDB-TABLE-226](#ddb-table-226), [DDB-TABLE-309](#ddb-table-309), [DDB-TABLE-221](#ddb-table-221) · hypotheses: H-R-017, H-R-006 ·
    evidence: table/mutation-matrix/replica-overrides
  - notes: Confirms H-R-017: schema is group-wide and symmetric (no primary region). The base returns to
    ACTIVE while replica indexes are still CREATING, so readiness must consider every region's IndexStatus
    (A.Replicas[].GlobalSecondaryIndexes carries no IndexStatus). GSI backfill on a 1-item table with a...
  - full notes: [details/DDB-TABLE-296.md](details/DDB-TABLE-296.md)

- <a id="ddb-table-321"></a>**DDB-TABLE-321** `cross-region` · impact medium · SUSPECTED CONTROLLER BUG · verified 2026-10-09
  **AAS read scaling of the source region pins the replica's read capacity via Replicas[].ProvisionedThroughputOverride**
  After Application Auto Scaling raised the source table's ReadCapacityUnits (1->2 in us-west-2; 5->10 in
  table/mutation-matrix/autoscaling-vs-throughput), DescribeTable showed the us-east-1 replica with
  ProvisionedThroughputOverride {ReadCapacityUnits: <old value>} (1 resp. 5) that the caller never set; before
  the scaling activity the replica had no override. Write capacity is table-wide and needs no override.
  - ACK: compare.is_ignored+delta_pre_compare, docs-only · ops: DescribeTable · fields:
    Replicas[].ProvisionedThroughputOverride.ReadCapacityUnits
  - repro: 2-replica PROVISIONED table -> register AAS read target with MinCapacity above current RCU in
    region A -> DescribeTable in A
  - handling: suspected controller bug - see Handling gaps
  - related: [DDB-TABLE-314](table-policy-kinesis-autoscaling.md#ddb-table-314), [DDB-TABLE-195](table-policy-kinesis-autoscaling.md#ddb-table-195), [DDB-TABLE-315](table-policy-kinesis-autoscaling.md#ddb-table-315), [DDB-TABLE-256](#ddb-table-256), [DDB-TABLE-250](#ddb-table-250), [DDB-TABLE-252](#ddb-table-252),
    [DDB-TABLE-251](#ddb-table-251), [DDB-TABLE-294](#ddb-table-294), [DDB-TABLE-229](table-global-tables.md#ddb-table-229), [DDB-TABLE-223](#ddb-table-223), [DDB-TABLE-307](#ddb-table-307) · hypotheses: H-S-039, H-R-033 ·
    evidence: table/sub-resources/replica-autoscaling-facade, table/mutation-matrix/autoscaling-vs-throughput
  - notes: A spec that omits replica overrides would see server-materialized overrides after any read scaling
    event in another region.
  - full notes: [details/DDB-TABLE-321.md](details/DDB-TABLE-321.md)

- <a id="ddb-table-327"></a>**DDB-TABLE-327** `cross-region` · impact high · unhandled (not handled in controller) · verified 2026-10-09
  **INACCESSIBLE replica: DP update and direct DeleteTable in the replica region are accepted; ReplicaUpdates.Delete refused while DP on**
  Global table (2019.11.21) us-west-2 -> us-east-1 replica encrypted with the MRK replica key; that key
  disabled; source TableStatus=ACTIVE, Replicas[].ReplicaStatus=INACCESSIBLE_ENCRYPTION_CREDENTIALS
  (Replicas[].ReplicaInaccessibleDateTime never appeared on the source side; the replica region's own
  DescribeTable shows TableStatus=INACCESSIBLE_ENCRYPTION_CREDENTIALS but no InaccessibleEncryptionDateTime
  either). In the replica region: ListTagsOfResource OK, TagResource OK, GetItem -> ValidationException 'KMS
  key disabled error: ...DisabledException', UpdateTable(DeletionProtectionEnabled=true) -> OK
  (TableStatus=UPDATING ~20 s, source UPDATING too). With DP on (attempt 1): UpdateTable ReplicaUpdates.Delete
  from the source AND a direct DeleteTable in the replica region -> ValidationException 'Cannot delete table
  <name> in region us-east-1 because it has deletion protection enabled. Disable deletion protection first.'
  After DP off on both sides (each -> OK/UPDATING, ~35 s): direct DeleteTable in the replica region -> OK,
  response TableStatus=DELETING; the source then shows ReplicaStatus=DELETING and a ReplicaUpdates.Delete
  issued 0.2 s later -> ResourceInUseException 'The resource which you are attempting to change is in use.'
  (the parent probe's ReplicaUpdates.Delete, issued 20 s after a source DP update, got the same
  ResourceInUseException). The removal itself is timed by table/cross-region/multi-region-kms-replica.
  - ACK: deletable.when, custom_update, requeue, synced.when · ops: UpdateTable, DeleteTable, DescribeTable,
    TagResource, GetItem · fields: ReplicaUpdates.Delete, Replicas.ReplicaStatus,
    Replicas.ReplicaInaccessibleDateTime, TableStatus
  - repro: Global table with a replica on a CMK; kms DisableKey in the replica region; wait for
    ReplicaStatus=INACCESSIBLE_ENCRYPTION_CREDENTIALS; UpdateTable ReplicaUpdates.Delete
  - handling: not handled in the controller (as of commit 34b85e6)
  - related: [DDB-TABLE-306](table-policy-kinesis-autoscaling.md#ddb-table-306), [DDB-TABLE-200](#ddb-table-200), [DDB-TABLE-296](#ddb-table-296), [DDB-TABLE-263](table-global-tables.md#ddb-table-263), [DDB-TABLE-315](table-policy-kinesis-autoscaling.md#ddb-table-315), [DDB-TABLE-314](table-policy-kinesis-autoscaling.md#ddb-table-314),
    [DDB-TABLE-297](#ddb-table-297), [DDB-TABLE-329](#ddb-table-329), [DDB-TABLE-311](#ddb-table-311), [DDB-TABLE-313](#ddb-table-313), [DDB-TABLE-309](#ddb-table-309), [DDB-TABLE-295](#ddb-table-295), [DDB-TABLE-328](table-streams-encryption-class.md#ddb-table-328),
    [DDB-TABLE-312](#ddb-table-312), [DDB-TABLE-310](#ddb-table-310) · hypotheses: H-T-108, H-T-104 · evidence:
    table/dependencies/replica-delete-while-inaccessible
  - notes: H-T-108 (INACCESSIBLE stage only; ARCHIVED untested): a broken replica can be dropped with a plain
    DeleteTable in its own region (2019.11.21 global tables allow that) even while INACCESSIBLE;
    ReplicaUpdates.Delete was not observed succeeding in this state because every attempt collided with an...
  - full notes: [details/DDB-TABLE-327.md](details/DDB-TABLE-327.md)

- <a id="ddb-table-329"></a>**DDB-TABLE-329** `cross-region` · impact high · unhandled (not handled in controller) · verified 2026-10-09
  **Replica CMK disabled: ReplicaStatus INACCESSIBLE_ENCRYPTION_CREDENTIALS after ~80 min; source stays ACTIVE/writable; no InaccessibleDateTime**
  Empty 2019.11.21 global table, replica created with ReplicaUpdates.Create{RegionName:us-east-1,
  KMSMasterKeyId:<bare mrk id>}: source TableStatus UPDATING 0-56 s, replica CREATING from 56 s, source back
  to ACTIVE at 67 s while the replica was still CREATING, UPDATING again 311-617 s, replica ACTIVE at 617 s
  (~10 min). kms DisableKey on the us-east-1 replica key only, 60 s polling of both regions: nothing changed
  for 79 min; at +4783 s the SAME poll showed source
  Replicas[].ReplicaStatus=INACCESSIBLE_ENCRYPTION_CREDENTIALS and replica-region
  TableStatus=INACCESSIBLE_ENCRYPTION_CREDENTIALS; source TableStatus stayed ACTIVE throughout;
  Replicas[].ReplicaInaccessibleDateTime was never populated, nor was
  SSEDescription.InaccessibleEncryptionDateTime in the replica region's DescribeTable (unlike a single-region
  table, which gets InaccessibleEncryptionDateTime). In that state: source GetItem OK, source PutItem OK,
  replica-region GetItem -> ValidationException 'KMS key disabled error: ...DisabledException ... is
  disabled', replica-region ListTagsOfResource OK, source UpdateTable(DeletionProtectionEnabled=true) -> OK
  (global-table DP change: source UPDATING ~60 s and the flag propagated to the replica), and during that
  minute every other write (source DP=false, replica-region UpdateTable DP, replica-region DeleteTable, source
  ReplicaUpdates.Delete) -> ResourceInUseException 'The resource which you are attempting to change is in
  use.'. The replica was finally removed with a direct DeleteTable in us-east-1 (see
  table/dependencies/replica-delete-while-inaccessible): ReplicaStatus=DELETING, gone from Replicas[] and
  ResourceNotFoundException in us-east-1 within 3 min. EnableKey-based recovery of the replica was not
  measured.
  - ACK: synced.when, requeue, custom_update, terminal_codes · ops: DescribeTable, UpdateTable, DeleteTable,
    GetItem, PutItem · fields: Replicas.ReplicaStatus, Replicas.ReplicaInaccessibleDateTime,
    ReplicaUpdates.Delete, TableStatus
  - repro: Global table (2019.11.21) with a replica encrypted by the MRK replica key; kms DisableKey in the
    replica region; poll both regions every 60 s; then ReplicaUpdates.Delete
  - measurements: replica_create_s=617.2, replica_inaccessible_detect_s=4783.2,
    replica_direct_delete_removal_s_upper=180
  - handling: not handled in the controller (as of commit 34b85e6)
  - related: [DDB-TABLE-226](#ddb-table-226), [DDB-TABLE-306](table-policy-kinesis-autoscaling.md#ddb-table-306), [DDB-TABLE-265](#ddb-table-265), [DDB-TABLE-308](#ddb-table-308), [DDB-TABLE-309](#ddb-table-309), [DDB-TABLE-187](#ddb-table-187),
    [DDB-TABLE-305](#ddb-table-305), [DDB-TABLE-249](#ddb-table-249), [DDB-TABLE-258](table-global-tables.md#ddb-table-258), [DDB-TABLE-297](#ddb-table-297), [DDB-TABLE-327](#ddb-table-327), [DDB-TABLE-263](table-global-tables.md#ddb-table-263), [DDB-TABLE-311](#ddb-table-311),
    [DDB-TABLE-313](#ddb-table-313), [DDB-TABLE-295](#ddb-table-295), [DDB-TABLE-328](table-streams-encryption-class.md#ddb-table-328), [DDB-TABLE-312](#ddb-table-312), [DDB-TABLE-310](#ddb-table-310) · hypotheses: H-T-108 ·
    evidence: table/cross-region/multi-region-kms-replica
  - notes: H-T-108 first stage only (INACCESSIBLE replica; the ARCHIVING/ARCHIVED tail is untested). Detection
    took ~80 min here vs 12-75 min for single-region tables in table/state-machine/kms-inaccessible-lifecycle:
    the status lags the KMS state by a long, variable interval. The source table keeps working...
  - full notes: [details/DDB-TABLE-329.md](details/DDB-TABLE-329.md)

## Scope

- <a id="ddb-tablereplicaautoscaling-001"></a>**DDB-TABLEREPLICAAUTOSCALING-001** `scope` · impact high · unhandled (not handled in controller) · verified 2026-10-09
  **Scope verdict: TableReplicaAutoScaling is a facade over Application Auto Scaling - skip it as an ACK resource**
  **Scope verdict: skip:no-crud**
  Identity test over three probes: (1) settings created via UpdateTableReplicaAutoScaling appear in
  application-autoscaling as a scalable target + target-tracking policy + 4 CloudWatch alarms per dimension;
  (2) objects created directly via RegisterScalableTarget/PutScalingPolicy (custom PolicyName, cooldowns,
  DisableScaleIn) appear verbatim in DescribeTableReplicaAutoScaling within 0.5 s; (3) DeleteTable/replica
  removal leave the AAS objects orphaned and intact. The DynamoDB API adds no state of its own: no
  Create/Delete (Update+Describe only), every non-disable Update must carry the full {MinimumUnits,
  MaximumUnits, ScalingPolicyUpdate}, AutoScalingRoleArn is forced to the service-linked role, PolicyName
  rename replaces, and the API is only callable on 2019.11.21 global tables (regional tables ->
  ResourceNotFoundException). It also hides state (a target without a policy reads as AutoScalingDisabled=true
  while AAS enforces its MinCapacity) and surfaces no scaling failures (quota breach only in
  describe-scaling-activities).
  - ACK: scope:skip, ignore.resource · ops: UpdateTableReplicaAutoScaling, DescribeTableReplicaAutoScaling,
    RegisterScalableTarget, PutScalingPolicy
  - repro: see the six evidence probes
  - handling: not handled in the controller (as of commit 34b85e6)
  - related: [DDB-TABLE-193](table-policy-kinesis-autoscaling.md#ddb-table-193), [DDB-TABLE-197](table-policy-kinesis-autoscaling.md#ddb-table-197), [DDB-TABLE-196](table-policy-kinesis-autoscaling.md#ddb-table-196), [DDB-TABLE-189](table-policy-kinesis-autoscaling.md#ddb-table-189), [DDB-TABLE-195](table-policy-kinesis-autoscaling.md#ddb-table-195), [DDB-TABLE-292](#ddb-table-292) ·
    hypotheses: H-R-131, H-R-132, H-R-133, H-R-104, H-R-050 · evidence:
    table/sub-resources/replica-autoscaling-facade, table/round-trip/autoscaling-settings,
    table/error-taxonomy/autoscaling-validation, table/mutation-matrix/autoscaling-vs-throughput,
    table/dependencies/autoscaling-gsi-and-orphans, table/cross-region/replica-autoscaling-facades
  - notes: Confirms H-R-131 verdict (a). The ACK applicationautoscaling-controller (ScalableTarget
    serviceNamespace=dynamodb resourceID=table/<name>[/index/<gsi>], ScalingPolicy) already owns 100% of this
    state. What the DynamoDB Table controller DOES need (H-R-132): a read-side guard - treat...
  - full notes: [details/DDB-TABLEREPLICAAUTOSCALING-001.md](details/DDB-TABLEREPLICAAUTOSCALING-001.md)

## Handling gaps (bugs to file)

- [DDB-TABLE-188](#ddb-table-188) - Adding a replica to a PROVISIONED table requires write autoscaling first: 'Table write
  capacity should either be Pay-Per-Request or AutoS... (suspected bug)
  Suspected controller bug confirmed by evidence: Suspicion confirmed: the prerequisite is server-enforced
  (188/249), cannot be met through the DynamoDB API (UpdateTableReplicaAutoScaling -> RNF on a regional table,
  187/188; a bare AAS target without a policy is insufficient, 188) and also covers every GSI write dimension
  (288/291). The condition is recoverable without a spec change (register AAS target+policy, then retry; first
  retry may 500, 249), so classifying the ValidationException as terminal wedges the CR. Follow-on: once
  autoscaled, AAS rewrites provisioned throughput out-of-band (314/315), so spec.provisionedThroughput on a
  PROVISIONED global table can never be stable.
  - handling_ref: `https://github.com/aws-controllers-k8s/community/issues/2610; 5bbfe82`
- [DDB-TABLE-309](#ddb-table-309) - Per-replica CMK: bare key id accepted and read back as the full key ARN;
  ReplicaUpdates.Update KMSMasterKeyId is always rejected (suspected bug)
  Suspected controller bug confirmed by evidence: Split verdict, net confirmed. TableClass half REFUTED:
  ReplicaTableClassSummary is absent for default STANDARD and appears only once a class is set explicitly
  (294/201), so a spec without tableClassOverride is stable (nil vs STANDARD must still compare equal). KMS
  half CONFIRMED: any bare key id/alias is read back as the full ARN (309), so a literal compare yields a
  perpetual delta - and the resulting ReplicaUpdates.Update{KMSMasterKeyId} is rejected unconditionally
  (309/295), i.e. terminal rather than merely unsynced. Further perpetual-delta sources: AAS materializes
  Replicas[].ProvisionedThroughputOverride nobody requested (321) and an override equal to the base value
  disappears from DescribeTable (252/250).
  - handling_ref: `pkg/resource/table/hooks_replica_updates.go:423-463;
    pkg/resource/table/hooks_replica_updates.go:28-147`
- [DDB-TABLE-321](#ddb-table-321) - AAS read scaling of the source region pins the replica's read capacity via
  Replicas[].ProvisionedThroughputOverride (suspected bug)
  Suspected controller bug confirmed by evidence: The GT's example is inverted but the loop is real:
  DescribeTable echoes EVERY table GSI under Replicas[].GlobalSecondaryIndexes with IndexName
  (+WarmThroughput) and no override (296/252), and AAS read scaling injects a ProvisionedThroughputOverride
  the spec never declared (321); both are observed-vs-desired differences that updateReplicaUpdate() cannot
  express as a valid action (RegionName-only Update is rejected: 294), so the hook returns an empty update and
  requeues with requeueWaitReplicasActive forever. Also: a same-value override Update is accepted and costs
  ~34s UPDATING (294), so even an expressible 'fix' churns.
  - handling_ref: `pkg/resource/table/hooks_replica_updates.go:423-463;
    pkg/resource/table/hooks_replica_updates.go:28-147; pkg/resource/table/hooks_replica_updates.go:39-45;
    pkg/resource/table/hooks_replica_updates.go:166-171`

## E2E timing

Values are seconds unless the key says otherwise; n = trials behind the numbers ('1 run' when the finding records none).

| finding | what | measurements | n |
| --- | --- | --- | --- |
| [DDB-TABLE-204](#ddb-table-204) | ReplicaUpdates.Delete deletes the regional table; source stays UPDATING ~36 s then replica DELETING ~88 s; response echoes ACTIVE | replica_delete_total_s=124.67, source_updating_s=36.3, replica_deleting_s=88.2 | 1 run |
| [DDB-TABLE-222](#ddb-table-222) | StreamSpecification is immutable while the table has replicas (disable / view-type change rejected in both regions) | replica_delete_until_entry_gone_s=244.41 | 1 run |
| [DDB-TABLE-226](#ddb-table-226) | Replica Create on an EMPTY table completes in ~20-30s; the UpdateTable response shows Replicas=[] (entry appears ~6s later) | create_total_s=28.0, update_table_latency_ms=2046, replicas_entry_appears_s=6.7, replica_region_visible_s=10.1, readd_total_s=23.0 | 1 run |
| [DDB-TABLE-228](#ddb-table-228) | Admissibility while a replica is DELETING, and A-entry-gone vs B-ResourceNotFound ordering | delete_total_s=242.3, a_entry_gone_s=207.8, b_notfound_s=242.3, readd_delete_total_s=96.8 | 1 run |
| [DDB-TABLE-230](#ddb-table-230) | Which table settings replicate to the replica region (tags, TTL, PITR, resource policy, insights, deletion protection, table class) | dp_toggle_updating_s_with_replica=39.3, propagation_watch_s=240 | 1 run |
| [DDB-TABLE-238](#ddb-table-238) | UpdateTableReplicaAutoScaling response echoes PRE-update settings; Describe reflects the change after 0.43s | converge_minimal_s=0.43, converge_custom_s=0.63, converge_disable_s=0.6 | 1 run |
| [DDB-TABLE-251](#ddb-table-251) | Manual ProvisionedThroughput changes in any region replicate group-wide (no override materialized); per-region RCU only via overrides | wcu_propagation_total_s=39.1 | 1 run |
| [DDB-TABLE-265](#ddb-table-265) | One Create/Delete replica action per UpdateTable call; a Create may be issued at any member's endpoint; Replicas[] order differs per region | us_east_1_create_total_s_1_item=660.0, eu_west_1_create_total_s_1_item=580.0 | 1 run |
| [DDB-TABLE-267](#ddb-table-267) | Source-region rule: a replica that was the endpoint/source of another replica added <24h ago cannot be removed until that replica is gone | eu_west_1_delete_total_s=121.0 | 1 run |
| [DDB-TABLE-296](#ddb-table-296) | GSI add/delete with a replica fans out to every region; a GSI add issued at the replica endpoint is accepted and propagates back | gsi1_total_s=575.4, gsi2_from_replica_total_s=541.5, gsi_delete_total_s=38.0 | 1 run |
| [DDB-TABLE-297](#ddb-table-297) | Deleting a replica: DeletionProtection on the replica blocks ReplicaUpdates.Delete; DeleteTable in the replica region removes the replica | replica_direct_delete_total_s=190.4, b_dp_toggle_base_updating_s=34.1 | 1 run |
| [DDB-TABLE-298](#ddb-table-298) | A CREATING replica is visible to DescribeTable only (other regional calls -> NotFound); DeleteTable on it -> ResourceInUseException | replica_visible_after_s=67.9, one_item_create_total_s=562.2 | 1 run |
| [DDB-TABLE-305](#ddb-table-305) | No stream view-type prerequisite: replicas are created on KEYS_ONLY/NEW_IMAGE/OLD_IMAGE streams unchanged; a disabled stream is re-enabled | replica_active_s_keys_only=14.4, replica_active_s_new_image=14.6, replica_active_s_old_image=17.8, replica_active_s_disabled_stream=33.6 | 1 run |
| [DDB-TABLE-308](#ddb-table-308) | Re-adding a replica after its Replicas[] entry vanishes is rejected until the replica-region table is gone (~20s gap) | a_entry_gone_s=184.0, b_notfound_gap_s=19.4, recreate_total_s=16.5 | 1 run |
| [DDB-TABLE-309](#ddb-table-309) | Per-replica CMK: bare key id accepted and read back as the full key ARN; ReplicaUpdates.Update KMSMasterKeyId is always rejected | create_total_s=40.8 | 1 run |
| [DDB-TABLE-310](#ddb-table-310) | Replica Create aborted asynchronously (KMS key disabled seconds after Create) leaves no trace: no Replicas entry, no CREATION_FAILED | abort_total_s=11.8 | 1 run |
| [DDB-TABLE-311](#ddb-table-311) | Replica CMK disabled: INACCESSIBLE status lags (none within 25 min; ~80 min in 329); control plane works, replica-region data plane fails | inaccessible_watch_s=1500, inaccessible_detect_s=null | 1 run |
| [DDB-TABLE-329](#ddb-table-329) | Replica CMK disabled: ReplicaStatus INACCESSIBLE_ENCRYPTION_CREDENTIALS after ~80 min; source stays ACTIVE/writable; no InaccessibleDateTime | replica_create_s=617.2, replica_inaccessible_detect_s=4783.2, replica_direct_delete_removal_s_upper=180 | 1 run |
| [DDB-TABLE-422](#ddb-table-422) | Doc claim C055 TRUE: OnDemandThroughputOverride.MaxReadRequestUnits is the replica table's maximum read request units | replica_create_to_active_s=161.5, source_updating_before_replica_entry_s=148.7, override_update_updating_s=35.0, override_clear_updating_s=35.1, replica_delete_total_s=184.3 | 1 run |

## Open questions

- [DDB-TABLE-426](#ddb-table-426) (unverified) - Doc claim C059 UNTESTABLE: A replica whose Region stays inaccessible > 20 h is
  removed from the replication group: VERDICT: UNTESTABLE - a Region outage cannot be induced from the lab; no
  existing finding covers it
- [DDB-TABLE-427](#ddb-table-427) (unverified) - Doc claim C060 UNTESTABLE: A replica whose KMS key stays inaccessible > 20 h is
  removed from the replication group: VERDICT: UNTESTABLE - needs a > 20 h key-inaccessible soak; the longest
  lab observation (~2 h) only reached INACCESSIBLE_ENCRYPTION_CREDENTIALS, no removal

<!-- preserved:start id=open-questions -->
<!-- open questions and follow-up experiments; survives re-renders -->
<!-- preserved:end -->

## Appendix: low-impact and duplicate findings

| id | category | impact | status | title | related | duplicate_of |
| --- | --- | --- | --- | --- | --- | --- |
| <a id="ddb-table-192"></a>**DDB-TABLE-192** | stale-response | medium | confirmed | UpdateTableReplicaAutoScaling response is stale: read settings still {AutoScalingDisabled:true} while Describe shows the new policy | [DDB-TABLE-238](#ddb-table-238), [DDB-TABLE-198](table-policy-kinesis-autoscaling.md#ddb-table-198), [DDB-TABLE-242](table-policy-kinesis-autoscaling.md#ddb-table-242), [DDB-TABLE-314](table-policy-kinesis-autoscaling.md#ddb-table-314) | [DDB-TABLE-238](#ddb-table-238) |
| <a id="ddb-table-199"></a>**DDB-TABLE-199** | update-granularity | high | confirmed | UpdateTable rejects ReplicaUpdates combined with any other mutation ('Replica modification must be the only operation') | [DDB-TABLE-433](table-streams-encryption-class.md#ddb-table-433), [DDB-TABLE-156](table-throughput-billing.md#ddb-table-156), [DDB-TABLE-358](table-throughput-billing.md#ddb-table-358), [DDB-TABLE-056](table-throughput-billing.md#ddb-table-056), [DDB-TABLE-174](table-indexes.md#ddb-table-174), [DDB-TABLE-224](#ddb-table-224), [DDB-TABLE-162](table-indexes.md#ddb-table-162), [DDB-TABLE-127](table-indexes.md#ddb-table-127) | [DDB-TABLE-224](#ddb-table-224) |
| <a id="ddb-table-200"></a>**DDB-TABLE-200** | update-granularity | high | confirmed | EVENTUAL ReplicaUpdates is strictly one action per call; a second Create while the first is CREATING -> ResourceInUseException | [DDB-TABLE-306](table-policy-kinesis-autoscaling.md#ddb-table-306), [DDB-TABLE-296](#ddb-table-296), [DDB-TABLE-263](table-global-tables.md#ddb-table-263), [DDB-TABLE-315](table-policy-kinesis-autoscaling.md#ddb-table-315), [DDB-TABLE-327](#ddb-table-327), [DDB-TABLE-314](table-policy-kinesis-autoscaling.md#ddb-table-314) | [DDB-TABLE-265](#ddb-table-265) |
| <a id="ddb-table-249"></a>**DDB-TABLE-249** | prerequisite | high | confirmed | PROVISIONED table needs write autoscaling before a replica can be added; DynamoDB mirrors the AAS targets into the replica region | [DDB-TABLE-188](#ddb-table-188), [DDB-TABLE-288](#ddb-table-288), [DDB-TABLE-291](table-policy-kinesis-autoscaling.md#ddb-table-291), [DDB-TABLE-322](table-policy-kinesis-autoscaling.md#ddb-table-322), [DDB-TABLE-290](table-policy-kinesis-autoscaling.md#ddb-table-290), [DDB-TABLE-194](#ddb-table-194), [DDB-TABLE-226](#ddb-table-226), [DDB-TABLE-306](table-policy-kinesis-autoscaling.md#ddb-table-306), [DDB-TABLE-265](#ddb-table-265), [DDB-TABLE-308](#ddb-table-308), [DDB-TABLE-309](#ddb-table-309), [DDB-TABLE-329](#ddb-table-329), [DDB-TABLE-187](#ddb-table-187), [DDB-TABLE-305](#ddb-table-305), [DDB-TABLE-258](table-global-tables.md#ddb-table-258) | [DDB-TABLE-188](#ddb-table-188) |
| <a id="ddb-table-262"></a>**DDB-TABLE-262** | delete-semantics | high | confirmed | After adding a replica, DeleteTable on the source fails ('acted as a source region ... last 24 hours') until that replica is removed | [DDB-TABLE-266](#ddb-table-266), [DDB-TABLE-267](#ddb-table-267), [DDB-TABLE-265](#ddb-table-265), [DDB-TABLE-297](#ddb-table-297), [DDB-TABLE-260](table-global-tables.md#ddb-table-260) | [DDB-TABLE-266](#ddb-table-266) |
| <a id="ddb-table-293"></a>**DDB-TABLE-293** | delete-semantics | medium | confirmed | Removing a replica leaves the replica region's AAS write target, policy and alarms orphaned | [DDB-TABLE-204](#ddb-table-204), [DDB-TABLE-308](#ddb-table-308), [DDB-TABLE-222](#ddb-table-222), [DDB-TABLE-297](#ddb-table-297), [DDB-TABLE-267](#ddb-table-267), [DDB-TABLE-261](table-global-tables.md#ddb-table-261), [DDB-TABLE-231](table-global-tables.md#ddb-table-231) | [DDB-TABLE-292](#ddb-table-292) |
| <a id="ddb-table-422"></a>**DDB-TABLE-422** | other | low | confirmed | Doc claim C055 TRUE: OnDemandThroughputOverride.MaxReadRequestUnits is the replica table's maximum read request units | [DDB-TABLE-250](#ddb-table-250) | - |
| <a id="ddb-table-426"></a>**DDB-TABLE-426** | other | low | unverified | Doc claim C059 UNTESTABLE: A replica whose Region stays inaccessible > 20 h is removed from the replication group | - | - |
| <a id="ddb-table-427"></a>**DDB-TABLE-427** | other | low | unverified | Doc claim C060 UNTESTABLE: A replica whose KMS key stays inaccessible > 20 h is removed from the replication group | [DDB-TABLE-329](#ddb-table-329), [DDB-TABLE-331](table-streams-encryption-class.md#ddb-table-331), [DDB-TABLE-327](#ddb-table-327) | - |

## Supplementary notes

<!-- preserved:start -->
<!-- preserved:end -->
