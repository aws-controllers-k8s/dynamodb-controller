<!-- generated: ack-api-quirks dynamodb 2026-10-09 model=2012-08-10 controller=34b85e6 -->
# Global tables: multi-region consistency, witnesses, legacy 2017.11.29 API
_MRSC/STRONG groups and witnesses, the legacy CreateGlobalTable/UpdateGlobalTable/GlobalTableSettings APIs and their scope verdicts._
Generated from ack-api-quirks `services/dynamodb` (render date in the marker above); model 2012-08-10 (service/dynamodb v1.39.8); controller commit 34b85e6; evidence: `services/dynamodb/probes/<probe id>/` in the lab repo.

## Overview

<!-- preserved:start id=overview -->
This document covers both generations of "global table": the 2019.11.21 replication group as DescribeTable exposes it (GlobalTableVersion, Replicas[], GlobalTableSettingsReplicationMode), its MRSC/STRONG variant with witnesses, and the legacy 2017.11.29 GlobalTable/GlobalTableSettings APIs. The most surprising facts are that CreateGlobalTable is rejected in every region in 2026 so neither legacy noun can have an instance, that a STRONG group must be born complete in ONE UpdateTable and is frozen afterwards, and that EVENTUAL groups take exactly one replica action per call; the replica lifecycle itself (create/delete timings, admissibility, error taxonomy) lives in table-replicas.md ([DDB-GLOBALTABLE-002](#ddb-globaltable-002), [DDB-TABLE-257](#ddb-table-257); [DDB-TABLE-265](table-replicas.md#ddb-table-265), table-replicas.md).

### Rules a reconciler must respect
- Legacy APIs are dead: CreateGlobalTable (2017.11.29) is ValidationException 'global tables version 2017.11.29 is not supported' from every endpoint (shape errors fire first, TableNotFound is never reached); Describe/UpdateGlobalTable and Describe/UpdateGlobalTableSettings return GlobalTableNotFoundException for regional tables, missing names AND 2019.11.21 groups; ListGlobalTables returns [] and a bogus RegionName is HTTP 500 - nothing can be created, listed or adopted ([DDB-GLOBALTABLE-002](#ddb-globaltable-002), [DDB-GLOBALTABLE-003](#ddb-globaltable-003), [DDB-GLOBALTABLE-001](#ddb-globaltable-001), [DDB-GLOBALTABLE-004](#ddb-globaltable-004), [DDB-GLOBALTABLESETTINGS-001](#ddb-globaltablesettings-001)).
- Scope verdicts: GlobalTable and GlobalTableSettings are skip (no reachable instance; the legacy autoscaling fields are untestable) - the controller's hooks catalog keeps the generated GlobalTable CRD with its e2e suite skipped for exactly this error ([GT-DDB-079](service.md#gt-ddb-079) (controller hooks catalog entry)) and maps GlobalTableNotFoundException to 404 ([GT-DDB-083](service.md#gt-ddb-083) (controller hooks catalog entry)); MultiRegionConsistency and witnesses are create-time, replica-coupled Table fields - spec.multiRegionConsistency immutable with nil == EVENTUAL and spec.globalTableWitnesses (max 1) applied together with the initial replica batch, status mirroring GlobalTableWitnesses[]; the 2019.11.21 equivalents of GlobalTableSettings are Table billing/throughput plus ReplicaUpdates overrides ([DDB-GLOBALTABLESETTINGS-001](#ddb-globaltablesettings-001), [DDB-GLOBALTABLESETTINGS-002](#ddb-globaltablesettings-002), [DDB-TABLE-264](#ddb-table-264)).
- MRSC create shape: STRONG needs 3 regions in ONE UpdateTable - 2 replica Creates (the only exception to one-action-per-call) or 1 Create + 1 witness; a witness requires STRONG, cannot share a region with a replica or the table, max 1 per call and per group; us-west-1 is not MRSC-capable; MultiRegionConsistency alone or GlobalTableWitnessUpdates alone do not count as a mutation ('At least one of ... is required'); STRONG cannot be applied to an existing EVENTUAL group and an existing STRONG group answers 'MultiRegionConsistency parameter is unsupported on existing global table' ([DDB-TABLE-257](#ddb-table-257), [DDB-TABLE-258](#ddb-table-258), [DDB-TABLE-202](#ddb-table-202), [DDB-TABLE-414](#ddb-table-414), [DDB-TABLE-428](#ddb-table-428); [DDB-TABLE-015](table-indexes.md#ddb-table-015), table-indexes.md).
- The controller cannot build or dissolve a STRONG group: the hooks catalog records MultiRegionConsistency as excluded from the CRD (generator.yaml:13-14; [GT-DDB-072](service.md#gt-ddb-072) (controller hooks catalog entry)), replicas added one per reconcile ([GT-DDB-032](service.md#gt-ddb-032) (controller hooks catalog entry)) and removed one Delete per reconcile by the delete hook ([GT-DDB-008](service.md#gt-ddb-008) (controller hooks catalog entry)), while a STRONG group needs all Creates in one call and all Deletes in one call ([DDB-TABLE-257](#ddb-table-257), [DDB-TABLE-258](#ddb-table-258), [DDB-TABLE-260](#ddb-table-260), [DDB-TABLE-261](#ddb-table-261)).
- MRSC is frozen: add/delete replica, delete witness alone, mode change and per-region DeleteTable ('Deletion of only one table replica is not supported') are all rejected; teardown is ONE UpdateTable with Delete for every replica AND the witness, after which the source is a plain regional table; DeletionProtection on a member is per region, holds UPDATING ~38 s and re-toggle is throttled 15 s ([DDB-TABLE-260](#ddb-table-260), [DDB-TABLE-261](#ddb-table-261), [DDB-TABLE-263](#ddb-table-263)).
- Describe shape: regional tables omit GlobalTableVersion, Replicas, MultiRegionConsistency, GlobalTableWitnesses and GlobalTableSettingsReplicationMode entirely (not empty lists); with a replica: GlobalTableVersion=2019.11.21, Replicas[] (never lists itself, order differs per region, inherited values absent, no ReplicaArn), GlobalTableSettingsReplicationMode=ENABLED_WITH_OVERRIDES (ENABLED/DISABLED settable even on a regional table, ENABLED_WITH_OVERRIDES not sendable); MultiRegionConsistency appears only as STRONG; GlobalTableWitnesses[{RegionName,WitnessStatus}] has no table, ARN or tags in the witness region (a same-named user table may coexist there); all keys disappear when the last replica goes ([DDB-TABLE-201](#ddb-table-201), [DDB-TABLE-229](#ddb-table-229), [DDB-TABLE-231](#ddb-table-231), [DDB-TABLE-320](#ddb-table-320), [DDB-TABLE-048](#ddb-table-048), [DDB-TABLE-259](#ddb-table-259)).
- Because DescribeTable lists every replica regardless of who created it, the hooks catalog flags computeReplicaupdatesDelta as deleting any observed region absent from the spec (pkg/resource/table/hooks.go:720-727, hooks_replica_updates.go:413-418; [GT-DDB-037](service.md#gt-ddb-037) (controller hooks catalog entry)): out-of-band replicas are removed and a CR bound to a replica-region table would try to delete the source ([DDB-TABLE-229](#ddb-table-229); [DDB-TABLE-265](table-replicas.md#ddb-table-265), table-replicas.md).
- Group-wide settings and autoscaling: a manual ProvisionedThroughput change in ANY region replicates group-wide with no override materialized; UpdateTableReplicaAutoScaling exists only for 2019.11.21 groups and rejects PAY_PER_REQUEST groups except AutoScalingDisabled; adding a PROVISIONED GSI auto-registers its write autoscaling, but one un-autoscaled GSI blocks every GSI UpdateTable; switching to PAY_PER_REQUEST keeps the AAS targets; a disabled replica CMK surfaces as ReplicaStatus=INACCESSIBLE_ENCRYPTION_CREDENTIALS only after ~80 min with the source still writable ([DDB-TABLE-251](table-replicas.md#ddb-table-251), [DDB-TABLE-187](table-replicas.md#ddb-table-187), [DDB-TABLE-190](table-replicas.md#ddb-table-190), [DDB-TABLE-329](table-replicas.md#ddb-table-329), table-replicas.md; [DDB-TABLE-242](table-policy-kinesis-autoscaling.md#ddb-table-242), [DDB-TABLE-322](table-policy-kinesis-autoscaling.md#ddb-table-322), [DDB-TABLE-291](table-policy-kinesis-autoscaling.md#ddb-table-291), [DDB-TABLE-317](table-policy-kinesis-autoscaling.md#ddb-table-317), table-subresources.md).

### Timing you should expect
- STRONG group ACTIVE: 15.6-15.7 s for two replica Creates, 59.5-86.0 s for replica + witness (n=4), with no Replicas/GlobalTableWitnesses entries for the first 5-32 s; dissolution 54.6-70 s (replica + witness), 62 s for three replicas ([DDB-TABLE-258](#ddb-table-258), [DDB-TABLE-261](#ddb-table-261)).
- EVENTUAL Create on an empty table: 17-28 s total (entry at ~6.7 s, replica region visible at ~10 s), re-add 23 s; with one item 562-660 s with the base ACTIVE -> UPDATING -> ACTIVE in between ([DDB-TABLE-226](table-replicas.md#ddb-table-226), [DDB-TABLE-298](table-replicas.md#ddb-table-298), [DDB-TABLE-265](table-replicas.md#ddb-table-265), [DDB-TABLE-329](table-replicas.md#ddb-table-329), table-replicas.md).
- EVENTUAL Delete: source UPDATING 36 s, replica DELETING 88 s, entry gone at 125 s; 208 s entry gone / 242 s replica 404 on a formerly non-empty table, 97 s for a re-added replica ([DDB-TABLE-204](table-replicas.md#ddb-table-204), [DDB-TABLE-228](table-replicas.md#ddb-table-228), table-replicas.md).
- Fan-out: PT propagation 38-39 s ([DDB-TABLE-251](table-replicas.md#ddb-table-251), table-replicas.md); DP toggle on an MRSC member 38.5 s ([DDB-TABLE-263](#ddb-table-263)); PAY_PER_REQUEST switch with replicas 218 s and GSI backfill on a global table 535 s ([DDB-TABLE-317](table-policy-kinesis-autoscaling.md#ddb-table-317), [DDB-TABLE-322](table-policy-kinesis-autoscaling.md#ddb-table-322), table-subresources.md); replica INACCESSIBLE detected at 4783 s after DisableKey ([DDB-TABLE-329](table-replicas.md#ddb-table-329), table-replicas.md).

### Known handling gaps in the controller
- The generated GlobalTable update path (pkg/resource/global_table/sdk.go:253-333, custom_api.go:21-30; generator.yaml:17-21) sends UpdateGlobalTable with ReplicaUpdates=[] which DynamoDB rejects with ValidationException, so it can never succeed, and ListGlobalTables with a bad RegionName is a 5xx a List would retry forever - moot in practice because CreateGlobalTable is rejected everywhere, so no GlobalTable CR ever reaches that path ([DDB-GLOBALTABLE-004](#ddb-globaltable-004)).

### Where to look next
- The only-member rule, PROVISIONED/AAS prerequisites, overrides, per-replica KMS, the 24 h source-region rule, settings replication, replica create/delete timings, admissibility and error taxonomy, and the AAS facade verdict ([DDB-TABLE-224](table-replicas.md#ddb-table-224), [DDB-TABLE-203](table-replicas.md#ddb-table-203), [DDB-TABLE-227](table-replicas.md#ddb-table-227), table-replicas.md); stream and SSE rules on regional tables and the single-region INACCESSIBLE state (table-streams-encryption-class.md); GSI rules on regional tables and the KeySchema/LSI immutability ([DDB-TABLE-357](table-indexes.md#ddb-table-357), table-indexes.md); the resource-policy Version quirk ([DDB-TABLE-354](table-policy-kinesis-autoscaling.md#ddb-table-354), table-subresources.md).
- Controller: pkg/resource/table/hooks_replica_updates.go, pkg/resource/global_table/, generator.yaml:13-31. Evidence: services/dynamodb/probes/table/cross-region/*, services/dynamodb/probes/globaltable/*.

Entries below are generated from the lab findings; low-impact items are in the appendix, long notes under details/.
<!-- preserved:end -->

## At a glance

- canonical findings: 22 (high 11 / medium 6 / low 5); duplicates folded into the appendix: 2
- handling: handled 14 · partial 0 · tracked 0 · unhandled 6 · suspect-bug 1 · n-a 1 (tracked = handled/partial whose reference is an open GitHub issue; counted as not handled)
- re-verified: 0 · last_verified: 2026-10-08..2026-10-09 · model: 2012-08-10 (service/dynamodb v1.39.8)
- categories: other 4, scope 4, cross-region 2, error-code 2, immutable-field 2, request-validation 2,
  response-fidelity 2, delete-semantics 1, identity 1, server-default 1, update-granularity 1

## Operations

| operation | kind | required inputs | declared error shapes | paginated |
| --- | --- | --- | --- | --- |
| CreateGlobalTable | create | GlobalTableName, ReplicationGroup | LimitExceededException, InternalServerError, GlobalTableAlreadyExistsException, TableNotFoundException | no |
| CreateTable | create | TableName | ResourceInUseException, LimitExceededException, InternalServerError | no |
| DeleteTable | delete | TableName | ResourceInUseException, ResourceNotFoundException, LimitExceededException, InternalServerError | no |
| DescribeGlobalTable | read | GlobalTableName | InternalServerError, GlobalTableNotFoundException | no |
| DescribeGlobalTableSettings | read | GlobalTableName | GlobalTableNotFoundException, InternalServerError | no |
| DescribeTable | read | TableName | ResourceNotFoundException, InternalServerError | no |
| ListGlobalTables | list | - | InternalServerError | no |
| ListTables | list | - | InternalServerError | yes |
| ListTagsOfResource | list | ResourceArn | ResourceNotFoundException, InternalServerError | yes |
| UpdateGlobalTable | update | GlobalTableName, ReplicaUpdates | InternalServerError, GlobalTableNotFoundException, ReplicaAlreadyExistsException, ReplicaNotFoundException, TableNotFoundException | no |
| UpdateGlobalTableSettings | update | GlobalTableName | GlobalTableNotFoundException, ReplicaNotFoundException, IndexNotFoundException, LimitExceededException, ResourceInUseException, InternalServerError | no |
| UpdateTable | update | TableName | ResourceInUseException, ResourceNotFoundException, LimitExceededException, InternalServerError | no |

## State machine

- **GlobalTableStatus**: CREATING, ACTIVE, DELETING, UPDATING (transitional: CREATING, DELETING, UPDATING)
- **IndexStatus**: CREATING, UPDATING, DELETING, ACTIVE (transitional: CREATING, UPDATING, DELETING)
- **ReplicaStatus**: CREATING, CREATION_FAILED, UPDATING, DELETING, ACTIVE, REGION_DISABLED,
  INACCESSIBLE_ENCRYPTION_CREDENTIALS, ARCHIVING, ARCHIVED, REPLICATION_NOT_AUTHORIZED (transitional:
  CREATING, UPDATING, DELETING, ARCHIVING)
- **SSEStatus**: ENABLING, ENABLED, DISABLING, DISABLED, UPDATING (transitional: ENABLING, DISABLING,
  UPDATING)
- **TableStatus**: CREATING, UPDATING, DELETING, ACTIVE, INACCESSIBLE_ENCRYPTION_CREDENTIALS, ARCHIVING,
  ARCHIVED, REPLICATION_NOT_AUTHORIZED (transitional: CREATING, UPDATING, DELETING, ARCHIVING)
- **WitnessStatus**: CREATING, DELETING, ACTIVE (transitional: CREATING, DELETING)

No high/medium state-machine findings.

## Identity and lookup

- <a id="ddb-table-231"></a>**DDB-TABLE-231** `identity` · impact medium · handled · verified 2026-10-09
  **GlobalTableVersion, Replicas and GlobalTableSettingsReplicationMode disappear again when the last replica is removed (not sticky)**
  Regional table: GlobalTableVersion, Replicas and GlobalTableSettingsReplicationMode absent;
  DescribeTableReplicaAutoScaling -> ResourceNotFoundException 'Global table with name: <name> does not
  exist.' With a replica: all three present (2019.11.21 / [..] / ENABLED_WITH_OVERRIDES). After the only
  replica was deleted: all three ABSENT again (Replicas key omitted, not []), DescribeTableReplicaAutoScaling
  -> ResourceNotFoundException again. The stream enabled by the service stays.
  - ACK: compare.nil_equals_zero_value, is_read_only · ops: DescribeTable · fields: GlobalTableVersion,
    Replicas
  - repro: DescribeTable before replica, after replica ACTIVE, after replica deleted
  - handling: handled via `apis/v1alpha1/table.go:212-215; apis/v1alpha1/table.go:238-240`
  - related: [DDB-TABLE-048](#ddb-table-048), [DDB-TABLE-320](#ddb-table-320), [DDB-TABLE-201](#ddb-table-201), [DDB-TABLE-229](#ddb-table-229), [DDB-TABLE-251](table-replicas.md#ddb-table-251), [DDB-TABLE-187](table-replicas.md#ddb-table-187),
    [DDB-TABLE-232](table-replicas.md#ddb-table-232), [DDB-TABLE-254](table-replicas.md#ddb-table-254), [DDB-TABLE-253](table-replicas.md#ddb-table-253), [DDB-TABLE-446](service.md#ddb-table-446), [DDB-TABLE-204](table-replicas.md#ddb-table-204), [DDB-TABLE-293](table-replicas.md#ddb-table-293), [DDB-TABLE-308](table-replicas.md#ddb-table-308),
    [DDB-TABLE-222](table-replicas.md#ddb-table-222), [DDB-TABLE-297](table-replicas.md#ddb-table-297), [DDB-TABLE-267](table-replicas.md#ddb-table-267), [DDB-TABLE-261](#ddb-table-261), [DDB-TABLE-221](table-replicas.md#ddb-table-221), [DDB-TABLE-305](table-replicas.md#ddb-table-305), [DDB-TABLE-224](table-replicas.md#ddb-table-224) ·
    hypotheses: H-R-030 · evidence: table/state-machine/replica-create-timeline
  - notes: Refutes contrarian H-R-030: GlobalTableVersion is not sticky; 'has GlobalTableVersion' == 'has
    replicas' in practice, but Replicas nil vs [] must still be treated as equal.

## Errors

- <a id="ddb-globaltable-004"></a>**DDB-GLOBALTABLE-004** `error-code` · impact medium · SUSPECTED CONTROLLER BUG · verified 2026-10-09
  **Legacy GlobalTable read/update APIs still answer - GlobalTableNotFoundException (HTTP 400) is the not-found signal; List returns []**
  For a plain regional table name and for a missing name alike: DescribeGlobalTable ->
  GlobalTableNotFoundException "Global table not found: Global table with name: '<name>' does not exist.";
  DescribeGlobalTableSettings, UpdateGlobalTableSettings (GlobalTableBillingMode or ReplicaSettingsUpdate) ->
  GlobalTableNotFoundException "Global table with name: '<name>' does not exist."; UpdateGlobalTable Delete ->
  GlobalTableNotFoundException; UpdateGlobalTable Create -> ValidationException "version 2017.11.29 is not
  supported" (checked before existence); UpdateGlobalTable ReplicaUpdates=[] -> ValidationException "One or
  more parameter values were invalid" (no detail). All are HTTP 400. ListGlobalTables returns 200 with
  GlobalTables=[] (no LastEvaluatedGlobalTableName key) for default, RegionName=us-east-1 and Limit=1;
  ListGlobalTables RegionName=us-fake-9 -> HTTP 500 InternalServerError "Internal server error".
  - ACK: exceptions.404, custom_find, docs-only · ops: DescribeGlobalTable, DescribeGlobalTableSettings,
    UpdateGlobalTable, UpdateGlobalTableSettings, ListGlobalTables
  - repro: Call each legacy GlobalTable API with GlobalTableName=<regional table> and =<missing name>;
    ListGlobalTables RegionName=us-fake-9
  - handling: suspected controller bug - see Handling gaps
  - related: [DDB-GLOBALTABLE-001](#ddb-globaltable-001), [DDB-GLOBALTABLESETTINGS-001](#ddb-globaltablesettings-001), [DDB-GLOBALTABLE-002](#ddb-globaltable-002), [DDB-GLOBALTABLE-003](#ddb-globaltable-003) ·
    hypotheses: H-R-049, H-R-045, H-R-040 · evidence: globaltable/round-trip/legacy-create
  - notes: A bogus RegionName on ListGlobalTables yields a 5xx (retryable-looking) rather than a
    ValidationException - a controller List would retry forever on a typo. GlobalTableNotFoundException would
    be the exceptions.404 code if the resource were ever implemented.
  - full notes: [details/DDB-GLOBALTABLE-004.md](details/DDB-GLOBALTABLE-004.md)

## Request validation

- <a id="ddb-table-048"></a>**DDB-TABLE-048** `request-validation` · impact medium · unhandled (not handled in controller) · verified 2026-10-08
  **GlobalTableSettingsReplicationMode on a regional table: ENABLED accepted, DISABLED accepted, ENABLED_WITH_OVERRIDES ValidationException**
  UpdateTable GlobalTableSettingsReplicationMode=ENABLED -> OK (changed paths
  ["GlobalTableSettingsReplicationMode"]); re-sent ENABLED -> OK. DISABLED -> OK; re-sent -> OK.
  ENABLED_WITH_OVERRIDES -> ValidationException 'Value 'ENABLED_WITH_OVERRIDES' at
  'GlobalTableSettingsReplicationMode' failed to satisfy constraint: Only ENABLED and DISABLED settings
  replication a'.
  - ACK: custom_update, scope:defer · ops: UpdateTable, DescribeTable · fields:
    GlobalTableSettingsReplicationMode
  - repro: PAY_PER_REQUEST regional table; UpdateTable GlobalTableSettingsReplicationMode=ENABLED, again,
    DISABLED, ENABLED_WITH_OVERRIDES
  - measurements: updating_s_enabled=0.0
  - handling: not handled in the controller (as of commit 34b85e6)
  - related: [DDB-TABLE-320](#ddb-table-320), [DDB-TABLE-231](#ddb-table-231), [DDB-TABLE-201](#ddb-table-201), [DDB-TABLE-229](#ddb-table-229), [DDB-TABLE-251](table-replicas.md#ddb-table-251) · evidence:
    table/mutation-matrix/stream-protection-throughput
  - notes: Hypotheses: H-T-146.
  - full notes: [details/DDB-TABLE-048.md](details/DDB-TABLE-048.md)

- <a id="ddb-table-257"></a>**DDB-TABLE-257** `request-validation` · impact high · handled · verified 2026-10-09
  **MRSC create shape rules - STRONG needs 3 regions in ONE UpdateTable (2 replica Creates, or 1 Create + 1 witness); witness needs STRONG**
  On a regional table, all synchronous HTTP 400 ValidationException: STRONG + ReplicaUpdates=[Create
  us-east-1] -> "Unsupported replica count for global tables with MultiRegionConsistency set to STRONG.";
  STRONG + GlobalTableWitnessUpdates only -> the generic "At least one of ProvisionedThroughput, ... or
  TableClass is required" (witness updates are not counted as a mutation); replica + witness without
  MultiRegionConsistency, or with EVENTUAL -> "MultiRegionConsistency must be set as STRONG when
  GlobalTableWitnessUpdates parameter is present."; witness in the replica's region -> "Cannot target multiple
  regions with the same action"; witness in the table's own region -> "Cannot add a witness in the same region
  as an existing replica when creating a global table with MultiRegionConsistency set to STRONG."; replica or
  witness in us-west-1 -> "Unsupported Region(s) specified for global tables with MultiRegionConsistency set
  to STRONG: [us-west-1]."; two witnesses -> "Value '[GlobalTableWitnessGroupUpdate(...)]' at
  'globalTableWitnessUpdates' failed to satisfy constraint: Member must have length less than or equal to 1";
  adding DeletionProtectionEnabled to the call -> "Requests that modify replicas or witnesses must not also
  modify other fields". Accepted: STRONG + [Create us-east-1] + witness us-east-2; STRONG + [Create us-east-1,
  Create us-east-2]; and (attempt 1) STRONG + [Create eu-west-1] + witness us-east-2.
  - ACK: custom_update, one-per-reconcile, terminal_codes · ops: UpdateTable · fields: MultiRegionConsistency,
    ReplicaUpdates, GlobalTableWitnessUpdates
  - repro: CreateTable (streams NEW_AND_OLD_IMAGES) -> UpdateTable with each listed
    MultiRegionConsistency/ReplicaUpdates/GlobalTableWitnessUpdates combination
  - handling: handled via `generator.yaml:13-14; e4a4d8c`
  - related: [DDB-TABLE-224](table-replicas.md#ddb-table-224), [DDB-TABLE-265](table-replicas.md#ddb-table-265), [DDB-TABLE-258](#ddb-table-258), [DDB-TABLE-223](table-replicas.md#ddb-table-223), [DDB-TABLE-259](#ddb-table-259), [DDB-TABLE-260](#ddb-table-260),
    [DDB-TABLE-261](#ddb-table-261), [DDB-TABLE-264](#ddb-table-264), [DDB-TABLE-202](#ddb-table-202), [DDB-TABLE-263](#ddb-table-263) · hypotheses: H-R-021, H-R-020, H-R-001 ·
    evidence: table/cross-region/mrsc-witness
  - notes: H-R-021 mostly confirmed (3-region minimum; witness or 2 creates; one witness) but the "restricted
    region set" part is refuted - a eu-west-1 replica with a us-east-2 witness was accepted; us-west-1 is
    simply not MRSC-capable. Because STRONG groups must be born complete, the one-replica-per-reconcile...
  - full notes: [details/DDB-TABLE-257.md](details/DDB-TABLE-257.md)

## Update granularity and ordering

- <a id="ddb-table-258"></a>**DDB-TABLE-258** `update-granularity` · impact high · handled · verified 2026-10-09
  **STRONG allows two ReplicaUpdates.Create actions in one call (EVENTUAL rejects it); MRSC groups reach ACTIVE in 16-86 s**
  UpdateTable MultiRegionConsistency=STRONG ReplicaUpdates=[Create us-east-1, Create us-east-2] returned 200
  (TableStatus=UPDATING, MultiRegionConsistency=STRONG and GlobalTableVersion=2019.11.21 already in the
  response) and both replicas were ACTIVE 15.6-15.7 s later (attempts 2 and 3). The same two-Create request
  without MultiRegionConsistency fails with "Update table operation with more than one create or delete
  replica actions not allowed". STRONG + [Create us-east-1] + witness us-east-2 reached ACTIVE after 59.5 s
  and 86.0 s (TableStatus UPDATING with no Replicas/GlobalTableWitnesses entries for the first 5-32 s, then
  CREATING entries; the replica table became describable in us-east-1 ~64 s in). Attempt 1 (eu-west-1 replica +
  us-east-2 witness): 78.4 s.
  - ACK: custom_update, e2e-timing, synced.when · ops: UpdateTable, DescribeTable · fields:
    MultiRegionConsistency, ReplicaUpdates, GlobalTableWitnessUpdates
  - repro: CreateTable (streams) -> UpdateTable MultiRegionConsistency=STRONG ReplicaUpdates=[Create
    us-east-1, Create us-east-2] -> poll DescribeTable
  - measurements: strong_two_replicas_to_active_s=15.7, strong_replica_plus_witness_to_active_s=86.0,
    strong_replica_plus_witness_to_active_s_attempt2=59.5, updating_before_entries_appear_s=31.7
  - handling: handled via `generator.yaml:27-31; pkg/resource/table/hooks_replica_updates.go:277-373; generator.yaml:13-14; e4a4d8c; test/e2e/tests/test_table_replicas.py:200-206; test/e2e/tests/test_table.py:34`
  - related: [DDB-TABLE-224](table-replicas.md#ddb-table-224), [DDB-TABLE-265](table-replicas.md#ddb-table-265), [DDB-TABLE-223](table-replicas.md#ddb-table-223), [DDB-TABLE-257](#ddb-table-257), [DDB-TABLE-226](table-replicas.md#ddb-table-226), [DDB-TABLE-306](table-policy-kinesis-autoscaling.md#ddb-table-306),
    [DDB-TABLE-308](table-replicas.md#ddb-table-308), [DDB-TABLE-309](table-replicas.md#ddb-table-309), [DDB-TABLE-329](table-replicas.md#ddb-table-329), [DDB-TABLE-187](table-replicas.md#ddb-table-187), [DDB-TABLE-305](table-replicas.md#ddb-table-305), [DDB-TABLE-249](table-replicas.md#ddb-table-249), [DDB-TABLE-259](#ddb-table-259),
    [DDB-TABLE-260](#ddb-table-260), [DDB-TABLE-261](#ddb-table-261), [DDB-TABLE-264](#ddb-table-264), [DDB-TABLE-202](#ddb-table-202), [DDB-TABLE-263](#ddb-table-263) · hypotheses: H-R-001, H-R-021 ·
    evidence: table/cross-region/mrsc-witness, table/cross-region/mrec-replica-updates
  - notes: Qualifies H-R-001 - the one-action-per-call rule has a STRONG exception. Synced must wait for
    Replicas[].ReplicaStatus and GlobalTableWitnesses[].WitnessStatus to be ACTIVE, not just TableStatus.

## Field behavior (defaults, normalization, shapes, immutability)

- <a id="ddb-table-202"></a>**DDB-TABLE-202** `immutable-field` · impact high · handled · verified 2026-10-09
  **MultiRegionConsistency=STRONG cannot be applied to an existing EVENTUAL group (validation order hides the immutability message)**
  On an ACTIVE EVENTUAL group (us-west-2 + us-east-1): UpdateTable MultiRegionConsistency=STRONG +
  ReplicaUpdates=[Create us-west-1] -> ValidationException "Unsupported replica count for global tables with
  MultiRegionConsistency set to STRONG."; STRONG + Create us-west-1 + witness us-west-1 -> ValidationException
  "Unsupported Region(s) specified for global tables with MultiRegionConsistency set to STRONG: [us-west-1].";
  STRONG or EVENTUAL alone -> ValidationException "At least one of ProvisionedThroughput, BillingMode, ... or
  TableClass is required"; GlobalTableWitnessUpdates=[Create] alone -> the same "At least one of" error. On an
  existing STRONG group the explicit message is "MultiRegionConsistency parameter is unsupported on existing
  global table" (see mrsc-witness).
  - ACK: is_immutable, terminal_codes · ops: UpdateTable · fields: MultiRegionConsistency,
    GlobalTableWitnessUpdates
  - repro: EVENTUAL group ACTIVE -> UpdateTable MultiRegionConsistency=STRONG ReplicaUpdates=[Create
    us-west-1]
  - handling: handled via `generator.yaml:13-14; e4a4d8c`
  - related: [DDB-TABLE-257](#ddb-table-257), [DDB-TABLE-258](#ddb-table-258), [DDB-TABLE-259](#ddb-table-259), [DDB-TABLE-260](#ddb-table-260), [DDB-TABLE-261](#ddb-table-261), [DDB-TABLE-264](#ddb-table-264),
    [DDB-TABLE-263](#ddb-table-263) · hypotheses: H-R-020, H-R-021 · evidence: table/cross-region/mrec-replica-updates
  - notes: Confirms H-R-020 step 2 (rejected). us-west-1 is not an MRSC-capable region;
    us-east-1/us-east-2/us-west-2/eu-west-1 are.

- <a id="ddb-table-260"></a>**DDB-TABLE-260** `immutable-field` · impact high · handled · verified 2026-10-09
  **MRSC membership and mode are frozen - add/delete replica, delete witness alone, MultiRegionConsistency change and DeleteTable all rejected**
  On the ACTIVE STRONG group (replica us-east-1, witness us-east-2), HTTP 400 ValidationException for:
  ReplicaUpdates=[Create eu-west-1] "Cannot add replicas to a global table with strong
  MultiRegionConsistency"; STRONG + [Create eu-west-1] "Unsupported replica count..."; ReplicaUpdates=[Delete
  us-east-1] alone "Update table delete operation does not include all witnesses.";
  GlobalTableWitnessUpdates=[Delete|Create] alone (with or without STRONG) "At least one of ... is required";
  MultiRegionConsistency=EVENTUAL + Create "MultiRegionConsistency parameter is unsupported on existing global
  table"; STRONG + Delete "Only Replica Create actions are supported when MultiRegionConsistency parameter is
  provided."; Delete replica + Delete non-existent witness "...one or more witnesses were not part of the
  global table. Please retry the request without these witnesses: [eu-west-1]."; DeleteTable in us-west-2 or
  in the replica region "Deletion of only one table replica is not supported for global tables with
  MultiRegionConsistency set to STRONG." On a 3-replica STRONG group ReplicaUpdates=[Delete one] and
  DeleteTable fail with that same message.
  - ACK: is_immutable, terminal_codes, custom_delete · ops: UpdateTable, DeleteTable · fields: ReplicaUpdates,
    GlobalTableWitnessUpdates, MultiRegionConsistency
  - repro: MRSC group ACTIVE -> UpdateTable ReplicaUpdates=[Create <region>]; [Delete <replica>];
    MultiRegionConsistency=EVENTUAL; DeleteTable
  - handling: handled via `templates/hooks/table/sdk_delete_pre_build_request.go.tpl:8-22; pkg/resource/table/hooks_replica_updates.go:256-263; generator.yaml:13-14; e4a4d8c`
  - related: [DDB-TABLE-266](table-replicas.md#ddb-table-266), [DDB-TABLE-262](table-replicas.md#ddb-table-262), [DDB-TABLE-267](table-replicas.md#ddb-table-267), [DDB-TABLE-265](table-replicas.md#ddb-table-265), [DDB-TABLE-297](table-replicas.md#ddb-table-297), [DDB-TABLE-257](#ddb-table-257),
    [DDB-TABLE-258](#ddb-table-258), [DDB-TABLE-259](#ddb-table-259), [DDB-TABLE-261](#ddb-table-261), [DDB-TABLE-264](#ddb-table-264), [DDB-TABLE-202](#ddb-table-202), [DDB-TABLE-263](#ddb-table-263) · hypotheses:
    H-R-020, H-R-022 · evidence: table/cross-region/mrsc-witness
  - notes: Confirms H-R-020 (mode immutable, with an explicit message) and H-R-022 steps 3-4; refutes H-R-022
    step 5 (DeleteTable per region is NOT a teardown path). Any spec change to replicas/witnesses of a STRONG
    table is terminal and must be reported as such.

- <a id="ddb-table-320"></a>**DDB-TABLE-320** `server-default` · impact medium · unhandled (not handled in controller) · verified 2026-10-09
  **GlobalTableSettingsReplicationMode absent by default on regional tables; appears as ENABLED_WITH_OVERRIDES once a replica exists**
  DescribeTable on the regional PROVISIONED table had keys [AttributeDefinitions, BillingModeSummary,
  CreationDateTime, DeletionProtectionEnabled, ItemCount, KeySchema, LatestStreamArn, LatestStreamLabel,
  ProvisionedThroughput, StreamSpecification, TableArn, TableId, TableName, TableSizeBytes, TableStatus,
  WarmThroughput]. With one 2019.11.21 replica it gained
  GlobalTableSettingsReplicationMode=ENABLED_WITH_OVERRIDES (table level and under Replicas[]),
  GlobalTableVersion=2019.11.21 and Replicas, while BillingModeSummary disappeared. After the replica was
  removed GlobalTableVersion and Replicas were absent again.
  - ACK: is_read_only, compare.is_ignored+delta_pre_compare · ops: DescribeTable, UpdateTable · fields:
    GlobalTableSettingsReplicationMode, GlobalTableVersion, Replicas, BillingModeSummary
  - repro: DescribeTable regional -> UpdateTable ReplicaUpdates Create -> DescribeTable -> ReplicaUpdates
    Delete -> DescribeTable
  - handling: not handled in the controller (as of commit 34b85e6)
  - related: [DDB-TABLE-048](#ddb-table-048), [DDB-TABLE-231](#ddb-table-231), [DDB-TABLE-201](#ddb-table-201), [DDB-TABLE-229](#ddb-table-229), [DDB-TABLE-251](table-replicas.md#ddb-table-251) · hypotheses: H-R-029 ·
    evidence: table/sub-resources/replica-autoscaling-facade
  - notes: Read-side of H-R-029 confirmed (never set by the caller). Setting it via UpdateTable was not
    attempted (other shard).
  - full notes: [details/DDB-TABLE-320.md](details/DDB-TABLE-320.md)

## Response fidelity and consistency

- <a id="ddb-table-201"></a>**DDB-TABLE-201** `response-fidelity` · impact medium · handled · verified 2026-10-09
  **EVENTUAL global tables omit MultiRegionConsistency in DescribeTable; only GlobalTableVersion=2019.11.21 and Replicas[] appear**
  Before any replica DescribeTable has no GlobalTableVersion, Replicas, MultiRegionConsistency or
  GlobalTableWitnesses keys. With a replica ACTIVE (created without MultiRegionConsistency) both regions
  report GlobalTableVersion=2019.11.21 and a Replicas entry with keys
  GlobalTableSettingsReplicationMode=ENABLED_WITH_OVERRIDES, RegionName, ReplicaStatus, WarmThroughput
  (12000/4000 ACTIVE); MultiRegionConsistency is still absent (it is only present, as STRONG, on MRSC tables)
  and GlobalTableWitnesses is absent. After the last replica is removed GlobalTableVersion and Replicas
  disappear again.
  - ACK: late_initialize, compare.nil_equals_zero_value, is_read_only · ops: DescribeTable · fields:
    MultiRegionConsistency, GlobalTableVersion, Replicas, GlobalTableWitnesses
  - repro: Regional table -> UpdateTable ReplicaUpdates=[Create us-east-1] -> DescribeTable in both regions
  - handling: handled via `pkg/resource/table/hooks_replica_updates.go:423-463; pkg/resource/table/hooks_replica_updates.go:28-147; generator.yaml:1-6; generator.yaml:15; generator.yaml:13-14; e4a4d8c; apis/v1alpha1/table.go:212-215; apis/v1alpha1/table.go:238-240`
  - related: [DDB-TABLE-220](#ddb-table-220), [DDB-TABLE-048](#ddb-table-048), [DDB-TABLE-320](#ddb-table-320), [DDB-TABLE-231](#ddb-table-231), [DDB-TABLE-229](#ddb-table-229), [DDB-TABLE-251](table-replicas.md#ddb-table-251) ·
    hypotheses: H-R-020 · evidence: table/cross-region/mrec-replica-updates
  - notes: Qualifies H-R-020 step 3 - the server default EVENTUAL is NOT echoed; a
    spec.multiRegionConsistency=EVENTUAL must be treated as equal to nil when computing deltas.

- <a id="ddb-table-259"></a>**DDB-TABLE-259** `response-fidelity` · impact high · handled · verified 2026-10-09
  **MRSC DescribeTable: MultiRegionConsistency=STRONG + GlobalTableWitnesses[{RegionName,WitnessStatus}]; witness region has no table**
  With the group ACTIVE, DescribeTable in us-west-2 and in the replica region both report
  MultiRegionConsistency=STRONG, GlobalTableVersion=2019.11.21, Replicas entries with keys
  GlobalTableSettingsReplicationMode, RegionName, ReplicaStatus, WarmThroughput and
  GlobalTableWitnesses=[{RegionName: us-east-2, WitnessStatus: ACTIVE}] (no ARN). In the witness region
  DescribeTable -> ResourceNotFoundException, ListTables does not contain the name, ListTagsOfResource on
  arn:aws:dynamodb:us-east-2:<acct>:table/<name> -> ResourceNotFoundException, and CreateTable with the same
  name there SUCCEEDS (a plain ACTIVE table coexisting with the witness; it was neither used nor touched when
  the witness was later deleted).
  - ACK: is_read_only, custom_field, late_initialize · ops: DescribeTable, ListTables, ListTagsOfResource,
    CreateTable · fields: MultiRegionConsistency, GlobalTableWitnesses, Replicas
  - repro: MRSC group ACTIVE -> DescribeTable in each region; CreateTable same name in the witness region
  - handling: handled via `generator.yaml:13-14; e4a4d8c`
  - related: [DDB-TABLE-257](#ddb-table-257), [DDB-TABLE-258](#ddb-table-258), [DDB-TABLE-260](#ddb-table-260), [DDB-TABLE-261](#ddb-table-261), [DDB-TABLE-264](#ddb-table-264), [DDB-TABLE-202](#ddb-table-202),
    [DDB-TABLE-263](#ddb-table-263) · hypotheses: H-R-022, H-R-020 · evidence: table/cross-region/mrsc-witness
  - notes: Confirms H-R-022 'witness is not a table'. A controller can only observe witnesses through the
    source/replica tables' DescribeTable; a same-named user table in the witness region is not a conflict
    signal.

## Delete semantics

- <a id="ddb-table-261"></a>**DDB-TABLE-261** `delete-semantics` · impact high · handled · verified 2026-10-09
  **MRSC teardown: ONE UpdateTable with Delete for every replica AND the witness; the source reverts to a plain regional table**
  UpdateTable ReplicaUpdates=[Delete us-east-1] GlobalTableWitnessUpdates=[Delete us-east-2] returned 200
  (TableStatus=UPDATING, echoed replica and witness still ACTIVE). DescribeTable stayed UPDATING with both
  ACTIVE for ~44 s, then showed ReplicaStatus/WitnessStatus DELETING for ~10 s, then TableStatus=ACTIVE with
  no Replicas, no GlobalTableWitnesses, no MultiRegionConsistency and no GlobalTableVersion keys (54.6 s
  total; attempt 1: ~70 s). The us-east-1 replica table was deleted; the stream stayed enabled. For a
  3-replica STRONG group, ReplicaUpdates=[Delete us-east-1, Delete us-east-2] in one call was accepted and
  settled in 62 s, after which DeleteTable succeeded. A stray same-named table in the witness region did not
  block the witness deletion.
  - ACK: custom_delete, pre-delete-cleanup, e2e-timing · ops: UpdateTable, DescribeTable, DeleteTable ·
    fields: ReplicaUpdates, GlobalTableWitnessUpdates, MultiRegionConsistency, GlobalTableVersion
  - repro: MRSC group -> UpdateTable ReplicaUpdates=[Delete <all replicas>] GlobalTableWitnessUpdates=[Delete
    <witness>] -> poll -> DeleteTable
  - measurements: dissolve_replica_plus_witness_s=54.56, dissolve_three_replicas_s=62.15,
    updating_before_deleting_entries_s=43.6
  - handling: handled via `templates/hooks/table/sdk_delete_pre_build_request.go.tpl:8-22; pkg/resource/table/hooks_replica_updates.go:256-263; generator.yaml:13-14; e4a4d8c`
  - related: [DDB-TABLE-204](table-replicas.md#ddb-table-204), [DDB-TABLE-293](table-replicas.md#ddb-table-293), [DDB-TABLE-308](table-replicas.md#ddb-table-308), [DDB-TABLE-222](table-replicas.md#ddb-table-222), [DDB-TABLE-297](table-replicas.md#ddb-table-297), [DDB-TABLE-267](table-replicas.md#ddb-table-267),
    [DDB-TABLE-231](#ddb-table-231), [DDB-TABLE-257](#ddb-table-257), [DDB-TABLE-258](#ddb-table-258), [DDB-TABLE-259](#ddb-table-259), [DDB-TABLE-260](#ddb-table-260), [DDB-TABLE-264](#ddb-table-264), [DDB-TABLE-202](#ddb-table-202),
    [DDB-TABLE-263](#ddb-table-263), [DDB-TABLE-250](table-replicas.md#ddb-table-250), [DDB-TABLE-294](table-replicas.md#ddb-table-294), [DDB-TABLE-296](table-replicas.md#ddb-table-296), [DDB-TABLE-226](table-replicas.md#ddb-table-226), [DDB-TABLE-309](table-replicas.md#ddb-table-309), [DDB-TABLE-221](table-replicas.md#ddb-table-221) ·
    hypotheses: H-R-022, H-R-001 · evidence: table/cross-region/mrsc-witness
  - notes: The harness deleter (one ReplicaUpdates Delete per replica) cannot tear down MRSC groups; a Table
    controller's delete must issue the combined call first. After dissolution the table is a plain regional
    table and an EVENTUAL replica could be added again (46 s).

## Cross-region

- <a id="ddb-table-229"></a>**DDB-TABLE-229** `cross-region` · impact high · handled · verified 2026-10-09
  **Replica-region DescribeTable: own TableId/ARN/stream, same GlobalTableVersion, Replicas lists the other region (never itself)**
  After ACTIVE both regions return the same key set. Equal: TableName, TableStatus, GlobalTableVersion
  (2019.11.21), StreamSpecification, TableClassSummary (absent for STANDARD), DeletionProtectionEnabled,
  SSEDescription, ItemCount, TableSizeBytes, GlobalTableSettingsReplicationMode (ENABLED_WITH_OVERRIDES,
  present only while replicas exist), WarmThroughput, ProvisionedThroughput (0/0 sentinel). Different:
  TableId, TableArn, CreationDateTime, LatestStreamArn, LatestStreamLabel,
  BillingModeSummary.LastUpdateToPayPerRequestDateTime. Replicas is symmetric: A lists [us-east-1], B lists
  [us-west-2]; never itself. A Replicas[] entry for a PPR replica with no overrides contains only RegionName,
  ReplicaStatus, WarmThroughput and GlobalTableSettingsReplicationMode - no ReplicaArn, no KMSMasterKeyId, no
  ProvisionedThroughputOverride, no ReplicaTableClassSummary, no ReplicaStatusPercentProgress.
  - ACK: custom_find, compare.is_ignored+delta_pre_compare, is_read_only · ops: DescribeTable · fields:
    Replicas, GlobalTableVersion, TableId, TableArn, LatestStreamArn
  - repro: DescribeTable in both regions once the replica is ACTIVE
  - handling: handled via `pkg/resource/table/hooks_replica_updates.go:423-463; pkg/resource/table/hooks_replica_updates.go:28-147; pkg/resource/table/hooks.go:720-727; pkg/resource/table/hooks_replica_updates.go:413-418; apis/v1alpha1/table.go:212-215; apis/v1alpha1/table.go:238-240`
  - related: [DDB-TABLE-048](#ddb-table-048), [DDB-TABLE-320](#ddb-table-320), [DDB-TABLE-231](#ddb-table-231), [DDB-TABLE-201](#ddb-table-201), [DDB-TABLE-251](table-replicas.md#ddb-table-251), [DDB-TABLE-250](table-replicas.md#ddb-table-250),
    [DDB-TABLE-252](table-replicas.md#ddb-table-252), [DDB-TABLE-321](table-replicas.md#ddb-table-321), [DDB-TABLE-294](table-replicas.md#ddb-table-294), [DDB-TABLE-223](table-replicas.md#ddb-table-223), [DDB-TABLE-307](table-replicas.md#ddb-table-307), [DDB-TABLE-232](table-replicas.md#ddb-table-232), [DDB-TABLE-265](table-replicas.md#ddb-table-265),
    [DDB-TABLE-189](table-policy-kinesis-autoscaling.md#ddb-table-189) · hypotheses: H-R-007, H-R-013, H-R-015 · evidence:
    table/state-machine/replica-create-timeline
  - notes: Confirms H-R-007. Qualifies H-R-013/H-R-015: inherited values are simply ABSENT in the replica
    entry (nil = inherit). ReplicaArn is not populated, so the replica's tags must be read via a constructed
    ARN (same name, replica region) rather than Replicas[].ReplicaArn.

- <a id="ddb-table-263"></a>**DDB-TABLE-263** `cross-region` · impact medium · unhandled (not handled in controller) · verified 2026-10-09
  **DeletionProtection on an MRSC member: table UPDATING ~38 s, not propagated to the replica, re-toggle throttled for 15 s**
  UpdateTable DeletionProtectionEnabled=true on the us-west-2 member of an ACTIVE MRSC group returned
  TableStatus=UPDATING; DescribeTable stayed UPDATING for 38.5 s (replica and witness ACTIVE throughout). The
  replica in us-east-1 still reported DeletionProtectionEnabled=false afterwards (setting is per region). An
  immediate DeletionProtectionEnabled=false -> ResourceInUseException; after the table was ACTIVE again ->
  ThrottlingException "Deletion protection setting for table <name> modified within the previous 15000
  milliseconds. Please try again after <ts>" (HTTP 400). While the table was UPDATING the MRSC dissolve call
  also got ResourceInUseException.
  - ACK: requeue, e2e-timing, synced.when · ops: UpdateTable, DescribeTable · fields:
    DeletionProtectionEnabled
  - repro: MRSC group ACTIVE -> UpdateTable DeletionProtectionEnabled=true -> poll DescribeTable; UpdateTable
    DeletionProtectionEnabled=false
  - measurements: updating_after_deletion_protection_toggle_s=38.47,
    deletion_protection_retoggle_throttle_s=15
  - handling: not handled in the controller (as of commit 34b85e6)
  - related: [DDB-TABLE-306](table-policy-kinesis-autoscaling.md#ddb-table-306), [DDB-TABLE-200](table-replicas.md#ddb-table-200), [DDB-TABLE-296](table-replicas.md#ddb-table-296), [DDB-TABLE-315](table-policy-kinesis-autoscaling.md#ddb-table-315), [DDB-TABLE-327](table-replicas.md#ddb-table-327), [DDB-TABLE-314](table-policy-kinesis-autoscaling.md#ddb-table-314),
    [DDB-TABLE-297](table-replicas.md#ddb-table-297), [DDB-TABLE-329](table-replicas.md#ddb-table-329), [DDB-TABLE-311](table-replicas.md#ddb-table-311), [DDB-TABLE-257](#ddb-table-257), [DDB-TABLE-258](#ddb-table-258), [DDB-TABLE-259](#ddb-table-259), [DDB-TABLE-260](#ddb-table-260),
    [DDB-TABLE-261](#ddb-table-261), [DDB-TABLE-264](#ddb-table-264), [DDB-TABLE-202](#ddb-table-202) · hypotheses: H-R-020 · evidence:
    table/cross-region/mrsc-witness
  - notes: On a regional table deletion protection flips without an UPDATING phase in other probes; on a
    global table member it is an asynchronous update that serialises with replica operations.
  - full notes: [details/DDB-TABLE-263.md](details/DDB-TABLE-263.md)

## Scope

- <a id="ddb-globaltable-001"></a>**DDB-GLOBALTABLE-001** `scope` · impact high · handled · verified 2026-10-09
  **Legacy GlobalTable APIs do not see 2019.11.21 tables (GlobalTableNotFoundException; UpdateGlobalTable Create -> version error)**
  **Scope verdict: skip:deprecated**
  For a 2019.11.21 EVENTUAL global table: DescribeGlobalTable (from either region),
  DescribeGlobalTableSettings, UpdateGlobalTableSettings (GlobalTableBillingMode or ReplicaSettingsUpdate) and
  UpdateGlobalTable Delete all fail with HTTP 400 GlobalTableNotFoundException "Global table with name:
  '<name>' does not exist."; UpdateGlobalTable Create fails with ValidationException "DynamoDB global tables
  version 2017.11.29 is not supported..."; ListGlobalTables (default, RegionName=us-east-1, or from the
  us-east-1 endpoint) returns an empty list.
  - ACK: scope:skip, ignore.resource · ops: DescribeGlobalTable, DescribeGlobalTableSettings,
    UpdateGlobalTableSettings, UpdateGlobalTable, ListGlobalTables
  - repro: 2019.11.21 global table -> each legacy GlobalTable API with its name
  - handling: handled via `apis/v1alpha1/table.go:212-215; apis/v1alpha1/table.go:238-240; test/e2e/tests/test_global_table.py:82; 089de59; generator.yaml:17-21; pkg/resource/global_table/custom_api.go:21-30; generator.yaml:126-129; pkg/resource/global_table/sdk.go:83-85`
  - related: [DDB-GLOBALTABLE-004](#ddb-globaltable-004), [DDB-GLOBALTABLESETTINGS-001](#ddb-globaltablesettings-001), [DDB-GLOBALTABLE-002](#ddb-globaltable-002), [DDB-GLOBALTABLE-003](#ddb-globaltable-003) ·
    hypotheses: H-R-045, H-R-049 · evidence: table/cross-region/mrec-replica-updates
  - notes: Confirms the 2019-side of H-R-045 and H-R-049 step 2. Together with CreateGlobalTable being
    rejected, no GlobalTable or GlobalTableSettings instance can exist in this account - both nouns should be
    skipped.

- <a id="ddb-globaltable-002"></a>**DDB-GLOBALTABLE-002** `scope` · impact high · handled · verified 2026-10-09
  **CreateGlobalTable (legacy 2017.11.29) is rejected everywhere in 2026 - 'global tables version 2017.11.29 is not supported'** (hypothesis refuted; behavior confirmed)
  **Scope verdict: skip:deprecated**
  With identical, empty, PAY_PER_REQUEST, stream-enabled (NEW_AND_OLD_IMAGES) tables ACTIVE in us-west-2 and
  us-east-1, CreateGlobalTable ReplicationGroup=[us-west-2, us-east-1] fails synchronously with HTTP 400
  ValidationException "One or more parameter values were invalid: DynamoDB global tables version 2017.11.29 is
  not supported. We recommend using DynamoDB global tables version 2019.11.21, instead of version 2017.11.29
  (Legacy)." from the us-west-2, us-east-1, us-east-2 and eu-west-1 endpoints. From ap-south-1 the message is
  region-specific and enumerates the 11 regions where the legacy version was ever supported ("not supported in
  this region 'ap-south-1', supported regions are : [eu-west-2, eu-west-1, ap-southeast-1, ap-southeast-2,
  ap-northeast-2, eu-central-1, ap-northeast-1, us-east-1, us-east-2, us-west-1, us-west-2]") yet the create
  is rejected in those regions too. Nothing is created (DescribeGlobalTable -> GlobalTableNotFoundException);
  the regional tables are untouched.
  - ACK: scope:skip, ignore.resource · ops: CreateGlobalTable
  - repro: CreateTable x2 (same name, streams NEW_AND_OLD_IMAGES) -> wait ACTIVE -> CreateGlobalTable
    ReplicationGroup=[us-west-2, us-east-1]
  - handling: handled via `test/e2e/tests/test_global_table.py:82; 089de59`
  - related: [DDB-GLOBALTABLE-005](#ddb-globaltable-005), [DDB-GLOBALTABLE-001](#ddb-globaltable-001), [DDB-GLOBALTABLE-004](#ddb-globaltable-004), [DDB-GLOBALTABLESETTINGS-001](#ddb-globaltablesettings-001),
    [DDB-GLOBALTABLE-003](#ddb-globaltable-003) · hypotheses: H-R-035, H-R-051, H-R-037, H-R-039, H-R-040, H-R-041, H-R-042, H-R-043,
    H-R-044 · evidence: globaltable/round-trip/legacy-create
  - notes: Refutes H-R-035 (status=refuted refers to the hypothesis; the rejection itself was observed 5x).
    Settles H-R-051: a GlobalTable CRD cannot create anything in 2026 and can only adopt pre-existing legacy
    groups (none can be made for testing) -> drop it in favour of Table.spec.replicas....
  - full notes: [details/DDB-GLOBALTABLE-002.md](details/DDB-GLOBALTABLE-002.md)

- <a id="ddb-globaltablesettings-001"></a>**DDB-GLOBALTABLESETTINGS-001** `scope` · impact high · handled · verified 2026-10-09
  **GlobalTableSettings has no reachable instance in 2026 - legacy-only, no create/delete, GlobalTableNotFoundException for every table**
  **Scope verdict: skip:deprecated**
  DescribeGlobalTableSettings and UpdateGlobalTableSettings (GlobalTableBillingMode or ReplicaSettingsUpdate)
  return HTTP 400 GlobalTableNotFoundException "Global table with name: '<name>' does not exist." for a plain
  regional table, for a missing name, and for 2019.11.21 global tables (EVENTUAL and STRONG, from the source
  or a replica region). The only operation that could create a legacy global table for them to describe,
  CreateGlobalTable, is rejected everywhere with "version 2017.11.29 is not supported". The API has no
  Create/Delete for settings; its leaves are just the regional tables' billing/capacity/auto-scaling settings.
  - ACK: scope:skip, ignore.resource · ops: DescribeGlobalTableSettings, UpdateGlobalTableSettings,
    CreateGlobalTable
  - repro: DescribeGlobalTableSettings / UpdateGlobalTableSettings with a regional, missing and 2019.11.21
    table name
  - handling: handled via `generator.yaml:126-129; pkg/resource/global_table/sdk.go:83-85`
  - related: [DDB-GLOBALTABLE-001](#ddb-globaltable-001), [DDB-GLOBALTABLE-004](#ddb-globaltable-004), [DDB-GLOBALTABLE-002](#ddb-globaltable-002), [DDB-GLOBALTABLE-003](#ddb-globaltable-003) · hypotheses:
    H-R-049, H-R-045 · evidence: globaltable/round-trip/legacy-create,
    table/cross-region/mrec-replica-updates, table/cross-region/mrsc-witness
  - notes: Confirms H-R-049 (not a standalone CRD) with the stronger conclusion that it should be skipped
    outright rather than folded into a GlobalTable resource, since that parent cannot be created either. The
    2019.11.21 equivalents are Table.spec.billingMode / provisionedThroughput and...
  - full notes: [details/DDB-GLOBALTABLESETTINGS-001.md](details/DDB-GLOBALTABLESETTINGS-001.md)

- <a id="ddb-table-264"></a>**DDB-TABLE-264** `scope` · impact high · handled · verified 2026-10-09
  **MultiRegionConsistency and witnesses belong on Table as create-time, replica-coupled fields (not a separate resource)**
  **Scope verdict: field-on-parent**
  MultiRegionConsistency and GlobalTableWitnessUpdates are only accepted inside the UpdateTable call that
  creates the first replicas of a regional table (STRONG needs 2 Creates or 1 Create + 1 witness in that one
  call), are reported read-only by DescribeTable as MultiRegionConsistency (STRONG only; absent for EVENTUAL)
  and GlobalTableWitnesses[], cannot be changed afterwards ("MultiRegionConsistency parameter is unsupported
  on existing global table"), and are removed only by one combined Delete call. The witness has no table, ARN,
  tags or describe path of its own.
  - ACK: scope:field-on-parent, is_immutable, custom_update, custom_delete · ops: UpdateTable, DescribeTable ·
    fields: MultiRegionConsistency, GlobalTableWitnessUpdates, GlobalTableWitnesses
  - repro: See table/cross-region/mrsc-witness steps 2-8
  - handling: handled via `generator.yaml:27-31; pkg/resource/table/hooks_replica_updates.go:277-373; generator.yaml:13-14; e4a4d8c`
  - related: [DDB-TABLE-257](#ddb-table-257), [DDB-TABLE-258](#ddb-table-258), [DDB-TABLE-259](#ddb-table-259), [DDB-TABLE-260](#ddb-table-260), [DDB-TABLE-261](#ddb-table-261), [DDB-TABLE-202](#ddb-table-202),
    [DDB-TABLE-263](#ddb-table-263) · hypotheses: H-R-020, H-R-021, H-R-022 · evidence: table/cross-region/mrsc-witness,
    table/cross-region/mrec-replica-updates
  - notes: Suggested shape: spec.multiRegionConsistency (immutable, default nil==EVENTUAL) and
    spec.globalTableWitnesses[] (immutable, max 1) on Table, applied together with the initial spec.replicas
    batch; status.globalTableWitnesses mirrors DescribeTable.

## Handling gaps (bugs to file)

- [DDB-GLOBALTABLE-004](#ddb-globaltable-004) - Legacy GlobalTable read/update APIs still answer - GlobalTableNotFoundException (HTTP 400)
  is the not-found signal; List returns [] (suspected bug)
  Suspected controller bug confirmed by evidence: UpdateGlobalTable with ReplicaUpdates=[] is rejected
  server-side with ValidationException 'One or more parameter values were invalid' (the Go SDK would reject a
  nil required member even earlier), so a ReplicaUpdates-less generated update can never succeed. Practically
  moot: CreateGlobalTable is rejected everywhere ([DDB-GLOBALTABLE-002](#ddb-globaltable-002)), so no GlobalTable CR reaches the
  update path.
  - handling_ref: `generator.yaml:17-21; pkg/resource/global_table/custom_api.go:21-30;
    pkg/resource/global_table/sdk.go:253-333; generator.yaml:125-139; generator.yaml:126-129;
    pkg/resource/global_table/sdk.go:83-85`

## E2E timing

Values are seconds unless the key says otherwise; n = trials behind the numbers ('1 run' when the finding records none).

| finding | what | measurements | n |
| --- | --- | --- | --- |
| [DDB-TABLE-258](#ddb-table-258) | STRONG allows two ReplicaUpdates.Create actions in one call (EVENTUAL rejects it); MRSC groups reach ACTIVE in 16-86 s | strong_two_replicas_to_active_s=15.7, strong_replica_plus_witness_to_active_s=86.0, strong_replica_plus_witness_to_active_s_attempt2=59.5, updating_before_entries_appear_s=31.7 | 1 run |
| [DDB-TABLE-261](#ddb-table-261) | MRSC teardown: ONE UpdateTable with Delete for every replica AND the witness; the source reverts to a plain regional table | dissolve_replica_plus_witness_s=54.56, dissolve_three_replicas_s=62.15, updating_before_deleting_entries_s=43.6 | 1 run |
| [DDB-TABLE-263](#ddb-table-263) | DeletionProtection on an MRSC member: table UPDATING ~38 s, not propagated to the replica, re-toggle throttled for 15 s | updating_after_deletion_protection_toggle_s=38.47, deletion_protection_retoggle_throttle_s=15 | 1 run |

## Open questions

- [DDB-GLOBALTABLE-006](#ddb-globaltable-006) (unverified) - Doc claim C028 UNTESTABLE: ListGlobalTables 'Limit defaults to 100' - the
  legacy API lists nothing in 2026; the default is unobservable: VERDICT: UNTESTABLE - legacy-only API with no
  reachable instances; the default cannot be exercised
- [DDB-GLOBALTABLESETTINGS-002](#ddb-globaltablesettings-002) (unverified) - Doc claim C041 UNTESTABLE:
  GlobalTableProvisionedWriteCapacityUnits = max writes/s before ThrottlingException: VERDICT: UNTESTABLE -
  data-plane throttling claim; additionally UpdateGlobalTableSettings (legacy 2017.11.29 global tables) has no
  reachable instance in 2026

<!-- preserved:start id=open-questions -->
<!-- open questions and follow-up experiments; survives re-renders -->
<!-- preserved:end -->

## Appendix: low-impact and duplicate findings

| id | category | impact | status | title | related | duplicate_of |
| --- | --- | --- | --- | --- | --- | --- |
| <a id="ddb-globaltable-003"></a>**DDB-GLOBALTABLE-003** | error-code | low | confirmed | CreateGlobalTable validation order - request-shape errors fire before the version check, the version check before TableNotFound | [DDB-GLOBALTABLE-001](#ddb-globaltable-001), [DDB-GLOBALTABLE-004](#ddb-globaltable-004), [DDB-GLOBALTABLESETTINGS-001](#ddb-globaltablesettings-001), [DDB-GLOBALTABLE-002](#ddb-globaltable-002) | - |
| <a id="ddb-globaltable-005"></a>**DDB-GLOBALTABLE-005** | scope | medium | confirmed | CreateGlobalTable (2017.11.29) is rejected: ValidationException 'global tables version 2017.11.29 is not supported' | - | [DDB-GLOBALTABLE-002](#ddb-globaltable-002) |
| <a id="ddb-globaltable-006"></a>**DDB-GLOBALTABLE-006** | other | low | unverified | Doc claim C028 UNTESTABLE: ListGlobalTables 'Limit defaults to 100' - the legacy API lists nothing in 2026; the default is unobservable | [DDB-GLOBALTABLE-002](#ddb-globaltable-002), [DDB-GLOBALTABLE-004](#ddb-globaltable-004), [DDB-GLOBALTABLE-001](#ddb-globaltable-001) | - |
| <a id="ddb-globaltablesettings-002"></a>**DDB-GLOBALTABLESETTINGS-002** | other | low | unverified | Doc claim C041 UNTESTABLE: GlobalTableProvisionedWriteCapacityUnits = max writes/s before ThrottlingException | [DDB-GLOBALTABLESETTINGS-001](#ddb-globaltablesettings-001), [DDB-GLOBALTABLE-002](#ddb-globaltable-002) | - |
| <a id="ddb-table-220"></a>**DDB-TABLE-220** | response-fidelity | low | confirmed | Plain regional table omits GlobalTableVersion, Replicas, MultiRegionConsistency and GlobalTableWitnesses entirely (not empty lists) | - | [DDB-TABLE-201](#ddb-table-201) |
| <a id="ddb-table-414"></a>**DDB-TABLE-414** | other | low | confirmed | Doc claim C046 TRUE: Only one witness can be created or deleted per UpdateTable operation | [DDB-TABLE-257](#ddb-table-257), [DDB-TABLE-260](#ddb-table-260), [DDB-TABLE-261](#ddb-table-261) | - |
| <a id="ddb-table-428"></a>**DDB-TABLE-428** | other | low | confirmed | Doc claim C061 TRUE: Only one witness Region can be configured per MRSC global table | [DDB-TABLE-257](#ddb-table-257), [DDB-TABLE-260](#ddb-table-260), [DDB-TABLE-259](#ddb-table-259) | - |

## Supplementary notes

<!-- preserved:start -->
<!-- preserved:end -->
