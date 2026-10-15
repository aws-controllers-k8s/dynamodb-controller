<!-- generated: ack-api-quirks dynamodb 2026-10-09 model=2012-08-10 controller=34b85e6 -->
# Table sub-resources (TTL, PITR, Contributor Insights)
_TTL, point-in-time recovery and Contributor Insights: API pairs managed outside CreateTable/UpdateTable/DescribeTable._
Generated from ack-api-quirks `services/dynamodb` (render date in the marker above); model 2012-08-10 (service/dynamodb v1.39.8); controller commit 34b85e6; evidence: `services/dynamodb/probes/<probe id>/` in the lab repo.

## Overview

<!-- preserved:start id=overview -->
This document covers the three Table properties that live behind their own API pairs instead of CreateTable/UpdateTable/DescribeTable: TTL (UpdateTimeToLive/DescribeTimeToLive), point-in-time recovery (UpdateContinuousBackups/DescribeContinuousBackups) and Contributor Insights (Update/Describe/ListContributorInsights), plus which of these calls are admitted in each table state. The most surprising facts are an invisible ~31 min TTL cooldown that surfaces as a terminal ValidationException, a PITR enable that is still accepted on a DELETING table and leaves an undeletable 35-day SYSTEM backup, and a Contributor Insights FAILED state that never recovers by itself ([DDB-TABLE-143](#ddb-table-143), [DDB-TABLE-453](#ddb-table-453), [DDB-TABLE-338](#ddb-table-338)). The resource policy, the Kinesis streaming destination and Application Auto Scaling have their own document, table-policy-kinesis-autoscaling.md.

### Rules a reconciler must respect
- TTL: a second UpdateTimeToLive within ~31 min of a successful change is ValidationException 'Time to live has been modified multiple times within a fixed interval' although DescribeTimeToLive already shows the settled status; re-enabling the same attribute is 'TimeToLive is already enabled', another attribute 'active on a different AttributeName', disabling a never-configured table 'TimeToLive is already disabled' (all ValidationException; these content checks run before the cooldown check) and TTL on the partition key is accepted - a rename is disable -> cooldown -> enable ([DDB-TABLE-143](#ddb-table-143), [DDB-TABLE-147](#ddb-table-147), [DDB-TABLE-146](#ddb-table-146)).
- DescribeTimeToLive cannot tell never-configured from disabled ({TimeToLiveStatus: DISABLED}, no AttributeName, so the previous attribute is unreadable), never showed ENABLING/DISABLING at 1 s polling, and the Update echoes the request shape (TimeToLiveSpecification{Enabled,AttributeName}) rather than the Describe shape (TimeToLiveDescription{TimeToLiveStatus}); on a CREATING or DELETING table it is ValidationException 'Cannot describe time to live while table is in <state> state', on a missing table ResourceNotFoundException while the ContinuousBackups pair says TableNotFoundException; 30 back-to-back DescribeTimeToLive calls throttle from the 5th ('Rate exceeded') while DescribeTable and ListTagsOfResource never did ([DDB-TABLE-144](#ddb-table-144), [DDB-TABLE-145](#ddb-table-145), [DDB-TABLE-360](#ddb-table-360); [DDB-TABLE-115](service.md#ddb-table-115), [DDB-TABLE-118](service.md#ddb-table-118), service.md; [DDB-TABLE-014](table-policy-kinesis-autoscaling.md#ddb-table-014), table-policy-kinesis-autoscaling.md).
- PITR: UpdateContinuousBackups is synchronous (the response already says ENABLED) and idempotent, but a disable/enable flap resets EarliestRestorableDateTime and LatestRestorableDateTime = max(enable time, now - 300 s); ContinuousBackupsStatus is NOT the PITR flag (DISABLED for ~3 s after ACTIVE, then ENABLED on its own, still ENABLED after a PITR disable - read PointInTimeRecoveryDescription.PointInTimeRecoveryStatus); RecoveryPeriodInDays defaults to 35, is changeable in place (1-35), rejected together with Enabled=false and reverts to 35 on re-enable ([DDB-TABLE-085](#ddb-table-085), [DDB-TABLE-385](#ddb-table-385), [DDB-TABLE-083](#ddb-table-083), [DDB-TABLE-111](#ddb-table-111), [DDB-TABLE-084](#ddb-table-084)).
- Never enable PITR on a DELETING table, and disable it before deleting: UpdateContinuousBackups(enable) is accepted (200 ENABLED) for ~1 s after DeleteTable and, like deleting any PITR table, creates the undeletable 35-day SYSTEM backup '<table>$DeletedTableBackup' (DeleteBackup is ValidationException 'not allowed to delete the system backup'); a disable issued while DELETING suppresses it; ExportTableToPointInTime on a DELETING source is likewise accepted in the first ~1 s and completes after the table is gone ([DDB-TABLE-453](#ddb-table-453), [DDB-TABLE-167](#ddb-table-167), [DDB-TABLE-088](#ddb-table-088), [DDB-TABLE-454](#ddb-table-454)).
- Contributor Insights: ENABLE returns ENABLING synchronously and settles in 1-16 s, DISABLE ~2 s, no cooldown; ENABLE/DISABLE on a settled state (or DISABLE on a never-configured table) is a 200 no-op echoing the settled status; a mode change is a re-ENABLE (ENABLING again), ENABLE without a mode reverts an out-of-band mode to ACCESSED_AND_THROTTLED_KEYS, and narrowing the mode at the quota is applied in place (4 -> 2 rules, same timestamp suffix) ([DDB-TABLE-090](#ddb-table-090), [DDB-TABLE-091](#ddb-table-091), [DDB-TABLE-341](#ddb-table-341); [DDB-TABLE-092](table-streams-encryption-class.md#ddb-table-092), table-streams-encryption-class.md).
- Past the CloudWatch quota of 100 Contributor Insights rules per region (2 rules per hash-only table or GSI, 4 with a range key) ENABLE is still 200 ENABLING, then FAILED ~2 s later with FailureException LimitExceededException and no rules created; FAILED never recovers by itself (45 polls after quota was freed), a re-ENABLE while exhausted fails again in 2 s without an error code, and only DISABLE (-> DISABLED in ~2 s, FailureException cleared) or a re-ENABLE after freeing quota leaves it - the controller's hooks catalog requeues while ENABLING/DISABLING (isTableContributorInsightsUpdating, hooks.go:140-147) but has no FAILED branch ([GT-DDB-050](service.md#gt-ddb-050) (controller hooks catalog entry)) ([DDB-TABLE-337](#ddb-table-337), [DDB-TABLE-338](#ddb-table-338), [DDB-TABLE-339](#ddb-table-339)).
- Insights state is per table AND per GSI (independent transitions, each consuming quota): Describe/Update with an LSI name, an unknown index or a missing table are ResourceNotFoundException, and on a CREATING GSI the index update is ResourceNotFoundException 'Index not found' for ~20 s and then ValidationException 'IndexStatus must be ACTIVE'; a never-configured table reads {TableName, ContributorInsightsStatus: DISABLED} (LastUpdateDateTime and ContributorInsightsMode appear only after a cycle); ListContributorInsights omits DISABLED entries but lists DISABLING and FAILED ones (fresh table -> empty page, table before GSIs, no trailing NextToken), Describe on FAILED omits ContributorInsightsRuleList, and the account-wide list with MaxResults returns dozens of EMPTY pages that each carry a NextToken - never stop at an empty page ([DDB-TABLE-094](#ddb-table-094), [DDB-TABLE-116](#ddb-table-116), [DDB-TABLE-089](#ddb-table-089), [DDB-TABLE-095](#ddb-table-095), [DDB-TABLE-343](#ddb-table-343), [DDB-TABLE-340](#ddb-table-340), [DDB-TABLE-344](#ddb-table-344)).
- No pre-delete cleanup is needed for Insights: DeleteTable with insights ENABLED removes the table and its CloudWatch rules together (~6 s), deleting a GSI with insights ENABLED is accepted and leaves no orphan in the list, Insights calls on a DELETING table still return 200 (Describe ENABLED, DISABLE -> DISABLING) and ResourceNotFoundException once it is gone, and a caller with dynamodb:* only (no cloudwatch permissions) enables Insights fine - the documented CloudWatch prerequisite is false ([DDB-TABLE-342](#ddb-table-342), [DDB-TABLE-112](#ddb-table-112), [DDB-TABLE-097](#ddb-table-097), [DDB-TABLE-345](#ddb-table-345)).
- Admission by table state: while CREATING every sub-resource API is ResourceNotFound/TableNotFound for the first ~1-5 s, then UpdateTimeToLive is ResourceInUseException, UpdateContinuousBackups/CreateBackup ContinuousBackupsUnavailableException 'Backups are being enabled' and Insights ValidationException 'TableStatus must be ACTIVE' until ACTIVE (while a replica is CREATING on a non-empty table, ~11 min, UpdateTimeToLive is ValidationException 'not allowed while the replica is being added' because TTL is group-wide); while UPDATING (stream toggle, TableClass switch) TTL/PITR/Insights are admitted and complete normally, and six write APIs fired within 0.12 s on an ACTIVE table all land; while DELETING, TTL/PITR/Insights updates and CreateBackup return 200 for the first ~1-1.6 s; in INACCESSIBLE_ENCRYPTION_CREDENTIALS PITR works but TTL/backup/Insights fail; a resource-policy Deny on dynamodb:UpdateTable leaves TTL/PITR untouched ([DDB-TABLE-116](#ddb-table-116), [DDB-TABLE-097](#ddb-table-097), [DDB-TABLE-453](#ddb-table-453); [DDB-TABLE-115](service.md#ddb-table-115), [DDB-TABLE-118](service.md#ddb-table-118), [DDB-TABLE-374](service.md#ddb-table-374), service.md; [DDB-TABLE-306](table-policy-kinesis-autoscaling.md#ddb-table-306), [DDB-TABLE-460](table-policy-kinesis-autoscaling.md#ddb-table-460), [DDB-TABLE-122](table-policy-kinesis-autoscaling.md#ddb-table-122), [DDB-TABLE-432](table-policy-kinesis-autoscaling.md#ddb-table-432), table-policy-kinesis-autoscaling.md; [DDB-TABLE-119](table-streams-encryption-class.md#ddb-table-119), [DDB-TABLE-435](table-streams-encryption-class.md#ddb-table-435), [DDB-TABLE-333](table-streams-encryption-class.md#ddb-table-333), table-streams-encryption-class.md).
- Three GSI entries are rendered here only because their operations include a sub-resource API: LSIs are create-only (UpdateTable has no LSI member), an unknown IndexName in UpdateTable is ResourceNotFoundException with an 'Index' message (WarmThroughput on a ghost index is HTTP 500), and the admission rules while a GSI is CREATING/DELETING; the index rules proper are in table-indexes.md ([DDB-TABLE-137](#ddb-table-137), [DDB-TABLE-161](#ddb-table-161), [DDB-TABLE-163](#ddb-table-163)).

### Timing you should expect
- TTL cooldown: rejected at 8.1 s, still rejected at 1811.6 s, first success 1871.7 s after the enable (60 s retry cadence, 1 table); the settled status is visible within 2.1 s and no ENABLING/DISABLING was observed on 5 tables at 1 s polling ([DDB-TABLE-143](#ddb-table-143), [DDB-TABLE-145](#ddb-table-145), [DDB-TABLE-144](#ddb-table-144)).
- PITR: on a fresh table DescribeContinuousBackups is TableNotFoundException for ~3.1 s and ContinuousBackupsStatus DISABLED until ~10.3 s (table ACTIVE at 7.2 s); the first UpdateContinuousBackups/CreateBackup succeed 1.4-2.7 s after ACTIVE while TTL/Insights succeed within 0.1 s; the enable-while-DELETING window is +0.03..+1.03 s (TableNotFoundException from +1.23 s while DescribeTable says DELETING until +5.2 s) and the SYSTEM backup is listed 5-8 s after DeleteTable ([DDB-TABLE-111](#ddb-table-111), [DDB-TABLE-116](#ddb-table-116), [DDB-TABLE-453](#ddb-table-453), [DDB-TABLE-167](#ddb-table-167); [DDB-TABLE-115](service.md#ddb-table-115), service.md).
- Insights: ENABLING 1.01-16.19 s (n=6; 1.05-3.07 s over 25 tables), DISABLING 2.02-2.03 s (n=3); ENABLE -> FAILED in 2.02 s at the quota, re-ENABLE after freeing quota settles in 2.16 s, DISABLE of a FAILED entry 2.16 s, mode change 2.14 s; a DISABLED entry leaves the list the moment Describe flips (3.08 s); table and rules gone 6.1 s after DeleteTable ([DDB-TABLE-090](#ddb-table-090), [DDB-TABLE-337](#ddb-table-337), [DDB-TABLE-338](#ddb-table-338), [DDB-TABLE-339](#ddb-table-339), [DDB-TABLE-341](#ddb-table-341), [DDB-TABLE-343](#ddb-table-343), [DDB-TABLE-342](#ddb-table-342)).

### Known handling gaps in the controller
- The hooks catalog records that ValidationException is terminal and only the 'already disabled' TTL text is tolerated (pkg/resource/table/hooks_ttl.go:85-92, hooks.go:210-218, generator.yaml:88-90; [GT-DDB-044](service.md#gt-ddb-044) (controller hooks catalog entry)); confirmed: any TTL edit within ~31 min of the previous change, including the controller's own post-create enable, parks the CR in Terminal until the spec changes again, and an attribute rename (disable -> cooldown -> enable) cannot complete ([DDB-TABLE-143](#ddb-table-143)). No other finding in this document is stored as suspect-bug or partial; the Insights FAILED branch and the PITR-on-DELETING hazard above are stored as unhandled.

### Where to look next
- The resource policy, Kinesis destination and Application Auto Scaling rules that used to share this document, including the policy/Kinesis admission matrix ([DDB-TABLE-234](table-policy-kinesis-autoscaling.md#ddb-table-234), [DDB-TABLE-236](table-policy-kinesis-autoscaling.md#ddb-table-236), table-policy-kinesis-autoscaling.md); the DP cooldown and tag lock that share the ThrottlingException retry-after format ([DDB-TABLE-445](service.md#ddb-table-445), [DDB-TABLE-464](service.md#ddb-table-464), service.md); INACCESSIBLE_ENCRYPTION_CREDENTIALS, where PITR works but TTL/backup/Insights fail ([DDB-TABLE-333](table-streams-encryption-class.md#ddb-table-333), table-streams-encryption-class.md); the SYSTEM backup on delete of a PITR table and what a restore drops (backup.md, table-restore.md). Evidence: services/dynamodb/probes/table/{sub-resources/{pitr-lifecycle,insights-lifecycle,insights-list-ghosts},mutation-matrix/ttl-updates,consistency-windows/continuous-backups-window,state-machine/subresource-admissibility,limits/insights-rule-quota,creative/{pitr-delete-system-backup,xs-insights-cw-quota}}/.

Entries below are generated from the lab findings; low-impact items are in the appendix, long notes under details/.
<!-- preserved:end -->

## At a glance

- canonical findings: 38 (high 16 / medium 17 / low 5); duplicates folded into the appendix: 3
- handling: handled 14 · partial 0 · tracked 0 · unhandled 21 · suspect-bug 1 · n-a 2 (tracked = handled/partial whose reference is an open GitHub issue; counted as not handled)
- re-verified: 4 · last_verified: 2026-10-08..2026-10-09 · model: 2012-08-10 (service/dynamodb v1.39.8)
- categories: async-state-machine 7, delete-semantics 6, response-fidelity 5, idempotency 4, other 3,
  error-code 2, read-gap 2, shape-mismatch 2, eventual-consistency 1, immutable-field 1, prerequisite 1,
  quota-limit 1, request-validation 1, scope 1, server-default 1

## Operations

| operation | kind | required inputs | declared error shapes | paginated |
| --- | --- | --- | --- | --- |
| CreateTable | create | TableName | ResourceInUseException, LimitExceededException, InternalServerError | no |
| DeleteBackup | delete | BackupArn | BackupNotFoundException, BackupInUseException, LimitExceededException, InternalServerError | no |
| DeleteTable | delete | TableName | ResourceInUseException, ResourceNotFoundException, LimitExceededException, InternalServerError | no |
| DescribeContinuousBackups | read | TableName | TableNotFoundException, InternalServerError | no |
| DescribeContributorInsights | read | TableName | ResourceNotFoundException, InternalServerError | no |
| DescribeExport | read | ExportArn | ExportNotFoundException, LimitExceededException, InternalServerError | no |
| DescribeTable | read | TableName | ResourceNotFoundException, InternalServerError | no |
| DescribeTimeToLive | read | TableName | ResourceNotFoundException, InternalServerError | no |
| ExportTableToPointInTime | other | TableArn, S3Bucket | TableNotFoundException, PointInTimeRecoveryUnavailableException, LimitExceededException, InvalidExportTimeException, ExportConflictException, InternalServerError | no |
| ListBackups | list | - | InternalServerError | yes |
| ListContributorInsights | list | - | ResourceNotFoundException, InternalServerError | no |
| ListTagsOfResource | list | ResourceArn | ResourceNotFoundException, InternalServerError | yes |
| Query | list | TableName | ProvisionedThroughputExceededException, ResourceNotFoundException, RequestLimitExceeded, InternalServerError, ThrottlingException | yes |
| RestoreTableToPointInTime | create | TargetTableName | TableAlreadyExistsException, TableNotFoundException, TableInUseException, LimitExceededException, InvalidRestoreTimeException, PointInTimeRecoveryUnavailableException, InternalServerError | no |
| TagResource | tag | ResourceArn, Tags | LimitExceededException, ResourceNotFoundException, InternalServerError, ResourceInUseException | no |
| UpdateContinuousBackups | update | TableName, PointInTimeRecoverySpecification | TableNotFoundException, ContinuousBackupsUnavailableException, InternalServerError | no |
| UpdateContributorInsights | update | TableName, ContributorInsightsAction | ResourceNotFoundException, InternalServerError | no |
| UpdateTable | update | TableName | ResourceInUseException, ResourceNotFoundException, LimitExceededException, InternalServerError | no |
| UpdateTimeToLive | update | TableName, TimeToLiveSpecification | ResourceInUseException, ResourceNotFoundException, LimitExceededException, InternalServerError | no |

Also referenced by findings but not in the dynamodb model (other services or annotated variants):
DescribeInsightRules

## State machine

- **ContinuousBackupsStatus**: ENABLED, DISABLED
- **ContributorInsightsStatus**: ENABLING, ENABLED, DISABLING, DISABLED, FAILED (transitional: ENABLING,
  DISABLING)
- **ExportStatus**: IN_PROGRESS, COMPLETED, FAILED (transitional: IN_PROGRESS)
- **IndexStatus**: CREATING, UPDATING, DELETING, ACTIVE (transitional: CREATING, UPDATING, DELETING)
- **PointInTimeRecoveryStatus**: ENABLED, DISABLED
- **ReplicaStatus**: CREATING, CREATION_FAILED, UPDATING, DELETING, ACTIVE, REGION_DISABLED,
  INACCESSIBLE_ENCRYPTION_CREDENTIALS, ARCHIVING, ARCHIVED, REPLICATION_NOT_AUTHORIZED (transitional:
  CREATING, UPDATING, DELETING, ARCHIVING)
- **SSEStatus**: ENABLING, ENABLED, DISABLING, DISABLED, UPDATING (transitional: ENABLING, DISABLING,
  UPDATING)
- **TableStatus**: CREATING, UPDATING, DELETING, ACTIVE, INACCESSIBLE_ENCRYPTION_CREDENTIALS, ARCHIVING,
  ARCHIVED, REPLICATION_NOT_AUTHORIZED (transitional: CREATING, UPDATING, DELETING, ARCHIVING)
- **TimeToLiveStatus**: ENABLING, DISABLING, ENABLED, DISABLED (transitional: ENABLING, DISABLING)
- **WitnessStatus**: CREATING, DELETING, ACTIVE (transitional: CREATING, DELETING)

- <a id="ddb-table-090"></a>**DDB-TABLE-090** `async-state-machine` · impact high · handled · verified 2026-10-08
  **UpdateContributorInsights(ENABLE) returns ENABLING synchronously; ENABLING settles in 1-16 s, DISABLING in ~2 s; no cooldown between toggles**
  ENABLE response: {'TableName': 'ackq-244c78-i1', 'ContributorInsightsStatus': 'ENABLING',
  'ContributorInsightsMode': 'ACCESSED_AND_THROTTLED_KEYS'}. Describe timeline: [{'value': 'ENABLING',
  'from_s': 0.01, 'to_s': 16.2, 'duration_s': 16.19}, {'value': 'ENABLED', 'from_s': 16.2, 'to_s': None,
  'duration_s': None}] (final ENABLED). DISABLE timeline: [{'value': 'DISABLING', 'from_s': 0.01, 'to_s':
  2.03, 'duration_s': 2.02}, {'value': 'DISABLED', 'from_s': 2.03, 'to_s': None, 'duration_s': None}].
  TableStatus after ENABLE: ACTIVE. ENABLE immediately after DISABLED -> 200 OK status=ENABLING (no cooldown).
  ENABLING durations observed (s): [16.19, 1.01, 1.02, 1.02, 1.01, 1.02]; DISABLING: [2.02, 2.02, 2.03].
  - ACK: requeue, synced.when · ops: UpdateContributorInsights, DescribeContributorInsights · fields:
    ContributorInsightsStatus
  - repro: UpdateContributorInsights(ENABLE) -> poll DescribeContributorInsights at 1s
  - measurements: enabling_s=[16.19, 1.01, 1.02, 1.02, 1.01, 1.02], disabling_s=[2.02, 2.02, 2.03]
  - handling: handled via `generator.yaml:78-83; pkg/resource/table/hooks.go:882-960; pkg/resource/table/hooks.go:913-930; pkg/resource/table/hooks.go:733-744; pkg/resource/table/hooks.go:140-147; templates/hooks/table/sdk_read_one_post_set_output.go.tpl:63-68`
  - related: [DDB-TABLE-089](#ddb-table-089), [DDB-TABLE-091](#ddb-table-091), [DDB-TABLE-092](table-streams-encryption-class.md#ddb-table-092), [DDB-TABLE-341](#ddb-table-341), [DDB-TABLE-339](#ddb-table-339), [DDB-TABLE-343](#ddb-table-343),
    [DDB-TABLE-340](#ddb-table-340) · evidence: table/sub-resources/insights-lifecycle
  - notes: Hypotheses: H-S-005. FAILED path (IAM/quota induced) not tested: requires a restricted principal or
    exhausting the CloudWatch rule quota.

- <a id="ddb-table-116"></a>**DDB-TABLE-116** `async-state-machine` · impact high · handled · verified 2026-10-08
  **GSI table: backups usable 1.4s after ACTIVE; insights on a CREATING GSI: RNF 'Index not found' ~20s, then 'IndexStatus must be ACTIVE'**
  GSI table CREATING (16.1s): DescribeContinuousBackups [('TableNotFoundException', 400, 'Table not found:
  ackq-34ad39-s2'), ('OK', 200, '')]; UpdateContinuousBackups [('TableNotFoundException', 400, 'Table not
  found: ackq-34ad39-s2'), ('ContinuousBackupsUnavailableException', 400, 'Backups are being enabled for the
  table: ackq-34ad39-s2. Please retry later')]; UpdateContributorInsights(table)
  [('ResourceNotFoundException', 400, 'Requested resource not found: Table: ackq-34ad39-s2 not found'),
  ('ValidationException', 400, 'Table or Index is not in a valid state to update Key Access Insights:
  TableStatus must be ACTIVE to enable ContributorIn')]; UpdateContributorInsights(index)
  [('ResourceNotFoundException', 400, 'Requested resource not found: Table: ackq-34ad39-s2 not found'),
  ('ValidationException', 400, 'Table or Index is not in a valid state to update Key Access Insights:
  TableStatus must be ACTIVE to enable ContributorIn')]; DescribeContributorInsights(index)
  [('ResourceNotFoundException', 400, 'Requested resource not found: Table: ackq-34ad39-s2 not found'), ('OK',
  200, '')]. First success after ACTIVE (s): {'DescribeContinuousBackups': None,
  'DescribeContributorInsights': None, 'ListContributorInsights': None, 'DescribeContributorInsights(index)':
  None, 'DescribeTimeToLive': 0.0, 'UpdateContributorInsights': 0.1, 'UpdateContributorInsights(index)': 0.1,
  'UpdateContinuousBackups': 1.4}; never succeeded: []. During UpdateTable(Create gsi2) backfill
  (TableStatus=UPDATING): {'DescribeTimeToLive': 'OK', 'UpdateTimeToLive': 'OK', 'DescribeContinuousBackups':
  'OK', 'UpdateContinuousBackups': 'OK', 'DescribeContributorInsights': 'OK', 'UpdateContributorInsights':
  'OK', 'ListContributorInsights': 'OK', 'DescribeContributorInsights(index)': "ResourceNotFoundException
  (HTTP 400) 'Requested resource not found: Index: gsi2 not found for table: ackq-34ad39-s2'",
  'UpdateContributorInsights(index)': "ResourceNotFoundException (HTTP 400) 'Requested resource not found:
  Index: gsi2 not found for table: ackq-34ad39-s2'"}; index ops series: [{'t_s': 1.7, 'table': 'UPDATING',
  'gsi2': ['CREATING'], 'UpdateCI(index)': {'ok': False, 'code': 'ResourceNotFoundException', 'http': 400,
  'message': 'Requested resource not found: Index: gsi2 not found for table: ackq-34ad39-s2'},
  'DescribeCI(index)': {'ok': False, 'code': 'ResourceNotFoundException', 'http': 400, 'message': 'Requested
  resource not found: Index: gsi2 not found for table: ackq-34ad39-s2'}, 'ListCI': {'ok': True, 'code': None,
  'http': 200, 'message': ''}}, {'t_s': 21.8, 'table': 'UPDATING', 'gsi2': ['CREATING'], 'UpdateCI(index)':
  {'ok': False, 'code': 'ValidationException', 'http': 400, 'message': 'Table or Index is not in a valid state
  to update Key Access Insights: IndexStatus must be ACTIVE to enable ContributorInsights.'},
  'DescribeCI(index)': {'ok': True, 'code': None, 'http': 200, 'message': ''}, 'ListCI': {'ok': True, 'code':
  None, 'http': 200, 'message': ''}}]. After gsi2 ACTIVE: {'UpdateContributorInsights(index gsi2 ACTIVE)':
  {'ok': True, 'code': None, 'http': 200, 'message': ''}, 'DescribeContributorInsights(index gsi2)': {'ok':
  True, 'code': None, 'http': 200, 'message': ''}, 'ttl_after_updating_window': {'TimeToLiveStatus':
  'ENABLED', 'AttributeName': 'ttl'}}. GSI add timeline: [{'value': ('ACTIVE', ('ACTIVE', 'CREATING')),
  'from_s': 0.01, 'to_s': 468.55, 'duration_s': 468.54}, {'value': ('ACTIVE', ('ACTIVE', 'ACTIVE')), 'from_s':
  468.55, 'to_s': None, 'duration_s': None}].
  - ACK: synced.when, requeue, terminal_codes · ops: UpdateContributorInsights, DescribeContributorInsights,
    UpdateContinuousBackups, UpdateTimeToLive, UpdateTable
  - repro: CreateTable with GSI; loop ops; UpdateTable Create gsi2; ops during backfill
  - measurements: gsi_table_create_to_active_s=16.1, cb_update_first_ok_after_active_s=1.4,
    insights_index_update_first_ok_after_active_s=0.1, gsi_add_table_updating_s=42,
    gsi_add_index_creating_s=468.5
  - handling: handled via `generator.yaml:104-109; pkg/resource/table/hooks.go:72-93; generator.yaml:46-50; pkg/resource/table/hooks_continuous_backup.go:27-94; generator.yaml:78-83; pkg/resource/table/hooks.go:882-960; templates/hooks/table/sdk_create_post_set_output.go.tpl:1-4; bcd26e1`
  - related: [DDB-TABLE-148](table-indexes.md#ddb-table-148), [DDB-TABLE-138](table-indexes.md#ddb-table-138), [DDB-TABLE-149](table-indexes.md#ddb-table-149), [DDB-TABLE-163](#ddb-table-163), [DDB-TABLE-458](table-indexes.md#ddb-table-458), [DDB-TABLE-165](table-indexes.md#ddb-table-165),
    [DDB-TABLE-168](table-indexes.md#ddb-table-168), [DDB-TABLE-169](table-indexes.md#ddb-table-169), [DDB-TABLE-123](table-indexes.md#ddb-table-123), [DDB-TABLE-134](table-indexes.md#ddb-table-134), [DDB-TABLE-161](#ddb-table-161), [DDB-TABLE-380](table-indexes.md#ddb-table-380), [DDB-TABLE-094](#ddb-table-094),
    [DDB-TABLE-137](#ddb-table-137), [DDB-TABLE-095](#ddb-table-095), [DDB-TABLE-344](#ddb-table-344), [DDB-TABLE-093](#ddb-table-093), [DDB-TABLE-112](#ddb-table-112) · evidence:
    table/state-machine/subresource-admissibility
  - notes: Hypotheses: H-S-025, H-S-015, H-S-103. Hypotheses: H-S-025 (partially confirmed: with the TABLE
    CREATING, index-level Update/Describe return the same codes as table-level, i.e. ResourceNotFoundException
    'Table: X not found' then ValidationException 'TableStatus must be ACTIVE'; with the table...
  - full notes: [details/DDB-TABLE-116.md](details/DDB-TABLE-116.md)

- <a id="ddb-table-143"></a>**DDB-TABLE-143** `async-state-machine` · impact high · SUSPECTED CONTROLLER BUG · verified 2026-10-09, re-verified
  **TTL cooldown is real and ~31 min: 2nd UpdateTimeToLive -> ValidationException even though Describe shows ENABLED; invisible in Describe**
  UpdateTimeToLive(Enabled=false, same attr) issued 8.1 s after a successful enable - DescribeTimeToLive
  already ENABLED (ENABLING was never observed at 1 s polling) - failed with ValidationException (HTTP 400)
  'Time to live has been modified multiple times within a fixed interval'. Retried every 60 s: still rejected
  at 1811 s, first 200 at 1872 s after the enable (~31 min, not the documented hour). The successful disable
  immediately started a new cooldown: UpdateTimeToLive(Enabled=true) right after it -> the same
  ValidationException. Failed attempts do not seem to extend the window. Nothing in DescribeTimeToLive (only
  TimeToLiveStatus/AttributeName) exposes the cooldown. Content errors are checked before the cooldown:
  'TimeToLive is already enabled' / 'already disabled' / 'active on a different AttributeName' are returned
  instead of the cooldown message.
  - ACK: requeue, terminal_codes · ops: UpdateTimeToLive, DescribeTimeToLive · fields: TimeToLiveSpecification
  - repro: UpdateTimeToLive(Enabled=true) -> poll DescribeTimeToLive until ENABLED ->
    UpdateTimeToLive(Enabled=false) -> retry every 60s
  - measurements: cooldown_until_success_s=1871.7, last_rejection_s=1811.6, first_rejection_s=8.1,
    retry_interval_s=60, enabling_state_observed=false
  - handling: suspected controller bug - see Handling gaps
  - related: [DDB-TABLE-144](#ddb-table-144), [DDB-TABLE-145](#ddb-table-145), [DDB-TABLE-146](#ddb-table-146), [DDB-TABLE-147](#ddb-table-147), [DDB-TABLE-114](service.md#ddb-table-114), [DDB-TABLE-377](service.md#ddb-table-377) ·
    evidence: table/mutation-matrix/ttl-updates, table/creative/reverify-set-a1
  - notes: Hypotheses: H-S-001, H-S-043, H-S-009. Hypotheses: H-S-001 confirmed (cooldown; but ~31 min not
    60), H-S-043 refuted (settled status does not unlock), H-S-009 confirmed (ValidationException is not a
    declared UpdateTimeToLive error shape).
  - full notes: [details/DDB-TABLE-143.md](details/DDB-TABLE-143.md)

- <a id="ddb-table-163"></a>**DDB-TABLE-163** `async-state-machine` · impact high · handled · verified 2026-10-08
  **Admissibility while a GSI is CREATING or DELETING: table IOPS/stream changes rejected in resource allocation, IOPS allowed once backfilling**
  Resource-allocation phase (TableStatus=UPDATING, IndexStatus=CREATING, Backfilling=false): table
  ProvisionedThroughput -> ResourceInUseException "Can't change table IOPS when an index is being created.
  Table: X Indexes: [gsi4]"; StreamSpecification -> "Can't change stream status when an index is being
  created..."; Update of the creating index -> 'Index is being created but is not backfilling yet. Table: X
  Index: gsi4'; Update of another GSI -> 200. Backfilling phase (TableStatus=ACTIVE, Backfilling=true): table
  ProvisionedThroughput -> 200 (table goes UPDATING), GSI Update of another index -> 200,
  DeletionProtectionEnabled -> 200, UpdateTimeToLive -> 200, TagResource -> 200 (a value with '|' was rejected
  for format only), another Create -> LimitExceededException, stream change right after the accepted IOPS
  change -> "Can't enable or disable stream while table IOPS are being updated". While an index is DELETING:
  table ProvisionedThroughput -> "Can't change table IOPS when an index is being deleted. Table: X Indexes:
  [gsi1]"; Create with the same name -> ResourceInUseException 'Index is being deleted. Table: X Index: gsi1'
  (also for a second Delete of it); Create/Delete of another index -> LimitExceededException; GSI Update of
  another index -> 200.
  - ACK: updateable.when, requeue · ops: UpdateTable, UpdateTimeToLive, TagResource · fields:
    GlobalSecondaryIndexUpdates, ProvisionedThroughput, StreamSpecification
  - repro: UpdateTable Create GSI; immediately UpdateTable ProvisionedThroughput; retry after Backfilling=true
  - handling: handled via `pkg/resource/table/hooks.go:226-238; pkg/resource/table/hooks_global_secondary_indexes.go:202-217`
  - related: [DDB-TABLE-148](table-indexes.md#ddb-table-148), [DDB-TABLE-138](table-indexes.md#ddb-table-138), [DDB-TABLE-149](table-indexes.md#ddb-table-149), [DDB-TABLE-458](table-indexes.md#ddb-table-458), [DDB-TABLE-116](#ddb-table-116), [DDB-TABLE-165](table-indexes.md#ddb-table-165),
    [DDB-TABLE-150](table-indexes.md#ddb-table-150), [DDB-TABLE-376](table-indexes.md#ddb-table-376), [DDB-TABLE-159](table-indexes.md#ddb-table-159), [DDB-TABLE-135](table-indexes.md#ddb-table-135), [DDB-TABLE-375](table-indexes.md#ddb-table-375), [DDB-TABLE-462](table-indexes.md#ddb-table-462), [DDB-TABLE-152](table-indexes.md#ddb-table-152),
    [DDB-TABLE-166](table-indexes.md#ddb-table-166), [DDB-TABLE-286](table-streams-encryption-class.md#ddb-table-286), [DDB-TABLE-175](table-indexes.md#ddb-table-175), [DDB-TABLE-174](table-indexes.md#ddb-table-174), [DDB-TABLE-382](table-streams-encryption-class.md#ddb-table-382) · evidence:
    table/mutation-matrix/gsi-update-granularity
  - notes: Confirms H-T-005 for ProvisionedThroughput; refutes it for StreamSpecification during the same
    window only because the IOPS change was in flight.

- <a id="ddb-table-337"></a>**DDB-TABLE-337** `async-state-machine` · impact high · unhandled (not handled in controller) · verified 2026-10-09
  **Insights ENABLE past the CW rule quota (100): 200 ENABLING, then FAILED in ~2s, FailureException LimitExceededException; no partial rules**
  Account quota 'Number of Contributor Insights rules' (service-quotas monitoring/L-DBD11BCC) = 100.0. Each
  ENABLE on a hash-only table created 2 CloudWatch rules, each hash+range table 4 (deltas [(2, 2), (4, 4)]),
  all settling ENABLING->ENABLED in 1.05-3.07 s. At 98/100 rules, ENABLE on a 4-rule table was still accepted
  synchronously (200, ContributorInsightsStatus=ENABLING, mode echoed) but DescribeContributorInsights showed
  FAILED 2.02s later with FailureException={ExceptionName: 'LimitExceededException', ExceptionDescription:
  'Amazon CloudWatch Contributor Insights rule limit reached. Please disable Contributor Insights for other
  tables/indexes OR disable other CloudWatch Contributor Insights rules before retrying.'}; the response has
  no ContributorInsightsRuleList key and keeps ContributorInsightsMode. Rule creation is atomic: the
  CloudWatch count stayed 98 (no rule named after the table) and a 2-rule table enabled right afterwards went
  ENABLED (98->100), so a FAILED attempt consumes no quota. FAILED stayed FAILED for 20 s.
  - ACK: terminal_codes, synced.when, requeue · ops: UpdateContributorInsights, DescribeContributorInsights ·
    fields: ContributorInsightsStatus, FailureException, ContributorInsightsRuleList
  - repro: 25 hash+range PPR tables: UpdateContributorInsights(ENABLE) each -> 98 rules (1 hash-only + 24);
    ENABLE a 26th hash+range table; DescribeContributorInsights at 1/s
  - measurements: quota=100.0, rules_before_failed=98, enabling_s_before_failed=2.02,
    enable_settle_s_min=1.05, enable_settle_s_max=3.07, tables_enabled=25, create_31_tables_all_active_s=8.8
  - handling: not handled in the controller (as of commit 34b85e6)
  - related: [DDB-TABLE-089](#ddb-table-089), [DDB-TABLE-090](#ddb-table-090), [DDB-TABLE-092](table-streams-encryption-class.md#ddb-table-092), [DDB-TABLE-093](#ddb-table-093), [DDB-TABLE-095](#ddb-table-095), [DDB-TABLE-097](#ddb-table-097),
    [DDB-TABLE-439](#ddb-table-439), [DDB-TABLE-338](#ddb-table-338), [DDB-TABLE-339](#ddb-table-339), [DDB-TABLE-340](#ddb-table-340), [DDB-TABLE-341](#ddb-table-341), [DDB-TABLE-342](#ddb-table-342) · hypotheses:
    H-S-127, H-S-022 · evidence: table/limits/insights-rule-quota
  - notes: H-S-127 LimitExceededException clause CONFIRMED (ExceptionName is the CloudWatch error name,
    description is a DynamoDB-authored sentence, not the raw CloudWatch message). The quota is account-wide
    across tables and GSIs: 25 sort-keyed tables (or ~50 hash-only ones) exhaust it; the sync response...
  - full notes: [details/DDB-TABLE-337.md](details/DDB-TABLE-337.md)

- <a id="ddb-table-338"></a>**DDB-TABLE-338** `async-state-machine` · impact high · unhandled (not handled in controller) · verified 2026-10-09
  **FAILED insights are sticky: no auto-recovery when quota frees (45s); re-ENABLE exhausted -> FAILED in 2s; after freeing -> ENABLED in 2s**
  On a FAILED entry (quota still exhausted) UpdateContributorInsights(ENABLE) -> 200 ENABLING, then FAILED
  again after 2.02s with the same LimitExceededException (idempotent retry, no error code). Disabling another
  table's insights (-4 rules, 94 rules left) did NOT change the FAILED entry: 45/45 one-second polls still
  FAILED, FailureException unchanged. A new ENABLE after freeing quota -> ENABLING -> ENABLED in 2.16s,
  FailureException gone, ContributorInsightsRuleList back (4 rules with a NEW timestamp suffix).
  - ACK: requeue, custom_update, terminal_codes · ops: UpdateContributorInsights, DescribeContributorInsights
    · fields: ContributorInsightsStatus, FailureException
  - repro: FAILED entry: ENABLE again -> FAILED; DISABLE insights on another table; poll Describe 45 s; ENABLE
    -> ENABLED
  - measurements: auto_recovery_polls_s=45, reenable_after_free_settle_s=2.16,
    reenable_exhausted_settle_s=2.14
  - handling: not handled in the controller (as of commit 34b85e6)
  - related: [DDB-TABLE-089](#ddb-table-089), [DDB-TABLE-090](#ddb-table-090), [DDB-TABLE-092](table-streams-encryption-class.md#ddb-table-092), [DDB-TABLE-093](#ddb-table-093), [DDB-TABLE-095](#ddb-table-095), [DDB-TABLE-097](#ddb-table-097),
    [DDB-TABLE-337](#ddb-table-337), [DDB-TABLE-439](#ddb-table-439), [DDB-TABLE-339](#ddb-table-339), [DDB-TABLE-340](#ddb-table-340), [DDB-TABLE-341](#ddb-table-341), [DDB-TABLE-342](#ddb-table-342) · hypotheses:
    H-S-127 · evidence: table/limits/insights-rule-quota
  - notes: A controller whose desired state is ENABLED must re-issue ENABLE to leave FAILED (the service never
    retries); re-issuing while the quota is still exhausted just re-fails (2 s), so back off on
    FailureException.ExceptionName=LimitExceededException instead of hot-looping.

- <a id="ddb-table-339"></a>**DDB-TABLE-339** `async-state-machine` · impact medium · unhandled (not handled in controller) · verified 2026-10-09
  **DISABLE on a FAILED insights entry: DISABLING (FailureException still shown) -> DISABLED in ~2s, FailureException cleared, leaves List**
  UpdateContributorInsights(DISABLE) on an entry in FAILED -> 200 with ContributorInsightsStatus=DISABLING and
  the mode echoed. During DISABLING Describe still carried FailureException (LimitExceededException). Final
  DISABLED after 2.16s: FailureException absent, ContributorInsightsMode retained
  (ACCESSED_AND_THROTTLED_KEYS), no ContributorInsightsRuleList; ListContributorInsights(TableName) -> 0
  summaries. CloudWatch count unchanged (98).
  - ACK: custom_delete, synced.when · ops: UpdateContributorInsights, DescribeContributorInsights,
    ListContributorInsights · fields: ContributorInsightsStatus, FailureException
  - repro: FAILED entry -> UpdateContributorInsights(DISABLE) -> Describe at 1/s
  - measurements: disabling_s=2.16
  - handling: not handled in the controller (as of commit 34b85e6)
  - related: [DDB-TABLE-089](#ddb-table-089), [DDB-TABLE-090](#ddb-table-090), [DDB-TABLE-092](table-streams-encryption-class.md#ddb-table-092), [DDB-TABLE-093](#ddb-table-093), [DDB-TABLE-095](#ddb-table-095), [DDB-TABLE-097](#ddb-table-097),
    [DDB-TABLE-091](#ddb-table-091), [DDB-TABLE-341](#ddb-table-341), [DDB-TABLE-343](#ddb-table-343), [DDB-TABLE-340](#ddb-table-340), [DDB-TABLE-337](#ddb-table-337), [DDB-TABLE-439](#ddb-table-439), [DDB-TABLE-338](#ddb-table-338),
    [DDB-TABLE-342](#ddb-table-342) · hypotheses: H-S-127, H-S-019 · evidence: table/limits/insights-rule-quota
  - notes: DISABLE is the way to clear a FAILED entry when the user gives up on insights; FailureException is
    not cleared until the status reaches DISABLED, so do not treat 'FailureException present' alone as
    'FAILED'.

## Idempotency

- <a id="ddb-table-085"></a>**DDB-TABLE-085** `idempotency` · impact high · handled · verified 2026-10-08
  **UpdateContinuousBackups is synchronous and idempotent, but a disable/enable flap resets EarliestRestorableDateTime**
  Statuses seen in 8 reads after enable: ['ENABLED'] (Update response already reported ENABLED). Enable when
  ENABLED -> 200 OK, EarliestRestorableDateTime unchanged=True. Disable when DISABLED -> 200 OK. Disable then
  enable back-to-back -> 200 OK / 200 OK; Earliest before=2026-10-08T23:05:04+00:00
  after=2026-10-08T23:05:24+00:00 (after-minus-reenable=-0.2s). TableStatus after updates: ACTIVE.
  - ACK: synced.when, custom_update · ops: UpdateContinuousBackups · fields:
    PointInTimeRecoverySpecification.PointInTimeRecoveryEnabled
  - repro: enable; enable; disable; disable; disable+enable within 1s; compare EarliestRestorableDateTime
  - measurements: flap_earliest_reset_delta_s=20
  - handling: handled via `generator.yaml:46-50; pkg/resource/table/hooks_continuous_backup.go:27-94`
  - related: [DDB-TABLE-083](#ddb-table-083), [DDB-TABLE-111](#ddb-table-111), [DDB-TABLE-115](service.md#ddb-table-115), [DDB-TABLE-084](#ddb-table-084), [DDB-TABLE-091](#ddb-table-091), [DDB-TABLE-341](#ddb-table-341),
    [DDB-TABLE-147](#ddb-table-147), [DDB-TABLE-300](table-policy-kinesis-autoscaling.md#ddb-table-300), [DDB-TABLE-207](table-policy-kinesis-autoscaling.md#ddb-table-207), [DDB-TABLE-383](table-streams-encryption-class.md#ddb-table-383), [DDB-TABLE-205](table-policy-kinesis-autoscaling.md#ddb-table-205) · evidence:
    table/sub-resources/pitr-lifecycle
  - notes: Hypotheses: H-S-014, H-S-016, H-S-106.

- <a id="ddb-table-091"></a>**DDB-TABLE-091** `idempotency` · impact medium · unhandled (not handled in controller) · verified 2026-10-08
  **Re-applying ENABLE on an ENABLED table -> 200 OK status=ENABLED; DISABLE on DISABLED -> 200 OK status=DISABLED**
  ENABLE when ENABLED: 200 OK status=ENABLED; statuses in the next 6 s: [('ENABLED',
  'ACCESSED_AND_THROTTLED_KEYS'), ('ENABLED', 'ACCESSED_AND_THROTTLED_KEYS'), ('ENABLED',
  'ACCESSED_AND_THROTTLED_KEYS'), ('ENABLED', 'ACCESSED_AND_THROTTLED_KEYS'), ('ENABLED',
  'ACCESSED_AND_THROTTLED_KEYS'), ('ENABLED', 'ACCESSED_AND_THROTTLED_KEYS')]; rule list unchanged=True.
  DISABLE when DISABLED: 200 OK status=DISABLED. DISABLE when never configured: 200 OK status=DISABLED.
  - ACK: none · ops: UpdateContributorInsights · fields: ContributorInsightsAction
  - repro: ENABLE; wait ENABLED; ENABLE; poll Describe
  - handling: not handled in the controller (as of commit 34b85e6)
  - related: [DDB-TABLE-089](#ddb-table-089), [DDB-TABLE-090](#ddb-table-090), [DDB-TABLE-092](table-streams-encryption-class.md#ddb-table-092), [DDB-TABLE-341](#ddb-table-341), [DDB-TABLE-339](#ddb-table-339), [DDB-TABLE-343](#ddb-table-343),
    [DDB-TABLE-340](#ddb-table-340), [DDB-TABLE-085](#ddb-table-085), [DDB-TABLE-147](#ddb-table-147), [DDB-TABLE-300](table-policy-kinesis-autoscaling.md#ddb-table-300), [DDB-TABLE-207](table-policy-kinesis-autoscaling.md#ddb-table-207), [DDB-TABLE-383](table-streams-encryption-class.md#ddb-table-383), [DDB-TABLE-205](table-policy-kinesis-autoscaling.md#ddb-table-205) ·
    evidence: table/sub-resources/insights-lifecycle
  - notes: Hypotheses: H-S-020.

- <a id="ddb-table-147"></a>**DDB-TABLE-147** `idempotency` · impact high · unhandled (not handled in controller) · verified 2026-10-09, re-verified
  **Re-applying TTL enable -> ValidationException: same attribute 'TimeToLive is already enabled'; other attribute 'active on a different Attr'**
  Settled ENABLED table past the cooldown (1872 s after enable): UpdateTimeToLive(Enabled=true, same attr) ->
  ValidationException (HTTP 400) 'TimeToLive is already enabled' (status stays ENABLED, no re-transition);
  UpdateTimeToLive(Enabled=true, other attr) -> ValidationException 'TimeToLive is active on a different
  AttributeName: current AttributeName is ttl_a'. Renaming therefore requires disable (right attr) -> ~31 min
  cooldown -> enable (new attr). Immediately after an enable (within the cooldown) the same two messages are
  returned, i.e. content validation precedes the cooldown check.
  - ACK: custom_update, compare.is_ignored+delta_pre_compare · ops: UpdateTimeToLive · fields:
    TimeToLiveSpecification
  - repro: enable ttl_a -> wait ENABLED (+cooldown) -> UpdateTimeToLive(enable ttl_a) /
    UpdateTimeToLive(enable ttl_b)
  - handling: not handled in the controller (as of commit 34b85e6)
  - related: [DDB-TABLE-143](#ddb-table-143), [DDB-TABLE-144](#ddb-table-144), [DDB-TABLE-145](#ddb-table-145), [DDB-TABLE-146](#ddb-table-146), [DDB-TABLE-114](service.md#ddb-table-114), [DDB-TABLE-377](service.md#ddb-table-377),
    [DDB-TABLE-085](#ddb-table-085), [DDB-TABLE-091](#ddb-table-091), [DDB-TABLE-341](#ddb-table-341), [DDB-TABLE-300](table-policy-kinesis-autoscaling.md#ddb-table-300), [DDB-TABLE-207](table-policy-kinesis-autoscaling.md#ddb-table-207), [DDB-TABLE-383](table-streams-encryption-class.md#ddb-table-383), [DDB-TABLE-205](table-policy-kinesis-autoscaling.md#ddb-table-205) ·
    evidence: table/mutation-matrix/ttl-updates, table/creative/reverify-set-b
  - notes: Hypotheses: H-S-013, H-S-001. Hypotheses: H-S-013 confirmed (idempotent re-apply is unsafe),
    H-S-001.

- <a id="ddb-table-341"></a>**DDB-TABLE-341** `idempotency` · impact medium · unhandled (not handled in controller) · verified 2026-10-09
  **Re-ENABLE same mode = synchronous no-op (response ENABLED); narrowing to THROTTLED_KEYS at the quota works (4 -> 2 rules, same timestamp)**
  With the CloudWatch quota fully used (100/100): UpdateContributorInsights(ENABLE) on an already ENABLED
  table with no mode -> 200 with ContributorInsightsStatus=ENABLED (not ENABLING), Describe unchanged (same 4
  rule names, same LastUpdateDateTime), CloudWatch count unchanged. ENABLE with
  ContributorInsightsMode=THROTTLED_KEYS on another ENABLED table -> 200 ENABLING -> ENABLED in 2.14s with 2
  rules (PKT/SKT), CloudWatch 100->98; the surviving rule names keep the original timestamp suffix (True),
  i.e. the mode change deletes the PKC/SKC rules in place rather than recreating.
  - ACK: compare.is_ignored+delta_pre_compare, custom_update · ops: UpdateContributorInsights · fields:
    ContributorInsightsAction, ContributorInsightsMode, ContributorInsightsRuleList
  - repro: ENABLED table: UpdateContributorInsights(ENABLE) -> ENABLED sync; UpdateContributorInsights(ENABLE,
    THROTTLED_KEYS) -> ENABLING -> ENABLED with 2 rules
  - measurements: mode_change_settle_s=2.14
  - handling: not handled in the controller (as of commit 34b85e6)
  - related: [DDB-TABLE-089](#ddb-table-089), [DDB-TABLE-090](#ddb-table-090), [DDB-TABLE-092](table-streams-encryption-class.md#ddb-table-092), [DDB-TABLE-093](#ddb-table-093), [DDB-TABLE-095](#ddb-table-095), [DDB-TABLE-097](#ddb-table-097),
    [DDB-TABLE-091](#ddb-table-091), [DDB-TABLE-339](#ddb-table-339), [DDB-TABLE-343](#ddb-table-343), [DDB-TABLE-340](#ddb-table-340), [DDB-TABLE-337](#ddb-table-337), [DDB-TABLE-439](#ddb-table-439), [DDB-TABLE-338](#ddb-table-338),
    [DDB-TABLE-342](#ddb-table-342), [DDB-TABLE-085](#ddb-table-085), [DDB-TABLE-147](#ddb-table-147), [DDB-TABLE-300](table-policy-kinesis-autoscaling.md#ddb-table-300), [DDB-TABLE-207](table-policy-kinesis-autoscaling.md#ddb-table-207), [DDB-TABLE-383](table-streams-encryption-class.md#ddb-table-383), [DDB-TABLE-205](table-policy-kinesis-autoscaling.md#ddb-table-205) ·
    hypotheses: H-S-127, H-S-022 · evidence: table/limits/insights-rule-quota
  - notes: Safe to re-send ENABLE every reconcile when the mode is unchanged (no churn, no quota risk).
    Widening THROTTLED_KEYS -> ACCESSED_AND_THROTTLED_KEYS at the quota was not tested (it would need +2 rules
    and presumably FAILs like a fresh enable).

## Errors

- <a id="ddb-table-097"></a>**DDB-TABLE-097** `error-code` · impact medium · handled · verified 2026-10-08
  **Contributor Insights calls on a DELETING table -> 200 / 200; on a deleted table -> ResourceNotFoundException**
  While DELETING: Describe -> 200 OK status=ENABLED; Update(DISABLE) -> 200 OK status=DISABLING;
  List(TableName) -> 200. After deletion: Describe -> ResourceNotFoundException (HTTP 400) 'Requested resource
  not found: Table: ackq-244c78-i1 not found'; Update -> ResourceNotFoundException (HTTP 400) 'Requested
  resource not found: Table: ackq-244c78-i1 not found'; List(TableName) -> ResourceNotFoundException.
  - ACK: exceptions.404, deletable.when · ops: DescribeContributorInsights, UpdateContributorInsights,
    ListContributorInsights
  - repro: DeleteTable then call the three insights APIs
  - handling: handled via `generator.yaml:84-87; pkg/resource/table/sdk.go:83-86`
  - related: [DDB-TABLE-122](table-policy-kinesis-autoscaling.md#ddb-table-122), [DDB-TABLE-236](table-policy-kinesis-autoscaling.md#ddb-table-236), [DDB-TABLE-374](service.md#ddb-table-374), [DDB-TABLE-453](#ddb-table-453), [DDB-TABLE-088](#ddb-table-088), [DDB-TABLE-235](table-policy-kinesis-autoscaling.md#ddb-table-235),
    [DDB-TABLE-342](#ddb-table-342), [DDB-TABLE-214](table-policy-kinesis-autoscaling.md#ddb-table-214), [DDB-TABLE-271](table-policy-kinesis-autoscaling.md#ddb-table-271), [DDB-TABLE-436](table-policy-kinesis-autoscaling.md#ddb-table-436), [DDB-TABLE-377](service.md#ddb-table-377) · evidence:
    table/sub-resources/insights-lifecycle
  - notes: Hypotheses: H-S-004.

- <a id="ddb-table-161"></a>**DDB-TABLE-161** `error-code` · impact high · handled · verified 2026-10-08
  **Unknown GSI in UpdateTable is ResourceNotFoundException with an 'Index' message; WarmThroughput on a ghost index returns HTTP 500**
  GlobalSecondaryIndexUpdates[Delete {IndexName: ghost}] and [Update {IndexName: ghost,
  ProvisionedThroughput}] -> ResourceNotFoundException 'Requested resource not found: Index ghost for table
  ackq-...-gsi-gran' (also when mixed with valid Update entries, which are then not applied). A missing table
  gives 'Requested resource not found: Table: X not found' - the word after 'found:' discriminates. [Update
  {IndexName: ghost, WarmThroughput {12001,4001}}] -> HTTP 500 InternalFailure with no message (1.7 s).
  UpdateContributorInsights/DescribeContributorInsights IndexName=ghost -> ResourceNotFoundException
  'Requested resource not found: Index: ghost not found for table: X'. Query IndexName=ghost ->
  ValidationException 'The table does not have the specified index: ghost'. IndexNotFoundException was never
  returned.
  - ACK: exceptions.404, terminal_codes · ops: UpdateTable, UpdateContributorInsights,
    DescribeContributorInsights, Query · fields: GlobalSecondaryIndexUpdates.Delete.IndexName,
    GlobalSecondaryIndexUpdates.Update.IndexName
  - repro: UpdateTable GlobalSecondaryIndexUpdates=[{Update:{IndexName:ghost,
    WarmThroughput:{ReadUnitsPerSecond:12001,WriteUnitsPerSecond:4001}}}]
  - handling: handled via `generator.yaml:84-87; pkg/resource/table/sdk.go:83-86`
  - related: [DDB-TABLE-448](service.md#ddb-table-448), [DDB-TABLE-456](service.md#ddb-table-456), [DDB-TABLE-458](table-indexes.md#ddb-table-458), [DDB-TABLE-380](table-indexes.md#ddb-table-380), [DDB-TABLE-094](#ddb-table-094), [DDB-TABLE-137](#ddb-table-137),
    [DDB-TABLE-116](#ddb-table-116) · evidence: table/mutation-matrix/gsi-update-granularity
  - notes: Confirms H-T-125 (IndexNotFoundException is dead); partially refutes H-T-055 (Update on a ghost
    index is also ResourceNotFoundException, not ValidationException). A controller mapping
    ResourceNotFoundException to 'table gone' must inspect the message. The 500 for WarmThroughput on an
    unknown index...
  - full notes: [details/DDB-TABLE-161.md](details/DDB-TABLE-161.md)

## Request validation

- <a id="ddb-table-146"></a>**DDB-TABLE-146** `request-validation` · impact high · handled · verified 2026-10-09, re-verified
  **UpdateTimeToLive validation: TTL on the partition key is ACCEPTED; already-disabled and wrong-attribute disables -> ValidationException**
  Never-configured table: disable(attr=x) -> ValidationException 'TimeToLive is already disabled';
  enable(AttributeName=<partition key 'pk'>) -> 200 and TTL became ENABLED on the key attribute;
  enable(256-char attr) -> ValidationException 'Member must have length less than or equal to 255'; empty attr
  -> rejected client-side (min length 1); omitting AttributeName (client validation bypassed) ->
  ValidationException 'Value null at timeToLiveSpecification.attributeName ... Member must not be null' for
  both enable and disable; omitting Enabled -> same 'Member must not be null' for
  timeToLiveSpecification.enabled. Settled ENABLED(ttl_a) table: disable with a different attribute ->
  ValidationException 'TimeToLive is active on a different AttributeName: current AttributeName is ttl_a';
  disable with the right attribute -> 200; disable again -> 'TimeToLive is already disabled'. All server
  errors are ValidationException HTTP 400.
  - ACK: custom_update, terminal_codes · ops: UpdateTimeToLive · fields:
    TimeToLiveSpecification.AttributeName, TimeToLiveSpecification.Enabled
  - repro: UpdateTimeToLive variants on a fresh table and on a settled ENABLED table
  - handling: handled via `pkg/resource/table/hooks_ttl.go:41-54; 4ea832a; pkg/resource/table/hooks.go:210-218; 4ea832a`
  - related: [DDB-TABLE-143](#ddb-table-143), [DDB-TABLE-144](#ddb-table-144), [DDB-TABLE-145](#ddb-table-145), [DDB-TABLE-147](#ddb-table-147), [DDB-TABLE-114](service.md#ddb-table-114), [DDB-TABLE-377](service.md#ddb-table-377) ·
    evidence: table/mutation-matrix/ttl-updates, table/creative/reverify-set-a1
  - notes: Hypotheses: H-S-013, H-S-002. Hypotheses: H-S-002 confirmed (AttributeName required to disable and
    must match), H-S-013 partially confirmed - no-ops are rejected, but the key-schema-attribute clause is
    REFUTED (accepted).

## Field behavior (defaults, normalization, shapes, immutability)

- <a id="ddb-table-083"></a>**DDB-TABLE-083** `shape-mismatch` · impact high · handled · verified 2026-10-08
  **ContinuousBackupsStatus is not the PITR flag: DISABLED for a few s after ACTIVE, then ENABLED on its own; stays ENABLED after PITR disable**
  Fresh table (immediately after ACTIVE): ContinuousBackupsStatus=DISABLED,
  PointInTimeRecoveryDescription={PointInTimeRecoveryStatus: DISABLED} (no other fields).
  UpdateContinuousBackups(enable) during that window -> 200 and ContinuousBackupsStatus=ENABLED with PITR
  fields [EarliestRestorableDateTime, LatestRestorableDateTime, PointInTimeRecoveryStatus,
  RecoveryPeriodInDays=35]. After PITR disable: ContinuousBackupsStatus stays ENABLED while
  PointInTimeRecoveryDescription shrinks to {PointInTimeRecoveryStatus: DISABLED}. Tables that never touch
  PITR also flip to ContinuousBackupsStatus=ENABLED by themselves within minutes (see
  table/consistency-windows/continuous-backups-window). The Update response has the same shape as Describe
  (top-level key ContinuousBackupsDescription only); the request bool PointInTimeRecoveryEnabled is never
  echoed and the status is a string enum.
  - ACK: custom_field, custom_update, compare.is_ignored+delta_pre_compare · ops: DescribeContinuousBackups,
    UpdateContinuousBackups · fields: ContinuousBackupsDescription.ContinuousBackupsStatus,
    ContinuousBackupsDescription.PointInTimeRecoveryDescription.PointInTimeRecoveryStatus,
    ContinuousBackupsDescription.PointInTimeRecoveryDescription.RecoveryPeriodInDays
  - repro: CreateTable -> DescribeContinuousBackups -> UpdateContinuousBackups(enable) -> Describe -> disable
    -> Describe
  - handling: handled via `generator.yaml:46-50; pkg/resource/table/hooks_continuous_backup.go:27-94; pkg/resource/table/hooks.go:697-718; pkg/resource/table/hooks_continuous_backup.go:36-44; pkg/resource/table/hooks.go:686-696; pkg/resource/table/hooks.go:729-731`
  - related: [DDB-TABLE-111](#ddb-table-111), [DDB-TABLE-115](service.md#ddb-table-115), [DDB-TABLE-084](#ddb-table-084), [DDB-TABLE-085](#ddb-table-085) · evidence:
    table/sub-resources/pitr-lifecycle
  - notes: Hypotheses: H-S-101, H-S-104, H-S-003. Confirms H-S-104 (no round-trippable field) and H-S-003
    (fields conditional on ENABLED). H-S-101 partially refuted: ContinuousBackupsStatus=DISABLED IS observable
    on a normal table right after creation.
  - full notes: [details/DDB-TABLE-083.md](details/DDB-TABLE-083.md)

- <a id="ddb-table-084"></a>**DDB-TABLE-084** `server-default` · impact high · handled · verified 2026-10-09, re-verified
  **RecoveryPeriodInDays: defaults to 35, changeable in place, rejected with Enabled=false, forgotten (back to 35) on disable/re-enable**
  Enable without period -> RecoveryPeriodInDays=35. Set 7 while enabled -> 200 OK (Describe=7); 0 ->
  ParamValidationError (HTTP None) 'Parameter validation failed:
  Invalid value for parameter PointInTimeRecoverySpecification.RecoveryPeriodInDays, value: 0, valid min
  value: 1'; 36 -> ValidationException (HTTP 400) '1 validation error detected: Value '36' at
  'pointInTimeRecoverySpecification.recoveryPeriodInDays' failed to satisfy constraint: Member must have value
  less tha'; -1 -> ParamValidationError (HTTP None) 'Parameter validation failed:
  Invalid value for parameter PointInTimeRecoverySpecification.RecoveryPeriodInDays, value: -1, valid min
  value: 1'; 1 -> 200 OK; 35 -> 200 OK. Disable with RecoveryPeriodInDays=7 -> ValidationException (HTTP 400)
  'Invalid Request: Cannot specify RecoveryPeriodInDays when disabling point-in-time recovery.'; with 35 ->
  ValidationException (HTTP 400) 'Invalid Request: Cannot specify RecoveryPeriodInDays when disabling
  point-in-time recovery.'. When DISABLED the field reads <absent>. Disable (period was 7) then enable without
  period -> period 35.
  - ACK: compare.nil_equals_zero_value, custom_update, late_initialize · ops: UpdateContinuousBackups,
    DescribeContinuousBackups · fields: PointInTimeRecoverySpecification.RecoveryPeriodInDays
  - repro: UpdateContinuousBackups(enable) ; (enable,7) ; (enable,0) ; (enable,36) ; (disable,7) ; (disable) ;
    (enable)
  - handling: handled via `pkg/resource/table/hooks.go:697-718; pkg/resource/table/hooks_continuous_backup.go:36-44`
  - related: [DDB-TABLE-083](#ddb-table-083), [DDB-TABLE-111](#ddb-table-111), [DDB-TABLE-115](service.md#ddb-table-115), [DDB-TABLE-085](#ddb-table-085) · evidence:
    table/sub-resources/pitr-lifecycle, table/creative/reverify-set-b
  - notes: Hypotheses: H-S-003, H-S-016, H-S-105.

- <a id="ddb-table-089"></a>**DDB-TABLE-089** `read-gap` · impact medium · handled · verified 2026-10-08
  **DescribeContributorInsights on a never-configured table: DISABLED; fields change after an enable/disable cycle**
  Fresh table response: {'TableName': 'ackq-244c78-i1', 'ContributorInsightsStatus': 'DISABLED'}. After
  ENABLE->ENABLED->DISABLE->DISABLED: {'TableName': 'ackq-244c78-i1', 'ContributorInsightsStatus': 'DISABLED',
  'LastUpdateDateTime': '2026-10-08T23:08:44.684000+00:00', 'ContributorInsightsMode':
  'ACCESSED_AND_THROTTLED_KEYS'}. ListContributorInsights(TableName) on the fresh table: {'ok': True,
  'NextToken': None, 'summaries': []}. DISABLE on a never-configured table -> 200 OK status=DISABLED.
  - ACK: compare.is_ignored+delta_pre_compare, custom_field · ops: DescribeContributorInsights,
    ListContributorInsights, UpdateContributorInsights · fields: ContributorInsightsStatus,
    ContributorInsightsMode, LastUpdateDateTime, ContributorInsightsRuleList
  - repro: CreateTable -> DescribeContributorInsights -> ENABLE -> wait -> DISABLE -> wait -> Describe
  - handling: handled via `generator.yaml:78-83; pkg/resource/table/hooks.go:882-960; pkg/resource/table/hooks.go:733-744; pkg/resource/table/hooks.go:905-908; pkg/resource/table/hooks.go:686-696; pkg/resource/table/hooks.go:729-731`
  - related: [DDB-TABLE-090](#ddb-table-090), [DDB-TABLE-091](#ddb-table-091), [DDB-TABLE-092](table-streams-encryption-class.md#ddb-table-092), [DDB-TABLE-341](#ddb-table-341), [DDB-TABLE-339](#ddb-table-339), [DDB-TABLE-343](#ddb-table-343),
    [DDB-TABLE-340](#ddb-table-340) · evidence: table/sub-resources/insights-lifecycle
  - notes: Hypotheses: H-S-019, H-S-020.

- <a id="ddb-table-137"></a>**DDB-TABLE-137** `immutable-field` · impact high · unhandled (not handled in controller) · verified 2026-10-08
  **LSIs are create-only: UpdateTable has no LSI member and GlobalSecondaryIndexUpdates rejects LSI names with ValidationException**
  The UpdateTable input shape has no LocalSecondaryIndex member. GlobalSecondaryIndexUpdates[Delete
  {IndexName: lsi-b}] -> ValidationException 'Cannot delete an index that is not a GlobalSecondaryIndex:
  lsi-b'; [Update ...] -> 'Cannot update an index that is not a GlobalSecondaryIndex: lsi-b'; [Create] with
  the LSI's name -> 'Attempting to create an index which already exists'. LocalSecondaryIndexDescription
  carries IndexName, KeySchema, Projection, IndexSizeBytes, ItemCount, IndexArn and no IndexStatus.
  UpdateContributorInsights with IndexName=lsi-b -> ResourceNotFoundException 'Index: lsi-b not found for
  table: X'.
  - ACK: is_immutable, terminal_codes · ops: UpdateTable, DescribeTable, UpdateContributorInsights · fields:
    LocalSecondaryIndexes, GlobalSecondaryIndexUpdates
  - repro: Table with LSI lsi-b; UpdateTable GlobalSecondaryIndexUpdates=[{Delete:{IndexName: lsi-b}}]
  - handling: not handled in the controller (as of commit 34b85e6)
  - related: [DDB-TABLE-161](#ddb-table-161), [DDB-TABLE-380](table-indexes.md#ddb-table-380), [DDB-TABLE-094](#ddb-table-094), [DDB-TABLE-116](#ddb-table-116), [DDB-TABLE-357](table-indexes.md#ddb-table-357), [DDB-TABLE-133](table-indexes.md#ddb-table-133) ·
    evidence: table/round-trip/gsi-lsi-describe
  - notes: Confirms H-T-045; any spec change to localSecondaryIndexes is a recreate-only condition.

- <a id="ddb-table-144"></a>**DDB-TABLE-144** `read-gap` · impact medium · handled · verified 2026-10-08
  **DescribeTimeToLive: never-configured and disabled tables are indistinguishable ({TimeToLiveStatus: DISABLED}, no AttributeName)**
  Fresh table: TimeToLiveDescription={TimeToLiveStatus: DISABLED}. Enabled: {TimeToLiveStatus: ENABLED,
  AttributeName: ttl_a}. After a disable (observed on two tables): {TimeToLiveStatus: DISABLED} with
  AttributeName absent - identical to the fresh table. No DISABLING or ENABLING state was ever observed with 1
  s polling; the first Describe after each Update already showed the settled status, so the previous attribute
  cannot be read back from a disabled table.
  - ACK: compare.is_ignored+delta_pre_compare, custom_field · ops: DescribeTimeToLive · fields:
    TimeToLiveDescription.AttributeName, TimeToLiveDescription.TimeToLiveStatus
  - repro: CreateTable -> DescribeTimeToLive; enable -> wait -> disable -> poll DescribeTimeToLive
  - handling: handled via `generator.yaml:42-45; pkg/resource/table/hooks_ttl.go:27-93; test/e2e/table.py:47-73; test/e2e/tests/test_table.py:351-386; pkg/resource/table/hooks_ttl.go:85-92; test/e2e/tests/test_table.py:385-386; pkg/resource/table/hooks.go:686-696; pkg/resource/table/hooks.go:729-731`
  - related: [DDB-TABLE-143](#ddb-table-143), [DDB-TABLE-145](#ddb-table-145), [DDB-TABLE-146](#ddb-table-146), [DDB-TABLE-147](#ddb-table-147), [DDB-TABLE-114](service.md#ddb-table-114), [DDB-TABLE-377](service.md#ddb-table-377) ·
    evidence: table/mutation-matrix/ttl-updates
  - notes: Hypotheses: H-S-012. Hypotheses: H-S-012 confirmed (attribute forgotten once DISABLED; the 'still
    present during DISABLING' clause could not be observed because DISABLING is never visible).

- <a id="ddb-table-145"></a>**DDB-TABLE-145** `shape-mismatch` · impact medium · handled · verified 2026-10-08
  **UpdateTimeToLive echoes the request (TimeToLiveSpecification{Enabled,AttributeName}); Describe uses TimeToLiveDescription{TimeToLiveStatus}**
  UpdateTimeToLive(Enabled=true, AttributeName=ttl_a) -> 200 {TimeToLiveSpecification: {Enabled: true,
  AttributeName: ttl_a}} (no status). DescribeTimeToLive 0-2 s later -> {TimeToLiveDescription:
  {TimeToLiveStatus: ENABLED, AttributeName: ttl_a}}; ENABLING was not observed on any of 5 tables at 1 s
  polling (the Update is effectively synchronous for an empty table). TableStatus stayed ACTIVE after the
  update. Steady 1/s DescribeTimeToLive polling intermittently returned ThrottlingException (HTTP 400) while
  other probes were active in the account (botocore retry masked it as 1-2 s latency).
  - ACK: custom_update, synced.when, requeue · ops: UpdateTimeToLive, DescribeTimeToLive · fields:
    TimeToLiveSpecification.Enabled, TimeToLiveDescription.TimeToLiveStatus
  - repro: UpdateTimeToLive(Enabled=true, AttributeName=ttl_a) then DescribeTimeToLive
  - measurements: enabled_visible_within_s=2.1, enabling_state_observed=false
  - handling: handled via `generator.yaml:42-45; pkg/resource/table/hooks_ttl.go:27-93; test/e2e/table.py:47-73; test/e2e/tests/test_table.py:351-386; pkg/resource/table/hooks_ttl.go:85-92; test/e2e/tests/test_table.py:385-386`
  - related: [DDB-TABLE-143](#ddb-table-143), [DDB-TABLE-144](#ddb-table-144), [DDB-TABLE-146](#ddb-table-146), [DDB-TABLE-147](#ddb-table-147), [DDB-TABLE-114](service.md#ddb-table-114), [DDB-TABLE-377](service.md#ddb-table-377) ·
    evidence: table/mutation-matrix/ttl-updates
  - notes: Hypotheses: H-S-011. Hypotheses: H-S-011 confirmed for the shape mismatch; the 'ENABLING lasts a
    while' part refuted (settles within the first poll).

## Response fidelity and consistency

- <a id="ddb-table-095"></a>**DDB-TABLE-095** `response-fidelity` · impact medium · unhandled (not handled in controller) · verified 2026-10-08
  **ListContributorInsights omits DISABLED entries (fresh table -> empty), pages table first then GSIs, last page has no NextToken**
  Fresh table: List(TableName) -> 200 with ContributorInsightsSummaries=[] (never-configured table is not
  listed). With table+GSI ENABLED, MaxResults=1: page 1 = [{TableName, ContributorInsightsStatus: ENABLED,
  ContributorInsightsMode}] + NextToken; page 2 = [{TableName, IndexName: gsi1, ...}] and no NextToken (2
  pages, no empty trailing page). After DISABLE on the table (GSI still ENABLED): List returns only the GSI
  entry - the DISABLED table entry disappears. A DISABLING entry is still listed. Account-wide List() with no
  TableName (MaxResults=100): 3 summaries, included the other probe table. Each summary carries
  ContributorInsightsMode while ENABLING/ENABLED/DISABLING.
  - ACK: custom_find, list_operation.match_fields · ops: ListContributorInsights · fields:
    ContributorInsightsSummaries, NextToken, MaxResults
  - repro: ListContributorInsights(TableName=<table with gsi>, MaxResults=1) loop
  - handling: not handled in the controller (as of commit 34b85e6)
  - related: [DDB-TABLE-094](#ddb-table-094), [DDB-TABLE-344](#ddb-table-344), [DDB-TABLE-093](#ddb-table-093), [DDB-TABLE-112](#ddb-table-112), [DDB-TABLE-116](#ddb-table-116) · evidence:
    table/sub-resources/insights-lifecycle
  - notes: Hypotheses: H-S-125. H-S-125 largely confirmed; the key addition is that List cannot be used to
    discover DISABLED/never-configured tables or indexes - Describe per table/index is the only way.
  - full notes: [details/DDB-TABLE-095.md](details/DDB-TABLE-095.md)

- <a id="ddb-table-111"></a>**DDB-TABLE-111** `eventual-consistency` · impact medium · handled · verified 2026-10-08
  **ContinuousBackupsStatus reads DISABLED for ~3 s after TableStatus=ACTIVE on a fresh table, then flips to ENABLED by itself**
  1/s DescribeContinuousBackups from CreateTable on a plain table (no PITR calls): [{'value':
  'ERR:TableNotFoundException', 'from_s': 0.03, 'to_s': 3.11, 'duration_s': 3.08}, {'value':
  'DISABLED/DISABLED', 'from_s': 3.11, 'to_s': 10.26, 'duration_s': 7.15}, {'value': 'ENABLED/DISABLED',
  'from_s': 10.26, 'to_s': None, 'duration_s': None}]. TableStatus transitions: [(0.0, 'CREATING'), (7.2,
  'ACTIVE')]. Final: {'ContinuousBackupsDescription': {'ContinuousBackupsStatus': 'ENABLED',
  'PointInTimeRecoveryDescription': {'PointInTimeRecoveryStatus': 'DISABLED'}}}. Enabling PITR during the
  DISABLED window is accepted (see table/sub-resources/pitr-lifecycle) and immediately reads
  ContinuousBackupsStatus=ENABLED; UpdateContinuousBackups/CreateBackup during the same window fail with
  ContinuousBackupsUnavailableException 'Backups are being enabled for the table ... Please retry later' (see
  table/state-machine/subresource-admissibility).
  - ACK: compare.is_ignored+delta_pre_compare, requeue, synced.when · ops: DescribeContinuousBackups,
    CreateTable · fields: ContinuousBackupsDescription.ContinuousBackupsStatus
  - repro: CreateTable; poll DescribeContinuousBackups at 1/s; never call UpdateContinuousBackups
  - measurements: table_not_found_window_s=3.08, disabled_window_s=7.15, disabled_after_active_s=3.1,
    create_to_active_s=7.2
  - handling: handled via `generator.yaml:104-109; pkg/resource/table/hooks.go:72-93; generator.yaml:46-50; pkg/resource/table/hooks_continuous_backup.go:27-94; generator.yaml:84-87; pkg/resource/table/sdk.go:83-86`
  - related: [DDB-TABLE-115](service.md#ddb-table-115), [DDB-TABLE-233](table-policy-kinesis-autoscaling.md#ddb-table-233), [DDB-TABLE-234](table-policy-kinesis-autoscaling.md#ddb-table-234), [DDB-TABLE-114](service.md#ddb-table-114), [DDB-TABLE-083](#ddb-table-083), [DDB-TABLE-084](#ddb-table-084),
    [DDB-TABLE-085](#ddb-table-085) · evidence: table/consistency-windows/continuous-backups-window
  - notes: H-S-101 ('ContinuousBackupsStatus is ENABLED for every ACTIVE table') is only true after this
    window; H-S-103 confirmed (200 + DISABLED variant).

- <a id="ddb-table-340"></a>**DDB-TABLE-340** `response-fidelity` · impact medium · unhandled (not handled in controller) · verified 2026-10-09
  **ListContributorInsights lists FAILED entries (status+mode) with/without TableName; Describe on FAILED omits ContributorInsightsRuleList**
  ListContributorInsights(TableName=<failed table>) -> [{'TableName': 'ackq-72c077-iq-r25',
  'ContributorInsightsStatus': 'FAILED', 'ContributorInsightsMode': 'ACCESSED_AND_THROTTLED_KEYS'}]; the
  account-wide ListContributorInsights() (26 summaries, statuses ['ENABLED', 'FAILED']) includes the same
  FAILED entry. DescribeContributorInsights on it returns keys ['ContributorInsightsMode',
  'ContributorInsightsStatus', 'FailureException', 'LastUpdateDateTime', 'TableName'] (no
  ContributorInsightsRuleList, unlike ENABLED entries) with LastUpdateDateTime = the time of the failure.
  - ACK: custom_find, synced.when · ops: ListContributorInsights, DescribeContributorInsights · fields:
    ContributorInsightsSummaries, ContributorInsightsStatus, ContributorInsightsRuleList
  - repro: Exhaust the rule quota; ENABLE one more table; ListContributorInsights with and without TableName
  - handling: not handled in the controller (as of commit 34b85e6)
  - related: [DDB-TABLE-089](#ddb-table-089), [DDB-TABLE-090](#ddb-table-090), [DDB-TABLE-092](table-streams-encryption-class.md#ddb-table-092), [DDB-TABLE-093](#ddb-table-093), [DDB-TABLE-095](#ddb-table-095), [DDB-TABLE-097](#ddb-table-097),
    [DDB-TABLE-091](#ddb-table-091), [DDB-TABLE-341](#ddb-table-341), [DDB-TABLE-339](#ddb-table-339), [DDB-TABLE-343](#ddb-table-343), [DDB-TABLE-337](#ddb-table-337), [DDB-TABLE-439](#ddb-table-439), [DDB-TABLE-338](#ddb-table-338),
    [DDB-TABLE-342](#ddb-table-342) · hypotheses: H-S-125, H-S-127 · evidence: table/limits/insights-rule-quota
  - notes: Complements [DDB-TABLE-095](#ddb-table-095) (List omits DISABLED): the List filter is 'not DISABLED', so FAILED
    entries are discoverable by List, and a FAILED entry disappears from List once it is DISABLED.

- <a id="ddb-table-343"></a>**DDB-TABLE-343** `response-fidelity` · impact medium · unhandled (not handled in controller) · verified 2026-10-09
  **No insights ghosts: an enabled-then-disabled table leaves ListContributorInsights the moment Describe says DISABLED; DISABLING is listed**
  3 hash-only tables, insights ENABLED (ENABLING -> ENABLED in <= 1 s). DISABLE on T0: while Describe showed
  DISABLING (3 polls at 1 s) List(TableName=T0) returned the entry as [['DISABLING',
  'ACCESSED_AND_THROTTLED_KEYS']]; at the poll where Describe showed DISABLED (t=3.08s) List(TableName=T0) was
  already empty. Afterwards T0 was absent from the account-wide list in 30/30 one-second polls over 30 s and
  from List(TableName) (0 summaries), while Describe(T0) kept DISABLED + ContributorInsightsMode +
  LastUpdateDateTime. Re-ENABLE puts it back (['ENABLED']); disabling 2 of 3 leaves exactly the ENABLED one
  (['ilg2']). Fresh tables: List(TableName) -> empty, Describe -> keys ['ContributorInsightsStatus',
  'TableName'].
  - ACK: custom_find, synced.when · ops: ListContributorInsights, DescribeContributorInsights,
    UpdateContributorInsights · fields: ContributorInsightsSummaries, ContributorInsightsStatus
  - repro: ENABLE insights on 3 tables; DISABLE one; poll Describe + List(TableName) at 1 s; poll account-wide
    List for 30 s
  - measurements: disabling_s=3.08, ghost_polls_30s=30
  - handling: not handled in the controller (as of commit 34b85e6)
  - related: [DDB-TABLE-089](#ddb-table-089), [DDB-TABLE-094](#ddb-table-094), [DDB-TABLE-095](#ddb-table-095), [DDB-TABLE-096](#ddb-table-096), [DDB-TABLE-112](#ddb-table-112), [DDB-TABLE-340](#ddb-table-340),
    [DDB-TABLE-090](#ddb-table-090), [DDB-TABLE-091](#ddb-table-091), [DDB-TABLE-092](table-streams-encryption-class.md#ddb-table-092), [DDB-TABLE-341](#ddb-table-341), [DDB-TABLE-339](#ddb-table-339) · hypotheses: H-S-125, H-S-023,
    H-S-019 · evidence: table/sub-resources/insights-list-ghosts
  - notes: Confirms and sharpens [DDB-TABLE-095](#ddb-table-095): the List filter is exactly 'status != DISABLED'
    (ENABLING/ENABLED/DISABLING/FAILED are listed, see [DDB-TABLE-340](#ddb-table-340)). List cannot be used to detect 'insights
    were once enabled'; only Describe (LastUpdateDateTime/mode present) can.

- <a id="ddb-table-344"></a>**DDB-TABLE-344** `response-fidelity` · impact medium · unhandled (not handled in controller) · verified 2026-10-09
  **Account-wide ListContributorInsights(MaxResults=N) returns EMPTY pages with a NextToken (42 pages for 9 live table/GSI entries at N=1)**
  ListContributorInsights without TableName iterates over an internal candidate set and emits a page per
  candidate slice even when every candidate is DISABLED: with 6 tables and 3 GSIs in the region and nothing
  enabled, MaxResults=1 -> 42 pages (all empty, each but the last with a NextToken), MaxResults=2 -> 21, 5 ->
  9, 100 -> 1 page, default -> 1 page. With 3 tables ENABLED, MaxResults=1 gave 41 pages with sizes [0, 0, 0,
  0, 0, 1, 0, 1, 0, 0]... (3 non-empty). The last page never carried a NextToken; List(TableName=x) is always
  one page without NextToken. The candidate count (42) exceeded the live tables+GSIs (9), i.e. it is not
  simply 'all tables'.
  - ACK: custom_find, list_operation.match_fields · ops: ListContributorInsights · fields: MaxResults,
    NextToken, ContributorInsightsSummaries
  - repro: ListContributorInsights(MaxResults=1) and follow NextToken until absent; count pages vs ListTables
  - measurements: pages_max_results_1=42, tables_in_region=6, gsis_in_region=3
  - handling: not handled in the controller (as of commit 34b85e6)
  - related: [DDB-TABLE-089](#ddb-table-089), [DDB-TABLE-094](#ddb-table-094), [DDB-TABLE-095](#ddb-table-095), [DDB-TABLE-096](#ddb-table-096), [DDB-TABLE-112](#ddb-table-112), [DDB-TABLE-340](#ddb-table-340),
    [DDB-TABLE-093](#ddb-table-093), [DDB-TABLE-116](#ddb-table-116) · hypotheses: H-S-125 · evidence: table/sub-resources/insights-list-ghosts
  - notes: REFUTES the H-S-125 clause 'never returns a NextToken that leads to an empty page'. A client that
    stops at the first empty page (a common shortcut) silently misses entries; always loop until NextToken is
    absent, or use MaxResults=100 (single page here). Status=DISABLED is never emitted, so the empty...
  - full notes: [details/DDB-TABLE-344.md](details/DDB-TABLE-344.md)

## Delete semantics

- <a id="ddb-table-088"></a>**DDB-TABLE-088** `delete-semantics` · impact medium · unhandled (not handled in controller) · verified 2026-10-08
  **SYSTEM backup on delete of a PITR table is suppressed when UpdateContinuousBackups(disable) is issued while DELETING**
  ListBackups(BackupType=SYSTEM) after DeleteTable: PITR-on table -> None; PITR-off table -> None.
  DeleteBackup on the system backup -> n/a.
  - ACK: pre-delete-cleanup, docs-only · ops: DeleteTable, ListBackups, DeleteBackup
  - repro: enable PITR; DeleteTable; poll ListBackups(BackupType=SYSTEM) 5 min
  - handling: not handled in the controller (as of commit 34b85e6)
  - related: [DDB-TABLE-113](#ddb-table-113), [DDB-TABLE-122](table-policy-kinesis-autoscaling.md#ddb-table-122), [DDB-TABLE-236](table-policy-kinesis-autoscaling.md#ddb-table-236), [DDB-TABLE-374](service.md#ddb-table-374), [DDB-TABLE-097](#ddb-table-097), [DDB-TABLE-453](#ddb-table-453),
    [DDB-TABLE-235](table-policy-kinesis-autoscaling.md#ddb-table-235), [DDB-TABLE-342](#ddb-table-342), [DDB-TABLE-214](table-policy-kinesis-autoscaling.md#ddb-table-214) · evidence: table/sub-resources/pitr-lifecycle
  - notes: Hypotheses: H-S-131. Re-interpreted after table/creative/pitr-delete-system-backup: this probe (and
    table/error-taxonomy/subresource-errors) issued UpdateContinuousBackups(PointInTimeRecoveryEnabled=false)
    while the table was DELETING (accepted, 200) and no SYSTEM backup ever appeared; a plain...
  - full notes: [details/DDB-TABLE-088.md](details/DDB-TABLE-088.md)

- <a id="ddb-table-112"></a>**DDB-TABLE-112** `delete-semantics` · impact medium · unhandled (not handled in controller) · verified 2026-10-08
  **Deleting a GSI with insights ENABLED (no DISABLE): accepted, no orphan in ListContributorInsights, CW rules removed at once**
  UpdateTable(Delete gsi1) with insights ENABLED on gsi1 -> {'ok': True, 'code': None, 'message': None,
  'table_status': 'UPDATING'}. During deletion: Describe(gsi) -> 200 {'TableName': 'ackq-c36bda-w2',
  'IndexName': 'gsi1', 'ContributorInsightsRuleList':
  ['DynamoDBContributorInsights-PKC-ackq-c36bda-w2-gsi1-1791501766746',
  'DynamoDBContributorInsights-PKT-ackq-c36bda-w2-gsi1-1791501766746'], 'ContributorInsightsStatus':
  'ENABLED', 'LastUpdateDateTime': '2026-10-08T23:22:46.892000+00:00', 'ContributorInsightsMode':
  'ACCESSED_AND_THROTTLED_KEYS'}; List -> {'code': None, 'summaries': [{'TableName': 'ackq-c36bda-w2',
  'IndexName': 'gsi1', 'ContributorInsightsStatus': 'ENABLED', 'ContributorInsightsMode':
  'ACCESSED_AND_THROTTLED_KEYS'}]}. Delete timeline [{'value': ('UPDATING', 1), 'from_s': 0.01, 'to_s': 4.05,
  'duration_s': 4.04}, {'value': ('ACTIVE', 0), 'from_s': 4.05, 'to_s': None, 'duration_s': None}]. Samples
  after the index is gone (t, Describe(gsi), List, CW rules): [(0.0, "ResourceNotFoundException (HTTP 400)
  'Requested resource not found: Index: gsi1 not found for table: ackq-c36bda-w2'", {'code': None,
  'summaries': []}, []), (30.1, "ResourceNotFoundException (HTTP 400) 'Requested resource not found: Index:
  gsi1 not found for table: ackq-c36bda-w2'", {'code': None, 'summaries': []}, []), (60.1,
  "ResourceNotFoundException (HTTP 400) 'Requested resource not found: Index: gsi1 not found for table:
  ackq-c36bda-w2'", {'code': None, 'summaries': []}, []), (120.1, "ResourceNotFoundException (HTTP 400)
  'Requested resource not found: Index: gsi1 not found for table: ackq-c36bda-w2'", {'code': None,
  'summaries': []}, [])]. CloudWatch rules before:
  ['DynamoDBContributorInsights-PKC-ackq-c36bda-w2-gsi1-1791501766746',
  'DynamoDBContributorInsights-PKT-ackq-c36bda-w2-gsi1-1791501766746']. DISABLE on the deleted index ->
  ResourceNotFoundException (HTTP 400) 'Requested resource not found: Index: gsi1 not found for table:
  ackq-c36bda-w2'.
  - ACK: pre-delete-cleanup, custom_find · ops: UpdateTable, DescribeContributorInsights,
    ListContributorInsights
  - repro: ENABLE insights on gsi; UpdateTable Delete gsi; sample Describe/List/CW for 2 min
  - handling: not handled in the controller (as of commit 34b85e6)
  - related: [DDB-TABLE-096](#ddb-table-096), [DDB-TABLE-094](#ddb-table-094), [DDB-TABLE-095](#ddb-table-095), [DDB-TABLE-344](#ddb-table-344), [DDB-TABLE-093](#ddb-table-093), [DDB-TABLE-116](#ddb-table-116) ·
    evidence: table/consistency-windows/continuous-backups-window
  - notes: Hypotheses: H-S-126. Hypotheses: H-S-126. Clean re-test (no DISABLE issued). Refutes the
    orphan-summary clause: List(TableName) is empty at t=0/30/60/120 s after the index is gone and the index's
    CloudWatch rules are deleted immediately; DISABLE on the deleted index -> ResourceNotFoundException.

- <a id="ddb-table-167"></a>**DDB-TABLE-167** `delete-semantics` · impact high · unhandled (not handled in controller) · verified 2026-10-08
  **Deleting a PITR table creates an undeletable 35-day SYSTEM backup '<table>$DeletedTableBackup' in ~8 s - unless PITR is disabled while DE...**
  DeleteTable on PITR-ENABLED table A -> SYSTEM backup ackq-96ea81-sb-a$DeletedTableBackup (first seen 8.3s
  after delete; expiry 2026-11-12 23:31:32.805000+00:00). Table B (PITR on, UpdateContinuousBackups(disable)
  issued while DELETING -> {'ok': True, 'code': None, 'http': 200, 'message': ''}, Describe then read
  DISABLED) -> SYSTEM backup None. Control table C (PITR never enabled) -> None. DeleteBackup on the system
  backup -> {'ok': False, 'code': 'ValidationException', 'http': 400, 'message': 'Invalid Request: User is not
  allowed to delete the system backup with arn
  arn:aws:dynamodb:us-west-2:<ACCOUNT>:table/ackq-96ea81-sb-a/backup/01791502292805-00000000. It will
  automatically expire on'}. RestoreTableFromBackup from it -> {'ok': True, 'code': None, 'http': 200,
  'message': ''}. Polled 249.9s.
  - ACK: pre-delete-cleanup, docs-only · ops: DeleteTable, ListBackups, DeleteBackup, UpdateContinuousBackups
  - repro: enable PITR; DeleteTable; (B: UpdateContinuousBackups disable while DELETING);
    ListBackups(BackupType=SYSTEM)
  - measurements: first_seen_after_delete_s.a=8.3
  - handling: not handled in the controller (as of commit 34b85e6)
  - related: [DDB-BACKUP-015](backup.md#ddb-backup-015), [DDB-BACKUP-014](backup.md#ddb-backup-014), [DDB-BACKUP-006](backup.md#ddb-backup-006), [DDB-TABLE-336](table-streams-encryption-class.md#ddb-table-336) · evidence:
    table/creative/pitr-delete-system-backup
  - notes: Hypotheses: H-S-131. Earlier sibling probes saw no SYSTEM backup for two PITR tables that both
    received UpdateContinuousBackups(disable) while DELETING - see behavior for whether that is the cause.

- <a id="ddb-table-342"></a>**DDB-TABLE-342** `delete-semantics` · impact medium · unhandled (not handled in controller) · verified 2026-10-09
  **DeleteTable with insights ENABLED needs no DISABLE: table and its CloudWatch rules vanish together (~6s); Describe -> ResourceNotFound**
  DeleteTable on a hash+range table whose insights were ENABLED (4 CloudWatch rules) -> 200. Polling every 2
  s: the table was gone (ResourceNotFoundException) at 6.1s and all 4 rules disappeared from
  DescribeInsightRules at 6.1s (rule counts per sample: [[0.0, 4], [2.1, 4], [4.1, 4], [6.1, 0]]).
  DescribeContributorInsights afterwards -> ResourceNotFoundException 'Requested resource not found: Table:
  ackq-72c077-iq-r01 not found'. Final CloudWatch rule count after deleting all 31 tables = baseline (0).
  - ACK: pre-delete-cleanup · ops: DeleteTable, DescribeInsightRules, DescribeContributorInsights · fields:
    ContributorInsightsStatus
  - repro: Table with insights ENABLED -> DeleteTable -> poll DescribeTable + cloudwatch:DescribeInsightRules
    every 2 s
  - measurements: table_gone_s=6.1, rules_gone_s=6.1
  - handling: not handled in the controller (as of commit 34b85e6)
  - related: [DDB-TABLE-089](#ddb-table-089), [DDB-TABLE-090](#ddb-table-090), [DDB-TABLE-092](table-streams-encryption-class.md#ddb-table-092), [DDB-TABLE-093](#ddb-table-093), [DDB-TABLE-095](#ddb-table-095), [DDB-TABLE-097](#ddb-table-097),
    [DDB-TABLE-112](#ddb-table-112), [DDB-TABLE-122](table-policy-kinesis-autoscaling.md#ddb-table-122), [DDB-TABLE-236](table-policy-kinesis-autoscaling.md#ddb-table-236), [DDB-TABLE-374](service.md#ddb-table-374), [DDB-TABLE-453](#ddb-table-453), [DDB-TABLE-088](#ddb-table-088), [DDB-TABLE-235](table-policy-kinesis-autoscaling.md#ddb-table-235),
    [DDB-TABLE-214](table-policy-kinesis-autoscaling.md#ddb-table-214), [DDB-TABLE-271](table-policy-kinesis-autoscaling.md#ddb-table-271), [DDB-TABLE-436](table-policy-kinesis-autoscaling.md#ddb-table-436), [DDB-TABLE-377](service.md#ddb-table-377), [DDB-TABLE-337](#ddb-table-337), [DDB-TABLE-439](#ddb-table-439), [DDB-TABLE-338](#ddb-table-338),
    [DDB-TABLE-339](#ddb-table-339), [DDB-TABLE-340](#ddb-table-340), [DDB-TABLE-341](#ddb-table-341) · hypotheses: H-S-126 · evidence:
    table/limits/insights-rule-quota
  - notes: No pre-delete DISABLE is needed (the service removes the rules with the table, same as for GSIs in
    [DDB-TABLE-112](#ddb-table-112)); the quota is released as soon as the table is gone.

- <a id="ddb-table-453"></a>**DDB-TABLE-453** `delete-semantics` · impact high · unhandled (not handled in controller) · verified 2026-10-09
  **PITR enable issued while a never-PITR table is DELETING is accepted (200 ENABLED) for ~1 s and creates the undeletable 35-day SYSTEM backup**
  Table created without PITR (PointInTimeRecoveryStatus DISABLED), DeleteTable -> DELETING.
  UpdateContinuousBackups(PointInTimeRecoveryEnabled=true) every 200 ms: 200 with
  PointInTimeRecoveryStatus=ENABLED from +0.03 s to +1.03 s (6 calls), then TableNotFoundException 'Table not
  found: <name>' from +1.23 s while DescribeTable still reports DELETING until +5.23 s. ~8 s later
  ListBackups(BackupType=SYSTEM) shows '<name>$DeletedTableBackup' (AVAILABLE, 0 bytes, BackupExpiryDateTime =
  +35 days). Reproduced with a single UpdateContinuousBackups(enable) at +0.88 s on a second never-PITR table
  (200 ENABLED; SYSTEM backup created at +5 s). DescribeBackup shows SourceTableFeatureDetails={} and
  DeleteBackup fails with ValidationException 'User is not allowed to delete the system backup with arn ... It
  will automatically expire on 2026-11-13T...'. A same-name re-create afterwards reads
  PointInTimeRecoveryStatus DISABLED (no leak).
  - ACK: pre-delete-cleanup, deletable.when, custom_delete · ops: UpdateContinuousBackups, DeleteTable,
    ListBackups, DeleteBackup · fields: PointInTimeRecoverySpecification, BackupType
  - repro: CreateTable (no PITR) -> ACTIVE; DeleteTable;
    UpdateContinuousBackups(PointInTimeRecoveryEnabled=true) within ~1 s; ListBackups(TableName,
    BackupType=SYSTEM) 10 s later
  - measurements: enable_accepted_window_s=[0.03, 1.03], system_backup_visible_after_s=8,
    backup_retention_days=35, trials=2
  - handling: not handled in the controller (as of commit 34b85e6)
  - related: [DDB-TABLE-088](#ddb-table-088), [DDB-TABLE-167](#ddb-table-167), [DDB-BACKUP-015](backup.md#ddb-backup-015), [DDB-TABLE-113](#ddb-table-113), [DDB-TABLE-122](table-policy-kinesis-autoscaling.md#ddb-table-122), [DDB-TABLE-236](table-policy-kinesis-autoscaling.md#ddb-table-236),
    [DDB-TABLE-374](service.md#ddb-table-374), [DDB-TABLE-097](#ddb-table-097), [DDB-TABLE-235](table-policy-kinesis-autoscaling.md#ddb-table-235), [DDB-TABLE-342](#ddb-table-342), [DDB-TABLE-214](table-policy-kinesis-autoscaling.md#ddb-table-214) · evidence:
    table/creative/deleting-lasting-effects, table/creative/clobber-followups
  - notes: Inverse of [DDB-TABLE-088](#ddb-table-088) (a disable issued while DELETING suppresses the backup). A controller
    whose PITR sync races a deletion (user deleted the table out of band, or the finalizer and the spec sync
    run in the same second) leaves a 35-day artifact in the account that nobody can delete. Two such...
  - full notes: [details/DDB-TABLE-453.md](details/DDB-TABLE-453.md)

- <a id="ddb-table-454"></a>**DDB-TABLE-454** `delete-semantics` · impact medium · unhandled (not handled in controller) · verified 2026-10-09
  **ExportTableToPointInTime on a DELETING source: accepted in the first ~1 s and COMPLETES after the table is gone; +1.5/+3 s -> TableNotFound**
  PITR-on 1-item table. DeleteTable, then ExportTableToPointInTime(fresh ClientToken) at +0.04 s -> 200
  ExportStatus=IN_PROGRESS; the export reached COMPLETED ~2.5 min later (ItemCount=1, manifest written to S3)
  although DescribeTable had returned ResourceNotFoundException from +5.04 s. On two further PITR-on tables a
  single export call at +1.71 s and at +3.19 s (TableStatus still DELETING) failed with TableNotFoundException
  'Table not found: arn:aws:dynamodb:...:table/<name>'. RestoreTableToPointInTime(UseLatestRestorableTime) at
  +2.44 s on the DELETING source -> TableNotFoundException 'Table not found'.
  - ACK: pre-delete-cleanup, deletable.when · ops: ExportTableToPointInTime, RestoreTableToPointInTime,
    DeleteTable, DescribeExport
  - repro: PITR on; DeleteTable; ExportTableToPointInTime at +0.05 s (200) / +1.5 s (TableNotFound);
    DescribeExport until terminal
  - measurements: export_accepted_at_s=0.04, export_in_progress_s=15.1, export_rejected_at_s=[1.71, 3.19],
    restore_rejected_at_s=2.44
  - handling: not handled in the controller (as of commit 34b85e6)
  - related: [DDB-EXPORT-013](export.md#ddb-export-013), [DDB-EXPORT-020](export.md#ddb-export-020), [DDB-TABLE-100](table-restore.md#ddb-table-100), [DDB-TABLE-374](service.md#ddb-table-374), [DDB-BACKUP-001](backup.md#ddb-backup-001), [DDB-BACKUP-007](backup.md#ddb-backup-007),
    [DDB-TABLE-118](service.md#ddb-table-118), [DDB-TABLE-087](table-restore.md#ddb-table-087), [DDB-TABLE-455](service.md#ddb-table-455), [DDB-TABLE-227](table-replicas.md#ddb-table-227), [DDB-TABLE-228](table-replicas.md#ddb-table-228), [DDB-EXPORT-021](export.md#ddb-export-021), [DDB-EXPORT-022](export.md#ddb-export-022),
    [DDB-EXPORT-016](export.md#ddb-export-016), [DDB-EXPORT-009](export.md#ddb-export-009), [DDB-EXPORT-011](export.md#ddb-export-011), [DDB-EXPORT-012](export.md#ddb-export-012) · evidence:
    table/creative/deleting-lasting-effects, table/creative/clobber-followups
  - notes: The PITR backend (export, restore, UpdateContinuousBackups, DescribeContinuousBackups,
    CreateBackup) forgets the table ~1.0-1.6 s into DELETING, ~4 s before DescribeTable does. An export job
    started in that window outlives the table as an immutable export record plus S3 objects.
  - full notes: [details/DDB-TABLE-454.md](details/DDB-TABLE-454.md)

## Quotas and rate limits

- <a id="ddb-table-360"></a>**DDB-TABLE-360** `quota-limit` · impact medium · handled · verified 2026-10-09
  **Read bursts on one table, no retries: ListTagsOfResource never throttled, DescribeTimeToLive throttles (30 sequential + 30/60 concurrent)**
  ListTagsOfResource.sequential: 30 calls in 0.218 s (137.4/s) -> {'OK': 30}; ListTagsOfResource.concurrent:
  30 calls in 0.208 s (144.1/s) -> {'OK': 30}; ListTagsOfResource.concurrent60: 60 calls in 0.288 s (208.1/s)
  -> {'OK': 60}; DescribeTimeToLive.sequential: 30 calls in 0.207 s (145.1/s) -> {'OK': 4,
  'ThrottlingException': 26} [ThrottlingException HTTP 400 'Rate exceeded'; first at index 4; retry after 1 s:
  {'ok': False, 'code': 'ThrottlingException', 'latency_ms': 6}]; DescribeTimeToLive.concurrent: 30 calls in
  0.181 s (165.4/s) -> {'OK': 30}; DescribeTable.sequential: 30 calls in 0.289 s (103.7/s) -> {'OK': 30};
  DescribeTable.concurrent: 30 calls in 0.13 s (230.3/s) -> {'OK': 30}.
  - ACK: requeue, e2e-timing · ops: ListTagsOfResource, DescribeTimeToLive, DescribeTable
  - repro: ACTIVE PPR table; botocore total_max_attempts=1; 30 sequential then 30 concurrent
    ListTagsOfResource(ResourceArn); same for DescribeTimeToLive; 60 concurrent ListTagsOfResource
  - measurements: list_tags_sequential_rate_per_s=137.4, list_tags_concurrent30_wall_s=0.208,
    list_tags_concurrent60_wall_s=0.288, describe_ttl_sequential_rate_per_s=145.1,
    describe_ttl_concurrent30_wall_s=0.181
  - handling: handled via `templates/hooks/table/sdk_read_one_post_set_output.go.tpl:57-72; bcd26e1`
  - related: [DDB-TABLE-099](service.md#ddb-table-099), [DDB-TABLE-130](service.md#ddb-table-130), [DDB-TABLE-053](service.md#ddb-table-053), [DDB-TABLE-403](service.md#ddb-table-403), [DDB-TABLE-445](service.md#ddb-table-445) · hypotheses: H-T-131 ·
    evidence: table/limits/read-api-bursts
  - notes: Qualifies H-T-131: the documented 10/s ListTagsOfResource limit is NOT enforced per table at these
    burst sizes. DescribeTimeToLive (documented 10/s) throttled - code/message above.
  - full notes: [details/DDB-TABLE-360.md](details/DDB-TABLE-360.md)

## Scope

- <a id="ddb-table-094"></a>**DDB-TABLE-094** `scope` · impact high · unhandled (not handled in controller) · verified 2026-10-08
  **Contributor Insights is per table and per GSI; LSI, unknown index, missing table (incl. List) -> ResourceNotFoundException**
  **Scope verdict: implement**
  Table ENABLE and GSI ENABLE back-to-back: 200 OK status=ENABLING / 200 OK status=ENABLING (both transition
  independently: table [{'value': 'ENABLING', 'from_s': 0.01, 'to_s': 1.03, 'duration_s': 1.02}, {'value':
  'ENABLED', 'from_s': 1.03, 'to_s': None, 'duration_s': None}], gsi [{'value': 'ENABLING', 'from_s': 0.01,
  'to_s': 1.03, 'duration_s': 1.02}, {'value': 'ENABLED', 'from_s': 1.03, 'to_s': None, 'duration_s': None}]).
  Describe(IndexName=LSI) -> ResourceNotFoundException (HTTP 400) 'Requested resource not found: Index: lsi1
  not found for table: ackq-244c78-i2'; Update(IndexName=LSI) -> ResourceNotFoundException (HTTP 400)
  'Requested resource not found: Index: lsi1 not found for table: ackq-244c78-i2'; Describe(bogus index) ->
  ResourceNotFoundException (HTTP 400) 'Requested resource not found: Index: nope not found for table:
  ackq-244c78-i2'; Update(bogus index) -> ResourceNotFoundException (HTTP 400) 'Requested resource not found:
  Index: nope not found for table: ackq-244c78-i2'; List(TableName=missing) -> ResourceNotFoundException.
  While the GSI table was CREATING: Describe(gsi) -> ResourceNotFoundException (HTTP 400) 'Requested resource
  not found: Table: ackq-244c78-i2 not found', Update(gsi) -> ResourceNotFoundException (HTTP 400) 'Requested
  resource not found: Table: ackq-244c78-i2 not found', Describe(table) -> ResourceNotFoundException (HTTP 400)
  'Requested resource not found: Table: ackq-244c78-i2 not found'. List(TableName) right after enabling:
  {'ok': True, 'NextToken': None, 'summaries': [{'TableName': 'ackq-244c78-i2', 'ContributorInsightsStatus':
  'ENABLING', 'ContributorInsightsMode': 'ACCESSED_AND_THROTTLED_KEYS'}, {'TableName': 'ackq-244c78-i2',
  'IndexName': 'gsi1', 'ContributorInsightsStatus': 'ENABLING', 'ContributorInsightsMode':
  'ACCESSED_AND_THROTTLED_KEYS'}]}.
  - ACK: custom_update, exceptions.404, terminal_codes · ops: UpdateContributorInsights,
    DescribeContributorInsights, ListContributorInsights · fields: IndexName
  - repro: table with GSI+LSI: ENABLE table, ENABLE gsi, ENABLE lsi, ENABLE bogus; List
  - handling: not handled in the controller (as of commit 34b85e6)
  - related: [DDB-TABLE-161](#ddb-table-161), [DDB-TABLE-380](table-indexes.md#ddb-table-380), [DDB-TABLE-137](#ddb-table-137), [DDB-TABLE-116](#ddb-table-116), [DDB-TABLE-357](table-indexes.md#ddb-table-357), [DDB-TABLE-133](table-indexes.md#ddb-table-133),
    [DDB-TABLE-095](#ddb-table-095), [DDB-TABLE-344](#ddb-table-344), [DDB-TABLE-093](#ddb-table-093), [DDB-TABLE-112](#ddb-table-112) · evidence:
    table/sub-resources/insights-lifecycle
  - notes: Hypotheses: H-S-023, H-S-025, H-S-128. H-S-023 confirmed (LSI and bogus index ->
    ResourceNotFoundException 'Index: <name> not found for table: <t>'; List on a missing table ->
    ResourceNotFoundException, not an empty page). H-S-025: while the table was CREATING every insights call
    (table-level and...
  - full notes: [details/DDB-TABLE-094.md](details/DDB-TABLE-094.md)

## Handling gaps (bugs to file)

- [DDB-TABLE-143](#ddb-table-143) - TTL cooldown is real and ~31 min: 2nd UpdateTimeToLive -> ValidationException even though
  Describe shows ENABLED; invisible in Describe (suspected bug)
  Suspected controller bug confirmed by evidence: The cooldown is real: a 2nd UpdateTimeToLive 8 s after a
  successful enable -> ValidationException 'Time to live has been modified multiple times within a fixed
  interval', rejected for ~31 min (1872 s), each successful change starts a new window, and DescribeTimeToLive
  exposes nothing about it. Since the controller treats ValidationException as terminal and only tolerates the
  'already disabled' prefix, any TTL spec edit within ~31 min of the previous change (including the
  controller's own post-create enable) parks the CR in ACK.Terminal until the spec changes again.
  [DDB-TABLE-147](#ddb-table-147) compounds it: re-sending enable or changing the attribute name -> 'already enabled' / 'active
  on a different AttributeName' (also ValidationException), and a rename requires disable -> cooldown ->
  enable, so the controller cannot complete a rename without tolerating/requeueing on these messages.
  - handling_ref: `pkg/resource/table/hooks_ttl.go:85-92; test/e2e/tests/test_table.py:385-386;
    pkg/resource/table/hooks.go:210-218; 4ea832a; generator.yaml:88-90; pkg/resource/table/hooks.go:210-218`

## E2E timing

Values are seconds unless the key says otherwise; n = trials behind the numbers ('1 run' when the finding records none).

| finding | what | measurements | n |
| --- | --- | --- | --- |
| [DDB-TABLE-085](#ddb-table-085) | UpdateContinuousBackups is synchronous and idempotent, but a disable/enable flap resets EarliestRestorableDateTime | flap_earliest_reset_delta_s=20 | 1 run |
| [DDB-TABLE-090](#ddb-table-090) | UpdateContributorInsights(ENABLE) returns ENABLING synchronously; ENABLING settles in 1-16 s, DISABLING in ~2 s; no cooldown between toggles | enabling_s=[16.19, 1.01, 1.02, 1.02, 1.01, 1.02], disabling_s=[2.02, 2.02, 2.03] | 1 run |
| [DDB-TABLE-111](#ddb-table-111) | ContinuousBackupsStatus reads DISABLED for ~3 s after TableStatus=ACTIVE on a fresh table, then flips to ENABLED by itself | table_not_found_window_s=3.08, disabled_window_s=7.15, disabled_after_active_s=3.1, create_to_active_s=7.2 | 1 run |
| [DDB-TABLE-116](#ddb-table-116) | GSI table: backups usable 1.4s after ACTIVE; insights on a CREATING GSI: RNF 'Index not found' ~20s, then 'IndexStatus must be ACTIVE' | gsi_table_create_to_active_s=16.1, cb_update_first_ok_after_active_s=1.4, insights_index_update_first_ok_after_active_s=0.1, gsi_add_table_updating_s=42, gsi_add_index_creating_s=468.5 | 1 run |
| [DDB-TABLE-143](#ddb-table-143) | TTL cooldown is real and ~31 min: 2nd UpdateTimeToLive -> ValidationException even though Describe shows ENABLED; invisible in Describe | cooldown_until_success_s=1871.7, last_rejection_s=1811.6, first_rejection_s=8.1, retry_interval_s=60, enabling_state_observed=false | 1 run |
| [DDB-TABLE-145](#ddb-table-145) | UpdateTimeToLive echoes the request (TimeToLiveSpecification{Enabled,AttributeName}); Describe uses TimeToLiveDescription{TimeToLiveStatus} | enabled_visible_within_s=2.1, enabling_state_observed=false | 1 run |
| [DDB-TABLE-167](#ddb-table-167) | Deleting a PITR table creates an undeletable 35-day SYSTEM backup '<table>$DeletedTableBackup' in ~8 s - unless PITR is disabled while DE... | first_seen_after_delete_s.a=8.3 | 1 run |
| [DDB-TABLE-337](#ddb-table-337) | Insights ENABLE past the CW rule quota (100): 200 ENABLING, then FAILED in ~2s, FailureException LimitExceededException; no partial rules | quota=100.0, rules_before_failed=98, enabling_s_before_failed=2.02, enable_settle_s_min=1.05, enable_settle_s_max=3.07, tables_enabled=25, create_31_tables_all_active_s=8.8 | 1 run |
| [DDB-TABLE-338](#ddb-table-338) | FAILED insights are sticky: no auto-recovery when quota frees (45s); re-ENABLE exhausted -> FAILED in 2s; after freeing -> ENABLED in 2s | auto_recovery_polls_s=45, reenable_after_free_settle_s=2.16, reenable_exhausted_settle_s=2.14 | 1 run |
| [DDB-TABLE-339](#ddb-table-339) | DISABLE on a FAILED insights entry: DISABLING (FailureException still shown) -> DISABLED in ~2s, FailureException cleared, leaves List | disabling_s=2.16 | 1 run |
| [DDB-TABLE-341](#ddb-table-341) | Re-ENABLE same mode = synchronous no-op (response ENABLED); narrowing to THROTTLED_KEYS at the quota works (4 -> 2 rules, same timestamp) | mode_change_settle_s=2.14 | 1 run |
| [DDB-TABLE-342](#ddb-table-342) | DeleteTable with insights ENABLED needs no DISABLE: table and its CloudWatch rules vanish together (~6s); Describe -> ResourceNotFound | table_gone_s=6.1, rules_gone_s=6.1 | 1 run |
| [DDB-TABLE-343](#ddb-table-343) | No insights ghosts: an enabled-then-disabled table leaves ListContributorInsights the moment Describe says DISABLED; DISABLING is listed | disabling_s=3.08, ghost_polls_30s=30 | 1 run |
| [DDB-TABLE-344](#ddb-table-344) | Account-wide ListContributorInsights(MaxResults=N) returns EMPTY pages with a NextToken (42 pages for 9 live table/GSI entries at N=1) | pages_max_results_1=42, tables_in_region=6, gsis_in_region=3 | 1 run |
| [DDB-TABLE-345](#ddb-table-345) | Doc claim C014 FALSE: a caller with dynamodb:* only (no cloudwatch:*) enables Contributor Insights fine (ENABLED in ~1 s)... | restricted_enable_to_terminal_s=1.0 | 1 run |
| [DDB-TABLE-360](#ddb-table-360) | Read bursts on one table, no retries: ListTagsOfResource never throttled, DescribeTimeToLive throttles (30 sequential + 30/60 concurrent) | list_tags_sequential_rate_per_s=137.4, list_tags_concurrent30_wall_s=0.208, list_tags_concurrent60_wall_s=0.288, describe_ttl_sequential_rate_per_s=145.1, describe_ttl_concurrent30_wall_s=0.181 | 1 run |
| [DDB-TABLE-385](#ddb-table-385) | Doc claim C012 TRUE: LatestRestorableDateTime = max(enable time, now - 300 s): exactly 5 min behind once PITR has been on for 5 min | lag_latest_s_by_since_enable_s.0=0.0, lag_latest_s_by_since_enable_s.3=3.7, lag_latest_s_by_since_enable_s.19=19.1, lag_latest_s_by_since_enable_s.22=22.7, lag_latest_s_by_since_enable_s.27=27.1, lag_latest_s_by_since_enable_s.87=87.3, lag_latest_s_by_since_enable_s.147=147.4, lag_latest_s_by_since_enable_s.207=207.6, lag_latest_s_by_since_enable_s.267=267.7, lag_latest_s_by_since_enable_s.327=300.0, lag_latest_s_by_since_enable_s.388=300.0, lag_latest_s_by_since_enable_s.448=300.0, lag_latest_s_by_since_enable_s.508=300.0, lag_latest_s_by_since_enable_s.568=300.0, lag_latest_s_by_since_enable_s.600=300.0, steady_state_lag_s=300.0 | 1 run |
| [DDB-TABLE-453](#ddb-table-453) | PITR enable issued while a never-PITR table is DELETING is accepted (200 ENABLED) for ~1 s and creates the undeletable 35-day SYSTEM backup | enable_accepted_window_s=[0.03, 1.03], system_backup_visible_after_s=8, backup_retention_days=35 | 2 |
| [DDB-TABLE-454](#ddb-table-454) | ExportTableToPointInTime on a DELETING source: accepted in the first ~1 s and COMPLETES after the table is gone; +1.5/+3 s -> TableNotFound | export_accepted_at_s=0.04, export_in_progress_s=15.1, export_rejected_at_s=[1.71, 3.19], restore_rejected_at_s=2.44 | 1 run |

## Open questions

- [DDB-TABLE-416](#ddb-table-416) (unverified) - Doc claim C048 UNTESTABLE: TTL deletions are removed from LSIs/GSIs
  immediately, eventually consistent like a standard delete: VERDICT: UNTESTABLE - data-plane (item
  expiry/propagation) behavior, outside the control-plane scope of this lab

<!-- preserved:start id=open-questions -->
<!-- open questions and follow-up experiments; survives re-renders -->
<!-- preserved:end -->

## Appendix: low-impact and duplicate findings

| id | category | impact | status | title | related | duplicate_of |
| --- | --- | --- | --- | --- | --- | --- |
| <a id="ddb-table-093"></a>**DDB-TABLE-093** | response-fidelity | low | confirmed | ContributorInsightsRuleList: 2 rules hash-only, 4 hash+range table, 2 hash-only GSI; names change across disable/re-enable | [DDB-TABLE-094](#ddb-table-094), [DDB-TABLE-095](#ddb-table-095), [DDB-TABLE-344](#ddb-table-344), [DDB-TABLE-112](#ddb-table-112), [DDB-TABLE-116](#ddb-table-116) | - |
| <a id="ddb-table-096"></a>**DDB-TABLE-096** | delete-semantics | medium | confirmed | Deleting a GSI with Contributor Insights ENABLED is accepted; afterwards Describe(gsi) -> ResourceNotFoundException, CW rules gone | - | [DDB-TABLE-112](#ddb-table-112) |
| <a id="ddb-table-113"></a>**DDB-TABLE-113** | delete-semantics | low | confirmed | No SYSTEM backup even ~15 min after deleting a PITR table whose PITR was disabled while DELETING | [DDB-TABLE-088](#ddb-table-088), [DDB-TABLE-453](#ddb-table-453) | [DDB-TABLE-088](#ddb-table-088) |
| <a id="ddb-table-345"></a>**DDB-TABLE-345** | prerequisite | low | confirmed | Doc claim C014 FALSE: a caller with dynamodb:* only (no cloudwatch:*) enables Contributor Insights fine (ENABLED in ~1 s)... | [DDB-TABLE-090](#ddb-table-090), [DDB-TABLE-089](#ddb-table-089) | - |
| <a id="ddb-table-385"></a>**DDB-TABLE-385** | other | low | confirmed | Doc claim C012 TRUE: LatestRestorableDateTime = max(enable time, now - 300 s): exactly 5 min behind once PITR has been on for 5 min | [DDB-TABLE-086](table-restore.md#ddb-table-086) | - |
| <a id="ddb-table-399"></a>**DDB-TABLE-399** | other | low | confirmed | Doc claim C027 TRUE: ListContributorInsights MaxResults caps the page - MaxResults=1 yields one summary per page plus NextToken | [DDB-TABLE-095](#ddb-table-095) | - |
| <a id="ddb-table-416"></a>**DDB-TABLE-416** | other | low | unverified | Doc claim C048 UNTESTABLE: TTL deletions are removed from LSIs/GSIs immediately, eventually consistent like a standard delete | - | - |
| <a id="ddb-table-439"></a>**DDB-TABLE-439** | quota-limit | medium | confirmed | Contributor Insights shares the CloudWatch quota of 100 rules/region... | [DDB-TABLE-093](#ddb-table-093), [DDB-TABLE-090](#ddb-table-090), [DDB-TABLE-089](#ddb-table-089), [DDB-TABLE-337](#ddb-table-337), [DDB-TABLE-338](#ddb-table-338), [DDB-TABLE-339](#ddb-table-339), [DDB-TABLE-340](#ddb-table-340), [DDB-TABLE-341](#ddb-table-341), [DDB-TABLE-342](#ddb-table-342) | [DDB-TABLE-337](#ddb-table-337) |

## Supplementary notes

<!-- preserved:start -->
<!-- preserved:end -->
