<!-- generated: ack-api-quirks dynamodb 2026-10-09 model=2012-08-10 controller=34b85e6 -->
# Table resource policy, Kinesis streaming destination and replica auto scaling
_Put/Get/DeleteResourcePolicy semantics, Kinesis streaming destination lifecycle, and the Application Auto Scaling facade behind UpdateTableReplicaAutoScaling._
Generated from ack-api-quirks `services/dynamodb` (render date in the marker above); model 2012-08-10 (service/dynamodb v1.39.8); controller commit 34b85e6; evidence: `services/dynamodb/probes/<probe id>/` in the lab repo.

## Overview

<!-- preserved:start id=overview -->
This document covers the Table sub-resources that have their own API pairs and their own consistency model: the resource policy (Put/Get/DeleteResourcePolicy, RevisionId), the Kinesis streaming destination (Enable/Disable/Update/DescribeKinesisStreamingDestination) and the Application Auto Scaling (AAS) facade behind UpdateTableReplicaAutoScaling/DescribeTableReplicaAutoScaling, plus which of these calls are admitted in each table state. The most surprising facts are that policy reads lag writes by ~1-2.7 s while enforcement lags by 221-237 s, that every effective policy change opens a 15 s per-table write cooldown surfacing as ResourceInUseException and then ThrottlingException, that the Kinesis APIs never return ResourceNotFoundException for a destination and never validate the stream synchronously, and that an AAS target without a policy reads as AutoScalingDisabled:true yet enforces its minimum ([DDB-TABLE-346](#ddb-table-346), [DDB-TABLE-324](#ddb-table-324), [DDB-TABLE-206](#ddb-table-206), [DDB-TABLE-318](#ddb-table-318), [DDB-TABLE-195](#ddb-table-195)). TTL, PITR and Contributor Insights are in table-subresources.md.

### Rules a reconciler must respect
- The resource policy is not read-your-writes: 0/30 immediate Gets after a Put returned the new RevisionId - Get serves PolicyNotFoundException (fresh Put) or the OLD document with HTTP 200 (replace; the deleted document after a Delete) for ~1.0-2.7 s, flips exactly once and never back, so the first observation of the new state is final; the RevisionId is the epoch-ms change time and an equivalent re-Put (whitespace, key order, 1-element list vs string, account id vs root ARN) is a 200 no-op with the same RevisionId - the controller's hooks catalog relies on these idempotent re-Puts ([GT-DDB-056](service.md#gt-ddb-056) (controller hooks catalog entry)) ([DDB-TABLE-248](#ddb-table-248), [DDB-TABLE-346](#ddb-table-346), [DDB-TABLE-244](#ddb-table-244), [DDB-TABLE-245](#ddb-table-245), [DDB-TABLE-246](#ddb-table-246), [DDB-TABLE-207](#ddb-table-207)).
- Policy writes are serialized per table: a Put of a different document or a Delete within 15 s of the last change is ResourceInUseException 'pending previous resource-based policy update' (~0.6-2 s) and then ThrottlingException 'modified within the previous 15000 milliseconds. Please try again after <ts>' whose timestamp is exactly RevisionId + 15 s and authoritative; read visibility does not unlock writes; ExpectedRevisionId = the RevisionId just returned by Put is never PolicyNotFoundException (the revision check already sees the new revision while Get does not) while a stale ExpectedRevisionId is PolicyNotFoundException 'did not match' on both Put and Delete; the controller handles neither the window nor the throttle text ([DDB-TABLE-206](#ddb-table-206), [DDB-TABLE-347](#ddb-table-347), [DDB-TABLE-209](#ddb-table-209); [DDB-TABLE-445](service.md#ddb-table-445), [DDB-TABLE-464](service.md#ddb-table-464), service.md).
- Compare policies on the canonical form: Get returns minified JSON in a fixed key order (Version, Statement / Sid, Effect, Principal, Action, Resource, Condition), 1-element Action/Resource lists as strings (multi-element lists verbatim), an account-id Principal as the root ARN and a duplicate [id, ARN] principal collapsed to one string, 'allow' as Allow and a Statement object as a 1-element list; a document sent without Version reads back with 2008-10-17 yet is NOT equivalent for idempotency, so re-Putting the Get output churns RevisionIds and runs into the 15 s cooldown (ping-pong); the controller's compareResourcePolicyDocument parses both documents and DeepEquals them (hooks_resource_policy.go:139-177; [GT-DDB-057](service.md#gt-ddb-057) (controller hooks catalog entry)), which still ping-pongs on the Version-less case - always send an explicit Version ([DDB-TABLE-208](#ddb-table-208), [DDB-TABLE-351](#ddb-table-351), [DDB-TABLE-352](#ddb-table-352), [DDB-TABLE-353](#ddb-table-353), [DDB-TABLE-355](#ddb-table-355), [DDB-TABLE-354](#ddb-table-354)).
- Validation and identity: a table that never had a policy answers Get with PolicyNotFoundException while Delete is an idempotent 200 with an empty body (after a real delete it echoes the deletion's RevisionId); ResourceArn must be a same-account/region/partition table ARN (bare name, other account/region/partition or an index ARN -> ValidationException; a missing or wrong-case table -> ResourceNotFoundException; a deleted table's ARN -> ResourceNotFoundException on Get/Put/Delete, so Delete is not idempotent across table deletion); Resource may name another table, '*' or this table's GSI (accepted), >20480 bytes is ValidationException not LimitExceededException, and duplicate Sids, a bare account-id string principal, an Allow to '*' ('unbounded access') and a non-existent principal are ValidationException ([DDB-TABLE-205](#ddb-table-205), [DDB-TABLE-212](#ddb-table-212), [DDB-TABLE-214](#ddb-table-214), [DDB-TABLE-211](#ddb-table-211), [DDB-TABLE-356](#ddb-table-356)).
- Policy enforcement lags the API: the first Deny ever applied to a table is enforced in ~2 s, every later change (delete, new Deny, replace with Allow) in 221-237 s although Get converges in ~2 s; a Deny on dynamodb:UpdateTable blocks every UpdateTable (even no-ops, by name or ARN) from ~4 s while TTL/PITR/tags/Delete pass; a Deny on dynamodb:DeleteTable blocks the owner's DeleteTable (AccessDeniedException, beats DP) and outlives its removal by ~226 s; the self-lockout check guards only PutResourcePolicy (a Deny on Put requires ConfirmRemoveSelfResourceAccess) while a Deny on DeleteResourcePolicy alone is accepted and blocks the caller's Delete a minute later; DeleteTable within ~1-2 s of an effective Put/Delete is ResourceInUseException 'has a pending resource-based policy update' while TableStatus=ACTIVE (a no-op re-Put does not open that window; DP and tag writes are admitted) ([DDB-TABLE-324](#ddb-table-324), [DDB-TABLE-432](#ddb-table-432), [DDB-TABLE-210](#ddb-table-210), [DDB-TABLE-270](#ddb-table-270), [DDB-TABLE-348](#ddb-table-348); [DDB-TABLE-431](table-streams-encryption-class.md#ddb-table-431), table-streams-encryption-class.md).
- Kinesis destination: Enable -> ENABLING 2-7 s -> ACTIVE, at most one live (non-DISABLED/ENABLE_FAILED) entry per table, and the ACTIVE entry carries only DestinationStatus and StreamArn (ApproximateCreationDateTimePrecision absent = MILLISECOND); every op on a non-ACTIVE entry is ValidationException - never ResourceNotFoundException, even for a never-attached or malformed ARN - and Enable never validates the stream synchronously (nonexistent, other-region, other-account or table ARN -> 200 ENABLING, then ENABLE_FAILED with a DestinationStatusDescription; ENABLE_FAILED entries accumulate one per bad ARN and a re-Enable flips rather than duplicates); Disable is ~2 s DISABLING and then a DISABLED entry lingers >=171 s (Describe lags Disable by 31-100 ms; Disable with a configuration is rejected), a precision Update holds UPDATING 124-152 s while a same-value Update is ValidationException 'Precision is already set'; a deleted Kinesis stream leaves the destination ACTIVE with no description and Update still succeeds; the destination is independent of DynamoDB Streams; DeleteTable is accepted with an ACTIVE or ENABLING destination and leaves the stream intact; none of this state machine is modelled in the controller ([DDB-TABLE-299](#ddb-table-299), [DDB-TABLE-300](#ddb-table-300), [DDB-TABLE-302](#ddb-table-302), [DDB-TABLE-318](#ddb-table-318), [DDB-TABLE-268](#ddb-table-268), [DDB-TABLE-301](#ddb-table-301), [DDB-TABLE-304](#ddb-table-304), [DDB-TABLE-319](#ddb-table-319), [DDB-TABLE-303](#ddb-table-303), [DDB-TABLE-235](#ddb-table-235)).
- Admission by table state: for the first ~1-2 s of CREATING every policy/Kinesis API is ResourceNotFoundException 'Table not found' (a create-time ResourcePolicy becomes readable at ~2.1 s while still CREATING); while UPDATING (stream toggle, TableClass switch) Put policy, Enable Kinesis and Insights ENABLE are admitted and complete normally; while DELETING, Get policy and Describe Kinesis still return 200 (the ACTIVE entry flips to UPDATING, not DISABLING), an identical policy re-Put and CreateBackup are 200 for ~1.6 s, a changed Put or a Delete is ResourceInUseException 'Table is being deleted' from +0.03 s and Enable/Disable Kinesis ValidationException, then everything is ResourceNotFoundException with no ghosts, and a same-name re-create reads fresh policy/Kinesis/Insights/TTL/tag state (0 stale reads); against a missing table the policy, Kinesis and autoscaling calls are ResourceNotFoundException while the ContinuousBackups pair is TableNotFoundException ([DDB-TABLE-234](#ddb-table-234), [DDB-TABLE-233](#ddb-table-233), [DDB-TABLE-460](#ddb-table-460), [DDB-TABLE-236](#ddb-table-236), [DDB-TABLE-122](#ddb-table-122), [DDB-TABLE-271](#ddb-table-271), [DDB-TABLE-436](#ddb-table-436), [DDB-TABLE-014](#ddb-table-014); [DDB-TABLE-374](service.md#ddb-table-374), service.md; [DDB-TABLE-119](table-streams-encryption-class.md#ddb-table-119), table-streams-encryption-class.md).
- Application Auto Scaling is per-region state the Table API never shows: DescribeTable carries no autoscaling indicator, so treat provisionedThroughput as unmanaged whenever a target exists; never-configured, policy-less and deregistered dimensions all read as {AutoScalingDisabled: true, ScalingPolicies: []} although a policy-less target still enforces its MinimumUnits; raising Min above the current capacity makes AAS issue its own UpdateTable within ~4 s, a manual value below Min is accepted and not re-enforced (none within 12 min), while a manual value inside the bounds on an idle table is scaled back to Min by the permanently-firing AlarmLow within ~82-92 s (burning a decrease); PolicyName is the policy identity (a re-send is a no-op, a new name REPLACES the old policy, one policy per dimension) and a minimal update reads back with the server name 'DynamoDB<Read|Write>CapacityUtilization:table/<name>', the service-linked role and no cooldowns; partial shapes (Min/Max without a policy, a policy without Min/Max, AutoScalingDisabled=true with Min/Max) are ValidationException; AutoScalingDisabled=true deregisters the target with its policies and alarms, leaves the sibling dimension untouched and applies in both regions of a global table ([DDB-TABLE-193](#ddb-table-193), [DDB-TABLE-195](#ddb-table-195), [DDB-TABLE-189](#ddb-table-189), [DDB-TABLE-243](#ddb-table-243), [DDB-TABLE-314](#ddb-table-314), [DDB-TABLE-315](#ddb-table-315), [DDB-TABLE-240](#ddb-table-240), [DDB-TABLE-237](#ddb-table-237), [DDB-TABLE-241](#ddb-table-241), [DDB-TABLE-242](#ddb-table-242), [DDB-TABLE-290](#ddb-table-290), [DDB-TABLE-198](#ddb-table-198); [DDB-TABLE-316](table-throughput-billing.md#ddb-table-316), table-throughput-billing.md).
- Autoscaling and indexes on PROVISIONED global tables: adding a GSI is ValidationException 'GSI write capacity should either be Pay-Per-Request or AutoScaled' while ANY existing GSI lacks write autoscaling, and once admitted DynamoDB auto-registers write autoscaling (Min 1 / Max 10 / target 70) for the new GSI in both regions; UpdateTableReplicaAutoScaling for a GSI that is still CREATING is ResourceNotFoundException and the GSI is absent from DescribeTableReplicaAutoScaling until ACTIVE, while deleting a GSI orphans its AAS target; switching the table to PAY_PER_REQUEST keeps the targets and policies (Describe shows Min/Max AND AutoScalingDisabled=true); the service-linked role appears as a side effect of global-table operations and table tags never cross the facade ([DDB-TABLE-291](#ddb-table-291), [DDB-TABLE-322](#ddb-table-322), [DDB-TABLE-323](#ddb-table-323), [DDB-TABLE-317](#ddb-table-317), [DDB-TABLE-191](#ddb-table-191), [DDB-TABLE-197](#ddb-table-197); [DDB-TABLE-188](table-replicas.md#ddb-table-188), table-replicas.md).

### Timing you should expect
- Policy visibility in Get: fresh Put min 1.05 / median 1.47 / max 1.71 s (n=6); replace min 1.47 / median 1.79 / max 2.73 s (n=6); delete -> PolicyNotFoundException min 0.84 / median 1.47 / max 2.32 s (n=6); at 100 ms polling the single flip came at 1.39-2.28 s (fresh), 1.18-2.30 s (replace) and 1.15-2.03 s (delete); the next different write is admitted 15.0-15.3 s after the change (ResourceInUse until ~0.6-2.4 s, Throttling after); DeleteTable is blocked until 1.04 s after a Put and 2.04 s after a policy Delete; enforcement: first Deny 2.05 s, Deny on UpdateTable 4.1 s, later changes 221-237 s (n=7); create-time policy: ResourceNotFound for 1.06 s, readable at 2.09 s, ACTIVE at 7.25 s ([DDB-TABLE-244](#ddb-table-244), [DDB-TABLE-245](#ddb-table-245), [DDB-TABLE-246](#ddb-table-246), [DDB-TABLE-346](#ddb-table-346), [DDB-TABLE-206](#ddb-table-206), [DDB-TABLE-347](#ddb-table-347), [DDB-TABLE-348](#ddb-table-348), [DDB-TABLE-324](#ddb-table-324), [DDB-TABLE-432](#ddb-table-432), [DDB-TABLE-233](#ddb-table-233)).
- Kinesis: ENABLING 2.0-7.1 s (n>=5), ENABLE_FAILED 1.01-2.11 s after Enable (61.7 s for an other-account ARN, 17.2 s when the stream is deleted mid-ENABLING), DISABLING ~1-2 s, DISABLED entry still listed at 171 s, precision UPDATING 124-152 s (3 runs; 123.6 s on a dead destination), Describe already shows ENABLING 71 ms after Enable but lags Disable by 31-100 ms, a dead stream watched ACTIVE for 293 s; DELETING with an ACTIVE destination lasted 5.4-6.1 s ([DDB-TABLE-299](#ddb-table-299), [DDB-TABLE-318](#ddb-table-318), [DDB-TABLE-268](#ddb-table-268), [DDB-TABLE-301](#ddb-table-301), [DDB-TABLE-300](#ddb-table-300), [DDB-TABLE-319](#ddb-table-319), [DDB-TABLE-304](#ddb-table-304), [DDB-TABLE-235](#ddb-table-235), [DDB-TABLE-236](#ddb-table-236)).
- Autoscaling: Min raised above capacity -> new RCU visible in DescribeTable at 4.4 s, ACTIVE at 33 s; manual RCU below Min untouched for 12 min (726 s watched); manual RCU inside the bounds reverted at 82 s, ACTIVE at Min at 92 s; a PROVISIONED -> PAY_PER_REQUEST switch of an autoscaled 2-replica table held UPDATING 218 s; Describe reflects a RegisterScalableTarget within 0.45 s ([DDB-TABLE-314](#ddb-table-314), [DDB-TABLE-315](#ddb-table-315), [DDB-TABLE-317](#ddb-table-317), [DDB-TABLE-195](#ddb-table-195); [DDB-TABLE-316](table-throughput-billing.md#ddb-table-316), table-throughput-billing.md).

### Known handling gaps in the controller
- No finding rendered in this document is stored as suspect-bug or partial (all 73 ids checked with `quirks finding show`), so the generated 'Handling gaps (bugs to file)' section below is empty by construction. The controller consequences above are derived from entries stored as handled or unhandled: the hooks catalog's idempotent re-Put strategy ([GT-DDB-056](service.md#gt-ddb-056) (controller hooks catalog entry)) and DeepEqual compare ([GT-DDB-057](service.md#gt-ddb-057) (controller hooks catalog entry)) leave the 15 s cooldown, the ThrottlingException retry-after text and the Version-less ping-pong unhandled ([DDB-TABLE-206](#ddb-table-206), [DDB-TABLE-347](#ddb-table-347), [DDB-TABLE-354](#ddb-table-354)), and the Kinesis destination and the autoscaling facade have no controller code at all ([DDB-TABLE-299](#ddb-table-299), [DDB-TABLE-195](#ddb-table-195)).

### Where to look next
- TTL, PITR and Contributor Insights, including the ~31 min TTL cooldown bug and the PITR-on-DELETING SYSTEM backup ([DDB-TABLE-143](table-subresources.md#ddb-table-143), [DDB-TABLE-453](table-subresources.md#ddb-table-453), table-subresources.md); the AAS prerequisite that wedges PROVISIONED replica adds and the TableReplicaAutoScaling scope verdict ([DDB-TABLE-188](table-replicas.md#ddb-table-188), [DDB-TABLEREPLICAAUTOSCALING-001](table-replicas.md#ddb-tablereplicaautoscaling-001), table-replicas.md); the decrease budget that AAS scale-ins consume ([DDB-TABLE-316](table-throughput-billing.md#ddb-table-316), [DDB-TABLE-184](table-throughput-billing.md#ddb-table-184), table-throughput-billing.md); the DP cooldown and tag lock that share the ThrottlingException retry-after format, and the account control-plane limiter ([DDB-TABLE-445](service.md#ddb-table-445), [DDB-TABLE-464](service.md#ddb-table-464), [DDB-TABLE-434](service.md#ddb-table-434), service.md); the Deny-on-DeleteTable finalizer trap and the DynamoDB Streams rules the Kinesis destination is independent of ([DDB-TABLE-431](table-streams-encryption-class.md#ddb-table-431), [DDB-TABLE-050](table-streams-encryption-class.md#ddb-table-050), table-streams-encryption-class.md). Evidence: services/dynamodb/probes/table/{sub-resources/{resource-policy,kinesis-destination,replica-autoscaling-facade},consistency-windows/{resource-policy-windows,policy-stale-read-sequence},state-machine/policy-kinesis-admissibility,error-taxonomy/{kinesis-destination-errors,autoscaling-validation},mutation-matrix/autoscaling-vs-throughput,dependencies/autoscaling-gsi-and-orphans,creative/{policy-enforcement-lag,policy-denies-finalizer,policy-kinesis-followups,gsi-add-on-provisioned-global-table}}/.

Entries below are generated from the lab findings; low-impact items are in the appendix, long notes under details/.
<!-- preserved:end -->

## At a glance

- canonical findings: 71 (high 41 / medium 18 / low 12); duplicates folded into the appendix: 2
- handling: handled 11 · partial 0 · tracked 0 · unhandled 53 · suspect-bug 0 · n-a 7 (tracked = handled/partial whose reference is an open GitHub issue; counted as not handled)
- re-verified: 5 · last_verified: 2026-10-08..2026-10-09 · model: 2012-08-10 (service/dynamodb v1.39.8)
- categories: async-state-machine 8, other 8, error-code 7, delete-semantics 6, normalization 6,
  request-validation 5, eventual-consistency 4, identity 4, prerequisite 4, response-fidelity 3,
  server-default 3, stale-response 3, idempotency 2, read-gap 2, requested-vs-effective 2,
  first-sync-destructive 1, quota-limit 1, sub-resource-api 1, tag-semantics 1
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
| DescribeContinuousBackups | read | TableName | TableNotFoundException, InternalServerError | no |
| DescribeContributorInsights | read | TableName | ResourceNotFoundException, InternalServerError | no |
| DescribeKinesisStreamingDestination | read | TableName | ResourceNotFoundException, InternalServerError | no |
| DescribeTable | read | TableName | ResourceNotFoundException, InternalServerError | no |
| DescribeTableReplicaAutoScaling | read | TableName | ResourceNotFoundException, InternalServerError | no |
| DescribeTimeToLive | read | TableName | ResourceNotFoundException, InternalServerError | no |
| DisableKinesisStreamingDestination | update | TableName, StreamArn | InternalServerError, LimitExceededException, ResourceInUseException, ResourceNotFoundException | no |
| EnableKinesisStreamingDestination | update | TableName, StreamArn | InternalServerError, LimitExceededException, ResourceInUseException, ResourceNotFoundException | no |
| GetItem | read | TableName, Key | ProvisionedThroughputExceededException, ResourceNotFoundException, RequestLimitExceeded, InternalServerError, ThrottlingException | no |
| GetResourcePolicy | read | ResourceArn | ResourceNotFoundException, InternalServerError, PolicyNotFoundException | no |
| ListContributorInsights | list | - | ResourceNotFoundException, InternalServerError | no |
| ListTagsOfResource | list | ResourceArn | ResourceNotFoundException, InternalServerError | yes |
| PutItem | create | TableName, Item | ConditionalCheckFailedException, ProvisionedThroughputExceededException, ResourceNotFoundException, ItemCollectionSizeLimitExceededException, TransactionConflictException, RequestLimitExceeded, InternalServerError, ReplicatedWriteConflictException, ThrottlingException | no |
| PutResourcePolicy | create | ResourceArn, Policy | ResourceNotFoundException, InternalServerError, LimitExceededException, PolicyNotFoundException, ResourceInUseException | no |
| TagResource | tag | ResourceArn, Tags | LimitExceededException, ResourceNotFoundException, InternalServerError, ResourceInUseException | no |
| UpdateContinuousBackups | update | TableName, PointInTimeRecoverySpecification | TableNotFoundException, ContinuousBackupsUnavailableException, InternalServerError | no |
| UpdateContributorInsights | update | TableName, ContributorInsightsAction | ResourceNotFoundException, InternalServerError | no |
| UpdateKinesisStreamingDestination | update | TableName, StreamArn | InternalServerError, LimitExceededException, ResourceInUseException, ResourceNotFoundException | no |
| UpdateTable | update | TableName | ResourceInUseException, ResourceNotFoundException, LimitExceededException, InternalServerError | no |
| UpdateTableReplicaAutoScaling | update | TableName | ResourceNotFoundException, ResourceInUseException, LimitExceededException, InternalServerError | no |
| UpdateTimeToLive | update | TableName, TimeToLiveSpecification | ResourceInUseException, ResourceNotFoundException, LimitExceededException, InternalServerError | no |

Also referenced by findings but not in the dynamodb model (other services or annotated variants):
DeleteScalingPolicy, PutScalingPolicy, RegisterScalableTarget

## State machine

- **BackupStatus**: CREATING, DELETED, AVAILABLE (transitional: CREATING)
- **ContributorInsightsStatus**: ENABLING, ENABLED, DISABLING, DISABLED, FAILED (transitional: ENABLING,
  DISABLING)
- **DestinationStatus**: ENABLING, ACTIVE, DISABLING, DISABLED, ENABLE_FAILED, UPDATING (transitional:
  ENABLING, DISABLING, UPDATING)
- **IndexStatus**: CREATING, UPDATING, DELETING, ACTIVE (transitional: CREATING, UPDATING, DELETING)
- **ReplicaStatus**: CREATING, CREATION_FAILED, UPDATING, DELETING, ACTIVE, REGION_DISABLED,
  INACCESSIBLE_ENCRYPTION_CREDENTIALS, ARCHIVING, ARCHIVED, REPLICATION_NOT_AUTHORIZED (transitional:
  CREATING, UPDATING, DELETING, ARCHIVING)
- **SSEStatus**: ENABLING, ENABLED, DISABLING, DISABLED, UPDATING (transitional: ENABLING, DISABLING,
  UPDATING)
- **TableStatus**: CREATING, UPDATING, DELETING, ACTIVE, INACCESSIBLE_ENCRYPTION_CREDENTIALS, ARCHIVING,
  ARCHIVED, REPLICATION_NOT_AUTHORIZED (transitional: CREATING, UPDATING, DELETING, ARCHIVING)
- **WitnessStatus**: CREATING, DELETING, ACTIVE (transitional: CREATING, DELETING)

- <a id="ddb-table-234"></a>**DDB-TABLE-234** `async-state-machine` · impact high · handled · verified 2026-10-09
  **First ~1-2 s of CREATING: every policy/Kinesis API -> ResourceNotFoundException (table not found); UPDATING: Put policy / Enable Kinesis OK**
  Right after CreateTable (TableStatus CREATING): GetResourcePolicy ResourceNotFoundException (HTTP 400)
  'Requested resource not found: Table: ackq-f57f9f-pk-b not found'; DescribeKinesisStreamingDestination
  ResourceNotFoundException (HTTP 400) 'Requested resource not found: Table: ackq-f57f9f-pk-b not found';
  PutResourcePolicy ResourceNotFoundException (HTTP 400) 'Requested resource not found: Table:
  ackq-f57f9f-pk-b not found'; DeleteResourcePolicy ResourceNotFoundException (HTTP 400) 'Requested resource
  not found: Table: ackq-f57f9f-pk-b not found'; EnableKinesisStreamingDestination ResourceNotFoundException
  (HTTP 400) 'Requested resource not found: Table: ackq-f57f9f-pk-b not found';
  UpdateKinesisStreamingDestination ResourceNotFoundException (HTTP 400) 'Requested resource not found: Table:
  ackq-f57f9f-pk-b not found'; DisableKinesisStreamingDestination ResourceNotFoundException (HTTP 400)
  'Requested resource not found: Table: ackq-f57f9f-pk-b not found'. After ACTIVE: Get ->
  PolicyNotFoundException (HTTP 400) 'Resource-based policy not found for the provided ResourceArn:
  arn:aws:dynamodb:us-west-2:<ACCOUNT>:table/ackq-f57f9f-pk-b', Kinesis list []. While UPDATING (UpdateTable
  PAY_PER_REQUEST->PROVISIONED returned TableStatus=UPDATING; UPDATING lasted 96.28s): Get
  PolicyNotFoundException (HTTP 400) 'Resource-based policy not found for the provided ResourceArn:
  arn:aws:dynamodb:us-west-2:<ACCOUNT>:table/ackq-f57f9f-pk-b'; Describe kinesis 200 OK; Put 200 OK; Delete
  right after that Put ResourceInUseException (HTTP 400) 'Attempt to change a resource which is still in use:
  Table is pending previous resource-based policy update: ackq-f57f9f-pk-b'; Enable 200 OK (DestinationStatus
  ENABLING) and it reached ACTIVE after 2.03s while the table was still UPDATING; Update(precision) during
  ENABLING ValidationException (HTTP 400) 'Table is not in a valid state to enable Kinesis Streaming
  Destination: Kinesis streaming is not in ACTIVE state. Updates are only allowed in ACTIVE st'; Disable
  during ENABLING ValidationException (HTTP 400) 'Table is not in a valid state to enable Kinesis Streaming
  Destination: KinesisStreamingDestination must be ACTIVE to perform DISABLE operation.'. TableStatus right
  after the mutators: UPDATING; the policy written during UPDATING persisted (200 OK).
  - ACK: synced.when, updateable.when, requeue, terminal_codes · ops: PutResourcePolicy, DeleteResourcePolicy,
    EnableKinesisStreamingDestination, UpdateKinesisStreamingDestination, DisableKinesisStreamingDestination,
    GetResourcePolicy, DescribeKinesisStreamingDestination
  - repro: CreateTable; immediately call every policy/kinesis API; wait ACTIVE;
    UpdateTable(BillingMode=PROVISIONED); immediately call every policy/kinesis API
  - measurements: updating_duration_s=96.28, kinesis_enabling_during_updating_s=2.03
  - handling: handled via `pkg/resource/table/hooks_resource_policy.go:97-103; pkg/resource/table/hooks_resource_policy.go:129-132; pkg/resource/table/hooks.go:175-196; 888fee6; templates/hooks/table/sdk_create_post_set_output.go.tpl:1-4; bcd26e1`
  - related: [DDB-TABLE-111](table-subresources.md#ddb-table-111), [DDB-TABLE-115](service.md#ddb-table-115), [DDB-TABLE-233](#ddb-table-233), [DDB-TABLE-114](service.md#ddb-table-114), [DDB-TABLE-299](#ddb-table-299), [DDB-TABLE-300](#ddb-table-300),
    [DDB-TABLE-301](#ddb-table-301), [DDB-TABLE-302](#ddb-table-302), [DDB-TABLE-304](#ddb-table-304), [DDB-TABLE-269](#ddb-table-269), [DDB-TABLE-268](#ddb-table-268), [DDB-TABLE-319](#ddb-table-319), [DDB-TABLE-303](#ddb-table-303),
    [DDB-TABLE-235](#ddb-table-235), [DDB-TABLE-236](#ddb-table-236), [DDB-TABLE-460](#ddb-table-460), [DDB-TABLE-117](table-streams-encryption-class.md#ddb-table-117), [DDB-TABLE-119](table-streams-encryption-class.md#ddb-table-119), [DDB-TABLE-287](table-streams-encryption-class.md#ddb-table-287), [DDB-TABLE-450](table-streams-encryption-class.md#ddb-table-450),
    [DDB-TABLE-435](table-streams-encryption-class.md#ddb-table-435), [DDB-TABLE-121](table-throughput-billing.md#ddb-table-121) · hypotheses: H-S-027, H-S-033, H-S-120 · evidence:
    table/state-machine/policy-kinesis-admissibility
  - notes: H-S-027: CREATING clause refuted (code is ResourceNotFoundException with the same 'Table: X not
    found' message as a missing table, NOT ResourceInUseException); UPDATING clause confirmed (Put succeeds,
    Enable succeeds too). Note the Delete right after a Put -> ResourceInUseException 'Table is pending...
  - full notes: [details/DDB-TABLE-234.md](details/DDB-TABLE-234.md)

- <a id="ddb-table-268"></a>**DDB-TABLE-268** `async-state-machine` · impact high · unhandled (not handled in controller) · verified 2026-10-09
  **ENABLE_FAILED entries accumulate (one per bad ARN, ~2.02s after Enable); Enable is rejected only while another entry is in flight/ACTIVE**
  Enable(nonexistent stream ARN n1) -> 200 OK DestinationStatus=ENABLING; timeline [('-none1=ENABLING', 2.02),
  ('-none1=ENABLE_FAILED', None)]; entry [{'stream': '-none1', 'status': 'ENABLE_FAILED', 'desc': 'User does
  not have a permission to use kinesis stream', 'precision': None}]. Enable(n1) again on its ENABLE_FAILED
  entry -> 200 OK; entries for n1 afterwards: 1 (flipped, not duplicated). Enable(n2) while n1 is
  ENABLE_FAILED -> 200 OK; entries [('-none2', 'ENABLE_FAILED'), ('-none1', 'ENABLE_FAILED')]. On the
  ENABLE_FAILED entry: Update -> ValidationException (HTTP 400) 'Table is not in a valid state to enable
  Kinesis Streaming Destination: Kinesis streaming is not in ACTIVE state. Updates are only allowed in ACTIVE
  state. TableName: ackq'; Disable -> ValidationException (HTTP 400) 'Table is not in a valid state to enable
  Kinesis Streaming Destination: KinesisStreamingDestination must be ACTIVE to perform DISABLE operation.'
  (timeline []; entries after [('-none2', 'ENABLE_FAILED', 'User does not have a permission to use kinesis
  stream'), ('-none1', 'ENABLE_FAILED', 'User does not have a permission to use kinesis stream')]).
  Enable(valid G1) alongside the failed entries -> 200 OK (timeline
  [('-fu-g1=ENABLING,-none2=ENABLE_FAILED,-none1=ENABLE_FAILED', 7.08),
  ('-fu-g1=ACTIVE,-none2=ENABLE_FAILED,-none1=ENABLE_FAILED', None)]). While G1 is ACTIVE: Enable(bad n3) ->
  ValidationException (HTTP 400) 'Table is not in a valid state to enable Kinesis Streaming Destination:
  EnableKinesisStreamingDestination must be DISABLED or ENABLE_FAILED to perform ENABLE operation.';
  Enable(valid G2) -> ValidationException (HTTP 400) 'Table is not in a valid state to enable Kinesis
  Streaming Destination: EnableKinesisStreamingDestination must be DISABLED or ENABLE_FAILED to perform ENABLE
  operation.'. After Disable(G1): Enable(n3) -> ValidationException (HTTP 400) 'Table is not in a valid state
  to enable Kinesis Streaming Destination: EnableKinesisStreamingDestination must be DISABLED or ENABLE_FAILED
  to perform ENABLE operation.' (entries [('-fu-g1', 'ACTIVE'), ('-none2', 'ENABLE_FAILED'), ('-none1',
  'ENABLE_FAILED')]). Re-Enable(G1) with the failed/disabled entries present -> ValidationException (HTTP 400)
  'Table is not in a valid state to enable Kinesis Streaming Destination: EnableKinesisStreamingDestination
  must be DISABLED or ENABLE_FAILED to perform ENABLE operation.'; final entries [('-fu-g1', 'ACTIVE'),
  ('-none2', 'ENABLE_FAILED'), ('-none1', 'ENABLE_FAILED')].
  - ACK: custom_update, list_operation.match_fields, requeue, terminal_codes, annotation-shadow-state · ops:
    EnableKinesisStreamingDestination, DescribeKinesisStreamingDestination,
    DisableKinesisStreamingDestination, UpdateKinesisStreamingDestination · fields:
    KinesisDataStreamDestinations[].DestinationStatus,
    KinesisDataStreamDestinations[].DestinationStatusDescription
  - repro: Enable(bogus ARN) -> poll -> ENABLE_FAILED; Enable(bogus) again; Enable(bogus2); Update/Disable the
    failed entry; Enable(valid); Enable(bogus3) while ACTIVE
  - measurements: enabling_to_enable_failed_s=2.02
  - handling: not handled in the controller (as of commit 34b85e6)
  - related: [DDB-TABLE-299](#ddb-table-299), [DDB-TABLE-300](#ddb-table-300), [DDB-TABLE-301](#ddb-table-301), [DDB-TABLE-302](#ddb-table-302), [DDB-TABLE-304](#ddb-table-304), [DDB-TABLE-269](#ddb-table-269),
    [DDB-TABLE-319](#ddb-table-319), [DDB-TABLE-303](#ddb-table-303), [DDB-TABLE-235](#ddb-table-235), [DDB-TABLE-236](#ddb-table-236), [DDB-TABLE-234](#ddb-table-234), [DDB-TABLE-460](#ddb-table-460) · hypotheses:
    H-S-034, H-S-037, H-S-038, H-S-113 · evidence: table/creative/policy-kinesis-followups
  - notes: H-S-037's 'ENABLE_FAILED entry for the same StreamArn blocks re-Enable with ResourceInUseException'
    is refuted (re-Enable is accepted and flips the entry). The admissibility rule is: a table may have at
    most one entry in ENABLING/ACTIVE/UPDATING/DISABLING; any number of DISABLED/ENABLE_FAILED...
  - full notes: [details/DDB-TABLE-268.md](details/DDB-TABLE-268.md)

- <a id="ddb-table-299"></a>**DDB-TABLE-299** `async-state-machine` · impact high · unhandled (not handled in controller) · verified 2026-10-09
  **Kinesis destination: ENABLING ~6 s then ACTIVE; at most one live (non-DISABLED/ENABLE_FAILED) entry per table; non-ACTIVE ops -> Validation**
  Fresh table: DescribeKinesisStreamingDestination -> 200 keys ['KinesisDataStreamDestinations', 'TableName'],
  KinesisDataStreamDestinations=[]; DescribeTable has no kinesis field. Enable(no config) -> 200 OK
  DestinationStatus=ENABLING, EnableKinesisStreamingConfiguration echoed as {} (empty object), response keys
  ['DestinationStatus', 'EnableKinesisStreamingConfiguration', 'StreamArn', 'TableName']; Describe 0.071s
  later already listed the entry as ENABLING. Timeline (destination, TableStatus, StreamSpecification
  present): [(['ks1=ENABLING', 'ACTIVE', False], 6.14), (['ks1=ACTIVE', 'ACTIVE', False], None)]. The ACTIVE
  entry has keys ['DestinationStatus', 'StreamArn'] only (no precision field). During ENABLING: Enable same
  stream -> ValidationException (HTTP 400) 'Table is not in a valid state to enable Kinesis Streaming
  Destination: EnableKinesisStreamingDestination must be DISABLED or ENABLE_FAILED to perform '; Enable
  another stream -> ValidationException (HTTP 400) 'Table is not in a valid state to enable Kinesis Streaming
  Destination: EnableKinesisStreamingDestination must be DISABLED or ENABLE_FAILED to perform '; Update ->
  ValidationException (HTTP 400) 'Table is not in a valid state to enable Kinesis Streaming Destination:
  Kinesis streaming is not in ACTIVE state. Updates are only allowed in ACTIVE st'; Disable ->
  ValidationException (HTTP 400) 'Table is not in a valid state to enable Kinesis Streaming Destination:
  KinesisStreamingDestination must be ACTIVE to perform DISABLE operation.'. While ACTIVE: Enable same stream
  -> ValidationException (HTTP 400) 'Table is not in a valid state to enable Kinesis Streaming Destination:
  EnableKinesisStreamingDestination must be DISABLED or ENABLE_FAILED to perform '; Enable with config ->
  ValidationException (HTTP 400) 'Table is not in a valid state to enable Kinesis Streaming Destination:
  EnableKinesisStreamingDestination must be DISABLED or ENABLE_FAILED to perform '; Enable a SECOND stream ->
  ValidationException (HTTP 400) 'Table is not in a valid state to enable Kinesis Streaming Destination:
  EnableKinesisStreamingDestination must be DISABLED or ENABLE_FAILED to perform '.
  - ACK: synced.when, requeue, custom_update, terminal_codes, one-per-reconcile · ops:
    EnableKinesisStreamingDestination, DescribeKinesisStreamingDestination, UpdateKinesisStreamingDestination,
    DisableKinesisStreamingDestination · fields: KinesisDataStreamDestinations[].DestinationStatus,
    EnableKinesisStreamingConfiguration, StreamArn
  - repro: CreateTable; Describe; Enable(K1); Describe immediately; Enable/Update/Disable during ENABLING;
    wait ACTIVE; Enable(K1); Enable(K2)
  - measurements: enabling_duration_s=6.14, describe_lag_after_enable_s=0.071
  - handling: not handled in the controller (as of commit 34b85e6)
  - related: [DDB-TABLE-300](#ddb-table-300), [DDB-TABLE-301](#ddb-table-301), [DDB-TABLE-302](#ddb-table-302), [DDB-TABLE-304](#ddb-table-304), [DDB-TABLE-269](#ddb-table-269), [DDB-TABLE-268](#ddb-table-268),
    [DDB-TABLE-319](#ddb-table-319), [DDB-TABLE-303](#ddb-table-303), [DDB-TABLE-235](#ddb-table-235), [DDB-TABLE-236](#ddb-table-236), [DDB-TABLE-234](#ddb-table-234), [DDB-TABLE-460](#ddb-table-460) · hypotheses:
    H-S-033, H-S-008, H-S-113, H-S-037, H-S-130 · evidence: table/sub-resources/kinesis-destination
  - notes: H-S-033: ENABLING->ACTIVE confirmed but lasts seconds (2-7s across probes), not 30-120s, and
    in-flight rejections are ValidationException 'Table is not in a valid state to enable Kinesis Streaming
    Destination: ...', never ResourceInUseException. H-S-113 (one stream per table) confirmed; H-S-037 (two...
  - full notes: [details/DDB-TABLE-299.md](details/DDB-TABLE-299.md)

- <a id="ddb-table-306"></a>**DDB-TABLE-306** `async-state-machine` · impact high · handled · verified 2026-10-09
  **Genuine CREATING window (1-item table, ~11 min): every UpdateTable incl. cancel-Delete -> ResourceInUseException; UpdateTimeToLive blocked**
  1-item PPR table: UpdateTable Create us-east-1 returned in 2s; Replicas[] showed CREATING at ~27s; us-east-1
  DescribeTable became non-NotFound at ~61s; base went ACTIVE[us-east-1 CREATING] at 27s, back to
  UPDATING[CREATING] at 332s, both ACTIVE at 641s (total 686s vs ~25s for an empty table;
  ReplicaStatusPercentProgress never populated). Ops issued at ~35s while Replicas[]=CREATING: ReplicaUpdates
  Update (TableClassOverride) / Create eu-west-1 / DeletionProtectionEnabled / Delete us-east-1 (cancel) ->
  ResourceInUseException 'The resource which you are attempting to change is in use.'; duplicate Create
  us-east-1 -> ResourceInUseException 'Global table with name: <name> already exists with replicas in regions:
  us-east-1, us-west-2.'; UpdateTimeToLive -> ValidationException 'Create/Update/Delete of replica is not
  allowed while the replica is being added to table with name: <name> in region ...' (TTL is group-wide, so it
  is blocked, unlike the empty-table case where it was accepted before the entry appeared); TagResource and
  PutItem -> 200; DescribeTableReplicaAutoScaling (base) -> ResourceNotFoundException 'Requested resource not
  found: Table: <name> not found (Service: AmazonDynamoDBv2 ...)' - it fans out to the not-yet-existing
  replica table. All us-east-1 calls at that moment -> ResourceNotFoundException (table not visible yet). Item
  written to the base during CREATING was present in the replica afterwards; tags were not copied.
  - ACK: updateable.when, deletable.when, requeue, e2e-timing · ops: UpdateTable, DeleteTable, TagResource,
    UpdateTimeToLive, PutItem, GetItem, DescribeTable, DescribeTableReplicaAutoScaling · fields:
    ReplicaUpdates, Replicas.ReplicaStatus, Replicas.ReplicaStatusPercentProgress, TableStatus
  - repro: PPR table + PutItem -> UpdateTable ReplicaUpdates=[Create us-east-1] -> wait for
    Replicas[].ReplicaStatus=CREATING -> issue each op
  - measurements: creating_visible_after_s=27.0, replica_region_visible_after_s=61.0,
    one_item_create_total_s=686.0
  - handling: handled via `generator.yaml:104-109; pkg/resource/table/hooks.go:72-93; pkg/resource/table/hooks_replica_updates.go:465-476; pkg/resource/table/hooks.go:89-92; templates/hooks/table/sdk_create_post_set_output.go.tpl:1-4; bcd26e1; test/e2e/tests/test_table_replicas.py:200-206; test/e2e/tests/test_table.py:34`
  - related: [DDB-TABLE-187](table-replicas.md#ddb-table-187), [DDB-TABLE-232](table-replicas.md#ddb-table-232), [DDB-TABLE-323](#ddb-table-323), [DDB-TABLE-226](table-replicas.md#ddb-table-226), [DDB-TABLE-265](table-replicas.md#ddb-table-265), [DDB-TABLE-308](table-replicas.md#ddb-table-308),
    [DDB-TABLE-309](table-replicas.md#ddb-table-309), [DDB-TABLE-329](table-replicas.md#ddb-table-329), [DDB-TABLE-305](table-replicas.md#ddb-table-305), [DDB-TABLE-249](table-replicas.md#ddb-table-249), [DDB-TABLE-258](table-global-tables.md#ddb-table-258), [DDB-TABLE-200](table-replicas.md#ddb-table-200), [DDB-TABLE-296](table-replicas.md#ddb-table-296),
    [DDB-TABLE-263](table-global-tables.md#ddb-table-263), [DDB-TABLE-315](#ddb-table-315), [DDB-TABLE-327](table-replicas.md#ddb-table-327), [DDB-TABLE-314](#ddb-table-314) · hypotheses: H-R-005, H-R-006, H-R-026,
    H-R-024, H-R-101 · evidence: table/dependencies/replica-prerequisites
  - notes: Confirms H-R-005 for non-empty tables (minutes, base flips ACTIVE->UPDATING->ACTIVE during the add)
    and H-R-006 (ResourceInUseException for every other UpdateTable; TTL also blocked - partial refutation of
    'UpdateTimeToLive succeeds'). Confirms H-R-026 part 1: a replica Create cannot be cancelled...
  - full notes: [details/DDB-TABLE-306.md](details/DDB-TABLE-306.md)

- <a id="ddb-table-314"></a>**DDB-TABLE-314** `async-state-machine` · impact high · unhandled (not handled in controller) · verified 2026-10-09
  **Autoscaling Min above current capacity: AAS issues its own UpdateTable immediately (RCU 5->10 visible at 4 s, ACTIVE after 33 s)**
  UpdateTableReplicaAutoScaling(read Min 10 Max 20) returned 200 with TableStatus UPDATING. DescribeTable
  transitions (t_s,status,rcu,wcu,decreases): [{"t_s": 0.2, "status": "UPDATING", "rcu": 5, "wcu": 5,
  "decreases": 0, "replica": {"us-east-1": ["ACTIVE", null]}}, {"t_s": 4.4, "status": "UPDATING", "rcu": 10,
  "wcu": 5, "decreases": 0, "replica": {"us-east-1": ["ACTIVE", {"ReadCapacityUnits": 5}]}}, {"t_s": 33.0,
  "status": "ACTIVE", "rcu": 10, "wcu": 5, "decreases": 0, "replica": {"us-east-1": ["ACTIVE",
  {"ReadCapacityUnits": 5}]}}]. Scaling activities: [{"start": "2026-10-09T00:48:36.763000+00:00", "end":
  "2026-10-09T00:49:07.252000+00:00", "status": "Successful", "desc": "Setting read capacity units to 10.",
  "cause": "minimum capacity was set to 10", "msg": "Successfully set read capacity units to 10. Change
  successfully fulfilled by dynamodb."}]. UpdateTable(DeletionProtectionEnabled) fired at the first UPDATING
  sample -> {"operation": "update_table", "ok": true, "code": null, "http_status": 200, "message": null,
  "latency_ms": 1022, "client_side": false}. Read alarm definitions: [{"name": "AlarmHigh", "metric":
  "ConsumedReadCapacityUnits", "metrics_expr": [], "stat": "Sum", "period": 60, "eval": 2, "datapoints": null,
  "op": "GreaterThanThreshold", "threshold": 210.0, "missing": null, "state": "INSUFFICIENT_DATA"}, {"name":
  "AlarmLow", "metric": "ConsumedReadCapacityUnits", "metrics_expr": [], "stat": "Sum", "period": 60, "eval":
  15, "datapoints": null, "op": "LessThanThreshold", "threshold": 150.0, "missing": null, "state":
  "INSUFFICIENT_DATA"}, {"name": "ProvisionedCapacityHigh", "metric": "ProvisionedReadCapacityUnits",
  "metrics_expr": [], "stat": "Average", "period": 300, "eval": 3, "datapoints": null, "op":
  "GreaterThanThreshold", "threshold": 5.0, "missing": null, "state": "INSUFFICIENT_DATA"}, {"name":
  "ProvisionedCapacityLow", "metric": "ProvisionedReadCapacityUnits", "metrics_expr": [], "stat": "Average",
  "period": 300, "eval": 3, "datapoints": null, "op.
  - ACK: requeue, compare.is_ignored+delta_pre_compare · ops: UpdateTableReplicaAutoScaling, UpdateTable,
    DescribeTable · fields: ProvisionedThroughput.ReadCapacityUnits, TableStatus
  - repro: RCU 5 table -> UpdateTableReplicaAutoScaling read Min 10 -> poll DescribeTable every 3 s +
    describe-scaling-activities
  - measurements: seconds_until_capacity_changed=4.4, elapsed_until_active_at_min_s=33.0
  - handling: not handled in the controller (as of commit 34b85e6)
  - related: [DDB-TABLE-238](table-replicas.md#ddb-table-238), [DDB-TABLE-192](table-replicas.md#ddb-table-192), [DDB-TABLE-198](#ddb-table-198), [DDB-TABLE-242](#ddb-table-242), [DDB-TABLE-195](#ddb-table-195), [DDB-TABLE-315](#ddb-table-315),
    [DDB-TABLE-256](table-replicas.md#ddb-table-256), [DDB-TABLE-321](table-replicas.md#ddb-table-321), [DDB-TABLE-306](#ddb-table-306), [DDB-TABLE-200](table-replicas.md#ddb-table-200), [DDB-TABLE-296](table-replicas.md#ddb-table-296), [DDB-TABLE-263](table-global-tables.md#ddb-table-263), [DDB-TABLE-327](table-replicas.md#ddb-table-327),
    [DDB-TABLE-251](table-replicas.md#ddb-table-251) · hypotheses: H-R-106, H-S-045 · evidence: table/mutation-matrix/autoscaling-vs-throughput
  - notes: H-R-106 confirmed with faster timing than hypothesised: the scaling activity ('Setting read
    capacity units to 10', cause 'minimum capacity was set to 10') started in the same second as
    UpdateTableReplicaAutoScaling, whose response already reported TableStatus UPDATING. The AAS-driven
    UPDATING lasted...
  - full notes: [details/DDB-TABLE-314.md](details/DDB-TABLE-314.md)

- <a id="ddb-table-323"></a>**DDB-TABLE-323** `async-state-machine` · impact high · unhandled (not handled in controller) · verified 2026-10-09
  **GSI autoscaling update while IndexStatus=CREATING -> RNF; the GSI is absent from Describe until ACTIVE; GSI delete orphans AAS**
  0.8s after UpdateTable created gsi1: write update -> {"operation": "update_table_replica_auto_scaling",
  "ok": false, "code": "ResourceNotFoundException", "http_status": 400, "message": "Failed to update settings
  for global table with name: ‘ackq-b9a764-as-gsiadd’ because the global secondary indexes with names:
  ‘[gsi1]’ do not exist.", "latency_ms": 778, "client_side": false}; read update -> {"operation":
  "update_table_replica_auto_scaling", "ok": false, "code": "ResourceNotFoundException", "http_status": 400,
  "message": "Failed to update settings for global table with name: ‘ackq-b9a764-as-gsiadd’ because a global
  secondary index with name: ‘gsi1’ does not exist in region: ‘us-west-2’.", "latency_ms": 722, "client_side":
  false}. Describe while CREATING: gsi1 entry null (region B null; TableStatus ACTIVE); AAS targets A [["/",
  "t:W", 1, 10]], B [["/", "t:W", 1, 10]]. GSI create timeline [{"value": "UPDATING|rep:ACTIVE|gsi:",
  "from_s": 0.16, "to_s": 31.35, "duration_s": 31.19}, {"value": "UPDATING|rep:ACTIVE|gsi:gsi1=CREATING",
  "from_s": 31.35, "to_s": 57.27, "duration_s": 25.92}, {"value": "ACTIVE|rep:ACTIVE|gsi:gsi1=CREATING",
  "from_s": 57.27, "to_s": 534.9, "duration_s": 477.63},. After ACTIVE: entry {"IndexName": "gsi1",
  "IndexStatus": "ACTIVE", "ProvisionedReadCapacityAutoScalingSettings": {"AutoScalingDisabled": true,
  "ScalingPolicies": []}, "ProvisionedWriteCapacityAutoScalingSettings": {"MinimumUnits": 1, "MaximumUnits":
  10, "AutoScalingRoleArn": "arn:aws:iam::<ACCOUNT>:role/<PRINCIPAL> (B {"IndexName": "gsi1", "IndexStatus":
  "ACTIVE", "ProvisionedReadCapacityAutoScalingSettings": {"AutoScalingDisabled": true, "ScalingPolicies":
  []}, "ProvisionedWriteCapacityAutoScalingSettings": {"Mini); targets A [["/", "t:W", 1, 10], ["/index/gsi1",
  "i:W", 1, 10]], B [["/", "t:W", 1, 10], ["/index/gsi1", "i:W", 1, 10]]. Final entry {"IndexName": "gsi1",
  "IndexStatus": "ACTIVE", "ProvisionedReadCapacityAutoScalingSettings": {"MinimumUnits": 1, "MaximumUnits":
  10, "AutoScalingRoleArn": "arn:aws:iam::<ACCOUNT>:role/<PRINCIPAL> targets A [["/", "t:W", 1, 10],
  ["/index/gsi1", "i:R", 1, 10], ["/index/gsi1", "i:W", 1, 12]] B [["/", "t:W", 1, 10], ["/index/gsi1", "i:W",
  1, 12]]. UpdateTable delete gsi1 -> 200; targets after GSI delete A [["/", "t:W", 1, 10], ["/index/gsi1",
  "i:R", 1, 10], ["/index/gsi1", "i:W", 1, 12]] B [["/", "t:W", 1, 10], ["/index/gsi1", "i:W", 1, 12]]; entry
  null.
  - ACK: requeue, terminal_codes, pre-delete-cleanup · ops: UpdateTable, UpdateTableReplicaAutoScaling,
    DescribeTableReplicaAutoScaling · fields: GlobalSecondaryIndexUpdates, IndexStatus,
    ReplicaGlobalSecondaryIndexUpdates
  - repro: UpdateTable add GSI -> UpdateTableReplicaAutoScaling for it immediately -> poll IndexStatus ->
    retry -> UpdateTable delete GSI -> describe-scalable-targets
  - handling: not handled in the controller (as of commit 34b85e6)
  - related: [DDB-TABLE-187](table-replicas.md#ddb-table-187), [DDB-TABLE-232](table-replicas.md#ddb-table-232), [DDB-TABLE-306](#ddb-table-306), [DDB-TABLE-242](#ddb-table-242), [DDB-TABLE-198](#ddb-table-198), [DDB-TABLE-290](#ddb-table-290),
    [DDB-TABLE-292](table-replicas.md#ddb-table-292), [DDB-TABLE-243](#ddb-table-243) · hypotheses: H-R-108 · evidence:
    table/creative/gsi-add-on-provisioned-global-table
  - notes: H-R-108 confirmed for the error code (ResourceNotFoundException 'the global secondary indexes with
    names: [gsi1] do not exist' / '... does not exist in region', not ResourceInUseException) and for
    'succeeds once ACTIVE' (write and read updates -> 200). New: the CREATING index is not listed under...
  - full notes: [details/DDB-TABLE-323.md](details/DDB-TABLE-323.md)

- <a id="ddb-table-348"></a>**DDB-TABLE-348** `async-state-machine` · impact high · unhandled (not handled in controller) · verified 2026-10-09
  **DeleteTable within ~1-2s of a policy Put/Delete -> ResourceInUseException (has a pending resource-based policy update) while ACTIVE**
  TableStatus was ACTIVE, yet DeleteTable 0.2 s after PutResourcePolicy failed with ResourceInUseException
  (HTTP 400) 'Attempt to change a resource which is still in use: Table: ackq-b044e1-srs-fu0 has a pending
  resource-based policy update.'; retries at 0.5 s failed too and the delete was accepted at 1.042 s. After
  DeleteResourcePolicy the block lasted until 2.04 s (ResourceInUseException->OK).
  UpdateTable(DeletionProtectionEnabled) 0.2 s after a Put -> OK; TagResource 0.2 s after a Put -> OK (both
  admitted). A no-op equivalent re-Put (same RevisionId) does not open the window: DeleteTable 0.2 s later ->
  OK.
  - ACK: requeue, custom_delete, terminal_codes · ops: DeleteTable, PutResourcePolicy, DeleteResourcePolicy,
    UpdateTable, TagResource · fields: ResourcePolicy, TableStatus
  - repro: ACTIVE table: PutResourcePolicy; DeleteTable at 0.2, 0.5, 1.0 ... s until 200; repeat after
    DeleteResourcePolicy; repeat with UpdateTable/TagResource; repeat after a no-op re-Put
  - measurements: delete_blocked_after_put_s=1.042, delete_blocked_after_policy_delete_s=2.04
  - handling: not handled in the controller (as of commit 34b85e6)
  - related: [DDB-TABLE-206](#ddb-table-206), [DDB-TABLE-209](#ddb-table-209), [DDB-TABLE-244](#ddb-table-244), [DDB-TABLE-245](#ddb-table-245), [DDB-TABLE-246](#ddb-table-246), [DDB-TABLE-248](#ddb-table-248),
    [DDB-TABLE-205](#ddb-table-205), [DDB-TABLE-069](table-streams-encryption-class.md#ddb-table-069), [DDB-TABLE-122](#ddb-table-122), [DDB-TABLE-236](#ddb-table-236), [DDB-TABLE-247](#ddb-table-247), [DDB-TABLE-347](#ddb-table-347), [DDB-TABLE-464](service.md#ddb-table-464),
    [DDB-TABLE-445](service.md#ddb-table-445), [DDB-TABLE-213](table-streams-encryption-class.md#ddb-table-213), [DDB-TABLE-172](service.md#ddb-table-172), [DDB-TABLE-119](table-streams-encryption-class.md#ddb-table-119), [DDB-TABLE-210](#ddb-table-210), [DDB-TABLE-432](#ddb-table-432) · hypotheses:
    H-S-027, H-S-007 · evidence: table/consistency-windows/policy-stale-read-sequence
  - notes: Found by accident when the probe's cleanup DeleteTable failed right after the last policy write.
    Mirrors [DDB-TABLE-069](table-streams-encryption-class.md#ddb-table-069) ('ACTIVE does not mean deletable'): a controller that writes the policy and deletes
    the table in the same reconcile (or a finalizer that runs right after a policy sync) must retry...
  - full notes: [details/DDB-TABLE-348.md](details/DDB-TABLE-348.md)

## Identity and lookup

- <a id="ddb-table-212"></a>**DDB-TABLE-212** `identity` · impact medium · unhandled (not handled in controller) · verified 2026-10-09
  **ResourceArn forms: bare name/other account/region/partition/index -> ValidationException; missing or
  wrong-case table -> ResourceNotFound** - GetResourcePolicy / PutResourcePolicy / DeleteResourcePolicy by
  ResourceArn form: bare table name -> ValidationException (HTTP 400) 'One or more parameter values were
  invalid: ARNs must start with 'arn:': ackq-71f899-... - see:
  [details/DDB-TABLE-212.md](details/DDB-TABLE-212.md)

- <a id="ddb-table-240"></a>**DDB-TABLE-240** `identity` · impact high · unhandled (not handled in controller) · verified 2026-10-09, re-verified
  **PolicyName is the policy identity: resend = no-op (same ARN/CreationTime); a NEW name REPLACES the old policy (one policy per dimension)**
  Custom PolicyName p1 + cooldowns round-trip diff: {"missing_in_observed": [], "value_changed": {},
  "extra_in_observed": []}; policy names after C: ['ackq-41e87e-p1'] (server-generated policy replaced).
  Resend identical -> 200, same PolicyARN+CreationTime per policy: {"ackq-41e87e-p1": true}. Same name new
  TargetValue -> 200, after: {"policies": [["ackq-41e87e-p1", {"TargetValue": 50.0,
  "PredefinedMetricSpecification": {"PredefinedMetricType": "DynamoDBReadCapacityUtilization"}}]], "same_arn":
  {"ackq-41e87e-p1": true}}. Different name p2 -> 200; Describe ScalingPolicies: [{"PolicyName":
  "ackq-41e87e-p2", "TargetTrackingScalingPolicyConfiguration": {"TargetValue": 40.0}}]; AAS:
  [["ackq-41e87e-p2", 40.0]]; alarms: ["AlarmHigh-a5d5e80a-f220-4634-ab9c-6ac7c6fd1fe1",
  "AlarmHigh-a84f162d-6a3a-4ac1-8590-bf6a41c5447c", "AlarmLow-0f864fcd-f316-49a7-99e7-9e0fc21ebf92",
  "AlarmLow-626b23aa-7767-458d-8602-1f0caf1dd909",
  "ProvisionedCapacityHigh-529e6f7c-f11f-4e5a-92e8-3ffd772de89e",
  "ProvisionedCapacityHigh-576ad820-8772-4711-bfd2-e32f27f244da",
  "ProvisionedCapacityLow-2fd9a0a0-fa17-4cc8-b741-85a30bdcd0a7",
  "ProvisionedCapacityLow-67bcf72a-08df-49e8-abb8-a2f5feabd605"].
  - ACK: custom_update, docs-only · ops: UpdateTableReplicaAutoScaling · fields:
    ScalingPolicyUpdate.PolicyName, ScalingPolicies
  - repro: read update PolicyName p1 -> resend -> same name new target -> PolicyName p2 -> Describe +
    describe-scaling-policies
  - handling: not handled in the controller (as of commit 34b85e6)
  - related: [DDB-TABLE-237](#ddb-table-237), [DDB-TABLE-239](table-replicas.md#ddb-table-239), [DDB-TABLE-191](#ddb-table-191), [DDB-TABLE-189](#ddb-table-189), [DDB-TABLE-196](#ddb-table-196) · hypotheses: H-R-109,
    H-S-041 · evidence: table/round-trip/autoscaling-settings, table/creative/reverify-set-a2
  - notes: REFUTES the H-R-109 claim that a different PolicyName accumulates a second policy: DynamoDB deletes
    the previous policy (AAS lists only the new one, old alarms removed). Custom PolicyName also replaced the
    server-generated 'DynamoDBReadCapacityUtilization:table/<name>' policy. Same-name updates keep...
  - full notes: [details/DDB-TABLE-240.md](details/DDB-TABLE-240.md)

- <a id="ddb-table-271"></a>**DDB-TABLE-271** `identity` · impact medium · unhandled (not handled in controller) · verified 2026-10-09
  **Same-name re-create: policy/Kinesis reads show no leak of the old table's state (0 stale reads at 0.5s
  polling, CreateTable..ACTIVE+10s)** - Old table R: policy 200 OK, destination [('-fu-g2', 'ACTIVE')];
  DeleteTable -> gone. - see: [details/DDB-TABLE-271.md](details/DDB-TABLE-271.md)

## Idempotency

- <a id="ddb-table-205"></a>**DDB-TABLE-205** `idempotency` · impact high · unhandled (not handled in controller) · verified 2026-10-09
  **No policy: Get -> PolicyNotFoundException; idempotent Delete -> 200 with empty body, or with the DELETION's RevisionId after a real delete**
  Table that never had a policy: GetResourcePolicy -> PolicyNotFoundException (HTTP 400) 'Resource-based
  policy not found for the provided ResourceArn: arn:aws:dynamodb:us-west-2:<ACCOUNT>:table/ackq-71f899-rp'.
  DeleteResourcePolicy -> 200 OK with response keys [] (no RevisionId); a second Delete -> 200 OK (still
  empty). Delete/Put with a bogus ExpectedRevisionId when no policy exists -> PolicyNotFoundException (HTTP 400)
  'Resource-based policy not found for the provided ResourceArn: Requested update for policy with revision id
  123456789, but table ackq-71f899-rp has no associated' / PolicyNotFoundException (HTTP 400) 'Resource-based
  policy not found for the provided ResourceArn: Requested update for policy with revision id 123456789, but
  table ackq-71f899-rp has no associated'. After a REAL delete (Delete(ExpectedRevisionId=current) -> 200,
  RevisionId = the removed policy's id: True), a further Delete 2s later -> 200 OK with
  RevisionId=1791505822353, which is the epoch-ms timestamp of the deletion itself (a tombstone revision), and
  a Put right after that -> ThrottlingException (HTTP 400) 'Resource-based policy for table ackq-71f899-rp
  modified within the previous 15000 milliseconds. Please try again after 2026-10-09T00:30:37.353Z.' whose
  'try again after' = tombstone+15s. Delete with ExpectedRevisionId=<removed id> after the delete ->
  PolicyNotFoundException (HTTP 400) 'Resource-based policy not found for the provided ResourceArn: Requested
  update for policy with revision id 1791505806672, but the policy associated to target ta'.
  - ACK: exceptions.404, custom_find, custom_delete, compare.nil_equals_zero_value · ops: GetResourcePolicy,
    DeleteResourcePolicy, PutResourcePolicy · fields: ResourcePolicy, ExpectedRevisionId, RevisionId
  - repro: CreateTable -> Get; Delete; Delete; Delete(ExpectedRevisionId=bogus);
    Put(ExpectedRevisionId=bogus). Then Put; wait 16s; Delete(ExpectedRevisionId=current); wait 2s; Delete ->
    RevisionId present
  - handling: not handled in the controller (as of commit 34b85e6)
  - related: [DDB-TABLE-206](#ddb-table-206), [DDB-TABLE-247](#ddb-table-247), [DDB-TABLE-347](#ddb-table-347), [DDB-TABLE-464](service.md#ddb-table-464), [DDB-TABLE-445](service.md#ddb-table-445), [DDB-TABLE-348](#ddb-table-348),
    [DDB-TABLE-213](table-streams-encryption-class.md#ddb-table-213), [DDB-TABLE-209](#ddb-table-209), [DDB-TABLE-246](#ddb-table-246), [DDB-TABLE-214](#ddb-table-214), [DDB-TABLE-085](table-subresources.md#ddb-table-085), [DDB-TABLE-091](table-subresources.md#ddb-table-091), [DDB-TABLE-341](table-subresources.md#ddb-table-341),
    [DDB-TABLE-147](table-subresources.md#ddb-table-147), [DDB-TABLE-300](#ddb-table-300), [DDB-TABLE-207](#ddb-table-207), [DDB-TABLE-383](table-streams-encryption-class.md#ddb-table-383) · hypotheses: H-S-026, H-S-006, H-S-112 ·
    evidence: table/sub-resources/resource-policy
  - notes: H-S-026 confirmed for the never-policied case (200, no RevisionId) and refined: after a real delete
    the idempotent Delete DOES return a RevisionId - the deletion timestamp, not the removed policy's id. The
    same RevisionId value is returned by Put and by the first Delete (H-S-112 confirmed).

- <a id="ddb-table-207"></a>**DDB-TABLE-207** `idempotency` · impact high · handled · verified 2026-10-09
  **RevisionId is the epoch-ms change time; equivalent re-Puts (whitespace/key order/list-vs-string/account-id) are no-ops, same id**
  PutResourcePolicy(pretty-printed) -> rev A. Re-Put identical -> same id (True). Minified -> same (True).
  Keys reordered -> same (True). Action as plain string instead of 1-element list -> same (True). Principal as
  bare account id instead of root ARN -> same (True). All of these were accepted INSIDE the 15s window that
  rejects different documents, i.e. equivalence is decided on the canonical form. Re-Put of document A after a
  different policy -> same id as the first A: False (ids are change timestamps, not content hashes). Ids are
  13-digit strings, strictly increasing in change order: True.
  - ACK: compare.is_ignored+delta_pre_compare, is_document, annotation-shadow-state · ops: PutResourcePolicy ·
    fields: ResourcePolicy, RevisionId
  - repro: Put(pretty); Put(pretty); Put(minified); Put(reordered); Put(action-string); Put(account-id
    principal); wait 16s; Put(other); wait 16s; Put(pretty) - compare RevisionIds
  - measurements: revision_id_len=13
  - handling: handled via `test/e2e/tests/test_table.py:1139-1180; pkg/resource/table/hooks_resource_policy.go:30-45; pkg/resource/table/hooks_resource_policy.go:139-177; pkg/resource/table/hooks_resource_policy_test.go:25-235`
  - related: [DDB-TABLE-208](#ddb-table-208), [DDB-TABLE-351](#ddb-table-351), [DDB-TABLE-352](#ddb-table-352), [DDB-TABLE-353](#ddb-table-353), [DDB-TABLE-354](#ddb-table-354), [DDB-TABLE-355](#ddb-table-355),
    [DDB-TABLE-356](#ddb-table-356), [DDB-TABLE-085](table-subresources.md#ddb-table-085), [DDB-TABLE-091](table-subresources.md#ddb-table-091), [DDB-TABLE-341](table-subresources.md#ddb-table-341), [DDB-TABLE-147](table-subresources.md#ddb-table-147), [DDB-TABLE-300](#ddb-table-300), [DDB-TABLE-383](table-streams-encryption-class.md#ddb-table-383),
    [DDB-TABLE-205](#ddb-table-205) · hypotheses: H-S-006, H-S-112, H-S-028 · evidence: table/sub-resources/resource-policy
  - notes: H-S-006 'whitespace-only change yields a new RevisionId' REFUTED; 'byte-identical re-Put returns
    the same id' confirmed. H-S-112: numerically monotonic in practice (timestamps), but string-compare is
    still the only documented contract.
  - full notes: [details/DDB-TABLE-207.md](details/DDB-TABLE-207.md)

## Errors

- <a id="ddb-table-014"></a>**DDB-TABLE-014** `error-code` · impact high · unhandled (not handled in controller) · verified 2026-10-08
  **Missing table: *ContinuousBackups/CreateBackup -> TableNotFoundException, ListBackups -> 200 empty, others ResourceNotFoundException**
  Against a nonexistent table name/ARN: ResourceNotFoundException (HTTP 400) from ['DescribeTable',
  'UpdateTable(DP)', 'DeleteTable', 'DescribeTimeToLive', 'UpdateTimeToLive', 'DescribeContributorInsights',
  'UpdateContributorInsights', 'DescribeKinesisStreamingDestination', 'DescribeTableReplicaAutoScaling',
  'ListTagsOfResource(arn)', 'TagResource(arn)', 'UntagResource(arn)', 'GetResourcePolicy(arn)',
  'PutResourcePolicy(arn)', 'DeleteResourcePolicy(arn)', 'PutItem']. Different outcomes: {'UpdateTable(name
  only)': {'code': 'ValidationException', 'http': 400, 'msg': 'At least one of ProvisionedThroughput,
  BillingMode, UpdateStreamEnabled, GlobalSecondaryIn', 'resp': None}, 'DescribeContinuousBackups': {'code':
  'TableNotFoundException', 'http': 400, 'msg': 'Table not found: ackq-ba1bb7-missing', 'resp': None},
  'UpdateContinuousBackups': {'code': 'TableNotFoundException', 'http': 400, 'msg': 'Table not found:
  ackq-ba1bb7-missing', 'resp': None}, 'CreateBackup': {'code': 'TableNotFoundException', 'http': 400, 'msg':
  'Table not found: ackq-ba1bb7-missing', 'resp': None}, 'ListBackups(TableName)': {'code': None, 'http': 200,
  'msg': '', 'resp': ['BackupSummaries']}, 'DescribeTable(bad chars)': {'code': 'ValidationException', 'http':
  400, 'msg': "1 validation error detected: Value 'ackq bad/name!' at 'tableName' failed to satisfy const",
  'resp': None}, 'DescribeTable(2 chars)': {'code': 'ValidationException', 'http': 400, 'msg': "1 validation
  error detected: Value 'ab' at 'tableName' failed to satisfy constraint: Membe", 'resp': None},
  'DescribeTable(256 chars)': {'code': 'ValidationException', 'http': 400, 'msg': "1 validation error
  detected: Value 'aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa", 'resp': None}}. Messages:
  DescribeTable 'Requested resource not found: Table: ackq-ba1bb7-missing not found'; ListTagsOfResource
  'Requested resource not found: ResourceArn: arn:aws:dynamodb:us-west-2:<ACCOUNT>:table/ackq-ba1bb7-missing
  not found'; DescribeTimeToLive 'Requested resource not found: Table: ackq-ba1bb7-missing not found'.
  - ACK: exceptions.404, terminal_codes · ops: DescribeTable, UpdateTable(DP), UpdateTable(name only),
    DeleteTable, DescribeTimeToLive, UpdateTimeToLive, DescribeContinuousBackups, UpdateContinuousBackups,
    DescribeContributorInsights, UpdateContributorInsights, DescribeKinesisStreamingDestination,
    DescribeTableReplicaAutoScaling
  - repro: Call each op with TableName/ResourceArn of a table that does not exist
  - handling: not handled in the controller (as of commit 34b85e6)
  - related: [DDB-TABLE-070](service.md#ddb-table-070), [DDB-TABLE-098](service.md#ddb-table-098), [DDB-TABLE-074](service.md#ddb-table-074), [DDB-TABLE-447](service.md#ddb-table-447) · evidence:
    table/error-taxonomy/missing-table-noop-update-dp
  - notes: Every not-found is HTTP 400, never 404. Ops whose code is not ResourceNotFoundException need their
    own 404 mapping or will be mis-classified.

- <a id="ddb-table-209"></a>**DDB-TABLE-209** `error-code` · impact high · handled · verified 2026-10-09
  **ExpectedRevisionId mismatch -> PolicyNotFoundException on Put and Delete (message: did not match); Delete echoes the removed id**
  PutResourcePolicy(ExpectedRevisionId=bogus) -> PolicyNotFoundException (HTTP 400) 'Resource-based policy not
  found for the provided ResourceArn: Requested update for policy with revision id 000000000000000000, but the
  policy associated to targ'. Put(ExpectedRevisionId=previous revision) -> PolicyNotFoundException (HTTP 400)
  'Resource-based policy not found for the provided ResourceArn: Requested update for policy with revision id
  1791505774984, but the policy associated to target ta'. Put(ExpectedRevisionId=current) -> 200 OK (new
  RevisionId: True). DeleteResourcePolicy(ExpectedRevisionId=stale) -> PolicyNotFoundException (HTTP 400)
  'Resource-based policy not found for the provided ResourceArn: Requested update for policy with revision id
  1791505790789, but the policy associated to target ta'. Delete(ExpectedRevisionId=current) -> 200 OK; the
  response RevisionId equals the removed policy's: True. Get reached PolicyNotFoundException 2.05s after the
  delete. The PolicyNotFoundException message distinguishes 'table X has no associated policy' from 'the
  policy associated ... revision id ... did not match' - the code does not.
  - ACK: terminal_codes, custom_update, requeue · ops: PutResourcePolicy, DeleteResourcePolicy · fields:
    ExpectedRevisionId, RevisionId
  - repro: Put(p, ExpectedRevisionId=bogus); Put(p, ExpectedRevisionId=current);
    Delete(ExpectedRevisionId=stale); Delete(ExpectedRevisionId=current)
  - handling: handled via `pkg/resource/table/hooks_resource_policy.go:97-103; pkg/resource/table/hooks_resource_policy.go:129-132`
  - related: [DDB-TABLE-205](#ddb-table-205), [DDB-TABLE-246](#ddb-table-246), [DDB-TABLE-347](#ddb-table-347), [DDB-TABLE-214](#ddb-table-214) · hypotheses: H-S-006, H-S-026,
    H-S-112 · evidence: table/sub-resources/resource-policy
  - notes: H-S-006 mismatch-code clause confirmed (PolicyNotFoundException, HTTP 400, for both Put and
    Delete).

- <a id="ddb-table-236"></a>**DDB-TABLE-236** `error-code` · impact high · unhandled (not handled in controller) · verified 2026-10-09
  **DELETING: policy/Kinesis reads 200; Put/Delete policy -> ResourceInUse; Enable/Disable -> Validation; gone -> ResourceNotFound, no ghosts**
  While TableStatus=DELETING (table with a policy and an ACTIVE destination): GetResourcePolicy 200 OK (old
  RevisionId); DescribeKinesisStreamingDestination 200 OK (entry ACTIVE then UPDATING); PutResourcePolicy
  ResourceInUseException (HTTP 400) 'Attempt to change a resource which is still in use: Table is being
  deleted: ackq-f57f9f-pk-a'; DeleteResourcePolicy ResourceInUseException (HTTP 400) 'Attempt to change a
  resource which is still in use: Table is being deleted: ackq-f57f9f-pk-a';
  EnableKinesisStreamingDestination(same stream) ValidationException (HTTP 400) 'Table is not in a valid state
  to enable Kinesis Streaming Destination: EnableKinesisStreamingDestination must be DISABLED or ENABLE_FAILED
  to perform '; Enable(other stream) ValidationException (HTTP 400) 'Table is not in a valid state to enable
  Kinesis Streaming Destination: EnableKinesisStreamingDestination must be DISABLED or ENABLE_FAILED to
  perform '; UpdateKinesisStreamingDestination 200 OK; DisableKinesisStreamingDestination ValidationException
  (HTTP 400) 'Table is not in a valid state to enable Kinesis Streaming Destination:
  KinesisStreamingDestination must be ACTIVE to perform DISABLE operation.'. DELETING lasted ~5.43s. From the
  first DescribeTable ResourceNotFoundException (t=5.43s) on, 60 more seconds of 1s polling produced 0 ghost
  200s: GetResourcePolicy -> ResourceNotFoundException 'Requested resource not found: Table: ackq-f57f9f-pk-a
  not found' and DescribeKinesisStreamingDestination -> ResourceNotFoundException 'Requested resource not
  found: Table: ackq-f57f9f-pk-a not found' from the very first poll.
  - ACK: exceptions.404, terminal_codes, deletable.when · ops: GetResourcePolicy,
    DescribeKinesisStreamingDestination, PutResourcePolicy, DeleteResourcePolicy,
    EnableKinesisStreamingDestination, DisableKinesisStreamingDestination, UpdateKinesisStreamingDestination
  - repro: table with policy + ACTIVE destination: DeleteTable; every 1s call Get/DescribeKinesis (+ each
    mutator once while DELETING); continue 60s after ResourceNotFoundException
  - measurements: ghost_reads_after_gone=0, deleting_duration_s=5.43
  - handling: not handled in the controller (as of commit 34b85e6)
  - related: [DDB-TABLE-122](#ddb-table-122), [DDB-TABLE-374](service.md#ddb-table-374), [DDB-TABLE-097](table-subresources.md#ddb-table-097), [DDB-TABLE-453](table-subresources.md#ddb-table-453), [DDB-TABLE-088](table-subresources.md#ddb-table-088), [DDB-TABLE-235](#ddb-table-235),
    [DDB-TABLE-342](table-subresources.md#ddb-table-342), [DDB-TABLE-214](#ddb-table-214), [DDB-TABLE-271](#ddb-table-271), [DDB-TABLE-436](#ddb-table-436), [DDB-TABLE-377](service.md#ddb-table-377), [DDB-TABLE-299](#ddb-table-299), [DDB-TABLE-300](#ddb-table-300),
    [DDB-TABLE-301](#ddb-table-301), [DDB-TABLE-302](#ddb-table-302), [DDB-TABLE-304](#ddb-table-304), [DDB-TABLE-269](#ddb-table-269), [DDB-TABLE-268](#ddb-table-268), [DDB-TABLE-319](#ddb-table-319), [DDB-TABLE-303](#ddb-table-303),
    [DDB-TABLE-234](#ddb-table-234), [DDB-TABLE-460](#ddb-table-460) · hypotheses: H-S-118, H-S-119, H-S-120 · evidence:
    table/state-machine/policy-kinesis-admissibility
  - notes: H-S-118 partially confirmed: Get and Describe kinesis return 200 during DELETING, but the
    destination shows UPDATING (not DISABLING). H-S-119 REFUTED for policy/kinesis: no ghost 200s after the
    table is gone, and both use ResourceNotFoundException 'Requested resource not found: Table: X not
    found'....
  - full notes: [details/DDB-TABLE-236.md](details/DDB-TABLE-236.md)

- <a id="ddb-table-302"></a>**DDB-TABLE-302** `error-code` · impact medium · unhandled (not handled in controller) · verified 2026-10-09
  **Disable of a never-enabled/nonexistent/malformed stream ARN -> ValidationException (must be ACTIVE to
  DISABLE), never ResourceNotFound** - With K1 DISABLED and K2 never attached: Disable(K2, an existing ACTIVE
  stream) -> ValidationException (HTTP 400) 'Table is not in a valid state to enable Kinesis Streaming
  Destination: KinesisStreamingDestination must... - see: [details/DDB-TABLE-302.md](details/DDB-TABLE-302.md)

- <a id="ddb-table-318"></a>**DDB-TABLE-318** `error-code` · impact high · unhandled (not handled in controller) · verified 2026-10-09
  **Enable never validates the stream synchronously: bad/missing/other-region/other-account/CREATING ARNs -> 200 ENABLING then ENABLE_FAILED**
  EnableKinesisStreamingDestination returned 200 DestinationStatus=ENABLING for every well-formed ARN and
  failed asynchronously with DestinationStatusDescription: nonexistent same-region stream -> 200 ENABLING ->
  ENABLE_FAILED after 2.11s ('User does not have a permission to use kinesis stream'); a DynamoDB table ARN ->
  200 ENABLING -> ENABLE_FAILED after 1.01s ('User does not have a permission to use kinesis stream'); an
  ACTIVE stream in us-east-1 -> 200 ENABLING -> ENABLE_FAILED after 1.01s ('The kinesis Arn used to enable
  kinesis replication belongs to a stream which is not in the current region'); same stream name under another
  account id -> 200 ENABLING -> ENABLE_FAILED after 61.72s ('The kinesis Arn used to enable kinesis
  replication belongs to a stream which is not in Active state'); a stream consumer ARN -> 200 ENABLING ->
  ENABLE_FAILED after 2.03s ('User does not have a permission to use kinesis stream'); a Firehose ARN -> 200
  ENABLING -> ENABLE_FAILED after 61.71s ('The kinesis Arn used to enable kinesis replication belongs to a
  stream which is not in Active state'); the stream name upper-cased -> 200 ENABLING -> ENABLE_FAILED after
  2.03s ('User does not have a permission to use kinesis stream'). A stream still CREATING -> 200 OK then
  ENABLE_FAILED 'User does not have a permission to use kinesis stream' after 2.02s; a stream DELETING -> 200
  OK then ENABLE_FAILED after 17.21s; after the stream is gone -> 200 OK then ENABLE_FAILED after 2.03s. Only
  'not-an-arn' was rejected, client-side by botocore (ParamValidationError (HTTP None) 'Parameter validation
  failed:
  Invalid length for parameter StreamArn, value: 10, valid min length: 37'). The not-found cases are described
  as a permission problem ('User does not have a permission to use kinesis stream'); cross-account and
  wrong-service ARNs take ~60s to fail.
  - ACK: terminal_codes, requeue, references, synced.when · ops: EnableKinesisStreamingDestination,
    DescribeKinesisStreamingDestination · fields: StreamArn,
    KinesisDataStreamDestinations[].DestinationStatus,
    KinesisDataStreamDestinations[].DestinationStatusDescription
  - repro: Enable with each ARN variant on an ACTIVE table; poll DescribeKinesisStreamingDestination at 1/s;
    create a stream and Enable before it is ACTIVE; delete a stream and Enable while DELETING
  - measurements: enable_failed_after_s_nonexistent=2.11, enable_failed_after_s_other_account=61.72,
    enable_failed_after_s_other_region=1.01
  - handling: not handled in the controller (as of commit 34b85e6)
  - hypotheses: H-S-034, H-S-114 · evidence: table/error-taxonomy/kinesis-destination-errors
  - notes: H-S-034 REFUTED on the synchronous channel: no ResourceNotFoundException/ValidationException for
    any ARN - all failures are asynchronous ENABLE_FAILED (the hypothesised 'only workflow failures end as
    ENABLE_FAILED' is backwards). H-S-114 REFUTED: other-region and consumer ARNs are also accepted...
  - full notes: [details/DDB-TABLE-318.md](details/DDB-TABLE-318.md)

- <a id="ddb-table-347"></a>**DDB-TABLE-347** `error-code` · impact high · unhandled (not handled in controller) · verified 2026-10-09
  **ExpectedRevisionId = RevisionId just returned by Put is never PolicyNotFound: ResourceInUse (~0.6-1.5s), Throttling until 15.0s, then OK**
  Right after PutResourcePolicy returned RevisionId R (Get still PolicyNotFoundException), Put(other,
  ExpectedRevisionId=R) at offsets 0..20 s gave ['ResourceInUseException->ThrottlingException->OK'] with first
  success at [15.088, 15.103] s; Delete(ExpectedRevisionId=R) gave
  ['ResourceInUseException->ThrottlingException->OK'], first success at [15.039, 15.04] s. Issuing the Put the
  instant Get first showed R ([1.308, 1.747] s) gave ['ResourceInUseException->ThrottlingException->OK',
  'ThrottlingException->OK'] - i.e. read visibility does not unlock writes; the per-table 15 s cooldown does.
  ResourceInUseException message: 'Table ... is pending previous resource-based policy update';
  ThrottlingException: 'modified within the previous 15000 milliseconds. Please try again after <ts>'. No
  PolicyNotFoundException was returned at any offset, so the revision check already sees the new revision
  while Get does not.
  - ACK: requeue, terminal_codes, custom_update · ops: PutResourcePolicy, DeleteResourcePolicy · fields:
    ExpectedRevisionId, RevisionId
  - repro: Put -> R; Put(other, ExpectedRevisionId=R) at 0, 0.1, 0.25 ... 20 s until OK; same with Delete;
    same starting when Get first returns R
  - measurements: put_expected_first_ok_s=[15.088, 15.103], delete_expected_first_ok_s=[15.039, 15.04],
    first_visible_s=[1.308, 1.747]
  - handling: not handled in the controller (as of commit 34b85e6)
  - related: [DDB-TABLE-206](#ddb-table-206), [DDB-TABLE-209](#ddb-table-209), [DDB-TABLE-244](#ddb-table-244), [DDB-TABLE-245](#ddb-table-245), [DDB-TABLE-246](#ddb-table-246), [DDB-TABLE-248](#ddb-table-248),
    [DDB-TABLE-205](#ddb-table-205), [DDB-TABLE-247](#ddb-table-247), [DDB-TABLE-464](service.md#ddb-table-464), [DDB-TABLE-445](service.md#ddb-table-445), [DDB-TABLE-348](#ddb-table-348), [DDB-TABLE-213](table-streams-encryption-class.md#ddb-table-213), [DDB-TABLE-214](#ddb-table-214) ·
    hypotheses: H-S-108, H-S-027, H-S-006 · evidence: table/consistency-windows/policy-stale-read-sequence
  - notes: Qualifies [DDB-TABLE-206](#ddb-table-206)/209: a controller that chains Put -> Put(ExpectedRevisionId=<returned>)
    within 15 s must treat ResourceInUseException and ThrottlingException as retry-after (both carry no
    revision information); PolicyNotFoundException in that window would mean a genuinely different revision...
  - full notes: [details/DDB-TABLE-347.md](details/DDB-TABLE-347.md)

- <a id="ddb-table-432"></a>**DDB-TABLE-432** `error-code` · impact medium · unhandled (not handled in controller) · verified 2026-10-09
  **Deny dynamodb:UpdateTable in the table policy is per-action: UpdateTable (name/ARN, no-op/real)
  AccessDenied; TTL/PITR/Tag/Delete succeed** - Resource policy {Deny, Principal:'*',
  Action:dynamodb:UpdateTable}: a same-value UpdateTable(DeletionProtectionEnabled=false) turned from 200 to
  AccessDeniedException 4.1 s after Put; a real change (StreamSpecificatio... - see:
  [details/DDB-TABLE-432.md](details/DDB-TABLE-432.md)

## Request validation

- <a id="ddb-table-210"></a>**DDB-TABLE-210** `request-validation` · impact high · unhandled (not handled in controller) · verified 2026-10-09
  **Lockout check guards only PutResourcePolicy: Deny on Put -> AccessDenied unless ConfirmRemoveSelfResourceAccess; Deny-Delete enforced later**
  Deny * on Put+DeleteResourcePolicy without ConfirmRemoveSelfResourceAccess -> AccessDeniedException (HTTP 400)
  'The new resource policy will not allow you to update the resource policy in the future.'; Confirm=false ->
  AccessDeniedException (HTTP 400) 'The new resource policy will not allow you to update the resource policy
  in the future.'; Deny only PutResourcePolicy -> AccessDeniedException (HTTP 400) 'The new resource policy
  will not allow you to update the resource policy in the future.'; Deny Put only for the caller's role via
  aws:PrincipalArn condition -> AccessDeniedException (HTTP 400) 'The new resource policy will not allow you
  to update the resource policy in the future.'. Deny only DeleteResourcePolicy (Principal *) -> 200 OK and
  16s later the caller's DeleteResourcePolicy still -> 200 OK. Deny for a non-existent account principal ->
  ValidationException (HTTP 400) 'One or more parameter values were invalid: Invalid principal in policy
  document.'. With ConfirmRemoveSelfResourceAccess=true (throwaway table) -> 200 OK; 16s later Get -> 200, Put
  -> AccessDeniedException (HTTP 400) 'User: arn:aws:sts::<ACCOUNT>:assumed-role/<PRINCIPAL> is not authorized
  to perform: dynamodb:PutResourcePolicy on resource: arn:aws:dynamodb:us-w', Put+Confirm ->
  AccessDeniedException (HTTP 400) 'User: arn:aws:sts::<ACCOUNT>:assumed-role/<PRINCIPAL> is not authorized to
  perform: dynamodb:PutResourcePolicy on resource: arn:aws:dynamodb:us-w', Delete -> AccessDeniedException
  (HTTP 400) 'User: arn:aws:sts::<ACCOUNT>:assumed-role/<PRINCIPAL> is not authorized to perform:
  dynamodb:DeleteResourcePolicy on resource: arn:aws:dynamodb:u' ('with an explicit deny in a resource-based
  policy'), DescribeTable/TagResource -> 200, DeleteTable -> accepted (a later manual DeleteTable succeeded;
  the in-probe attempt hit the tag write lock).
  - ACK: terminal_codes, custom_field, is_iam_policy · ops: PutResourcePolicy, DeleteResourcePolicy · fields:
    Policy, ConfirmRemoveSelfResourceAccess
  - repro: Put Deny-* on dynamodb:PutResourcePolicy with and without ConfirmRemoveSelfResourceAccess; Put
    Deny-* on dynamodb:DeleteResourcePolicy only; then Delete
  - handling: not handled in the controller (as of commit 34b85e6)
  - related: [DDB-TABLE-270](#ddb-table-270), [DDB-TABLE-324](#ddb-table-324), [DDB-TABLE-431](table-streams-encryption-class.md#ddb-table-431), [DDB-TABLE-432](#ddb-table-432), [DDB-TABLE-172](service.md#ddb-table-172), [DDB-TABLE-348](#ddb-table-348),
    [DDB-TABLE-119](table-streams-encryption-class.md#ddb-table-119) · hypotheses: H-S-029 · evidence: table/sub-resources/resource-policy
  - notes: H-S-029 lockout clause: code is AccessDeniedException (hyp said ValidationException). The check
    simulates the caller against the new document (a condition-scoped Deny on the caller's role is caught). A
    confirmed lockout is irreversible for a non-root caller but does not block DeleteTable. Surprise:...
  - full notes: [details/DDB-TABLE-210.md](details/DDB-TABLE-210.md)

- <a id="ddb-table-211"></a>**DDB-TABLE-211** `request-validation` · impact medium · unhandled (not handled in controller) · verified 2026-10-09
  **PutResourcePolicy validation: Resource may name another table/*/index (accepted); >20480 bytes ->
  ValidationException, not LimitExceeded** - Statement Resource = another table's ARN -> 200 OK; Resource='*'
  -> 200 OK; Resource = this table's GSI ARN -> 200 OK; Resource omitted -> ValidationException (HTTP 400)
  'One or more parameter values were invalid: Inv... - see:
  [details/DDB-TABLE-211.md](details/DDB-TABLE-211.md)

- <a id="ddb-table-241"></a>**DDB-TABLE-241** `request-validation` · impact high · unhandled (not handled in controller) · verified 2026-10-09
  **Partial autoscaling update shapes on an existing dimension: Min/Max-only -> ValidationException, policy-only -> ValidationException, empt...**
  Results (code, message): {"E_min_max_only": ["ValidationException", "Failed to update settings for global
  table with name: ‘ackq-41e87e-as-rt’: Parameters 'ScalingPolicyUpdate' are required unless auto scaling is
  being disabled."], "E_policy_only_no_min_max": ["ValidationException", "Failed to update settings for global
  table with name: ‘ackq-41e87e-as-rt’: Parameters 'MaximumUnits', 'MinimumUnits' are required unless auto
  scaling is being d"], "E_policy_only_named_p1": ["ValidationException", "Failed to update settings for
  global table with name: ‘ackq-41e87e-as-rt’: Parameters 'MaximumUnits', 'MinimumUnits' are required unless
  auto scaling is being d"], "E_min_only_with_policy": ["ValidationException", "Failed to update settings for
  global table with name: ‘ackq-41e87e-as-rt’: Parameters 'MaximumUnits' are required unless auto scaling is
  being disabled."], "E_max_only_with_policy": ["ValidationException", "Failed to update settings for global
  table with name: ‘ackq-41e87e-as-rt’: Parameters 'MinimumUnits' are required unless auto scaling is being
  disabled."], "E_empty_struct": ["ValidationException", "Failed to update settings for global table with
  name: ‘ackq-41e87e-as-rt’: Parameters 'MaximumUnits', 'ScalingPolicyUpdate', 'MinimumUnits' are required
  unless "], "E_role_only": ["ValidationException", "Failed to update settings for global table with name:
  ‘ackq-41e87e-as-rt’: Parameters 'MaximumUnits', 'ScalingPolicyUpdate', 'MinimumUnits' are required unless
  "], "E_disabled_false_only": ["ValidationException", "Failed to update settings for global table with name:
  ‘ackq-41e87e-as-rt’: Parameters 'MaximumUnits', 'ScalingPolicyUpdate', 'MinimumUnits' are required unless
  "]}. Policies on the read dimension afterwards: [["ackq-41e87e-p2", 40.0]].
  - ACK: custom_update, terminal_codes · ops: UpdateTableReplicaAutoScaling · fields: MinimumUnits,
    MaximumUnits, ScalingPolicyUpdate, AutoScalingDisabled, AutoScalingRoleArn
  - repro: on a dimension that already has a target+policy send each partial AutoScalingSettingsUpdate shape
  - handling: not handled in the controller (as of commit 34b85e6)
  - related: [DDB-TABLE-253](table-replicas.md#ddb-table-253), [DDB-TABLE-242](#ddb-table-242), [DDB-TABLE-243](#ddb-table-243), [DDB-TABLE-255](table-replicas.md#ddb-table-255) · hypotheses: H-R-111, H-S-042 ·
    evidence: table/round-trip/autoscaling-settings
  - notes: REFUTES H-R-111's last clause: even on a dimension that already has a target+policy, Min/Max
    without ScalingPolicyUpdate -> ValidationException 'Parameters 'ScalingPolicyUpdate' are required unless
    auto scaling is being disabled'; every non-disable update must carry MinimumUnits, MaximumUnits AND...
  - full notes: [details/DDB-TABLE-241.md](details/DDB-TABLE-241.md)

- <a id="ddb-table-270"></a>**DDB-TABLE-270** `request-validation` · impact medium · unhandled (not handled in controller) · verified 2026-10-09
  **A policy denying only dynamodb:DeleteResourcePolicy is accepted without Confirm; 60s later the caller's
  Delete -> AccessDeniedException** - PutResourcePolicy(Deny Principal * on dynamodb:DeleteResourcePolicy +
  Allow GetItem) without ConfirmRemoveSelfResourceAccess -> 200 OK. - see:
  [details/DDB-TABLE-270.md](details/DDB-TABLE-270.md)

- <a id="ddb-table-356"></a>**DDB-TABLE-356** `request-validation` · impact medium · unhandled (not handled in controller) · verified 2026-10-09
  **PutResourcePolicy rejections (ValidationException 400): duplicate Sids, bare-string account Principal,
  Allow to *, non-existent principal** - Two statements with the same Sid -> ValidationException 'One or more
  parameter values were invalid: Invalid policy document: The Statement Ids in the policy are not unique'. -
  see: [details/DDB-TABLE-356.md](details/DDB-TABLE-356.md)

## Update granularity and ordering

- <a id="ddb-table-191"></a>**DDB-TABLE-191** `prerequisite` · impact medium · unhandled (not handled in controller) · verified 2026-10-09
  **Service-linked role AWSServiceRoleForApplicationAutoScaling_DynamoDBTable appears as a side effect of
  global-table operations** - iam:GetRole for the role returned NoSuchEntity at 00:16 UTC (account had never
  used DynamoDB autoscaling); at 00:27:16 UTC it existed, created while the only activity in the account was
  an earlier attempt of this prob... - see: [details/DDB-TABLE-191.md](details/DDB-TABLE-191.md)

- <a id="ddb-table-291"></a>**DDB-TABLE-291** `prerequisite` · impact high · unhandled (not handled in controller) · verified 2026-10-09
  **UpdateTable GSI Create on a PROVISIONED global table is rejected while ANY existing GSI lacks write autoscaling**
  UpdateTable(GlobalSecondaryIndexUpdates Create gsi1, PROVISIONED 1/1) on the 2-replica PROVISIONED table ->
  {"operation": "update_table", "ok": false, "code": "ValidationException", "http_status": 400, "message":
  "GSI write capacity should either be Pay-Per-Request or AutoScaled.", "latency_ms": 884, "client_side":
  false}. Nonexistent index in UpdateTableReplicaAutoScaling -> {"operation":
  "update_table_replica_auto_scaling", "ok": false, "code": "ResourceNotFoundException", "http_status": 400,
  "message": "Failed to update settings for global table with name: ‘ackq-cf966e-as-gsi’ because the global
  secondary indexes with names: ‘[nope]’ do not exist.", "latency_ms": 769, "client_side": false}.
  - ACK: custom_update, terminal_codes · ops: UpdateTable, UpdateTableReplicaAutoScaling · fields:
    GlobalSecondaryIndexUpdates
  - repro: global PROVISIONED table -> UpdateTable add GSI
  - handling: not handled in the controller (as of commit 34b85e6)
  - related: [DDB-TABLE-249](table-replicas.md#ddb-table-249), [DDB-TABLE-288](table-replicas.md#ddb-table-288), [DDB-TABLE-322](#ddb-table-322), [DDB-TABLE-290](#ddb-table-290), [DDB-TABLE-194](table-replicas.md#ddb-table-194) · hypotheses: H-R-108 ·
    evidence: table/dependencies/autoscaling-gsi-and-orphans,
    table/creative/gsi-add-on-provisioned-global-table
  - notes: CORRECTED by table/creative/gsi-add-on-provisioned-global-table - the rejection was caused by the
    EXISTING gsi0, whose write autoscaling had just been disabled via UpdateTableReplicaAutoScaling, not by
    the new GSI; on a global table with no un-autoscaled GSI the same...
  - full notes: [details/DDB-TABLE-291.md](details/DDB-TABLE-291.md)

- <a id="ddb-table-303"></a>**DDB-TABLE-303** `prerequisite` · impact medium · unhandled (not handled in controller) · verified 2026-10-09
  **Kinesis destination is independent of DynamoDB Streams: no StreamSpecification appears, and toggling
  Streams leaves the destination ACTIVE** - With the destination ACTIVE on a table created without Streams,
  DescribeTable shows StreamSpecification=None LatestStreamArn=None. - see:
  [details/DDB-TABLE-303.md](details/DDB-TABLE-303.md)

- <a id="ddb-table-319"></a>**DDB-TABLE-319** `prerequisite` · impact high · unhandled (not handled in controller) · verified 2026-10-09
  **Deleted Kinesis stream: destination stays ACTIVE (293s watched, no description) and Update still succeeds; Disable+recreate+Enable works**
  Enable(E3) -> ACTIVE; DeleteStream(E3) -> 200; the stream was gone from Kinesis at t=10s but
  DescribeKinesisStreamingDestination kept reporting ['self:e3=ACTIVE'] at every 10s sample for 293s with no
  DestinationStatusDescription. UpdateKinesisStreamingDestination(precision) on the dead destination -> 200
  OK, UPDATING for 123.55s, then ACTIVE with the new precision. Disable -> 200 OK (Describe right after still
  ACTIVE for ~100ms; see the Describe-lag finding). Recreating a stream with the same name (identical ARN) and
  Enable 6s later -> 200 OK, ENABLING 2.02s, then ACTIVE (precision reset: [('self:e3', 'ACTIVE', None)]).
  Deleting the stream DURING ENABLING instead: Enable(E1) -> 200 ENABLING, DeleteStream 50ms later -> 200;
  ENABLING lasted 17.19s (vs 2-7s normally) and ended as ENABLE_FAILED 'User does not have a permission to use
  kinesis stream'.
  - ACK: references, annotation-shadow-state, custom_update, pre-delete-cleanup · ops:
    EnableKinesisStreamingDestination, DescribeKinesisStreamingDestination, UpdateKinesisStreamingDestination,
    DisableKinesisStreamingDestination · fields: KinesisDataStreamDestinations[].DestinationStatus,
    KinesisDataStreamDestinations[].DestinationStatusDescription
  - repro: Enable; wait ACTIVE; DeleteStream; Describe every 10s for 5 min; Update; Disable; recreate the
    stream; Enable. Separately: Enable then DeleteStream within 100ms; poll
  - measurements: broken_watch_s=293, stream_gone_at_s=10, update_on_dead_destination_updating_s=123.55,
    enabling_when_stream_deleted_mid_flight_s=17.19
  - handling: not handled in the controller (as of commit 34b85e6)
  - related: [DDB-TABLE-299](#ddb-table-299), [DDB-TABLE-300](#ddb-table-300), [DDB-TABLE-301](#ddb-table-301), [DDB-TABLE-302](#ddb-table-302), [DDB-TABLE-304](#ddb-table-304), [DDB-TABLE-269](#ddb-table-269),
    [DDB-TABLE-268](#ddb-table-268), [DDB-TABLE-303](#ddb-table-303), [DDB-TABLE-235](#ddb-table-235), [DDB-TABLE-236](#ddb-table-236), [DDB-TABLE-234](#ddb-table-234), [DDB-TABLE-460](#ddb-table-460) · hypotheses:
    H-S-035, H-S-034, H-S-038 · evidence: table/error-taxonomy/kinesis-destination-errors
  - notes: H-S-035 confirmed: DestinationStatus is not a health signal; the controller must cross-check the
    Kinesis stream (DescribeStreamSummary) itself. H-S-034's ENABLE_FAILED recipe (stream deleted
    mid-ENABLING) confirmed, but with the misleading 'permission' description. H-S-038's 'Disable of...
  - full notes: [details/DDB-TABLE-319.md](details/DDB-TABLE-319.md)

## Field behavior (defaults, normalization, shapes, immutability)

- <a id="ddb-table-193"></a>**DDB-TABLE-193** `read-gap` · impact high · unhandled (not handled in controller) · verified 2026-10-09
  **DescribeTable carries no autoscaling indicator: changed paths after registering autoscaling = none**
  DescribeTable paths that changed between before and after the DynamoDB autoscaling updates (bounds
  containing current capacity): []; new keys []; payload mentions autoscaling: False.
  - ACK: custom_field, compare.is_ignored+delta_pre_compare · ops: DescribeTable,
    DescribeTableReplicaAutoScaling · fields: ProvisionedThroughput
  - repro: DescribeTable -> UpdateTableReplicaAutoScaling -> DescribeTable -> diff
  - handling: not handled in the controller (as of commit 34b85e6)
  - related: [DDB-TABLEREPLICAAUTOSCALING-001](table-replicas.md#ddb-tablereplicaautoscaling-001), [DDB-TABLE-197](#ddb-table-197), [DDB-TABLE-196](#ddb-table-196), [DDB-TABLE-189](#ddb-table-189), [DDB-TABLE-195](#ddb-table-195),
    [DDB-TABLE-292](table-replicas.md#ddb-table-292) · hypotheses: H-R-119 · evidence: table/sub-resources/replica-autoscaling-facade

- <a id="ddb-table-208"></a>**DDB-TABLE-208** `normalization` · impact high · handled · verified 2026-10-09
  **GetResourcePolicy returns a canonicalized document (minified, 1-element Action -> string, account id -> root ARN); byte-stable if canonical**
  Pretty-printed Put (331 bytes) reads back as 234 bytes: byte-identical=False, json-equal=False (the
  1-element Action list came back as a string). Minified Put byte-identical=False; reordered keys
  byte-identical=False (keys come back as Version/Statement/Sid/Effect/Principal/Action/Resource); Action as
  string byte-identical=False (type str); Principal '<account-id>' reads back as {'AWS':
  'arn:aws:iam::<ACCOUNT>:root'}; a 19456-byte whitespace-padded document reads back as 228 bytes. Example
  read-back:
  {"Version":"2012-10-17","Statement":[{"Sid":"AckqAllowRead","Effect":"Allow","Principal":{"AWS":"arn:aws:iam::<ACCOUNT>:root"},"Action":"dynamodb:GetItem","Resource":"arn:aws:dynamodb:us-west-2:<ACCOUNT>:table/ackq-71f899-rp"}]}
  - ACK: is_document, is_iam_policy, compare.is_ignored+delta_pre_compare · ops: PutResourcePolicy,
    GetResourcePolicy · fields: ResourcePolicy, Policy
  - repro: Put each formatting variant; GetResourcePolicy until RevisionId matches; compare bytes
  - measurements: get_lag_s_after_first_put=2.04, pretty_sent_len=331, returned_len=234
  - handling: handled via `generator.yaml:32-37; pkg/resource/table/hooks_resource_policy.go:30-137; pkg/resource/table/hooks_resource_policy.go:139-177; pkg/resource/table/hooks_resource_policy_test.go:25-235`
  - related: [DDB-TABLE-244](#ddb-table-244), [DDB-TABLE-245](#ddb-table-245), [DDB-TABLE-246](#ddb-table-246), [DDB-TABLE-248](#ddb-table-248), [DDB-TABLE-346](#ddb-table-346), [DDB-TABLE-233](#ddb-table-233),
    [DDB-TABLE-351](#ddb-table-351), [DDB-TABLE-207](#ddb-table-207), [DDB-TABLE-352](#ddb-table-352), [DDB-TABLE-353](#ddb-table-353), [DDB-TABLE-354](#ddb-table-354), [DDB-TABLE-355](#ddb-table-355), [DDB-TABLE-356](#ddb-table-356) ·
    hypotheses: H-S-028 · evidence: table/sub-resources/resource-policy
  - notes: H-S-028 REFUTED: the service canonicalizes IAM-style; a string comparison of spec vs Get sees
    permanent drift for pretty-printed/list-form specs - compare parsed+canonicalized JSON or track
    RevisionId.
  - full notes: [details/DDB-TABLE-208.md](details/DDB-TABLE-208.md)

- <a id="ddb-table-233"></a>**DDB-TABLE-233** `read-gap` · impact high · handled · verified 2026-10-09, re-verified
  **Create-time ResourcePolicy readable ~2.09s after CreateTable while still CREATING; earlier reads -> ResourceNotFoundException**
  CreateTable(ResourcePolicy=p) -> 200 TableStatus=CREATING; TableDescription has no policy/revision field
  (keys ['AttributeDefinitions', 'BillingModeSummary', 'CreationDateTime', 'DeletionProtectionEnabled',
  'ItemCount', 'KeySchema', 'ProvisionedThroughput', 'TableArn', 'TableId', 'TableName', 'TableSizeBytes',
  'TableStatus']). Polling every 1s: for the first 1.06s GetResourcePolicy -> ResourceNotFoundException
  'Requested resource not found: Table: ackq-f57f9f-pk-a not found' and DescribeKinesisStreamingDestination ->
  ResourceNotFoundException (same message), both while DescribeTable already returns the table as CREATING.
  From 2.09s (table still CREATING until 7.25s) Get -> 200 with RevisionId 1791506149206 (= epoch ms of the
  create) and Describe kinesis -> 200 with an empty list. Timeline (table, get, kinesis): [(['CREATING',
  'ERR:ResourceNotFoundException', 'ERR:ResourceNotFoundException'], 0.0, 1.06), (['CREATING',
  'OK:1791506149206', 'EMPTY'], 2.09, 6.22), (['ACTIVE', 'OK:1791506149206', 'EMPTY'], 7.25, 12.41)].
  DescribeTable at ACTIVE exposes none of the sub-resources: ['AttributeDefinitions', 'BillingModeSummary',
  'CreationDateTime', 'DeletionProtectionEnabled', 'ItemCount', 'KeySchema', 'ProvisionedThroughput',
  'TableArn', 'TableId', 'TableName', 'TableSizeBytes', 'TableStatus', 'WarmThroughput'].
  - ACK: late_initialize, custom_create, synced.when, requeue · ops: CreateTable, GetResourcePolicy,
    DescribeKinesisStreamingDestination · fields: ResourcePolicy, RevisionId
  - repro: CreateTable(ResourcePolicy=...); poll DescribeTable + GetResourcePolicy +
    DescribeKinesisStreamingDestination every 1s until 5s after ACTIVE
  - measurements: active_at_s=7.25, first_get_200_s=2.09, rnf_window_s=1.06
  - handling: handled via `generator.yaml:32-37; pkg/resource/table/hooks_resource_policy.go:30-137; test/e2e/tests/test_table.py:1139-1180; pkg/resource/table/hooks_resource_policy.go:30-45; pkg/resource/table/hooks_tags.go:138-168; pkg/resource/table/hooks.go:549-553; templates/hooks/table/sdk_create_post_set_output.go.tpl:1-4; bcd26e1`
  - related: [DDB-TABLE-111](table-subresources.md#ddb-table-111), [DDB-TABLE-115](service.md#ddb-table-115), [DDB-TABLE-234](#ddb-table-234), [DDB-TABLE-114](service.md#ddb-table-114), [DDB-TABLE-244](#ddb-table-244), [DDB-TABLE-245](#ddb-table-245),
    [DDB-TABLE-246](#ddb-table-246), [DDB-TABLE-248](#ddb-table-248), [DDB-TABLE-346](#ddb-table-346), [DDB-TABLE-208](#ddb-table-208), [DDB-TABLE-351](#ddb-table-351) · hypotheses: H-S-030, H-S-118 ·
    evidence: table/state-machine/policy-kinesis-admissibility, table/creative/reverify-set-a2
  - notes: H-S-030 partially refuted: the create-time policy IS readable before ACTIVE (about 2s after
    CreateTable), and the early error is ResourceNotFoundException (table-level metadata lag), not
    PolicyNotFoundException. The 'not visible in CreateTable/DescribeTable output' clause is confirmed.
  - full notes: [details/DDB-TABLE-233.md](details/DDB-TABLE-233.md)

- <a id="ddb-table-237"></a>**DDB-TABLE-237** `server-default` · impact medium · unhandled (not handled in controller) · verified 2026-10-09
  **Minimal autoscaling update reads back with server PolicyName
  'DynamoDB<Read|Write>CapacityUtilization:table/<name>', SLR role, no cooldowns** - After
  UpdateTableReplicaAutoScaling(read Min 1 Max 10 TargetValue 70) Describe shows settings keys
  ['AutoScalingRoleArn', 'MaximumUnits', 'MinimumUnits', 'ScalingPolicies'], policy keys ['PolicyName',
  'TargetTrackingS... - see: [details/DDB-TABLE-237.md](details/DDB-TABLE-237.md)

- <a id="ddb-table-300"></a>**DDB-TABLE-300** `server-default` · impact high · unhandled (not handled in controller) · verified 2026-10-09, re-verified
  **ApproximateCreationDateTimePrecision absent until set (absent == MILLISECOND server-side); Update -> UPDATING ~124-152 s; same value -> 400**
  After Enable without configuration the entry has NO ApproximateCreationDateTimePrecision field (keys
  ['DestinationStatus', 'StreamArn']). Update(MICROSECOND) -> 200 OK DestinationStatus=UPDATING,
  UpdateKinesisStreamingConfiguration echoed {'ApproximateCreationDateTimePrecision': 'MICROSECOND'}; Describe
  right after: status UPDATING, precision MICROSECOND (the new value is shown while UPDATING); UPDATING lasted
  152.04s (124-152s across 3 runs). During UPDATING: Update again -> ValidationException (HTTP 400) 'Table is
  not in a valid state to enable Kinesis Streaming Destination: Kinesis streaming is not in ACTIVE state.
  Updates are only allowed in ACTIVE st'; Disable -> ValidationException (HTTP 400) 'Table is not in a valid
  state to enable Kinesis Streaming Destination: KinesisStreamingDestination must be ACTIVE to perform DISABLE
  operation.'; Enable -> ValidationException (HTTP 400) 'Table is not in a valid state to enable Kinesis
  Streaming Destination: EnableKinesisStreamingDestination must be DISABLED or ENABLE_FAILED to perform '.
  Update to the SAME precision -> ValidationException (HTTP 400) 'Invalid Request: Precision is already set to
  the desired value of MICROSECOND for tableId: 9b2e64ec-aad6-4e61-8d61-259070a9ccc7, kdsArn: arn:aws:kines'.
  Update without UpdateKinesisStreamingConfiguration -> ValidationException (HTTP 400) 'Streaming destination
  cannot be updated with given parameters: UpdateKinesisStreamingConfiguration cannot be null or contain only
  null values'; with an empty configuration -> ValidationException (HTTP 400) 'Streaming destination cannot be
  updated with given parameters: UpdateKinesisStreamingConfiguration cannot be null or contain only null
  values'. After Disable + re-Enable the field is absent again: [None].
  - ACK: compare.nil_equals_zero_value, late_initialize, custom_update, requeue, e2e-timing · ops:
    UpdateKinesisStreamingDestination, DescribeKinesisStreamingDestination · fields:
    KinesisDataStreamDestinations[].ApproximateCreationDateTimePrecision, UpdateKinesisStreamingConfiguration
  - repro: Enable without config; Describe; Update(MICROSECOND); poll 1/s; Update(MICROSECOND) again; Update
    without config; Disable; Enable; Describe
  - measurements: updating_duration_s=152.04
  - handling: not handled in the controller (as of commit 34b85e6)
  - related: [DDB-TABLE-299](#ddb-table-299), [DDB-TABLE-301](#ddb-table-301), [DDB-TABLE-302](#ddb-table-302), [DDB-TABLE-304](#ddb-table-304), [DDB-TABLE-269](#ddb-table-269), [DDB-TABLE-268](#ddb-table-268),
    [DDB-TABLE-319](#ddb-table-319), [DDB-TABLE-303](#ddb-table-303), [DDB-TABLE-235](#ddb-table-235), [DDB-TABLE-236](#ddb-table-236), [DDB-TABLE-234](#ddb-table-234), [DDB-TABLE-460](#ddb-table-460), [DDB-TABLE-085](table-subresources.md#ddb-table-085),
    [DDB-TABLE-091](table-subresources.md#ddb-table-091), [DDB-TABLE-341](table-subresources.md#ddb-table-341), [DDB-TABLE-147](table-subresources.md#ddb-table-147), [DDB-TABLE-207](#ddb-table-207), [DDB-TABLE-383](table-streams-encryption-class.md#ddb-table-383), [DDB-TABLE-205](#ddb-table-205) · hypotheses:
    H-S-036 · evidence: table/sub-resources/kinesis-destination, table/creative/reverify-set-a2
  - notes: H-S-036: 'Describe materializes MILLISECOND' REFUTED (field absent, so a controller must treat
    absent == MILLISECOND); 'Update transitions UPDATING->ACTIVE in seconds' refuted (2-2.5 minutes); 'same
    precision -> ValidationException' confirmed ('Precision is already set to the desired value')....
  - full notes: [details/DDB-TABLE-300.md](details/DDB-TABLE-300.md)

- <a id="ddb-table-315"></a>**DDB-TABLE-315** `requested-vs-effective` · impact high · unhandled (not handled in controller) · verified 2026-10-09
  **Manual UpdateTable RCU below the autoscaling Min is accepted and NOT re-enforced by AAS within 12 min (no activity, no alarm)**
  With read autoscaling Min 10 Max 20 and RCU 10, UpdateTable RCU=5 -> {"operation": "update_table", "ok":
  false, "code": "ResourceInUseException", "http_status": 400, "message": "The resource which you are
  attempting to change is in use.", "latency_ms": 567, "client_side": false}. Transitions: [{"t_s": 0.2,
  "status": "UPDATING", "rcu": 10, "wcu": 5, "decreases": 0, "replica": {"us-east-1": ["ACTIVE",
  {"ReadCapacityUnits": 5}]}}, {"t_s": 41.2, "status": "ACTIVE", "rcu": 5, "wcu": 5, "decreases": 1,
  "replica": {"us-east-1": ["ACTIVE", null]}}, {"t_s": 491.3, "status": "ACTIVE", "rcu": 5, "wcu": 1,
  "decreases": 2, "replica": {"us-east-1": ["ACTIVE", null]}}]. Scaling activities: []. Alarm states after:
  {"write": {"AlarmHigh": "OK", "AlarmLow": "ALARM", "ProvisionedCapacityHigh": "INSUFFICIENT_DATA",
  "ProvisionedCapacityLow": "OK"}, "read": {"AlarmHigh": "OK", "AlarmLow": "ALARM", "ProvisionedCapacityHigh":
  "OK", "ProvisionedCapacityLow": "OK"}}.
  - ACK: compare.is_ignored+delta_pre_compare, custom_update · ops: UpdateTable, DescribeTable · fields:
    ProvisionedThroughput.ReadCapacityUnits
  - repro: autoscaled read Min 10 -> UpdateTable RCU=5 -> poll DescribeTable 10 s /
    describe-scaling-activities 30 s
  - measurements: seconds_until_reenforced=0.2, watch_elapsed_s=726.4
  - handling: not handled in the controller (as of commit 34b85e6)
  - related: [DDB-TABLE-314](#ddb-table-314), [DDB-TABLE-195](#ddb-table-195), [DDB-TABLE-256](table-replicas.md#ddb-table-256), [DDB-TABLE-321](table-replicas.md#ddb-table-321), [DDB-TABLE-306](#ddb-table-306), [DDB-TABLE-200](table-replicas.md#ddb-table-200),
    [DDB-TABLE-296](table-replicas.md#ddb-table-296), [DDB-TABLE-263](table-global-tables.md#ddb-table-263), [DDB-TABLE-327](table-replicas.md#ddb-table-327) · hypotheses: H-R-117, H-R-132 · evidence:
    table/mutation-matrix/autoscaling-vs-throughput
  - notes: REFUTES H-R-117's 'AAS re-enforces the bounds without an alarm breach within ~10 minutes': with Min
    10 and a manual RCU=5, no scaling activity occurred in 726 s; ProvisionedCapacityLow stayed OK (threshold
    5.0, LessThanThreshold, 3x300 s) and AlarmLow was already in ALARM (idle table) but cannot...
  - full notes: [details/DDB-TABLE-315.md](details/DDB-TABLE-315.md)

- <a id="ddb-table-317"></a>**DDB-TABLE-317** `requested-vs-effective` · impact medium · unhandled (not handled in controller) · verified 2026-10-09
  **Switching an autoscaled global table to PAY_PER_REQUEST keeps the AAS targets/policies; Describe shows
  Min/Max AND AutoScalingDisabled=true** - UpdateTable BillingMode=PAY_PER_REQUEST -> 200, timeline [{"value":
  "UPDATING|rep:ACTIVE", "from_s": 0.15, "to_s": 218.12, "duration_s": 217.97}, {"value": "ACTIVE|rep:ACTIVE",
  "from_s": 218.12, "to_s": null, "duratio... - see: [details/DDB-TABLE-317.md](details/DDB-TABLE-317.md)

- <a id="ddb-table-322"></a>**DDB-TABLE-322** `server-default` · impact high · unhandled (not handled in controller) · verified 2026-10-09
  **Adding a PROVISIONED GSI to a PROVISIONED global table succeeds and DynamoDB auto-registers AAS write autoscaling for the new GSI**
  UpdateTable(GlobalSecondaryIndexUpdates Create gsi1, ProvisionedThroughput 1/1) on a 2-replica PROVISIONED
  table whose table write capacity is autoscaled -> 200 (no autoscaling parameters exist on UpdateTable).
  While IndexStatus=CREATING an AAS scalable target table/<name>/index/gsi1 dynamodb:index:WriteCapacityUnits
  already existed in BOTH regions; once ACTIVE the GSI reads in DescribeTableReplicaAutoScaling as write
  {MinimumUnits 1, MaximumUnits 10, SLR role, policy
  'DynamoDBWriteCapacityUtilization:table/<name>/index/gsi1' TargetValue 70} and read
  {AutoScalingDisabled:true, ScalingPolicies:[]}. AAS RegisterScalableTarget for the not-yet-existing index id
  -> ValidationException 'DynamoDB index does not exist: table/<name>/index/gsi1'.
  - ACK: late_initialize, compare.is_ignored+delta_pre_compare, scope:skip · ops: UpdateTable,
    RegisterScalableTarget, PutScalingPolicy · fields: GlobalSecondaryIndexUpdates
  - repro: PROVISIONED global table -> UpdateTable add GSI (ValidationException) -> register-scalable-target
    table/<n>/index/<gsi> + put-scaling-policy -> UpdateTable add GSI
  - handling: not handled in the controller (as of commit 34b85e6)
  - related: [DDB-TABLE-249](table-replicas.md#ddb-table-249), [DDB-TABLE-288](table-replicas.md#ddb-table-288), [DDB-TABLE-291](#ddb-table-291), [DDB-TABLE-290](#ddb-table-290), [DDB-TABLE-194](table-replicas.md#ddb-table-194), [DDB-TABLE-296](table-replicas.md#ddb-table-296),
    [DDB-TABLE-251](table-replicas.md#ddb-table-251), [DDB-TABLE-265](table-replicas.md#ddb-table-265), [DDB-TABLE-294](table-replicas.md#ddb-table-294), [DDB-TABLE-295](table-replicas.md#ddb-table-295), [DDB-TABLE-297](table-replicas.md#ddb-table-297) · hypotheses: H-R-108, H-R-131 ·
    evidence: table/creative/gsi-add-on-provisioned-global-table
  - notes: REFUTES the doc-based clause of H-R-108 ('a GSI added later gets NO autoscaling') for 2019.11.21
    PROVISIONED global tables: write autoscaling Min 1 Max 10 target 70 is created server-side (this is how
    DynamoDB keeps the 'GSI write capacity must be autoscaled' invariant). The earlier rejection seen...
  - full notes: [details/DDB-TABLE-322.md](details/DDB-TABLE-322.md)

- <a id="ddb-table-351"></a>**DDB-TABLE-351** `normalization` · impact high · unhandled (not handled in controller) · verified 2026-10-09
  **Policy canonical form is a byte-stable fixpoint (minified; Version,Statement / Sid,Effect,Principal,Action,Resource,Condition); re-Put no-op**
  20/25 formatting variants were accepted; every read-back was minified (no whitespace outside strings) with
  top-level keys ['Version', 'Statement'] and statement keys in the order ['Sid', 'Effect', 'Principal',
  'Action', 'Resource', 'Condition'] (Condition last, NotAction in Action's slot). A document SENT in that
  form read back byte-identical (True), and the Get output of a scrambled-key variant Put on a fresh table
  read back byte-identical (True); a pretty-printed re-Put of it was a no-op (same RevisionId: True).
  Re-Putting the Get output inside the 15 s window returned the same RevisionId (no-op) for 19/20 accepted
  variants; the exception is 'Version omitted' (see the Version finding). Get showed the new RevisionId
  1.07-2.35 s after Put.
  - ACK: is_document, is_iam_policy, compare.is_ignored+delta_pre_compare · ops: PutResourcePolicy,
    GetResourcePolicy · fields: Policy, RevisionId
  - repro: Put each variant on a hash-only PPR table; Get until RevisionId matches; compare bytes; re-Put the
    Get output; Put the Get output on a fresh table
  - measurements: variants_accepted=20, reput_noop=19, get_lag_s_min=1.07, get_lag_s_max=2.35
  - handling: not handled in the controller (as of commit 34b85e6)
  - related: [DDB-TABLE-207](#ddb-table-207), [DDB-TABLE-208](#ddb-table-208), [DDB-TABLE-211](#ddb-table-211), [DDB-TABLE-244](#ddb-table-244), [DDB-TABLE-245](#ddb-table-245), [DDB-TABLE-246](#ddb-table-246),
    [DDB-TABLE-248](#ddb-table-248), [DDB-TABLE-346](#ddb-table-346), [DDB-TABLE-233](#ddb-table-233), [DDB-TABLE-352](#ddb-table-352), [DDB-TABLE-353](#ddb-table-353), [DDB-TABLE-354](#ddb-table-354), [DDB-TABLE-355](#ddb-table-355),
    [DDB-TABLE-356](#ddb-table-356) · hypotheses: H-S-028 · evidence: table/round-trip/policy-canonicalization
  - notes: Refines [DDB-TABLE-208](#ddb-table-208) ('never byte-identical'): the service's canonical form IS reproducible, so a
    controller can canonicalize the spec (minify, fixed key order, the rewrites below) and string-compare, or
    simply compare parsed JSON after applying the rewrites. Variants NOT no-op on re-Put:...
  - full notes: [details/DDB-TABLE-351.md](details/DDB-TABLE-351.md)

- <a id="ddb-table-352"></a>**DDB-TABLE-352** `normalization` · impact high · unhandled (not handled in controller) · verified 2026-10-09
  **Principal rewrites: {AWS: account-id} -> root ARN; duplicate [id, ARN] -> one string; Allow * rejected (unbounded access); bare id rejected**
  Principal {"AWS": "<account-id>"} reads back as {'AWS': 'arn:aws:iam::<ACCOUNT>:root'}. Principal {"AWS":
  ["<account-id>", "arn:aws:iam::<account-id>:root"]} (same principal twice) reads back collapsed to the
  STRING {'AWS': 'arn:aws:iam::<ACCOUNT>:root'} (list -> string after de-duplication). Principal "*" ->
  ValidationException 'Resource-based policy grants unbounded access in one or more elements. Please revise
  the policy to ensure least-privilege form of access'; {"AWS": "*"} -> ValidationException 'Resource-based
  policy grants unbounded access in one or more elements. Please revise the policy to ensure least-privilege
  form of access' (an Allow to everyone is refused by a DynamoDB guardrail, not by IAM syntax). Principal
  "<account-id>" as a bare string -> ValidationException 'One or more parameter values were invalid: Invalid
  policy document: Syntax error at position (1,94)'. A list naming own root plus arn:aws:iam::<ACCOUNT>:root
  -> ValidationException 'One or more parameter values were invalid: Invalid principal in policy document.'
  (non-existent account; IAM validates principals exist).
  - ACK: is_iam_policy, compare.is_ignored+delta_pre_compare, terminal_codes · ops: PutResourcePolicy,
    GetResourcePolicy · fields: Policy.Statement.Principal
  - repro: Put each Principal form; Get after ~2 s; compare
  - handling: not handled in the controller (as of commit 34b85e6)
  - related: [DDB-TABLE-207](#ddb-table-207), [DDB-TABLE-208](#ddb-table-208), [DDB-TABLE-211](#ddb-table-211), [DDB-TABLE-244](#ddb-table-244), [DDB-TABLE-351](#ddb-table-351), [DDB-TABLE-353](#ddb-table-353),
    [DDB-TABLE-354](#ddb-table-354), [DDB-TABLE-355](#ddb-table-355), [DDB-TABLE-356](#ddb-table-356) · hypotheses: H-S-028 · evidence:
    table/round-trip/policy-canonicalization
  - notes: Account ids must be expanded to root ARNs and principal lists de-duplicated/collapsed before
    comparing with Get; the 'unbounded access' ValidationException is terminal (user must fix the spec) and is
    specific to Allow-* resource policies.
  - full notes: [details/DDB-TABLE-352.md](details/DDB-TABLE-352.md)

- <a id="ddb-table-353"></a>**DDB-TABLE-353** `normalization` · impact medium · unhandled (not handled in controller) · verified 2026-10-09
  **Action/Resource rewrites: 1-element list -> string; multi-element lists verbatim (order, duplicates kept);
  lowercase action stored as-is** - Action ["dynamodb:GetItem"] -> dynamodb:GetItem and Resource [<arn>] ->
  string (str). - see: [details/DDB-TABLE-353.md](details/DDB-TABLE-353.md)

- <a id="ddb-table-354"></a>**DDB-TABLE-354** `normalization` · impact high · unhandled (not handled in controller) · verified 2026-10-09
  **Version omitted -> Get adds Version 2008-10-17 (not 2012), yet omitted vs explicit 2008 are DIFFERENT documents for idempotency (ping-pong)**
  A policy sent without Version reads back with 2008-10-17 (IAM's legacy default; explicit 2008-10-17 and
  2012-10-17 are preserved). But the two forms are not equivalent to the RevisionId/idempotency check: Put(no
  Version) -> rev 1791521505559; Put(the Get output with 2008-10-17) 16 s later -> NEW rev 1791521523543 (a
  real change, not the usual no-op; inside the 15 s window it is even ThrottlingException); Put(no Version)
  again -> new rev again (True); Put(no Version) twice -> no-op (True). So the Get output is NOT a fixpoint
  for a Version-less spec: a controller that re-Puts what it read (or compares spec vs Get) will churn
  RevisionIds every reconcile and hit the 15 s ThrottlingException.
  - ACK: is_iam_policy, compare.is_ignored+delta_pre_compare, annotation-shadow-state · ops:
    PutResourcePolicy, GetResourcePolicy · fields: Policy.Version, RevisionId
  - repro: Put({Statement:[...]}) ; Get -> Version 2008-10-17 ; 16 s ; Put(Get output) -> new RevisionId ; 16
    s ; Put(no Version) -> new RevisionId ; Put(no Version) -> same
  - handling: not handled in the controller (as of commit 34b85e6)
  - related: [DDB-TABLE-207](#ddb-table-207), [DDB-TABLE-208](#ddb-table-208), [DDB-TABLE-211](#ddb-table-211), [DDB-TABLE-244](#ddb-table-244), [DDB-TABLE-207](#ddb-table-207), [DDB-TABLE-351](#ddb-table-351),
    [DDB-TABLE-352](#ddb-table-352), [DDB-TABLE-353](#ddb-table-353), [DDB-TABLE-355](#ddb-table-355), [DDB-TABLE-356](#ddb-table-356) · hypotheses: H-S-028 · evidence:
    table/round-trip/policy-canonicalization
  - notes: Qualifies [DDB-TABLE-207](#ddb-table-207) (equivalent re-Puts are no-ops): equivalence is computed on the submitted
    document with Version treated as a literal field, while Get fills the default. Controllers should always
    send an explicit Version (2012-10-17) and treat a missing Version in the spec as 2008-10-17 when...
  - full notes: [details/DDB-TABLE-354.md](details/DDB-TABLE-354.md)

- <a id="ddb-table-355"></a>**DDB-TABLE-355** `normalization` · impact medium · unhandled (not handled in controller) · verified 2026-10-09
  **Lenient parsing: Effect allow -> Allow (not rejected); Statement object -> 1-element list; unicode escapes
  decoded; absent Sid stays absent** - "Effect": "allow" -> Allow (rewritten 200, not rejected). - see:
  [details/DDB-TABLE-355.md](details/DDB-TABLE-355.md)

## Response fidelity and consistency

- <a id="ddb-table-189"></a>**DDB-TABLE-189** `response-fidelity` · impact medium · unhandled (not handled in controller) · verified 2026-10-09
  **Autoscaling never configured reads as {"AutoScalingDisabled": true, "ScalingPolicies": []}; AAS-created
  custom policy round-trips: diff {...** - 2-replica PROVISIONED table: description keys ['Replicas',
  'TableName', 'TableStatus']; replica keys ['GlobalSecondaryIndexes', 'RegionName',
  'ReplicaProvisionedReadCapacityAutoScalingSettings', 'ReplicaProvisionedWri... - see:
  [details/DDB-TABLE-189.md](details/DDB-TABLE-189.md)

- <a id="ddb-table-243"></a>**DDB-TABLE-243** `response-fidelity` · impact medium · unhandled (not handled in controller) · verified 2026-10-09
  **AAS delete-scaling-policy leaves a target that Describe shows as {"AutoScalingDisabled": true,
  "ScalingPolicies": []}; DynamoDB policy-on...** - Write settings before {"MinimumUnits": 1, "MaximumUnits":
  10, "AutoScalingRoleArn": "arn:aws:iam::<ACCOUNT>:role/<PRINCIPAL>", "ScalingPolicies": [{"PolicyName":
  "ackq-41e87e-w0", "TargetTrackingScalingPolicyConfi. - see:
  [details/DDB-TABLE-243.md](details/DDB-TABLE-243.md)

- <a id="ddb-table-244"></a>**DDB-TABLE-244** `eventual-consistency` · impact high · handled · verified 2026-10-09
  **After Put on a policy-less table, Get returns PolicyNotFoundException for ~1.467s (max 1.707s) in 6/6 trials, then the new RevisionId**
  6 trials of PutResourcePolicy (200, RevisionId R) on a table with no policy, then GetResourcePolicy every
  200ms: Get kept returning PolicyNotFoundException (HTTP 400) in 6 trials; time until Get returned R = {'n':
  6, 'min': 1.048, 'median': 1.467, 'max': 1.707} (last PolicyNotFound seen at {'n': 6, 'min': 0.839,
  'median': 1.257, 'max': 1.498}). Intermediate 200 with a different RevisionId: 0 trials; flapping back after
  the first match: 0; other error codes while polling: ['PolicyNotFoundException']; trials not converging
  within 30s: 0. Put latency ms {'n': 12, 'min': 73, 'median': 84.5, 'max': 130}; Get latency ms {'n': 18,
  'min': 7, 'median': 8.0, 'max': 10}.
  - ACK: requeue, late_initialize, e2e-timing · ops: PutResourcePolicy, GetResourcePolicy · fields:
    ResourcePolicy, RevisionId
  - repro: ACTIVE table without policy: PutResourcePolicy; GetResourcePolicy every 200ms for up to 30s; repeat
  - measurements: put_fresh_first_match_s.n=6, put_fresh_first_match_s.min=1.048,
    put_fresh_first_match_s.median=1.467, put_fresh_first_match_s.max=1.707, trials=6,
    trials_with_pnf_after_put=6
  - handling: handled via `pkg/resource/table/hooks_resource_policy.go:97-103; pkg/resource/table/hooks_resource_policy.go:129-132; test/e2e/tests/test_table.py:1139-1180; pkg/resource/table/hooks_resource_policy.go:30-45`
  - related: [DDB-TABLE-245](#ddb-table-245), [DDB-TABLE-246](#ddb-table-246), [DDB-TABLE-248](#ddb-table-248), [DDB-TABLE-346](#ddb-table-346), [DDB-TABLE-233](#ddb-table-233), [DDB-TABLE-208](#ddb-table-208),
    [DDB-TABLE-351](#ddb-table-351) · hypotheses: H-S-107, H-S-007, H-S-044 · evidence:
    table/consistency-windows/resource-policy-windows
  - notes: H-S-107/H-S-007 PolicyNotFound-after-Put clause: confirmed; the first 200 already carries the Put's
    RevisionId (no intermediate revision): True. H-S-044's 'read-your-writes in practice' is refuted by these
    windows (see the read-your-writes finding).

- <a id="ddb-table-245"></a>**DDB-TABLE-245** `stale-response` · impact high · handled · verified 2026-10-09
  **Put over an existing policy: Get serves the OLD document+RevisionId (HTTP 200) for ~1.792s (max 2.734s) in 6/6 trials**
  Put(B) replacing policy A (16s after A), then Get every 200ms: Put results ['200 OK']; time until Get
  returned B's RevisionId = {'n': 6, 'min': 1.471, 'median': 1.792, 'max': 2.734}; trials where Get returned
  the OLD RevisionId with HTTP 200 and no staleness indicator = 6 (53 stale reads in total);
  PolicyNotFoundException reads during the switch = 0; flapping after the first match = 0; timeouts = 0.
  - ACK: requeue, compare.is_ignored+delta_pre_compare, annotation-shadow-state · ops: PutResourcePolicy,
    GetResourcePolicy · fields: ResourcePolicy, RevisionId
  - repro: Put(A); wait 16s; Put(B); GetResourcePolicy every 200ms until RevisionId==B
  - measurements: put_over_first_match_s.n=6, put_over_first_match_s.min=1.471,
    put_over_first_match_s.median=1.792, put_over_first_match_s.max=2.734, stale_reads_total=53
  - handling: handled via `test/e2e/tests/test_table.py:1139-1180; pkg/resource/table/hooks_resource_policy.go:30-45`
  - related: [DDB-TABLE-244](#ddb-table-244), [DDB-TABLE-246](#ddb-table-246), [DDB-TABLE-248](#ddb-table-248), [DDB-TABLE-346](#ddb-table-346), [DDB-TABLE-233](#ddb-table-233), [DDB-TABLE-208](#ddb-table-208),
    [DDB-TABLE-351](#ddb-table-351) · hypotheses: H-S-107, H-S-007 · evidence: table/consistency-windows/resource-policy-windows
  - notes: Stale old-document window after replacement observed; RevisionId (not the document) is the
    convergence signal.

- <a id="ddb-table-246"></a>**DDB-TABLE-246** `stale-response` · impact high · handled · verified 2026-10-09, re-verified
  **After Delete, Get returns the deleted policy for ~1.472s (max 2.316s) in 6/6 trials; Put(ExpectedRevisionId=that rev) -> PolicyNotFound**
  DeleteResourcePolicy (200; response RevisionId equals the removed policy's: [True, True, True, True, True,
  True]) then Get every 200ms: time to the first PolicyNotFoundException = {'n': 6, 'min': 0.842, 'median':
  1.472, 'max': 2.316}; trials with stale 200s carrying the deleted RevisionId = 6 (43 reads); flapping after
  the first PNF = 0. Immediately after Delete: Get -> 200 OK | 200 OK | 200 OK;
  PutResourcePolicy(ExpectedRevisionId=deleted revision) -> PolicyNotFoundException (HTTP 400) 'Resource-based
  policy not found for the provided ResourceArn: Requested update for policy with revision id 1791506242313,
  but the policy ass' | PolicyNotFoundException (HTTP 400) 'Resource-based policy not found for the provided
  ResourceArn: Requested update for policy with revision id 1791506274554, but the policy ass' |
  PolicyNotFoundException (HTTP 400) 'Resource-based policy not found for the provided ResourceArn: Requested
  update for policy with revision id 1791506306710, but the policy ass'; PutResourcePolicy without
  ExpectedRevisionId -> ResourceInUseException (HTTP 400) 'Attempt to change a resource which is still in use:
  Table is pending previous resource-based policy update: ackq-a80758-rpw' | ResourceInUseException (HTTP 400)
  'Attempt to change a resource which is still in use: Table is pending previous resource-based policy update:
  ackq-a80758-rpw' | ResourceInUseException (HTTP 400) 'Attempt to change a resource which is still in use:
  Table is pending previous resource-based policy update: ackq-a80758-rpw'; 16s later
  Put(ExpectedRevisionId=the revision the stale Get returned) -> PolicyNotFoundException (HTTP 400)
  'Resource-based policy not found for the provided ResourceArn: Requested update for policy with revision id
  1791506242313, but table ackq-a80' | PolicyNotFoundException (HTTP 400) 'Resource-based policy not found for
  the provided ResourceArn: Requested update for policy with revision id 1791506274554, but table ackq-a80' |
  PolicyNotFoundException (HTTP 400) 'Resource-based policy not found for the provided ResourceArn: Requested
  update for policy with revision id 1791506306710, but table ackq-a80'.
  - ACK: requeue, custom_delete, terminal_codes · ops: DeleteResourcePolicy, GetResourcePolicy,
    PutResourcePolicy · fields: ResourcePolicy, ExpectedRevisionId
  - repro: Put(A); wait 16s; Delete; Get every 200ms until PolicyNotFoundException; separately Delete then
    Put(B, ExpectedRevisionId=A) and Put(B)
  - measurements: delete_first_pnf_s.n=6, delete_first_pnf_s.min=0.842, delete_first_pnf_s.median=1.472,
    delete_first_pnf_s.max=2.316, stale_reads_total=43
  - handling: handled via `pkg/resource/table/hooks_resource_policy.go:97-103; pkg/resource/table/hooks_resource_policy.go:129-132; test/e2e/tests/test_table.py:1139-1180; pkg/resource/table/hooks_resource_policy.go:30-45`
  - related: [DDB-TABLE-244](#ddb-table-244), [DDB-TABLE-245](#ddb-table-245), [DDB-TABLE-248](#ddb-table-248), [DDB-TABLE-346](#ddb-table-346), [DDB-TABLE-233](#ddb-table-233), [DDB-TABLE-208](#ddb-table-208),
    [DDB-TABLE-351](#ddb-table-351), [DDB-TABLE-205](#ddb-table-205), [DDB-TABLE-209](#ddb-table-209), [DDB-TABLE-347](#ddb-table-347), [DDB-TABLE-214](#ddb-table-214) · hypotheses: H-S-108, H-S-007,
    H-S-026 · evidence: table/consistency-windows/resource-policy-windows, table/creative/reverify-set-a1
  - notes: H-S-108: stale reads after Delete confirmed; Put with the stale ExpectedRevisionId inside the
    cooldown -> PolicyNotFoundException (the 15s cooldown fires before the revision check is observable);
    after the cooldown -> PolicyNotFoundException.

- <a id="ddb-table-248"></a>**DDB-TABLE-248** `eventual-consistency` · impact high · handled · verified 2026-10-09
  **Read-your-writes: 0/30 Put->immediate Get returned the new RevisionId (5 PolicyNotFound, 25 stale 200)**
  30 trials (round-robin over 5 ACTIVE tables, each table written at most once per 16s) of
  PutResourcePolicy(distinct document) immediately followed by one GetResourcePolicy: matches=0, Get errors=5
  (['PolicyNotFoundException']), stale 200 with the previous RevisionId=25, Put failures=[]. Put latency ms
  {'n': 30, 'min': 71, 'median': 78.0, 'max': 158}, Get latency ms {'n': 30, 'min': 6, 'median': 7.0, 'max':
  15}.
  - ACK: requeue, late_initialize · ops: PutResourcePolicy, GetResourcePolicy
  - repro: loop over 5 tables: Put(distinct policy); Get; compare RevisionId (no delay)
  - measurements: ryw_trials=30, ryw_matches=0, ryw_pnf=5, ryw_stale=25
  - handling: handled via `test/e2e/tests/test_table.py:1139-1180; pkg/resource/table/hooks_resource_policy.go:30-45`
  - related: [DDB-TABLE-244](#ddb-table-244), [DDB-TABLE-245](#ddb-table-245), [DDB-TABLE-246](#ddb-table-246), [DDB-TABLE-346](#ddb-table-346), [DDB-TABLE-233](#ddb-table-233), [DDB-TABLE-208](#ddb-table-208),
    [DDB-TABLE-351](#ddb-table-351) · hypotheses: H-S-044, H-S-007 · evidence: table/consistency-windows/resource-policy-windows
  - notes: H-S-044 (read-your-writes in practice) REFUTED: 30/30 immediate reads did not reflect the write.

- <a id="ddb-table-304"></a>**DDB-TABLE-304** `stale-response` · impact medium · unhandled (not handled in controller) · verified 2026-10-09
  **Describe lags Disable ~100ms (still ACTIVE after Disable returned DISABLING); Enable 10ms after Disable
  rejected, ~600ms after accepted** - Disable(K1) -> 200 OK DestinationStatus=DISABLING; Enable(K1) 11ms later
  -> ValidationException (HTTP 400) 'Table is not in a valid state to enable Kinesis Streaming Destination:
  EnableKinesisStreamingDestination must... - see: [details/DDB-TABLE-304.md](details/DDB-TABLE-304.md)

- <a id="ddb-table-324"></a>**DDB-TABLE-324** `eventual-consistency` · impact high · unhandled (not handled in controller) · verified 2026-10-09
  **Policy enforcement lags the API by ~221-237s for every change after the first (GetResourcePolicy converges in ~2s; first Put: 2s)**
  Throwaway table, policy = Deny * dynamodb:DescribeTable; DescribeTable + GetResourcePolicy polled every 1s.
  Sequence of changes -> (seconds until DescribeTable reflected the change, seconds until GetResourcePolicy
  showed the new RevisionId/PolicyNotFound): [('deny', 2.05, 2.05), ('delete', 228.08, 2.04), ('deny', 236.53,
  1.03), ('replace-with-allow', 236.59, 2.04), ('deny', 221.17, 2.04), ('delete', 237.23, 1.02), ('deny',
  236.53, 3.07), ('replace-with-allow', 236.66, 3.06)]. The first Deny ever applied to the table was enforced
  after 2.05s; every later change (Delete of the Deny, a new Deny, replacing the Deny with an Allow-only
  document) took 221-237s to be enforced although GetResourcePolicy reflected each change within ~2s. Deny
  message: 'User: arn:aws:sts::<ACCOUNT>:assumed-role/<PRINCIPAL> is not authorized to perform:
  dynamodb:DescribeTable on resource: arn:aws:dynamodb:us-west-'.
  - ACK: e2e-timing, requeue, docs-only · ops: PutResourcePolicy, DeleteResourcePolicy, GetResourcePolicy ·
    fields: ResourcePolicy
  - repro: Put Deny-* on dynamodb:DescribeTable; poll DescribeTable + GetResourcePolicy at 1s; Delete; poll;
    Put Deny; Put Allow-only; poll; repeat
  - measurements: first_deny_enforced_s=2.05, later_changes_enforced_s.n=7,
    later_changes_enforced_s.min=221.17, later_changes_enforced_s.median=236.53,
    later_changes_enforced_s.max=237.23, get_converged_s.n=4, get_converged_s.min=1.03,
    get_converged_s.median=2.04, get_converged_s.max=3.07
  - handling: not handled in the controller (as of commit 34b85e6)
  - related: [DDB-TABLE-210](#ddb-table-210), [DDB-TABLE-270](#ddb-table-270), [DDB-TABLE-431](table-streams-encryption-class.md#ddb-table-431), [DDB-TABLE-432](#ddb-table-432) · hypotheses: H-S-007, H-S-029 ·
    evidence: table/creative/policy-enforcement-lag
  - notes: Consistent with a ~4-minute authorization cache per table that is primed on first evaluation: once
    primed, neither tightening nor loosening the policy takes effect for ~220-240s, so 'RevisionId converged'
    (2s) must not be read as 'policy in effect'. Explains the apparent non-enforcement of a...
  - full notes: [details/DDB-TABLE-324.md](details/DDB-TABLE-324.md)

- <a id="ddb-table-346"></a>**DDB-TABLE-346** `eventual-consistency` · impact high · unhandled (not handled in controller) · verified 2026-10-09
  **GetResourcePolicy at 100 ms after a write flips exactly once, never back: PNF->NEW (fresh), OLD->NEW (replace), OLD->PNF (delete); 6/6**
  Get every 100 ms for 5 s after each write (6 trials per phase, 3 tables, 46-47 polls/trial, Get latency 8.5
  ms, no throttling at 10 Get/s). Fresh Put: pattern ['PNF->NEW'] in all trials; first Get at ~0.1 s was
  already PolicyNotFoundException; the switch to the new RevisionId happened once at 1.79 s (min 1.385, max
  2.278) with no intermediate 200 of another revision. Replace: ['OLD->NEW'] in all trials - the OLD
  document/RevisionId is served with HTTP 200 until 1.577 s (max 2.295), then the new one;
  PolicyNotFoundException never appeared in between. Delete: ['OLD->PNF'] in all trials; the deleted document
  is served until 1.368 s (max 2.025), then PolicyNotFoundException for the rest of the 5 s. No trial flapped
  back (non-monotonic sequences: 0/0/0), so the first observation of the new state is final.
  DeleteResourcePolicy echoed the removed RevisionId in 6 trials.
  - ACK: requeue, late_initialize, compare.is_ignored+delta_pre_compare · ops: PutResourcePolicy,
    GetResourcePolicy, DeleteResourcePolicy · fields: ResourcePolicy, RevisionId
  - repro: Put; Get every 100 ms for 5 s; 16 s later Put(other); same; 16 s later Delete; same; x6
  - measurements: fresh_first_new_s.n=6, fresh_first_new_s.min=1.385, fresh_first_new_s.median=1.79,
    fresh_first_new_s.max=2.278, replace_last_old_s.n=6, replace_last_old_s.min=1.184,
    replace_last_old_s.median=1.577, replace_last_old_s.max=2.295, delete_last_old_s.n=6,
    delete_last_old_s.min=1.148, delete_last_old_s.median=1.368, delete_last_old_s.max=2.025,
    get_latency_ms.n=6, get_latency_ms.min=8.0, get_latency_ms.median=8.5, get_latency_ms.max=9.0
  - handling: not handled in the controller (as of commit 34b85e6)
  - related: [DDB-TABLE-206](#ddb-table-206), [DDB-TABLE-209](#ddb-table-209), [DDB-TABLE-244](#ddb-table-244), [DDB-TABLE-245](#ddb-table-245), [DDB-TABLE-246](#ddb-table-246), [DDB-TABLE-248](#ddb-table-248),
    [DDB-TABLE-205](#ddb-table-205), [DDB-TABLE-233](#ddb-table-233), [DDB-TABLE-208](#ddb-table-208), [DDB-TABLE-351](#ddb-table-351) · hypotheses: H-S-007, H-S-107, H-S-108,
    H-S-044 · evidence: table/consistency-windows/policy-stale-read-sequence
  - notes: Confirms H-S-007/H-S-107/H-S-108 and refutes H-S-044 at 100 ms resolution; the previously reported
    200 ms windows ([DDB-TABLE-244](#ddb-table-244)..246) are reproduced with the exact sequence: a single monotonic switch
    ~1.3-2.4 s after the write. A controller can therefore poll until the RevisionId returned by Put is...
  - full notes: [details/DDB-TABLE-346.md](details/DDB-TABLE-346.md)

## Sub-resources

- <a id="ddb-table-195"></a>**DDB-TABLE-195** `sub-resource-api` · impact high · unhandled (not handled in controller) · verified 2026-10-09, re-verified
  **AAS target without a policy reads as {AutoScalingDisabled:true, ScalingPolicies:[]} in Describe yet AAS enforces its MinCapacity**
  RegisterScalableTarget(read, Min 2 Max 12, no policy, no RoleARN) -> Describe 0.45s later: read settings
  {"AutoScalingDisabled": true, "ScalingPolicies": []} (2 s later {"AutoScalingDisabled": true,
  "ScalingPolicies": []}; region B {"AutoScalingDisabled": true, "ScalingPolicies": []}).
  DeregisterScalableTarget -> Describe: {"AutoScalingDisabled": true, "ScalingPolicies": []}. AAS target
  roles: [{"dim": "ReadCapacityUnits", "min": 2, "max": 12, "role":
  "AWSServiceRoleForApplicationAutoScaling_DynamoDBTable"}, {"dim": "WriteCapacityUnits", "min": 1, "max": 10,
  "role": "AWSServiceRoleForApplicationAutoScaling_DynamoDBTable"}].
  - ACK: scope:skip, docs-only · ops: DescribeTableReplicaAutoScaling, RegisterScalableTarget · fields:
    ScalingPolicies, MinimumUnits, MaximumUnits, AutoScalingDisabled
  - repro: application-autoscaling register-scalable-target (no policy) -> DescribeTableReplicaAutoScaling ->
    deregister -> Describe
  - handling: not handled in the controller (as of commit 34b85e6)
  - related: [DDB-TABLE-189](#ddb-table-189), [DDB-TABLE-243](#ddb-table-243), [DDB-TABLE-232](table-replicas.md#ddb-table-232), [DDB-TABLE-317](#ddb-table-317), [DDB-TABLE-289](table-replicas.md#ddb-table-289), [DDB-TABLE-237](#ddb-table-237),
    [DDB-TABLE-314](#ddb-table-314), [DDB-TABLE-315](#ddb-table-315), [DDB-TABLE-256](table-replicas.md#ddb-table-256), [DDB-TABLE-321](table-replicas.md#ddb-table-321), [DDB-TABLEREPLICAAUTOSCALING-001](table-replicas.md#ddb-tablereplicaautoscaling-001),
    [DDB-TABLE-193](#ddb-table-193), [DDB-TABLE-197](#ddb-table-197), [DDB-TABLE-196](#ddb-table-196), [DDB-TABLE-292](table-replicas.md#ddb-table-292) · hypotheses: H-R-104, H-R-110, H-R-131 ·
    evidence: table/sub-resources/replica-autoscaling-facade, table/creative/reverify-set-b
  - notes: REFUTES the H-R-104 / H-R-110 expectation of an 'enabled but inert' (AutoScalingDisabled=false,
    ScalingPolicies=[]) state: a target without a policy is indistinguishable from 'no autoscaling' in
    DescribeTableReplicaAutoScaling (its Min/Max are hidden), yet AAS still enforced MinCapacity: scaling...
  - full notes: [details/DDB-TABLE-195.md](details/DDB-TABLE-195.md)

## Delete semantics

- <a id="ddb-table-122"></a>**DDB-TABLE-122** `delete-semantics` · impact medium · unhandled (not handled in controller) · verified 2026-10-08
  **During DELETING, CreateBackup and an identical policy re-Put are 200 for ~1.6 s; a changed policy and
  every UpdateTable -> ResourceInUse** - Ops issued while DescribeTable reported TableStatus=DELETING:
  {'stream_toggle': 'ResourceInUseException', 'tableclass_ia': 'ResourceInUseException', 'sse_toggle':
  'ResourceInUseException', 'create_backup': 'OK', 'put_... - see:
  [details/DDB-TABLE-122.md](details/DDB-TABLE-122.md)

- <a id="ddb-table-198"></a>**DDB-TABLE-198** `delete-semantics` · impact high · unhandled (not handled in controller) · verified 2026-10-09
  **Write AutoScalingDisabled=true on a PROVISIONED global table -> 200: AAS write target+alarms removed in BOTH regions, read untouched**
  UpdateTableReplicaAutoScaling(write {AutoScalingDisabled:true}) on the 2-replica PROVISIONED table -> 200
  (message 'None'); response write a/b {"MinimumUnits": 1, "MaximumUnits": 10, "AutoScalingRoleArn":
  "arn:aws:iam::<ACCOUNT>:role/<PRINCIPAL>", "ScalingPolicies": [{"PolicyName":
  "ackq-0ad4a5-custom-write-policy", "TargetTrackingScalingPolicyConfiguration": {"DisableScaleIn": true,
  "ScaleInCooldown": 120, "ScaleOutCooldown": 60, "TargetValue": 55.0}}]} / {"MinimumUnits": 1,
  "MaximumUnits": 10, "AutoScalingRoleArn": "arn:aws:iam::<ACCOUNT>:role/<PRINCIPAL>", "ScalingPolicies":
  [{"PolicyName": "ackq-0ad4a5-custom-write-policy", "TargetTrackingScalingPolicyConfiguration":
  {"DisableScaleIn": true, "ScaleInCooldown": 120, "ScaleOutCooldown": 60, "TargetValue": 55.0}}]}; Describe 1
  s later a/b {"AutoScalingDisabled": true, "ScalingPolicies": []} / {"AutoScalingDisabled": true,
  "ScalingPolicies": []}; AAS targets A [{"dim": "ReadCapacityUnits", "min": 1, "max": 10, "role":
  "AWSServiceRoleForApplicationAutoScaling_DynamoDBTable"}], B []; alarms A
  ['TargetTracking-table/ackq-0ad4a5-as-prov-AlarmHigh-bfd36409-08b7-4c20-947c-a6992f50af07',
  'TargetTracking-table/ackq-0ad4a5-as-prov-AlarmLow-e5d3d0f5-fe10-4dfc-9bd0-4d4bb0687b6c',
  'TargetTracking-table/ackq-0ad4a5-as-prov-ProvisionedCapacityHigh-dd056ac9-82ff-45c9-9664-a03efb91ee9d',
  'TargetTracking-table/ackq-0ad4a5-as-prov-ProvisionedCapacityLow-6cec6cdf-7f51-499b-b883-d2057302ab5e'], B
  []. Read disable via ReplicaUpdates -> 200; targets A then []. Region-B targets after the replica was
  removed: [].
  - ACK: custom_update, docs-only · ops: UpdateTableReplicaAutoScaling · fields: AutoScalingDisabled
  - repro: autoscaled 2-replica PROVISIONED table -> UpdateTableReplicaAutoScaling write
    AutoScalingDisabled=true -> describe-scalable-targets both regions
  - handling: not handled in the controller (as of commit 34b85e6)
  - related: [DDB-TABLE-238](table-replicas.md#ddb-table-238), [DDB-TABLE-192](table-replicas.md#ddb-table-192), [DDB-TABLE-242](#ddb-table-242), [DDB-TABLE-314](#ddb-table-314), [DDB-TABLE-290](#ddb-table-290), [DDB-TABLE-292](table-replicas.md#ddb-table-292),
    [DDB-TABLE-323](#ddb-table-323), [DDB-TABLE-243](#ddb-table-243) · hypotheses: H-R-110, H-S-042 · evidence:
    table/sub-resources/replica-autoscaling-facade
  - notes: H-R-110 confirmed for the AAS/CloudWatch side: disable == DeregisterScalableTarget for that
    dimension in every replica region (alarms gone). The response still echoed the old write settings (stale);
    Describe 1 s later showed {AutoScalingDisabled:true, ScalingPolicies:[]}. Note the table stays a...
  - full notes: [details/DDB-TABLE-198.md](details/DDB-TABLE-198.md)

- <a id="ddb-table-214"></a>**DDB-TABLE-214** `delete-semantics` · impact medium · handled · verified 2026-10-09
  **Policy APIs on a deleted table's ARN: Get/Put/Delete -> ResourceNotFoundException (Delete is not
  idempotent across table deletion)** - 3s after DescribeTable first returned ResourceNotFoundException:
  GetResourcePolicy(table ARN) -> ResourceNotFoundException (HTTP 400) 'Requested resource not found: Table:
  ackq-71f899-rp not found'; PutResourcePolicy... - see: [details/DDB-TABLE-214.md](details/DDB-TABLE-214.md)

- <a id="ddb-table-235"></a>**DDB-TABLE-235** `delete-semantics` · impact high · unhandled (not handled in controller) · verified 2026-10-09
  **DeleteTable is accepted with an ACTIVE or ENABLING Kinesis destination (no disable needed); the Kinesis stream is left intact**
  Enable -> 200 OK (DestinationStatus ENABLING, EnableKinesisStreamingConfiguration echoed as {}); ENABLING
  lasted 7.1s; TableStatus stayed ACTIVE. DeleteTable with the destination ACTIVE -> 200 OK (TableStatus
  DELETING); the Kinesis stream afterwards: ACTIVE. Table C: Enable (-> ENABLING) then DeleteTable immediately
  -> 200 OK; right after, destination list [['ks2', 'ENABLING', None]] and TableStatus DELETING; deletion
  timeline (table, kinesis): [(['DELETING', 'ks2=ENABLING'], 6.12), (['ERR:ResourceNotFoundException',
  'ERR:ResourceNotFoundException'], None)]. Table B (destination ks1=ACTIVE) DeleteTable -> 200 OK. During A's
  DELETING the destination list read: [('ks1=ACTIVE', 0.0, 0.0), ('ks1=UPDATING', 1.29, 4.4)] (the ACTIVE
  entry flipped to UPDATING, not DISABLING).
  - ACK: deletable.when, pre-delete-cleanup, e2e-timing · ops: DeleteTable, EnableKinesisStreamingDestination,
    DescribeKinesisStreamingDestination
  - repro: Enable Kinesis destination; wait ACTIVE; DeleteTable. Separately: Enable; DeleteTable immediately
    while ENABLING
  - measurements: enabling_duration_s=7.1, c_deleting_duration_s=6.12
  - handling: not handled in the controller (as of commit 34b85e6)
  - related: [DDB-TABLE-122](#ddb-table-122), [DDB-TABLE-236](#ddb-table-236), [DDB-TABLE-374](service.md#ddb-table-374), [DDB-TABLE-097](table-subresources.md#ddb-table-097), [DDB-TABLE-453](table-subresources.md#ddb-table-453), [DDB-TABLE-088](table-subresources.md#ddb-table-088),
    [DDB-TABLE-342](table-subresources.md#ddb-table-342), [DDB-TABLE-214](#ddb-table-214), [DDB-TABLE-299](#ddb-table-299), [DDB-TABLE-300](#ddb-table-300), [DDB-TABLE-301](#ddb-table-301), [DDB-TABLE-302](#ddb-table-302), [DDB-TABLE-304](#ddb-table-304),
    [DDB-TABLE-269](#ddb-table-269), [DDB-TABLE-268](#ddb-table-268), [DDB-TABLE-319](#ddb-table-319), [DDB-TABLE-303](#ddb-table-303), [DDB-TABLE-234](#ddb-table-234), [DDB-TABLE-460](#ddb-table-460) · hypotheses:
    H-S-116, H-S-033 · evidence: table/state-machine/policy-kinesis-admissibility
  - notes: H-S-116: ACTIVE clause confirmed; ENABLING clause (ResourceInUseException) REFUTED - DeleteTable is
    accepted while the destination is ENABLING. H-S-033: ENABLING lasted ~2-7s here, far below the
    hypothesized 30-120s; TableStatus never left ACTIVE.

- <a id="ddb-table-242"></a>**DDB-TABLE-242** `delete-semantics` · impact high · unhandled (not handled in controller) · verified 2026-10-09
  **AutoScalingDisabled=true deregisters the AAS target (policies+alarms gone), sibling dimension untouched; true+Min/Max -> ValidationExcept...**
  Before: read target {"MinCapacity": 2, "MaxCapacity": 20, "RoleARN":
  "arn:aws:iam::<ACCOUNT>:role/<PRINCIPAL>", "CreationTime": "2026-10-09T00:39:31.049000+00:00",
  "SuspendedState": {"DynamicScalingInSuspended": false, "DynamicScalingOutSuspended": false,
  "ScheduledScalingSuspended": false}}, alarms ['AlarmHigh-a5d5e80a-f220-4634-ab9c-6ac7c6fd1fe1',
  'AlarmHigh-a84f162d-6a3a-4ac1-8590-bf6a41c5447c', 'AlarmLow-0f864fcd-f316-49a7-99e7-9e0fc21ebf92',
  'AlarmLow-626b23aa-7767-458d-8602-1f0caf1dd909',
  'ProvisionedCapacityHigh-529e6f7c-f11f-4e5a-92e8-3ffd772de89e',
  'ProvisionedCapacityHigh-576ad820-8772-4711-bfd2-e32f27f244da',
  'ProvisionedCapacityLow-2fd9a0a0-fa17-4cc8-b741-85a30bdcd0a7',
  'ProvisionedCapacityLow-67bcf72a-08df-49e8-abb8-a2f5feabd605']. After read {AutoScalingDisabled:true} (200,
  response settings {"MinimumUnits": 2, "MaximumUnits": 20, "AutoScalingRoleArn":
  "arn:aws:iam::<ACCOUNT>:role/<PRINCIPAL>", "ScalingPolicies": [{"PolicyName": "ackq-41e87e-p2",
  "TargetTrackingScalingPolicyConfiguration": {"TargetValue": 40.0}}]}, Describe {"AutoScalingDisabled": true,
  "ScalingPolicies": []}): read target null, read policies [], write target {"MinCapacity": 1, "MaxCapacity":
  10, "RoleARN": "arn:aws:iam::<ACCOUNT>:role/<PRINCIPAL>", "CreationTime":
  "2026-10-09T00:39:06.027000+00:00", "SuspendedState": {"DynamicScalingInSuspended": false,
  "DynamicScalingOutSuspended": false, "ScheduledScalingSuspended": false}}, write policies
  ['ackq-41e87e-w0'], alarms ['AlarmHigh-a84f162d-6a3a-4ac1-8590-bf6a41c5447c',
  'AlarmLow-0f864fcd-f316-49a7-99e7-9e0fc21ebf92',
  'ProvisionedCapacityHigh-529e6f7c-f11f-4e5a-92e8-3ffd772de89e',
  'ProvisionedCapacityLow-2fd9a0a0-fa17-4cc8-b741-85a30bdcd0a7']. Disable again -> 200. {true, Min, Max} ->
  {"operation": "update_table_replica_auto_scaling", "ok": false, "code": "ValidationException",
  "http_status": 400, "message": "Failed to update settings for global table with name ‘ackq-41e87e-as-rt’:
  Parameters 'MaximumUnits', 'MinimumUnits' must be left blank when disabling auto scaling.", "latency_ms":
  23, "client_side": false}; {true, ScalingPolicyUpdate} -> {"operation": "update_table_replica_auto_scaling",
  "ok": false, "code": "ValidationException", "http_status": 400, "message": "Failed to update settings for
  global table with name ‘ackq-41e87e-as-rt’: Parameters 'ScalingPolicyUpdate' must be left blank when
  disabling auto scaling.", "latency_ms": 30, "client_side": false} (write target after: {"MinCapacity": 1,
  "MaxCapacity": 10, "RoleARN": "arn:aws:iam::<ACCOUNT>:role/<PRINCIPAL>", "CreationTime":
  "2026-10-09T00:39:06.027000+00:00", "SuspendedState": {"DynamicScalingInSuspended": false,
  "DynamicScalingOutSuspended": false, "ScheduledScalingSuspended": false}}). On the disabled read dimension:
  {false} -> {"operation": "update_table_replica_auto_scaling", "ok": false, "code": "ValidationException",
  "http_status": 400, "message": "Failed to update settings for global table with name: ‘ackq-41e87e-as-rt’:
  Parameters 'MaximumUnits', 'ScalingPolicyUpdate', 'MinimumUnits' are required unless auto scaling is being
  disabled.", "latency_ms": 29, "client_side": false}; {false, Min, Max} -> {"operation":
  "update_table_replica_auto_scaling", "ok": false, "code": "ValidationException", "http_status": 400,
  "message": "Failed to update settings for global table with name: ‘ackq-41e87e-as-rt’: Parameters
  'ScalingPolicyUpdate' are required unless auto scaling is being disabled.", "latency_ms": 23, "client_side":
  false}; {false, Min, Max, policy} -> 200; policy names afte [truncated in evidence]
  - ACK: custom_update, pre-delete-cleanup · ops: UpdateTableReplicaAutoScaling · fields: AutoScalingDisabled,
    MinimumUnits, MaximumUnits, ScalingPolicyUpdate
  - repro: autoscaled read+write -> read {AutoScalingDisabled:true} -> describe-scalable-targets/-policies,
    describe-alarms -> combined/false shapes
  - handling: not handled in the controller (as of commit 34b85e6)
  - related: [DDB-TABLE-238](table-replicas.md#ddb-table-238), [DDB-TABLE-192](table-replicas.md#ddb-table-192), [DDB-TABLE-198](#ddb-table-198), [DDB-TABLE-314](#ddb-table-314), [DDB-TABLE-290](#ddb-table-290), [DDB-TABLE-292](table-replicas.md#ddb-table-292),
    [DDB-TABLE-323](#ddb-table-323), [DDB-TABLE-243](#ddb-table-243), [DDB-TABLE-241](#ddb-table-241), [DDB-TABLE-253](table-replicas.md#ddb-table-253), [DDB-TABLE-255](table-replicas.md#ddb-table-255) · hypotheses: H-R-110, H-S-042,
    H-R-120 · evidence: table/round-trip/autoscaling-settings
  - notes: H-R-110 confirmed: disable == DeregisterScalableTarget (policies and the 4 CloudWatch alarms of
    that dimension disappear; the sibling dimension keeps its target). {AutoScalingDisabled:true}+Min/Max ->
    ValidationException 'Parameters 'MaximumUnits', 'MinimumUnits' must be left blank when disabling...
  - full notes: [details/DDB-TABLE-242.md](details/DDB-TABLE-242.md)

- <a id="ddb-table-301"></a>**DDB-TABLE-301** `delete-semantics` · impact high · unhandled (not handled in controller) · verified 2026-10-09
  **Disable: ~2s DISABLING, then a DISABLED entry lingers (>=171s); re-Enable flips it (no duplicate); Disable w/ config -> ValidationException**
  Disable(K1, EnableKinesisStreamingConfiguration={MICROSECOND}) -> ValidationException (HTTP 400) 'Stream
  cannot be disabled using EnableKinesisStreamingConfiguration parameters'. Disable(K1) -> 200 OK
  DestinationStatus=DISABLING. A second Disable issued during DISABLING -> 200 OK (it blocked 1460 ms and
  returned DISABLING); Update during DISABLING -> ValidationException (HTTP 400) 'Table is not in a valid
  state to enable Kinesis Streaming Destination: Kinesis streaming is not in ACTIVE state. Updates are only
  allowed in ACTIVE st'. The first Describe after the second Disable already showed DISABLED (DISABLING lasts
  ~1-2s). The DISABLED entry stayed listed at every 10s sample for 171s: [(0, 'ks1=DISABLED'), (30,
  'ks1=DISABLED'), (60, 'ks1=DISABLED'), (90, 'ks1=DISABLED'), (121, 'ks1=DISABLED'), (151, 'ks1=DISABLED')].
  On the DISABLED entry: Update -> ValidationException (HTTP 400) 'Table is not in a valid state to enable
  Kinesis Streaming Destination: Kinesis streaming is not in ACTIVE state. Updates are only allowed in ACTIVE
  st'; Disable -> ValidationException (HTTP 400) 'Table is not in a valid state to enable Kinesis Streaming
  Destination: KinesisStreamingDestination must be ACTIVE to perform DISABLE operation.'. Re-Enable(K1) -> 200
  OK ENABLING; Describe 0.049s later: [('ks1', 'ENABLING')]; ENABLING lasted 3.03s; entries after: [('ks1',
  'ACTIVE', None)] (count for K1 = 1, i.e. the DISABLED entry flipped rather than a second entry being added).
  - ACK: custom_delete, compare.is_ignored+delta_pre_compare, requeue, list_operation.match_fields,
    custom_field · ops: DisableKinesisStreamingDestination, DescribeKinesisStreamingDestination,
    EnableKinesisStreamingDestination, UpdateKinesisStreamingDestination · fields:
    KinesisDataStreamDestinations[].DestinationStatus, EnableKinesisStreamingConfiguration
  - repro: Enable; wait ACTIVE; Disable with EnableKinesisStreamingConfiguration; Disable; Disable again
    immediately; poll 1/s; keep describing for 3 min; Update/Disable the DISABLED entry; Enable again
  - measurements: disabled_entry_still_listed_after_s=171, re_enabling_duration_s=3.03,
    second_disable_latency_ms=1460
  - handling: not handled in the controller (as of commit 34b85e6)
  - related: [DDB-TABLE-299](#ddb-table-299), [DDB-TABLE-300](#ddb-table-300), [DDB-TABLE-302](#ddb-table-302), [DDB-TABLE-304](#ddb-table-304), [DDB-TABLE-269](#ddb-table-269), [DDB-TABLE-268](#ddb-table-268),
    [DDB-TABLE-319](#ddb-table-319), [DDB-TABLE-303](#ddb-table-303), [DDB-TABLE-235](#ddb-table-235), [DDB-TABLE-236](#ddb-table-236), [DDB-TABLE-234](#ddb-table-234), [DDB-TABLE-460](#ddb-table-460) · hypotheses:
    H-S-008, H-S-038, H-S-036, H-S-115 · evidence: table/sub-resources/kinesis-destination
  - notes: H-S-008 DISABLED-lingers clause confirmed (>= 3 min; a controller must filter
    DISABLED/ENABLE_FAILED entries out of the desired-state diff). H-S-038 'Disable accepts and ignores
    EnableKinesisStreamingConfiguration' REFUTED (ValidationException 'Stream cannot be disabled using...
  - full notes: [details/DDB-TABLE-301.md](details/DDB-TABLE-301.md)

## Quotas and rate limits

- <a id="ddb-table-206"></a>**DDB-TABLE-206** `quota-limit` · impact high · unhandled (not handled in controller) · verified 2026-10-09
  **Policy writes serialized per resource: ResourceInUseException while applying (~2s), then ThrottlingException until 15s after the change**
  After a Put changed the policy (RevisionId R = epoch ms of the change; R - caller wall clock at the call =
  69 ms), a Put of a DIFFERENT document 2.4s later -> ThrottlingException (HTTP 400) 'Resource-based policy
  for table ackq-71f899-rp modified within the previous 15000 milliseconds. Please try again after
  2026-10-09T00:29:34.602Z.'; a Delete 2.4s later -> ThrottlingException (HTTP 400) 'Resource-based policy for
  table ackq-71f899-rp modified within the previous 15000 milliseconds. Please try again after
  2026-10-09T00:29:34.602Z.'; the message's 'try again after' timestamp is exactly R+15000 ms. Retrying every
  0.5s, the first success came 15.30s after R (attempt at 14.75s still failed). Equivalent-document re-Puts
  inside the window succeed as no-ops (same RevisionId). With ExpectedRevisionId=current inside the window the
  code is instead ResourceInUseException ('Attempt to change a resource which is still in use: Table is
  pending previous resource-based policy update: ac'); with a stale ExpectedRevisionId ->
  PolicyNotFoundException (revision check first). The window also applies after a Delete (Put 0.07s after
  Delete -> ThrottlingException (HTTP 400) 'Resource-based policy for table ackq-71f899-rp modified within the
  previous 15000 milliseconds. Please try again after 2026-10-09T00:30:37.353Z.') and independently per slot:
  the stream ARN's slot gave ThrottlingException 'Resource-based policy for stream <label> modified within the
  previous 15000 milliseconds' with first success after 15.56s, while a table-slot write 1.6s after the stream
  write succeeded.
  - ACK: requeue, terminal_codes, one-per-reconcile, e2e-timing · ops: PutResourcePolicy, DeleteResourcePolicy
    · fields: ResourcePolicy, RevisionId, ExpectedRevisionId
  - repro: Put(p1); Put(p2) immediately -> ThrottlingException 'modified within the previous 15000
    milliseconds. Please try again after <ts>'; Put(p2, ExpectedRevisionId=rev1) -> ResourceInUseException;
    retry until success
  - measurements: cooldown_first_success_s=15.3, try_again_minus_revision_ms=15000,
    stream_slot_first_success_s=15.56
  - handling: not handled in the controller (as of commit 34b85e6)
  - related: [DDB-TABLE-247](#ddb-table-247), [DDB-TABLE-347](#ddb-table-347), [DDB-TABLE-464](service.md#ddb-table-464), [DDB-TABLE-445](service.md#ddb-table-445), [DDB-TABLE-348](#ddb-table-348), [DDB-TABLE-213](table-streams-encryption-class.md#ddb-table-213),
    [DDB-TABLE-205](#ddb-table-205) · hypotheses: H-S-027, H-S-006, H-S-112 · evidence: table/sub-resources/resource-policy
  - notes: Qualifies H-S-027. Overlapping writes are rejected in two phases. Phase 1 (~0-2s, while the
    previous write is still applying and Get still returns the old state) -> ResourceInUseException 'Attempt
    to change a resource which is still in use: Table|Stream is pending previous resource-based policy...
  - full notes: [details/DDB-TABLE-206.md](details/DDB-TABLE-206.md)

## Adoption and first-sync hazards

- <a id="ddb-table-290"></a>**DDB-TABLE-290** `first-sync-destructive` · impact high · unhandled (not handled in controller) · verified 2026-10-09
  **Autoscaling dimensions are independent: disabling table write left 3 target(s) in A (GSI untouched); alarms {"-AlarmHigh": 1, "-AlarmLow"...**
  With 4 dimensions enabled (targets [["/", "table:R", 1, 10], ["/", "table:W", 1, 10], ["/index/gsi0",
  "index:R", 1, 10], ["/index/gsi0", "index:W", 1, 12]]), UpdateTableReplicaAutoScaling table write
  {AutoScalingDisabled:true} -> 200; remaining targets A [["/", "table:R", 1, 10], ["/index/gsi0", "index:R",
  1, 10], ["/index/gsi0", "index:W", 1, 12]], B [["/index/gsi0", "index:W", 1, 12]]; alarms {"-AlarmHigh": 1,
  "-AlarmLow": 1, "-ProvisionedCapacityHigh": 1, "-ProvisionedCapacityLow": 1, "/index/gsi0-AlarmHigh": 2,
  "/index/gsi0-AlarmLow": 2, "/index/gsi0-ProvisionedCapacityHigh": 2, "/index/gsi0-ProvisionedCapacityLow":
  2}; table write settings {"AutoScalingDisabled": true, "ScalingPolicies": []}; table read Min 1; gsi0 entry
  {"IndexName": "gsi0", "IndexStatus": "ACTIVE", "ProvisionedReadCapacityAutoScalingSettings":
  {"MinimumUnits": 1, "MaximumUnits": 10, "AutoScalingRoleArn": "arn:aws:iam::<ACCOUNT>:role/<PRINCIPAL> Then
  GSI write disable -> 200; remaining A [["/", "table:R", 1, 10], ["/index/gsi0", "index:R", 1, 10]], B [].
  Re-enable table write -> 200.
  - ACK: custom_update, docs-only · ops: UpdateTableReplicaAutoScaling · fields: AutoScalingDisabled,
    GlobalSecondaryIndexUpdates
  - repro: 4 autoscaled dimensions -> disable table write -> describe-scalable-targets / describe-alarms ->
    disable GSI write
  - handling: not handled in the controller (as of commit 34b85e6)
  - related: [DDB-TABLE-249](table-replicas.md#ddb-table-249), [DDB-TABLE-288](table-replicas.md#ddb-table-288), [DDB-TABLE-291](#ddb-table-291), [DDB-TABLE-322](#ddb-table-322), [DDB-TABLE-194](table-replicas.md#ddb-table-194), [DDB-TABLE-242](#ddb-table-242),
    [DDB-TABLE-198](#ddb-table-198), [DDB-TABLE-292](table-replicas.md#ddb-table-292), [DDB-TABLE-323](#ddb-table-323), [DDB-TABLE-243](#ddb-table-243) · hypotheses: H-R-120, H-R-110 · evidence:
    table/dependencies/autoscaling-gsi-and-orphans
  - notes: H-R-120 confirmed: each disable deregisters exactly one AAS target (in every replica region for
    write dimensions) and its 4 alarms; sibling dimensions and GSIs are untouched. A 'nil = disabled'
    Table-field model would emit one destructive deregistration per dimension on first sync.

## Handling gaps (bugs to file)

None recorded for this document's findings; see [service.md 'Handling gaps summary'](service.md#handling-gaps-summary) for the service-wide list.

## E2E timing

Values are seconds unless the key says otherwise; n = trials behind the numbers ('1 run' when the finding records none).

| finding | what | measurements | n |
| --- | --- | --- | --- |
| [DDB-TABLE-206](#ddb-table-206) | Policy writes serialized per resource: ResourceInUseException while applying (~2s), then ThrottlingException until 15s after the change | cooldown_first_success_s=15.3, try_again_minus_revision_ms=15000, stream_slot_first_success_s=15.56 | 1 run |
| [DDB-TABLE-207](#ddb-table-207) | RevisionId is the epoch-ms change time; equivalent re-Puts (whitespace/key order/list-vs-string/account-id) are no-ops, same id | revision_id_len=13 | 1 run |
| [DDB-TABLE-208](#ddb-table-208) | GetResourcePolicy returns a canonicalized document (minified, 1-element Action -> string, account id -> root ARN); byte-stable if canonical | get_lag_s_after_first_put=2.04, pretty_sent_len=331, returned_len=234 | 1 run |
| [DDB-TABLE-233](#ddb-table-233) | Create-time ResourcePolicy readable ~2.09s after CreateTable while still CREATING; earlier reads -> ResourceNotFoundException | active_at_s=7.25, first_get_200_s=2.09, rnf_window_s=1.06 | 1 run |
| [DDB-TABLE-234](#ddb-table-234) | First ~1-2 s of CREATING: every policy/Kinesis API -> ResourceNotFoundException (table not found); UPDATING: Put policy / Enable Kinesis OK | updating_duration_s=96.28, kinesis_enabling_during_updating_s=2.03 | 1 run |
| [DDB-TABLE-235](#ddb-table-235) | DeleteTable is accepted with an ACTIVE or ENABLING Kinesis destination (no disable needed); the Kinesis stream is left intact | enabling_duration_s=7.1, c_deleting_duration_s=6.12 | 1 run |
| [DDB-TABLE-236](#ddb-table-236) | DELETING: policy/Kinesis reads 200; Put/Delete policy -> ResourceInUse; Enable/Disable -> Validation; gone -> ResourceNotFound, no ghosts | ghost_reads_after_gone=0, deleting_duration_s=5.43 | 1 run |
| [DDB-TABLE-244](#ddb-table-244) | After Put on a policy-less table, Get returns PolicyNotFoundException for ~1.467s (max 1.707s) in 6/6 trials, then the new RevisionId | put_fresh_first_match_s.n=6, put_fresh_first_match_s.min=1.048, put_fresh_first_match_s.median=1.467, put_fresh_first_match_s.max=1.707, trials_with_pnf_after_put=6 | 6 |
| [DDB-TABLE-245](#ddb-table-245) | Put over an existing policy: Get serves the OLD document+RevisionId (HTTP 200) for ~1.792s (max 2.734s) in 6/6 trials | put_over_first_match_s.n=6, put_over_first_match_s.min=1.471, put_over_first_match_s.median=1.792, put_over_first_match_s.max=2.734, stale_reads_total=53 | 1 run |
| [DDB-TABLE-246](#ddb-table-246) | After Delete, Get returns the deleted policy for ~1.472s (max 2.316s) in 6/6 trials; Put(ExpectedRevisionId=that rev) -> PolicyNotFound | delete_first_pnf_s.n=6, delete_first_pnf_s.min=0.842, delete_first_pnf_s.median=1.472, delete_first_pnf_s.max=2.316, stale_reads_total=43 | 1 run |
| [DDB-TABLE-248](#ddb-table-248) | Read-your-writes: 0/30 Put->immediate Get returned the new RevisionId (5 PolicyNotFound, 25 stale 200) | ryw_trials=30, ryw_matches=0, ryw_pnf=5, ryw_stale=25 | 1 run |
| [DDB-TABLE-268](#ddb-table-268) | ENABLE_FAILED entries accumulate (one per bad ARN, ~2.02s after Enable); Enable is rejected only while another entry is in flight/ACTIVE | enabling_to_enable_failed_s=2.02 | 1 run |
| [DDB-TABLE-299](#ddb-table-299) | Kinesis destination: ENABLING ~6 s then ACTIVE; at most one live (non-DISABLED/ENABLE_FAILED) entry per table; non-ACTIVE ops -> Validation | enabling_duration_s=6.14, describe_lag_after_enable_s=0.071 | 1 run |
| [DDB-TABLE-300](#ddb-table-300) | ApproximateCreationDateTimePrecision absent until set (absent == MILLISECOND server-side); Update -> UPDATING ~124-152 s; same value -> 400 | updating_duration_s=152.04 | 1 run |
| [DDB-TABLE-301](#ddb-table-301) | Disable: ~2s DISABLING, then a DISABLED entry lingers (>=171s); re-Enable flips it (no duplicate); Disable w/ config -> ValidationException | disabled_entry_still_listed_after_s=171, re_enabling_duration_s=3.03, second_disable_latency_ms=1460 | 1 run |
| [DDB-TABLE-304](#ddb-table-304) | Describe lags Disable ~100ms (still ACTIVE after Disable returned DISABLING); Enable 10ms after Disable rejected, ~600ms after accepted | stale_active_after_disable_ms=93 | 1 run |
| [DDB-TABLE-306](#ddb-table-306) | Genuine CREATING window (1-item table, ~11 min): every UpdateTable incl. cancel-Delete -> ResourceInUseException; UpdateTimeToLive blocked | creating_visible_after_s=27.0, replica_region_visible_after_s=61.0, one_item_create_total_s=686.0 | 1 run |
| [DDB-TABLE-314](#ddb-table-314) | Autoscaling Min above current capacity: AAS issues its own UpdateTable immediately (RCU 5->10 visible at 4 s, ACTIVE after 33 s) | seconds_until_capacity_changed=4.4, elapsed_until_active_at_min_s=33.0 | 1 run |
| [DDB-TABLE-315](#ddb-table-315) | Manual UpdateTable RCU below the autoscaling Min is accepted and NOT re-enforced by AAS within 12 min (no activity, no alarm) | seconds_until_reenforced=0.2, watch_elapsed_s=726.4 | 1 run |
| [DDB-TABLE-318](#ddb-table-318) | Enable never validates the stream synchronously: bad/missing/other-region/other-account/CREATING ARNs -> 200 ENABLING then ENABLE_FAILED | enable_failed_after_s_nonexistent=2.11, enable_failed_after_s_other_account=61.72, enable_failed_after_s_other_region=1.01 | 1 run |
| [DDB-TABLE-319](#ddb-table-319) | Deleted Kinesis stream: destination stays ACTIVE (293s watched, no description) and Update still succeeds; Disable+recreate+Enable works | broken_watch_s=293, stream_gone_at_s=10, update_on_dead_destination_updating_s=123.55, enabling_when_stream_deleted_mid_flight_s=17.19 | 1 run |
| [DDB-TABLE-324](#ddb-table-324) | Policy enforcement lags the API by ~221-237s for every change after the first (GetResourcePolicy converges in ~2s; first Put: 2s) | first_deny_enforced_s=2.05, later_changes_enforced_s.n=7, later_changes_enforced_s.min=221.17, later_changes_enforced_s.median=236.53, later_changes_enforced_s.max=237.23, get_converged_s.n=4, get_converged_s.min=1.03, get_converged_s.median=2.04, get_converged_s.max=3.07 | 1 run |
| [DDB-TABLE-346](#ddb-table-346) | GetResourcePolicy at 100 ms after a write flips exactly once, never back: PNF->NEW (fresh), OLD->NEW (replace), OLD->PNF (delete); 6/6 | fresh_first_new_s.n=6, fresh_first_new_s.min=1.385, fresh_first_new_s.median=1.79, fresh_first_new_s.max=2.278, replace_last_old_s.n=6, replace_last_old_s.min=1.184, replace_last_old_s.median=1.577, replace_last_old_s.max=2.295, delete_last_old_s.n=6, delete_last_old_s.min=1.148, delete_last_old_s.median=1.368, delete_last_old_s.max=2.025, get_latency_ms.n=6, get_latency_ms.min=8.0, get_latency_ms.median=8.5, get_latency_ms.max=9.0 | 1 run |
| [DDB-TABLE-347](#ddb-table-347) | ExpectedRevisionId = RevisionId just returned by Put is never PolicyNotFound: ResourceInUse (~0.6-1.5s), Throttling until 15.0s, then OK | put_expected_first_ok_s=[15.088, 15.103], delete_expected_first_ok_s=[15.039, 15.04], first_visible_s=[1.308, 1.747] | 1 run |
| [DDB-TABLE-348](#ddb-table-348) | DeleteTable within ~1-2s of a policy Put/Delete -> ResourceInUseException (has a pending resource-based policy update) while ACTIVE | delete_blocked_after_put_s=1.042, delete_blocked_after_policy_delete_s=2.04 | 1 run |
| [DDB-TABLE-351](#ddb-table-351) | Policy canonical form is a byte-stable fixpoint (minified; Version,Statement / Sid,Effect,Principal,Action,Resource,Condition); re-Put no-op | variants_accepted=20, reput_noop=19, get_lag_s_min=1.07, get_lag_s_max=2.35 | 1 run |
| [DDB-TABLE-432](#ddb-table-432) | Deny dynamodb:UpdateTable in the table policy is per-action: UpdateTable (name/ARN, no-op/real) AccessDenied; TTL/PITR/Tag/Delete succeed | deny_enforced_after_put_s=4.1 | 1 run |

## Open questions

<!-- preserved:start id=open-questions -->
<!-- open questions and follow-up experiments; survives re-renders -->
<!-- preserved:end -->

## Appendix: low-impact and duplicate findings

| id | category | impact | status | title | related | duplicate_of |
| --- | --- | --- | --- | --- | --- | --- |
| <a id="ddb-table-196"></a>**DDB-TABLE-196** | response-fidelity | low | confirmed | AAS refuses StepScaling (AccessDeniedException) and CustomizedMetricSpecification (ValidationException) on dynamodb dimensions | [DDB-TABLE-237](#ddb-table-237), [DDB-TABLE-240](#ddb-table-240), [DDB-TABLE-239](table-replicas.md#ddb-table-239), [DDB-TABLE-191](#ddb-table-191), [DDB-TABLE-189](#ddb-table-189), [DDB-TABLEREPLICAAUTOSCALING-001](table-replicas.md#ddb-tablereplicaautoscaling-001), [DDB-TABLE-193](#ddb-table-193), [DDB-TABLE-197](#ddb-table-197), [DDB-TABLE-195](#ddb-table-195), [DDB-TABLE-292](table-replicas.md#ddb-table-292) | - |
| <a id="ddb-table-197"></a>**DDB-TABLE-197** | tag-semantics | low | confirmed | Tags do not cross the autoscaling facade (table tags vs scalable-target tags) and Describe exposes no ScalableTargetARN | [DDB-TABLEREPLICAAUTOSCALING-001](table-replicas.md#ddb-tablereplicaautoscaling-001), [DDB-TABLE-193](#ddb-table-193), [DDB-TABLE-196](#ddb-table-196), [DDB-TABLE-189](#ddb-table-189), [DDB-TABLE-195](#ddb-table-195), [DDB-TABLE-292](table-replicas.md#ddb-table-292) | - |
| <a id="ddb-table-247"></a>**DDB-TABLE-247** | error-code | high | confirmed | 2nd write while the 1st applies: Put/Delete -> ResourceInUseException (~1-2s), then ThrottlingException until 15s; equivalent Put -> 200 | [DDB-TABLE-206](#ddb-table-206), [DDB-TABLE-347](#ddb-table-347), [DDB-TABLE-464](service.md#ddb-table-464), [DDB-TABLE-445](service.md#ddb-table-445), [DDB-TABLE-348](#ddb-table-348), [DDB-TABLE-213](table-streams-encryption-class.md#ddb-table-213), [DDB-TABLE-205](#ddb-table-205) | [DDB-TABLE-206](#ddb-table-206) |
| <a id="ddb-table-269"></a>**DDB-TABLE-269** | stale-response | medium | confirmed | DescribeKinesisStreamingDestination lags Disable: 31ms and 84ms after Disable returned DISABLING, Describe still listed the entry as ACTIVE | [DDB-TABLE-299](#ddb-table-299), [DDB-TABLE-300](#ddb-table-300), [DDB-TABLE-301](#ddb-table-301), [DDB-TABLE-302](#ddb-table-302), [DDB-TABLE-304](#ddb-table-304), [DDB-TABLE-268](#ddb-table-268), [DDB-TABLE-319](#ddb-table-319), [DDB-TABLE-303](#ddb-table-303), [DDB-TABLE-235](#ddb-table-235), [DDB-TABLE-236](#ddb-table-236), [DDB-TABLE-234](#ddb-table-234), [DDB-TABLE-460](#ddb-table-460) | [DDB-TABLE-304](#ddb-table-304) |
| <a id="ddb-table-391"></a>**DDB-TABLE-391** | other | low | confirmed | Doc claim C009 TRUE: DeleteResourcePolicy returns 200 at once but applies asynchronously (~1.5 s stale reads, ResourceInUse for a 2nd write) | [DDB-TABLE-246](#ddb-table-246), [DDB-TABLE-247](#ddb-table-247) | - |
| <a id="ddb-table-395"></a>**DDB-TABLE-395** | other | low | confirmed | Doc claim C021 TRUE: GetResourcePolicy is eventually consistent - 0/30 immediate reads after Put returned the new RevisionId | [DDB-TABLE-248](#ddb-table-248), [DDB-TABLE-244](#ddb-table-244) | - |
| <a id="ddb-table-396"></a>**DDB-TABLE-396** | other | low | confirmed | Doc claim C022 TRUE: after Put on a policy-less table, GetResourcePolicy returns PolicyNotFoundException for ~1.5 s (max 1.7 s, 6/6) | [DDB-TABLE-244](#ddb-table-244) | - |
| <a id="ddb-table-397"></a>**DDB-TABLE-397** | other | low | confirmed | Doc claim C023 TRUE: 'wait a few seconds and retry GetResourcePolicy' - reads converge within 1-2.7 s after Put/Delete | [DDB-TABLE-244](#ddb-table-244), [DDB-TABLE-245](#ddb-table-245), [DDB-TABLE-246](#ddb-table-246) | - |
| <a id="ddb-table-398"></a>**DDB-TABLE-398** | other | low | confirmed | Doc claim C024 TRUE: policy enforcement is eventually consistent - changes after the first take ~221-237 s to apply (Get converges in ~2 s) | [DDB-TABLE-324](#ddb-table-324), [DDB-TABLE-270](#ddb-table-270) | - |
| <a id="ddb-table-401"></a>**DDB-TABLE-401** | other | low | confirmed | Doc claim C031 TRUE: PutResourcePolicy application is eventually consistent - Get lags ~1.5-2.7 s, enforcement ~4 min after the first change | [DDB-TABLE-244](#ddb-table-244), [DDB-TABLE-245](#ddb-table-245), [DDB-TABLE-324](#ddb-table-324) | - |
| <a id="ddb-table-402"></a>**DDB-TABLE-402** | other | low | confirmed | Doc claim C032 TRUE: PutResourcePolicy responds 200 at once while the change applies asynchronously (~1-2 s 'pending' window) | [DDB-TABLE-247](#ddb-table-247), [DDB-TABLE-206](#ddb-table-206), [DDB-TABLE-244](#ddb-table-244) | - |
| <a id="ddb-table-404"></a>**DDB-TABLE-404** | other | low | confirmed | Doc claim C033 TRUE: GetResourcePolicy is eventually consistent; the policy may not be visible right after Put | [DDB-TABLE-244](#ddb-table-244), [DDB-TABLE-245](#ddb-table-245), [DDB-TABLE-248](#ddb-table-248) | - |
| <a id="ddb-table-436"></a>**DDB-TABLE-436** | identity | low | confirmed | No leak to a same-name re-created table: Contributor Insights, TTL, resource policy and tags read fresh on the new incarnation | [DDB-TABLE-271](#ddb-table-271), [DDB-TABLE-292](table-replicas.md#ddb-table-292), [DDB-TABLE-016](table-streams-encryption-class.md#ddb-table-016), [DDB-TABLE-378](table-streams-encryption-class.md#ddb-table-378), [DDB-TABLE-214](#ddb-table-214), [DDB-TABLE-236](#ddb-table-236), [DDB-TABLE-097](table-subresources.md#ddb-table-097), [DDB-TABLE-342](table-subresources.md#ddb-table-342), [DDB-TABLE-374](service.md#ddb-table-374), [DDB-TABLE-377](service.md#ddb-table-377) | - |
| <a id="ddb-table-460"></a>**DDB-TABLE-460** | async-state-machine | low | confirmed | Kinesis Enable and Contributor Insights ENABLE are admitted during a TableClass switch, complete normally and do not reset TableStatus | [DDB-TABLE-286](table-streams-encryption-class.md#ddb-table-286), [DDB-TABLE-450](table-streams-encryption-class.md#ddb-table-450), [DDB-TABLE-287](table-streams-encryption-class.md#ddb-table-287), [DDB-TABLE-300](#ddb-table-300), [DDB-TABLE-299](#ddb-table-299), [DDB-TABLE-301](#ddb-table-301), [DDB-TABLE-302](#ddb-table-302), [DDB-TABLE-304](#ddb-table-304), [DDB-TABLE-269](#ddb-table-269), [DDB-TABLE-268](#ddb-table-268), [DDB-TABLE-319](#ddb-table-319), [DDB-TABLE-303](#ddb-table-303), [DDB-TABLE-235](#ddb-table-235), [DDB-TABLE-236](#ddb-table-236), [DDB-TABLE-234](#ddb-table-234), [DDB-TABLE-117](table-streams-encryption-class.md#ddb-table-117), [DDB-TABLE-119](table-streams-encryption-class.md#ddb-table-119), [DDB-TABLE-435](table-streams-encryption-class.md#ddb-table-435), [DDB-TABLE-121](table-throughput-billing.md#ddb-table-121) | - |

## Supplementary notes

<!-- preserved:start -->
<!-- preserved:end -->
