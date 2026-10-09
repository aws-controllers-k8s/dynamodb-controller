<!-- generated: ack-api-quirks dynamodb 2026-10-09 model=2012-08-10 controller=34b85e6 -->
# DDB-TABLE-234: First ~1-2 s of CREATING: every policy/Kinesis API -> ResourceNotFoundException (table not found); UPDATING: Put policy / Enable Kinesis OK
_Full entry and notes of one finding; its summary entry is in
[table-policy-kinesis-autoscaling.md](../table-policy-kinesis-autoscaling.md). Generated from ack-api-quirks
`services/dynamodb` (model 2012-08-10, controller commit 34b85e6); edit the finding in the lab, not here._

## Finding

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
  - related: [DDB-TABLE-111](../table-subresources.md#ddb-table-111), [DDB-TABLE-115](../service.md#ddb-table-115), [DDB-TABLE-233](../table-policy-kinesis-autoscaling.md#ddb-table-233), [DDB-TABLE-114](../service.md#ddb-table-114), [DDB-TABLE-299](../table-policy-kinesis-autoscaling.md#ddb-table-299), [DDB-TABLE-300](../table-policy-kinesis-autoscaling.md#ddb-table-300),
    [DDB-TABLE-301](../table-policy-kinesis-autoscaling.md#ddb-table-301), [DDB-TABLE-302](../table-policy-kinesis-autoscaling.md#ddb-table-302), [DDB-TABLE-304](../table-policy-kinesis-autoscaling.md#ddb-table-304), [DDB-TABLE-269](../table-policy-kinesis-autoscaling.md#ddb-table-269), [DDB-TABLE-268](../table-policy-kinesis-autoscaling.md#ddb-table-268), [DDB-TABLE-319](../table-policy-kinesis-autoscaling.md#ddb-table-319), [DDB-TABLE-303](../table-policy-kinesis-autoscaling.md#ddb-table-303),
    [DDB-TABLE-235](../table-policy-kinesis-autoscaling.md#ddb-table-235), [DDB-TABLE-236](../table-policy-kinesis-autoscaling.md#ddb-table-236), [DDB-TABLE-460](../table-policy-kinesis-autoscaling.md#ddb-table-460), [DDB-TABLE-117](../table-streams-encryption-class.md#ddb-table-117), [DDB-TABLE-119](../table-streams-encryption-class.md#ddb-table-119), [DDB-TABLE-287](../table-streams-encryption-class.md#ddb-table-287), [DDB-TABLE-450](../table-streams-encryption-class.md#ddb-table-450),
    [DDB-TABLE-435](../table-streams-encryption-class.md#ddb-table-435), [DDB-TABLE-121](../table-throughput-billing.md#ddb-table-121) · hypotheses: H-S-027, H-S-033, H-S-120 · evidence:
    table/state-machine/policy-kinesis-admissibility

## Notes

H-S-027: CREATING clause refuted (code is ResourceNotFoundException with the same 'Table: X not found' message
as a missing table, NOT ResourceInUseException); UPDATING clause confirmed (Put succeeds, Enable succeeds
too). Note the Delete right after a Put -> ResourceInUseException 'Table is pending previous resource-based
policy update' (H-S-027's code appears here). Update/Disable during ENABLING -> ValidationException (H-S-033
said ResourceInUseException).

Contradiction with [DDB-TABLE-233](../table-policy-kinesis-autoscaling.md#ddb-table-233): 234 says every policy/Kinesis API -> ResourceNotFoundException while
CREATING; 233 (same probe run, sibling table) shows GetResourcePolicy and DescribeKinesisStreamingDestination
return 200 from ~2.09 s while the table is still CREATING until 7.25 s. 234's calls were single shots 'right
after CreateTable', i.e. inside the ~1-2 s metadata-lag window Resolution: keep both; 233 is canonical for
reads; 234's claim holds only for the first ~1-2 s (title fix); policy/Kinesis mutators later in CREATING were
not tested
