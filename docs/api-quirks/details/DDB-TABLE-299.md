<!-- generated: ack-api-quirks dynamodb 2026-10-09 model=2012-08-10 controller=34b85e6 -->
# DDB-TABLE-299: Kinesis destination: ENABLING ~6 s then ACTIVE; at most one live (non-DISABLED/ENABLE_FAILED) entry per table; non-ACTIVE ops -> Validation
_Full entry and notes of one finding; its summary entry is in
[table-policy-kinesis-autoscaling.md](../table-policy-kinesis-autoscaling.md). Generated from ack-api-quirks
`services/dynamodb` (model 2012-08-10, controller commit 34b85e6); edit the finding in the lab, not here._

## Finding

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
  - related: [DDB-TABLE-300](../table-policy-kinesis-autoscaling.md#ddb-table-300), [DDB-TABLE-301](../table-policy-kinesis-autoscaling.md#ddb-table-301), [DDB-TABLE-302](../table-policy-kinesis-autoscaling.md#ddb-table-302), [DDB-TABLE-304](../table-policy-kinesis-autoscaling.md#ddb-table-304), [DDB-TABLE-269](../table-policy-kinesis-autoscaling.md#ddb-table-269), [DDB-TABLE-268](../table-policy-kinesis-autoscaling.md#ddb-table-268),
    [DDB-TABLE-319](../table-policy-kinesis-autoscaling.md#ddb-table-319), [DDB-TABLE-303](../table-policy-kinesis-autoscaling.md#ddb-table-303), [DDB-TABLE-235](../table-policy-kinesis-autoscaling.md#ddb-table-235), [DDB-TABLE-236](../table-policy-kinesis-autoscaling.md#ddb-table-236), [DDB-TABLE-234](../table-policy-kinesis-autoscaling.md#ddb-table-234), [DDB-TABLE-460](../table-policy-kinesis-autoscaling.md#ddb-table-460) · hypotheses:
    H-S-033, H-S-008, H-S-113, H-S-037, H-S-130 · evidence: table/sub-resources/kinesis-destination

## Notes

H-S-033: ENABLING->ACTIVE confirmed but lasts seconds (2-7s across probes), not 30-120s, and in-flight
rejections are ValidationException 'Table is not in a valid state to enable Kinesis Streaming Destination:
...', never ResourceInUseException. H-S-113 (one stream per table) confirmed; H-S-037 (two allowed) refuted -
a second Enable is rejected with the same 'must be DISABLED or ENABLE_FAILED' message as a duplicate. H-S-130:
no Describe lag after Enable (entry visible at 71ms).

Contradiction with [DDB-TABLE-268](../table-policy-kinesis-autoscaling.md#ddb-table-268): 299 states 'one destination per table'; 268 and [DDB-TABLE-301](../table-policy-kinesis-autoscaling.md#ddb-table-301) show several
DISABLED/ENABLE_FAILED entries coexisting with one ACTIVE entry (list of 3), re-Enable flipping an existing
entry, and Enable rejected only while another entry is ENABLING/ACTIVE/UPDATING/DISABLING Resolution: keep
both; 268's rule 'at most one live entry, any number of DISABLED/ENABLE_FAILED' is canonical; fix 299's title
