<!-- generated: ack-api-quirks dynamodb 2026-10-09 model=2012-08-10 controller=34b85e6 -->
# DDB-TABLE-300: ApproximateCreationDateTimePrecision absent until set (absent == MILLISECOND server-side); Update -> UPDATING ~124-152 s; same value -> 400
_Full entry and notes of one finding; its summary entry is in
[table-policy-kinesis-autoscaling.md](../table-policy-kinesis-autoscaling.md). Generated from ack-api-quirks
`services/dynamodb` (model 2012-08-10, controller commit 34b85e6); edit the finding in the lab, not here._

## Finding

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
  - related: [DDB-TABLE-299](../table-policy-kinesis-autoscaling.md#ddb-table-299), [DDB-TABLE-301](../table-policy-kinesis-autoscaling.md#ddb-table-301), [DDB-TABLE-302](../table-policy-kinesis-autoscaling.md#ddb-table-302), [DDB-TABLE-304](../table-policy-kinesis-autoscaling.md#ddb-table-304), [DDB-TABLE-269](../table-policy-kinesis-autoscaling.md#ddb-table-269), [DDB-TABLE-268](../table-policy-kinesis-autoscaling.md#ddb-table-268),
    [DDB-TABLE-319](../table-policy-kinesis-autoscaling.md#ddb-table-319), [DDB-TABLE-303](../table-policy-kinesis-autoscaling.md#ddb-table-303), [DDB-TABLE-235](../table-policy-kinesis-autoscaling.md#ddb-table-235), [DDB-TABLE-236](../table-policy-kinesis-autoscaling.md#ddb-table-236), [DDB-TABLE-234](../table-policy-kinesis-autoscaling.md#ddb-table-234), [DDB-TABLE-460](../table-policy-kinesis-autoscaling.md#ddb-table-460), [DDB-TABLE-085](../table-subresources.md#ddb-table-085),
    [DDB-TABLE-091](../table-subresources.md#ddb-table-091), [DDB-TABLE-341](../table-subresources.md#ddb-table-341), [DDB-TABLE-147](../table-subresources.md#ddb-table-147), [DDB-TABLE-207](../table-policy-kinesis-autoscaling.md#ddb-table-207), [DDB-TABLE-383](../table-streams-encryption-class.md#ddb-table-383), [DDB-TABLE-205](../table-policy-kinesis-autoscaling.md#ddb-table-205) · hypotheses:
    H-S-036 · evidence: table/sub-resources/kinesis-destination, table/creative/reverify-set-a2

## Notes

H-S-036: 'Describe materializes MILLISECOND' REFUTED (field absent, so a controller must treat absent ==
MILLISECOND); 'Update transitions UPDATING->ACTIVE in seconds' refuted (2-2.5 minutes); 'same precision ->
ValidationException' confirmed ('Precision is already set to the desired value'). Disabling resets the
precision (re-Enable without config -> absent).

Contradiction with [DDB-TABLE-460](../table-policy-kinesis-autoscaling.md#ddb-table-460): 300's title says the precision is 'absent until set (not MILLISECOND)'; 460
shows UpdateKinesisStreamingDestination re-sending MILLISECOND on an entry with no precision field ->
ValidationException 'Precision is already set to the desired value of MILLISECOND', i.e. the server's
effective default IS MILLISECOND (300's notes agree: treat absent == MILLISECOND) Resolution: keep both; 300
is canonical with its title corrected; 460 extends it with the implicit-default re-send rejection
