<!-- generated: ack-api-quirks dynamodb 2026-10-09 model=2012-08-10 controller=34b85e6 -->
# DDB-TABLE-302: Disable of a never-enabled/nonexistent/malformed stream ARN -> ValidationException (must be ACTIVE to DISABLE), never ResourceNotFound
_Full entry and notes of one finding; its summary entry is in
[table-policy-kinesis-autoscaling.md](../table-policy-kinesis-autoscaling.md). Generated from ack-api-quirks
`services/dynamodb` (model 2012-08-10, controller commit 34b85e6); edit the finding in the lab, not here._

## Finding

- <a id="ddb-table-302"></a>**DDB-TABLE-302** `error-code` · impact medium · unhandled (not handled in controller) · verified 2026-10-09
  **Disable of a never-enabled/nonexistent/malformed stream ARN -> ValidationException (must be ACTIVE to DISABLE), never ResourceNotFound**
  With K1 DISABLED and K2 never attached: Disable(K2, an existing ACTIVE stream) -> ValidationException (HTTP 400)
  'Table is not in a valid state to enable Kinesis Streaming Destination: KinesisStreamingDestination must be
  ACTIVE to perform DISABLE operation.'; Disable(nonexistent stream ARN) -> ValidationException (HTTP 400)
  'Table is not in a valid state to enable Kinesis Streaming Destination: KinesisStreamingDestination must be
  ACTIVE to perform DISABLE operation.'; Disable('not-an-arn') -> ValidationException (HTTP 400) 'Table is not
  in a valid state to enable Kinesis Streaming Destination: KinesisStreamingDestination must be ACTIVE to
  perform DISABLE operation.'; Update(K2 never attached) -> ValidationException (HTTP 400) 'Table is not in a
  valid state to enable Kinesis Streaming Destination: No streaming destination with streamArn:
  arn:aws:kinesis:us-west-2:<ACCOUNT>'. The Disable message does not say which stream or that it is unknown;
  only Update distinguishes 'No streaming destination with streamArn'.
  - ACK: terminal_codes, custom_delete · ops: DisableKinesisStreamingDestination,
    UpdateKinesisStreamingDestination · fields: StreamArn
  - repro: Disable a stream that was never enabled on the table; Disable a nonexistent stream ARN; Disable a
    malformed ARN; Update a never-enabled stream
  - handling: not handled in the controller (as of commit 34b85e6)
  - related: [DDB-TABLE-299](../table-policy-kinesis-autoscaling.md#ddb-table-299), [DDB-TABLE-300](../table-policy-kinesis-autoscaling.md#ddb-table-300), [DDB-TABLE-301](../table-policy-kinesis-autoscaling.md#ddb-table-301), [DDB-TABLE-304](../table-policy-kinesis-autoscaling.md#ddb-table-304), [DDB-TABLE-269](../table-policy-kinesis-autoscaling.md#ddb-table-269), [DDB-TABLE-268](../table-policy-kinesis-autoscaling.md#ddb-table-268),
    [DDB-TABLE-319](../table-policy-kinesis-autoscaling.md#ddb-table-319), [DDB-TABLE-303](../table-policy-kinesis-autoscaling.md#ddb-table-303), [DDB-TABLE-235](../table-policy-kinesis-autoscaling.md#ddb-table-235), [DDB-TABLE-236](../table-policy-kinesis-autoscaling.md#ddb-table-236), [DDB-TABLE-234](../table-policy-kinesis-autoscaling.md#ddb-table-234), [DDB-TABLE-460](../table-policy-kinesis-autoscaling.md#ddb-table-460) · hypotheses:
    H-S-038 · evidence: table/sub-resources/kinesis-destination

## Notes

H-S-038 never-enabled clause refuted: a controller cannot distinguish 'nothing to disable' from 'wrong state'
by code, only by Describe. Idempotent delete must be implemented as 'Describe, then Disable only if the entry
is ACTIVE'.
