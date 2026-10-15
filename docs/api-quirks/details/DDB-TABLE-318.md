<!-- generated: ack-api-quirks dynamodb 2026-10-09 model=2012-08-10 controller=34b85e6 -->
# DDB-TABLE-318: Enable never validates the stream synchronously: bad/missing/other-region/other-account/CREATING ARNs -> 200 ENABLING then ENABLE_FAILED
_Full entry and notes of one finding; its summary entry is in
[table-policy-kinesis-autoscaling.md](../table-policy-kinesis-autoscaling.md). Generated from ack-api-quirks
`services/dynamodb` (model 2012-08-10, controller commit 34b85e6); edit the finding in the lab, not here._

## Finding

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

## Notes

H-S-034 REFUTED on the synchronous channel: no ResourceNotFoundException/ValidationException for any ARN - all
failures are asynchronous ENABLE_FAILED (the hypothesised 'only workflow failures end as ENABLE_FAILED' is
backwards). H-S-114 REFUTED: other-region and consumer ARNs are also accepted synchronously; the region
constraint only appears in DestinationStatusDescription. A controller must poll Describe after Enable and
surface DestinationStatusDescription as a terminal condition; it cannot rely on the Enable response.
