<!-- generated: ack-api-quirks dynamodb 2026-10-09 model=2012-08-10 controller=34b85e6 -->
# DDB-TABLE-303: Kinesis destination is independent of DynamoDB Streams: no StreamSpecification appears, and toggling Streams leaves the destination ACTIVE
_Full entry and notes of one finding; its summary entry is in
[table-policy-kinesis-autoscaling.md](../table-policy-kinesis-autoscaling.md). Generated from ack-api-quirks
`services/dynamodb` (model 2012-08-10, controller commit 34b85e6); edit the finding in the lab, not here._

## Finding

- <a id="ddb-table-303"></a>**DDB-TABLE-303** `prerequisite` · impact medium · unhandled (not handled in controller) · verified 2026-10-09
  **Kinesis destination is independent of DynamoDB Streams: no StreamSpecification appears, and toggling Streams leaves the destination ACTIVE**
  With the destination ACTIVE on a table created without Streams, DescribeTable shows StreamSpecification=None
  LatestStreamArn=None. UpdateTable(StreamEnabled=true) -> 200 OK (TableStatus UPDATING; UPDATING 5.06s);
  destination afterwards [('ks1', 'ACTIVE')]. UpdateTable(StreamEnabled=false) -> 200 OK (UPDATING 4.05s);
  destination afterwards [('ks1', 'ACTIVE')]; StreamSpecification after: None (LatestStreamArn still reported:
  True).
  - ACK: none, compare.is_ignored+delta_pre_compare · ops: EnableKinesisStreamingDestination, UpdateTable,
    DescribeTable · fields: StreamSpecification, KinesisDataStreamDestinations
  - repro: Enable Kinesis on a table without Streams; DescribeTable; UpdateTable(StreamSpecification enabled);
    UpdateTable(disabled); DescribeKinesisStreamingDestination
  - handling: not handled in the controller (as of commit 34b85e6)
  - related: [DDB-TABLE-299](../table-policy-kinesis-autoscaling.md#ddb-table-299), [DDB-TABLE-300](../table-policy-kinesis-autoscaling.md#ddb-table-300), [DDB-TABLE-301](../table-policy-kinesis-autoscaling.md#ddb-table-301), [DDB-TABLE-302](../table-policy-kinesis-autoscaling.md#ddb-table-302), [DDB-TABLE-304](../table-policy-kinesis-autoscaling.md#ddb-table-304), [DDB-TABLE-269](../table-policy-kinesis-autoscaling.md#ddb-table-269),
    [DDB-TABLE-268](../table-policy-kinesis-autoscaling.md#ddb-table-268), [DDB-TABLE-319](../table-policy-kinesis-autoscaling.md#ddb-table-319), [DDB-TABLE-235](../table-policy-kinesis-autoscaling.md#ddb-table-235), [DDB-TABLE-236](../table-policy-kinesis-autoscaling.md#ddb-table-236), [DDB-TABLE-234](../table-policy-kinesis-autoscaling.md#ddb-table-234), [DDB-TABLE-460](../table-policy-kinesis-autoscaling.md#ddb-table-460) · hypotheses:
    H-S-117 · evidence: table/sub-resources/kinesis-destination

## Notes

H-S-117 confirmed.
