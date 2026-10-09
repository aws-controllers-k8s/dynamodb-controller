<!-- generated: ack-api-quirks dynamodb 2026-10-09 model=2012-08-10 controller=34b85e6 -->
# DDB-TABLE-180: UpdateTable response and DescribeTable show requested StreamSpecification/LatestStreamArn during UPDATING; TableClassSummary only after
_Full entry and notes of one finding; its summary entry is in
[table-streams-encryption-class.md](../table-streams-encryption-class.md). Generated from ack-api-quirks
`services/dynamodb` (model 2012-08-10, controller commit 34b85e6); edit the finding in the lab, not here._

## Finding

- <a id="ddb-table-180"></a>**DDB-TABLE-180** `requested-vs-effective` · impact medium · handled · verified 2026-10-08
  **UpdateTable response and DescribeTable show requested StreamSpecification/LatestStreamArn during UPDATING; TableClassSummary only after**
  Stream enable -> OK response={"TableStatus": "UPDATING", "StreamSpecification": {"StreamEnabled": true,
  "StreamViewType": "KEYS_ONLY"}, "LatestStreamArn":
  "arn:aws:dynamodb:us-west-2:<ACCOUNT>:table/ackq-2e2def-rf-e/stream/2026-10-08T23:40:47.869"}; describe
  transitions={"timed_out": false, "transitions": [{"at_s": 0.01, "value": {"TableStatus": "UPDATING",
  "StreamSpecification": {"StreamEnabled": true, "StreamViewType": "KEYS_ONLY"}, "LatestStreamArn":
  "arn:aws:dynamodb:us-west-2:<ACCOUNT>:table/ackq-2e2d. Stream disable -> OK response={"TableStatus":
  "UPDATING", "StreamSpecification": "<absent>", "LatestStreamArn":
  "arn:aws:dynamodb:us-west-2:<ACCOUNT>:table/ackq-2e2def-rf-e/stream/2026-10-08T23:40:47.869"}; describe
  transitions={"timed_out": false, "transitions": [{"at_s": 0.01, "value": {"TableStatus": "UPDATING",
  "StreamSpecification": "<absent>", "LatestStreamArn": "arn:aws:dynamodb:us-west-2:<ACCOUNT>:tab. TableClass
  -> STANDARD_INFREQUENT_ACCESS -> OK response={"TableStatus": "UPDATING", "TableClassSummary": "<absent>"};
  describe transitions={"timed_out": false, "transitions": [{"at_s": 0.01, "value": {"TableStatus":
  "UPDATING", "TableClassSummary": "<absent>"}}, {"at_s": 6.07, "value": {"TableStatus": "ACTIVE",
  "TableClassSummary": {"TableClass": "STANDARD_INFREQUENT_ACCESS", "LastUpdateDateTime": "2026-10-08
  23:41:02.319000+00:00"}}}]}. Re-send IA after ACTIVE -> OK response={"TableStatus": "UPDATING",
  "TableClassSummary": {"TableClass": "STANDARD_INFREQUENT_ACCESS", "LastUpdateDateTime": "2026-10-08
  23:41:02.319000+00:00"}}; describe transitions={"timed_out": true, "transitions": [{"at_s": 0.01, "value":
  {"TableStatus": "ACTIVE", "TableClassSummary": {"TableClass": "STANDARD_INFREQUENT_ACCESS",
  "LastUpdateDateTime": "2026-10-08 23:41:02.319000+00:00"}}}]}; status 10s later: ACTIVE.
  - ACK: synced.when, requeue, compare.is_ignored+delta_pre_compare · ops: UpdateTable, DescribeTable ·
    fields: StreamSpecification, LatestStreamArn, TableClassSummary, TableStatus
  - repro: PPR table; UpdateTable StreamSpecification enable; DescribeTable at 0.5s; UpdateTable
    TableClass=STANDARD_INFREQUENT_ACCESS; DescribeTable at 2s
  - measurements: stream_enable_updating_s=5.13, tableclass_updating_s=6.07
  - handling: handled via `pkg/resource/table/hooks.go:306; pkg/resource/table/hooks.go:325-335; test/e2e/table.py:88-100; pkg/resource/table/sdk.go:369-380`
  - related: [DDB-TABLE-004](../table-streams-encryption-class.md#ddb-table-004), [DDB-TABLE-026](../table-streams-encryption-class.md#ddb-table-026), [DDB-TABLE-177](../table-streams-encryption-class.md#ddb-table-177), [DDB-TABLE-365](../table-streams-encryption-class.md#ddb-table-365), [DDB-TABLE-284](../table-streams-encryption-class.md#ddb-table-284), [DDB-TABLE-019](../table-streams-encryption-class.md#ddb-table-019),
    [DDB-TABLE-283](../table-streams-encryption-class.md#ddb-table-283), [DDB-TABLE-052](../table-streams-encryption-class.md#ddb-table-052), [DDB-TABLE-285](../table-streams-encryption-class.md#ddb-table-285), [DDB-TABLE-451](../table-streams-encryption-class.md#ddb-table-451), [DDB-TABLE-287](../table-streams-encryption-class.md#ddb-table-287), [DDB-TABLE-120](../table-streams-encryption-class.md#ddb-table-120), [DDB-TABLE-060](../table-throughput-billing.md#ddb-table-060),
    [DDB-TABLE-370](../table-throughput-billing.md#ddb-table-370), [DDB-TABLE-179](../table-throughput-billing.md#ddb-table-179), [DDB-TABLE-066](../table-throughput-billing.md#ddb-table-066), [DDB-TABLE-371](../table-streams-encryption-class.md#ddb-table-371), [DDB-TABLE-140](../table-streams-encryption-class.md#ddb-table-140), [DDB-TABLE-064](../table-throughput-billing.md#ddb-table-064), [DDB-TABLE-183](../table-throughput-billing.md#ddb-table-183),
    [DDB-TABLE-156](../table-throughput-billing.md#ddb-table-156), [DDB-TABLE-050](../table-streams-encryption-class.md#ddb-table-050), [DDB-TABLE-362](../table-streams-encryption-class.md#ddb-table-362), [DDB-TABLE-367](../table-streams-encryption-class.md#ddb-table-367), [DDB-TABLE-378](../table-streams-encryption-class.md#ddb-table-378), [DDB-TABLE-373](../service.md#ddb-table-373), [DDB-TABLE-013](../service.md#ddb-table-013),
    [DDB-TABLE-071](../service.md#ddb-table-071), [DDB-TABLE-213](../table-streams-encryption-class.md#ddb-table-213), [DDB-TABLE-018](../table-streams-encryption-class.md#ddb-table-018) · evidence: table/response-fidelity/create-update-response

## Notes

Contradiction with [DDB-TABLE-052](../table-streams-encryption-class.md#ddb-table-052), [DDB-TABLE-284](../table-streams-encryption-class.md#ddb-table-284), [DDB-TABLE-365](../table-streams-encryption-class.md#ddb-table-365), [DDB-TABLE-018](../table-streams-encryption-class.md#ddb-table-018): 052 reports TableClass UPDATING
31.4 s; 284 (0.5 s polling) measured 4.09/3.58 s, 365 6.1/4.0 s, 180 6.07 s, 018 6.08 s. 284's notes reconcile
it: 052's number is 'elapsed_before_wait_s'/'total_updating_s_upper_bound' from coarse polling, not the switch
duration Resolution: keep both; 284 canonical for the duration; retitle 052
