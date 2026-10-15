<!-- generated: ack-api-quirks dynamodb 2026-10-09 model=2012-08-10 controller=34b85e6 -->
# DDB-TABLE-233: Create-time ResourcePolicy readable ~2.09s after CreateTable while still CREATING; earlier reads -> ResourceNotFoundException
_Full entry and notes of one finding; its summary entry is in
[table-policy-kinesis-autoscaling.md](../table-policy-kinesis-autoscaling.md). Generated from ack-api-quirks
`services/dynamodb` (model 2012-08-10, controller commit 34b85e6); edit the finding in the lab, not here._

## Finding

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
  - related: [DDB-TABLE-111](../table-subresources.md#ddb-table-111), [DDB-TABLE-115](../service.md#ddb-table-115), [DDB-TABLE-234](../table-policy-kinesis-autoscaling.md#ddb-table-234), [DDB-TABLE-114](../service.md#ddb-table-114), [DDB-TABLE-244](../table-policy-kinesis-autoscaling.md#ddb-table-244), [DDB-TABLE-245](../table-policy-kinesis-autoscaling.md#ddb-table-245),
    [DDB-TABLE-246](../table-policy-kinesis-autoscaling.md#ddb-table-246), [DDB-TABLE-248](../table-policy-kinesis-autoscaling.md#ddb-table-248), [DDB-TABLE-346](../table-policy-kinesis-autoscaling.md#ddb-table-346), [DDB-TABLE-208](../table-policy-kinesis-autoscaling.md#ddb-table-208), [DDB-TABLE-351](../table-policy-kinesis-autoscaling.md#ddb-table-351) · hypotheses: H-S-030, H-S-118 ·
    evidence: table/state-machine/policy-kinesis-admissibility, table/creative/reverify-set-a2

## Notes

H-S-030 partially refuted: the create-time policy IS readable before ACTIVE (about 2s after CreateTable), and
the early error is ResourceNotFoundException (table-level metadata lag), not PolicyNotFoundException. The 'not
visible in CreateTable/DescribeTable output' clause is confirmed.

Contradiction with [DDB-TABLE-234](../table-policy-kinesis-autoscaling.md#ddb-table-234): 234 says every policy/Kinesis API -> ResourceNotFoundException while
CREATING; 233 (same probe run, sibling table) shows GetResourcePolicy and DescribeKinesisStreamingDestination
return 200 from ~2.09 s while the table is still CREATING until 7.25 s. 234's calls were single shots 'right
after CreateTable', i.e. inside the ~1-2 s metadata-lag window Resolution: keep both; 233 is canonical for
reads; 234's claim holds only for the first ~1-2 s (title fix); policy/Kinesis mutators later in CREATING were
not tested
