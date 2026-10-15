<!-- generated: ack-api-quirks dynamodb 2026-10-09 model=2012-08-10 controller=34b85e6 -->
# DDB-TABLE-360: Read bursts on one table, no retries: ListTagsOfResource never throttled, DescribeTimeToLive throttles (30 sequential + 30/60 concurrent)
_Full entry and notes of one finding; its summary entry is in
[table-subresources.md](../table-subresources.md). Generated from ack-api-quirks `services/dynamodb` (model
2012-08-10, controller commit 34b85e6); edit the finding in the lab, not here._

## Finding

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
  - related: [DDB-TABLE-099](../service.md#ddb-table-099), [DDB-TABLE-130](../service.md#ddb-table-130), [DDB-TABLE-053](../service.md#ddb-table-053), [DDB-TABLE-403](../service.md#ddb-table-403), [DDB-TABLE-445](../service.md#ddb-table-445) · hypotheses: H-T-131 ·
    evidence: table/limits/read-api-bursts

## Notes

Qualifies H-T-131: the documented 10/s ListTagsOfResource limit is NOT enforced per table at these burst
sizes. DescribeTimeToLive (documented 10/s) throttled - code/message above.

Contradiction with [DDB-TABLE-099](../service.md#ddb-table-099): 099 says 15 concurrent DescribeTimeToLive all pass and ThrottlingException
appears only under shared account load; 360 shows 30 sequential DescribeTimeToLive on one table throttle from
call #4 ('Rate exceeded') while 30 concurrent pass. [DDB-TABLE-403](../service.md#ddb-table-403) reconciles: the limiter is a
per-HTTP-connection bucket (~4 burst, refill ~2 s); fresh connections never throttle and the earlier 1/s
throttles were this bucket, not shared load Resolution: keep both; 403 is canonical; 099's attribution to
account-wide load is wrong (title fix)
