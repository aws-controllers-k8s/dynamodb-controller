<!-- generated: ack-api-quirks dynamodb 2026-10-09 model=2012-08-10 controller=34b85e6 -->
# DDB-TABLE-099: 15 concurrent DescribeContinuousBackups/DescribeTimeToLive on fresh connections all pass; 'Rate exceeded' is per-connection, not shared load
_Full entry and notes of one finding; its summary entry is in [service.md](../service.md). Generated from
ack-api-quirks `services/dynamodb` (model 2012-08-10, controller commit 34b85e6); edit the finding in the lab,
not here._

## Finding

- <a id="ddb-table-099"></a>**DDB-TABLE-099** `quota-limit` · impact medium · handled · verified 2026-10-08
  **15 concurrent DescribeContinuousBackups/DescribeTimeToLive on fresh connections all pass; 'Rate exceeded' is per-connection, not shared load**
  Burst of 15 concurrent DescribeContinuousBackups with SDK retries disabled completed in 134 ms, all 15 HTTP
  200; a single call 2 s later also 200. Same for 15 concurrent DescribeTimeToLive (52 ms, 15x200). Earlier in
  the session, with several probes polling in the same account, a 1/s DescribeTimeToLive poll loop received
  ThrottlingException (HTTP 400) on single calls (table/mutation-matrix/ttl-updates, t3/t5 timelines), so the
  limit is account-wide and bursty rather than a hard per-caller 10/s.
  - ACK: requeue, runtime-gap · ops: DescribeContinuousBackups, DescribeTimeToLive
  - repro: 15 threads x 1 DescribeContinuousBackups with max_attempts=1
  - measurements: burst_span_ms=134, throttled_count_cb=0, throttled_count_ttl=0
  - handling: handled via `templates/hooks/table/sdk_read_one_post_set_output.go.tpl:57-72; bcd26e1`
  - related: [DDB-TABLE-360](../table-subresources.md#ddb-table-360), [DDB-TABLE-403](../service.md#ddb-table-403), [DDB-TABLE-445](../service.md#ddb-table-445) · evidence: table/error-taxonomy/subresource-errors

## Notes

Contradiction with [DDB-TABLE-360](../table-subresources.md#ddb-table-360): 099 says 15 concurrent DescribeTimeToLive all pass and ThrottlingException
appears only under shared account load; 360 shows 30 sequential DescribeTimeToLive on one table throttle from
call #4 ('Rate exceeded') while 30 concurrent pass. [DDB-TABLE-403](../service.md#ddb-table-403) reconciles: the limiter is a
per-HTTP-connection bucket (~4 burst, refill ~2 s); fresh connections never throttle and the earlier 1/s
throttles were this bucket, not shared load Resolution: keep both; 403 is canonical; 099's attribution to
account-wide load is wrong (title fix)
