<!-- generated: ack-api-quirks dynamodb 2026-10-09 model=2012-08-10 controller=34b85e6 -->
# DDB-TABLE-326: After 4 decreases, exactly one more is admitted 3600 s after the last one (not at the top of the hour); 6th -> LimitExceeded again
_Full entry and notes of one finding; its summary entry is in
[table-throughput-billing.md](../table-throughput-billing.md). Generated from ack-api-quirks
`services/dynamodb` (model 2012-08-10, controller commit 34b85e6); edit the finding in the lab, not here._

## Finding

- <a id="ddb-table-326"></a>**DDB-TABLE-326** `quota-limit` · impact high · handled · verified 2026-10-09
  **After 4 decreases, exactly one more is admitted 3600 s after the last one (not at the top of the hour); 6th -> LimitExceeded again**
  PROVISIONED 10/10 table, RCU 10->9->8->7->6 in 4 calls (UPDATING 1-2 s each). 5th decrease ->
  LimitExceededException HTTP 400: 'Subscriber limit exceeded: Provisioned throughput decreases are limited
  within a given UTC day. After the first 4 decreases, each subsequent decrease in the same UTC day can be
  performed at most once every 3600 seconds. Number of decreases today: 4. Last decrease at Friday, October 9,
  2026 at 12:21:34 AM Coordinated Universal Time. Next decrease can be made at Friday, October 9, 2026 at
  1:21:34 AM Coordinated Universal Time'. The same decrease retried at T_last+5,10,...,55 min, at the top of
  the hour (01:00:20), and every minute from +56 min: all LimitExceededException up to T_last+3540 s; admitted
  at T_last+3600 s (01:21:34). NumberOfDecreasesToday then 5, LastDecreaseDateTime moved to 01:21:36. Failed
  attempts never moved NumberOfDecreasesToday or LastDecreaseDateTime. Immediately after the admitted decrease
  a further decrease -> LimitExceededException ('Number of decreases today: 5 ... Next decrease can be made at
  ... 2:21:36 AM'), still rejected 5 min later: one token per 3600 s measured from the LAST ACCEPTED decrease,
  no bucket refill.
  - ACK: requeue, terminal_codes, custom_update · ops: UpdateTable, DescribeTable · fields:
    ProvisionedThroughput, ProvisionedThroughput.NumberOfDecreasesToday,
    ProvisionedThroughput.LastDecreaseDateTime
  - repro: PROVISIONED table; 5 consecutive RCU decreases; retry the 5th every 5 min then every minute from
    +56 min; on success retry immediately
  - measurements: refill_after_last_decrease_s=3600.1, fifth_rejection_latency_ms=18,
    last_failed_attempt_since_last_decrease_s=3540.1
  - handling: handled via `pkg/resource/table/hooks.go:81-84; pkg/resource/table/hooks.go:198-202; generator.yaml:88-90; pkg/resource/table/sdk.go:1234-1250`
  - related: [DDB-TABLE-184](../table-throughput-billing.md#ddb-table-184), [DDB-TABLE-185](../table-throughput-billing.md#ddb-table-185), [DDB-TABLE-381](../table-throughput-billing.md#ddb-table-381) · hypotheses: H-T-113, H-T-047 · evidence:
    table/limits/decrease-hourly-refill

## Notes

Confirms the hourly-refill half of H-T-113 as 'once per 3600 s after the last accepted decrease' (the
4-then-LimitExceeded half and the 00:00 UTC reset were measured in
table/limits/provisioned-decrease-billing-flip). The next-allowed time is only available by parsing the
message ('Next decrease can be made at <locale-formatted timestamp>') or by adding 3600 s to
ProvisionedThroughput.LastDecreaseDateTime; there is no Retry-After.

Contradiction with [DDB-TABLE-185](../table-throughput-billing.md#ddb-table-185): 185's guidance computes the earliest retry as min(next top-of-hour, next
00:00 UTC); 326 measured the refill as exactly 3600 s after the last ACCEPTED decrease (01:21:34 after
00:21:34; the 01:00:20 top-of-hour attempt failed). Behavior texts agree (185's own message said 12:00:00 AM
because midnight came first); only 185's derived rule is wrong Resolution: keep both; 326 canonical for the
refill rule: next_allowed = min(LastDecreaseDateTime + 3600 s, next 00:00 UTC) or parse 'Next decrease can be
made at'
