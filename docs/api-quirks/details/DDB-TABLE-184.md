<!-- generated: ack-api-quirks dynamodb 2026-10-09 model=2012-08-10 controller=34b85e6 -->
# DDB-TABLE-184: Provisioned decreases: 4 accepted back-to-back, 5th -> LimitExceededException naming the next allowed time; counted per call/table
_Full entry and notes of one finding; its summary entry is in
[table-throughput-billing.md](../table-throughput-billing.md). Generated from ack-api-quirks
`services/dynamodb` (model 2012-08-10, controller commit 34b85e6); edit the finding in the lab, not here._

## Finding

- <a id="ddb-table-184"></a>**DDB-TABLE-184** `quota-limit` · impact high · handled · verified 2026-10-09
  **Provisioned decreases: 4 accepted back-to-back, 5th -> LimitExceededException naming the next allowed time; counted per call/table**
  Fresh PROVISIONED 10/10 table: ProvisionedThroughput has NumberOfDecreasesToday=0 and no
  LastDecreaseDateTime / LastIncreaseDateTime members. Four decreases (10/10->9/9 both dimensions, then WCU
  9->8->7->6) issued ~2 s apart, each waiting ACTIVE (UPDATING 2.0 s each), were accepted; the UpdateTable
  response reports the OLD units and the OLD NumberOfDecreasesToday (0,1,2,3) but a NEW LastDecreaseDateTime,
  while DescribeTable after ACTIVE shows the new units and the counter 1,2,3,4. The 5th decrease ->
  LimitExceededException HTTP 400 "Subscriber limit exceeded: Provisioned throughput decreases are limited
  within a given UTC day. After the first 4 decreases, each subsequent decrease in the same UTC day can be
  performed at most once every 3600 seconds. Number of decreases today: 4. Last decrease at Thursday, October
  8, 2026 at 11:20:48 PM Coordinated Universal Time. Next decrease can be made at Friday, October 9, 2026 at
  12:00:00 AM Coordinated Universal Time." An immediate retry and a mixed RCU-down/WCU-up request get the same
  error; an increase (both dimensions up) is accepted. Decreasing both RCU and WCU in one call counted as one
  decrease. Another table (separate probe) was unaffected: the budget is per table.
  - ACK: terminal_codes, requeue, one-per-reconcile · ops: UpdateTable, DescribeTable · fields:
    ProvisionedThroughput, ProvisionedThroughput.NumberOfDecreasesToday,
    ProvisionedThroughput.LastDecreaseDateTime, ProvisionedThroughput.LastIncreaseDateTime
  - repro: CreateTable PROVISIONED 10/10; UpdateTable decreasing WCU by 1 five times, waiting ACTIVE between
    calls
  - measurements: updating_s_per_decrease=2.02, decreases_before_reject=4
  - handling: handled via `pkg/resource/table/hooks.go:81-84; pkg/resource/table/hooks.go:198-202; pkg/resource/table/hooks.go:306; pkg/resource/table/hooks.go:325-335; generator.yaml:88-90; pkg/resource/table/sdk.go:1234-1250`
  - related: [DDB-TABLE-185](../table-throughput-billing.md#ddb-table-185), [DDB-TABLE-326](../table-throughput-billing.md#ddb-table-326), [DDB-TABLE-381](../table-throughput-billing.md#ddb-table-381), [DDB-TABLE-063](../table-throughput-billing.md#ddb-table-063), [DDB-TABLE-104](../table.md#ddb-table-104) · evidence:
    table/limits/provisioned-decrease-billing-flip

## Notes

Confirms H-T-047 and the first half of H-T-113 (4 immediate decreases, then one per 3600 s, next-allowed time
in the message). The message is the only machine-readable source of the next-allowed timestamp;
NumberOfDecreasesToday in the UpdateTable response lags by one (stale-response), so a controller must read
DescribeTable after ACTIVE to know the real count. The error is deterministic until the hour/day boundary, so
it should be treated as terminal-for-now with a requeue computed from the message or from the top of the next
hour, not hot-retried.
