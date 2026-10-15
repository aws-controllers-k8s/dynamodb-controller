<!-- generated: ack-api-quirks dynamodb 2026-10-09 model=2012-08-10 controller=34b85e6 -->
# DDB-TABLE-316: Manual RCU raised inside the bounds on an idle table was scaled back to Min by the already-firing AlarmLow after 82 s
_Full entry and notes of one finding; its summary entry is in
[table-throughput-billing.md](../table-throughput-billing.md). Generated from ack-api-quirks
`services/dynamodb` (model 2012-08-10, controller commit 34b85e6); edit the finding in the lab, not here._

## Finding

- <a id="ddb-table-316"></a>**DDB-TABLE-316** `requested-vs-effective` · impact high · unhandled (not handled in controller) · verified 2026-10-09
  **Manual RCU raised inside the bounds on an idle table was scaled back to Min by the already-firing AlarmLow after 82 s**
  UpdateTable RCU=15 (bounds 10-20, read AlarmLow already in ALARM because the table had been idle >15 min) ->
  200; table ACTIVE at RCU 15 after 41 s; at 82 s AAS started 'Setting read capacity units to 10' (cause:
  AlarmLow in state ALARM triggered policy DynamoDBReadCapacityUtilization:table/<name>) and the table was
  ACTIVE at RCU 10 at 92 s (NumberOfDecreasesToday 3). A GetItem trickle (1 per 5 s) was running. The write
  dimension (WCU 5 re-applied by the same UpdateTable) kept AlarmLow in ALARM. After the scale-in all four
  read alarms went INSUFFICIENT_DATA briefly.
  - ACK: compare.is_ignored+delta_pre_compare, docs-only · ops: UpdateTable, DescribeTable · fields:
    ProvisionedThroughput
  - repro: autoscaled read 10-20 -> UpdateTable RCU=15 -> trickle reads -> poll for ~20 min; watch write
    dimension with no traffic
  - measurements: seconds_until_scale_in=82.2, seconds_until_active_at_min=92.4,
    idle_write_scale_in_after_policy_create_s=620
  - handling: not handled in the controller (as of commit 34b85e6)
  - hypotheses: H-R-117, H-R-132 · evidence: table/mutation-matrix/autoscaling-vs-throughput

## Notes

H-R-117 'inside bounds is left alone until an alarm fires' confirmed literally, but on an idle table the
scale-in alarm is permanently in ALARM, so any manual value above Min is reverted within ~1.5 min. Combined
with the previous finding: on an idle table the only stable manual values are <= Min (below Min is tolerated
indefinitely).
