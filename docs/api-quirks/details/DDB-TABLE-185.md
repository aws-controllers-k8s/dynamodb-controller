<!-- generated: ack-api-quirks dynamodb 2026-10-09 model=2012-08-10 controller=34b85e6 -->
# DDB-TABLE-185: NumberOfDecreasesToday resets to 0 exactly at 00:00:00 UTC (not 24 h after the first decrease); LastDecreaseDateTime is kept
_Full entry and notes of one finding; its summary entry is in
[table-throughput-billing.md](../table-throughput-billing.md). Generated from ack-api-quirks
`services/dynamodb` (model 2012-08-10, controller commit 34b85e6); edit the finding in the lab, not here._

## Finding

- <a id="ddb-table-185"></a>**DDB-TABLE-185** `quota-limit` · impact medium · unhandled (not handled in controller) · verified 2026-10-09
  **NumberOfDecreasesToday resets to 0 exactly at 00:00:00 UTC (not 24 h after the first decrease); LastDecreaseDateTime is kept**
  With the daily budget exhausted (NumberOfDecreasesToday=4, 5th decrease rejected at 23:20 UTC),
  DescribeTable sampled every 5-10 s showed 4 through 23:59:55 UTC and 0 at the 00:00:00 UTC sample;
  LastDecreaseDateTime (23:20:48) and LastIncreaseDateTime were NOT cleared by the reset. Three further
  decreases at 00:00:46, 00:00:48 and 00:00:50 UTC were all accepted (counter 1,2,3), i.e. the full budget of
  4 immediate decreases is restored at midnight UTC, 40 minutes after the quota had been exhausted; the error
  message had correctly announced "Next decrease can be made at ... 12:00:00 AM Coordinated Universal Time"
  rather than the hourly 3600 s refill.
  - ACK: requeue, terminal_codes · ops: UpdateTable, DescribeTable · fields:
    ProvisionedThroughput.NumberOfDecreasesToday, ProvisionedThroughput.LastDecreaseDateTime
  - repro: Exhaust 4 decreases before midnight UTC; DescribeTable every 5 s across 00:00:00 UTC; decrease
    three more times
  - measurements: reset_observed_at_utc_s_after_midnight=0, post_reset_decreases_accepted=3
  - handling: not handled in the controller (as of commit 34b85e6)
  - related: [DDB-TABLE-184](../table-throughput-billing.md#ddb-table-184), [DDB-TABLE-326](../table-throughput-billing.md#ddb-table-326), [DDB-TABLE-381](../table-throughput-billing.md#ddb-table-381) · evidence:
    table/limits/provisioned-decrease-billing-flip

## Notes

Confirms the UTC-midnight reset and the never-cleared LastDecreaseDateTime parts of H-T-113. The hourly refill
(one extra decrease at the top of the next hour) could not be observed separately because the first boundary
after exhaustion was midnight, which restores the full budget. A controller can compute the earliest retry as
min(next top-of-hour, next 00:00 UTC) and should prefer parsing the "Next decrease can be made at" text when
present.

Contradiction with [DDB-TABLE-326](../table-throughput-billing.md#ddb-table-326): 185's guidance computes the earliest retry as min(next top-of-hour, next
00:00 UTC); 326 measured the refill as exactly 3600 s after the last ACCEPTED decrease (01:21:34 after
00:21:34; the 01:00:20 top-of-hour attempt failed). Behavior texts agree (185's own message said 12:00:00 AM
because midnight came first); only 185's derived rule is wrong Resolution: keep both; 326 canonical for the
refill rule: next_allowed = min(LastDecreaseDateTime + 3600 s, next 00:00 UTC) or parse 'Next decrease can be
made at'
