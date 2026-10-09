<!-- generated: ack-api-quirks dynamodb 2026-10-09 model=2012-08-10 controller=34b85e6 -->
# DDB-TABLE-179: WarmThroughput increase is async (~6.5 min) with TableStatus ACTIVE: only WarmThroughput.Status=UPDATING signals it; decrease rejected
_Full entry and notes of one finding; its summary entry is in
[table-throughput-billing.md](../table-throughput-billing.md). Generated from ack-api-quirks
`services/dynamodb` (model 2012-08-10, controller commit 34b85e6); edit the finding in the lab, not here._

## Finding

- <a id="ddb-table-179"></a>**DDB-TABLE-179** `async-state-machine` · impact high · unhandled (not handled in controller) · verified 2026-10-08
  **WarmThroughput increase is async (~6.5 min) with TableStatus ACTIVE: only WarmThroughput.Status=UPDATING signals it; decrease rejected**
  Before: {"ReadUnitsPerSecond": 12000, "WriteUnitsPerSecond": 4000, "Status": "ACTIVE"}. UpdateTable
  WarmThroughput +1/+1 -> OK response={"TableStatus": "ACTIVE", "WarmThroughput": {"ReadUnitsPerSecond":
  12000, "WriteUnitsPerSecond": 4000, "Status": "UPDATING"}}; describe transitions={"timed_out": false,
  "transitions": [{"at_s": 0.01, "value": {"TableStatus": "ACTIVE", "WarmThroughput": {"ReadUnitsPerSecond":
  12000, "WriteUnitsPerSecond": 4000, "Status": "UPDATING"}}}, {"at_s": 388.78, "value": {"TableStatus":
  "ACTIVE", "WarmThroughput": {"ReadUnitsPerSecond": 12001, "WriteUnitsPerSecond": 4001, "Status":
  "ACTIVE"}}}]}. Re-send same -> OK response={"TableStatus": "ACTIVE", "WarmThroughput":
  {"ReadUnitsPerSecond": 12001, "WriteUnitsPerSecond": 4001, "Status": "UPDATING"}}; describe
  transitions={"timed_out": true, "transitions": [{"at_s": 0.01, "value": {"TableStatus": "ACTIVE",
  "WarmThroughput": {"ReadUnitsPerSecond": 12001, "Write. Decrease back -> ValidationException (HTTP 400):
  'One or more parameter values were invalid: Requested ReadUnitsPerSecond for WarmThroughput for table is
  lower than current WarmThroughput, decreasing WarmThroughput is not supported'. {WriteUnitsPerSecond:+2}
  only -> OK response={"TableStatus": "ACTIVE", "WarmThroughput": {"ReadUnitsPerSecond": 12001,
  "WriteUnitsPerSecond": 4001, "Status": "UPDATING"}}; describe transitions={"timed_out": false,
  "transitions": [{"at_s": 0.01, "value": {"TableStatus": "ACTIVE", "WarmThroughput": {"ReadUnitsPerSecond":
  12001, "WriteUnitsPerSecond": 4001, "Status": "UPDATING"}}}, {"at_s": 2.02, "value": {"TableStatus":
  "ACTIVE", "WarmThroughput": {"ReadUnitsPerSecond": 12001, "WriteUnitsPerSecond": 4002, "Status":
  "ACTIVE"}}}]}.
  - ACK: synced.when, requeue, compare.is_ignored+delta_pre_compare, custom_update · ops: UpdateTable,
    DescribeTable · fields: WarmThroughput.ReadUnitsPerSecond, WarmThroughput.WriteUnitsPerSecond,
    WarmThroughput.Status
  - repro: PPR table; UpdateTable WarmThroughput 12001/4001; poll DescribeTable WarmThroughput.Status; then
    12000/4000
  - measurements: warm_increase_completion_s=388.8, warm_increase_completion_s_first_run=500.9,
    warm_partial_write_increase_completion_s=2.0
  - handling: not handled in the controller (as of commit 34b85e6)
  - related: [DDB-TABLE-066](../table-throughput-billing.md#ddb-table-066), [DDB-TABLE-459](../table-throughput-billing.md#ddb-table-459), [DDB-TABLE-049](../table-throughput-billing.md#ddb-table-049), [DDB-TABLE-061](../table-throughput-billing.md#ddb-table-061), [DDB-TABLE-059](../table-throughput-billing.md#ddb-table-059), [DDB-TABLE-069](../table-streams-encryption-class.md#ddb-table-069),
    [DDB-TABLE-121](../table-throughput-billing.md#ddb-table-121), [DDB-TABLE-060](../table-throughput-billing.md#ddb-table-060), [DDB-TABLE-370](../table-throughput-billing.md#ddb-table-370), [DDB-TABLE-284](../table-streams-encryption-class.md#ddb-table-284), [DDB-TABLE-371](../table-streams-encryption-class.md#ddb-table-371), [DDB-TABLE-140](../table-streams-encryption-class.md#ddb-table-140), [DDB-TABLE-064](../table-throughput-billing.md#ddb-table-064),
    [DDB-TABLE-183](../table-throughput-billing.md#ddb-table-183), [DDB-TABLE-180](../table-streams-encryption-class.md#ddb-table-180), [DDB-TABLE-177](../table-streams-encryption-class.md#ddb-table-177), [DDB-TABLE-156](../table-throughput-billing.md#ddb-table-156) · evidence:
    table/response-fidelity/create-update-response

## Notes

A +1/+1 increase from 12000/4000 completed after 388.8s (and 500.9s in an earlier run); DescribeTable kept the
OLD values with Status=UPDATING until completion, TableStatus never left ACTIVE. Re-sending the same values is
accepted (200, Status=UPDATING briefly ~2s). Decrease -> ValidationException 'decreasing WarmThroughput is not
supported'. A single-member {WriteUnitsPerSecond:+1} completed in 2s.

Contradiction with [DDB-TABLE-049](../table-throughput-billing.md#ddb-table-049), [DDB-TABLE-066](../table-throughput-billing.md#ddb-table-066), [DDB-TABLE-061](../table-throughput-billing.md#ddb-table-061), [DDB-TABLE-059](../table-throughput-billing.md#ddb-table-059): 049 title/behavior:
WarmThroughput 'back to 12000/4000 -> OK' (a decrease accepted); 179/066/061/059: any value below the current
WarmThroughput is ValidationException 'decreasing WarmThroughput is not supported'. 049's chain ran within
seconds of a 12001/4001 increase whose job was still UPDATING (effective value still 12000/4000, measurements
all 0.0 s), so 12000/4000 was equal-to-current, not a decrease (066: equal accepted; 459: in-flight second
request accepted) Resolution: keep both; 179 canonical for the decrease rule; retitle 049 (no real decrease
was tested)

Contradiction with [DDB-TABLE-066](../table-throughput-billing.md#ddb-table-066), [DDB-TABLE-049](../table-throughput-billing.md#ddb-table-049): 066 behavior: 'Re-send same after ACTIVE ->
ValidationException lower than current' vs 179: 'Re-send same -> OK (Status=UPDATING ~2 s)' and 049 're-sent
-> OK'. In 066 a second increase had been merged (final 14000/6000) so the re-sent first request (13000/5000)
was genuinely lower; 066's own notes state equal values are accepted Resolution: keep both; 179 canonical; 066
is already marked duplicate of 179
