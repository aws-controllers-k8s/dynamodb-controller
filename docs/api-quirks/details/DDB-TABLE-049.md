<!-- generated: ack-api-quirks dynamodb 2026-10-09 model=2012-08-10 controller=34b85e6 -->
# DDB-TABLE-049: WarmThroughput UpdateTable chain inside one in-flight increase: increase, partial struct, re-sends and 'back to 12000/4000' all 200
_Full entry and notes of one finding; its summary entry is in
[table-throughput-billing.md](../table-throughput-billing.md). Generated from ack-api-quirks
`services/dynamodb` (model 2012-08-10, controller commit 34b85e6); edit the finding in the lab, not here._

## Finding

- <a id="ddb-table-049"></a>**DDB-TABLE-049** `update-granularity` · impact medium · handled · verified 2026-10-08
  **WarmThroughput UpdateTable chain inside one in-flight increase: increase, partial struct, re-sends and 'back to 12000/4000' all 200**
  WarmThroughput 12001/4001 -> OK (changed ["WarmThroughput.Status"]); re-sent -> OK.
  {ReadUnitsPerSecond:12002} only -> OK (changed []); re-sent -> OK. back to 12000/4000 -> OK; re-sent -> OK.
  - ACK: custom_update, requeue · ops: UpdateTable, DescribeTable · fields: WarmThroughput
  - repro: PAY_PER_REQUEST table; UpdateTable WarmThroughput 12001/4001; again; {Read:12002}; 12000/4000
  - measurements: warm-up=0.0, warm-partial-read=0.0, warm-down-to-default=0.0
  - handling: handled via `pkg/resource/table/hooks.go:338-432; generator.yaml:91-92`
  - related: [DDB-TABLE-179](../table-throughput-billing.md#ddb-table-179), [DDB-TABLE-066](../table-throughput-billing.md#ddb-table-066), [DDB-TABLE-459](../table-throughput-billing.md#ddb-table-459), [DDB-TABLE-061](../table-throughput-billing.md#ddb-table-061), [DDB-TABLE-059](../table-throughput-billing.md#ddb-table-059), [DDB-TABLE-069](../table-streams-encryption-class.md#ddb-table-069),
    [DDB-TABLE-121](../table-throughput-billing.md#ddb-table-121) · evidence: table/mutation-matrix/stream-protection-throughput

## Notes

Contradiction with [DDB-TABLE-179](../table-throughput-billing.md#ddb-table-179), [DDB-TABLE-066](../table-throughput-billing.md#ddb-table-066), [DDB-TABLE-061](../table-throughput-billing.md#ddb-table-061), [DDB-TABLE-059](../table-throughput-billing.md#ddb-table-059): 049 title/behavior:
WarmThroughput 'back to 12000/4000 -> OK' (a decrease accepted); 179/066/061/059: any value below the current
WarmThroughput is ValidationException 'decreasing WarmThroughput is not supported'. 049's chain ran within
seconds of a 12001/4001 increase whose job was still UPDATING (effective value still 12000/4000, measurements
all 0.0 s), so 12000/4000 was equal-to-current, not a decrease (066: equal accepted; 459: in-flight second
request accepted) Resolution: keep both; 179 canonical for the decrease rule; retitle 049 (no real decrease
was tested)

Contradiction with [DDB-TABLE-066](../table-throughput-billing.md#ddb-table-066), [DDB-TABLE-179](../table-throughput-billing.md#ddb-table-179): 066 behavior: 'Re-send same after ACTIVE ->
ValidationException lower than current' vs 179: 'Re-send same -> OK (Status=UPDATING ~2 s)' and 049 're-sent
-> OK'. In 066 a second increase had been merged (final 14000/6000) so the re-sent first request (13000/5000)
was genuinely lower; 066's own notes state equal values are accepted Resolution: keep both; 179 canonical; 066
is already marked duplicate of 179
