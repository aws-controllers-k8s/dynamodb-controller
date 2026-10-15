<!-- generated: ack-api-quirks dynamodb 2026-10-09 model=2012-08-10 controller=34b85e6 -->
# DDB-TABLE-061: WarmThroughput on PROVISIONED tables tracks the highest RCU/WCU ever provisioned, never decreases; 12000/4000 after switch to PPR
_Full entry and notes of one finding; its summary entry is in
[table-throughput-billing.md](../table-throughput-billing.md). Generated from ack-api-quirks
`services/dynamodb` (model 2012-08-10, controller commit 34b85e6); edit the finding in the lab, not here._

## Finding

- <a id="ddb-table-061"></a>**DDB-TABLE-061** `server-default` · impact high · unhandled (not handled in controller) · verified 2026-10-08
  **WarmThroughput on PROVISIONED tables tracks the highest RCU/WCU ever provisioned, never decreases; 12000/4000 after switch to PPR**
  PROVISIONED 1/1 -> WarmThroughput 1/1; after 2/2 -> 2/2; after 3/3 -> 3/3; after decreasing to 1/1 and then
  read 2/write 1 WarmThroughput stayed 3/3. UpdateTable WarmThroughput 1/1 -> ValidationException 'Requested
  ReadUnitsPerSecond for WarmThroughput for table is lower than current WarmThroughput, decreasing
  WarmThroughput is not supported'; 50/50 accepted (TableStatus stays ACTIVE, WarmThroughput.Status=UPDATING).
  After switching to PAY_PER_REQUEST WarmThroughput is 12000/4000 and stays 12000/4000 after switching back to
  PROVISIONED 1/1.
  - ACK: compare.is_ignored+delta_pre_compare, is_read_only, custom_update · ops: UpdateTable, DescribeTable ·
    fields: WarmThroughput.ReadUnitsPerSecond, WarmThroughput.WriteUnitsPerSecond
  - repro: PROVISIONED 1/1 table; UpdateTable PT 2/2, 3/3, 1/1; DescribeTable WarmThroughput after each;
    UpdateTable WarmThroughput 1/1
  - handling: not handled in the controller (as of commit 34b85e6)
  - related: [DDB-TABLE-068](../table-throughput-billing.md#ddb-table-068), [DDB-TABLE-179](../table-throughput-billing.md#ddb-table-179), [DDB-TABLE-066](../table-throughput-billing.md#ddb-table-066), [DDB-TABLE-459](../table-throughput-billing.md#ddb-table-459), [DDB-TABLE-049](../table-throughput-billing.md#ddb-table-049), [DDB-TABLE-059](../table-throughput-billing.md#ddb-table-059),
    [DDB-TABLE-069](../table-streams-encryption-class.md#ddb-table-069), [DDB-TABLE-121](../table-throughput-billing.md#ddb-table-121), [DDB-TABLE-032](../table-throughput-billing.md#ddb-table-032), [DDB-TABLE-028](../table-throughput-billing.md#ddb-table-028), [DDB-TABLE-033](../table-throughput-billing.md#ddb-table-033), [DDB-TABLE-039](../table-throughput-billing.md#ddb-table-039), [DDB-TABLE-045](../table-throughput-billing.md#ddb-table-045),
    [DDB-TABLE-058](../table-throughput-billing.md#ddb-table-058), [DDB-TABLE-060](../table-throughput-billing.md#ddb-table-060), [DDB-TABLE-370](../table-throughput-billing.md#ddb-table-370), [DDB-TABLE-057](../table-throughput-billing.md#ddb-table-057), [DDB-TABLE-063](../table-throughput-billing.md#ddb-table-063), [DDB-TABLE-164](../table-indexes.md#ddb-table-164) · evidence:
    table/mutation-matrix/billing-capacity

## Notes

Any spec.warmThroughput lower than the current server value is unsatisfiable; the controller must treat
WarmThroughput as monotonic.

Contradiction with [DDB-TABLE-049](../table-throughput-billing.md#ddb-table-049), [DDB-TABLE-179](../table-throughput-billing.md#ddb-table-179), [DDB-TABLE-066](../table-throughput-billing.md#ddb-table-066), [DDB-TABLE-059](../table-throughput-billing.md#ddb-table-059): 049 title/behavior:
WarmThroughput 'back to 12000/4000 -> OK' (a decrease accepted); 179/066/061/059: any value below the current
WarmThroughput is ValidationException 'decreasing WarmThroughput is not supported'. 049's chain ran within
seconds of a 12001/4001 increase whose job was still UPDATING (effective value still 12000/4000, measurements
all 0.0 s), so 12000/4000 was equal-to-current, not a decrease (066: equal accepted; 459: in-flight second
request accepted) Resolution: keep both; 179 canonical for the decrease rule; retitle 049 (no real decrease
was tested)
