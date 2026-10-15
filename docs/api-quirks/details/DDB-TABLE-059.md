<!-- generated: ack-api-quirks dynamodb 2026-10-09 model=2012-08-10 controller=34b85e6 -->
# DDB-TABLE-059: UpdateTable: OnDemandThroughput on PROVISIONED and ProvisionedThroughput on PPR rejected; WarmThroughput below current Warm value rejected
_Full entry and notes of one finding; its summary entry is in
[table-throughput-billing.md](../table-throughput-billing.md). Generated from ack-api-quirks
`services/dynamodb` (model 2012-08-10, controller commit 34b85e6); edit the finding in the lab, not here._

## Finding

- <a id="ddb-table-059"></a>**DDB-TABLE-059** `request-validation` · impact high · unhandled (not handled in controller) · verified 2026-10-08
  **UpdateTable: OnDemandThroughput on PROVISIONED and ProvisionedThroughput on PPR rejected; WarmThroughput below current Warm value rejected**
  PROVISIONED table: OnDemandThroughput -> ValidationException: 'One or more parameter values were invalid:
  MaxReadRequestUnits for OnDemandThroughput cannot be specified when the table BillingMode is PROVISIONED'.
  WarmThroughput 1/1 (below RCU/WCU) -> ValidationException: 'One or more parameter values were invalid:
  Requested ReadUnitsPerSecond for WarmThroughput for table is lower than current WarmThroughput, decreasing
  WarmThroughput is not supported'. WarmThroughput 50/50 (above) -> OK (response TableStatus=ACTIVE, UPDATING
  0.0s); Describe {"TableStatus": "ACTIVE", "BillingModeSummary": "<absent>", "ProvisionedThroughput":
  {"LastIncreaseDateTime": "2026-10-08 23:09:30.195000+00:00", "LastDecreaseDateTime": "2026-10-08
  23:09:29.159000+00:00", "NumberOfDecreasesToday": 1, "ReadCapacityUnits": 2, "WriteCapacityUnits": 1},
  "OnDemandThroughput": "<absent>", "WarmThroughput": {"ReadUnitsPerSecond": 3, "WriteUnitsPerSecond": 3,
  "Status": "UPDATING"}, "TableClassSummary": "<absent>"}. PPR table: ProvisionedThroughput 1/1 ->
  ValidationException: 'One or more parameter values were invalid: Neither ReadCapacityUnits nor
  WriteCapacityUnits can be specified when BillingMode is PAY_PER_REQUEST'. C after switch to PROVISIONED:
  OnDemandThroughput -> ValidationException: 'One or more parameter values were invalid: MaxReadRequestUnits
  for OnDemandThroughput cannot be specified when the table BillingMode is PROVISIONED'.
  - ACK: custom_update, compare.is_ignored+delta_pre_compare · ops: UpdateTable · fields: OnDemandThroughput,
    ProvisionedThroughput, WarmThroughput
  - repro: PROVISIONED table: UpdateTable OnDemandThroughput{100,100}; PPR table: UpdateTable
    ProvisionedThroughput{1,1}
  - handling: not handled in the controller (as of commit 34b85e6)
  - related: [DDB-TABLE-179](../table-throughput-billing.md#ddb-table-179), [DDB-TABLE-066](../table-throughput-billing.md#ddb-table-066), [DDB-TABLE-459](../table-throughput-billing.md#ddb-table-459), [DDB-TABLE-049](../table-throughput-billing.md#ddb-table-049), [DDB-TABLE-061](../table-throughput-billing.md#ddb-table-061), [DDB-TABLE-069](../table-streams-encryption-class.md#ddb-table-069),
    [DDB-TABLE-121](../table-throughput-billing.md#ddb-table-121), [DDB-TABLE-032](../table-throughput-billing.md#ddb-table-032), [DDB-TABLE-028](../table-throughput-billing.md#ddb-table-028), [DDB-TABLE-068](../table-throughput-billing.md#ddb-table-068), [DDB-TABLE-033](../table-throughput-billing.md#ddb-table-033), [DDB-TABLE-039](../table-throughput-billing.md#ddb-table-039), [DDB-TABLE-045](../table-throughput-billing.md#ddb-table-045),
    [DDB-TABLE-024](../table-throughput-billing.md#ddb-table-024), [DDB-TABLE-038](../table-throughput-billing.md#ddb-table-038), [DDB-TABLE-056](../table-throughput-billing.md#ddb-table-056), [DDB-TABLE-057](../table-throughput-billing.md#ddb-table-057), [DDB-TABLE-018](../table-streams-encryption-class.md#ddb-table-018), [DDB-TABLE-433](../table-streams-encryption-class.md#ddb-table-433) · evidence:
    table/mutation-matrix/billing-capacity

## Notes

Contradiction with [DDB-TABLE-049](../table-throughput-billing.md#ddb-table-049), [DDB-TABLE-179](../table-throughput-billing.md#ddb-table-179), [DDB-TABLE-066](../table-throughput-billing.md#ddb-table-066), [DDB-TABLE-061](../table-throughput-billing.md#ddb-table-061): 049 title/behavior:
WarmThroughput 'back to 12000/4000 -> OK' (a decrease accepted); 179/066/061/059: any value below the current
WarmThroughput is ValidationException 'decreasing WarmThroughput is not supported'. 049's chain ran within
seconds of a 12001/4001 increase whose job was still UPDATING (effective value still 12000/4000, measurements
all 0.0 s), so 12000/4000 was equal-to-current, not a decrease (066: equal accepted; 459: in-flight second
request accepted) Resolution: keep both; 179 canonical for the decrease rule; retitle 049 (no real decrease
was tested)
