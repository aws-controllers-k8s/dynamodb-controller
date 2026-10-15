<!-- generated: ack-api-quirks dynamodb 2026-10-09 model=2012-08-10 controller=34b85e6 -->
# DDB-TABLE-381: Two writers racing full ProvisionedThroughput structs: loser gets ResourceInUseException; its stale retry is a budget-burning decrease
_Full entry and notes of one finding; its summary entry is in
[table-throughput-billing.md](../table-throughput-billing.md). Generated from ack-api-quirks
`services/dynamodb` (model 2012-08-10, controller commit 34b85e6); edit the finding in the lab, not here._

## Finding

- <a id="ddb-table-381"></a>**DDB-TABLE-381** `quota-limit` · impact medium · unhandled (not handled in controller) · verified 2026-10-09
  **Two writers racing full ProvisionedThroughput structs: loser gets ResourceInUseException; its stale retry is a budget-burning decrease**
  PROVISIONED table at 6/6 with NumberOfDecreasesToday=4. Writer A: UpdateTable ProvisionedThroughput {20,6}
  -> 200 (TableStatus UPDATING ~1.0 s). Writer B 0.3 s later: {6,20} -> ResourceInUseException 'Attempt to
  change a resource which is still in use: Table IOPS are currently being updated. Table: <name>'. Final state
  {20,6}. B's retry after ACTIVE with the same stale struct -> LimitExceededException 'Provisioned throughput
  decreases are limited within a given UTC day ... Number of decreases today: 4' because re-sending the stale
  RCU=6 is a decrease of A's 20. With budget left, the retry would have silently reverted A's change.
  - ACK: requeue, custom_update · ops: UpdateTable · fields: ProvisionedThroughput
  - repro: PROVISIONED table; thread A UpdateTable PT {20,w}; thread B at +0.3 s UpdateTable PT {r,20}; wait
    ACTIVE; B re-sends
  - measurements: writer_b_delay_s=0.3, updating_window_s=1.01
  - handling: not handled in the controller (as of commit 34b85e6)
  - related: [DDB-TABLE-184](../table-throughput-billing.md#ddb-table-184), [DDB-TABLE-163](../table-subresources.md#ddb-table-163), [DDB-TABLE-006](../table.md#ddb-table-006), [DDB-TABLE-063](../table-throughput-billing.md#ddb-table-063), [DDB-TABLE-103](../service.md#ddb-table-103), [DDB-TABLE-005](../table.md#ddb-table-005),
    [DDB-TABLE-101](../service.md#ddb-table-101), [DDB-TABLE-104](../table.md#ddb-table-104), [DDB-TABLE-185](../table-throughput-billing.md#ddb-table-185), [DDB-TABLE-326](../table-throughput-billing.md#ddb-table-326) · evidence: table/creative/update-atomicity

## Notes

There is no optimistic concurrency on UpdateTable (no revision/ETag); the only protection is the ~1-2 s
UPDATING window. Because ProvisionedThroughput must carry both RCU and WCU, a controller that computes the
struct from a stale read and retries after ResourceInUseException will overwrite the other writer's dimension
and, if that is lower, consume a decrease. Re-read DescribeTable before every retry.
