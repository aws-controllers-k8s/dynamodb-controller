<!-- generated: ack-api-quirks dynamodb 2026-10-09 model=2012-08-10 controller=34b85e6 -->
# DDB-TABLE-459: WarmThroughput job is last-writer-wins: a higher request 1 s into a running Warm update is accepted and applied; no write resets its Status
_Full entry and notes of one finding; its summary entry is in
[table-throughput-billing.md](../table-throughput-billing.md). Generated from ack-api-quirks
`services/dynamodb` (model 2012-08-10, controller commit 34b85e6); edit the finding in the lab, not here._

## Finding

- <a id="ddb-table-459"></a>**DDB-TABLE-459** `async-state-machine` · impact medium · unhandled (not handled in controller) · verified 2026-10-09
  **WarmThroughput job is last-writer-wins: a higher request 1 s into a running Warm update is accepted and applied; no write resets its Status**
  PAY_PER_REQUEST tables: UpdateTable(WarmThroughput 13000/5000) at t0 (TableStatus stays ACTIVE,
  WarmThroughput.Status=UPDATING, values still 12000/4000); write at +0.1 s (SSESpecification KMS -> 200 /
  DeletionProtection -> 200 / OnDemandThroughput 100/100 -> 200 / TagResource -> 200);
  UpdateTable(WarmThroughput 14000/6000) at +1.0 s -> 200 in all 5 cells (response Status UPDATING, old
  values). WarmThroughput.Status stayed UPDATING continuously and flipped to ACTIVE once, after 441-514 s,
  with the values of the SECOND request (14000/6000) in every cell including the no-write control. No write
  reset the Status early; the overlapping SSE change finished (ENABLED) at +82 s inside the Warm window.
  - ACK: synced.when, requeue, custom_update · ops: UpdateTable, DescribeTable · fields: WarmThroughput,
    SSESpecification, DeletionProtectionEnabled, OnDemandThroughput
  - repro: UpdateTable(WarmThroughput 13000/5000); 1 s later UpdateTable(WarmThroughput 14000/6000) -> 200;
    DescribeTable until WarmThroughput.Status=ACTIVE (~8 min): 14000/6000
  - measurements: warm_updating_s=[441.46, 451.46, 473.59, 493.76, 513.88]
  - handling: not handled in the controller (as of commit 34b85e6)
  - related: [DDB-TABLE-066](../table-throughput-billing.md#ddb-table-066), [DDB-TABLE-121](../table-throughput-billing.md#ddb-table-121), [DDB-TABLE-179](../table-throughput-billing.md#ddb-table-179), [DDB-TABLE-049](../table-throughput-billing.md#ddb-table-049), [DDB-TABLE-061](../table-throughput-billing.md#ddb-table-061), [DDB-TABLE-059](../table-throughput-billing.md#ddb-table-059),
    [DDB-TABLE-069](../table-streams-encryption-class.md#ddb-table-069), [DDB-TABLE-001](../table.md#ddb-table-001), [DDB-TABLE-002](../table-streams-encryption-class.md#ddb-table-002), [DDB-TABLE-370](../table-throughput-billing.md#ddb-table-370), [DDB-TABLE-369](../table-streams-encryption-class.md#ddb-table-369), [DDB-TABLE-052](../table-streams-encryption-class.md#ddb-table-052), [DDB-TABLE-285](../table-streams-encryption-class.md#ddb-table-285),
    [DDB-TABLE-120](../table-streams-encryption-class.md#ddb-table-120), [DDB-TABLE-065](../table-streams-encryption-class.md#ddb-table-065), [DDB-TABLE-054](../table-streams-encryption-class.md#ddb-table-054), [DDB-TABLE-010](../table.md#ddb-table-010) · evidence: table/creative/clobber-gsi-warm

## Notes

Extends [DDB-TABLE-066](../table-throughput-billing.md#ddb-table-066) (equal values accepted during a Warm update) with a real second change: it is accepted
and wins, so unlike the TableClass ([DDB-TABLE-287](../table-streams-encryption-class.md#ddb-table-287)) and billing ([DDB-TABLE-452](../table-throughput-billing.md#ddb-table-452)) cases there is no lost write
here. OnDemandThroughput was admitted during the Warm job (it is ResourceInUse during
TableClass/stream/billing/GSI jobs).
