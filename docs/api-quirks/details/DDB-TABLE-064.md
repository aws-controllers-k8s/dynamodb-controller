<!-- generated: ack-api-quirks dynamodb 2026-10-09 model=2012-08-10 controller=34b85e6 -->
# DDB-TABLE-064: PROVISIONED->PAY_PER_REQUEST keeps the table UPDATING ~129s while BillingModeSummary already says PAY_PER_REQUEST
_Full entry and notes of one finding; its summary entry is in
[table-throughput-billing.md](../table-throughput-billing.md). Generated from ack-api-quirks
`services/dynamodb` (model 2012-08-10, controller commit 34b85e6); edit the finding in the lab, not here._

## Finding

- <a id="ddb-table-064"></a>**DDB-TABLE-064** `async-state-machine` · impact high · handled · verified 2026-10-08
  **PROVISIONED->PAY_PER_REQUEST keeps the table UPDATING ~129s while BillingModeSummary already says PAY_PER_REQUEST**
  UpdateTable(BillingMode=PAY_PER_REQUEST) response: TableStatus=UPDATING, BillingModeSummary={'BillingMode':
  'PAY_PER_REQUEST'}, ProvisionedThroughput={'LastIncreaseDateTime': datetime.datetime(2026, 10, 8, 23, 8, 48,
  363000, tzinfo=tzlocal()), 'LastDecreaseDateTime': datetime.datetime(2026, 10, 8, 23, 8, 49, 404000,
  tzinfo=tzlocal()), 'NumberOfDecreasesToday': 0, 'ReadCapacityUnits': 0, 'WriteCapacityUnits': 0}.
  DescribeTable timeline of (status, billing, rcu, warm): [(0.01, {'status': 'UPDATING', 'billing':
  'PAY_PER_REQUEST', 'rcu': 0, 'warm': (2, 'ACTIVE')}), (128.79, {'status': 'ACTIVE', 'billing':
  'PAY_PER_REQUEST', 'rcu': 0, 'warm': (12000, 'ACTIVE')})]. After ACTIVE: {'BillingModeSummary':
  {'BillingMode': 'PAY_PER_REQUEST', 'LastUpdateToPayPerRequestDateTime': datetime.datetime(2026, 10, 8, 23,
  10, 58, 74000, tzinfo=tzlocal())}, 'ProvisionedThroughput': {'LastIncreaseDateTime': datetime.datetime(2026,
  10, 8, 23, 8, 48, 363000, tzinfo=tzlocal()), 'NumberOfDecreasesToday': 0, 'ReadCapacityUnits': 0,
  'WriteCapacityUnits': 0}, 'WarmThroughput': {'ReadUnitsPerSecond': 12000, 'WriteUnitsPerSecond': 4000,
  'Status': 'ACTIVE'}, 'OnDemandThroughput': None}. Immediate switch back to PROVISIONED -> OK ''. During the
  switch: DeleteTable -> ResourceInUseException, UpdateTable(DP) -> OK(UPDATING).
  - ACK: synced.when, requeue, terminal_codes · ops: UpdateTable, DescribeTable · fields: BillingMode,
    BillingModeSummary, ProvisionedThroughput
  - repro: PROVISIONED table -> UpdateTable(BillingMode=PAY_PER_REQUEST); poll DescribeTable 1/s; then
    UpdateTable(BillingMode=PROVISIONED)
  - measurements: billing_updating_s=128.78
  - handling: handled via `pkg/resource/table/hooks.go:306; pkg/resource/table/hooks.go:325-335; test/e2e/tests/test_table.py:37-42; test/e2e/tests/test_table.py:544-556`
  - related: [DDB-TABLE-056](../table-throughput-billing.md#ddb-table-056), [DDB-TABLE-062](../table-throughput-billing.md#ddb-table-062), [DDB-TABLE-067](../table-throughput-billing.md#ddb-table-067), [DDB-TABLE-183](../table-throughput-billing.md#ddb-table-183), [DDB-TABLE-369](../table-streams-encryption-class.md#ddb-table-369), [DDB-TABLE-452](../table-throughput-billing.md#ddb-table-452),
    [DDB-TABLE-055](../table-throughput-billing.md#ddb-table-055), [DDB-TABLE-433](../table-streams-encryption-class.md#ddb-table-433), [DDB-TABLE-156](../table-throughput-billing.md#ddb-table-156), [DDB-TABLE-057](../table-throughput-billing.md#ddb-table-057), [DDB-TABLE-025](../table-throughput-billing.md#ddb-table-025), [DDB-TABLE-058](../table-throughput-billing.md#ddb-table-058), [DDB-TABLE-033](../table-throughput-billing.md#ddb-table-033),
    [DDB-TABLE-060](../table-throughput-billing.md#ddb-table-060), [DDB-TABLE-370](../table-throughput-billing.md#ddb-table-370), [DDB-TABLE-284](../table-streams-encryption-class.md#ddb-table-284), [DDB-TABLE-179](../table-throughput-billing.md#ddb-table-179), [DDB-TABLE-066](../table-throughput-billing.md#ddb-table-066), [DDB-TABLE-371](../table-streams-encryption-class.md#ddb-table-371), [DDB-TABLE-140](../table-streams-encryption-class.md#ddb-table-140),
    [DDB-TABLE-180](../table-streams-encryption-class.md#ddb-table-180), [DDB-TABLE-177](../table-streams-encryption-class.md#ddb-table-177) · evidence: table/state-machine/billing-sse-warm-throughput

## Notes

H-T-012: compare billing_updating_s with throughput_updating_s=2.03. BillingModeSummary reflects the target
mode as soon as the response, before ACTIVE. H-T-012 CONFIRMED (128.8s vs 2.0s for a throughput change;
response and every poll already reported PAY_PER_REQUEST). SURPRISE: immediate switch back to PROVISIONED was
accepted (UPDATING 64.9s) and a second switch to PAY_PER_REQUEST 1 min later too (UPDATING 5.1s) - no 24h
cooldown was enforced on this fresh table.
