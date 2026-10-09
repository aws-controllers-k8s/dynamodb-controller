<!-- generated: ack-api-quirks dynamodb 2026-10-09 model=2012-08-10 controller=34b85e6 -->
# DDB-TABLE-174: GSI Create cannot share an UpdateTable with PT/stream/SSE/TableClass/DeletionProtection/Warm changes; OK with BillingMode or GSI Update
_Full entry and notes of one finding; its summary entry is in [table-indexes.md](../table-indexes.md).
Generated from ack-api-quirks `services/dynamodb` (model 2012-08-10, controller commit 34b85e6); edit the
finding in the lab, not here._

## Finding

- <a id="ddb-table-174"></a>**DDB-TABLE-174** `update-granularity` · impact high · handled · verified 2026-10-08
  **GSI Create cannot share an UpdateTable with PT/stream/SSE/TableClass/DeletionProtection/Warm changes; OK with BillingMode or GSI Update**
  On idle PROVISIONED tables, UpdateTable {GlobalSecondaryIndexUpdates:[Create gsi2] + X} fails with
  ValidationException for X = ProvisionedThroughput ('You cannot create or delete index while updating table
  IOPS'), StreamSpecification ('You cannot create or delete index while changing stream status'),
  DeletionProtectionEnabled ('DeletionProtection modification must be the only operation in the request'),
  SSESpecification ('Server-Side Encryption modification must be the only operation in the request'),
  TableClass ('TableClass modification must be the only operation in the request') and WarmThroughput ('Create
  global secondary index cannot be specified when updating WarmThroughput'); [Create gsi2, Delete gsi1] ->
  LimitExceededException 'Only 1 online index can be created or deleted simultaneously per table'. Accepted:
  [Create gsi2, Update gsi1 throughput] (both applied) and Create gsi2 + BillingMode=PAY_PER_REQUEST (table
  UPDATING 112 s for the switch while the index built; both applied). Each X alone was accepted on its own
  table.
  - ACK: one-per-reconcile, custom_update · ops: UpdateTable · fields: GlobalSecondaryIndexUpdates.Create,
    ProvisionedThroughput, StreamSpecification, SSESpecification, TableClass, DeletionProtectionEnabled,
    WarmThroughput, BillingMode
  - repro: UpdateTable TableName=T AttributeDefinitions=[pk,a] GlobalSecondaryIndexUpdates=[{Create gsi2}]
    DeletionProtectionEnabled=true
  - measurements: billing_switch_with_create_table_updating_s=112.5, gsi_create_total_s_min=506,
    gsi_create_total_s_max=996.6
  - handling: handled via `pkg/resource/table/hooks.go:220-304; test/e2e/tests/test_table.py:878-952; test/e2e/tests/test_table.py:724-729; test/e2e/tests/test_table.py:864-876`
  - related: [DDB-TABLE-433](../table-streams-encryption-class.md#ddb-table-433), [DDB-TABLE-156](../table-throughput-billing.md#ddb-table-156), [DDB-TABLE-358](../table-throughput-billing.md#ddb-table-358), [DDB-TABLE-056](../table-throughput-billing.md#ddb-table-056), [DDB-TABLE-199](../table-replicas.md#ddb-table-199), [DDB-TABLE-224](../table-replicas.md#ddb-table-224),
    [DDB-TABLE-162](../table-indexes.md#ddb-table-162), [DDB-TABLE-127](../table-indexes.md#ddb-table-127), [DDB-TABLE-175](../table-indexes.md#ddb-table-175), [DDB-TABLE-382](../table-streams-encryption-class.md#ddb-table-382), [DDB-TABLE-163](../table-subresources.md#ddb-table-163), [DDB-TABLE-286](../table-streams-encryption-class.md#ddb-table-286), [DDB-TABLE-159](../table-indexes.md#ddb-table-159),
    [DDB-TABLE-152](../table-indexes.md#ddb-table-152), [DDB-TABLE-380](../table-indexes.md#ddb-table-380) · evidence: table/mutation-matrix/gsi-combined-updates

## Notes

H-T-017 partially confirmed: table ProvisionedThroughput is rejected as predicted, but
DeletionProtectionEnabled and StreamSpecification are NOT accepted alongside a Create, while a BillingMode
switch IS. A reconciler must emit the GSI Create alone (or with GSI Updates / a billing switch) and defer
every other field to a later reconcile.
