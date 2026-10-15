<!-- generated: ack-api-quirks dynamodb 2026-10-09 model=2012-08-10 controller=34b85e6 -->
# DDB-TABLE-057: Re-sending the current BillingMode / ProvisionedThroughput via UpdateTable: which combinations are no-ops vs ValidationException
_Full entry and notes of one finding; its summary entry is in
[table-throughput-billing.md](../table-throughput-billing.md). Generated from ack-api-quirks
`services/dynamodb` (model 2012-08-10, controller commit 34b85e6); edit the finding in the lab, not here._

## Finding

- <a id="ddb-table-057"></a>**DDB-TABLE-057** `idempotency` · impact high · handled · verified 2026-10-08
  **Re-sending the current BillingMode / ProvisionedThroughput via UpdateTable: which combinations are no-ops vs ValidationException**
  PROVISIONED 1/1 table: BillingMode=PROVISIONED alone -> ValidationException: 'One or more parameter values
  were invalid: ProvisionedThroughput must be specified when BillingMode is PROVISIONED'. PT 1/1 (same) ->
  ValidationException: 'The provisioned throughput for the table will not change. The requested value equals
  the current value. Current ReadCapacityUnits provisioned for the table: 1. Requested ReadCapacityUnits: 1.
  Current WriteCapacityUnits p'. BillingMode=PROVISIONED + PT 1/1 (same) -> ValidationException: 'The
  provisioned throughput for the table will not change. The requested value equals the current value. Current
  ReadCapacityUnits provisioned for the table: 1. Requested ReadCapacityUnits: 1. Current WriteCapacityUnits
  p'. PT 2/2 then PT 2/2 again -> ValidationException: 'The provisioned throughput for the table will not
  change. The requested value equals the current value. Current ReadCapacityUnits provisioned for the table: 2.
  Requested ReadCapacityUnits: 2. Current WriteCapacityUnits p'. BillingMode=PROVISIONED + PT 3/3 (change with
  mode) -> OK (response TableStatus=UPDATING, UPDATING 2.02s). After switching C to PROVISIONED:
  BillingMode+PT re-sent -> ValidationException: 'The provisioned throughput for the table will not change.
  The requested value equals the current value. Current ReadCapacityUnits provisioned for the table: 1.
  Requested ReadCapacityUnits: 1. Current WriteCapacityUnits p'; BillingMode alone -> ValidationException:
  'One or more parameter values were invalid: ProvisionedThroughput must be specified when BillingMode is
  PROVISIONED'. PPR table: BillingMode=PAY_PER_REQUEST re-sent -> OK (response TableStatus=UPDATING, UPDATING
  0.0s).
  - ACK: custom_update, compare.is_ignored+delta_pre_compare · ops: UpdateTable · fields: BillingMode,
    ProvisionedThroughput
  - repro: PROVISIONED table; UpdateTable with the same BillingMode and/or the same ProvisionedThroughput
  - handling: handled via `pkg/resource/table/hooks.go:338-432; generator.yaml:91-92`
  - related: [DDB-TABLE-369](../table-streams-encryption-class.md#ddb-table-369), [DDB-TABLE-452](../table-throughput-billing.md#ddb-table-452), [DDB-TABLE-156](../table-throughput-billing.md#ddb-table-156), [DDB-TABLE-064](../table-throughput-billing.md#ddb-table-064), [DDB-TABLE-177](../table-streams-encryption-class.md#ddb-table-177), [DDB-TABLE-019](../table-streams-encryption-class.md#ddb-table-019),
    [DDB-TABLE-018](../table-streams-encryption-class.md#ddb-table-018), [DDB-TABLE-365](../table-streams-encryption-class.md#ddb-table-365), [DDB-TABLE-183](../table-throughput-billing.md#ddb-table-183), [DDB-TABLE-358](../table-throughput-billing.md#ddb-table-358), [DDB-TABLE-283](../table-streams-encryption-class.md#ddb-table-283), [DDB-TABLE-024](../table-throughput-billing.md#ddb-table-024), [DDB-TABLE-038](../table-throughput-billing.md#ddb-table-038),
    [DDB-TABLE-039](../table-throughput-billing.md#ddb-table-039), [DDB-TABLE-056](../table-throughput-billing.md#ddb-table-056), [DDB-TABLE-059](../table-throughput-billing.md#ddb-table-059), [DDB-TABLE-433](../table-streams-encryption-class.md#ddb-table-433), [DDB-TABLE-058](../table-throughput-billing.md#ddb-table-058), [DDB-TABLE-060](../table-throughput-billing.md#ddb-table-060), [DDB-TABLE-370](../table-throughput-billing.md#ddb-table-370),
    [DDB-TABLE-061](../table-throughput-billing.md#ddb-table-061), [DDB-TABLE-063](../table-throughput-billing.md#ddb-table-063), [DDB-TABLE-164](../table-indexes.md#ddb-table-164) · evidence: table/mutation-matrix/billing-capacity

## Notes

Hypotheses: H-T-029.

Contradiction with [DDB-TABLE-156](../table-throughput-billing.md#ddb-table-156), [DDB-TABLE-019](../table-streams-encryption-class.md#ddb-table-019), [DDB-TABLE-177](../table-streams-encryption-class.md#ddb-table-177), [DDB-TABLE-452](../table-throughput-billing.md#ddb-table-452): 156 title says the
PAY_PER_REQUEST re-send 'briefly puts the table into UPDATING'; its evidence (gsi-billing-throughput result
ppr_billing_resend_same) only has the response TableStatus=UPDATING. 019/177 show DescribeTable ACTIVE at once
and 057 measured 'UPDATING 0.0s'; 452 shows the same call is swallowed as a no-op even during an in-flight
switch Resolution: keep both; 177 canonical; retitle 156
