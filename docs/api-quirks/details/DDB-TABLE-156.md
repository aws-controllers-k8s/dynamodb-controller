<!-- generated: ack-api-quirks dynamodb 2026-10-09 model=2012-08-10 controller=34b85e6 -->
# DDB-TABLE-156: Re-sending BillingMode=PAY_PER_REQUEST on a PAY_PER_REQUEST table is a 200 no-op: response says UPDATING, DescribeTable stays ACTIVE
_Full entry and notes of one finding; its summary entry is in
[table-throughput-billing.md](../table-throughput-billing.md). Generated from ack-api-quirks
`services/dynamodb` (model 2012-08-10, controller commit 34b85e6); edit the finding in the lab, not here._

## Finding

- <a id="ddb-table-156"></a>**DDB-TABLE-156** `idempotency` · impact medium · handled · verified 2026-10-08
  **Re-sending BillingMode=PAY_PER_REQUEST on a PAY_PER_REQUEST table is a 200 no-op: response says UPDATING, DescribeTable stays ACTIVE**
  UpdateTable BillingMode=PAY_PER_REQUEST on a table already in PAY_PER_REQUEST returns 200 with
  TableStatus=UPDATING in the response (no 'will not change' error, unlike re-sending identical
  ProvisionedThroughput). Combined with DeletionProtectionEnabled in the same call it fails with
  ValidationException 'DeletionProtection modification must be the only operation in the request'.
  - ACK: compare.is_ignored+delta_pre_compare, custom_update · ops: UpdateTable · fields: BillingMode
  - repro: PAY_PER_REQUEST table; UpdateTable BillingMode=PAY_PER_REQUEST
  - handling: handled via `pkg/resource/table/hooks.go:338-432; generator.yaml:91-92`
  - related: [DDB-TABLE-369](../table-streams-encryption-class.md#ddb-table-369), [DDB-TABLE-452](../table-throughput-billing.md#ddb-table-452), [DDB-TABLE-057](../table-throughput-billing.md#ddb-table-057), [DDB-TABLE-064](../table-throughput-billing.md#ddb-table-064), [DDB-TABLE-177](../table-streams-encryption-class.md#ddb-table-177), [DDB-TABLE-019](../table-streams-encryption-class.md#ddb-table-019),
    [DDB-TABLE-018](../table-streams-encryption-class.md#ddb-table-018), [DDB-TABLE-365](../table-streams-encryption-class.md#ddb-table-365), [DDB-TABLE-183](../table-throughput-billing.md#ddb-table-183), [DDB-TABLE-358](../table-throughput-billing.md#ddb-table-358), [DDB-TABLE-283](../table-streams-encryption-class.md#ddb-table-283), [DDB-TABLE-060](../table-throughput-billing.md#ddb-table-060), [DDB-TABLE-370](../table-throughput-billing.md#ddb-table-370),
    [DDB-TABLE-284](../table-streams-encryption-class.md#ddb-table-284), [DDB-TABLE-179](../table-throughput-billing.md#ddb-table-179), [DDB-TABLE-066](../table-throughput-billing.md#ddb-table-066), [DDB-TABLE-371](../table-streams-encryption-class.md#ddb-table-371), [DDB-TABLE-140](../table-streams-encryption-class.md#ddb-table-140), [DDB-TABLE-180](../table-streams-encryption-class.md#ddb-table-180), [DDB-TABLE-433](../table-streams-encryption-class.md#ddb-table-433),
    [DDB-TABLE-056](../table-throughput-billing.md#ddb-table-056), [DDB-TABLE-174](../table-indexes.md#ddb-table-174), [DDB-TABLE-199](../table-replicas.md#ddb-table-199), [DDB-TABLE-224](../table-replicas.md#ddb-table-224), [DDB-TABLE-162](../table-indexes.md#ddb-table-162), [DDB-TABLE-127](../table-indexes.md#ddb-table-127) · evidence:
    table/mutation-matrix/gsi-billing-throughput

## Notes

Complements H-T-030: a controller that echoes billingMode on every reconcile causes a spurious UPDATING cycle.

Contradiction with [DDB-TABLE-019](../table-streams-encryption-class.md#ddb-table-019), [DDB-TABLE-177](../table-streams-encryption-class.md#ddb-table-177), [DDB-TABLE-057](../table-throughput-billing.md#ddb-table-057), [DDB-TABLE-452](../table-throughput-billing.md#ddb-table-452): 156 title says the
PAY_PER_REQUEST re-send 'briefly puts the table into UPDATING'; its evidence (gsi-billing-throughput result
ppr_billing_resend_same) only has the response TableStatus=UPDATING. 019/177 show DescribeTable ACTIVE at once
and 057 measured 'UPDATING 0.0s'; 452 shows the same call is swallowed as a no-op even during an in-flight
switch Resolution: keep both; 177 canonical; retitle 156
