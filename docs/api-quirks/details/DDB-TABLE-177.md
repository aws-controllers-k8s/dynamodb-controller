<!-- generated: ack-api-quirks dynamodb 2026-10-09 model=2012-08-10 controller=34b85e6 -->
# DDB-TABLE-177: No-op UpdateTable calls (TableClass=STANDARD when unset, same BillingMode) return TableStatus=UPDATING but DescribeTable stays ACTIVE
_Full entry and notes of one finding; its summary entry is in
[table-streams-encryption-class.md](../table-streams-encryption-class.md). Generated from ack-api-quirks
`services/dynamodb` (model 2012-08-10, controller commit 34b85e6); edit the finding in the lab, not here._

## Finding

- <a id="ddb-table-177"></a>**DDB-TABLE-177** `stale-response` · impact high · handled · verified 2026-10-08
  **No-op UpdateTable calls (TableClass=STANDARD when unset, same BillingMode) return TableStatus=UPDATING but DescribeTable stays ACTIVE**
  TableClass=STANDARD on a table with no TableClassSummary -> OK response={"TableStatus": "UPDATING",
  "TableClassSummary": "<absent>", "BillingModeSummary": {"BillingMode": "PAY_PER_REQUEST",
  "LastUpdateToPayPerRequestDateTime": "2026-10-08 23:33:00.069000+00:00"}}; describe
  transitions={"timed_out": true, "transitions": [{"at_s": 0.01, "value": {"TableStatus": "ACTIVE",
  "TableClassSummary": "<absent>", "BillingModeSummary": {"BillingMode": "PAY_PER_REQUEST",
  "LastUpdateToPayPerRequestDateTime": "2026-10-08 23:33:00.069000+00:00"}}}]}. BillingMode=PAY_PER_REQUEST
  (same) -> OK response={"TableStatus": "UPDATING", "TableClassSummary": "<absent>", "BillingModeSummary":
  {"BillingMode": "PAY_PER_REQUEST", "LastUpdateToPayPerRequestDateTime": "2026-10-08
  23:33:00.069000+00:00"}}; describe transitions={"timed_out": true, "transitions": [{"at_s": 0.01, "value":
  {"TableStatus": "ACTIVE", "TableClassSummary": "<absent>", "BillingModeSummary": {"BillingMode":
  "PAY_PER_REQUEST". TableClass=STANDARD again -> OK response={"TableStatus": "UPDATING", "TableClassSummary":
  "<absent>", "BillingModeSummary": {"BillingMode": "PAY_PER_REQUEST", "LastUpdateToPayPerRequestDateTime":
  "2026-10-08 23:33:00.069000+00:00"}}; describe transitions={"timed_out": true, "transitions": [{"at_s":
  0.01, "value": {"TableStatus". TableClassSummary afterwards: {"TableClassSummary": "<absent>"}.
  - ACK: compare.is_ignored+delta_pre_compare, synced.when, requeue · ops: UpdateTable, DescribeTable ·
    fields: TableClass, TableClassSummary, BillingMode, TableStatus
  - repro: PPR table never given a TableClass; UpdateTable TableClass=STANDARD; DescribeTable at 0.5s
    intervals
  - handling: handled via `generator.yaml:12; templates/hooks/table/sdk_read_one_post_set_output.go.tpl:46-50`
  - related: [DDB-TABLE-019](../table-streams-encryption-class.md#ddb-table-019), [DDB-TABLE-156](../table-throughput-billing.md#ddb-table-156), [DDB-TABLE-057](../table-throughput-billing.md#ddb-table-057), [DDB-TABLE-018](../table-streams-encryption-class.md#ddb-table-018), [DDB-TABLE-365](../table-streams-encryption-class.md#ddb-table-365), [DDB-TABLE-183](../table-throughput-billing.md#ddb-table-183),
    [DDB-TABLE-358](../table-throughput-billing.md#ddb-table-358), [DDB-TABLE-283](../table-streams-encryption-class.md#ddb-table-283), [DDB-TABLE-026](../table-streams-encryption-class.md#ddb-table-026), [DDB-TABLE-284](../table-streams-encryption-class.md#ddb-table-284), [DDB-TABLE-180](../table-streams-encryption-class.md#ddb-table-180), [DDB-TABLE-060](../table-throughput-billing.md#ddb-table-060), [DDB-TABLE-370](../table-throughput-billing.md#ddb-table-370),
    [DDB-TABLE-179](../table-throughput-billing.md#ddb-table-179), [DDB-TABLE-066](../table-throughput-billing.md#ddb-table-066), [DDB-TABLE-371](../table-streams-encryption-class.md#ddb-table-371), [DDB-TABLE-140](../table-streams-encryption-class.md#ddb-table-140), [DDB-TABLE-064](../table-throughput-billing.md#ddb-table-064), [DDB-TABLE-452](../table-throughput-billing.md#ddb-table-452) · evidence:
    table/response-fidelity/create-update-response

## Notes

UpdateTable's TableStatus is not a reliable signal that an asynchronous update started; and setting
TableClass=STANDARD never makes TableClassSummary appear, so a spec of STANDARD diffs forever against nil.

Contradiction with [DDB-TABLE-156](../table-throughput-billing.md#ddb-table-156), [DDB-TABLE-019](../table-streams-encryption-class.md#ddb-table-019), [DDB-TABLE-057](../table-throughput-billing.md#ddb-table-057), [DDB-TABLE-452](../table-throughput-billing.md#ddb-table-452): 156 title says the
PAY_PER_REQUEST re-send 'briefly puts the table into UPDATING'; its evidence (gsi-billing-throughput result
ppr_billing_resend_same) only has the response TableStatus=UPDATING. 019/177 show DescribeTable ACTIVE at once
and 057 measured 'UPDATING 0.0s'; 452 shows the same call is swallowed as a no-op even during an in-flight
switch Resolution: keep both; 177 canonical; retitle 156
