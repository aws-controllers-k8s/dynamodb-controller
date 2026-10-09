<!-- generated: ack-api-quirks dynamodb 2026-10-09 model=2012-08-10 controller=34b85e6 -->
# DDB-TABLE-183: BillingMode flipped PROVISIONED->PAY_PER_REQUEST->PROVISIONED->PAY_PER_REQUEST within 4 minutes: no once-per-24h rejection
_Full entry and notes of one finding; its summary entry is in
[table-throughput-billing.md](../table-throughput-billing.md). Generated from ack-api-quirks
`services/dynamodb` (model 2012-08-10, controller commit 34b85e6); edit the finding in the lab, not here._

## Finding

- <a id="ddb-table-183"></a>**DDB-TABLE-183** `quota-limit` · impact medium · handled · verified 2026-10-09
  **BillingMode flipped PROVISIONED->PAY_PER_REQUEST->PROVISIONED->PAY_PER_REQUEST within 4 minutes: no once-per-24h rejection** (hypothesis refuted; behavior confirmed)
  Fresh PROVISIONED 1/1 table (DescribeTable shows no BillingModeSummary at all): UpdateTable
  BillingMode=PAY_PER_REQUEST -> 200, TableStatus UPDATING for 171.6 s; the UpdateTable response echoes
  ProvisionedThroughput 0/0 and a LastDecreaseDateTime stamp, BillingModeSummary {BillingMode:
  PAY_PER_REQUEST} without LastUpdateToPayPerRequestDateTime, which appears only once ACTIVE. Re-sending
  BillingMode=PAY_PER_REQUEST while already PPR -> 200 (no-op, status UPDATING in the response). UpdateTable
  back to PROVISIONED 1/1 two minutes later -> 200, UPDATING 60.5 s, BillingModeSummary keeps
  LastUpdateToPayPerRequestDateTime of the earlier switch. A second switch to PAY_PER_REQUEST one minute after
  that -> 200 again. No LimitExceededException or ValidationException at any point.
  - ACK: none, e2e-timing · ops: UpdateTable, DescribeTable · fields: BillingMode,
    BillingModeSummary.LastUpdateToPayPerRequestDateTime, ProvisionedThroughput
  - repro: CreateTable PROVISIONED 1/1; UpdateTable PAY_PER_REQUEST; wait ACTIVE; UpdateTable PROVISIONED 1/1;
    wait ACTIVE; UpdateTable PAY_PER_REQUEST
  - measurements: to_ppr_updating_s=171.61, back_to_provisioned_updating_s=60.53
  - handling: handled via `generator.yaml:7-9; templates/hooks/table/sdk_read_one_post_set_output.go.tpl:51-55; test/e2e/tests/test_table.py:558-575; test/e2e/tests/test_table.py:37-42; test/e2e/tests/test_table.py:544-556`
  - related: [DDB-TABLE-067](../table-throughput-billing.md#ddb-table-067), [DDB-TABLE-056](../table-throughput-billing.md#ddb-table-056), [DDB-TABLE-062](../table-throughput-billing.md#ddb-table-062), [DDB-TABLE-064](../table-throughput-billing.md#ddb-table-064), [DDB-TABLE-369](../table-streams-encryption-class.md#ddb-table-369), [DDB-TABLE-452](../table-throughput-billing.md#ddb-table-452),
    [DDB-TABLE-055](../table-throughput-billing.md#ddb-table-055), [DDB-TABLE-433](../table-streams-encryption-class.md#ddb-table-433), [DDB-TABLE-177](../table-streams-encryption-class.md#ddb-table-177), [DDB-TABLE-019](../table-streams-encryption-class.md#ddb-table-019), [DDB-TABLE-156](../table-throughput-billing.md#ddb-table-156), [DDB-TABLE-057](../table-throughput-billing.md#ddb-table-057), [DDB-TABLE-018](../table-streams-encryption-class.md#ddb-table-018),
    [DDB-TABLE-365](../table-streams-encryption-class.md#ddb-table-365), [DDB-TABLE-358](../table-throughput-billing.md#ddb-table-358), [DDB-TABLE-283](../table-streams-encryption-class.md#ddb-table-283), [DDB-TABLE-025](../table-throughput-billing.md#ddb-table-025), [DDB-TABLE-058](../table-throughput-billing.md#ddb-table-058), [DDB-TABLE-033](../table-throughput-billing.md#ddb-table-033), [DDB-TABLE-060](../table-throughput-billing.md#ddb-table-060),
    [DDB-TABLE-370](../table-throughput-billing.md#ddb-table-370), [DDB-TABLE-284](../table-streams-encryption-class.md#ddb-table-284), [DDB-TABLE-179](../table-throughput-billing.md#ddb-table-179), [DDB-TABLE-066](../table-throughput-billing.md#ddb-table-066), [DDB-TABLE-371](../table-streams-encryption-class.md#ddb-table-371), [DDB-TABLE-140](../table-streams-encryption-class.md#ddb-table-140), [DDB-TABLE-180](../table-streams-encryption-class.md#ddb-table-180) ·
    evidence: table/limits/provisioned-decrease-billing-flip

## Notes

Refutes H-T-046 (the once-per-24h billing-mode switch rule is not enforced in this account/region as of this
run). The expensive part is the duration: switching to PAY_PER_REQUEST kept the table UPDATING for ~3 minutes
versus ~2 s for a throughput change, so a controller flip-flopping billing mode pays minutes of UPDATING, not
an error. Note also that a PPR table reports ProvisionedThroughput {ReadCapacityUnits: 0, WriteCapacityUnits:
0, NumberOfDecreasesToday: 0} and a LastDecreaseDateTime set by the mode switch.
