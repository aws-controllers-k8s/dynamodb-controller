<!-- generated: ack-api-quirks dynamodb 2026-10-09 model=2012-08-10 controller=34b85e6 -->
# DDB-TABLE-383: No-op UpdateTable re-sends are free: 20 same-value BillingMode/DP/TableClass calls in 0.4 s -> all 200, no throttle/UPDATING/quota/cooldown
_Full entry and notes of one finding; its summary entry is in
[table-streams-encryption-class.md](../table-streams-encryption-class.md). Generated from ack-api-quirks
`services/dynamodb` (model 2012-08-10, controller commit 34b85e6); edit the finding in the lab, not here._

## Finding

- <a id="ddb-table-383"></a>**DDB-TABLE-383** `idempotency` · impact medium · unhandled (not handled in controller) · verified 2026-10-09
  **No-op UpdateTable re-sends are free: 20 same-value BillingMode/DP/TableClass calls in 0.4 s -> all 200, no throttle/UPDATING/quota/cooldown**
  PAY_PER_REQUEST table. 20 back-to-back UpdateTable(BillingMode=PAY_PER_REQUEST) in 0.4 s: 20x 200 (response
  TableStatus=UPDATING every time), DescribeTable polled at 0.25 s for 8 s never left ACTIVE,
  BillingModeSummary.LastUpdateToPayPerRequestDateTime unchanged. A no-op BillingMode re-send immediately
  followed by a real StreamSpecification enable -> 200 (no ResourceInUseException), and a no-op BillingMode
  re-send issued while a real stream change is UPDATING -> 200. 20x
  UpdateTable(DeletionProtectionEnabled=<same>) in 0.4 s: 20x 200 and a REAL flip right after -> 200 (the 15 s
  cooldown is not armed by no-ops). 20x UpdateTable(TableClass=STANDARD) on a table with no TableClassSummary
  in 0.4 s: 20x 200, TableClassSummary stays absent, the 2-per-30-days budget is intact (IA then STANDARD
  accepted afterwards, 3rd rejected). Contrast: 20 identical UpdateContinuousBackups(PITR=true) in 0.2 s ->
  11x 200 + 9x ThrottlingException 'Rate exceeded', and the following disable was throttled too.
  - ACK: compare.is_ignored+delta_pre_compare, requeue · ops: UpdateTable, UpdateContinuousBackups,
    DescribeTable · fields: BillingMode, DeletionProtectionEnabled, TableClass,
    PointInTimeRecoverySpecification
  - repro: PPR table; for i in 1..20: UpdateTable BillingMode=PAY_PER_REQUEST; DescribeTable every 0.25 s;
    compare BillingModeSummary
  - measurements: noop_updatetable_burst_calls=20, noop_updatetable_burst_span_s=0.4,
    noop_updatetable_throttled=0, pitr_burst_calls=20, pitr_burst_span_s=0.2, pitr_burst_throttled=9
  - handling: not handled in the controller (as of commit 34b85e6)
  - related: [DDB-TABLE-156](../table-throughput-billing.md#ddb-table-156), [DDB-TABLE-177](../table-streams-encryption-class.md#ddb-table-177), [DDB-TABLE-019](../table-streams-encryption-class.md#ddb-table-019), [DDB-TABLE-283](../table-streams-encryption-class.md#ddb-table-283), [DDB-TABLE-053](../service.md#ddb-table-053), [DDB-TABLE-099](../service.md#ddb-table-099),
    [DDB-TABLE-015](../table-indexes.md#ddb-table-015), [DDB-TABLE-434](../service.md#ddb-table-434), [DDB-TABLE-435](../table-streams-encryption-class.md#ddb-table-435), [DDB-TABLE-445](../service.md#ddb-table-445), [DDB-TABLE-085](../table-subresources.md#ddb-table-085), [DDB-TABLE-091](../table-subresources.md#ddb-table-091), [DDB-TABLE-341](../table-subresources.md#ddb-table-341),
    [DDB-TABLE-147](../table-subresources.md#ddb-table-147), [DDB-TABLE-300](../table-policy-kinesis-autoscaling.md#ddb-table-300), [DDB-TABLE-207](../table-policy-kinesis-autoscaling.md#ddb-table-207), [DDB-TABLE-205](../table-policy-kinesis-autoscaling.md#ddb-table-205) · evidence: table/creative/noop-resend-storm

## Notes

Extends [DDB-TABLE-156](../table-throughput-billing.md#ddb-table-156)/177/019: the 'UPDATING' in the response of a no-op is purely cosmetic (never visible in
DescribeTable at 0.25 s resolution) and no-ops neither consume the TableClass budget nor arm the DP cooldown
nor count against the account control-plane throttle that [DDB-TABLE-053](../service.md#ddb-table-053) saw at ~1 real UpdateTable/s. A
level-triggered controller re-sending BillingMode/DP/TableClass every loop is therefore harmless on the
UpdateTable side, but the same pattern on the sub-resource APIs hits the documented 10/s 'Rate exceeded'
throttle - and in this run the throttled PITR disable before DeleteTable left PITR on, so the delete produced
an undeletable 35-day SYSTEM backup ([DDB-TABLE-015](../table-indexes.md#ddb-table-015)).
