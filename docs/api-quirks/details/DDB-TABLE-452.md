<!-- generated: ack-api-quirks dynamodb 2026-10-09 model=2012-08-10 controller=34b85e6 -->
# DDB-TABLE-452: Reversing an in-flight billing switch: PPR->PROV then 'back to PPR' = 200 but dropped; PROV->PPR then 'back to PROV' = ResourceInUse
_Full entry and notes of one finding; its summary entry is in
[table-throughput-billing.md](../table-throughput-billing.md). Generated from ack-api-quirks
`services/dynamodb` (model 2012-08-10, controller commit 34b85e6); edit the finding in the lab, not here._

## Finding

- <a id="ddb-table-452"></a>**DDB-TABLE-452** `stale-response` · impact high · unhandled (not handled in controller) · verified 2026-10-09
  **Reversing an in-flight billing switch: PPR->PROV then 'back to PPR' = 200 but dropped; PROV->PPR then 'back to PROV' = ResourceInUse**
  Table A (created PAY_PER_REQUEST): UpdateTable(PROVISIONED 1/1) -> UPDATING; while UPDATING: +1 s
  BillingMode=PAY_PER_REQUEST -> OK (resp UPDATING/PAY_PER_REQUEST rcu=0); +3 s PROVISIONED 2/2 ->
  ResourceInUseException; +6 s PAY_PER_REQUEST -> OK (resp UPDATING/PAY_PER_REQUEST rcu=0). ACTIVE after 92.23
  s; state 60 s later: {"t_s": 92.23, "status": "ACTIVE", "bm": "PROVISIONED", "bm_ts": "2026-10-09
  05:57:32.214000+00:00", "rcu": 1, "wcu": 1, "warm": "12000/4000"}. Retry PAY_PER_REQUEST once ACTIVE -> OK,
  timeline [{"t_s": 0.04, "status": "UPDATING", "bm": "PAY_PER_REQUEST", "bm_ts": "2026-10-09
  05:57:32.214000+00:00", "rcu": 0, "wcu": 0, "warm": "12000/4000"}, {"t_s": 4.13, "status": "UPDATING", "bm":
  "PAY_PER_REQUEST", "bm_ts": "2026-10-09 06:00:21.474000+00:00", "rcu": 0, "wcu": 0, "warm": "12000/4000"},
  {"t_s": 4.64, "status": "ACTIVE", "bm": "PAY_PER_REQUEST", "bm_ts": "2026-10-09 06:00:21.474000+00:00",
  "rcu": 0, "wcu": 0, "warm": "12000/4000"}]. Table B (created PROVISIONED 1/1): UpdateTable(PAY_PER_REQUEST)
  -> UPDATING; while UPDATING: +1 s PROVISIONED 1/1 -> ResourceInUseException; +3 s PROVISIONED 2/2 ->
  ResourceInUseException; +6 s PROVISIONED 1/1 -> ResourceInUseException. ACTIVE after 128.63 s; state 60 s
  later: {"t_s": 128.63, "status": "ACTIVE", "bm": "PAY_PER_REQUEST", "bm_ts": "2026-10-09
  05:59:52.468000+00:00", "rcu": 0, "wcu": 0, "warm": "12000/4000"}. Retry PROVISIONED 1/1 once ACTIVE -> OK,
  timeline [{"t_s": 0.04, "status": "UPDATING", "bm": "PROVISIONED", "bm_ts": "2026-10-09
  05:59:52.468000+00:00", "rcu": 1, "wcu": 1, "warm": "12000/4000"}, {"t_s": 54.78, "status": "ACTIVE", "bm":
  "PROVISIONED", "bm_ts": "2026-10-09 05:59:52.468000+00:00", "rcu": 1, "wcu": 1, "warm": "12000/4000"}].
  Messages: {"diffpt": "Attempt to change a resource which is still in use: Table IOPS are currently being
  updated. Table: ackq-71fb57-br-b", "rev1": "Attempt to change a resource which is still in use: Table IOPS
  are currently being updated. Table: ackq-71fb57-br-b", "rev2": "Attempt to change a resource which is still
  in use: Table IOPS are currently being updated. Table: ackq-71fb57-br-b"}
  - ACK: synced.when, requeue, custom_update, one-per-reconcile · ops: UpdateTable, DescribeTable · fields:
    BillingMode, ProvisionedThroughput, BillingModeSummary, TableStatus
  - repro: CreateTable PPR; UpdateTable(BillingMode=PROVISIONED 1/1); 1 s later
    UpdateTable(BillingMode=PAY_PER_REQUEST); watch DescribeTable until ACTIVE + 60 s
  - handling: not handled in the controller (as of commit 34b85e6)
  - related: [DDB-TABLE-156](../table-throughput-billing.md#ddb-table-156), [DDB-TABLE-057](../table-throughput-billing.md#ddb-table-057), [DDB-TABLE-019](../table-streams-encryption-class.md#ddb-table-019), [DDB-TABLE-064](../table-throughput-billing.md#ddb-table-064), [DDB-TABLE-369](../table-streams-encryption-class.md#ddb-table-369), [DDB-TABLE-056](../table-throughput-billing.md#ddb-table-056),
    [DDB-TABLE-062](../table-throughput-billing.md#ddb-table-062), [DDB-TABLE-067](../table-throughput-billing.md#ddb-table-067), [DDB-TABLE-183](../table-throughput-billing.md#ddb-table-183), [DDB-TABLE-055](../table-throughput-billing.md#ddb-table-055), [DDB-TABLE-433](../table-streams-encryption-class.md#ddb-table-433), [DDB-TABLE-177](../table-streams-encryption-class.md#ddb-table-177) · evidence:
    table/creative/billing-reversal-noop

## Notes

Follow-up of table/creative/clobber-matrix where 9/9 PPR->PROVISIONED cells (incl. the no-write control)
accepted a reverse PAY_PER_REQUEST request at +1 s with 200 and finished PROVISIONED. The reversal is
evidently classified as a no-op re-send of the still-committed mode ([DDB-TABLE-156](../table-throughput-billing.md#ddb-table-156)) rather than queued or
rejected.

Contradiction with [DDB-TABLE-156](../table-throughput-billing.md#ddb-table-156), [DDB-TABLE-019](../table-streams-encryption-class.md#ddb-table-019), [DDB-TABLE-177](../table-streams-encryption-class.md#ddb-table-177), [DDB-TABLE-057](../table-throughput-billing.md#ddb-table-057): 156 title says the
PAY_PER_REQUEST re-send 'briefly puts the table into UPDATING'; its evidence (gsi-billing-throughput result
ppr_billing_resend_same) only has the response TableStatus=UPDATING. 019/177 show DescribeTable ACTIVE at once
and 057 measured 'UPDATING 0.0s'; 452 shows the same call is swallowed as a no-op even during an in-flight
switch Resolution: keep both; 177 canonical; retitle 156
