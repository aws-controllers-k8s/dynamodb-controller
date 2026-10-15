<!-- generated: ack-api-quirks dynamodb 2026-10-09 model=2012-08-10 controller=34b85e6 -->
# DDB-TABLE-369: Billing-mode switch UPDATING: PT/OnDemand/stream/Delete -> ResourceInUse, DP and Warm admitted; reverse switch 200 but lost (71.9 s / 2.0 s)
_Full entry and notes of one finding; its summary entry is in
[table-streams-encryption-class.md](../table-streams-encryption-class.md). Generated from ack-api-quirks
`services/dynamodb` (model 2012-08-10, controller commit 34b85e6); edit the finding in the lab, not here._

## Finding

- <a id="ddb-table-369"></a>**DDB-TABLE-369** `async-state-machine` · impact high · handled · verified 2026-10-09
  **Billing-mode switch UPDATING: PT/OnDemand/stream/Delete -> ResourceInUse, DP and Warm admitted; reverse switch 200 but lost (71.9 s / 2.0 s)**
  FRESH PAY_PER_REQUEST table, first switch to PROVISIONED 1/1: trigger 200 OK (TableStatus=UPDATING);
  UPDATING 71.9 s (timeline (TableStatus, BillingModeSummary, RCU): [(('UPDATING', 'PROVISIONED', 1), 71.87),
  (('ACTIVE', 'PROVISIONED', 1), None)]). Fired right after: billing_switch_back_ppr -> 200 OK
  (TableStatus=UPDATING) @+0.06s; pt_change_2 -> ResourceInUseException (HTTP 400) 'Attempt to change a
  resource which is still in use: Table IOPS are currently being updated. Table: ackq-90be14-upd2f' @+0.4s;
  warm_increase -> 200 OK (TableStatus=UPDATING) @+0.77s; odt_change -> ResourceInUseException (HTTP 400)
  'Attempt to change a resource which is still in use: OnDemandThroughput cannot be updated while BillingMode
  update is in progress' @+1.1s; stream_enable -> ResourceInUseException (HTTP 400) 'Attempt to change a
  resource which is still in use: Can't enable or disable stream while table IOPS are being updated. Table:
  ackq-90be14-upd2f' @+1.43s; delete_table -> ResourceInUseException (HTTP 400) 'Attempt to change a resource
  which is still in use: Table: ackq-90be14-upd2f is in the process of being updated.' @+1.75s; dp_toggle ->
  200 OK (TableStatus=UPDATING) @+2.09s. Mid-window: billing_switch_back_ppr -> 200 OK (TableStatus=UPDATING)
  @+8.5s; pt_change_2 -> ResourceInUseException (HTTP 400) 'Attempt to change a resource which is still in
  use: Table IOPS are currently being updated. Table: ackq-90be14-upd2f' @+8.83s; warm_increase -> 200 OK
  (TableStatus=UPDATING) @+9.19s; odt_change -> ResourceInUseException (HTTP 400) 'Attempt to change a
  resource which is still in use: OnDemandThroughput cannot be updated while BillingMode update is in
  progress' @+9.52s; stream_enable -> ResourceInUseException (HTTP 400) 'Attempt to change a resource which is
  still in use: Can't enable or disable stream while table IOPS are being updated. Table: ackq-90be14-upd2f'
  @+9.85s; delete_table -> ResourceInUseException (HTTP 400) 'Attempt to change a resource which is still in
  use: Table: ackq-90be14-upd2f is in the process of being updated.' @+10.19s; dp_toggle -> skipped (dp
  cooldown). State after: {'status': 'ACTIVE', 'billing': 'PROVISIONED', 'rcu': 1, 'wcu': 1, 'warm': (12000,
  4000, 'UPDATING'), 'odt': None, 'dp': True}. FRESH table back to PAY_PER_REQUEST: trigger 200 OK
  (TableStatus=UPDATING); UPDATING 2.0 s ([(('UPDATING', 'PAY_PER_REQUEST', 0), 2.02), (('ACTIVE',
  'PAY_PER_REQUEST', 0), None)]). Right after: billing_switch_back_provisioned -> ResourceInUseException (HTTP 400)
  'Attempt to change a resource which is still in use: Table IOPS are currently being updated. Table:
  ackq-90be14-upd2f' @+0.06s; billing_resend_ppr -> ResourceInUseException (HTTP 400) 'Attempt to change a
  resource which is still in use: Table IOPS are currently being updated. Table: ackq-90be14-upd2f' @+0.39s;
  pt_change_7 -> ResourceInUseException (HTTP 400) 'Attempt to change a resource which is still in use: Table
  IOPS are currently being updated. Table: ackq-90be14-upd2f' @+0.72s; odt_change -> ValidationException (HTTP 400)
  'One or more parameter values were invalid: MaxReadRequestUnits for OnDemandThroughput cannot be specified
  when the table BillingMode is PROVISIONED' @+1.05s; warm_increase -> 200 OK (TableStatus=UPDATING) @+1.41s;
  delete_table -> ResourceInUseException (HTTP 400) 'Attempt to change a resource which is still in use:
  Table: ackq-90be14-upd2f is in the process of being updated.' @+1.73s; dp_toggle -> 200 OK
  (TableStatus=UPDATING) @+2.07s. Mid-window: . State after: {'status': 'ACTIVE', 'billing':
  'PAY_PER_REQUEST', 'rcu': 0, 'wcu': 0, 'warm': (12000, 4000, 'UPDATING'), 'odt': None, 'dp': False}. Same
  table re-flipped later (PPR->PROVISIONED again): UPDATING only 2.0 s; t0 round: billing_switch_back_ppr ->
  200 OK (TableStatus=UPDATING) @+0.07s; pt_change_2 -> ResourceInUseException (HTTP 400) 'Attempt to change a
  resource which is still in use: Table IOPS are currently being updated. Table: ackq-90be14-upd2' @+0.4s;
  warm_increase -> 200 OK (TableStatu [truncated in evidence]
  - ACK: updateable.when, deletable.when, requeue, synced.when · ops: UpdateTable, DeleteTable, DescribeTable
    · fields: BillingMode, ProvisionedThroughput, WarmThroughput, OnDemandThroughput,
    DeletionProtectionEnabled, StreamSpecification
  - repro: PPR table; UpdateTable(BillingMode=PROVISIONED, PT 1/1); immediately
    UpdateTable(BillingMode=PAY_PER_REQUEST) / PT 2/2 / WarmThroughput+1000 / OnDemandThroughput / stream
    enable / DeleteTable / DP toggle; repeat at +8 s; then the reverse switch
  - measurements: fresh_ppr_to_provisioned_updating_s=71.9, fresh_provisioned_to_ppr_updating_s=2.0,
    reflip_ppr_to_provisioned_updating_s=2.0, reflip_provisioned_to_ppr_updating_s=0
  - handling: handled via `pkg/resource/table/hooks.go:81-84; pkg/resource/table/hooks.go:198-202`
  - related: [DDB-TABLE-063](../table-throughput-billing.md#ddb-table-063), [DDB-TABLE-064](../table-throughput-billing.md#ddb-table-064), [DDB-TABLE-119](../table-streams-encryption-class.md#ddb-table-119), [DDB-TABLE-002](../table-streams-encryption-class.md#ddb-table-002), [DDB-TABLE-156](../table-throughput-billing.md#ddb-table-156), [DDB-TABLE-056](../table-throughput-billing.md#ddb-table-056),
    [DDB-TABLE-062](../table-throughput-billing.md#ddb-table-062), [DDB-TABLE-067](../table-throughput-billing.md#ddb-table-067), [DDB-TABLE-183](../table-throughput-billing.md#ddb-table-183), [DDB-TABLE-452](../table-throughput-billing.md#ddb-table-452), [DDB-TABLE-055](../table-throughput-billing.md#ddb-table-055), [DDB-TABLE-433](../table-streams-encryption-class.md#ddb-table-433), [DDB-TABLE-057](../table-throughput-billing.md#ddb-table-057),
    [DDB-TABLE-001](../table.md#ddb-table-001), [DDB-TABLE-370](../table-throughput-billing.md#ddb-table-370), [DDB-TABLE-052](../table-streams-encryption-class.md#ddb-table-052), [DDB-TABLE-285](../table-streams-encryption-class.md#ddb-table-285), [DDB-TABLE-120](../table-streams-encryption-class.md#ddb-table-120), [DDB-TABLE-065](../table-streams-encryption-class.md#ddb-table-065), [DDB-TABLE-459](../table-throughput-billing.md#ddb-table-459),
    [DDB-TABLE-054](../table-streams-encryption-class.md#ddb-table-054), [DDB-TABLE-010](../table.md#ddb-table-010), [DDB-TABLE-069](../table-streams-encryption-class.md#ddb-table-069), [DDB-TABLE-066](../table-throughput-billing.md#ddb-table-066), [DDB-TABLE-284](../table-streams-encryption-class.md#ddb-table-284), [DDB-TABLE-334](../table-streams-encryption-class.md#ddb-table-334) · hypotheses:
    H-T-001, H-T-002 · evidence: table/state-machine/updating-second-mutation

## Notes

Confirms H-T-002 (DeleteTable during UPDATING -> ResourceInUseException 'is in the process of being updated')
and qualifies H-T-001: during a billing-mode UPDATING window the rejection is per-field, not blanket -
ProvisionedThroughput/OnDemandThroughput/stream are ResourceInUse ('Table IOPS are currently being updated' /
'OnDemandThroughput cannot be updated while BillingMode update is in progress'), DeletionProtectionEnabled and
WarmThroughput are admitted. SURPRISE: during PPR->PROVISIONED, UpdateTable(BillingMode=PAY_PER_REQUEST)
returns 200 with TableStatus=UPDATING and a TableDescription echoing PAY_PER_REQUEST/0/0, yet the table ends
PROVISIONED - the call is swallowed as a re-send of the pre-switch mode. The reverse (PROVISIONED->PPR then
BillingMode=PROVISIONED+PT) is ResourceInUse because the PT member triggers the IOPS check. OnDemandThroughput
error precedence: validated against the PRE-switch billing mode (ValidationException on a PROVISIONED table)
before the state check.
