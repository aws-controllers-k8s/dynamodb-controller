<!-- generated: ack-api-quirks dynamodb 2026-10-09 model=2012-08-10 controller=34b85e6 -->
# DDB-TABLE-018: No-op UpdateTable re-sends are per-field: DP/BillingMode/TableClass 200; Stream and SSE Enabled:false re-sends ValidationException
_Full entry and notes of one finding; its summary entry is in
[table-streams-encryption-class.md](../table-streams-encryption-class.md). Generated from ack-api-quirks
`services/dynamodb` (model 2012-08-10, controller commit 34b85e6); edit the finding in the lab, not here._

## Finding

- <a id="ddb-table-018"></a>**DDB-TABLE-018** `idempotency` · impact high · unhandled (not handled in controller) · verified 2026-10-08
  **No-op UpdateTable re-sends are per-field: DP/BillingMode/TableClass 200; Stream and SSE Enabled:false re-sends ValidationException**
  Outcome of UpdateTable re-sending the current value, field by field (OK(status) or error code):
  {'dp_true_resend_immediate': 'OK(ACTIVE)', 'dp_false_resend_immediate': 'ThrottlingException',
  'dp_false_resend_after_cooldown': 'OK(ACTIVE)', 'billing_ppr_resend': 'OK(UPDATING)',
  'tableclass_standard_resend': 'OK(UPDATING)', 'tableclass_ia_resend': 'OK(UPDATING)',
  'sse_enabled_false_resend': 'ValidationException', 'stream_disabled_resend_when_no_stream':
  'ValidationException', 'stream_resend_identical': 'ValidationException',
  'stream_disable_resend_after_disabled': 'ValidationException', 'ondemand_throughput_resend_same':
  'ThrottlingException', 'provisioned_throughput_on_ppr_table': 'ValidationException',
  'stream_change_viewtype_while_enabled': 'ValidationException', 'stream_enabled_true_without_viewtype':
  'ValidationException'}. Error messages: {'dp_false_resend_immediate': 'Deletion protection setting for table
  ackq-ba1bb7-err modified within the previous 15000 milliseconds. Please ', 'sse_enabled_false_resend': 'One
  or more parameter values were invalid: Table is already encrypted by default',
  'stream_disabled_resend_when_no_stream': 'Table has no stream to disable: TableName: ackq-ba1bb7-err',
  'stream_resend_identical': 'Table already has an enabled stream: TableName: ackq-ba1bb7-err',
  'stream_disable_resend_after_disabled': 'Table has no stream to disable: TableName: ackq-ba1bb7-err',
  'ondemand_throughput_resend_same': 'The rate of control plane requests made by this account is too high',
  'provisioned_throughput_on_ppr_table': 'One or more parameter values were invalid: Neither ReadCapacityUnits
  nor WriteCapacityUnits can be specified w', 'stream_change_viewtype_while_enabled': 'Table already has an
  enabled stream: TableName: ackq-ba1bb7-err', 'stream_enabled_true_without_viewtype': 'One or more parameter
  values were invalid: If stream is being enabled then UpdateViewType is required'}.
  - ACK: custom_update, compare.is_ignored+delta_pre_compare, one-per-reconcile · ops: UpdateTable · fields:
    DeletionProtectionEnabled, BillingMode, TableClass, SSESpecification, StreamSpecification,
    OnDemandThroughput
  - repro: On an ACTIVE table call UpdateTable with each field set to its current value
  - measurements: stream_enable_updating_s=4.05, stream_disable_updating_s=5.07, tableclass_updating_s=6.08
  - handling: not handled in the controller (as of commit 34b85e6)
  - related: [DDB-TABLE-177](../table-streams-encryption-class.md#ddb-table-177), [DDB-TABLE-019](../table-streams-encryption-class.md#ddb-table-019), [DDB-TABLE-156](../table-throughput-billing.md#ddb-table-156), [DDB-TABLE-057](../table-throughput-billing.md#ddb-table-057), [DDB-TABLE-365](../table-streams-encryption-class.md#ddb-table-365), [DDB-TABLE-183](../table-throughput-billing.md#ddb-table-183),
    [DDB-TABLE-358](../table-throughput-billing.md#ddb-table-358), [DDB-TABLE-283](../table-streams-encryption-class.md#ddb-table-283), [DDB-TABLE-024](../table-throughput-billing.md#ddb-table-024), [DDB-TABLE-038](../table-throughput-billing.md#ddb-table-038), [DDB-TABLE-039](../table-throughput-billing.md#ddb-table-039), [DDB-TABLE-056](../table-throughput-billing.md#ddb-table-056), [DDB-TABLE-059](../table-throughput-billing.md#ddb-table-059),
    [DDB-TABLE-433](../table-streams-encryption-class.md#ddb-table-433), [DDB-TABLE-050](../table-streams-encryption-class.md#ddb-table-050), [DDB-TABLE-035](../table-streams-encryption-class.md#ddb-table-035), [DDB-TABLE-002](../table-streams-encryption-class.md#ddb-table-002), [DDB-TABLE-029](../table-streams-encryption-class.md#ddb-table-029), [DDB-TABLE-040](../table-throughput-billing.md#ddb-table-040), [DDB-TABLE-036](../table-streams-encryption-class.md#ddb-table-036),
    [DDB-TABLE-052](../table-streams-encryption-class.md#ddb-table-052), [DDB-TABLE-284](../table-streams-encryption-class.md#ddb-table-284), [DDB-TABLE-180](../table-streams-encryption-class.md#ddb-table-180), [DDB-TABLE-141](../table-streams-encryption-class.md#ddb-table-141), [DDB-TABLE-082](../table-streams-encryption-class.md#ddb-table-082), [DDB-TABLE-078](../table-streams-encryption-class.md#ddb-table-078), [DDB-TABLE-065](../table-streams-encryption-class.md#ddb-table-065) ·
    evidence: table/error-taxonomy/missing-table-noop-update-dp

## Notes

H-T-069: compare DP (accepted?) vs TableClass/BillingMode/Stream (rejected?). The controller cannot rely on a
uniform no-op rule. OnDemandThroughput re-send result inconclusive: hit account-level ThrottlingException 'The
rate of control plane requests made by this account is too high' (shared account, other runners active).

Contradiction with [DDB-TABLE-052](../table-streams-encryption-class.md#ddb-table-052), [DDB-TABLE-284](../table-streams-encryption-class.md#ddb-table-284), [DDB-TABLE-365](../table-streams-encryption-class.md#ddb-table-365), [DDB-TABLE-180](../table-streams-encryption-class.md#ddb-table-180): 052 reports TableClass UPDATING
31.4 s; 284 (0.5 s polling) measured 4.09/3.58 s, 365 6.1/4.0 s, 180 6.07 s, 018 6.08 s. 284's notes reconcile
it: 052's number is 'elapsed_before_wait_s'/'total_updating_s_upper_bound' from coarse polling, not the switch
duration Resolution: keep both; 284 canonical for the duration; retitle 052

Contradiction with [DDB-TABLE-141](../table-streams-encryption-class.md#ddb-table-141), [DDB-TABLE-082](../table-streams-encryption-class.md#ddb-table-082), [DDB-TABLE-078](../table-streams-encryption-class.md#ddb-table-078), [DDB-TABLE-065](../table-streams-encryption-class.md#ddb-table-065): 018 title generalizes 'SSE
re-sends -> ValidationException' from its single Enabled:false cell ('Table is already encrypted by default');
141/082/078 show that re-sending {Enabled:true}, SSEType-only, alias or key-id returns 200, re-encrypts for
~21 s and burns one of the 4 daily SSE changes; only Enabled:false and the exact-ARN form are
ValidationException, and 065 shows an identical re-send during the SSE job is also 200 Resolution: keep all;
141/082 canonical for the SSE re-send rule; retitle 018
