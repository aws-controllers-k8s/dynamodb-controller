<!-- generated: ack-api-quirks dynamodb 2026-10-09 model=2012-08-10 controller=34b85e6 -->
# DDB-TABLE-361: UpdateTable response-echo matrix: SSE change answers with a Status-only stub, OnDemandThroughput is synchronous (new values)
_Full entry and notes of one finding; its summary entry is in
[table-streams-encryption-class.md](../table-streams-encryption-class.md). Generated from ack-api-quirks
`services/dynamodb` (model 2012-08-10, controller commit 34b85e6); edit the finding in the lab, not here._

## Finding

- <a id="ddb-table-361"></a>**DDB-TABLE-361** `stale-response` · impact medium · unhandled (not handled in controller) · verified 2026-10-09
  **UpdateTable response-echo matrix: SSE change answers with a Status-only stub, OnDemandThroughput is synchronous (new values)**
  Which UpdateTable mutations echo the requested value in the response and in DescribeTable at T+0 (new cells
  from this probe, known cells from related ids): OnDemandThroughput set {100,100} and change {200,200} ->
  response and T+0 Describe both show the NEW values with TableStatus=ACTIVE (synchronous, no UPDATING
  window). SSESpecification {Enabled:true} on an unencrypted table -> response TableDescription.SSEDescription
  is the stub {Status: UPDATING} with NO SSEType and NO KMSMasterKeyArn; DescribeTable returns the same stub
  for 7-9 s, then {UPDATING, KMS, <aws-managed ARN>} until ENABLED at ~22 s. SSESpecification {Enabled:false}
  -> response and Describe echo the OLD full description {UPDATING, KMS, old ARN} for ~8 s, then the stub
  {Status: UPDATING} for ~13 s, then SSEDescription disappears (~21 s). Known: DeletionProtection new/sync
  ([DDB-TABLE-017](../table-streams-encryption-class.md#ddb-table-017)), StreamSpecification new + UPDATING ([DDB-TABLE-180](../table-streams-encryption-class.md#ddb-table-180)), ProvisionedThroughput OLD
  ([DDB-TABLE-060](../table-throughput-billing.md#ddb-table-060)), WarmThroughput OLD + Status UPDATING ([DDB-TABLE-066](../table-throughput-billing.md#ddb-table-066)), TableClassSummary OLD
  ([DDB-TABLE-284](../table-streams-encryption-class.md#ddb-table-284)), GSI Update OLD / GSI Create new ([DDB-TABLE-165](../table-indexes.md#ddb-table-165)).
  - ACK: synced.when, compare.is_ignored+delta_pre_compare, requeue · ops: UpdateTable, DescribeTable ·
    fields: SSESpecification, SSEDescription.SSEType, SSEDescription.KMSMasterKeyArn, OnDemandThroughput
  - repro: PPR table -> UpdateTable SSESpecification{Enabled:true}; record response.SSEDescription and poll
    DescribeTable 1/s (stub {Status:UPDATING} for ~9 s). Then Enabled:false (old key echoed ~8 s, stub ~13 s,
    absent at ~21 s). Then UpdateTable OnDemandThroughput {100,100} and {200,200}: response == Describe(T+0) ==
    requested, TableStatus ACTIVE.
  - measurements: sse_enable_stub_s=9.1, sse_enable_total_s=22.3, sse_disable_old_key_echo_s=8.1,
    sse_disable_stub_s=13.2, sse_disable_total_s=21.3, odt_updating_window_s=0
  - handling: not handled in the controller (as of commit 34b85e6)
  - related: [DDB-TABLE-065](../table-streams-encryption-class.md#ddb-table-065), [DDB-TABLE-079](../table-streams-encryption-class.md#ddb-table-079), [DDB-TABLE-060](../table-throughput-billing.md#ddb-table-060), [DDB-TABLE-066](../table-throughput-billing.md#ddb-table-066), [DDB-TABLE-180](../table-streams-encryption-class.md#ddb-table-180), [DDB-TABLE-284](../table-streams-encryption-class.md#ddb-table-284),
    [DDB-TABLE-165](../table-indexes.md#ddb-table-165), [DDB-TABLE-017](../table-streams-encryption-class.md#ddb-table-017), [DDB-TABLE-375](../table-indexes.md#ddb-table-375), [DDB-TABLE-462](../table-indexes.md#ddb-table-462), [DDB-TABLE-458](../table-indexes.md#ddb-table-458), [DDB-TABLE-155](../table-indexes.md#ddb-table-155) · evidence:
    table/creative/xs-response-echo

## Notes

Extends [DDB-TABLE-065](../table-streams-encryption-class.md#ddb-table-065)/079 with the member-stripping sequence: a reconciler that maps SSEDescription.SSEType or
KMSMasterKeyArn into status/delta sees nil members (not just Status=UPDATING) for ~9-13 s in BOTH directions,
and sees the old key with Status=UPDATING for the first ~8 s of a disable. Treat
SSEDescription.Status==UPDATING as 'in progress' and do not compute an SSE delta until it is ENABLED/absent.
Cross-service seed: ElastiCache stale Modify echo of the old Durability.
