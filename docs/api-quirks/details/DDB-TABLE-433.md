<!-- generated: ack-api-quirks dynamodb 2026-10-09 model=2012-08-10 controller=34b85e6 -->
# DDB-TABLE-433: UpdateTable is one-logical-change-per-call: DP, SSE, TableClass, WarmThroughput each 'must be the only operation'; most pairs rejected
_Full entry and notes of one finding; its summary entry is in
[table-streams-encryption-class.md](../table-streams-encryption-class.md). Generated from ack-api-quirks
`services/dynamodb` (model 2012-08-10, controller commit 34b85e6); edit the finding in the lab, not here._

## Finding

- <a id="ddb-table-433"></a>**DDB-TABLE-433** `update-granularity` · impact high · unhandled (not handled in controller) · verified 2026-10-09
  **UpdateTable is one-logical-change-per-call: DP, SSE, TableClass, WarmThroughput each 'must be the only operation'; most pairs rejected**
  33 combinations of plain UpdateTable fields sent in ONE call on fresh tables, all answered synchronously
  (7-9 ms) and nothing partially applied. Rejected with ValidationException 'One or more parameter values were
  invalid: <X> modification must be the only operation in the request' for X = Server-Side Encryption,
  TableClass, DeletionProtection and 'WarmThroughput must be the only operation in the request' whenever
  SSESpecification / TableClass / DeletionProtectionEnabled / WarmThroughput is paired with anything
  (DP+STREAM, DP+ODT, DP+BillingMode, DP+PT, SSE+*, CLASS+*, WARM+*). OnDemandThroughput: 'OnDemandThroughput
  can only be combined with BillingMode in an UpdateTable request' (STREAM+ODT, WARM+ODT); StreamSpecification +
  ProvisionedThroughput or + BillingMode=PROVISIONED: 'You cannot modify stream status while updating table
  IOPS'. The only accepted pairs: BillingMode=PAY_PER_REQUEST + OnDemandThroughput (200, both applied) and
  BillingMode=PAY_PER_REQUEST + StreamSpecification (200, both applied), on a PROVISIONED table. Precedence of
  the exclusivity message when several exclusive fields are present: SSE > TableClass > DeletionProtection >
  WarmThroughput (e.g. DP+SSE names SSE, DP+CLASS names TableClass, WARM+DP names DP).
  - ACK: one-per-reconcile, custom_update, requeue · ops: UpdateTable · fields: DeletionProtectionEnabled,
    SSESpecification, TableClass, WarmThroughput, OnDemandThroughput, StreamSpecification, BillingMode,
    ProvisionedThroughput
  - repro: Fresh PPR (or PROVISIONED 1/1) table per combination; UpdateTable with the two (or 3-5) fields
    together; DescribeTable at T+0 to confirm nothing applied; for accepted calls wait ACTIVE and verify both
    changes.
  - measurements: combinations_tested=33, accepted=2, ppr_plus_odt_updating_s=42.3,
    ppr_plus_stream_updating_s=16.2, billing_to_provisioned_alone_updating_s=60.5
  - handling: not handled in the controller (as of commit 34b85e6)
  - related: [DDB-TABLE-156](../table-throughput-billing.md#ddb-table-156), [DDB-TABLE-174](../table-indexes.md#ddb-table-174), [DDB-TABLE-199](../table-replicas.md#ddb-table-199), [DDB-TABLE-224](../table-replicas.md#ddb-table-224), [DDB-TABLE-152](../table-indexes.md#ddb-table-152), [DDB-TABLE-056](../table-throughput-billing.md#ddb-table-056),
    [DDB-TABLE-062](../table-throughput-billing.md#ddb-table-062), [DDB-TABLE-064](../table-throughput-billing.md#ddb-table-064), [DDB-TABLE-067](../table-throughput-billing.md#ddb-table-067), [DDB-TABLE-183](../table-throughput-billing.md#ddb-table-183), [DDB-TABLE-369](../table-streams-encryption-class.md#ddb-table-369), [DDB-TABLE-452](../table-throughput-billing.md#ddb-table-452), [DDB-TABLE-055](../table-throughput-billing.md#ddb-table-055),
    [DDB-TABLE-024](../table-throughput-billing.md#ddb-table-024), [DDB-TABLE-038](../table-throughput-billing.md#ddb-table-038), [DDB-TABLE-039](../table-throughput-billing.md#ddb-table-039), [DDB-TABLE-057](../table-throughput-billing.md#ddb-table-057), [DDB-TABLE-059](../table-throughput-billing.md#ddb-table-059), [DDB-TABLE-018](../table-streams-encryption-class.md#ddb-table-018), [DDB-TABLE-358](../table-throughput-billing.md#ddb-table-358),
    [DDB-TABLE-162](../table-indexes.md#ddb-table-162), [DDB-TABLE-127](../table-indexes.md#ddb-table-127), [DDB-TABLE-382](../table-streams-encryption-class.md#ddb-table-382) · evidence: table/creative/xs-update-combos,
    table/creative/xs-update-combos-2

## Notes

Generalizes [DDB-TABLE-156](../table-throughput-billing.md#ddb-table-156) (DP + BillingMode) and 174/199/224 (GSI Create, ReplicaUpdates) to ALL plain fields:
a generated sdkUpdate that copies every changed spec field into one UpdateTable request fails whenever two of
{deletionProtection, sse, tableClass, warmThroughput, stream, provisioned/billing, onDemandThroughput} change
in the same reconcile. The controller must serialize: one UpdateTable per logical change, waiting for
TableStatus=ACTIVE (and SSEDescription.Status / WarmThroughput.Status not UPDATING) between them, with the
exceptions BillingMode->PPR + ODT and BillingMode->PPR + stream which may be batched. ElastiCache
ModifyServerlessCache / SNS SetTopicAttributes analogy.
