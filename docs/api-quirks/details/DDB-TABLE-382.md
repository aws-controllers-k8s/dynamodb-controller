<!-- generated: ack-api-quirks dynamodb 2026-10-09 model=2012-08-10 controller=34b85e6 -->
# DDB-TABLE-382: UpdateTable is single-concern: 33/35 pairs of PT/ODT, Stream, DP, SSE, TableClass, Warm rejected on an idle table; only BillingMode combines
_Full entry and notes of one finding; its summary entry is in
[table-streams-encryption-class.md](../table-streams-encryption-class.md). Generated from ack-api-quirks
`services/dynamodb` (model 2012-08-10, controller commit 34b85e6); edit the finding in the lab, not here._

## Finding

- <a id="ddb-table-382"></a>**DDB-TABLE-382** `update-granularity` · impact high · unhandled (not handled in controller) · verified 2026-10-09
  **UpdateTable is single-concern: 33/35 pairs of PT/ODT, Stream, DP, SSE, TableClass, Warm rejected on an idle table; only BillingMode combines**
  Idle ACTIVE tables (PROVISIONED 10/10 and PAY_PER_REQUEST), every pair sent with both members valid on their
  own: 33 of 35 pairs fail synchronously with ValidationException and nothing is applied. Messages:
  'DeletionProtection modification must be the only operation in the request' (DP + anything), 'Server-Side
  Encryption modification must be the only operation in the request' (SSE + anything), 'TableClass
  modification must be the only operation in the request', 'WarmThroughput must be the only operation in the
  request', 'OnDemandThroughput can only be combined with BillingMode in an UpdateTable request' (ODT +
  Stream/Warm) and - for ProvisionedThroughput + StreamSpecification and for BillingMode=PROVISIONED(+PT) +
  StreamSpecification - 'You cannot modify stream status while updating table IOPS' although no IOPS update is
  running; PT + GlobalSecondaryIndexUpdates.Delete likewise -> 'You cannot create or delete index while
  updating table IOPS'. Accepted pairs: BillingMode=PAY_PER_REQUEST (switch from PROVISIONED, or no-op
  re-send) + StreamSpecification (both applied; switch UPDATING 147 s), OnDemandThroughput +
  BillingMode=PAY_PER_REQUEST (no-op), and the known BillingMode/PT/GSI-Update family.
  - ACK: one-per-reconcile, custom_update, requeue · ops: UpdateTable · fields: ProvisionedThroughput,
    OnDemandThroughput, StreamSpecification, DeletionProtectionEnabled, SSESpecification, TableClass,
    WarmThroughput, BillingMode
  - repro: CreateTable PROVISIONED 10/10 -> ACTIVE -> UpdateTable with {ProvisionedThroughput 11/11 +
    StreamSpecification enable}; repeat for each pair (toggle values derived from DescribeTable)
  - measurements: pairs_tested=35, pairs_rejected=33, switch_to_ppr_plus_stream_updating_s=147.5
  - handling: not handled in the controller (as of commit 34b85e6)
  - related: [DDB-TABLE-156](../table-throughput-billing.md#ddb-table-156), [DDB-TABLE-174](../table-indexes.md#ddb-table-174), [DDB-TABLE-224](../table-replicas.md#ddb-table-224), [DDB-TABLE-433](../table-streams-encryption-class.md#ddb-table-433), [DDB-TABLE-175](../table-indexes.md#ddb-table-175), [DDB-TABLE-163](../table-subresources.md#ddb-table-163),
    [DDB-TABLE-286](../table-streams-encryption-class.md#ddb-table-286), [DDB-TABLE-159](../table-indexes.md#ddb-table-159), [DDB-TABLE-152](../table-indexes.md#ddb-table-152), [DDB-TABLE-380](../table-indexes.md#ddb-table-380) · evidence: table/creative/update-pair-matrix,
    table/creative/update-atomicity

## Notes

Extends [DDB-TABLE-174](../table-indexes.md#ddb-table-174) (GSI Create + X) and [DDB-TABLE-156](../table-throughput-billing.md#ddb-table-156) (BillingMode + DP) to the full pairwise matrix of
non-index members: a controller that sends "everything that differs" in one UpdateTable can never succeed when
two concerns differ; it must issue one call per concern (DP, SSE, TableClass, Warm, Stream/IOPS) and wait for
each to settle. The two 'while updating table IOPS' / 'while changing stream status' messages are STATIC
combination rules phrased like transient state conflicts: a controller that treats them as retryable and
requeues the same request will loop forever. The only 2-concern calls that work are BillingMode-centred
(BillingMode+PT+GSI Update for PPR->PROVISIONED, BillingMode->PAY_PER_REQUEST + Stream, BillingMode +
OnDemandThroughput).
