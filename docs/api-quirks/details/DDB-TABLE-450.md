<!-- generated: ack-api-quirks dynamodb 2026-10-09 model=2012-08-10 controller=34b85e6 -->
# DDB-TABLE-450: Clobber matrix (3 jobs x 8 writes): only the SSE write resets TableStatus, only in a TableClass switch; Warm/DP/tag/TTL/PITR/policy never do
_Full entry and notes of one finding; its summary entry is in
[table-streams-encryption-class.md](../table-streams-encryption-class.md). Generated from ack-api-quirks
`services/dynamodb` (model 2012-08-10, controller commit 34b85e6); edit the finding in the lab, not here._

## Finding

- <a id="ddb-table-450"></a>**DDB-TABLE-450** `async-state-machine` · impact high · unhandled (not handled in controller) · verified 2026-10-09
  **Clobber matrix (3 jobs x 8 writes): only the SSE write resets TableStatus, only in a TableClass switch; Warm/DP/tag/TTL/PITR/policy never do**
  One fresh PAY_PER_REQUEST table per cell; job J at t0, write W at +0.1 s, reverse job J' at +1.0 s,
  DescribeTable every 0.25 s. TableClass switch (-> IA; indicator TableClassSummary): sse: W=OK (resp
  TableStatus ACTIVE), first ACTIVE 0.26s, job done 3.1s, J' LOST; dp: W=OK (resp TableStatus UPDATING), first
  ACTIVE 4.39s, job done 4.39s, J' rejected:ResourceInUseException; warm: W=OK (resp TableStatus UPDATING),
  first ACTIVE 3.62s, job done 3.36s, J' rejected:ResourceInUseException; odt: W=ResourceInUseException, first
  ACTIVE 3.9s, job done 3.9s, J' rejected:ResourceInUseException; tag: W=OK, first ACTIVE 4.4s, job done 4.4s,
  J' rejected:ResourceInUseException; ttl: W=OK, first ACTIVE 4.39s, job done 4.39s, J'
  rejected:ResourceInUseException; pitr: W=OK, first ACTIVE 3.09s, job done 3.09s, J'
  rejected:ResourceInUseException; policy: W=OK, first ACTIVE 3.35s, job done 3.35s, J'
  rejected:ResourceInUseException; none (control): UPDATING for the whole 45 s watch, TableClassSummary=IA
  appeared 46 s after the request (switch durations vary 3-46 s), J' rejected:ResourceInUseException. Stream
  enable (indicator DescribeStream.StreamStatus): sse: W=OK (resp TableStatus ACTIVE), first ACTIVE 4.23s, job
  done 4.23s, J' rejected:ResourceInUseException; dp: W=OK (resp TableStatus UPDATING), first ACTIVE 3.97s,
  job done 3.97s, J' rejected:ResourceInUseException; warm: W=OK (resp TableStatus UPDATING), first ACTIVE
  4.52s, job done 4.52s, J' rejected:ResourceInUseException; odt: W=ResourceInUseException, first ACTIVE 4.5s,
  job done 4.5s, J' rejected:ResourceInUseException; tag: W=OK, first ACTIVE 5.31s, job done 5.31s, J'
  rejected:ResourceInUseException; ttl: W=OK, first ACTIVE 3.69s, job done 3.69s, J'
  rejected:ResourceInUseException; pitr: W=OK, first ACTIVE 3.73s, job done 3.73s, J'
  rejected:ResourceInUseException; policy: W=OK, first ACTIVE 4.51s, job done 4.51s, J'
  rejected:ResourceInUseException; none: W=-, first ACTIVE 3.16s, job done 3.16s, J'
  rejected:ResourceInUseException. Billing switch PPR->PROVISIONED 1/1 (indicator BillingModeSummary, which
  flips to PROVISIONED at +0.1 s already): sse: W=OK at +1.28 s after one ThrottlingException retry (resp
  TableStatus ACTIVE), first ACTIVE 61.43s, J' LOST; dp: W=OK (resp TableStatus UPDATING), first ACTIVE
  86.73s, job done 0.13s, J' LOST; warm: W=OK (resp TableStatus UPDATING), first ACTIVE 107.98s, job done
  0.17s, J' LOST; odt: W=ResourceInUseException, first ACTIVE 64.47s, job done 0.13s, J' LOST; tag: W=OK,
  first ACTIVE 90.76s, job done 0.24s, J' LOST; ttl: W=OK, first ACTIVE 108.05s, job done 0.17s, J' LOST;
  pitr: W=OK, first ACTIVE 115.09s, job done 0.17s, J' LOST; policy: W=OK, first ACTIVE 160.63s, job done
  0.2s, J' LOST; none: W=-, first ACTIVE 118.1s, job done 0.11s, J' LOST. Write admissibility:
  OnDemandThroughput is ResourceInUseException during all three jobs ('OnDemandThroughput cannot be updated
  while BillingMode update is in progress' / TableClass / stream variants); SSE, DP, WarmThroughput,
  TagResource, TTL, PITR and PutResourcePolicy are accepted during all three. Only the SSE write's own
  UpdateTable response says TableStatus=ACTIVE; DP and Warm responses echo UPDATING. Only in the TableClass
  cell did DescribeTable follow the SSE response (ACTIVE at +0.26 s with the class still pending,
  [DDB-TABLE-287](../table-streams-encryption-class.md#ddb-table-287)); during the stream enable the SSE response said ACTIVE but DescribeTable kept UPDATING and
  the stream-disable J' was ResourceInUseException. Every bl cell including the no-write control shows J'
  (BillingMode=PAY_PER_REQUEST) accepted with 200 and lost - that is a property of the billing switch itself,
  see table/creative/billing-reversal-noop, not a clobber.
  - ACK: synced.when, one-per-reconcile, requeue, custom_update · ops: UpdateTable, TagResource,
    UpdateTimeToLive, UpdateContinuousBackups, PutResourcePolicy, DescribeTable · fields: TableStatus,
    TableClass, StreamSpecification, BillingMode, SSESpecification, WarmThroughput, DeletionProtectionEnabled
  - repro: UpdateTable(J); W at +0.1 s; UpdateTable(reverse of J) at +1.0 s; DescribeTable every 0.25 s until
    quiescent
  - measurements: cells=27, premature_active_cells=1, tc_updating_s_control=46, st_updating_s=[4.23, 3.97,
    4.52, 4.5, 5.31, 3.69, 3.73, 4.51, 3.16], bl_first_active_s=[61.43, 86.73, 107.98, 64.47, 90.76, 108.05,
    115.09, 160.63, 118.1]
  - handling: not handled in the controller (as of commit 34b85e6)
  - related: [DDB-TABLE-287](../table-streams-encryption-class.md#ddb-table-287), [DDB-TABLE-285](../table-streams-encryption-class.md#ddb-table-285), [DDB-TABLE-286](../table-streams-encryption-class.md#ddb-table-286), [DDB-TABLE-119](../table-streams-encryption-class.md#ddb-table-119), [DDB-TABLE-458](../table-indexes.md#ddb-table-458), [DDB-TABLE-462](../table-indexes.md#ddb-table-462),
    [DDB-TABLE-175](../table-indexes.md#ddb-table-175), [DDB-TABLE-117](../table-streams-encryption-class.md#ddb-table-117), [DDB-TABLE-234](../table-policy-kinesis-autoscaling.md#ddb-table-234), [DDB-TABLE-460](../table-policy-kinesis-autoscaling.md#ddb-table-460), [DDB-TABLE-435](../table-streams-encryption-class.md#ddb-table-435), [DDB-TABLE-121](../table-throughput-billing.md#ddb-table-121) · evidence:
    table/creative/clobber-matrix

## Notes

Generalizes [DDB-TABLE-287](../table-streams-encryption-class.md#ddb-table-287) across jobs and writes: the TableStatus reset is specific to the SSE write path x
TableClass job. The WarmThroughput increase is admitted during TableClass and stream jobs (not listed in
[DDB-TABLE-286](../table-streams-encryption-class.md#ddb-table-286)) and does not reset the status. The 9 'LOST' bl cells are explained by
table/creative/billing-reversal-noop.
