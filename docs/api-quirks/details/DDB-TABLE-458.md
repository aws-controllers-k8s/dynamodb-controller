<!-- generated: ack-api-quirks dynamodb 2026-10-09 model=2012-08-10 controller=34b85e6 -->
# DDB-TABLE-458: GSI-add resource allocation (UPDATING 27-56 s): SSE/DP/tag admitted, TableStatus not reset; WarmThroughput -> HTTP 500 'system maintenance'
_Full entry and notes of one finding; its summary entry is in [table-indexes.md](../table-indexes.md).
Generated from ack-api-quirks `services/dynamodb` (model 2012-08-10, controller commit 34b85e6); edit the
finding in the lab, not here._

## Finding

- <a id="ddb-table-458"></a>**DDB-TABLE-458** `error-code` · impact medium · unhandled (not handled in controller) · verified 2026-10-09
  **GSI-add resource allocation (UPDATING 27-56 s): SSE/DP/tag admitted, TableStatus not reset; WarmThroughput -> HTTP 500 'system maintenance'**
  1-item PAY_PER_REQUEST tables, UpdateTable(GSI Create) at t0 (response TableStatus=UPDATING,
  IndexStatus=CREATING, Backfilling=false), one write at +0.1 s, UpdateTable(StreamSpecification enable) at
  +1.0 s, DescribeTable every 0.25 s. Writes: SSESpecification KMS -> 200 with TableStatus=ACTIVE in its own
  response but DescribeTable kept UPDATING (no clobber; SSEDescription ENABLED at +22 s while still UPDATING);
  DeletionProtection -> 200 (response UPDATING); TagResource -> 200; OnDemandThroughput ->
  ResourceInUseException 'OnDemandThroughput cannot be updated while index creation is in progress for
  indexes: [gsi1]'; WarmThroughput 13000/5000 -> InternalServerError (HTTP 500) 'Table is under system
  maintenance, please try again later'. The stream enable at +1.0 s was ResourceInUseException "Can't change
  stream status when an index is being created" in all 6 cells. TableStatus returned to ACTIVE after 27-56 s
  with IndexStatus CREATING/Backfilling=true; the index became ACTIVE after 510-539 s in four cells and after
  1001-1003 s in the two cells that had received the SSE change or the (rejected) WarmThroughput call.
  - ACK: terminal_codes, requeue, synced.when · ops: UpdateTable, TagResource, DescribeTable · fields:
    GlobalSecondaryIndexUpdates, WarmThroughput, SSESpecification, OnDemandThroughput, StreamSpecification,
    TableStatus
  - repro: UpdateTable(GSI Create) on a 1-item table; UpdateTable(WarmThroughput 13000/5000) 0.1 s later ->
    HTTP 500; UpdateTable(SSESpecification) -> 200 but TableStatus stays UPDATING
  - measurements: updating_s=[27.16, 27.18, 28.16, 38.07, 42.31, 56.25], index_active_s=[510.59, 512.39,
    524.71, 538.62, 1001.31, 1003.3]
  - handling: not handled in the controller (as of commit 34b85e6)
  - related: [DDB-TABLE-148](../table-indexes.md#ddb-table-148), [DDB-TABLE-163](../table-subresources.md#ddb-table-163), [DDB-TABLE-175](../table-indexes.md#ddb-table-175), [DDB-TABLE-450](../table-streams-encryption-class.md#ddb-table-450), [DDB-TABLE-161](../table-subresources.md#ddb-table-161), [DDB-TABLE-138](../table-indexes.md#ddb-table-138),
    [DDB-TABLE-149](../table-indexes.md#ddb-table-149), [DDB-TABLE-116](../table-subresources.md#ddb-table-116), [DDB-TABLE-165](../table-indexes.md#ddb-table-165), [DDB-TABLE-166](../table-indexes.md#ddb-table-166), [DDB-TABLE-376](../table-indexes.md#ddb-table-376), [DDB-TABLE-286](../table-streams-encryption-class.md#ddb-table-286), [DDB-TABLE-462](../table-indexes.md#ddb-table-462),
    [DDB-TABLE-287](../table-streams-encryption-class.md#ddb-table-287), [DDB-TABLE-448](../service.md#ddb-table-448), [DDB-TABLE-456](../service.md#ddb-table-456), [DDB-TABLE-135](../table-indexes.md#ddb-table-135), [DDB-TABLE-154](../table-throughput-billing.md#ddb-table-154), [DDB-TABLE-128](../table-indexes.md#ddb-table-128), [DDB-TABLE-153](../table-indexes.md#ddb-table-153),
    [DDB-TABLE-375](../table-indexes.md#ddb-table-375), [DDB-TABLE-361](../table-streams-encryption-class.md#ddb-table-361), [DDB-TABLE-155](../table-indexes.md#ddb-table-155) · evidence: table/creative/clobber-gsi-warm

## Notes

The SSE path does not clobber the GSI job's UPDATING marker (it does clobber a TableClass switch,
[DDB-TABLE-287](../table-streams-encryption-class.md#ddb-table-287)/450). The HTTP 500 for a WarmThroughput change during index resource allocation is a state
conflict reported as a server error; a controller treating 5xx as retryable will simply retry, but one
treating it as terminal will fail the resource. The longer backfill in the SSE/Warm cells is a correlation
from one run each, not a proven cause.

Contradiction with [DDB-TABLE-448](../service.md#ddb-table-448): 448 (title + behavior) says every HTTP 500 seen is a deterministic,
permanent request-shape bug with exactly two codes / three messages (InternalFailure '', InternalServerError
'Internal server error' / KMS text); 458 observed InternalServerError 'Table is under system maintenance,
please try again later' for a table WarmThroughput change during GSI resource allocation - a state conflict
(the same request shape is valid on an idle table, 155) that clears when the index settles Resolution: keep
both; 448 stays the catalogue but must list 458's variant as a WAIT-class (retry-after-state-change) 500;
448's title corrected (title_fixes). Controller rule: a 5xx is permanent only for the {} / ghost-index shapes,
not for every 5xx

Contradiction with [DDB-TABLE-148](../table-indexes.md#ddb-table-148), [DDB-TABLE-116](../table-subresources.md#ddb-table-116): 148 attributes the ~16.5-min (990 s) GSI backfill to a
PAY_PER_REQUEST table vs 507-537 s on PROVISIONED; 458 measured 510-539 s on four PPR tables and 1001-1003 s
on two PPR tables, 116 468 s on a PPR table - the duration is bimodal (~8.5 min or ~16.5 min) and not a
billing-mode effect Resolution: keep both; 148's title range (7-16 min) stands, but drop the billing-mode
attribution when summarising; readiness timeouts must allow >=17 min regardless of BillingMode
