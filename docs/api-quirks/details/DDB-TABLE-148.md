<!-- generated: ack-api-quirks dynamodb 2026-10-09 model=2012-08-10 controller=34b85e6 -->
# DDB-TABLE-148: GSI added via UpdateTable: TableStatus UPDATING only ~25-55s (resource allocation), then ACTIVE while the index backfills 7-16 min
_Full entry and notes of one finding; its summary entry is in [table-indexes.md](../table-indexes.md).
Generated from ack-api-quirks `services/dynamodb` (model 2012-08-10, controller commit 34b85e6); edit the
finding in the lab, not here._

## Finding

- <a id="ddb-table-148"></a>**DDB-TABLE-148** `async-state-machine` · impact high · handled · verified 2026-10-08
  **GSI added via UpdateTable: TableStatus UPDATING only ~25-55s (resource allocation), then ACTIVE while the index backfills 7-16 min**
  After UpdateTable GlobalSecondaryIndexUpdates[Create] on an EMPTY table the response shows
  TableStatus=UPDATING and the new index IndexStatus=CREATING, Backfilling=false. That state lasts 24-55 s
  (resource allocation); then TableStatus returns to ACTIVE while the index stays CREATING with
  Backfilling=true for the remaining 7-16 minutes (measured 537 s and 507 s on a PROVISIONED table, ~16.5 min
  on a PAY_PER_REQUEST table). CreateTable with GSIs behaves differently: TableStatus=CREATING with all
  indexes CREATING, and everything flips to ACTIVE together after 14-20 s. A GSI delete makes
  TableStatus=UPDATING / IndexStatus=DELETING for 5-6 s; a GSI throughput Update makes only
  IndexStatus=UPDATING (~2 s) with TableStatus=ACTIVE.
  - ACK: synced.when, requeue, e2e-timing · ops: UpdateTable, DescribeTable, CreateTable · fields:
    TableStatus, GlobalSecondaryIndexes.IndexStatus, GlobalSecondaryIndexes.Backfilling
  - repro: ACTIVE empty table; UpdateTable Create GSI; DescribeTable every 1s until IndexStatus=ACTIVE
  - measurements: resource_allocation_s_min=24.3, resource_allocation_s_max=55.0,
    gsi_create_total_s_provisioned=537.5, gsi_create_total_s_ppr_approx=990, create_table_with_2_gsis_s=16.2,
    gsi_delete_s=5.1, gsi_throughput_update_s=2.0
  - handling: handled via `pkg/resource/table/hooks_global_secondary_indexes.go:30-39; templates/hooks/table/sdk_read_one_post_set_output.go.tpl:1-28; test/e2e/tests/test_table.py:724-729; test/e2e/tests/test_table.py:864-876`
  - related: [DDB-TABLE-138](../table-indexes.md#ddb-table-138), [DDB-TABLE-149](../table-indexes.md#ddb-table-149), [DDB-TABLE-163](../table-subresources.md#ddb-table-163), [DDB-TABLE-458](../table-indexes.md#ddb-table-458), [DDB-TABLE-116](../table-subresources.md#ddb-table-116), [DDB-TABLE-165](../table-indexes.md#ddb-table-165),
    [DDB-TABLE-168](../table-indexes.md#ddb-table-168), [DDB-TABLE-169](../table-indexes.md#ddb-table-169), [DDB-TABLE-123](../table-indexes.md#ddb-table-123), [DDB-TABLE-134](../table-indexes.md#ddb-table-134) · evidence: table/state-machine/gsi-lifecycle

## Notes

Confirms H-T-003 (a reconciler gating on TableStatus alone sees an idle table for ~90% of a GSI build) and
H-T-014's 'all indexes ACTIVE when TableStatus flips' for CreateTable. Readiness = TableStatus ACTIVE AND
every IndexStatus ACTIVE.

Contradiction with [DDB-TABLE-116](../table-subresources.md#ddb-table-116): 116's title/notes claim insights on the CREATING GSI are
ResourceNotFoundException 'for the whole 7.8 min backfill' and label the TableStatus=UPDATING window
'backfill'; 116's own series shows RNF 'Index: gsi2 not found' only at t=1.7 s and at t=21.8 s (still
UPDATING) DescribeContributorInsights(index) -> 200 and Update -> ValidationException 'IndexStatus must be
ACTIVE to enable ContributorInsights'; per 148/138 the UPDATING window is resource allocation
(Backfilling=false), the backfill runs with TableStatus=ACTIVE Resolution: keep 116 (data is sound,
measurements 42 s / 468 s agree with 148) with the corrected title; 148 is canonical for phase naming; an
insights reconciler must treat RNF-on-index as transient and then wait for IndexStatus=ACTIVE

Contradiction with [DDB-TABLE-458](../table-indexes.md#ddb-table-458), [DDB-TABLE-116](../table-subresources.md#ddb-table-116): 148 attributes the ~16.5-min (990 s) GSI backfill to a
PAY_PER_REQUEST table vs 507-537 s on PROVISIONED; 458 measured 510-539 s on four PPR tables and 1001-1003 s
on two PPR tables, 116 468 s on a PPR table - the duration is bimodal (~8.5 min or ~16.5 min) and not a
billing-mode effect Resolution: keep both; 148's title range (7-16 min) stands, but drop the billing-mode
attribution when summarising; readiness timeouts must allow >=17 min regardless of BillingMode
