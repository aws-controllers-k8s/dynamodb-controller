<!-- generated: ack-api-quirks dynamodb 2026-10-09 model=2012-08-10 controller=34b85e6 -->
# DDB-TABLE-165: UpdateTable response echoes the OLD throughput for a GSI Update (IndexStatus=UPDATING) but the new entry for a GSI Create
_Full entry and notes of one finding; its summary entry is in [table-indexes.md](../table-indexes.md).
Generated from ack-api-quirks `services/dynamodb` (model 2012-08-10, controller commit 34b85e6); edit the
finding in the lab, not here._

## Finding

- <a id="ddb-table-165"></a>**DDB-TABLE-165** `stale-response` · impact medium · partially handled · verified 2026-10-08
  **UpdateTable response echoes the OLD throughput for a GSI Update (IndexStatus=UPDATING) but the new entry for a GSI Create**
  UpdateTable [Update gsi1 1/1 -> 2/2, Update gsi2 1/1 -> 2/2] returned TableStatus=ACTIVE and both indexes
  with IndexStatus=UPDATING but ProvisionedThroughput still 1/1 (and 2/2 -> 2/3 returned 2/2); DescribeTable
  2-4 s later shows 2/2 with LastIncreaseDateTime. For a GSI Create the response already contains the new
  index with IndexStatus=CREATING, Backfilling=false, its ProvisionedThroughput, IndexArn, IndexSizeBytes=0
  and ItemCount=0, identical to an immediate DescribeTable; for a GSI Delete the response shows
  IndexStatus=DELETING. In a PAY_PER_REQUEST->PROVISIONED switch the response did show the new 50/50 for table
  and GSI.
  - ACK: synced.when, requeue · ops: UpdateTable, DescribeTable · fields:
    GlobalSecondaryIndexes.ProvisionedThroughput, GlobalSecondaryIndexes.IndexStatus
  - repro: UpdateTable GlobalSecondaryIndexUpdates=[Update gsi1 PT 2/2]; compare TableDescription with
    DescribeTable
  - handling: partially handled via `pkg/resource/table/hooks_global_secondary_indexes.go:84-131; pkg/resource/table/hooks_global_secondary_indexes.go:246-258` - see Handling gaps
  - related: [DDB-TABLE-148](../table-indexes.md#ddb-table-148), [DDB-TABLE-138](../table-indexes.md#ddb-table-138), [DDB-TABLE-149](../table-indexes.md#ddb-table-149), [DDB-TABLE-163](../table-subresources.md#ddb-table-163), [DDB-TABLE-458](../table-indexes.md#ddb-table-458), [DDB-TABLE-116](../table-subresources.md#ddb-table-116),
    [DDB-TABLE-361](../table-streams-encryption-class.md#ddb-table-361), [DDB-TABLE-375](../table-indexes.md#ddb-table-375), [DDB-TABLE-462](../table-indexes.md#ddb-table-462), [DDB-TABLE-155](../table-indexes.md#ddb-table-155) · evidence:
    table/mutation-matrix/gsi-update-granularity

## Notes

Confirms H-T-060 for GSI throughput updates: do not persist the UpdateTable response as observed state.

Partially handled in controller via: [GT-DDB-066](../service.md#gt-ddb-066) (controller hooks catalog entry): updateGSIs only sends IndexName + ProvisionedThroughput +
OnDemandThroughput (hooks_global_secondary_indexes.go:246-254) although equalGlobalSecondaryIndexes al (scorer
verdict: partial match)
