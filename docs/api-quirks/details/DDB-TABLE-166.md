<!-- generated: ack-api-quirks dynamodb 2026-10-09 model=2012-08-10 controller=34b85e6 -->
# DDB-TABLE-166: DeletionProtectionEnabled does not protect GSIs: a GSI Delete on a protected table succeeds
_Full entry and notes of one finding; its summary entry is in [table-indexes.md](../table-indexes.md).
Generated from ack-api-quirks `services/dynamodb` (model 2012-08-10, controller commit 34b85e6); edit the
finding in the lab, not here._

## Finding

- <a id="ddb-table-166"></a>**DDB-TABLE-166** `first-sync-destructive` · impact high · partially handled · verified 2026-10-08
  **DeletionProtectionEnabled does not protect GSIs: a GSI Delete on a protected table succeeds**
  On a table with DeletionProtectionEnabled=true, UpdateTable GlobalSecondaryIndexUpdates[Delete gsi1] returns
  200, the index goes DELETING and is gone 5 s later; its attribute is pruned from AttributeDefinitions.
  Nothing in the API gates index deletion. Re-creating an index with the same name (different Projection, ALL)
  succeeded once the entry had disappeared and took 507 s to become ACTIVE on the empty table.
  - ACK: custom_update, one-per-reconcile · ops: UpdateTable · fields: DeletionProtectionEnabled,
    GlobalSecondaryIndexUpdates.Delete
  - repro: Table DeletionProtectionEnabled=true with gsi1; UpdateTable GlobalSecondaryIndexUpdates=[Delete
    gsi1]
  - measurements: gsi_delete_s=4.8, gsi_recreate_s=507.0
  - handling: partially handled via `pkg/resource/table/hooks_global_secondary_indexes.go:84-131; pkg/resource/table/hooks_global_secondary_indexes.go:246-258` - see Handling gaps
  - related: [DDB-TABLE-151](../table-indexes.md#ddb-table-151), [DDB-TABLE-149](../table-indexes.md#ddb-table-149), [DDB-TABLE-163](../table-subresources.md#ddb-table-163), [DDB-TABLE-376](../table-indexes.md#ddb-table-376), [DDB-TABLE-458](../table-indexes.md#ddb-table-458), [DDB-TABLE-286](../table-streams-encryption-class.md#ddb-table-286) ·
    evidence: table/mutation-matrix/gsi-update-granularity

## Notes

Confirms H-T-068 and H-T-067: adoption logic must treat an empty spec.globalSecondaryIndexes as 'unspecified'
or it will drop indexes.

Partially handled in controller via: [GT-DDB-066](../service.md#gt-ddb-066) (controller hooks catalog entry): updateGSIs only sends IndexName + ProvisionedThroughput +
OnDemandThroughput (hooks_global_secondary_indexes.go:246-254) although equalGlobalSecondaryIndexes al (scorer
verdict: partial match)
