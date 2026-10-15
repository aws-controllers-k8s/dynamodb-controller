<!-- generated: ack-api-quirks dynamodb 2026-10-09 model=2012-08-10 controller=34b85e6 -->
# DDB-TABLE-159: One GSI Create/Delete per UpdateTable and per table at a time; violations are LimitExceededException, not ValidationException
_Full entry and notes of one finding; its summary entry is in [table-indexes.md](../table-indexes.md).
Generated from ack-api-quirks `services/dynamodb` (model 2012-08-10, controller commit 34b85e6); edit the
finding in the lab, not here._

## Finding

- <a id="ddb-table-159"></a>**DDB-TABLE-159** `update-granularity` · impact high · handled · verified 2026-10-08
  **One GSI Create/Delete per UpdateTable and per table at a time; violations are LimitExceededException, not ValidationException**
  GlobalSecondaryIndexUpdates with [Create g3, Create g4], [Create g3, Delete g1] or [Delete g1, Delete g2] in
  one call fail with LimitExceededException 'Subscriber limit exceeded: Only 1 online index can be created or
  deleted simultaneously per table' (HTTP 400, 1.7-2.8 s latency). The identical code and message are returned
  for a Create or Delete of another index while one index is CREATING (both phases) or DELETING. Two Update
  actions on two different indexes in one call are accepted (each index UPDATING ~2 s, TableStatus stays
  ACTIVE), and a GSI Update of another index is accepted while an index is CREATING or DELETING.
  - ACK: one-per-reconcile, requeue, terminal_codes · ops: UpdateTable · fields: GlobalSecondaryIndexUpdates
  - repro: ACTIVE table with gsi1,gsi2; UpdateTable GlobalSecondaryIndexUpdates=[Create gsi3, Create gsi4]
    (+AttributeDefinitions)
  - measurements: limit_exceeded_latency_ms=2403, two_updates_settle_s=4.0
  - handling: handled via `pkg/resource/table/hooks_global_secondary_indexes.go:30-39; templates/hooks/table/sdk_read_one_post_set_output.go.tpl:1-28; pkg/resource/table/hooks_global_secondary_indexes.go:180-199; pkg/resource/table/hooks_global_secondary_indexes.go:246-275; pkg/resource/table/hooks.go:226-238; pkg/resource/table/hooks_global_secondary_indexes.go:202-217`
  - related: [DDB-TABLE-150](../table-indexes.md#ddb-table-150), [DDB-TABLE-376](../table-indexes.md#ddb-table-376), [DDB-TABLE-163](../table-subresources.md#ddb-table-163), [DDB-TABLE-135](../table-indexes.md#ddb-table-135), [DDB-TABLE-382](../table-streams-encryption-class.md#ddb-table-382), [DDB-TABLE-174](../table-indexes.md#ddb-table-174),
    [DDB-TABLE-152](../table-indexes.md#ddb-table-152), [DDB-TABLE-380](../table-indexes.md#ddb-table-380), [DDB-TABLE-171](../table-throughput-billing.md#ddb-table-171), [DDB-TABLE-157](../table-indexes.md#ddb-table-157), [DDB-TABLE-154](../table-throughput-billing.md#ddb-table-154), [DDB-TABLE-170](../table-indexes.md#ddb-table-170), [DDB-TABLE-169](../table-indexes.md#ddb-table-169) ·
    evidence: table/mutation-matrix/gsi-update-granularity

## Notes

H-T-004 confirmed; H-T-016 partially: the multi-action rejection is LimitExceededException (same code as the
retryable 'one at a time' and the terminal capacity/decrease quotas), so the controller must match the message
'Only 1 online index can be created or deleted simultaneously' and treat it as 'requeue after the index
settles'.
