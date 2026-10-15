<!-- generated: ack-api-quirks dynamodb 2026-10-09 model=2012-08-10 controller=34b85e6 -->
# DDB-TABLE-160: Per-index and per-entry action rules in GlobalSecondaryIndexUpdates are ValidationExceptions; empty list is rejected
_Full entry and notes of one finding; its summary entry is in [table-indexes.md](../table-indexes.md).
Generated from ack-api-quirks `services/dynamodb` (model 2012-08-10, controller commit 34b85e6); edit the
finding in the lab, not here._

## Finding

- <a id="ddb-table-160"></a>**DDB-TABLE-160** `request-validation` · impact medium · handled · verified 2026-10-08
  **Per-index and per-entry action rules in GlobalSecondaryIndexUpdates are ValidationExceptions; empty list is rejected**
  [Delete gsi1, Create gsi1] (same name), [Update gsi2, Update gsi2] or [Delete gsi2, Update gsi2] ->
  ValidationException 'One or more parameter values were invalid: Only one global secondary index update per
  index is allowed simultaneously. Index: gsi1'. One entry carrying both Update and Delete -> 'Only one global
  secondary index action is allowed per GlobalSecondaryIndexUpdate object'; an empty entry {} -> 'One of
  GlobalSecondaryIndexUpdate.Update, GlobalSecondaryIndexUpdate.Create, GlobalSecondaryIndexUpdate.Delete must
  not be null'. GlobalSecondaryIndexUpdates=[] alone -> 'At least one of ProvisionedThroughput, BillingMode,
  UpdateStreamEnabled, GlobalSecondaryIndexUpdates, SSESpecification, ReplicaUpdates, ... is required' (same
  as an UpdateTable with no change); GlobalSecondaryIndexUpdates=[] together with DeletionProtectionEnabled ->
  'List of GlobalSecondaryIndexUpdates is empty'.
  - ACK: custom_update, terminal_codes · ops: UpdateTable · fields: GlobalSecondaryIndexUpdates
  - repro: UpdateTable GlobalSecondaryIndexUpdates=[] DeletionProtectionEnabled=true
  - handling: handled via `pkg/resource/table/hooks.go:220-304; test/e2e/tests/test_table.py:878-952`
  - related: [DDB-TABLE-015](../table-indexes.md#ddb-table-015), [DDB-TABLE-047](../table-throughput-billing.md#ddb-table-047), [DDB-TABLE-437](../table-throughput-billing.md#ddb-table-437), [DDB-TABLE-449](../service.md#ddb-table-449) · evidence:
    table/mutation-matrix/gsi-update-granularity

## Notes

Confirms H-T-018 (never send an empty list) and the same-name Delete+Create part of H-T-067 (must be two
calls, the second after the index entry is gone).

Contradiction with [DDB-TABLE-015](../table-indexes.md#ddb-table-015): per 359's notes: 015 says UpdateTable GlobalSecondaryIndexUpdates=[] is
treated as absent ('At least one of ...' ValidationException), 160 says it is rejected as 'List of
GlobalSecondaryIndexUpdates is empty'; 359 reconciles: absent only for the at-least-one-parameter check,
rejected (and the carrier change NOT applied) once any effective member is present Resolution: keep both; 359
is canonical
