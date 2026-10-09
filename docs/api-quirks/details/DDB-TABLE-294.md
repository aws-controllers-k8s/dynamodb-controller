<!-- generated: ack-api-quirks dynamodb 2026-10-09 model=2012-08-10 controller=34b85e6 -->
# DDB-TABLE-294: ReplicaUpdates.Update: RegionName-only rejected; same-value TableClassOverride accepted (UPDATING ~35s); class reported per replica
_Full entry and notes of one finding; its summary entry is in [table-replicas.md](../table-replicas.md).
Generated from ack-api-quirks `services/dynamodb` (model 2012-08-10, controller commit 34b85e6); edit the
finding in the lab, not here._

## Finding

- <a id="ddb-table-294"></a>**DDB-TABLE-294** `normalization` · impact high · handled · verified 2026-10-09
  **ReplicaUpdates.Update: RegionName-only rejected; same-value TableClassOverride accepted (UPDATING ~35s); class reported per replica**
  Update{RegionName only} -> ValidationException 'There are no actions specified in the Replica Update Action
  of the request.' Update{TableClassOverride:STANDARD} while the replica is STANDARD -> 200, table UPDATING
  for 34s (same-value Update is not a free no-op). Update{TableClassOverride:STANDARD_INFREQUENT_ACCESS} ->
  200; after 34s A.Replicas[us-east-1].ReplicaTableClassSummary.TableClass=STANDARD_INFREQUENT_ACCESS and
  us-east-1's own TableClassSummary=STANDARD_INFREQUENT_ACCESS, while A's TableClassSummary stays absent
  (STANDARD) and B's Replicas[us-west-2].ReplicaTableClassSummary stays absent. The UpdateTable response does
  not yet show the new class. Re-sending IA -> 200 and 58s UPDATING. UpdateTable TableClass=STANDARD issued
  directly in us-east-1 -> 200; A then shows ReplicaTableClassSummary.TableClass=STANDARD explicitly (with
  LastUpdateDateTime) - i.e. default STANDARD is omitted but an explicitly set STANDARD is reported.
  UpdateTable TableClass=IA on the BASE with a replica -> LimitExceededException 'Limit exceeded for replica
  in us-east-1. Updates to TableClass are limited to 2 times in 30 day(s)': a base TableClass change is
  applied to every replica.
  - ACK: compare.is_ignored+delta_pre_compare, compare.nil_equals_zero_value, custom_update, terminal_codes ·
    ops: UpdateTable, DescribeTable · fields: ReplicaUpdates.Update.TableClassOverride,
    Replicas.ReplicaTableClassSummary, TableClassSummary, TableClass
  - repro: table with ACTIVE replica -> UpdateTable ReplicaUpdates=[{Update:{RegionName, TableClassOverride}}]
    -> DescribeTable both regions
  - handling: handled via `pkg/resource/table/hooks_replica_updates.go:423-463; pkg/resource/table/hooks_replica_updates.go:28-147; pkg/resource/table/hooks_replica_updates.go:195-254; pkg/resource/table/hooks_replica_updates.go:348-360`
  - related: [DDB-TABLE-203](../table-replicas.md#ddb-table-203), [DDB-TABLE-223](../table-replicas.md#ddb-table-223), [DDB-TABLE-307](../table-replicas.md#ddb-table-307), [DDB-TABLE-225](../table-replicas.md#ddb-table-225), [DDB-TABLE-308](../table-replicas.md#ddb-table-308), [DDB-TABLE-437](../table-throughput-billing.md#ddb-table-437),
    [DDB-TABLE-250](../table-replicas.md#ddb-table-250), [DDB-TABLE-252](../table-replicas.md#ddb-table-252), [DDB-TABLE-251](../table-replicas.md#ddb-table-251), [DDB-TABLE-321](../table-replicas.md#ddb-table-321), [DDB-TABLE-229](../table-global-tables.md#ddb-table-229), [DDB-TABLE-296](../table-replicas.md#ddb-table-296), [DDB-TABLE-265](../table-replicas.md#ddb-table-265),
    [DDB-TABLE-295](../table-replicas.md#ddb-table-295), [DDB-TABLE-297](../table-replicas.md#ddb-table-297), [DDB-TABLE-322](../table-policy-kinesis-autoscaling.md#ddb-table-322), [DDB-TABLE-204](../table-replicas.md#ddb-table-204), [DDB-TABLE-261](../table-global-tables.md#ddb-table-261), [DDB-TABLE-226](../table-replicas.md#ddb-table-226), [DDB-TABLE-309](../table-replicas.md#ddb-table-309),
    [DDB-TABLE-221](../table-replicas.md#ddb-table-221) · hypotheses: H-R-012, H-R-015 · evidence: table/mutation-matrix/replica-overrides

## Notes

Qualifies H-R-012 (no-op Update IS rejected only when no action field is present; a same-value override is
accepted and costs an UPDATING cycle) and H-R-015 (ReplicaTableClassSummary absent for default STANDARD,
present once set explicitly - nil vs STANDARD must compare equal). The 2-per-30-days TableClass limit is
counted per replica and consumed by base-level changes too.
