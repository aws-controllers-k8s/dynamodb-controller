<!-- generated: ack-api-quirks dynamodb 2026-10-09 model=2012-08-10 controller=34b85e6 -->
# DDB-TABLE-307: ReplicaUpdates: unknown GSI name in a Create override is silently accepted; Delete/Update of a never-replica region -> ValidationException
_Full entry and notes of one finding; its summary entry is in [table-replicas.md](../table-replicas.md).
Generated from ack-api-quirks `services/dynamodb` (model 2012-08-10, controller commit 34b85e6); edit the
finding in the lab, not here._

## Finding

- <a id="ddb-table-307"></a>**DDB-TABLE-307** `error-code` · impact high · unhandled (not handled in controller) · verified 2026-10-09
  **ReplicaUpdates: unknown GSI name in a Create override is silently accepted; Delete/Update of a never-replica region -> ValidationException**
  Create eu-west-1 with GlobalSecondaryIndexes=[{IndexName:'nope'}] on a table without GSIs -> 200 (HTTP 200,
  4.1s) and the replica was created normally - the unknown index override is ignored, not validated.
  ReplicaUpdates=[Delete ca-central-1] (never a replica) and [Update ca-central-1 TableClassOverride] ->
  ValidationException 'Update global table operation failed because one or more replicas were not part of the
  global table. Please retry the request without these replicas: [ca-central-1].' (not
  ResourceNotFoundException). The remaining two checks in this run (alias key on an AWS-owned table;
  ProvisionedThroughputOverride on a PPR Create) were confounded by the eu-west-1 table still DELETING
  ('already existed as tables'); the PPR override was accepted (200) in
  table/error-taxonomy/replica-sync-validation and the alias case is rejected with 'KMSMasterKeyId must be
  specified for each replica' in table/mutation-matrix/replica-overrides.
  - ACK: terminal_codes, custom_update, compare.is_ignored+delta_pre_compare · ops: UpdateTable, DescribeTable
    · fields: ReplicaUpdates, Replicas.ProvisionedThroughputOverride
  - repro: UpdateTable ReplicaUpdates with each malformed action against a table with one ACTIVE replica
  - handling: not handled in the controller (as of commit 34b85e6)
  - related: [DDB-TABLE-203](../table-replicas.md#ddb-table-203), [DDB-TABLE-223](../table-replicas.md#ddb-table-223), [DDB-TABLE-225](../table-replicas.md#ddb-table-225), [DDB-TABLE-308](../table-replicas.md#ddb-table-308), [DDB-TABLE-294](../table-replicas.md#ddb-table-294), [DDB-TABLE-437](../table-throughput-billing.md#ddb-table-437),
    [DDB-TABLE-250](../table-replicas.md#ddb-table-250), [DDB-TABLE-252](../table-replicas.md#ddb-table-252), [DDB-TABLE-251](../table-replicas.md#ddb-table-251), [DDB-TABLE-321](../table-replicas.md#ddb-table-321), [DDB-TABLE-229](../table-global-tables.md#ddb-table-229) · hypotheses: H-R-014, H-R-024 ·
    evidence: table/dependencies/replica-prerequisites

## Notes

Refutes the GSI half of H-R-014 (no ValidationException for an unknown index) and the Create half for
ProvisionedThroughputOverride on PAY_PER_REQUEST (accepted). Confirms H-R-024 for non-replica Delete/Update
(ValidationException with the region list). A controller cannot rely on server validation of replica GSI
override names.

Contradiction with [DDB-TABLE-223](../table-replicas.md#ddb-table-223), [DDB-TABLE-203](../table-replicas.md#ddb-table-203), [DDB-TABLE-225](../table-replicas.md#ddb-table-225): 223's behavior text records delete_non_replica
-> 200 and unknown_gsi_override -> 'already existed as tables'; 307 (clean re-test) and 203 show Delete of a
never-replica region -> ValidationException 'not part of the global table' and an unknown GSI override -> 200
silently accepted Resolution: 223's own CAVEAT explains it: eu-west-1 had become a real replica mid-sequence,
so those entries measured a real delete and a duplicate Create. 307/203 canonical for those two cases; 223
stays canonical for self/invalid/opt-in/empty region, mixed actions and Delete from the replica endpoint
