<!-- generated: ack-api-quirks dynamodb 2026-10-09 model=2012-08-10 controller=34b85e6 -->
# DDB-TABLE-203: ReplicaUpdates duplicate taxonomy: Create existing / Delete non-member / Create local region are all ValidationException
_Full entry and notes of one finding; its summary entry is in [table-replicas.md](../table-replicas.md).
Generated from ack-api-quirks `services/dynamodb` (model 2012-08-10, controller commit 34b85e6); edit the
finding in the lab, not here._

## Finding

- <a id="ddb-table-203"></a>**DDB-TABLE-203** `error-code` · impact medium · unhandled (not handled in controller) · verified 2026-10-09
  **ReplicaUpdates duplicate taxonomy: Create existing / Delete non-member / Create local region are all ValidationException**
  ReplicaUpdates=[Create <existing member>] -> ValidationException "Failed to create a the new replica of
  table with name: '<name>' because one or more replicas already existed as tables." (sic); [Delete
  ca-central-1] (non-member) -> ValidationException "Update global table operation failed because one or more
  replicas were not part of the global table. Please retry the request without these replicas:
  [ca-central-1]."; [Create us-west-2] (the table's own region) -> ValidationException "Cannot add or delete
  the local region through ReplicaUpdates. Use CreateTable, DeleteTable, or UpdateTable as required." None of
  these use the ReplicaAlreadyExistsException / ReplicaNotFoundException codes declared for the legacy API.
  - ACK: terminal_codes, custom_update · ops: UpdateTable · fields: ReplicaUpdates
  - repro: EVENTUAL group -> UpdateTable ReplicaUpdates=[Create <member>]; [Delete <non-member>]; [Create
    <local region>]
  - handling: not handled in the controller (as of commit 34b85e6)
  - related: [DDB-TABLE-223](../table-replicas.md#ddb-table-223), [DDB-TABLE-307](../table-replicas.md#ddb-table-307), [DDB-TABLE-225](../table-replicas.md#ddb-table-225), [DDB-TABLE-308](../table-replicas.md#ddb-table-308), [DDB-TABLE-294](../table-replicas.md#ddb-table-294), [DDB-TABLE-437](../table-throughput-billing.md#ddb-table-437) ·
    hypotheses: H-R-001, H-R-042 · evidence: table/cross-region/mrec-replica-updates

## Notes

A controller diffing spec.replicas against status must pre-filter the local region and already-present members
or it will hit terminal ValidationExceptions; message text (not code) is the only way to tell these apart.

Contradiction with [DDB-TABLE-223](../table-replicas.md#ddb-table-223), [DDB-TABLE-307](../table-replicas.md#ddb-table-307), [DDB-TABLE-225](../table-replicas.md#ddb-table-225): 223's behavior text records delete_non_replica
-> 200 and unknown_gsi_override -> 'already existed as tables'; 307 (clean re-test) and 203 show Delete of a
never-replica region -> ValidationException 'not part of the global table' and an unknown GSI override -> 200
silently accepted Resolution: 223's own CAVEAT explains it: eu-west-1 had become a real replica mid-sequence,
so those entries measured a real delete and a duplicate Create. 307/203 canonical for those two cases; 223
stays canonical for self/invalid/opt-in/empty region, mixed actions and Delete from the replica endpoint
