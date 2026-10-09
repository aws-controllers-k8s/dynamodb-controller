<!-- generated: ack-api-quirks dynamodb 2026-10-09 model=2012-08-10 controller=34b85e6 -->
# DDB-TABLE-225: ReplicaUpdates.Create is rejected when a standalone table with the same name already exists in the target region
_Full entry and notes of one finding; its summary entry is in [table-replicas.md](../table-replicas.md).
Generated from ack-api-quirks `services/dynamodb` (model 2012-08-10, controller commit 34b85e6); edit the
finding in the lab, not here._

## Finding

- <a id="ddb-table-225"></a>**DDB-TABLE-225** `identity` · impact high · unhandled (not handled in controller) · verified 2026-10-09
  **ReplicaUpdates.Create is rejected when a standalone table with the same name already exists in the target region**
  With a non-replica table of the same name ACTIVE in us-east-1, UpdateTable ReplicaUpdates=[Create us-east-1]
  fails synchronously (~0.6s) with ValidationException 'Failed to create a the new replica of table with name:
  <name> because one or more replicas already existed as tables.' The same code/message is returned while that
  table is DELETING (so the controller cannot distinguish 'leftover table' from 'replica being torn down').
  The existing table is never adopted; the message does not name the region. The identical message is returned
  for a duplicate Create of an existing replica (after ACTIVE, and while the replica is DELETING).
  - ACK: terminal_codes, requeue · ops: UpdateTable, CreateTable · fields: ReplicaUpdates
  - repro: CreateTable X in us-east-1; CreateTable X (streams) in us-west-2; UpdateTable X
    ReplicaUpdates=[Create us-east-1]
  - handling: not handled in the controller (as of commit 34b85e6)
  - related: [DDB-TABLE-203](../table-replicas.md#ddb-table-203), [DDB-TABLE-223](../table-replicas.md#ddb-table-223), [DDB-TABLE-307](../table-replicas.md#ddb-table-307), [DDB-TABLE-308](../table-replicas.md#ddb-table-308), [DDB-TABLE-294](../table-replicas.md#ddb-table-294), [DDB-TABLE-437](../table-throughput-billing.md#ddb-table-437) ·
    hypotheses: H-R-025 · evidence: table/state-machine/replica-create-timeline

## Notes

Confirms H-R-025. The same message covers three situations (foreign same-name table, replica already present,
replica still DELETING in its region); a controller must DescribeTable in the target region to tell them apart
and treat it as retryable only in the DELETING case.

Contradiction with [DDB-TABLE-223](../table-replicas.md#ddb-table-223), [DDB-TABLE-307](../table-replicas.md#ddb-table-307), [DDB-TABLE-203](../table-replicas.md#ddb-table-203): 223's behavior text records delete_non_replica
-> 200 and unknown_gsi_override -> 'already existed as tables'; 307 (clean re-test) and 203 show Delete of a
never-replica region -> ValidationException 'not part of the global table' and an unknown GSI override -> 200
silently accepted Resolution: 223's own CAVEAT explains it: eu-west-1 had become a real replica mid-sequence,
so those entries measured a real delete and a duplicate Create. 307/203 canonical for those two cases; 223
stays canonical for self/invalid/opt-in/empty region, mixed actions and Delete from the replica endpoint
