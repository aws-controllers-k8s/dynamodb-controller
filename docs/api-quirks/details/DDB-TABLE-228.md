<!-- generated: ack-api-quirks dynamodb 2026-10-09 model=2012-08-10 controller=34b85e6 -->
# DDB-TABLE-228: Admissibility while a replica is DELETING, and A-entry-gone vs B-ResourceNotFound ordering
_Full entry and notes of one finding; its summary entry is in [table-replicas.md](../table-replicas.md).
Generated from ack-api-quirks `services/dynamodb` (model 2012-08-10, controller commit 34b85e6); edit the
finding in the lab, not here._

## Finding

- <a id="ddb-table-228"></a>**DDB-TABLE-228** `async-state-machine` · impact high · unhandled (not handled in controller) · verified 2026-10-09
  **Admissibility while a replica is DELETING, and A-entry-gone vs B-ResourceNotFound ordering**
  Issued 3-7s after ReplicaUpdates.Delete (A: UPDATING, Replicas=[us-east-1 ACTIVE]): Create eu-west-1 /
  duplicate Delete / DeletionProtectionEnabled -> ResourceInUseException; Create us-east-1 (re-add while
  deleting) -> ValidationException 'because one or more replicas already existed as tables'; TagResource,
  UpdateContinuousBackups, DeleteResourcePolicy, UpdateContributorInsights, DescribeTableReplicaAutoScaling ->
  200; UpdateTimeToLive -> ValidationException 'Time to live has been modified multiple times within a fixed
  interval' (TTL rate limit, not state). Timeline: A UPDATING[us-east-1 ACTIVE] + B UPDATING for 46s -> A
  ACTIVE[us-east-1 DELETING] + B DELETING for 53s -> A UPDATING[us-east-1 DELETING] for 108s -> A ACTIVE with
  Replicas gone at 208s while B was still DELETING (Replicas=[] and GlobalTableVersion already dropped in B)
  -> B ResourceNotFoundException at 242s. Re-add after B was gone: 200 immediately. The second delete (after
  re-add) took 97s with no second UPDATING phase.
  - ACK: deletable.when, requeue, updateable.when · ops: UpdateTable, DescribeTable · fields: ReplicaUpdates,
    Replicas
  - repro: UpdateTable ReplicaUpdates=[Delete us-east-1]; poll both regions; re-issue Create as soon as
    Replicas is empty
  - measurements: delete_total_s=242.3, a_entry_gone_s=207.8, b_notfound_s=242.3, readd_delete_total_s=96.8
  - handling: not handled in the controller (as of commit 34b85e6)
  - related: [DDB-BACKUP-001](../backup.md#ddb-backup-001), [DDB-BACKUP-007](../backup.md#ddb-backup-007), [DDB-TABLE-100](../table-restore.md#ddb-table-100), [DDB-TABLE-118](../service.md#ddb-table-118), [DDB-TABLE-087](../table-restore.md#ddb-table-087), [DDB-TABLE-454](../table-subresources.md#ddb-table-454),
    [DDB-TABLE-455](../service.md#ddb-table-455), [DDB-TABLE-227](../table-replicas.md#ddb-table-227) · hypotheses: H-R-006, H-R-026 · evidence:
    table/state-machine/replica-create-timeline

## Notes

Confirms H-R-006 for DELETING. Qualifies H-R-026: A's Replicas[] empties ~35s before the replica table
disappears; a Create issued in that gap is rejected with the 'already existed as tables' ValidationException
(observed via the re-add-while-deleting op); the exact gap behaviour is measured in
table/dependencies/replica-prerequisites. Note the base table flips ACTIVE->UPDATING->ACTIVE twice during one
replica delete, so 'TableStatus==ACTIVE' alone is not a completion signal; use Replicas[] emptiness.
