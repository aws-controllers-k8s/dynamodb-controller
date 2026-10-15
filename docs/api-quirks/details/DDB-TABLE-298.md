<!-- generated: ack-api-quirks dynamodb 2026-10-09 model=2012-08-10 controller=34b85e6 -->
# DDB-TABLE-298: A CREATING replica is visible to DescribeTable only (other regional calls -> NotFound); DeleteTable on it -> ResourceInUseException
_Full entry and notes of one finding; its summary entry is in [table-replicas.md](../table-replicas.md).
Generated from ack-api-quirks `services/dynamodb` (model 2012-08-10, controller commit 34b85e6); edit the
finding in the lab, not here._

## Finding

- <a id="ddb-table-298"></a>**DDB-TABLE-298** `async-state-machine` · impact high · unhandled (not handled in controller) · verified 2026-10-09
  **A CREATING replica is visible to DescribeTable only (other regional calls -> NotFound); DeleteTable on it -> ResourceInUseException**
  1-item source table; the us-east-1 replica table became visible 68s after Create as TableStatus=CREATING
  with Replicas=[us-west-2 ACTIVE], GlobalTableVersion set, no StreamSpecification/LatestStreamArn yet, its
  own TableId. While CREATING, from the us-east-1 endpoint: DescribeTimeToLive / ListTagsOfResource /
  UpdateTable(DP) / TagResource / UpdateTimeToLive / PutItem / GetItem -> ResourceNotFoundException
  ('Requested resource not found: Table: <name> not found'); DescribeContinuousBackups /
  UpdateContinuousBackups -> TableNotFoundException ('Table not found: <name>');
  DescribeTableReplicaAutoScaling -> 200; ReplicaUpdates Create eu-west-1 -> ValidationException
  'Create/Update/Delete of replica is not allowed while the replica is being added to table with name: <name>
  in region ...'; ReplicaUpdates Delete us-west-2 (the source) -> ValidationException 'Replica cannot be
  deleted because it has acted as a source region for new replica(s) being added to the table in the last 24
  hours.'; DeleteTable on the CREATING replica -> ResourceInUseException 'Attempt to change a resource which
  is still in use: Table: <name> is being used.' PutItem on the base during CREATING -> 200 and the item was
  present in the replica afterwards. Base: ACTIVE[us-east-1 CREATING] 175s -> UPDATING[CREATING] 309s -> both
  ACTIVE at 494s (+68s). Replica ItemCount reported 0 (stale) although items exist.
  - ACK: updateable.when, deletable.when, custom_find, requeue · ops: DescribeTable, UpdateTable, DeleteTable,
    TagResource, UpdateTimeToLive, UpdateContinuousBackups, PutItem, GetItem · fields: Replicas, TableStatus,
    DeletionProtectionEnabled
  - repro: 1-item PPR table -> UpdateTable Create us-east-1 -> wait until us-east-1 DescribeTable succeeds ->
    issue each op against us-east-1 -> poll both regions
  - measurements: replica_visible_after_s=67.9, one_item_create_total_s=562.2
  - handling: not handled in the controller (as of commit 34b85e6)
  - related: [DDB-BACKUP-001](../backup.md#ddb-backup-001), [DDB-TABLE-217](../table-restore.md#ddb-table-217), [DDB-IMPORT-001](../import.md#ddb-import-001), [DDB-TABLE-447](../service.md#ddb-table-447), [DDB-TABLE-277](../table-restore.md#ddb-table-277), [DDB-IMPORT-006](../import.md#ddb-import-006) ·
    hypotheses: H-R-006, H-R-007, H-R-009, H-R-026 · evidence: table/state-machine/replica-creating-side-ops

## Notes

Qualifies H-R-006/H-R-007: during CREATING the replica region's DescribeTable works but sub-resource APIs
disagree on the not-found code (ResourceNotFoundException vs TableNotFoundException for continuous backups).
H-R-009/H-R-026: the creation cannot be cancelled from either side (ResourceInUseException /
ValidationException). The base TableStatus flips ACTIVE->UPDATING->ACTIVE during a single replica creation;
use Replicas[].ReplicaStatus, not TableStatus, as the completion signal.
