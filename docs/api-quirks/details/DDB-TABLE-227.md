<!-- generated: ack-api-quirks dynamodb 2026-10-09 model=2012-08-10 controller=34b85e6 -->
# DDB-TABLE-227: Admissibility while a replica is CREATING: which UpdateTable / tag / TTL / PITR / policy / backup calls succeed
_Full entry and notes of one finding; its summary entry is in [table-replicas.md](../table-replicas.md).
Generated from ack-api-quirks `services/dynamodb` (model 2012-08-10, controller commit 34b85e6); edit the
finding in the lab, not here._

## Finding

- <a id="ddb-table-227"></a>**DDB-TABLE-227** `async-state-machine` · impact high · unhandled (not handled in controller) · verified 2026-10-09
  **Admissibility while a replica is CREATING: which UpdateTable / tag / TTL / PITR / policy / backup calls succeed**
  Issued 3-7s after ReplicaUpdates.Create (A: TableStatus=UPDATING, Replicas still []): UpdateTable Create
  eu-west-1 / duplicate Create us-east-1 / DeletionProtectionEnabled / TableClass -> ResourceInUseException
  'The resource which you are attempting to change is in use.' (HTTP 400). UpdateTable Update{us-east-1
  TableClassOverride} and ReplicaUpdates Delete us-east-1 -> ValidationException 'Update global table
  operation failed because one or more replicas were not part of the global table. Please retry the request
  without these replicas: ...' (the entry did not exist yet). StreamSpecification{StreamEnabled:false} ->
  ValidationException 'Disabling Stream is not allowed for a Global Table replica.' TagResource,
  UpdateTimeToLive, UpdateContinuousBackups(PITR), PutResourcePolicy, UpdateContributorInsights, CreateBackup
  and DescribeTableReplicaAutoScaling all succeeded (200) during the window.
  - ACK: updateable.when, requeue, custom_update · ops: UpdateTable, TagResource, UpdateTimeToLive,
    UpdateContinuousBackups, PutResourcePolicy, UpdateContributorInsights, CreateBackup,
    DescribeTableReplicaAutoScaling
  - repro: UpdateTable Create replica; within 10s issue each op; record code
  - handling: not handled in the controller (as of commit 34b85e6)
  - related: [DDB-BACKUP-001](../backup.md#ddb-backup-001), [DDB-BACKUP-007](../backup.md#ddb-backup-007), [DDB-TABLE-100](../table-restore.md#ddb-table-100), [DDB-TABLE-118](../service.md#ddb-table-118), [DDB-TABLE-087](../table-restore.md#ddb-table-087), [DDB-TABLE-454](../table-subresources.md#ddb-table-454),
    [DDB-TABLE-455](../service.md#ddb-table-455), [DDB-TABLE-228](../table-replicas.md#ddb-table-228) · hypotheses: H-R-006, H-R-026, H-R-024 · evidence:
    table/state-machine/replica-create-timeline

## Notes

Confirms H-R-006 for the UpdateTable half (ResourceInUseException for any other UpdateTable mutation,
including a second replica action) and for the sub-resource half (tags/TTL/PITR/policy/insights/backup are
admitted). H-R-026 (cancel during CREATING) could not be exercised here because Replicas[] was still empty;
see table/dependencies/replica-prerequisites for the genuine CREATING window (-> ResourceInUseException).
