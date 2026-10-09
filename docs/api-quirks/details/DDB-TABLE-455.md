<!-- generated: ack-api-quirks dynamodb 2026-10-09 model=2012-08-10 controller=34b85e6 -->
# DDB-TABLE-455: DELETING phase map: TTL/Insights/Kinesis Enable accept once in the first second, each backend forgets the table at its own time
_Full entry and notes of one finding; its summary entry is in [service.md](../service.md). Generated from
ack-api-quirks `services/dynamodb` (model 2012-08-10, controller commit 34b85e6); edit the finding in the lab,
not here._

## Finding

- <a id="ddb-table-455"></a>**DDB-TABLE-455** `delete-semantics` · impact medium · unhandled (not handled in controller) · verified 2026-10-09
  **DELETING phase map: TTL/Insights/Kinesis Enable accept once in the first second, each backend forgets the table at its own time**
  One table per API, DeleteTable then the API every 200 ms (DescribeTable in the same slot; tables gone at
  3.8-5.8 s). UpdateTimeToLive(enable): 200 at +0.04 s, then ValidationException 'TimeToLive is already
  enabled' until the table is gone (+5.44 s), then ResourceNotFoundException.
  UpdateContributorInsights(ENABLE): 200 ENABLING from +0.03 to +1.43 s (8 calls), then ValidationException
  'Table or Index is not in a valid state to update Key Access Insights: TableStatus must be ACTIVE to enable
  ContributorInsights.' until gone, then ResourceNotFoundException; no CloudWatch insight rule existed at any
  of 7 checks over 60 s after deletion and ListContributorInsights had no ghost.
  EnableKinesisStreamingDestination on a table WITHOUT a destination: 200 ENABLING at +0.03 s only, then
  ValidationException '...must be DISABLED or ENABLE_FAILED to perform' (the just-accepted entry blocks
  re-enable) until gone, then ResourceNotFoundException; DescribeKinesisStreamingDestination after deletion ->
  ResourceNotFoundException. UpdateTable(DeletionProtection / stream / WarmThroughput): ResourceInUseException
  'Table is being deleted' for the whole DELETING window, then ResourceNotFoundException.
  UpdateTableReplicaAutoScaling (Min/Max only): ValidationException "Parameters 'ScalingPolicyUpdate' are
  required unless auto scaling is being disabled" both while DELETING and after the table is gone (shape
  validation precedes the existence check). Reads: ListTagsOfResource 200 until +1.23 s then
  ResourceNotFoundException; GetResourcePolicy 200 (old RevisionId) until +5.23 s, PolicyNotFoundException at
  +5.43 s (one slot), ResourceNotFoundException from +5.63 s; DescribeContinuousBackups 200 until +1.03 s then
  TableNotFoundException; DescribeContributorInsights 200 DISABLED until gone. Same-name re-create of the
  ttl/ins/pitr/kin tables: TTL DISABLED, insights DISABLED, PITR DISABLED, no Kinesis destination - nothing
  leaked.
  - ACK: deletable.when, pre-delete-cleanup, requeue, exceptions.404 · ops: UpdateTimeToLive,
    UpdateContributorInsights, EnableKinesisStreamingDestination, UpdateTable, UpdateTableReplicaAutoScaling,
    ListTagsOfResource, GetResourcePolicy, DescribeContinuousBackups, DescribeContributorInsights
  - repro: DeleteTable; call the API every 200 ms until 1 s after DescribeTable returns
    ResourceNotFoundException; then describe-insight-rules / re-create the name
  - measurements: ttl_ok_until_s=0.04, insights_ok_until_s=1.43, kinesis_ok_until_s=0.03,
    list_tags_ok_until_s=1.23, get_policy_ok_until_s=5.23, policy_not_found_at_s=5.43,
    describe_cb_ok_until_s=1.03, table_gone_s=[5.64, 5.43, 5.23, 3.83, 5.04, 5.83, 5.63]
  - handling: not handled in the controller (as of commit 34b85e6)
  - related: [DDB-TABLE-374](../service.md#ddb-table-374), [DDB-TABLE-118](../service.md#ddb-table-118), [DDB-TABLE-236](../table-policy-kinesis-autoscaling.md#ddb-table-236), [DDB-TABLE-104](../table.md#ddb-table-104), [DDB-TABLE-122](../table-policy-kinesis-autoscaling.md#ddb-table-122), [DDB-TABLE-342](../table-subresources.md#ddb-table-342),
    [DDB-BACKUP-001](../backup.md#ddb-backup-001), [DDB-BACKUP-007](../backup.md#ddb-backup-007), [DDB-TABLE-100](../table-restore.md#ddb-table-100), [DDB-TABLE-087](../table-restore.md#ddb-table-087), [DDB-TABLE-454](../table-subresources.md#ddb-table-454), [DDB-TABLE-227](../table-replicas.md#ddb-table-227), [DDB-TABLE-228](../table-replicas.md#ddb-table-228)
    · evidence: table/creative/deleting-lasting-effects

## Notes

Extends [DDB-TABLE-374](../service.md#ddb-table-374) (policy/backup/tag) to the remaining APIs and [DDB-TABLE-236](../table-policy-kinesis-autoscaling.md#ddb-table-236) (whose table had an ACTIVE
destination, hence ValidationException) with the no-destination case where Enable is accepted. The transient
PolicyNotFoundException just before the 404 could be misread by a policy reconciler as 'policy removed, re-put
it'.

Contradiction with [DDB-TABLE-100](../table-restore.md#ddb-table-100), [DDB-TABLE-118](../service.md#ddb-table-118), [DDB-TABLE-454](../table-subresources.md#ddb-table-454): 100/118 state without a time bound that a
DELETING source is 'still restorable' and accepts
CreateBackup/UpdateContinuousBackups/DescribeContinuousBackups; 454 got TableNotFoundException from
RestoreTableToPointInTime at +2.44 s and ExportTableToPointInTime at +1.71 s into DELETING, and 455 saw
DescribeContinuousBackups flip to TableNotFoundException at +1.03 s while DescribeTable still returned
DELETING until ~5 s Resolution: all true; the backup/PITR backend forgets the table ~1-1.6 s into DELETING, ~4
s before DescribeTable does - 454/455 canonical for the boundary, 100/118 retitled to carry the ~1 s qualifier
