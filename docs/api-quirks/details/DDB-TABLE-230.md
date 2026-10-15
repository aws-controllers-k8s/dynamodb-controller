<!-- generated: ack-api-quirks dynamodb 2026-10-09 model=2012-08-10 controller=34b85e6 -->
# DDB-TABLE-230: Which table settings replicate to the replica region (tags, TTL, PITR, resource policy, insights, deletion protection, table class)
_Full entry and notes of one finding; its summary entry is in [table-replicas.md](../table-replicas.md).
Generated from ack-api-quirks `services/dynamodb` (model 2012-08-10, controller commit 34b85e6); edit the
finding in the lab, not here._

## Finding

- <a id="ddb-table-230"></a>**DDB-TABLE-230** `cross-region` · impact high · unhandled (not handled in controller) · verified 2026-10-09
  **Which table settings replicate to the replica region (tags, TTL, PITR, resource policy, insights, deletion protection, table class)**
  Right after the replica became ACTIVE, us-east-1 showed: TTL ENABLED (set on A during CREATING -> replicated
  at creation), tags [] (A had 3 tags), PITR DISABLED (A ENABLED), resource policy PolicyNotFoundException (A
  had one), ContributorInsights DISABLED (A ENABLED), DeletionProtectionEnabled false, stream
  NEW_AND_OLD_IMAGES (own ARN). Then set on A: a new tag, PITR (already on), resource policy, insights,
  DeletionProtectionEnabled=true (UPDATING for 39s). After 240s of polling (every 10s) NONE of tags / PITR /
  policy / insights / deletion protection had appeared in us-east-1. Only TimeToLive is group-wide (and the
  re-created replica inherited it again).
  - ACK: tags.custom-sync, custom_update, compare.is_ignored+delta_pre_compare · ops: TagResource,
    UpdateTimeToLive, UpdateContinuousBackups, PutResourcePolicy, UpdateContributorInsights, UpdateTable ·
    fields: Tags, TimeToLiveSpecification, PointInTimeRecoverySpecification, DeletionProtectionEnabled,
    ResourcePolicy
  - repro: Set each setting on the base table, read the equivalent in the replica region
  - measurements: dp_toggle_updating_s_with_replica=39.3, propagation_watch_s=240
  - handling: not handled in the controller (as of commit 34b85e6)
  - related: [DDB-TABLE-215](../table-restore.md#ddb-table-215), [DDB-BACKUP-009](../backup.md#ddb-backup-009), [DDB-BACKUP-021](../backup.md#ddb-backup-021), [DDB-BACKUP-004](../backup.md#ddb-backup-004), [DDB-TABLE-218](../table-restore.md#ddb-table-218) · hypotheses:
    H-R-028 · evidence: table/state-machine/replica-create-timeline

## Notes

Confirms H-R-028: tags, PITR, resource policy, Contributor Insights and DeletionProtection are regional and
must be applied per replica region with a regional client; TTL is replicated. Enabling DeletionProtection on a
table with a replica keeps TableStatus UPDATING for ~40s (vs seconds on a regional table).
