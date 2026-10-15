<!-- generated: ack-api-quirks dynamodb 2026-10-09 model=2012-08-10 controller=34b85e6 -->
# DDB-TABLE-118: DELETING table: Update TTL/PITR/Insights, CreateBackup, Restore and most Describes return 200 in the first ~1 s; DescribeTimeToLive rejects
_Full entry and notes of one finding; its summary entry is in [service.md](../service.md). Generated from
ack-api-quirks `services/dynamodb` (model 2012-08-10, controller commit 34b85e6); edit the finding in the lab,
not here._

## Finding

- <a id="ddb-table-118"></a>**DDB-TABLE-118** `error-code` · impact high · handled · verified 2026-10-08
  **DELETING table: Update TTL/PITR/Insights, CreateBackup, Restore and most Describes return 200 in the first ~1 s; DescribeTimeToLive rejects**
  While TableStatus=DELETING (observed in table/error-taxonomy/subresource-errors,
  table/sub-resources/pitr-lifecycle, table/sub-resources/insights-lifecycle): UpdateTimeToLive -> 200;
  UpdateContinuousBackups(disable) -> 200; UpdateContributorInsights -> 200 (DISABLING); CreateBackup -> 200
  (an AVAILABLE USER backup of the dying table is created); RestoreTableToPointInTime(UseLatestRestorableTime)
  -> 200 (a new table is created from the deleting source); DescribeContinuousBackups,
  DescribeContributorInsights, ListContributorInsights -> 200; DescribeTimeToLive -> ValidationException (HTTP 400)
  'Cannot describe time to live while table is in DELETING state: Current table state is DELETING'. Once the
  table is gone: DescribeTimeToLive/UpdateTimeToLive/Describe+Update+ListContributorInsights ->
  ResourceNotFoundException 'Requested resource not found: Table: X not found' (byte-identical to the
  CREATING-phase message);
  DescribeContinuousBackups/UpdateContinuousBackups/RestoreTableToPointInTime/CreateBackup ->
  TableNotFoundException 'Table not found: X'. All HTTP 400.
  - ACK: exceptions.404, deletable.when, terminal_codes · ops: DescribeTimeToLive, UpdateTimeToLive,
    DescribeContinuousBackups, UpdateContinuousBackups, DescribeContributorInsights,
    UpdateContributorInsights, ListContributorInsights
  - repro: DeleteTable; call each sub-resource API immediately and after the table is gone
  - handling: handled via `generator.yaml:84-87; pkg/resource/table/sdk.go:83-86`
  - related: [DDB-BACKUP-001](../backup.md#ddb-backup-001), [DDB-BACKUP-007](../backup.md#ddb-backup-007), [DDB-TABLE-100](../table-restore.md#ddb-table-100), [DDB-TABLE-087](../table-restore.md#ddb-table-087), [DDB-TABLE-454](../table-subresources.md#ddb-table-454), [DDB-TABLE-455](../service.md#ddb-table-455),
    [DDB-TABLE-227](../table-replicas.md#ddb-table-227), [DDB-TABLE-228](../table-replicas.md#ddb-table-228) · evidence: table/error-taxonomy/subresource-errors,
    table/state-machine/subresource-admissibility, table/sub-resources/insights-lifecycle,
    table/sub-resources/pitr-lifecycle

## Notes

Hypotheses: H-S-004. Hypotheses: H-S-004. In this probe the DeleteTable itself was refused (deletion
protection left on by a throttled UpdateTable), so the DELETING observations above come from the three sibling
probes listed in evidence.

Contradiction with [DDB-TABLE-100](../table-restore.md#ddb-table-100), [DDB-TABLE-454](../table-subresources.md#ddb-table-454), [DDB-TABLE-455](../service.md#ddb-table-455): 100/118 state without a time bound that a
DELETING source is 'still restorable' and accepts
CreateBackup/UpdateContinuousBackups/DescribeContinuousBackups; 454 got TableNotFoundException from
RestoreTableToPointInTime at +2.44 s and ExportTableToPointInTime at +1.71 s into DELETING, and 455 saw
DescribeContinuousBackups flip to TableNotFoundException at +1.03 s while DescribeTable still returned
DELETING until ~5 s Resolution: all true; the backup/PITR backend forgets the table ~1-1.6 s into DELETING, ~4
s before DescribeTable does - 454/455 canonical for the boundary, 100/118 retitled to carry the ~1 s qualifier
