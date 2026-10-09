<!-- generated: ack-api-quirks dynamodb 2026-10-09 model=2012-08-10 controller=34b85e6 -->
# DDB-TABLE-115: CREATING: TTL/Insights updates -> ResourceNotFound then ResourceInUse/Validation; PITR/backup -> TableNotFound then CBUnavailableException
_Full entry and notes of one finding; its summary entry is in [service.md](../service.md). Generated from
ack-api-quirks `services/dynamodb` (model 2012-08-10, controller commit 34b85e6); edit the finding in the lab,
not here._

## Finding

- <a id="ddb-table-115"></a>**DDB-TABLE-115** `async-state-machine` · impact high · handled · verified 2026-10-08
  **CREATING: TTL/Insights updates -> ResourceNotFound then ResourceInUse/Validation; PITR/backup -> TableNotFound then CBUnavailableException**
  Plain table CREATING (6.5s): UpdateTimeToLive [('ResourceNotFoundException', 400, 'Requested resource not
  found: Table: ackq-34ad39-s1 not found'), ('ResourceInUseException', 400, 'Attempt to change a resource
  which is still in use: Table ackq-34ad39-s1 is being created')]; DescribeTimeToLive [('ValidationException',
  400, 'Cannot describe time to live while table is in CREATING state: Current table state is CREATING')];
  UpdateContinuousBackups [('TableNotFoundException', 400, 'Table not found: ackq-34ad39-s1'),
  ('ContinuousBackupsUnavailableException', 400, 'Backups are being enabled for the table: ackq-34ad39-s1.
  Please retry later')]; DescribeContinuousBackups [('TableNotFoundException', 400, 'Table not found:
  ackq-34ad39-s1'), ('OK', 200, '')]; UpdateContributorInsights [('ResourceNotFoundException', 400, 'Requested
  resource not found: Table: ackq-34ad39-s1 not found'), ('ValidationException', 400, 'Table or Index is not
  in a valid state to update Key Access Insights: TableStatus must be ACTIVE to enable ContributorIn')];
  DescribeContributorInsights [('ResourceNotFoundException', 400, 'Requested resource not found: Table:
  ackq-34ad39-s1 not found'), ('OK', 200, '')]; ListContributorInsights [('ResourceNotFoundException', 400,
  'Requested resource not found: Table: ackq-34ad39-s1 not found'), ('OK', 200, '')]; CreateBackup
  [('TableNotFoundException', 400, 'Table not found: ackq-34ad39-s1'),
  ('ContinuousBackupsUnavailableException', 400, 'Backups are being enabled for the table: ackq-34ad39-s1.
  Please retry later')]. First success after first ACTIVE (s): {'DescribeContinuousBackups': None,
  'DescribeContributorInsights': None, 'ListContributorInsights': None, 'DescribeTimeToLive': 0.0,
  'UpdateTimeToLive': 0.0, 'UpdateContributorInsights': 0.1, 'UpdateContinuousBackups': 2.6, 'CreateBackup':
  2.7}. Describe values while CREATING: ttl=None cb={'ContinuousBackupsStatus': 'DISABLED',
  'PointInTimeRecoveryDescription': {'PointInTimeRecoveryStatus': 'DISABLED'}}
  ci={'CREATING/DescribeContributorInsights': {'status': 'DISABLED', 'keys': ['ContributorInsightsStatus',
  'TableName']}}.
  - ACK: synced.when, requeue, terminal_codes, exceptions.404 · ops: UpdateTimeToLive, DescribeTimeToLive,
    UpdateContinuousBackups, DescribeContinuousBackups, UpdateContributorInsights,
    DescribeContributorInsights, CreateBackup
  - repro: CreateTable then call each op every 1.2s until success
  - measurements: create_to_active_s=6.5, first_ok_after_active_s.DescribeContinuousBackups=null,
    first_ok_after_active_s.DescribeContributorInsights=null,
    first_ok_after_active_s.ListContributorInsights=null, first_ok_after_active_s.DescribeTimeToLive=0.0,
    first_ok_after_active_s.UpdateTimeToLive=0.0, first_ok_after_active_s.UpdateContributorInsights=0.1,
    first_ok_after_active_s.UpdateContinuousBackups=2.6, first_ok_after_active_s.CreateBackup=2.7
  - handling: handled via `generator.yaml:104-109; pkg/resource/table/hooks.go:72-93; test/e2e/table.py:47-73; test/e2e/tests/test_table.py:351-386; generator.yaml:46-50; pkg/resource/table/hooks_continuous_backup.go:27-94; generator.yaml:78-83; pkg/resource/table/hooks.go:882-960; generator.yaml:84-87; pkg/resource/table/sdk.go:83-86; templates/hooks/table/sdk_create_post_set_output.go.tpl:1-4; bcd26e1`
  - related: [DDB-TABLE-114](../service.md#ddb-table-114), [DDB-TABLE-111](../table-subresources.md#ddb-table-111), [DDB-TABLE-233](../table-policy-kinesis-autoscaling.md#ddb-table-233), [DDB-TABLE-234](../table-policy-kinesis-autoscaling.md#ddb-table-234), [DDB-TABLE-083](../table-subresources.md#ddb-table-083), [DDB-TABLE-084](../table-subresources.md#ddb-table-084),
    [DDB-TABLE-085](../table-subresources.md#ddb-table-085) · evidence: table/state-machine/subresource-admissibility

## Notes

Hypotheses: H-S-010, H-S-015, H-S-103, H-S-004. Hypotheses: H-S-010 (partially: first
ResourceNotFoundException 'Table: X not found' - identical text to a deleted table - then, ~2.6 s into
CREATING, ResourceInUseException 'Attempt to change a resource which is still in use: Table X is being
created'; DescribeTimeToLive does NOT succeed while CREATING: ValidationException 'Cannot describe time to
live while table is in CREATING state'), H-S-015/H-S-103 (confirmed: TableNotFoundException for ~2.6 s, then
ContinuousBackupsUnavailableException 'Backups are being enabled for the table: X. Please retry later' for
Update/CreateBackup until ~2.6 s AFTER TableStatus=ACTIVE; DescribeContinuousBackups succeeds from ~2.6 s into
CREATING with ContinuousBackupsStatus=DISABLED), H-S-004 (continuous-backups family uses
TableNotFoundException; no TableInUseException seen). UpdateContributorInsights while CREATING ->
ValidationException 'Table or Index is not in a valid state to update Key Access Insights: TableStatus must be
ACTIVE...'. Describe/List ContributorInsights succeed from ~2.6 s into CREATING (status DISABLED). None of the
successful sub-resource updates changed TableStatus (stayed ACTIVE).

Contradiction with [DDB-TABLE-083](../table-subresources.md#ddb-table-083): 083 says UpdateContinuousBackups(enable) issued while
ContinuousBackupsStatus still reads DISABLED right after ACTIVE -> 200; 115 says
UpdateContinuousBackups/CreateBackup fail with ContinuousBackupsUnavailableException 'Backups are being
enabled for the table' until ~2.6 s AFTER ACTIVE. [DDB-TABLE-111](../table-subresources.md#ddb-table-111) cites both and measures the DISABLED read
lasting ~3.1 s after ACTIVE Resolution: keep both; the gate is time-based (~2.6 s after ACTIVE) and ends ~0.5
s before the status read flips, so both observations fit; a controller must retry on
ContinuousBackupsUnavailableException rather than key on ContinuousBackupsStatus
