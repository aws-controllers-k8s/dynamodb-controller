<!-- generated: ack-api-quirks dynamodb 2026-10-09 model=2012-08-10 controller=34b85e6 -->
# DDB-BACKUP-002: CreateBackup fails with ContinuousBackupsUnavailableException for the first ~3-6 s after a table turns ACTIVE
_Full entry and notes of one finding; its summary entry is in [backup.md](../backup.md). Generated from
ack-api-quirks `services/dynamodb` (model 2012-08-10, controller commit 34b85e6); edit the finding in the lab,
not here._

## Finding

- <a id="ddb-backup-002"></a>**DDB-BACKUP-002** `prerequisite` · impact high · handled · verified 2026-10-09
  **CreateBackup fails with ContinuousBackupsUnavailableException for the first ~3-6 s after a table turns ACTIVE**
  Right after DescribeTable first reported ACTIVE, CreateBackup returned ContinuousBackupsUnavailableException
  (HTTP 400, 'Backups are being enabled for the table: <name>. Please retry later') at +0.1 s and +3.2 s and
  succeeded at +6.23 s with no change to the table. The same window affects UpdateContinuousBackups (observed
  in sibling probes: 3-6 s).
  - ACK: requeue, references · ops: CreateBackup, UpdateContinuousBackups
  - repro: CreateTable -> poll until ACTIVE -> CreateBackup every 3 s and record codes
  - measurements: window_after_active_s=6.23, attempts=3
  - handling: handled via `generator.yaml:46-50; pkg/resource/table/hooks_continuous_backup.go:27-94; templates/hooks/table/sdk_create_post_set_output.go.tpl:1-4; bcd26e1; generator.yaml:141-144; pkg/resource/backup/sdk.go:82-84; test/e2e/tests/test_backup.py:37-75`
  - related: [DDB-EXPORT-014](../export.md#ddb-export-014), [DDB-TABLE-447](../service.md#ddb-table-447), [DDB-IMPORT-001](../import.md#ddb-import-001) · hypotheses: H-B-002 · evidence:
    backup/state-machine/lifecycle

## Notes

H-B-002 confirmed in kind but the window is seconds, not minutes. Message differs from the modelled doc string
('Backups have not yet been enabled for this table').

Contradiction with [DDB-EXPORT-014](../export.md#ddb-export-014), [DDB-TABLE-447](../service.md#ddb-table-447): 014's title claims UpdateContinuousBackups right after
ACTIVE fails with ContinuousBackupsUnavailableException, but its data is attempts=1, window 0.0 s, code null
on both tables (no failure at all); 002 observed the error at +0.1 s and +3.2 s with success at +6.23 s for
CreateBackup and reports 3-6 s for UpdateContinuousBackups; 447's live rows reproduce it for both ops in two
regions Resolution: keep both; 002 canonical for the code/message; the post-ACTIVE window is 0-6 s and not
deterministic, so a controller must requeue on the code rather than sleep a fixed time (014 retitled)
