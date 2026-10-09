<!-- generated: ack-api-quirks dynamodb 2026-10-09 model=2012-08-10 controller=34b85e6 -->
# DDB-EXPORT-014: UpdateContinuousBackups(PITR on) right after CreateTable->ACTIVE succeeded on the first attempt (0 s window) on both tables
_Full entry and notes of one finding; its summary entry is in [export.md](../export.md). Generated from
ack-api-quirks `services/dynamodb` (model 2012-08-10, controller commit 34b85e6); edit the finding in the lab,
not here._

## Finding

- <a id="ddb-export-014"></a>**DDB-EXPORT-014** `eventual-consistency` · impact medium · handled · verified 2026-10-09
  **UpdateContinuousBackups(PITR on) right after CreateTable->ACTIVE succeeded on the first attempt (0 s window) on both tables**
  Enabling PITR immediately after the table turned ACTIVE: {"t1": {"attempts": 1, "window_s": 0.0, "codes":
  [null]}, "t2": {"attempts": 1, "window_s": 0.0, "codes": [null]}}. Message: 'None'. Retrying every 2s
  succeeded after the window.
  - ACK: requeue, post-create-nudge · ops: CreateTable, UpdateContinuousBackups
  - repro: CreateTable -> wait ACTIVE -> UpdateContinuousBackups(PITR on) at once, retry every 2s
  - measurements: window_s_t1=0.0, window_s_t2=0.0
  - handling: handled via `templates/hooks/table/sdk_create_post_set_output.go.tpl:1-4; bcd26e1`
  - related: [DDB-BACKUP-002](../backup.md#ddb-backup-002), [DDB-TABLE-447](../service.md#ddb-table-447), [DDB-IMPORT-001](../import.md#ddb-import-001) · hypotheses: H-B-025 · evidence:
    export/idempotency/client-token

## Notes

Also seen in export/state-machine/lifecycle and export/error-taxonomy/sync-validation (first attempt
rejected). Prerequisite for any Export CRD that enables PITR on its source.

Contradiction with [DDB-BACKUP-002](../backup.md#ddb-backup-002), [DDB-TABLE-447](../service.md#ddb-table-447): 014's title claims UpdateContinuousBackups right after
ACTIVE fails with ContinuousBackupsUnavailableException, but its data is attempts=1, window 0.0 s, code null
on both tables (no failure at all); 002 observed the error at +0.1 s and +3.2 s with success at +6.23 s for
CreateBackup and reports 3-6 s for UpdateContinuousBackups; 447's live rows reproduce it for both ops in two
regions Resolution: keep both; 002 canonical for the code/message; the post-ACTIVE window is 0-6 s and not
deterministic, so a controller must requeue on the code rather than sleep a fixed time (014 retitled)
