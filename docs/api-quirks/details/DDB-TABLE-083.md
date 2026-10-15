<!-- generated: ack-api-quirks dynamodb 2026-10-09 model=2012-08-10 controller=34b85e6 -->
# DDB-TABLE-083: ContinuousBackupsStatus is not the PITR flag: DISABLED for a few s after ACTIVE, then ENABLED on its own; stays ENABLED after PITR disable
_Full entry and notes of one finding; its summary entry is in
[table-subresources.md](../table-subresources.md). Generated from ack-api-quirks `services/dynamodb` (model
2012-08-10, controller commit 34b85e6); edit the finding in the lab, not here._

## Finding

- <a id="ddb-table-083"></a>**DDB-TABLE-083** `shape-mismatch` · impact high · handled · verified 2026-10-08
  **ContinuousBackupsStatus is not the PITR flag: DISABLED for a few s after ACTIVE, then ENABLED on its own; stays ENABLED after PITR disable**
  Fresh table (immediately after ACTIVE): ContinuousBackupsStatus=DISABLED,
  PointInTimeRecoveryDescription={PointInTimeRecoveryStatus: DISABLED} (no other fields).
  UpdateContinuousBackups(enable) during that window -> 200 and ContinuousBackupsStatus=ENABLED with PITR
  fields [EarliestRestorableDateTime, LatestRestorableDateTime, PointInTimeRecoveryStatus,
  RecoveryPeriodInDays=35]. After PITR disable: ContinuousBackupsStatus stays ENABLED while
  PointInTimeRecoveryDescription shrinks to {PointInTimeRecoveryStatus: DISABLED}. Tables that never touch
  PITR also flip to ContinuousBackupsStatus=ENABLED by themselves within minutes (see
  table/consistency-windows/continuous-backups-window). The Update response has the same shape as Describe
  (top-level key ContinuousBackupsDescription only); the request bool PointInTimeRecoveryEnabled is never
  echoed and the status is a string enum.
  - ACK: custom_field, custom_update, compare.is_ignored+delta_pre_compare · ops: DescribeContinuousBackups,
    UpdateContinuousBackups · fields: ContinuousBackupsDescription.ContinuousBackupsStatus,
    ContinuousBackupsDescription.PointInTimeRecoveryDescription.PointInTimeRecoveryStatus,
    ContinuousBackupsDescription.PointInTimeRecoveryDescription.RecoveryPeriodInDays
  - repro: CreateTable -> DescribeContinuousBackups -> UpdateContinuousBackups(enable) -> Describe -> disable
    -> Describe
  - handling: handled via `generator.yaml:46-50; pkg/resource/table/hooks_continuous_backup.go:27-94; pkg/resource/table/hooks.go:697-718; pkg/resource/table/hooks_continuous_backup.go:36-44; pkg/resource/table/hooks.go:686-696; pkg/resource/table/hooks.go:729-731`
  - related: [DDB-TABLE-111](../table-subresources.md#ddb-table-111), [DDB-TABLE-115](../service.md#ddb-table-115), [DDB-TABLE-084](../table-subresources.md#ddb-table-084), [DDB-TABLE-085](../table-subresources.md#ddb-table-085) · evidence:
    table/sub-resources/pitr-lifecycle

## Notes

Hypotheses: H-S-101, H-S-104, H-S-003. Confirms H-S-104 (no round-trippable field) and H-S-003 (fields
conditional on ENABLED). H-S-101 partially refuted: ContinuousBackupsStatus=DISABLED IS observable on a normal
table right after creation.

Contradiction with [DDB-TABLE-115](../service.md#ddb-table-115): 083 says UpdateContinuousBackups(enable) issued while
ContinuousBackupsStatus still reads DISABLED right after ACTIVE -> 200; 115 says
UpdateContinuousBackups/CreateBackup fail with ContinuousBackupsUnavailableException 'Backups are being
enabled for the table' until ~2.6 s AFTER ACTIVE. [DDB-TABLE-111](../table-subresources.md#ddb-table-111) cites both and measures the DISABLED read
lasting ~3.1 s after ACTIVE Resolution: keep both; the gate is time-based (~2.6 s after ACTIVE) and ends ~0.5
s before the status read flips, so both observations fit; a controller must retry on
ContinuousBackupsUnavailableException rather than key on ContinuousBackupsStatus
