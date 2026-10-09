<!-- generated: ack-api-quirks dynamodb 2026-10-09 model=2012-08-10 controller=34b85e6 -->
# DDB-EXPORT-004: Incremental window violations (<15 min, >24 h, before table creation, inverted, future) are all InvalidExportTimeException
_Full entry and notes of one finding; its summary entry is in [export.md](../export.md). Generated from
ack-api-quirks `services/dynamodb` (model 2012-08-10, controller commit 34b85e6); edit the finding in the lab,
not here._

## Finding

- <a id="ddb-export-004"></a>**DDB-EXPORT-004** `request-validation` · impact medium · unhandled (not handled in controller) · verified 2026-10-09
  **Incremental window violations (<15 min, >24 h, before table creation, inverted, future) are all InvalidExportTimeException**
  10-minute window -> InvalidExportTimeException 'Incremental export period from time cannot be less than the
  table creation time : 2026-10-09T00:41:50Z'; 25-hour window -> InvalidExportTimeException 'Incremental
  export period from time cannot be less than the table creation time : 2026-10-07T22:51:50Z'; window entirely
  before PITR enable -> InvalidExportTimeException 'Incremental export period from time cannot be less than
  the table creation time : 2026-10-08T22:51:50Z'; from before enable, to omitted ->
  InvalidExportTimeException 'Incremental export period from time cannot be less than the table creation time
  : 2026-10-08T22:51:50Z'; from=enable+1s to omitted (fresh table) -> InvalidExportTimeException 'Difference
  between export period from time and export period to time is less than 15 minutes'; from after to ->
  InvalidExportTimeException 'Incremental export period from time cannot be less than the table creation time
  : 2026-10-08T23:51:50Z'; to in the future -> InvalidExportTimeException 'Incremental export period to time
  cannot be greater than the current time : 2026-10-09T01:51:50Z'.
  - ACK: terminal_codes, requeue · ops: ExportTableToPointInTime · fields:
    IncrementalExportSpecification.ExportFromTime, IncrementalExportSpecification.ExportToTime
  - repro: INCREMENTAL_EXPORT with each window variant ~1 minute after enabling PITR
  - handling: not handled in the controller (as of commit 34b85e6)
  - related: [DDB-EXPORT-001](../export.md#ddb-export-001), [DDB-EXPORT-003](../export.md#ddb-export-003), [DDB-EXPORT-019](../export.md#ddb-export-019) · hypotheses: H-B-117 · evidence:
    export/error-taxonomy/sync-validation

## Notes

All window violations are InvalidExportTimeException (never ValidationException), refuting the mixed-codes
part of H-B-117. Check precedence: 'from time cannot be less than the table creation time' fires before the
15-min/24-h length checks (the 10-min and 25-h cases hit it because the table was <1h old); the length rule
message is 'Difference between export period from time and export period to time is less than 15 minutes'; 'to
time cannot be greater than the current time'.
