<!-- generated: ack-api-quirks dynamodb 2026-10-09 model=2012-08-10 controller=34b85e6 -->
# DDB-EXPORT-003: ExportType/IncrementalExportSpecification couplings are all ValidationException; ExportFromTime is required for INCREMENTAL_EXPORT
_Full entry and notes of one finding; its summary entry is in [export.md](../export.md). Generated from
ack-api-quirks `services/dynamodb` (model 2012-08-10, controller commit 34b85e6); edit the finding in the lab,
not here._

## Finding

- <a id="ddb-export-003"></a>**DDB-EXPORT-003** `request-validation` · impact medium · unhandled (not handled in controller) · verified 2026-10-09
  **ExportType/IncrementalExportSpecification couplings are all ValidationException; ExportFromTime is required for INCREMENTAL_EXPORT**
  IncrementalExportSpecification with ExportType omitted -> ValidationException 'Invalid Request: Export Type
  expected to be Incremental Export when incremental export specification is provided'; with
  ExportType=FULL_EXPORT -> ValidationException 'Invalid Request: When Export Type is Full Export ,
  Incremental Export Specification should not be provided'; ExportTime together with INCREMENTAL_EXPORT ->
  InvalidExportTimeException 'Incremental export period from time cannot be less than the table creation time
  : 2026-10-08T22:51:50Z'; ExportViewType=NEW_IMAGES -> ValidationException '1 validation error detected:
  Value 'NEW_IMAGES' at 'incrementalExportSpecification.exportViewType' failed to satisfy constraint: Member
  must'; INCREMENTAL_EXPORT without a spec -> ValidationException 'Invalid Request: Incremental Export
  Specification expected when export type is set as Incremental Export'; empty spec -> ValidationException
  'Invalid Request: ExportFromTime must be provided in IncrementalExportSpecification'; spec with only
  ExportViewType -> ValidationException 'Invalid Request: ExportFromTime must be provided in
  IncrementalExportSpecification'; ExportFromTime omitted -> ValidationException 'Invalid Request:
  ExportFromTime must be provided in IncrementalExportSpecification'.
  - ACK: terminal_codes, docs-only · ops: ExportTableToPointInTime · fields: ExportType, ExportTime,
    IncrementalExportSpecification.ExportViewType, IncrementalExportSpecification.ExportFromTime
  - repro: ExportTableToPointInTime with each coupling variant on a PITR table
  - handling: not handled in the controller (as of commit 34b85e6)
  - related: [DDB-EXPORT-001](../export.md#ddb-export-001), [DDB-EXPORT-004](../export.md#ddb-export-004), [DDB-EXPORT-019](../export.md#ddb-export-019) · hypotheses: H-B-118, H-B-117 · evidence:
    export/error-taxonomy/sync-validation

## Notes

H-B-118 confirmed on all four couplings (all ValidationException with explicit messages); ExportFromTime is
effectively required for INCREMENTAL_EXPORT ('ExportFromTime must be provided') although the model marks it
optional (H-B-117). ExportTime + INCREMENTAL_EXPORT was not isolated: the window check
(InvalidExportTimeException) fired first.
