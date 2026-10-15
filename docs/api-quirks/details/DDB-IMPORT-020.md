<!-- generated: ack-api-quirks dynamodb 2026-10-09 model=2012-08-10 controller=34b85e6 -->
# DDB-IMPORT-020: Export->Import round trip: export root prefix -> FAILED; data/ + GZIP -> COMPLETED; data/ + NONE -> FAILED...
_Full entry and notes of one finding; its summary entry is in [import.md](../import.md). Generated from
ack-api-quirks `services/dynamodb` (model 2012-08-10, controller commit 34b85e6); edit the finding in the lab,
not here._

## Finding

- <a id="ddb-import-020"></a>**DDB-IMPORT-020** `prerequisite` · impact medium · unhandled (not handled in controller) · verified 2026-10-09
  **Export->Import round trip: export root prefix -> FAILED; data/ + GZIP -> COMPLETED; data/ + NONE -> FAILED...**
  A FULL_EXPORT (DYNAMODB_JSON) of a 3-item table wrote 9 objects under
  exp/AWSDynamoDB/01791506763707-476754e8/ (manifest-summary.json, manifest-files.json, data/*.json.gz; data
  sample line: ). ImportTable results: root: FAILED FailureCode=ItemValidationError processed=7 imported=3
  errors=4 table=ACTIVE scan=3 | gzip: COMPLETED processed=3 imported=3 errors=0 table=ACTIVE scan=3 | none:
  FAILED FailureCode=ItemValidationError processed=4 imported=0 errors=4 table=ACTIVE scan=0 | auto: FAILED
  FailureCode=ItemValidationError processed=4 imported=0 errors=4 table=ACTIVE scan=0.
  - ACK: docs-only, terminal_codes, references · ops: ExportTableToPointInTime, ImportTable, DescribeImport ·
    fields: S3BucketSource.S3KeyPrefix, InputCompressionType, InputFormat
  - repro: Export 3-item PITR table -> ImportTable from <root>/ (GZIP), <root>/data/ (GZIP), <root>/data/
    (NONE), <root>/data/ (omitted) -> poll to terminal -> Scan(COUNT)
  - measurements: export_completed_s=739.41, imports_terminal_s=136.15, export_item_count=3,
    export_billed_size_bytes=0
  - handling: not handled in the controller (as of commit 34b85e6)
  - related: [DDB-IMPORT-013](../import.md#ddb-import-013), [DDB-IMPORT-014](../import.md#ddb-import-014), [DDB-IMPORT-007](../import.md#ddb-import-007), [DDB-IMPORT-012](../import.md#ddb-import-012) · hypotheses: H-B-134 · evidence:
    import/dependencies/from-export

## Notes

FailureMessages: root='Some of the items failed validation checks and were not imported. Please check
CloudWatch error logs for more details.' none='Some of the items failed validation checks and were not
imported. Please check CloudWatch error logs for more details.' auto='Some of the items failed validation
checks and were not imported. Please check CloudWatch error logs for more details.'.
