<!-- generated: ack-api-quirks dynamodb 2026-10-09 model=2012-08-10 controller=34b85e6 -->
# DDB-IMPORT-015: CSV without InputFormatOptions: COMPLETED, options omitted in DescribeImport (no default Delimiter); GZIP on a plain file -> FAILED
_Full entry and notes of one finding; its summary entry is in [import.md](../import.md). Generated from
ack-api-quirks `services/dynamodb` (model 2012-08-10, controller commit 34b85e6); edit the finding in the lab,
not here._

## Finding

- <a id="ddb-import-015"></a>**DDB-IMPORT-015** `server-default` · impact medium · unhandled (not handled in controller) · verified 2026-10-09
  **CSV without InputFormatOptions: COMPLETED, options omitted in DescribeImport (no default Delimiter); GZIP on a plain file -> FAILED**
  CSV with a header row and no InputFormatOptions -> csv-plain: ImportStatus=COMPLETED FailureCode=None
  Processed=3 Imported=3 Errors=0; table=ACTIVE ItemCount=0 scan=3; DescribeImport.InputFormatOptions=null.
  CSV with Delimiter=';' + HeaderList -> csv-options: ImportStatus=COMPLETED FailureCode=None Processed=3
  Imported=3 Errors=0; table=ACTIVE ItemCount=0 scan=3; echo={"Csv": {"Delimiter": ";", "HeaderList": ["pk",
  "val"]}}. InputCompressionType=GZIP on an uncompressed file -> FAILED FailureCode=ItemValidationError
  FailureMessage='Some of the items failed validation checks and were not imported. Please check CloudWatch
  error logs for more details.'.
  - ACK: compare.is_ignored+delta_pre_compare, terminal_codes · ops: ImportTable, DescribeImport · fields:
    InputFormatOptions.Csv.Delimiter, InputFormatOptions.Csv.HeaderList, InputCompressionType
  - repro: ImportTable CSV without/with InputFormatOptions; ImportTable GZIP on plain file -> DescribeImport
    at terminal
  - handling: not handled in the controller (as of commit 34b85e6)
  - hypotheses: H-B-128 · evidence: import/error-taxonomy/failure-modes

## Notes

H-B-128 CSV half refuted: no server default Delimiter=',' is reported, InputFormatOptions is simply omitted;
explicit options are echoed verbatim. A compression mismatch is not detected up front: it surfaces as
ItemValidationError (ProcessedItemCount=1, ImportedItemCount=0) with the table left ACTIVE and empty.
