<!-- generated: ack-api-quirks dynamodb 2026-10-09 model=2012-08-10 controller=34b85e6 -->
# DDB-IMPORT-013: Any bad line makes the import FAILED/ItemValidationError, yet valid rows are written (table ACTIVE with ImportedItemCount items)
_Full entry and notes of one finding; its summary entry is in [import.md](../import.md). Generated from
ack-api-quirks `services/dynamodb` (model 2012-08-10, controller commit 34b85e6); edit the finding in the lab,
not here._

## Finding

- <a id="ddb-import-013"></a>**DDB-IMPORT-013** `async-state-machine` · impact high · unhandled (not handled in controller) · verified 2026-10-09
  **Any bad line makes the import FAILED/ItemValidationError, yet valid rows are written (table ACTIVE with ImportedItemCount items)**
  10-line file with one item lacking the key attribute -> missing-key: ImportStatus=FAILED
  FailureCode=ItemValidationError Processed=10 Imported=9 Errors=1; table=ACTIVE ItemCount=0 scan=9. 10-line
  file with one unparseable line -> garbage-line: ImportStatus=FAILED FailureCode=ItemValidationError
  Processed=6 Imported=5 Errors=1; table=ACTIVE ItemCount=0 scan=5. FailureMessage(missing-key)='Some of the
  items failed validation checks and were not imported. Please check CloudWatch error logs for more details.'.
  - ACK: terminal_codes, synced.when · ops: ImportTable, DescribeImport, Scan · fields:
    ImportTableDescription.FailureCode, ImportTableDescription.ErrorCount,
    ImportTableDescription.ImportedItemCount
  - repro: Upload 10 DYNAMODB_JSON lines with 1 bad line -> ImportTable -> poll to terminal -> DescribeTable +
    Scan(COUNT)
  - measurements: missing_key_failed_after_s=174.3, garbage_line_failed_after_s=90.1
  - handling: not handled in the controller (as of commit 34b85e6)
  - related: [DDB-IMPORT-014](../import.md#ddb-import-014), [DDB-IMPORT-020](../import.md#ddb-import-020), [DDB-IMPORT-007](../import.md#ddb-import-007), [DDB-IMPORT-012](../import.md#ddb-import-012) · hypotheses: H-B-033, H-B-126 ·
    evidence: import/error-taxonomy/failure-modes

## Notes

H-B-126 confirmed, H-B-033 refuted: COMPLETED is only reachable with ErrorCount=0. FAILED does not mean empty -
deleting/re-creating the table on FAILED destroys partially imported data. Service-side durations: missing-key
174s, garbage-line 90s.

Contradiction with [DDB-IMPORT-007](../import.md#ddb-import-007): 007's behavior ends with 'the table is ResourceNotFoundException with
ItemCount=None' after the FAILED import, implying FAILED removes the target; 007's own poll log recorded
(FAILED, ACTIVE) three times, and 013/014/020 show FAILED imports leave an ACTIVE table holding the
successfully imported rows Resolution: 013 canonical; 007's final table read is inconsistent with its own poll
log (likely taken after cleanup) - FAILED never deletes the target, deleting it on FAILED destroys partial
data
