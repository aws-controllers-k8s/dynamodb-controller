<!-- generated: ack-api-quirks dynamodb 2026-10-09 model=2012-08-10 controller=34b85e6 -->
# DDB-IMPORT-007: Mutating the S3 source objects while an import is IN_PROGRESS -> final ImportStatus FAILED
_Full entry and notes of one finding; its summary entry is in [import.md](../import.md). Generated from
ack-api-quirks `services/dynamodb` (model 2012-08-10, controller commit 34b85e6); edit the finding in the lab,
not here._

## Finding

- <a id="ddb-import-007"></a>**DDB-IMPORT-007** `async-state-machine` · impact medium · unhandled (not handled in controller) · verified 2026-10-09
  **Mutating the S3 source objects while an import is IN_PROGRESS -> final ImportStatus FAILED**
  A ~30.8 MB import (30 objects) had 15 objects deleted and 1 overwritten at t+61.5s (import status
  IN_PROGRESS at that moment). Final DescribeImport: ImportStatus=FAILED FailureCode=ItemValidationError
  FailureMessage=Some of the items failed validation checks and were not imported. Please check CloudWatch
  error logs for more details. ProcessedItemCount=18061 ImportedItemCount=18050 ErrorCount=11; the table is
  ResourceNotFoundException with ItemCount=None (Scan COUNT=None).
  - ACK: terminal_codes, docs-only · ops: ImportTable, DescribeImport
  - repro: Upload 30x1MB DYNAMODB_JSON objects -> ImportTable -> at t+60s delete half + overwrite one -> poll
    to terminal
  - handling: not handled in the controller (as of commit 34b85e6)
  - related: [DDB-IMPORT-013](../import.md#ddb-import-013), [DDB-IMPORT-014](../import.md#ddb-import-014), [DDB-IMPORT-020](../import.md#ddb-import-020), [DDB-IMPORT-012](../import.md#ddb-import-012) · hypotheses: H-B-131 · evidence:
    import/state-machine/lifecycle

## Notes

Statuses seen for t4: [['IN_PROGRESS', 'ERR:ResourceNotFoundException'], ['IN_PROGRESS',
'ERR:ResourceNotFoundException'], ['IN_PROGRESS', 'CREATING'], ['IN_PROGRESS', 'CREATING'], ['IN_PROGRESS',
'CREATING'], ['IN_PROGRESS', 'CREATING'], ['FAILED', 'ACTIVE'], ['FAILED', 'ACTIVE'], ['FAILED', 'ACTIVE']]

Contradiction with [DDB-IMPORT-013](../import.md#ddb-import-013): 007's behavior ends with 'the table is ResourceNotFoundException with
ItemCount=None' after the FAILED import, implying FAILED removes the target; 007's own poll log recorded
(FAILED, ACTIVE) three times, and 013/014/020 show FAILED imports leave an ACTIVE table holding the
successfully imported rows Resolution: 013 canonical; 007's final table read is inconsistent with its own poll
log (likely taken after cleanup) - FAILED never deletes the target, deleting it on FAILED destroys partial
data
