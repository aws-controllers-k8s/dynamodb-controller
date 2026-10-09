<!-- generated: ack-api-quirks dynamodb 2026-10-09 model=2012-08-10 controller=34b85e6 -->
# DDB-IMPORT-017: ImportTable ClientToken: identical replay -> same ARN; changed S3KeyPrefix silently ignored; changed TableName -> ImportConflictException
_Full entry and notes of one finding; its summary entry is in [import.md](../import.md). Generated from
ack-api-quirks `services/dynamodb` (model 2012-08-10, controller commit 34b85e6); edit the finding in the lab,
not here._

## Finding

- <a id="ddb-import-017"></a>**DDB-IMPORT-017** `idempotency` · impact high · unhandled (not handled in controller) · verified 2026-10-09
  **ImportTable ClientToken: identical replay -> same ARN; changed S3KeyPrefix silently ignored; changed TableName -> ImportConflictException**
  While the first import was IN_PROGRESS: identical replay -> 200 same ARN as #1 (IN_PROGRESS) (HTTP 200);
  same token with a different S3KeyPrefix -> 200 same ARN as #1 (IN_PROGRESS) ''; same token with a different
  TableName -> ImportConflictException 'Import conflict: Duplicate request detected with conflicting
  parameters'. After the import COMPLETED: identical replay -> ResourceInUseException; same token + other
  prefix -> 200 same ARN as #1 (FAILED). After the imported table was deleted, the identical replay -> 200
  same ARN as #1 (FAILED) (DescribeTable afterwards: ResourceNotFoundException).
  - ACK: custom_create, terminal_codes, requeue · ops: ImportTable · fields: ClientToken,
    S3BucketSource.S3KeyPrefix, TableCreationParameters.TableName
  - repro: ImportTable(token=T) -> replay same/changed params while IN_PROGRESS, after COMPLETED, and after
    DeleteTable of the target
  - handling: not handled in the controller (as of commit 34b85e6)
  - related: [DDB-EXPORT-009](../export.md#ddb-export-009), [DDB-EXPORT-010](../export.md#ddb-export-010), [DDB-IMPORT-018](../import.md#ddb-import-018), [DDB-IMPORT-019](../import.md#ddb-import-019), [DDB-IMPORT-001](../import.md#ddb-import-001) · hypotheses:
    H-B-018, H-B-036 · evidence: import/idempotency/client-token

## Notes

The doc promises 'IdempotentParameterMismatch'; the wire code is ImportConflictException ('Import conflict:
Duplicate request detected with conflicting parameters') and it fired only for a changed TableName, NOT for a
changed S3KeyPrefix (same ARN returned, change silently ignored). While the target table exists the
ResourceInUseException check precedes the token replay; once the table is deleted the token replays the old
(FAILED) import without creating anything, so a controller that deletes and re-creates must rotate the token.
