<!-- generated: ack-api-quirks dynamodb 2026-10-09 model=2012-08-10 controller=34b85e6 -->
# DDB-IMPORT-012: ImportTable(bad bucket): 200 then FAILED S3NoSuchBucket in ~6s, no table created; empty prefix: COMPLETED with an empty ACTIVE table
_Full entry and notes of one finding; its summary entry is in [import.md](../import.md). Generated from
ack-api-quirks `services/dynamodb` (model 2012-08-10, controller commit 34b85e6); edit the finding in the lab,
not here._

## Finding

- <a id="ddb-import-012"></a>**DDB-IMPORT-012** `requested-vs-effective` · impact high · unhandled (not handled in controller) · verified 2026-10-09
  **ImportTable(bad bucket): 200 then FAILED S3NoSuchBucket in ~6s, no table created; empty prefix: COMPLETED with an empty ACTIVE table**
  ImportTable(S3Bucket=<nonexistent>) returned 200 ok (IN_PROGRESS). DescribeImport keeps TableArn=present.
  Final DescribeImport: bad-bucket: ImportStatus=FAILED FailureCode=S3NoSuchBucket Processed=0 Imported=0
  Errors=0; table=ResourceNotFoundException ItemCount=None scan=None (FailureMessage: The specified bucket
  does not exist (Service: Amazon S3; Status Code: 404; Error Code: NoSuchBucket; Request ID:
  YKWGHBH09YM211VG; S3 Extended Request ID: eW+Mqmru0bDZElFBows4BrnDVn0veDga4QbIP1UrxUrRa). Empty prefix case:
  empty-prefix: ImportStatus=COMPLETED FailureCode=None Processed=0 Imported=0 Errors=0; table=ACTIVE
  ItemCount=0 scan=0. Table first seen: bad-bucket=null empty-prefix={"elapsed_s": 261.5, "table_status":
  "ACTIVE", "import_status": "COMPLETED"}; table gone again at: bad-bucket=null empty-prefix=null.
  - ACK: terminal_codes, synced.when, custom_create · ops: ImportTable, DescribeImport, DescribeTable ·
    fields: S3BucketSource.S3Bucket, ImportTableDescription.FailureCode, ImportTableDescription.TableArn
  - repro: ImportTable(S3Bucket=does-not-exist) -> poll DescribeImport + DescribeTable every 15s to terminal
  - measurements: bad_bucket_failed_after_s=5.6, empty_prefix_completed_after_s=100.2
  - handling: not handled in the controller (as of commit 34b85e6)
  - related: [DDB-EXPORT-010](../export.md#ddb-export-010), [DDB-EXPORT-011](../export.md#ddb-export-011), [DDB-EXPORT-005](../export.md#ddb-export-005), [DDB-EXPORT-012](../export.md#ddb-export-012), [DDB-IMPORT-013](../import.md#ddb-import-013), [DDB-IMPORT-014](../import.md#ddb-import-014),
    [DDB-IMPORT-020](../import.md#ddb-import-020), [DDB-IMPORT-007](../import.md#ddb-import-007) · hypotheses: H-B-033, H-B-127 · evidence:
    import/error-taxonomy/failure-modes

## Notes

H-B-033 partially refuted (bad bucket does not leave an ACTIVE table; an empty prefix is COMPLETED not
FAILED); H-B-127 confirmed for the pre-copy failure: DescribeTable never succeeded for the bad-bucket target
although DescribeImport still returns TableArn and TableId (dangling reference). Service-side duration from
StartTime/EndTime: bad-bucket 5.6s; empty-prefix 100s.
