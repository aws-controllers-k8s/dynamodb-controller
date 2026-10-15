<!-- generated: ack-api-quirks dynamodb 2026-10-09 model=2012-08-10 controller=34b85e6 -->
# DDB-EXPORT-002: Export TableArn: other account -> AccessDeniedException; ARN region ignored, name looked up in endpoint region -> TableNotFoundException
_Full entry and notes of one finding; its summary entry is in [export.md](../export.md). Generated from
ack-api-quirks `services/dynamodb` (model 2012-08-10, controller commit 34b85e6); edit the finding in the lab,
not here._

## Finding

- <a id="ddb-export-002"></a>**DDB-EXPORT-002** `error-code` · impact medium · unhandled (not handled in controller) · verified 2026-10-09
  **Export TableArn: other account -> AccessDeniedException; ARN region ignored, name looked up in endpoint region -> TableNotFoundException**
  From the us-east-1 endpoint with a us-west-2 TableArn -> TableNotFoundException 'Table not found:
  arn:aws:dynamodb:us-west-2:<ACCOUNT>:table/ackq-86a5a7-exp-val'. TableArn with another account id ->
  AccessDeniedException 'Access is denied'. TableArn with another region -> TableNotFoundException 'Table not
  found: arn:aws:dynamodb:us-east-1:<ACCOUNT>:table/ackq-86a5a7-exp-val'. Index ARN -> ValidationException
  'Invalid Request: Table ARN is invalid.'. S3Bucket given as an S3 ARN -> ValidationException '1 validation
  error detected: Value 'arn:aws:s3:::ackq-86a5a7-bkt' at 's3Bucket' failed to satisfy co'; uppercase bucket
  name -> accepted(IN_PROGRESS); S3BucketOwner='not-an-account' -> ValidationException '1 validation error
  detected: Value 'not-an-account' at 's3BucketOwner' failed to satisfy constraint:'; ExportFormat=CSV ->
  ValidationException '1 validation error detected: Value 'CSV' at 'exportFormat' failed to satisfy
  constraint: Member must'; ExportType=PARTIAL_EXPORT -> ValidationException '1 validation error detected:
  Value 'PARTIAL_EXPORT' at 'exportType' failed to satisfy constraint: Me'; S3SseAlgorithm='aws:kms' ->
  ValidationException '1 validation error detected: Value 'aws:kms' at 's3SseAlgorithm' failed to satisfy
  constraint: Membe'.
  - ACK: terminal_codes, references · ops: ExportTableToPointInTime · fields: TableArn, S3Bucket,
    S3BucketOwner, ExportFormat, ExportType, S3SseAlgorithm
  - repro: ExportTableToPointInTime with cross-account / cross-region ARNs and malformed enum/bucket values
  - handling: not handled in the controller (as of commit 34b85e6)
  - related: [DDB-EXPORT-001](../export.md#ddb-export-001), [DDB-TABLE-447](../service.md#ddb-table-447), [DDB-BACKUP-012](../backup.md#ddb-backup-012), [DDB-TABLE-274](../table-restore.md#ddb-table-274), [DDB-TABLE-273](../table-restore.md#ddb-table-273), [DDB-TABLE-272](../table-restore.md#ddb-table-272) ·
    hypotheses: H-B-124 · evidence: export/error-taxonomy/sync-validation

## Notes

H-B-124 refuted on both counts: neither case is a ValidationException. The ARN's region component is not
validated (a us-east-1 ARN sent to us-west-2 fails with TableNotFoundException quoting the us-east-1 ARN; a
us-west-2 ARN sent to us-east-1 likewise), and a foreign account id yields AccessDeniedException (same for
DescribeImport/DescribeExport). Uppercase bucket names pass validation and fail asynchronously with
S3NoSuchBucket.
