<!-- generated: ack-api-quirks dynamodb 2026-10-09 model=2012-08-10 controller=34b85e6 -->
# DDB-IMPORT-009: ImportTable request validation: BillingMode/ProvisionedThroughput, PROVISIONED+OnDemandThroughput, CSV delimiter, options/format coupling
_Full entry and notes of one finding; its summary entry is in [import.md](../import.md). Generated from
ack-api-quirks `services/dynamodb` (model 2012-08-10, controller commit 34b85e6); edit the finding in the lab,
not here._

## Finding

- <a id="ddb-import-009"></a>**DDB-IMPORT-009** `request-validation` · impact medium · unhandled (not handled in controller) · verified 2026-10-09
  **ImportTable request validation: BillingMode/ProvisionedThroughput, PROVISIONED+OnDemandThroughput, CSV delimiter, options/format coupling**
  TableCreationParameters without BillingMode and ProvisionedThroughput -> 400 ValidationException 'One or
  more parameter values were invalid: ReadCapacityUnits and WriteCapacityUnits must both be specified when
  BillingMode is PROVISIONED'. PROVISIONED + OnDemandThroughput -> ValidationException 'One or more parameter
  values were invalid: MaxWriteRequestUnits for OnDemandThroughput cannot be specified when table BillingMode
  is PROVISIONED.'. CSV Delimiter=';;' -> ValidationException '2 validation errors detected: Value ';;' at
  'inputFormatOptions.csv.delimiter' failed to satisfy constraint: Member must satisfy regular expression
  pattern: [,;'. InputFormatOptions.Csv with InputFormat=DYNAMODB_JSON -> ValidationException 'Invalid
  Request: Unsupported InputFormatOptions for the given input format: DYNAMODB_JSON.'.
  InputCompressionType=BZIP2 -> ValidationException '1 validation error detected: Value 'BZIP2' at
  'inputCompressionType' failed to satisfy constraint: Member must satisfy enum value set: [ZSTD, NONE,
  GZIP]'. SSESpecification with a nonexistent KMS alias -> ValidationException 'KMS validation error:
  com.amazonaws.services.kms.model.NotFoundException: Alias
  arn:aws:kms:us-west-2:<ACCOUNT>:alias/ackq-does-not-exist-cc7907 is not found'.
  - ACK: terminal_codes, docs-only · ops: ImportTable · fields: TableCreationParameters.BillingMode,
    TableCreationParameters.ProvisionedThroughput, TableCreationParameters.OnDemandThroughput,
    InputFormatOptions.Csv.Delimiter, InputCompressionType,
    TableCreationParameters.SSESpecification.KMSMasterKeyId
  - repro: ImportTable with each invalid combination; record code/message
  - handling: not handled in the controller (as of commit 34b85e6)
  - hypotheses: H-B-035, H-B-135 · evidence: import/error-taxonomy/failure-modes

## Notes

H-B-035 confirmed: omitting BillingMode defaults to PROVISIONED and the request is then rejected for missing
ReadCapacityUnits/WriteCapacityUnits. H-B-135 confirmed: OnDemandThroughput with PROVISIONED is rejected
synchronously. SSESpecification with a nonexistent KMS alias is rejected synchronously (ValidationException
wrapping KMS NotFoundException), i.e. KMS keys ARE validated at ImportTable time unlike the S3 bucket.
