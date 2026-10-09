<!-- generated: ack-api-quirks dynamodb 2026-10-09 model=2012-08-10 controller=34b85e6 -->
# DDB-TABLE-037: CreateTable with a non-existent KMS alias or another account's key ARN: codes and messages
_Full entry and notes of one finding; its summary entry is in
[table-streams-encryption-class.md](../table-streams-encryption-class.md). Generated from ack-api-quirks
`services/dynamodb` (model 2012-08-10, controller commit 34b85e6); edit the finding in the lab, not here._

## Finding

- <a id="ddb-table-037"></a>**DDB-TABLE-037** `error-code` · impact medium · unhandled (not handled in controller) · verified 2026-10-08
  **CreateTable with a non-existent KMS alias or another account's key ARN: codes and messages**
  KMSMasterKeyId=alias/ackq-does-not-exist -> ValidationException: 'KMS validation error:
  com.amazonaws.services.kms.model.NotFoundException: Alias
  arn:aws:kms:us-west-2:<ACCOUNT>:alias/ackq-does-not-exist is not found. (Service: AWSKMS; Status Code: 400;
  Error Code: NotFoundException; Request ID: 9122b94b-c5e1-468d-8fdd-b1d4fed10e13; Proxy: null)'.
  KMSMasterKeyId=<other-account key ARN> -> AccessDeniedException: 'KMS key access denied error:
  com.amazonaws.services.kms.model.AWSKMSException: User: arn:aws:sts::<ACCOUNT>:assumed-role/<PRINCIPAL> is
  not authorized to perform: kms:DescribeKey on this resource because the resource does not exist in this
  Region, no resource-based policies allow acce'.
  - ACK: terminal_codes · ops: CreateTable · fields: SSESpecification.KMSMasterKeyId
  - repro: CreateTable SSESpecification={Enabled:true,SSEType:KMS,KMSMasterKeyId:'alias/does-not-exist'}
  - handling: not handled in the controller (as of commit 34b85e6)
  - related: [DDB-TABLE-020](../table-streams-encryption-class.md#ddb-table-020), [DDB-TABLE-080](../table-streams-encryption-class.md#ddb-table-080), [DDB-TABLE-021](../table-streams-encryption-class.md#ddb-table-021), [DDB-TABLE-139](../table-streams-encryption-class.md#ddb-table-139), [DDB-TABLE-022](../table-streams-encryption-class.md#ddb-table-022), [DDB-TABLE-023](../table-streams-encryption-class.md#ddb-table-023) ·
    evidence: table/weird-inputs/create-validation

## Notes

Hypotheses: H-T-111.
