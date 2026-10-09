<!-- generated: ack-api-quirks dynamodb 2026-10-09 model=2012-08-10 controller=34b85e6 -->
# DDB-TABLE-023: KMS key policy denying kms:CreateGrant -> AccessDeniedException (400) from CreateTable/UpdateTable, naming the caller's STS session
_Full entry and notes of one finding; its summary entry is in
[table-streams-encryption-class.md](../table-streams-encryption-class.md). Generated from ack-api-quirks
`services/dynamodb` (model 2012-08-10, controller commit 34b85e6); edit the finding in the lab, not here._

## Finding

- <a id="ddb-table-023"></a>**DDB-TABLE-023** `error-code` · impact medium · unhandled (not handled in controller) · verified 2026-10-08
  **KMS key policy denying kms:CreateGrant -> AccessDeniedException (400) from CreateTable/UpdateTable, naming the caller's STS session**
  With a key policy containing Deny kms:CreateGrant, CreateTable/UpdateTable(SSESpecification KMS <key>) fail
  synchronously with AccessDeniedException HTTP 400: 'KMS key access denied error:
  com.amazonaws.services.kms.model.AWSKMSException: User: arn:aws:sts::<ACCOUNT>:assumed-role/<PRINCIPAL> is
  not authorized to perform: kms:CreateGrant on resource:
  arn:aws:kms:us-west-2:<ACCOUNT>:key/5b40e09a-d91a-4e01-8216-4ddfbfa9362a with an explicit '. The grant is
  created with the caller's identity (message names the assumed-role session), so the controller's IAM
  principal must hold kms:CreateGrant on the key.
  - ACK: terminal_codes, docs-only · ops: CreateTable, UpdateTable · fields: SSESpecification.KMSMasterKeyId
  - repro: create-key with policy Deny kms:CreateGrant; CreateTable with KMSMasterKeyId=<key>
  - handling: not handled in the controller (as of commit 34b85e6)
  - related: [DDB-TABLE-020](../table-streams-encryption-class.md#ddb-table-020), [DDB-TABLE-080](../table-streams-encryption-class.md#ddb-table-080), [DDB-TABLE-021](../table-streams-encryption-class.md#ddb-table-021), [DDB-TABLE-139](../table-streams-encryption-class.md#ddb-table-139), [DDB-TABLE-022](../table-streams-encryption-class.md#ddb-table-022), [DDB-TABLE-037](../table-streams-encryption-class.md#ddb-table-037) ·
    evidence: table/error-taxonomy/kms-key-states

## Notes

Not ValidationException: generic 'AccessDenied' handling (often mapped to terminal) applies, which is correct
here.
