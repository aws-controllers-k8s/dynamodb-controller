<!-- generated: ack-api-quirks dynamodb 2026-10-09 model=2012-08-10 controller=34b85e6 -->
# DDB-TABLE-020: CreateTable with unusable KMS key is rejected synchronously (ValidationException, embedded KMS text), no table left behind
_Full entry and notes of one finding; its summary entry is in
[table-streams-encryption-class.md](../table-streams-encryption-class.md). Generated from ack-api-quirks
`services/dynamodb` (model 2012-08-10, controller commit 34b85e6); edit the finding in the lab, not here._

## Finding

- <a id="ddb-table-020"></a>**DDB-TABLE-020** `error-code` · impact high · unhandled (not handled in controller) · verified 2026-10-08
  **CreateTable with unusable KMS key is rejected synchronously (ValidationException, embedded KMS text), no table left behind**
  CreateTable(SSESpecification{Enabled,KMS,KMSMasterKeyId=<key>}) outcomes by key state: {'disabled':
  'ValidationException', 'pending_deletion': 'ValidationException', 'asymmetric_rsa2048':
  'InternalServerError', 'deny_creategrant': 'AccessDeniedException', 'other_region_arn':
  'ValidationException', 'nonexistent_keyid': 'ValidationException', 'nonexistent_alias':
  'ValidationException', 'disabled_by_alias': 'ValidationException'}. Messages: {'disabled': 'KMS key disabled
  error: com.amazonaws.services.kms.model.DisabledException:
  arn:aws:kms:us-west-2:<ACCOUNT>:key/1cb8c9d5-9941-4339-a86b-e80c2159783', 'pending_deletion': 'KMS
  validation error: com.amazonaws.services.kms.model.KMSInvalidStateException:
  arn:aws:kms:us-west-2:<ACCOUNT>:key/1ed86bdb-3db8-481c-922b-402e98', 'asymmetric_rsa2048': 'KMS internal
  error: com.amazonaws.services.kms.model.AWSKMSException: EncryptionContext is supported only when creating a
  grant for a symmetric encryp', 'deny_creategrant': 'KMS key access denied error:
  com.amazonaws.services.kms.model.AWSKMSException: User: arn:aws:sts::<ACCOUNT>:assumed-role/<PRINCIPAL> is',
  'other_region_arn': 'KMS validation error: com.amazonaws.services.kms.model.NotFoundException: Invalid arn
  us-east-1 (Service: AWSKMS; Status Code: 400; Error Code: NotFou', 'nonexistent_keyid': "KMS validation
  error: com.amazonaws.services.kms.model.NotFoundException: Key
  'arn:aws:kms:us-west-2:<ACCOUNT>:key/c9bfce6a-71e3-40d0-bfc5-f594a60e", 'nonexistent_alias': 'KMS validation
  error: com.amazonaws.services.kms.model.NotFoundException: Alias
  arn:aws:kms:us-west-2:<ACCOUNT>:alias/ackq-f1d9f4-nope is not found', 'disabled_by_alias': 'KMS key disabled
  error: com.amazonaws.services.kms.model.DisabledException:
  arn:aws:kms:us-west-2:<ACCOUNT>:key/1cb8c9d5-9941-4339-a86b-e80c2159783'}. DescribeTable right after each
  rejection: {'disabled': 'ERR:ResourceNotFoundException', 'pending_deletion':
  'ERR:ResourceNotFoundException', 'asymmetric_rsa2048': 'ERR:ResourceNotFoundException', 'deny_creategrant':
  'ERR:ResourceNotFoundException', 'other_region_arn': 'ERR:ResourceNotFoundException', 'nonexistent_keyid':
  'ERR:ResourceNotFoundException', 'nonexistent_alias': 'ERR:ResourceNotFoundException', 'disabled_by_alias':
  'ERR:ResourceNotFoundException'}.
  - ACK: terminal_codes, custom_create · ops: CreateTable · fields: SSESpecification.KMSMasterKeyId
  - repro: CreateTable with KMSMasterKeyId of a disabled / pending-deletion / RSA / CreateGrant-denied /
    nonexistent key
  - handling: not handled in the controller (as of commit 34b85e6)
  - related: [DDB-TABLE-080](../table-streams-encryption-class.md#ddb-table-080), [DDB-TABLE-021](../table-streams-encryption-class.md#ddb-table-021), [DDB-TABLE-139](../table-streams-encryption-class.md#ddb-table-139), [DDB-TABLE-022](../table-streams-encryption-class.md#ddb-table-022), [DDB-TABLE-023](../table-streams-encryption-class.md#ddb-table-023), [DDB-TABLE-037](../table-streams-encryption-class.md#ddb-table-037) ·
    evidence: table/error-taxonomy/kms-key-states

## Notes

H-T-111 CreateTable half. Causes are distinguishable only by message substring if the code is the same.
H-T-111 PARTIALLY REFUTED: disabled/pending-deletion/nonexistent/other-region keys -> ValidationException as
predicted, but an asymmetric key -> InternalServerError (HTTP 500) and a CreateGrant-denied key ->
AccessDeniedException (HTTP 400).
