<!-- generated: ack-api-quirks dynamodb 2026-10-09 model=2012-08-10 controller=34b85e6 -->
# DDB-TABLE-461: Unusable KMS key taxonomy by key type: HMAC and RSA keys -> HTTP 500 on Create/Update/RestoreFromBackup/RestoreToPointInTime...
_Full entry and notes of one finding; its summary entry is in
[table-streams-encryption-class.md](../table-streams-encryption-class.md). Generated from ack-api-quirks
`services/dynamodb` (model 2012-08-10, controller commit 34b85e6); edit the finding in the lab, not here._

## Finding

- <a id="ddb-table-461"></a>**DDB-TABLE-461** `error-code` · impact high · unhandled (not handled in controller) · verified 2026-10-09
  **Unusable KMS key taxonomy by key type: HMAC and RSA keys -> HTTP 500 on Create/Update/RestoreFromBackup/RestoreToPointInTime...**
  The wire code for an unusable KMSMasterKeyId depends on WHY the key is unusable, not on the operation: a
  symmetric-encryption-incapable key (HMAC_256 GENERATE_VERIFY_MAC, like the known RSA_2048 case) -> HTTP 500
  InternalServerError 'KMS internal error: com.amazonaws.services.kms.model.AWSKMSException: EncryptionContext
  is supported only when creating a grant for a symmetric encryption KMS key. (Service: AWSKMS; Status Code:
  400; Error Code: ValidationException; Request ID: <uuid>; Proxy: null)' on CreateTable, UpdateTable,
  RestoreTableFromBackup AND RestoreTableToPointInTime (SSESpecificationOverride), deterministic (6/6, 30-200
  ms); another service's AWS-managed key ('alias/aws/s3') -> HTTP 400 AccessDeniedException 'KMS key access
  denied error: ...AWSKMSException: User: <caller ARN> is not authorized to perform: kms:CreateGrant on
  resource: <key ARN> because no resource-based policy allows the kms:CreateGrant action (...)'; a disabled
  key -> 400 ValidationException 'KMS key disabled error: ...DisabledException: <key ARN> is disabled...'; a
  nonexistent key ARN -> 400 ValidationException 'KMS validation error: ...NotFoundException: Key '<arn>' does
  not exist...'. In every case the target/new table does NOT exist afterwards (checked at +0 s, +1 s and in a
  later sweep) and an existing table keeps its SSEDescription; an immediate retry of the 500 returns the same
  500 and a corrected retry to the same target name is accepted (200). All three codes are PERMANENT-SPEC for
  the controller; the 500 and the AccessDenied would be misclassified as 'transient' / 'IAM problem' by a
  code-based mapper - the stable substrings are 'EncryptionContext is supported only when creating a grant for
  a symmetric encryption KMS key', 'kms:CreateGrant', 'is disabled', 'does not exist'. Every text carries a
  per-request KMS request id (uuid).
  - ACK: terminal_codes, requeue · ops: CreateTable, UpdateTable, RestoreTableFromBackup,
    RestoreTableToPointInTime · fields: SSESpecification.KMSMasterKeyId,
    SSESpecificationOverride.KMSMasterKeyId
  - repro: CreateKey KeySpec=HMAC_256 KeyUsage=GENERATE_VERIFY_MAC; RestoreTableFromBackup
    SSESpecificationOverride={Enabled:true,SSEType:KMS,KMSMasterKeyId:<hmac arn>} -> 500; UpdateTable
    SSESpecification KMSMasterKeyId=alias/aws/s3 -> AccessDenied
  - handling: not handled in the controller (as of commit 34b85e6)
  - related: [DDB-TABLE-022](../table-streams-encryption-class.md#ddb-table-022), [DDB-TABLE-080](../table-streams-encryption-class.md#ddb-table-080), [DDB-TABLE-023](../table-streams-encryption-class.md#ddb-table-023), [DDB-TABLE-037](../table-streams-encryption-class.md#ddb-table-037), [DDB-TABLE-463](../table-restore.md#ddb-table-463), [DDB-TABLE-457](../service.md#ddb-table-457),
    [DDB-TABLE-276](../table-restore.md#ddb-table-276), [DDB-TABLE-219](../table-restore.md#ddb-table-219) · evidence: table/creative/restore-5xx-side-effect,
    table/creative/degenerate-5xx-hunt

## Notes

Extends [DDB-TABLE-022](../table-streams-encryption-class.md#ddb-table-022)/080 (RSA_2048 -> 500 on Create/Update) to HMAC keys and to both restore paths, and adds
the AccessDenied class for foreign AWS-managed keys. Rows (this probe):
rst_bk_hmac -> 500 InternalServerError 63ms 'KMS internal error:
com.amazonaws.services.kms.model.AWSKMSException: EncryptionContext is supported only when creating a grant
for a symmetric encryption KMS key. (Service: AWSKMS; Status Code: 400; Error Code: ValidationException;
Request ID: 258de1ca-3d57-4e6d-8533-800eba0eecad; Proxy: null)'
rst_bk_hmac_retry_same_target -> 500 InternalServerError 32ms 'KMS internal error:
com.amazonaws.services.kms.model.AWSKMSException: EncryptionContext is supported only when creating a grant
for a symmetric encryption KMS key. (Service: AWSKMS; Status Code: 400; Error Code: ValidationException;
Request ID: dda009f8-bfe0-4658-81b2-26f0f8885f13; Proxy: null)'
rst_bk_rsa -> 500 InternalServerError 35ms 'KMS internal error:
com.amazonaws.services.kms.model.AWSKMSException: EncryptionContext is supported only when creating a grant
for a symmetric encryption KMS key. (Service: AWSKMS; Status Code: 400; Error Code: ValidationException;
Request ID: d382b9f5-616d-4d67-a4a7-a66191d55829; Proxy: null)'
rst_bk_disabled -> 400 ValidationException 28ms 'KMS key disabled error:
com.amazonaws.services.kms.model.DisabledException:
arn:aws:kms:us-west-2:<ACCOUNT>:key/0d63bd02-afec-4e02-b215-48e1b2054618 is disabled. (Service: AWSKMS; Status
Code: 400; Error Code: DisabledException; Request ID: e7d8541b-033d-4089-86ae-5329ccf2cf2b; Proxy: null)'
rst_bk_nokey -> 400 ValidationException 22ms 'KMS validation error:
com.amazonaws.services.kms.model.NotFoundException: Key
'arn:aws:kms:us-west-2:<ACCOUNT>:key/00000000-0000-4000-8000-<ACCOUNT>' does not exist (Service: AWSKMS;
Status Code: 400; Error Code: NotFoundException; Request ID: a9386632-acbb-4aee-a835-668201a9d975; Proxy:
null)'
rst_pitr_hmac -> 500 InternalServerError 66ms 'KMS internal error:
com.amazonaws.services.kms.model.AWSKMSException: EncryptionContext is supported only when creating a grant
for a symmetric encryption KMS key. (Service: AWSKMS; Status Code: 400; Error Code: ValidationException;
Request ID: 31608b6f-e7f6-4262-ac74-6d02796d2ad0; Proxy: null)'
ct_hmac -> 500 InternalServerError 39ms 'KMS internal error: com.amazonaws.services.kms.model.AWSKMSException:
EncryptionContext is supported only when creating a grant for a symmetric encryption KMS key. (Service:
AWSKMS; Status Code: 400; Error Code: ValidationException; Request ID: 3f52ba66-0caf-4d5f-874f-b5c721f4aac1;
Proxy: null)'
ut_hmac_on_src -> 500 InternalServerError 192ms 'KMS internal error:
com.amazonaws.services.kms.model.AWSKMSException: EncryptionContext is supported only when creating a grant
for a symmetric encryption KMS key. (Service: AWSKMS; Status Code: 400; Error Code: ValidationException;
Request ID: d498af9c-a220-4b25-9ea1-8fbc1800d99d; Proxy: null)'
Rows (table/creative/degenerate-5xx-hunt):
ut_sse_alias_aws_s3 -> 400 AccessDeniedException 189ms 'KMS key access denied error:
com.amazonaws.services.kms.model.AWSKMSException: User: arn:aws:sts::<ACCOUNT>:assumed-role/<PRINCIPAL> is not
authorized to perform: kms:CreateGrant on resource:
arn:aws:kms:us-west-2:<ACCOUNT>:key/3b1a7419-d170-49d4-9a64-59dee41e3eea because no resource-based policy
allows the kms'
ct_sse_alias_aws_s3 -> 400 AccessDeniedException 41ms 'KMS key access denied error:
com.amazonaws.services.kms.model.AWSKMSException: User: arn:aws:sts::<ACCOUNT>:assumed-role/<PRINCIPAL> is not
authorized to perform: kms:CreateGrant on resource:
arn:aws:kms:us-west-2:<ACCOUNT>:key/3b1a7419-d170-49d4-9a64-59dee41e3eea because no resource-based policy
allows the kms'
ut_sse_hmac_key -> 500 InternalServerError 187ms 'KMS internal error:
com.amazonaws.services.kms.model.AWSKMSException: EncryptionContext is supported only when creating a grant
for a symmetric encryption KMS key. (Service: AWSKMS; Status Code: 400; Error Code: ValidationException;
Request ID: 234f7469-2126-4278-8828-189e7e30a883; Proxy: null)'
ct_sse_hmac_key -> 500 InternalServerError 35ms 'KMS internal error:
com.amazonaws.services.kms.model.AWSKMSException: EncryptionContext is supported only when creating a grant
for a symmetric encryption KMS key. (Service: AWSKMS; Status Code: 400; Error Code: ValidationException;
Request ID: 8eef99af-2e8d-4ed8-aa65-e2277a81bf8b; Proxy: null)'
rst_sse_override_hmac -> 500 InternalServerError 48ms 'KMS internal error:
com.amazonaws.services.kms.model.AWSKMSException: EncryptionContext is supported only when creating a grant
for a symmetric encryption KMS key. (Service: AWSKMS; Status Code: 400; Error Code: ValidationException;
Request ID: 6b86bb8d-c0a5-44be-9035-eefa95788b97; Proxy: null)'
Target existence after each failed restore: {"rst_bk_hmac": {"exists": false, "code":
"ResourceNotFoundException"}, "rst_bk_rsa": {"exists": false, "code": "ResourceNotFoundException"},
"rst_bk_disabled": {"exists": false, "code": "ResourceNotFoundException"}, "rst_bk_nokey": {"exists": false,
"code": "ResourceNotFoundException"}, "rst_pitr_hmac": {"exists": false, "code": "ResourceNotFoundException"}}
Note: in degenerate-5xx-hunt the restore target DID appear right after the HMAC 500 - isolated in
table/creative/restore-odt-empty-side-effect: it was the NEXT call (OnDemandThroughputOverride={}) that
created it, not the 500.
