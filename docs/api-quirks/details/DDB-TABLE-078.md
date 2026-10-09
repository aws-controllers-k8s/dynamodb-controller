<!-- generated: ack-api-quirks dynamodb 2026-10-09 model=2012-08-10 controller=34b85e6 -->
# DDB-TABLE-078: KMS alias resolved once at Create/UpdateTable: repointing the alias does not move the table; re-sending the alias migrates it
_Full entry and notes of one finding; its summary entry is in
[table-streams-encryption-class.md](../table-streams-encryption-class.md). Generated from ack-api-quirks
`services/dynamodb` (model 2012-08-10, controller commit 34b85e6); edit the finding in the lab, not here._

## Finding

- <a id="ddb-table-078"></a>**DDB-TABLE-078** `normalization` · impact high · unhandled (not handled in controller) · verified 2026-10-08
  **KMS alias resolved once at Create/UpdateTable: repointing the alias does not move the table; re-sending the alias migrates it**
  CreateTable KMSMasterKeyId=alias -> SSEDescription.KMSMasterKeyArn = K1 ARN. Re-sending the same alias -> OK
  (response TableStatus=ACTIVE, response SSE={"Status": "UPDATING", "SSEType": "KMS", "KMSMasterKeyArn":
  "arn:aws:kms:us-west-2:<ACCOUNT>:key/f481fbde-3c2b-4538-b2da-34a4fbb96282"}; settled in 22.27s via
  ACTIVE/UPDATING->ACTIVE/ENABLED; after={"Status": "ENABLED", "SSEType": "KMS", "KMSMasterKeyArn":
  "arn:aws:kms:us-west-2:<ACCOUNT>:key/f481fbde-3c2b-4538-b2da-34a4fbb96282"}); same key as ARN ->
  ValidationException: 'One or more parameter values were invalid: Table is already encrypted with given
  KMSMasterKeyId. Use KMSMasterKeyId parameter if you want to change Master Key'; as key id -> OK (response
  TableStatus=ACTIVE, response SSE={"Status": "UPDATING", "SSEType": "KMS", "KMSMasterKeyArn":
  "arn:aws:kms:us-west-2:<ACCOUNT>:key/f481fbde-3c2b-4538-b2da-34a4fbb96282"}; settled in 21.26s via
  ACTIVE/UPDATING->ACTIVE/ENABLED; after={"Status": "ENABLED", "SSEType": "KMS", "KMSMasterKeyArn":
  "arn:aws:kms:us-west-2:<ACCOUNT>:key/f481fbde-3c2b-4538-b2da-34a4fbb96282"}). After kms:UpdateAlias to K2,
  DescribeTable watched for 150s: followed alias = False. Re-sending UpdateTable
  SSESpecification{Enabled:true,SSEType:KMS,KMSMasterKeyId:alias} -> OK (response TableStatus=ACTIVE, response
  SSE={"Status": "UPDATING", "SSEType": "KMS", "KMSMasterKeyArn":
  "arn:aws:kms:us-west-2:<ACCOUNT>:key/f481fbde-3c2b-4538-b2da-34a4fbb96282"}; settled in 22.27s via
  ACTIVE/UPDATING->ACTIVE/UPDATING->ACTIVE/ENABLED; after={"Status": "ENABLED", "SSEType": "KMS",
  "KMSMasterKeyArn": "arn:aws:kms:us-west-2:<ACCOUNT>:key/3ae36f3d-c39c-4ca2-ad5c-65b410eaa7ef"}). Final key
  is K2: True.
  - ACK: custom_update, compare.is_ignored+delta_pre_compare, references · ops: CreateTable, UpdateTable,
    DescribeTable · fields: SSESpecification.KMSMasterKeyId, SSEDescription.KMSMasterKeyArn
  - repro: CreateTable with KMSMasterKeyId=alias/x -> K1; kms update-alias alias/x -> K2; DescribeTable (still
    K1); UpdateTable with the same alias
  - measurements: alias_drift_watch_s=150, resend_alias_settle_s=22.27
  - handling: not handled in the controller (as of commit 34b85e6)
  - related: [DDB-TABLE-082](../table-streams-encryption-class.md#ddb-table-082), [DDB-TABLE-141](../table-streams-encryption-class.md#ddb-table-141), [DDB-TABLE-140](../table-streams-encryption-class.md#ddb-table-140), [DDB-TABLE-371](../table-streams-encryption-class.md#ddb-table-371), [DDB-TABLE-065](../table-streams-encryption-class.md#ddb-table-065), [DDB-TABLE-079](../table-streams-encryption-class.md#ddb-table-079),
    [DDB-TABLE-081](../table-streams-encryption-class.md#ddb-table-081), [DDB-TABLE-142](../table-streams-encryption-class.md#ddb-table-142), [DDB-TABLE-120](../table-streams-encryption-class.md#ddb-table-120), [DDB-TABLE-018](../table-streams-encryption-class.md#ddb-table-018) · evidence: table/mutation-matrix/sse-kms

## Notes

Hypotheses: H-T-109.

Contradiction with [DDB-TABLE-018](../table-streams-encryption-class.md#ddb-table-018), [DDB-TABLE-141](../table-streams-encryption-class.md#ddb-table-141), [DDB-TABLE-082](../table-streams-encryption-class.md#ddb-table-082), [DDB-TABLE-065](../table-streams-encryption-class.md#ddb-table-065): 018 title generalizes 'SSE
re-sends -> ValidationException' from its single Enabled:false cell ('Table is already encrypted by default');
141/082/078 show that re-sending {Enabled:true}, SSEType-only, alias or key-id returns 200, re-encrypts for
~21 s and burns one of the 4 daily SSE changes; only Enabled:false and the exact-ARN form are
ValidationException, and 065 shows an identical re-send during the SSE job is also 200 Resolution: keep all;
141/082 canonical for the SSE re-send rule; retitle 018
