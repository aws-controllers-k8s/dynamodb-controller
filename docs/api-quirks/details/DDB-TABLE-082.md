<!-- generated: ack-api-quirks dynamodb 2026-10-09 model=2012-08-10 controller=34b85e6 -->
# DDB-TABLE-082: Re-sending the current KMS key by ARN is a ValidationException no-op; by alias or key id it triggers a ~22s re-encryption
_Full entry and notes of one finding; its summary entry is in
[table-streams-encryption-class.md](../table-streams-encryption-class.md). Generated from ack-api-quirks
`services/dynamodb` (model 2012-08-10, controller commit 34b85e6); edit the finding in the lab, not here._

## Finding

- <a id="ddb-table-082"></a>**DDB-TABLE-082** `idempotency` · impact high · SUSPECTED CONTROLLER BUG · verified 2026-10-08
  **Re-sending the current KMS key by ARN is a ValidationException no-op; by alias or key id it triggers a ~22s re-encryption**
  Table encrypted with CMK K1 (created via alias). UpdateTable
  SSESpecification{Enabled:true,SSEType:KMS,KMSMasterKeyId:<K1 ARN>} -> ValidationException 'One or more
  parameter values were invalid: Table is already encrypted with given KMSMasterKeyId. Use KMSMasterKeyId
  parameter if you want to change Master Key'. The same with KMSMasterKeyId=<alias> or <key id> -> 200,
  response SSEDescription.Status=UPDATING (TableStatus ACTIVE), DescribeTable SSE Status UPDATING for 21-22s,
  final KMSMasterKeyArn unchanged. Each such re-send counts against the 4-per-24h encryption-change quota.
  - ACK: custom_update, compare.is_ignored+delta_pre_compare, references · ops: UpdateTable, DescribeTable ·
    fields: SSESpecification.KMSMasterKeyId, SSEDescription.KMSMasterKeyArn, SSEDescription.Status
  - repro: CMK table; UpdateTable SSESpecification with the same key as ARN / alias / key id; DescribeTable at
    1s
  - measurements: resend_alias_sse_updating_s=22.27, resend_keyid_sse_updating_s=21.26
  - handling: suspected controller bug - see Handling gaps
  - related: [DDB-TABLE-078](../table-streams-encryption-class.md#ddb-table-078), [DDB-TABLE-141](../table-streams-encryption-class.md#ddb-table-141), [DDB-TABLE-140](../table-streams-encryption-class.md#ddb-table-140), [DDB-TABLE-371](../table-streams-encryption-class.md#ddb-table-371), [DDB-TABLE-065](../table-streams-encryption-class.md#ddb-table-065), [DDB-TABLE-079](../table-streams-encryption-class.md#ddb-table-079),
    [DDB-TABLE-081](../table-streams-encryption-class.md#ddb-table-081), [DDB-TABLE-142](../table-streams-encryption-class.md#ddb-table-142), [DDB-TABLE-120](../table-streams-encryption-class.md#ddb-table-120), [DDB-TABLE-018](../table-streams-encryption-class.md#ddb-table-018) · evidence: table/mutation-matrix/sse-kms

## Notes

Only the ARN form is compared against SSEDescription.KMSMasterKeyArn server-side; a controller should resolve
alias/id to the ARN (kms:DescribeKey) before deciding whether to send SSESpecification.

Contradiction with [DDB-TABLE-018](../table-streams-encryption-class.md#ddb-table-018), [DDB-TABLE-141](../table-streams-encryption-class.md#ddb-table-141), [DDB-TABLE-078](../table-streams-encryption-class.md#ddb-table-078), [DDB-TABLE-065](../table-streams-encryption-class.md#ddb-table-065): 018 title generalizes 'SSE
re-sends -> ValidationException' from its single Enabled:false cell ('Table is already encrypted by default');
141/082/078 show that re-sending {Enabled:true}, SSEType-only, alias or key-id returns 200, re-encrypts for
~21 s and burns one of the 4 daily SSE changes; only Enabled:false and the exact-ARN form are
ValidationException, and 065 shows an identical re-send during the SSE job is also 200 Resolution: keep all;
141/082 canonical for the SSE re-send rule; retitle 018

Suspected controller bug confirmed by evidence: Re-sending the current key by alias or key id is accepted and
triggers a ~22 s re-encryption that counts toward the 4-per-24h SSE quota; only the ARN form is a rejected
no-op (082). Same on AWS-managed-key tables: re-sending {Enabled:true[,SSEType:KMS][,alias/aws/dynamodb]}
re-encrypts and burns quota (141); after 4 such re-sends every SSE change is LimitExceededException for 6 h
(081, 142). A perpetual alias/ID delta escalates to a quota lockout within 4 reconciles, exactly as suspected.
The same hazard applies to a spec with enabled:true and no key if the compare includes the observed
KMSMasterKeyArn-derived kmsMasterKeyID.
