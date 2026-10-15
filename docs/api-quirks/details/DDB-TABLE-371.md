<!-- generated: ack-api-quirks dynamodb 2026-10-09 model=2012-08-10 controller=34b85e6 -->
# DDB-TABLE-371: KMS key change echoes the OLD KMSMasterKeyArn with Status=UPDATING for ~8-9 s in the UpdateTable response and DescribeTable
_Full entry and notes of one finding; its summary entry is in
[table-streams-encryption-class.md](../table-streams-encryption-class.md). Generated from ack-api-quirks
`services/dynamodb` (model 2012-08-10, controller commit 34b85e6); edit the finding in the lab, not here._

## Finding

- <a id="ddb-table-371"></a>**DDB-TABLE-371** `stale-response` · impact high · unhandled (not handled in controller) · verified 2026-10-09
  **KMS key change echoes the OLD KMSMasterKeyArn with Status=UPDATING for ~8-9 s in the UpdateTable response and DescribeTable**
  Table encrypted with the AWS-managed key. UpdateTable SSESpecification{Enabled:true, SSEType:KMS,
  KMSMasterKeyId:<CMK ARN>} -> 200, TableStatus=ACTIVE, response SSEDescription = {Status: UPDATING, SSEType:
  KMS, KMSMasterKeyArn: <OLD aws-managed ARN>}; DescribeTable keeps showing the OLD key with Status=UPDATING
  for 9.1 s, then the NEW key with Status=UPDATING for 14.2 s, then {ENABLED, KMS, <CMK>} at 23.3 s. The
  reverse change (KMSMasterKeyId=alias/aws/dynamodb) behaves identically: old CMK echoed for 8.1 s, new key
  UPDATING for 14.2 s, ENABLED at 22.3 s. By contrast CreateTable with SSESpecification returns {ENABLED, KMS,
  <ARN>} immediately in the create response.
  - ACK: synced.when, compare.is_ignored+delta_pre_compare, requeue · ops: UpdateTable, DescribeTable ·
    fields: SSESpecification.KMSMasterKeyId, SSEDescription.KMSMasterKeyArn, SSEDescription.Status
  - repro: CreateTable SSESpecification{Enabled:true,SSEType:KMS} -> wait ENABLED -> UpdateTable with a CMK
    ARN; record the response SSEDescription and poll DescribeTable at 1/s until Status=ENABLED with the new
    ARN; repeat back to alias/aws/dynamodb.
  - measurements: to_cmk_old_key_echo_s=9.1, to_cmk_new_key_updating_s=14.2, to_cmk_total_s=23.3,
    to_aws_managed_old_key_echo_s=8.1, to_aws_managed_total_s=22.3
  - handling: not handled in the controller (as of commit 34b85e6)
  - related: [DDB-TABLE-065](../table-streams-encryption-class.md#ddb-table-065), [DDB-TABLE-079](../table-streams-encryption-class.md#ddb-table-079), [DDB-TABLE-082](../table-streams-encryption-class.md#ddb-table-082), [DDB-TABLE-141](../table-streams-encryption-class.md#ddb-table-141), [DDB-TABLE-078](../table-streams-encryption-class.md#ddb-table-078), [DDB-TABLE-060](../table-throughput-billing.md#ddb-table-060),
    [DDB-TABLE-370](../table-throughput-billing.md#ddb-table-370), [DDB-TABLE-284](../table-streams-encryption-class.md#ddb-table-284), [DDB-TABLE-179](../table-throughput-billing.md#ddb-table-179), [DDB-TABLE-066](../table-throughput-billing.md#ddb-table-066), [DDB-TABLE-140](../table-streams-encryption-class.md#ddb-table-140), [DDB-TABLE-064](../table-throughput-billing.md#ddb-table-064), [DDB-TABLE-183](../table-throughput-billing.md#ddb-table-183),
    [DDB-TABLE-180](../table-streams-encryption-class.md#ddb-table-180), [DDB-TABLE-177](../table-streams-encryption-class.md#ddb-table-177), [DDB-TABLE-156](../table-throughput-billing.md#ddb-table-156), [DDB-TABLE-081](../table-streams-encryption-class.md#ddb-table-081), [DDB-TABLE-142](../table-streams-encryption-class.md#ddb-table-142), [DDB-TABLE-120](../table-streams-encryption-class.md#ddb-table-120) · evidence:
    table/creative/xs-kms-echo-tags

## Notes

Direct DynamoDB counterpart of the ElastiCache 'Modify response echoes the old Durability' seed. A reconciler
that reads back SSEDescription.KMSMasterKeyArn right after UpdateTable (or within ~9 s) sees the previous key
and would compute a delta and re-issue the change; the re-issue is rejected with ResourceInUseException while
SSEDescription.Status=UPDATING ([DDB-TABLE-065](../table-streams-encryption-class.md#ddb-table-065)) and, once ENABLED, a re-send by alias/key-id re-encrypts and
burns the 4-per-24h quota ([DDB-TABLE-082](../table-streams-encryption-class.md#ddb-table-082)/141). Gate the SSE delta on Status==ENABLED and compare the resolved
key ARN.
