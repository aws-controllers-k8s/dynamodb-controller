<!-- generated: ack-api-quirks dynamodb 2026-10-09 model=2012-08-10 controller=34b85e6 -->
# DDB-TABLE-313: CMK-encrypted table: every replica Create must carry a KMSMasterKeyId resolvable in the replica region; no mixing CMK and AWS-managed keys
_Full entry and notes of one finding; its summary entry is in [table-replicas.md](../table-replicas.md).
Generated from ack-api-quirks `services/dynamodb` (model 2012-08-10, controller commit 34b85e6); edit the
finding in the lab, not here._

## Finding

- <a id="ddb-table-313"></a>**DDB-TABLE-313** `normalization` · impact high · unhandled (not handled in controller) · verified 2026-10-09
  **CMK-encrypted table: every replica Create must carry a KMSMasterKeyId resolvable in the replica region; no mixing CMK and AWS-managed keys**
  Base with a us-west-2 CMK. Create us-east-1 WITHOUT KMSMasterKeyId -> ValidationException 'One or more
  parameter values were invalid: KMSMasterKeyId must be specified for each replica.' Create eu-west-1 with
  KMSMasterKeyId='alias/aws/dynamodb' -> ValidationException 'One or more parameter values were invalid: All
  replica keys must either be Customer Managed CMK or AWS Managed CMK.' Create us-east-1 with the us-west-2
  key ARN -> ValidationException 'KMS validation error for region us-east-1:
  com.amazonaws.services.kms.model.NotFoundException: Invalid arn us-west-2 ...'; with the bare us-west-2 key
  id -> ValidationException "KMS validation error for region us-east-1: ... Key
  'arn:aws:kms:us-east-1:<acct>:key/<id>' does not exist" (bare ids are resolved in the replica region). All
  synchronous, 1.7-3.6s latency.
  - ACK: compare.is_ignored+delta_pre_compare, references, custom_update · ops: UpdateTable, DescribeTable ·
    fields: ReplicaUpdates.Create.KMSMasterKeyId, Replicas.KMSMasterKeyId, SSEDescription.KMSMasterKeyArn
  - repro: table with SSE CMK -> UpdateTable ReplicaUpdates=[Create us-east-1] / [Create eu-west-1
    KMSMasterKeyId=alias/aws/dynamodb] -> DescribeTable everywhere
  - measurements: default_key_create_total_s=null
  - handling: not handled in the controller (as of commit 34b85e6)
  - related: [DDB-TABLE-309](../table-replicas.md#ddb-table-309), [DDB-TABLE-295](../table-replicas.md#ddb-table-295), [DDB-TABLE-328](../table-streams-encryption-class.md#ddb-table-328), [DDB-TABLE-312](../table-replicas.md#ddb-table-312), [DDB-TABLE-310](../table-replicas.md#ddb-table-310), [DDB-TABLE-311](../table-replicas.md#ddb-table-311),
    [DDB-TABLE-329](../table-replicas.md#ddb-table-329), [DDB-TABLE-327](../table-replicas.md#ddb-table-327) · hypotheses: H-R-016 · evidence: table/state-machine/replica-creation-failed

## Notes

Confirms H-R-016 (per-region key resolution; alias 'alias/aws/dynamodb' is NOT accepted on a CMK group, so
there is no AWS-managed default for a CMK table's replicas). For an AWS-owned-key table the replica Create
needs no key and any KMSMasterKeyId is rejected (table/mutation-matrix/replica-overrides).
