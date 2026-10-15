<!-- generated: ack-api-quirks dynamodb 2026-10-09 model=2012-08-10 controller=34b85e6 -->
# DDB-TABLE-295: Per-replica KMSMasterKeyId rejected on an AWS-owned-key table; SSESpecification KMS on the base fans out to the replica (regional key)
_Full entry and notes of one finding; its summary entry is in [table-replicas.md](../table-replicas.md).
Generated from ack-api-quirks `services/dynamodb` (model 2012-08-10, controller commit 34b85e6); edit the
finding in the lab, not here._

## Finding

- <a id="ddb-table-295"></a>**DDB-TABLE-295** `cross-region` · impact medium · unhandled (not handled in controller) · verified 2026-10-09
  **Per-replica KMSMasterKeyId rejected on an AWS-owned-key table; SSESpecification KMS on the base fans out to the replica (regional key)**
  Base with default (AWS-owned) encryption and an ACTIVE replica: ReplicaUpdates=[Update{us-east-1,
  KMSMasterKeyId:'alias/aws/dynamodb'}] -> ValidationException 'One or more parameter values were invalid:
  KMSMasterKeyId must be specified for each replica.' UpdateTable SSESpecification{Enabled:true, SSEType:KMS}
  on the base -> 200; TableStatus stayed ACTIVE while SSEDescription.Status=UPDATING in BOTH regions;
  afterwards A.Replicas[us-east-1].KMSMasterKeyId = arn:aws:kms:us-east-1:...:key/<aws-managed key of
  us-east-1> and B.Replicas[us-west-2].KMSMasterKeyId = the us-west-2 AWS managed key ARN. Encryption type is
  group-wide; the key itself is regional and reported as a full ARN per replica.
  - ACK: custom_update, compare.is_ignored+delta_pre_compare · ops: UpdateTable, DescribeTable · fields:
    ReplicaUpdates.Update.KMSMasterKeyId, Replicas.KMSMasterKeyId, SSESpecification, SSEDescription
  - repro: UpdateTable ReplicaUpdates=[{Update:{RegionName:us-east-1, KMSMasterKeyId:alias/aws/dynamodb}}];
    UpdateTable SSESpecification{Enabled:true,SSEType:KMS}
  - handling: not handled in the controller (as of commit 34b85e6)
  - related: [DDB-TABLE-296](../table-replicas.md#ddb-table-296), [DDB-TABLE-251](../table-replicas.md#ddb-table-251), [DDB-TABLE-265](../table-replicas.md#ddb-table-265), [DDB-TABLE-294](../table-replicas.md#ddb-table-294), [DDB-TABLE-297](../table-replicas.md#ddb-table-297), [DDB-TABLE-322](../table-policy-kinesis-autoscaling.md#ddb-table-322),
    [DDB-TABLE-313](../table-replicas.md#ddb-table-313), [DDB-TABLE-309](../table-replicas.md#ddb-table-309), [DDB-TABLE-328](../table-streams-encryption-class.md#ddb-table-328), [DDB-TABLE-312](../table-replicas.md#ddb-table-312), [DDB-TABLE-310](../table-replicas.md#ddb-table-310), [DDB-TABLE-311](../table-replicas.md#ddb-table-311), [DDB-TABLE-329](../table-replicas.md#ddb-table-329),
    [DDB-TABLE-327](../table-replicas.md#ddb-table-327) · hypotheses: H-R-016 · evidence: table/mutation-matrix/replica-overrides

## Notes

Partially covers H-R-016: SSE type changes on the base fan out to replicas; the per-replica key is readable as
an ARN only (read-gap for alias inputs). CMK-per-replica rules are in
table/state-machine/replica-kms-lifecycle and replica-creation-failed. Note the misleading message text ('must
be specified for each replica') for a type-mixing error.
