<!-- generated: ack-api-quirks dynamodb 2026-10-09 model=2012-08-10 controller=34b85e6 -->
# DDB-TABLE-309: Per-replica CMK: bare key id accepted and read back as the full key ARN; ReplicaUpdates.Update KMSMasterKeyId is always rejected
_Full entry and notes of one finding; its summary entry is in [table-replicas.md](../table-replicas.md).
Generated from ack-api-quirks `services/dynamodb` (model 2012-08-10, controller commit 34b85e6); edit the
finding in the lab, not here._

## Finding

- <a id="ddb-table-309"></a>**DDB-TABLE-309** `normalization` · impact high · SUSPECTED CONTROLLER BUG · verified 2026-10-09
  **Per-replica CMK: bare key id accepted and read back as the full key ARN; ReplicaUpdates.Update KMSMasterKeyId is always rejected**
  Base encrypted with a us-west-2 CMK. Create us-east-1 with KMSMasterKeyId='<bare us-east-1 key id>' -> 200
  (response Replicas=[]); both ACTIVE after 41s (A UPDATING[] for 23s before the CREATING entry appeared).
  A.Replicas[us-east-1].KMSMasterKeyId = 'arn:aws:kms:us-east-1:<acct>:key/<id>' (full ARN); us-east-1
  SSEDescription.KMSMasterKeyArn = the same ARN; the replica's view of the base lists
  Replicas[us-west-2].KMSMasterKeyId = the us-west-2 CMK ARN. ReplicaUpdates=[Update{us-east-1,
  KMSMasterKeyId:<same ARN>}], [... <same bare id>] and [... 'alias/aws/dynamodb'] ALL fail with
  ValidationException 'One or more parameter values were invalid: KMSMasterKeyId must be specified for each
  replica.' (the same text an AWS-owned-key table returns, see table/mutation-matrix/replica-overrides).
  UpdateTable SSESpecification{Enabled:true, SSEType:KMS} (AWS managed, no key) on the base -> 200 with
  SSEDescription.Status=UPDATING in both regions; an SSESpecification change issued directly in us-east-1
  right after -> ResourceInUseException 'Server-Side Encryption is still being updated'. Earlier
  (table/state-machine/replica-creation-failed): Create without KMSMasterKeyId on a CMK table -> the same
  'must be specified for each replica' error; Create with 'alias/aws/dynamodb' on a CMK table -> 'All replica
  keys must either be Customer Managed CMK or AWS Managed CMK.'; Create with a us-west-2 key ARN/id for
  us-east-1 -> 'KMS validation error for region us-east-1: ... NotFoundException: Invalid arn us-west-2' /
  "Key 'arn:aws:kms:us-east-1:...:key/<id>' does not exist" (a bare id is resolved in the REPLICA region).
  - ACK: compare.is_ignored+delta_pre_compare, references, custom_update · ops: UpdateTable, DescribeTable ·
    fields: ReplicaUpdates.Create.KMSMasterKeyId, ReplicaUpdates.Update.KMSMasterKeyId,
    Replicas.KMSMasterKeyId, SSEDescription.KMSMasterKeyArn
  - repro: table with CMK SSE -> UpdateTable ReplicaUpdates=[Create us-east-1 KMSMasterKeyId=<bare id>] ->
    DescribeTable both regions -> Update variants
  - measurements: create_total_s=40.8
  - handling: suspected controller bug - see Handling gaps
  - related: [DDB-TABLE-226](../table-replicas.md#ddb-table-226), [DDB-TABLE-306](../table-policy-kinesis-autoscaling.md#ddb-table-306), [DDB-TABLE-265](../table-replicas.md#ddb-table-265), [DDB-TABLE-308](../table-replicas.md#ddb-table-308), [DDB-TABLE-329](../table-replicas.md#ddb-table-329), [DDB-TABLE-187](../table-replicas.md#ddb-table-187),
    [DDB-TABLE-305](../table-replicas.md#ddb-table-305), [DDB-TABLE-249](../table-replicas.md#ddb-table-249), [DDB-TABLE-258](../table-global-tables.md#ddb-table-258), [DDB-TABLE-313](../table-replicas.md#ddb-table-313), [DDB-TABLE-295](../table-replicas.md#ddb-table-295), [DDB-TABLE-328](../table-streams-encryption-class.md#ddb-table-328), [DDB-TABLE-312](../table-replicas.md#ddb-table-312),
    [DDB-TABLE-310](../table-replicas.md#ddb-table-310), [DDB-TABLE-311](../table-replicas.md#ddb-table-311), [DDB-TABLE-327](../table-replicas.md#ddb-table-327), [DDB-TABLE-204](../table-replicas.md#ddb-table-204), [DDB-TABLE-261](../table-global-tables.md#ddb-table-261), [DDB-TABLE-250](../table-replicas.md#ddb-table-250), [DDB-TABLE-294](../table-replicas.md#ddb-table-294),
    [DDB-TABLE-296](../table-replicas.md#ddb-table-296), [DDB-TABLE-221](../table-replicas.md#ddb-table-221) · hypotheses: H-R-016, H-R-012 · evidence:
    table/state-machine/replica-kms-lifecycle

## Notes

Confirms H-R-016: keys must resolve in the replica region, a bare id is accepted and normalised to an ARN
(spec never string-equals status), all replicas must share the key TYPE (all CMK or all AWS managed). Refutes
the assumption that the replica key can be rotated through ReplicaUpdates.Update - that path is rejected
unconditionally; a per-replica key change must be an SSESpecification UpdateTable in the replica region.
Qualifies H-R-012: the no-change rejection message is misleading.

Suspected controller bug confirmed by evidence: Split verdict, net confirmed. TableClass half REFUTED:
ReplicaTableClassSummary is absent for default STANDARD and appears only once a class is set explicitly
(294/201), so a spec without tableClassOverride is stable (nil vs STANDARD must still compare equal). KMS half
CONFIRMED: any bare key id/alias is read back as the full ARN (309), so a literal compare yields a perpetual
delta - and the resulting ReplicaUpdates.Update{KMSMasterKeyId} is rejected unconditionally (309/295), i.e.
terminal rather than merely unsynced. Further perpetual-delta sources: AAS materializes
Replicas[].ProvisionedThroughputOverride nobody requested (321) and an override equal to the base value
disappears from DescribeTable (252/250).
