<!-- generated: ack-api-quirks dynamodb 2026-10-09 model=2012-08-10 controller=34b85e6 -->
# DDB-TABLE-323: GSI autoscaling update while IndexStatus=CREATING -> RNF; the GSI is absent from Describe until ACTIVE; GSI delete orphans AAS
_Full entry and notes of one finding; its summary entry is in
[table-policy-kinesis-autoscaling.md](../table-policy-kinesis-autoscaling.md). Generated from ack-api-quirks
`services/dynamodb` (model 2012-08-10, controller commit 34b85e6); edit the finding in the lab, not here._

## Finding

- <a id="ddb-table-323"></a>**DDB-TABLE-323** `async-state-machine` · impact high · unhandled (not handled in controller) · verified 2026-10-09
  **GSI autoscaling update while IndexStatus=CREATING -> RNF; the GSI is absent from Describe until ACTIVE; GSI delete orphans AAS**
  0.8s after UpdateTable created gsi1: write update -> {"operation": "update_table_replica_auto_scaling",
  "ok": false, "code": "ResourceNotFoundException", "http_status": 400, "message": "Failed to update settings
  for global table with name: ‘ackq-b9a764-as-gsiadd’ because the global secondary indexes with names:
  ‘[gsi1]’ do not exist.", "latency_ms": 778, "client_side": false}; read update -> {"operation":
  "update_table_replica_auto_scaling", "ok": false, "code": "ResourceNotFoundException", "http_status": 400,
  "message": "Failed to update settings for global table with name: ‘ackq-b9a764-as-gsiadd’ because a global
  secondary index with name: ‘gsi1’ does not exist in region: ‘us-west-2’.", "latency_ms": 722, "client_side":
  false}. Describe while CREATING: gsi1 entry null (region B null; TableStatus ACTIVE); AAS targets A [["/",
  "t:W", 1, 10]], B [["/", "t:W", 1, 10]]. GSI create timeline [{"value": "UPDATING|rep:ACTIVE|gsi:",
  "from_s": 0.16, "to_s": 31.35, "duration_s": 31.19}, {"value": "UPDATING|rep:ACTIVE|gsi:gsi1=CREATING",
  "from_s": 31.35, "to_s": 57.27, "duration_s": 25.92}, {"value": "ACTIVE|rep:ACTIVE|gsi:gsi1=CREATING",
  "from_s": 57.27, "to_s": 534.9, "duration_s": 477.63},. After ACTIVE: entry {"IndexName": "gsi1",
  "IndexStatus": "ACTIVE", "ProvisionedReadCapacityAutoScalingSettings": {"AutoScalingDisabled": true,
  "ScalingPolicies": []}, "ProvisionedWriteCapacityAutoScalingSettings": {"MinimumUnits": 1, "MaximumUnits":
  10, "AutoScalingRoleArn": "arn:aws:iam::<ACCOUNT>:role/<PRINCIPAL> (B {"IndexName": "gsi1", "IndexStatus":
  "ACTIVE", "ProvisionedReadCapacityAutoScalingSettings": {"AutoScalingDisabled": true, "ScalingPolicies":
  []}, "ProvisionedWriteCapacityAutoScalingSettings": {"Mini); targets A [["/", "t:W", 1, 10], ["/index/gsi1",
  "i:W", 1, 10]], B [["/", "t:W", 1, 10], ["/index/gsi1", "i:W", 1, 10]]. Final entry {"IndexName": "gsi1",
  "IndexStatus": "ACTIVE", "ProvisionedReadCapacityAutoScalingSettings": {"MinimumUnits": 1, "MaximumUnits":
  10, "AutoScalingRoleArn": "arn:aws:iam::<ACCOUNT>:role/<PRINCIPAL> targets A [["/", "t:W", 1, 10],
  ["/index/gsi1", "i:R", 1, 10], ["/index/gsi1", "i:W", 1, 12]] B [["/", "t:W", 1, 10], ["/index/gsi1", "i:W",
  1, 12]]. UpdateTable delete gsi1 -> 200; targets after GSI delete A [["/", "t:W", 1, 10], ["/index/gsi1",
  "i:R", 1, 10], ["/index/gsi1", "i:W", 1, 12]] B [["/", "t:W", 1, 10], ["/index/gsi1", "i:W", 1, 12]]; entry
  null.
  - ACK: requeue, terminal_codes, pre-delete-cleanup · ops: UpdateTable, UpdateTableReplicaAutoScaling,
    DescribeTableReplicaAutoScaling · fields: GlobalSecondaryIndexUpdates, IndexStatus,
    ReplicaGlobalSecondaryIndexUpdates
  - repro: UpdateTable add GSI -> UpdateTableReplicaAutoScaling for it immediately -> poll IndexStatus ->
    retry -> UpdateTable delete GSI -> describe-scalable-targets
  - handling: not handled in the controller (as of commit 34b85e6)
  - related: [DDB-TABLE-187](../table-replicas.md#ddb-table-187), [DDB-TABLE-232](../table-replicas.md#ddb-table-232), [DDB-TABLE-306](../table-policy-kinesis-autoscaling.md#ddb-table-306), [DDB-TABLE-242](../table-policy-kinesis-autoscaling.md#ddb-table-242), [DDB-TABLE-198](../table-policy-kinesis-autoscaling.md#ddb-table-198), [DDB-TABLE-290](../table-policy-kinesis-autoscaling.md#ddb-table-290),
    [DDB-TABLE-292](../table-replicas.md#ddb-table-292), [DDB-TABLE-243](../table-policy-kinesis-autoscaling.md#ddb-table-243) · hypotheses: H-R-108 · evidence:
    table/creative/gsi-add-on-provisioned-global-table

## Notes

H-R-108 confirmed for the error code (ResourceNotFoundException 'the global secondary indexes with names:
[gsi1] do not exist' / '... does not exist in region', not ResourceInUseException) and for 'succeeds once
ACTIVE' (write and read updates -> 200). New: the CREATING index is not listed under
Replicas[].GlobalSecondaryIndexes at all (entry absent, TableStatus reported ACTIVE by the autoscaling
Describe while DescribeTable said UPDATING), so a controller cannot even observe IndexStatus=CREATING through
this API. UpdateTable(GlobalSecondaryIndexUpdates Delete gsi1) left the GSI's AAS targets (index read+write in
A, index write in B) and policies orphaned.
