<!-- generated: ack-api-quirks dynamodb 2026-10-09 model=2012-08-10 controller=34b85e6 -->
# DDB-TABLE-189: Autoscaling never configured reads as {"AutoScalingDisabled": true, "ScalingPolicies": []}; AAS-created custom policy round-trips: diff {...
_Full entry and notes of one finding; its summary entry is in
[table-policy-kinesis-autoscaling.md](../table-policy-kinesis-autoscaling.md). Generated from ack-api-quirks
`services/dynamodb` (model 2012-08-10, controller commit 34b85e6); edit the finding in the lab, not here._

## Finding

- <a id="ddb-table-189"></a>**DDB-TABLE-189** `response-fidelity` · impact medium · unhandled (not handled in controller) · verified 2026-10-09
  **Autoscaling never configured reads as {"AutoScalingDisabled": true, "ScalingPolicies": []}; AAS-created custom policy round-trips: diff {...**
  2-replica PROVISIONED table: description keys ['Replicas', 'TableName', 'TableStatus']; replica keys
  ['GlobalSecondaryIndexes', 'RegionName', 'ReplicaProvisionedReadCapacityAutoScalingSettings',
  'ReplicaProvisionedWriteCapacityAutoScalingSettings', 'ReplicaStatus']; read settings (never configured)
  {"AutoScalingDisabled": true, "ScalingPolicies": []}; write settings (created via AAS with custom
  PolicyName, cooldowns 120/60, DisableScaleIn true, target 55) {"MinimumUnits": 1, "MaximumUnits": 10,
  "AutoScalingRoleArn": "arn:aws:iam::<ACCOUNT>:role/<PRINCIPAL>", "ScalingPolicies": [{"PolicyName":
  "ackq-0ad4a5-custom-write-policy", "TargetTrackingScalingPolicyConfiguration": {"DisableScaleIn": true,
  "ScaleInCooldown": 120, "ScaleOutCooldown": 60, "TargetValue": 55.0}}]}; region-B write settings
  {"MinimumUnits": 1, "MaximumUnits": 10, "AutoScalingRoleArn": "arn:aws:iam::<ACCOUNT>:role/<PRINCIPAL>",
  "ScalingPolicies": [{"PolicyName": "ackq-0ad4a5-custom-write-policy", "TargetTrackingS. Replica order from
  us-west-2 ['us-east-1', 'us-west-2'], from us-east-1 ['us-east-1', 'us-west-2'] (identical payload: True).
  PAY_PER_REQUEST global table Describe: {"TableName": "ackq-0ad4a5-as-ppr", "TableStatus": "ACTIVE",
  "Replicas": [{"RegionName": "us-east-1", "GlobalSecondaryIndexes": [],
  "ReplicaProvisionedReadCapacityAutoScalingSettings": {"AutoScalingDisabled": true, "ScalingPolicies": []},
  "ReplicaProvisionedWriteCapacityAutoScalingSettings": {"AutoScalingDisabled": true, "ScalingPolicies": []},
  "ReplicaStatus": "ACTIVE"}, {"RegionName": "us-west-2", "GlobalSecondaryIndexes": [],
  "ReplicaProvisionedReadCapacityAutoScalingSettings": {"AutoScalingD.
  - ACK: compare.nil_equals_zero_value, scope:skip · ops: DescribeTableReplicaAutoScaling · fields:
    ReplicaProvisionedReadCapacityAutoScalingSettings, ReplicaProvisionedWriteCapacityAutoScalingSettings,
    ScalingPolicies
  - repro: AAS register+put-scaling-policy (write) -> add replica -> DescribeTableReplicaAutoScaling from both
    regions
  - handling: not handled in the controller (as of commit 34b85e6)
  - related: [DDB-TABLE-195](../table-policy-kinesis-autoscaling.md#ddb-table-195), [DDB-TABLE-243](../table-policy-kinesis-autoscaling.md#ddb-table-243), [DDB-TABLE-232](../table-replicas.md#ddb-table-232), [DDB-TABLE-317](../table-policy-kinesis-autoscaling.md#ddb-table-317), [DDB-TABLE-289](../table-replicas.md#ddb-table-289), [DDB-TABLE-237](../table-policy-kinesis-autoscaling.md#ddb-table-237),
    [DDB-TABLE-240](../table-policy-kinesis-autoscaling.md#ddb-table-240), [DDB-TABLE-239](../table-replicas.md#ddb-table-239), [DDB-TABLE-191](../table-policy-kinesis-autoscaling.md#ddb-table-191), [DDB-TABLE-196](../table-policy-kinesis-autoscaling.md#ddb-table-196), [DDB-TABLE-190](../table-replicas.md#ddb-table-190),
    [DDB-TABLEREPLICAAUTOSCALING-001](../table-replicas.md#ddb-tablereplicaautoscaling-001), [DDB-TABLE-193](../table-policy-kinesis-autoscaling.md#ddb-table-193), [DDB-TABLE-197](../table-policy-kinesis-autoscaling.md#ddb-table-197), [DDB-TABLE-292](../table-replicas.md#ddb-table-292), [DDB-TABLE-265](../table-replicas.md#ddb-table-265), [DDB-TABLE-229](../table-global-tables.md#ddb-table-229)
    · hypotheses: H-R-102, H-R-104, H-S-040 · evidence: table/sub-resources/replica-autoscaling-facade
