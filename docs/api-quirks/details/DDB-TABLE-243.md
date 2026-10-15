<!-- generated: ack-api-quirks dynamodb 2026-10-09 model=2012-08-10 controller=34b85e6 -->
# DDB-TABLE-243: AAS delete-scaling-policy leaves a target that Describe shows as {"AutoScalingDisabled": true, "ScalingPolicies": []}; DynamoDB policy-on...
_Full entry and notes of one finding; its summary entry is in
[table-policy-kinesis-autoscaling.md](../table-policy-kinesis-autoscaling.md). Generated from ack-api-quirks
`services/dynamodb` (model 2012-08-10, controller commit 34b85e6); edit the finding in the lab, not here._

## Finding

- <a id="ddb-table-243"></a>**DDB-TABLE-243** `response-fidelity` · impact medium · unhandled (not handled in controller) · verified 2026-10-09
  **AAS delete-scaling-policy leaves a target that Describe shows as {"AutoScalingDisabled": true, "ScalingPolicies": []}; DynamoDB policy-on...**
  Write settings before {"MinimumUnits": 1, "MaximumUnits": 10, "AutoScalingRoleArn":
  "arn:aws:iam::<ACCOUNT>:role/<PRINCIPAL>", "ScalingPolicies": [{"PolicyName": "ackq-41e87e-w0",
  "TargetTrackingScalingPolicyConfi. After deleting the only AAS write policy: Describe
  {"AutoScalingDisabled": true, "ScalingPolicies": []}, AAS target {"MinCapacity": 1, "MaxCapacity": 10,
  "RoleARN": "arn:aws:iam::<ACCOUNT>:role/<PRINCIPAL>", "CreationTime": "2026-10-09T00:39:06.027000+00:00",
  "SuspendedState": {"DynamicScalingInSuspended": false, "DynamicScalingOutSuspended": false,
  "ScheduledScalingSuspended": false}}, alarms ['AlarmHigh-9cd234c1-1422-4227-95ac-7d1efa131896',
  'AlarmLow-2409d75e-f90e-4257-ad6f-853bc1796090',
  'ProvisionedCapacityHigh-c5dd6b0b-bf14-4ac6-9208-c9d91efea309',
  'ProvisionedCapacityLow-2c7e17a2-d7ef-49f2-a829-7bf5e47018aa']. UpdateTableReplicaAutoScaling write
  {ScalingPolicyUpdate only} -> {"operation": "update_table_replica_auto_scaling", "ok": false, "code":
  "ValidationException", "http_status": 400, "message": "Failed to update settings for global table with name:
  ‘ackq-41e87e-as-rt’: Parameters 'MaximumUnits', 'MinimumUnits' are required unless auto scaling is being
  disabled.", "latency_ms": 33, "client_side": false}; {Min,Max,policy} -> 200; after: {"describe":
  {"MinimumUnits": 1, "MaximumUnits": 10, "AutoScalingRoleArn": "arn:aws:iam::<ACCOUNT>:role/<PRINCIPAL>",
  "ScalingPolicies": [{"PolicyName": "DynamoDBWriteCapacityUtilization:table/ackq-41e87e-as-rt",
  "TargetTrackingScalingPolicyConfiguration": {"TargetValue": 70.0}}]}, "aas_policies":
  [["DynamoDBWriteCapacityUtilization:table/ackq-41e87e-as-rt", 70.0]], "aas_target": {"MinCap.
  - ACK: compare.nil_equals_zero_value, docs-only · ops: DescribeTableReplicaAutoScaling,
    UpdateTableReplicaAutoScaling, DeleteScalingPolicy · fields: ScalingPolicies, AutoScalingDisabled
  - repro: application-autoscaling delete-scaling-policy (write) -> DescribeTableReplicaAutoScaling ->
    UpdateTableReplicaAutoScaling write ScalingPolicyUpdate only
  - handling: not handled in the controller (as of commit 34b85e6)
  - related: [DDB-TABLE-242](../table-policy-kinesis-autoscaling.md#ddb-table-242), [DDB-TABLE-198](../table-policy-kinesis-autoscaling.md#ddb-table-198), [DDB-TABLE-290](../table-policy-kinesis-autoscaling.md#ddb-table-290), [DDB-TABLE-292](../table-replicas.md#ddb-table-292), [DDB-TABLE-323](../table-policy-kinesis-autoscaling.md#ddb-table-323), [DDB-TABLE-189](../table-policy-kinesis-autoscaling.md#ddb-table-189),
    [DDB-TABLE-195](../table-policy-kinesis-autoscaling.md#ddb-table-195), [DDB-TABLE-232](../table-replicas.md#ddb-table-232), [DDB-TABLE-317](../table-policy-kinesis-autoscaling.md#ddb-table-317), [DDB-TABLE-289](../table-replicas.md#ddb-table-289), [DDB-TABLE-237](../table-policy-kinesis-autoscaling.md#ddb-table-237), [DDB-TABLE-241](../table-policy-kinesis-autoscaling.md#ddb-table-241), [DDB-TABLE-253](../table-replicas.md#ddb-table-253),
    [DDB-TABLE-255](../table-replicas.md#ddb-table-255) · hypotheses: H-R-110, H-R-104 · evidence: table/round-trip/autoscaling-settings

## Notes

H-R-110's 'enabled but inert' state is not observable: a target without policies is rendered as
{AutoScalingDisabled:true, ScalingPolicies:[]} even though the AAS target (Min 1 Max 10) still exists and
keeps its ProvisionedCapacityHigh/Low alarms. The DynamoDB facade cannot add a policy to it without re-sending
Min/Max.
