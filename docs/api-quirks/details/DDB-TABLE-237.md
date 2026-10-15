<!-- generated: ack-api-quirks dynamodb 2026-10-09 model=2012-08-10 controller=34b85e6 -->
# DDB-TABLE-237: Minimal autoscaling update reads back with server PolicyName 'DynamoDB<Read|Write>CapacityUtilization:table/<name>', SLR role, no cooldowns
_Full entry and notes of one finding; its summary entry is in
[table-policy-kinesis-autoscaling.md](../table-policy-kinesis-autoscaling.md). Generated from ack-api-quirks
`services/dynamodb` (model 2012-08-10, controller commit 34b85e6); edit the finding in the lab, not here._

## Finding

- <a id="ddb-table-237"></a>**DDB-TABLE-237** `server-default` · impact medium · unhandled (not handled in controller) · verified 2026-10-09
  **Minimal autoscaling update reads back with server PolicyName 'DynamoDB<Read|Write>CapacityUtilization:table/<name>', SLR role, no cooldowns**
  After UpdateTableReplicaAutoScaling(read Min 1 Max 10 TargetValue 70) Describe shows settings keys
  ['AutoScalingRoleArn', 'MaximumUnits', 'MinimumUnits', 'ScalingPolicies'], policy keys ['PolicyName',
  'TargetTrackingScalingPolicyConfiguration'], TargetTracking keys ['TargetValue'], AutoScalingDisabled
  present: False, PolicyName DynamoDBReadCapacityUtilization:table/ackq-41e87e-as-rt, AutoScalingRoleArn is
  the service-linked role: True. AAS view of the target {"MinCapacity": 1, "MaxCapacity": 10, "RoleARN":
  "arn:aws:iam::<ACCOUNT>:role/<PRINCIPAL>", "CreationTime": "2026-10-09T00:39:31.049000+00:00",
  "SuspendedState": {"DynamicScalingInSuspended": false, "DynamicScalingOutSuspended": false,
  "ScheduledScalingSuspended": false}} and policy [{"PolicyName":
  "DynamoDBReadCapacityUtilization:table/ackq-41e87e-as-rt", "PolicyARN":
  "arn:aws:autoscaling:us-west-2:<ACCOUNT>:scalingPolicy:c6338041-e00e-40ba-b0d9-0c644f2c751e:resource/dynamodb/table/ackq-41e87e-as-rt:policyName/DynamoDBReadCapacityUtilization:table/ackq-41e87e-as-rt",
  "CreationTime": "2026-10-09T00:39:31.085000+00:00", "cfg": {"TargetValue": 70.0,
  "PredefinedMetricSpecification": {"PredefinedMetricType": "DynamoDBReadCapacityUtilization"}}, "alarms":
  ["TargetTracking-tab.
  - ACK: compare.is_ignored+delta_pre_compare, late_initialize · ops: UpdateTableReplicaAutoScaling,
    DescribeTableReplicaAutoScaling · fields: PolicyName, AutoScalingRoleArn, ScaleInCooldown,
    ScaleOutCooldown, DisableScaleIn, AutoScalingDisabled
  - repro: UpdateTableReplicaAutoScaling read
    {Min,Max,ScalingPolicyUpdate.TargetTrackingScalingPolicyConfiguration.TargetValue} -> Describe
  - handling: not handled in the controller (as of commit 34b85e6)
  - related: [DDB-TABLE-189](../table-policy-kinesis-autoscaling.md#ddb-table-189), [DDB-TABLE-195](../table-policy-kinesis-autoscaling.md#ddb-table-195), [DDB-TABLE-243](../table-policy-kinesis-autoscaling.md#ddb-table-243), [DDB-TABLE-232](../table-replicas.md#ddb-table-232), [DDB-TABLE-317](../table-policy-kinesis-autoscaling.md#ddb-table-317), [DDB-TABLE-289](../table-replicas.md#ddb-table-289),
    [DDB-TABLE-240](../table-policy-kinesis-autoscaling.md#ddb-table-240), [DDB-TABLE-239](../table-replicas.md#ddb-table-239), [DDB-TABLE-191](../table-policy-kinesis-autoscaling.md#ddb-table-191), [DDB-TABLE-196](../table-policy-kinesis-autoscaling.md#ddb-table-196) · hypotheses: H-S-041 · evidence:
    table/round-trip/autoscaling-settings

## Notes

H-S-041 largely confirmed: PolicyName is server-generated, AutoScalingRoleArn is forced to the SLR,
ScaleIn/ScaleOutCooldown and DisableScaleIn are ABSENT (not 0/false) in Describe and in AAS, and
AutoScalingDisabled is absent (not false) when enabled. Only {AutoScalingDisabled:true, ScalingPolicies:[]}
appears when disabled.
