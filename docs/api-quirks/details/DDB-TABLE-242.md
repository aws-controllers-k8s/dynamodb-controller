<!-- generated: ack-api-quirks dynamodb 2026-10-09 model=2012-08-10 controller=34b85e6 -->
# DDB-TABLE-242: AutoScalingDisabled=true deregisters the AAS target (policies+alarms gone), sibling dimension untouched; true+Min/Max -> ValidationExcept...
_Full entry and notes of one finding; its summary entry is in
[table-policy-kinesis-autoscaling.md](../table-policy-kinesis-autoscaling.md). Generated from ack-api-quirks
`services/dynamodb` (model 2012-08-10, controller commit 34b85e6); edit the finding in the lab, not here._

## Finding

- <a id="ddb-table-242"></a>**DDB-TABLE-242** `delete-semantics` · impact high · unhandled (not handled in controller) · verified 2026-10-09
  **AutoScalingDisabled=true deregisters the AAS target (policies+alarms gone), sibling dimension untouched; true+Min/Max -> ValidationExcept...**
  Before: read target {"MinCapacity": 2, "MaxCapacity": 20, "RoleARN":
  "arn:aws:iam::<ACCOUNT>:role/<PRINCIPAL>", "CreationTime": "2026-10-09T00:39:31.049000+00:00",
  "SuspendedState": {"DynamicScalingInSuspended": false, "DynamicScalingOutSuspended": false,
  "ScheduledScalingSuspended": false}}, alarms ['AlarmHigh-a5d5e80a-f220-4634-ab9c-6ac7c6fd1fe1',
  'AlarmHigh-a84f162d-6a3a-4ac1-8590-bf6a41c5447c', 'AlarmLow-0f864fcd-f316-49a7-99e7-9e0fc21ebf92',
  'AlarmLow-626b23aa-7767-458d-8602-1f0caf1dd909',
  'ProvisionedCapacityHigh-529e6f7c-f11f-4e5a-92e8-3ffd772de89e',
  'ProvisionedCapacityHigh-576ad820-8772-4711-bfd2-e32f27f244da',
  'ProvisionedCapacityLow-2fd9a0a0-fa17-4cc8-b741-85a30bdcd0a7',
  'ProvisionedCapacityLow-67bcf72a-08df-49e8-abb8-a2f5feabd605']. After read {AutoScalingDisabled:true} (200,
  response settings {"MinimumUnits": 2, "MaximumUnits": 20, "AutoScalingRoleArn":
  "arn:aws:iam::<ACCOUNT>:role/<PRINCIPAL>", "ScalingPolicies": [{"PolicyName": "ackq-41e87e-p2",
  "TargetTrackingScalingPolicyConfiguration": {"TargetValue": 40.0}}]}, Describe {"AutoScalingDisabled": true,
  "ScalingPolicies": []}): read target null, read policies [], write target {"MinCapacity": 1, "MaxCapacity":
  10, "RoleARN": "arn:aws:iam::<ACCOUNT>:role/<PRINCIPAL>", "CreationTime":
  "2026-10-09T00:39:06.027000+00:00", "SuspendedState": {"DynamicScalingInSuspended": false,
  "DynamicScalingOutSuspended": false, "ScheduledScalingSuspended": false}}, write policies
  ['ackq-41e87e-w0'], alarms ['AlarmHigh-a84f162d-6a3a-4ac1-8590-bf6a41c5447c',
  'AlarmLow-0f864fcd-f316-49a7-99e7-9e0fc21ebf92',
  'ProvisionedCapacityHigh-529e6f7c-f11f-4e5a-92e8-3ffd772de89e',
  'ProvisionedCapacityLow-2fd9a0a0-fa17-4cc8-b741-85a30bdcd0a7']. Disable again -> 200. {true, Min, Max} ->
  {"operation": "update_table_replica_auto_scaling", "ok": false, "code": "ValidationException",
  "http_status": 400, "message": "Failed to update settings for global table with name ‘ackq-41e87e-as-rt’:
  Parameters 'MaximumUnits', 'MinimumUnits' must be left blank when disabling auto scaling.", "latency_ms":
  23, "client_side": false}; {true, ScalingPolicyUpdate} -> {"operation": "update_table_replica_auto_scaling",
  "ok": false, "code": "ValidationException", "http_status": 400, "message": "Failed to update settings for
  global table with name ‘ackq-41e87e-as-rt’: Parameters 'ScalingPolicyUpdate' must be left blank when
  disabling auto scaling.", "latency_ms": 30, "client_side": false} (write target after: {"MinCapacity": 1,
  "MaxCapacity": 10, "RoleARN": "arn:aws:iam::<ACCOUNT>:role/<PRINCIPAL>", "CreationTime":
  "2026-10-09T00:39:06.027000+00:00", "SuspendedState": {"DynamicScalingInSuspended": false,
  "DynamicScalingOutSuspended": false, "ScheduledScalingSuspended": false}}). On the disabled read dimension:
  {false} -> {"operation": "update_table_replica_auto_scaling", "ok": false, "code": "ValidationException",
  "http_status": 400, "message": "Failed to update settings for global table with name: ‘ackq-41e87e-as-rt’:
  Parameters 'MaximumUnits', 'ScalingPolicyUpdate', 'MinimumUnits' are required unless auto scaling is being
  disabled.", "latency_ms": 29, "client_side": false}; {false, Min, Max} -> {"operation":
  "update_table_replica_auto_scaling", "ok": false, "code": "ValidationException", "http_status": 400,
  "message": "Failed to update settings for global table with name: ‘ackq-41e87e-as-rt’: Parameters
  'ScalingPolicyUpdate' are required unless auto scaling is being disabled.", "latency_ms": 23, "client_side":
  false}; {false, Min, Max, policy} -> 200; policy names afte [truncated in evidence]
  - ACK: custom_update, pre-delete-cleanup · ops: UpdateTableReplicaAutoScaling · fields: AutoScalingDisabled,
    MinimumUnits, MaximumUnits, ScalingPolicyUpdate
  - repro: autoscaled read+write -> read {AutoScalingDisabled:true} -> describe-scalable-targets/-policies,
    describe-alarms -> combined/false shapes
  - handling: not handled in the controller (as of commit 34b85e6)
  - related: [DDB-TABLE-238](../table-replicas.md#ddb-table-238), [DDB-TABLE-192](../table-replicas.md#ddb-table-192), [DDB-TABLE-198](../table-policy-kinesis-autoscaling.md#ddb-table-198), [DDB-TABLE-314](../table-policy-kinesis-autoscaling.md#ddb-table-314), [DDB-TABLE-290](../table-policy-kinesis-autoscaling.md#ddb-table-290), [DDB-TABLE-292](../table-replicas.md#ddb-table-292),
    [DDB-TABLE-323](../table-policy-kinesis-autoscaling.md#ddb-table-323), [DDB-TABLE-243](../table-policy-kinesis-autoscaling.md#ddb-table-243), [DDB-TABLE-241](../table-policy-kinesis-autoscaling.md#ddb-table-241), [DDB-TABLE-253](../table-replicas.md#ddb-table-253), [DDB-TABLE-255](../table-replicas.md#ddb-table-255) · hypotheses: H-R-110, H-S-042,
    H-R-120 · evidence: table/round-trip/autoscaling-settings

## Notes

H-R-110 confirmed: disable == DeregisterScalableTarget (policies and the 4 CloudWatch alarms of that dimension
disappear; the sibling dimension keeps its target). {AutoScalingDisabled:true}+Min/Max -> ValidationException
'Parameters 'MaximumUnits', 'MinimumUnits' must be left blank when disabling auto scaling.';
+ScalingPolicyUpdate -> 'Parameters 'ScalingPolicyUpdate' must be left blank when disabling auto scaling.'.
Disabling twice is 200 (idempotent). Re-enabling requires the full {Min,Max,ScalingPolicyUpdate} (H-S-042
confirmed); the re-enabled policy gets the server-generated name again.
