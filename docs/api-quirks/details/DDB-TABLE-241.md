<!-- generated: ack-api-quirks dynamodb 2026-10-09 model=2012-08-10 controller=34b85e6 -->
# DDB-TABLE-241: Partial autoscaling update shapes on an existing dimension: Min/Max-only -> ValidationException, policy-only -> ValidationException, empt...
_Full entry and notes of one finding; its summary entry is in
[table-policy-kinesis-autoscaling.md](../table-policy-kinesis-autoscaling.md). Generated from ack-api-quirks
`services/dynamodb` (model 2012-08-10, controller commit 34b85e6); edit the finding in the lab, not here._

## Finding

- <a id="ddb-table-241"></a>**DDB-TABLE-241** `request-validation` · impact high · unhandled (not handled in controller) · verified 2026-10-09
  **Partial autoscaling update shapes on an existing dimension: Min/Max-only -> ValidationException, policy-only -> ValidationException, empt...**
  Results (code, message): {"E_min_max_only": ["ValidationException", "Failed to update settings for global
  table with name: ‘ackq-41e87e-as-rt’: Parameters 'ScalingPolicyUpdate' are required unless auto scaling is
  being disabled."], "E_policy_only_no_min_max": ["ValidationException", "Failed to update settings for global
  table with name: ‘ackq-41e87e-as-rt’: Parameters 'MaximumUnits', 'MinimumUnits' are required unless auto
  scaling is being d"], "E_policy_only_named_p1": ["ValidationException", "Failed to update settings for
  global table with name: ‘ackq-41e87e-as-rt’: Parameters 'MaximumUnits', 'MinimumUnits' are required unless
  auto scaling is being d"], "E_min_only_with_policy": ["ValidationException", "Failed to update settings for
  global table with name: ‘ackq-41e87e-as-rt’: Parameters 'MaximumUnits' are required unless auto scaling is
  being disabled."], "E_max_only_with_policy": ["ValidationException", "Failed to update settings for global
  table with name: ‘ackq-41e87e-as-rt’: Parameters 'MinimumUnits' are required unless auto scaling is being
  disabled."], "E_empty_struct": ["ValidationException", "Failed to update settings for global table with
  name: ‘ackq-41e87e-as-rt’: Parameters 'MaximumUnits', 'ScalingPolicyUpdate', 'MinimumUnits' are required
  unless "], "E_role_only": ["ValidationException", "Failed to update settings for global table with name:
  ‘ackq-41e87e-as-rt’: Parameters 'MaximumUnits', 'ScalingPolicyUpdate', 'MinimumUnits' are required unless
  "], "E_disabled_false_only": ["ValidationException", "Failed to update settings for global table with name:
  ‘ackq-41e87e-as-rt’: Parameters 'MaximumUnits', 'ScalingPolicyUpdate', 'MinimumUnits' are required unless
  "]}. Policies on the read dimension afterwards: [["ackq-41e87e-p2", 40.0]].
  - ACK: custom_update, terminal_codes · ops: UpdateTableReplicaAutoScaling · fields: MinimumUnits,
    MaximumUnits, ScalingPolicyUpdate, AutoScalingDisabled, AutoScalingRoleArn
  - repro: on a dimension that already has a target+policy send each partial AutoScalingSettingsUpdate shape
  - handling: not handled in the controller (as of commit 34b85e6)
  - related: [DDB-TABLE-253](../table-replicas.md#ddb-table-253), [DDB-TABLE-242](../table-policy-kinesis-autoscaling.md#ddb-table-242), [DDB-TABLE-243](../table-policy-kinesis-autoscaling.md#ddb-table-243), [DDB-TABLE-255](../table-replicas.md#ddb-table-255) · hypotheses: H-R-111, H-S-042 ·
    evidence: table/round-trip/autoscaling-settings

## Notes

REFUTES H-R-111's last clause: even on a dimension that already has a target+policy, Min/Max without
ScalingPolicyUpdate -> ValidationException 'Parameters 'ScalingPolicyUpdate' are required unless auto scaling
is being disabled'; every non-disable update must carry MinimumUnits, MaximumUnits AND ScalingPolicyUpdate
(full replacement). AutoScalingDisabled=false alone and AutoScalingRoleArn alone are rejected the same way.
