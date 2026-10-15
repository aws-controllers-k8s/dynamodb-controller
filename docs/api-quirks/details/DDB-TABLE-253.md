<!-- generated: ack-api-quirks dynamodb 2026-10-09 model=2012-08-10 controller=34b85e6 -->
# DDB-TABLE-253: UpdateTableReplicaAutoScaling validation: AAS errors pass through verbatim (TargetValue 10-90, not 20-90); negative cooldown accepted
_Full entry and notes of one finding; its summary entry is in [table-replicas.md](../table-replicas.md).
Generated from ack-api-quirks `services/dynamodb` (model 2012-08-10, controller commit 34b85e6); edit the
finding in the lab, not here._

## Finding

- <a id="ddb-table-253"></a>**DDB-TABLE-253** `request-validation` · impact high · unhandled (not handled in controller) · verified 2026-10-09
  **UpdateTableReplicaAutoScaling validation: AAS errors pass through verbatim (TargetValue 10-90, not 20-90); negative cooldown accepted**
  One call per case (code, http, message): {"update_missing": ["ResourceNotFoundException", 400, "Global table
  with name: 'ackq-a9cc82-nope' does not exist."], "min_gt_max": ["ValidationException", 400, "Maximum
  capacity cannot be less than minimum capacity (Service: AWSApplicationAutoScaling; Status Code: 400; Error
  Code: ValidationException; Request ID: 933f88a0-18d1-4878-85a4-6ca0f7db48ae; Proxy: n"], "min_eq_max":
  ["200", 200, ""], "min_zero": ["ParamValidationError", null, "Parameter validation failed:\nInvalid value
  for parameter ReplicaUpdates[0].ReplicaProvisionedReadCapacityAutoScalingUpdate.MinimumUnits, value: 0,
  valid min value: 1"], "min_negative": ["ParamValidationError", null, "Parameter validation failed:\nInvalid
  value for parameter ReplicaUpdates[0].ReplicaProvisionedReadCapacityAutoScalingUpdate.MinimumUnits, value:
  -1, valid min value: 1"], "target_19_9": ["200", 200, ""], "target_90_1": ["ValidationException", 400, "For
  predefined metric type DynamoDBReadCapacityUtilization, target value must be between '10.0' and '90.0', but
  was '90.1'. (Service: AWSApplicationAutoScaling; Status Code: 400; Error Code: Validatio"], "target_0":
  ["ValidationException", 400, "For target tracking scaling, target value must be between '8.51592E-109' and
  '1.174271E108', but was '0.0'. (Service: AWSApplicationAutoScaling; Status Code: 400; Error Code:
  ValidationException; Requ"], "target_100": ["ValidationException", 400, "For predefined metric type
  DynamoDBReadCapacityUtilization, target value must be between '10.0' and '90.0', but was '100.0'. (Service:
  AWSApplicationAutoScaling; Status Code: 400; Error Code: Validati"], "target_missing":
  ["ParamValidationError", null, "Parameter validation failed:\nMissing required parameter in
  ReplicaUpdates[0].ReplicaProvisionedReadCapacityAutoScalingUpdate.ScalingPolicyUpdate.TargetTrackingScalingPolicyConfiguration:
  \"TargetValue\""], "policy_without_config": ["ParamValidationError", null, "Parameter validation
  failed:\nMissing required parameter in
  ReplicaUpdates[0].ReplicaProvisionedReadCapacityAutoScalingUpdate.ScalingPolicyUpdate:
  \"TargetTrackingScalingPolicyConfiguration\""], "negative_scale_in_cooldown": ["200", 200, ""],
  "huge_cooldown": ["200", 200, ""], "min_max_without_policy_fresh": ["ValidationException", 400, "Failed to
  update settings for global table with name: ‘ackq-a9cc82-as-err’: Parameters 'ScalingPolicyUpdate' are
  required unless auto scaling is being disabled."], "policy_without_min_max_fresh": ["ValidationException",
  400, "Failed to update settings for global table with name: ‘ackq-a9cc82-as-err’: Parameters 'MaximumUnits',
  'MinimumUnits' are required unless auto scaling is being disabled."], "min_only_fresh":
  ["ValidationException", 400, "Failed to update settings for global table with name: ‘ackq-a9cc82-as-err’:
  Parameters 'MaximumUnits' are required unless auto scaling is being disabled."], "empty_struct":
  ["ValidationException", 400, "Failed to update settings for global table with name: ‘ackq-a9cc82-as-err’:
  Parameters 'MaximumUnits', 'ScalingPolicyUpdate', 'MinimumUnits' are required unless auto scaling is being
  disabled."], "disabled_true_never_enabled": ["200", 200, ""], "disabled_false_never_enabled":
  ["ValidationException", 400, "Failed to update settings for global table with name: ‘ackq-a9cc82-as-err’:
  Parameters 'MaximumUnits', 'ScalingPolicyUpdate', 'MinimumUnits' are required unless auto scaling is being
  disabled."], "disabled_false_full_fresh": ["200", 200, ""], "empty_replica_updates":
  ["ParamValidationError", null, "Parameter validation failed:\nInvalid length for parameter ReplicaUpdates,
  value: 0, valid min length: 1"], "replica_updates_region_only": ["ValidationException", 400, "Failed to
  update settings for global table with name: ‘ackq-a9cc82-as-err’ because at least one update parameter must
  be specified for region: ‘us-west-2’."], "foreign_region_eu_west_1": ["ResourceNotFou [truncated in
  evidence]
  - ACK: terminal_codes, custom_update · ops: UpdateTableReplicaAutoScaling · fields: MinimumUnits,
    MaximumUnits, TargetValue, ScaleInCooldown, ScaleOutCooldown, ReplicaUpdates.RegionName,
    GlobalSecondaryIndexUpdates.IndexName, TableName
  - repro: 2019.11.21 PROVISIONED global table; send each invalid/partial UpdateTableReplicaAutoScaling shape
    against the never-configured read dimension
  - handling: not handled in the controller (as of commit 34b85e6)
  - related: [DDB-TABLE-187](../table-replicas.md#ddb-table-187), [DDB-TABLE-232](../table-replicas.md#ddb-table-232), [DDB-TABLE-254](../table-replicas.md#ddb-table-254), [DDB-TABLE-231](../table-global-tables.md#ddb-table-231), [DDB-TABLE-446](../service.md#ddb-table-446), [DDB-TABLE-241](../table-policy-kinesis-autoscaling.md#ddb-table-241),
    [DDB-TABLE-242](../table-policy-kinesis-autoscaling.md#ddb-table-242), [DDB-TABLE-243](../table-policy-kinesis-autoscaling.md#ddb-table-243), [DDB-TABLE-255](../table-replicas.md#ddb-table-255) · hypotheses: H-R-111, H-R-108 · evidence:
    table/error-taxonomy/autoscaling-validation

## Notes

H-R-111 partially confirmed/partially refuted: min>max -> ValidationException whose text is the AAS error
('Maximum capacity cannot be less than minimum capacity (Service: AWSApplicationAutoScaling; ...)');
TargetValue 19.9 ACCEPTED, 90.1/100 rejected with 'target value must be between 10.0 and 90.0' (docs say
20-90); TargetValue 0 -> AAS range error; MinimumUnits 0/-1 are rejected client-side by botocore (min 1);
ScaleInCooldown -1 and ScaleOutCooldown 1e9 are ACCEPTED and echoed verbatim; Min==Max accepted (AAS
immediately set RCU to 5); Max 1e9 accepted. Min/Max without ScalingPolicyUpdate and ScalingPolicyUpdate
without Min/Max are ValidationException on a fresh dimension (same as on an existing one).
AutoScalingDisabled=true on a never-enabled dimension -> 200 (idempotent); AutoScalingDisabled=false alone ->
ValidationException. Unknown RegionName (foreign, bogus, upper-case) and unknown IndexName ->
ResourceNotFoundException (not ValidationException), confirming H-R-108's RNF-for-index clause; duplicate
RegionName / no update parameter -> ValidationException. No LimitExceededException was ever returned.
