<!-- generated: ack-api-quirks dynamodb 2026-10-09 model=2012-08-10 controller=34b85e6 -->
# DDB-TABLE-198: Write AutoScalingDisabled=true on a PROVISIONED global table -> 200: AAS write target+alarms removed in BOTH regions, read untouched
_Full entry and notes of one finding; its summary entry is in
[table-policy-kinesis-autoscaling.md](../table-policy-kinesis-autoscaling.md). Generated from ack-api-quirks
`services/dynamodb` (model 2012-08-10, controller commit 34b85e6); edit the finding in the lab, not here._

## Finding

- <a id="ddb-table-198"></a>**DDB-TABLE-198** `delete-semantics` · impact high · unhandled (not handled in controller) · verified 2026-10-09
  **Write AutoScalingDisabled=true on a PROVISIONED global table -> 200: AAS write target+alarms removed in BOTH regions, read untouched**
  UpdateTableReplicaAutoScaling(write {AutoScalingDisabled:true}) on the 2-replica PROVISIONED table -> 200
  (message 'None'); response write a/b {"MinimumUnits": 1, "MaximumUnits": 10, "AutoScalingRoleArn":
  "arn:aws:iam::<ACCOUNT>:role/<PRINCIPAL>", "ScalingPolicies": [{"PolicyName":
  "ackq-0ad4a5-custom-write-policy", "TargetTrackingScalingPolicyConfiguration": {"DisableScaleIn": true,
  "ScaleInCooldown": 120, "ScaleOutCooldown": 60, "TargetValue": 55.0}}]} / {"MinimumUnits": 1,
  "MaximumUnits": 10, "AutoScalingRoleArn": "arn:aws:iam::<ACCOUNT>:role/<PRINCIPAL>", "ScalingPolicies":
  [{"PolicyName": "ackq-0ad4a5-custom-write-policy", "TargetTrackingScalingPolicyConfiguration":
  {"DisableScaleIn": true, "ScaleInCooldown": 120, "ScaleOutCooldown": 60, "TargetValue": 55.0}}]}; Describe 1
  s later a/b {"AutoScalingDisabled": true, "ScalingPolicies": []} / {"AutoScalingDisabled": true,
  "ScalingPolicies": []}; AAS targets A [{"dim": "ReadCapacityUnits", "min": 1, "max": 10, "role":
  "AWSServiceRoleForApplicationAutoScaling_DynamoDBTable"}], B []; alarms A
  ['TargetTracking-table/ackq-0ad4a5-as-prov-AlarmHigh-bfd36409-08b7-4c20-947c-a6992f50af07',
  'TargetTracking-table/ackq-0ad4a5-as-prov-AlarmLow-e5d3d0f5-fe10-4dfc-9bd0-4d4bb0687b6c',
  'TargetTracking-table/ackq-0ad4a5-as-prov-ProvisionedCapacityHigh-dd056ac9-82ff-45c9-9664-a03efb91ee9d',
  'TargetTracking-table/ackq-0ad4a5-as-prov-ProvisionedCapacityLow-6cec6cdf-7f51-499b-b883-d2057302ab5e'], B
  []. Read disable via ReplicaUpdates -> 200; targets A then []. Region-B targets after the replica was
  removed: [].
  - ACK: custom_update, docs-only · ops: UpdateTableReplicaAutoScaling · fields: AutoScalingDisabled
  - repro: autoscaled 2-replica PROVISIONED table -> UpdateTableReplicaAutoScaling write
    AutoScalingDisabled=true -> describe-scalable-targets both regions
  - handling: not handled in the controller (as of commit 34b85e6)
  - related: [DDB-TABLE-238](../table-replicas.md#ddb-table-238), [DDB-TABLE-192](../table-replicas.md#ddb-table-192), [DDB-TABLE-242](../table-policy-kinesis-autoscaling.md#ddb-table-242), [DDB-TABLE-314](../table-policy-kinesis-autoscaling.md#ddb-table-314), [DDB-TABLE-290](../table-policy-kinesis-autoscaling.md#ddb-table-290), [DDB-TABLE-292](../table-replicas.md#ddb-table-292),
    [DDB-TABLE-323](../table-policy-kinesis-autoscaling.md#ddb-table-323), [DDB-TABLE-243](../table-policy-kinesis-autoscaling.md#ddb-table-243) · hypotheses: H-R-110, H-S-042 · evidence:
    table/sub-resources/replica-autoscaling-facade

## Notes

H-R-110 confirmed for the AAS/CloudWatch side: disable == DeregisterScalableTarget for that dimension in every
replica region (alarms gone). The response still echoed the old write settings (stale); Describe 1 s later
showed {AutoScalingDisabled:true, ScalingPolicies:[]}. Note the table stays a PROVISIONED global table without
write autoscaling, a state UpdateTable(ReplicaUpdates Create) would have refused to create.
