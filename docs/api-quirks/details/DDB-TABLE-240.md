<!-- generated: ack-api-quirks dynamodb 2026-10-09 model=2012-08-10 controller=34b85e6 -->
# DDB-TABLE-240: PolicyName is the policy identity: resend = no-op (same ARN/CreationTime); a NEW name REPLACES the old policy (one policy per dimension)
_Full entry and notes of one finding; its summary entry is in
[table-policy-kinesis-autoscaling.md](../table-policy-kinesis-autoscaling.md). Generated from ack-api-quirks
`services/dynamodb` (model 2012-08-10, controller commit 34b85e6); edit the finding in the lab, not here._

## Finding

- <a id="ddb-table-240"></a>**DDB-TABLE-240** `identity` · impact high · unhandled (not handled in controller) · verified 2026-10-09, re-verified
  **PolicyName is the policy identity: resend = no-op (same ARN/CreationTime); a NEW name REPLACES the old policy (one policy per dimension)**
  Custom PolicyName p1 + cooldowns round-trip diff: {"missing_in_observed": [], "value_changed": {},
  "extra_in_observed": []}; policy names after C: ['ackq-41e87e-p1'] (server-generated policy replaced).
  Resend identical -> 200, same PolicyARN+CreationTime per policy: {"ackq-41e87e-p1": true}. Same name new
  TargetValue -> 200, after: {"policies": [["ackq-41e87e-p1", {"TargetValue": 50.0,
  "PredefinedMetricSpecification": {"PredefinedMetricType": "DynamoDBReadCapacityUtilization"}}]], "same_arn":
  {"ackq-41e87e-p1": true}}. Different name p2 -> 200; Describe ScalingPolicies: [{"PolicyName":
  "ackq-41e87e-p2", "TargetTrackingScalingPolicyConfiguration": {"TargetValue": 40.0}}]; AAS:
  [["ackq-41e87e-p2", 40.0]]; alarms: ["AlarmHigh-a5d5e80a-f220-4634-ab9c-6ac7c6fd1fe1",
  "AlarmHigh-a84f162d-6a3a-4ac1-8590-bf6a41c5447c", "AlarmLow-0f864fcd-f316-49a7-99e7-9e0fc21ebf92",
  "AlarmLow-626b23aa-7767-458d-8602-1f0caf1dd909",
  "ProvisionedCapacityHigh-529e6f7c-f11f-4e5a-92e8-3ffd772de89e",
  "ProvisionedCapacityHigh-576ad820-8772-4711-bfd2-e32f27f244da",
  "ProvisionedCapacityLow-2fd9a0a0-fa17-4cc8-b741-85a30bdcd0a7",
  "ProvisionedCapacityLow-67bcf72a-08df-49e8-abb8-a2f5feabd605"].
  - ACK: custom_update, docs-only · ops: UpdateTableReplicaAutoScaling · fields:
    ScalingPolicyUpdate.PolicyName, ScalingPolicies
  - repro: read update PolicyName p1 -> resend -> same name new target -> PolicyName p2 -> Describe +
    describe-scaling-policies
  - handling: not handled in the controller (as of commit 34b85e6)
  - related: [DDB-TABLE-237](../table-policy-kinesis-autoscaling.md#ddb-table-237), [DDB-TABLE-239](../table-replicas.md#ddb-table-239), [DDB-TABLE-191](../table-policy-kinesis-autoscaling.md#ddb-table-191), [DDB-TABLE-189](../table-policy-kinesis-autoscaling.md#ddb-table-189), [DDB-TABLE-196](../table-policy-kinesis-autoscaling.md#ddb-table-196) · hypotheses: H-R-109,
    H-S-041 · evidence: table/round-trip/autoscaling-settings, table/creative/reverify-set-a2

## Notes

REFUTES the H-R-109 claim that a different PolicyName accumulates a second policy: DynamoDB deletes the
previous policy (AAS lists only the new one, old alarms removed). Custom PolicyName also replaced the
server-generated 'DynamoDBReadCapacityUtilization:table/<name>' policy. Same-name updates keep the PolicyARN.
