<!-- generated: ack-api-quirks dynamodb 2026-10-09 model=2012-08-10 controller=34b85e6 -->
# DDB-TABLE-195: AAS target without a policy reads as {AutoScalingDisabled:true, ScalingPolicies:[]} in Describe yet AAS enforces its MinCapacity
_Full entry and notes of one finding; its summary entry is in
[table-policy-kinesis-autoscaling.md](../table-policy-kinesis-autoscaling.md). Generated from ack-api-quirks
`services/dynamodb` (model 2012-08-10, controller commit 34b85e6); edit the finding in the lab, not here._

## Finding

- <a id="ddb-table-195"></a>**DDB-TABLE-195** `sub-resource-api` · impact high · unhandled (not handled in controller) · verified 2026-10-09, re-verified
  **AAS target without a policy reads as {AutoScalingDisabled:true, ScalingPolicies:[]} in Describe yet AAS enforces its MinCapacity**
  RegisterScalableTarget(read, Min 2 Max 12, no policy, no RoleARN) -> Describe 0.45s later: read settings
  {"AutoScalingDisabled": true, "ScalingPolicies": []} (2 s later {"AutoScalingDisabled": true,
  "ScalingPolicies": []}; region B {"AutoScalingDisabled": true, "ScalingPolicies": []}).
  DeregisterScalableTarget -> Describe: {"AutoScalingDisabled": true, "ScalingPolicies": []}. AAS target
  roles: [{"dim": "ReadCapacityUnits", "min": 2, "max": 12, "role":
  "AWSServiceRoleForApplicationAutoScaling_DynamoDBTable"}, {"dim": "WriteCapacityUnits", "min": 1, "max": 10,
  "role": "AWSServiceRoleForApplicationAutoScaling_DynamoDBTable"}].
  - ACK: scope:skip, docs-only · ops: DescribeTableReplicaAutoScaling, RegisterScalableTarget · fields:
    ScalingPolicies, MinimumUnits, MaximumUnits, AutoScalingDisabled
  - repro: application-autoscaling register-scalable-target (no policy) -> DescribeTableReplicaAutoScaling ->
    deregister -> Describe
  - handling: not handled in the controller (as of commit 34b85e6)
  - related: [DDB-TABLE-189](../table-policy-kinesis-autoscaling.md#ddb-table-189), [DDB-TABLE-243](../table-policy-kinesis-autoscaling.md#ddb-table-243), [DDB-TABLE-232](../table-replicas.md#ddb-table-232), [DDB-TABLE-317](../table-policy-kinesis-autoscaling.md#ddb-table-317), [DDB-TABLE-289](../table-replicas.md#ddb-table-289), [DDB-TABLE-237](../table-policy-kinesis-autoscaling.md#ddb-table-237),
    [DDB-TABLE-314](../table-policy-kinesis-autoscaling.md#ddb-table-314), [DDB-TABLE-315](../table-policy-kinesis-autoscaling.md#ddb-table-315), [DDB-TABLE-256](../table-replicas.md#ddb-table-256), [DDB-TABLE-321](../table-replicas.md#ddb-table-321), [DDB-TABLEREPLICAAUTOSCALING-001](../table-replicas.md#ddb-tablereplicaautoscaling-001),
    [DDB-TABLE-193](../table-policy-kinesis-autoscaling.md#ddb-table-193), [DDB-TABLE-197](../table-policy-kinesis-autoscaling.md#ddb-table-197), [DDB-TABLE-196](../table-policy-kinesis-autoscaling.md#ddb-table-196), [DDB-TABLE-292](../table-replicas.md#ddb-table-292) · hypotheses: H-R-104, H-R-110, H-R-131 ·
    evidence: table/sub-resources/replica-autoscaling-facade, table/creative/reverify-set-b

## Notes

REFUTES the H-R-104 / H-R-110 expectation of an 'enabled but inert' (AutoScalingDisabled=false,
ScalingPolicies=[]) state: a target without a policy is indistinguishable from 'no autoscaling' in
DescribeTableReplicaAutoScaling (its Min/Max are hidden), yet AAS still enforced MinCapacity: scaling activity
'Setting read capacity units to 2.' (cause 'minimum capacity was set to 2') raised
ProvisionedThroughput.ReadCapacityUnits 1 -> 2 within ~1 s of RegisterScalableTarget (DescribeTable
LastIncreaseDateTime 2026-10-09T00:31:06.525000+00:00).
