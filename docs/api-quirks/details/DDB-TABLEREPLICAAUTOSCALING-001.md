<!-- generated: ack-api-quirks dynamodb 2026-10-09 model=2012-08-10 controller=34b85e6 -->
# DDB-TABLEREPLICAAUTOSCALING-001: Scope verdict: TableReplicaAutoScaling is a facade over Application Auto Scaling - skip it as an ACK resource
_Full entry and notes of one finding; its summary entry is in [table-replicas.md](../table-replicas.md).
Generated from ack-api-quirks `services/dynamodb` (model 2012-08-10, controller commit 34b85e6); edit the
finding in the lab, not here._

## Finding

- <a id="ddb-tablereplicaautoscaling-001"></a>**DDB-TABLEREPLICAAUTOSCALING-001** `scope` · impact high · unhandled (not handled in controller) · verified 2026-10-09
  **Scope verdict: TableReplicaAutoScaling is a facade over Application Auto Scaling - skip it as an ACK resource**
  **Scope verdict: skip:no-crud**
  Identity test over three probes: (1) settings created via UpdateTableReplicaAutoScaling appear in
  application-autoscaling as a scalable target + target-tracking policy + 4 CloudWatch alarms per dimension;
  (2) objects created directly via RegisterScalableTarget/PutScalingPolicy (custom PolicyName, cooldowns,
  DisableScaleIn) appear verbatim in DescribeTableReplicaAutoScaling within 0.5 s; (3) DeleteTable/replica
  removal leave the AAS objects orphaned and intact. The DynamoDB API adds no state of its own: no
  Create/Delete (Update+Describe only), every non-disable Update must carry the full {MinimumUnits,
  MaximumUnits, ScalingPolicyUpdate}, AutoScalingRoleArn is forced to the service-linked role, PolicyName
  rename replaces, and the API is only callable on 2019.11.21 global tables (regional tables ->
  ResourceNotFoundException). It also hides state (a target without a policy reads as AutoScalingDisabled=true
  while AAS enforces its MinCapacity) and surfaces no scaling failures (quota breach only in
  describe-scaling-activities).
  - ACK: scope:skip, ignore.resource · ops: UpdateTableReplicaAutoScaling, DescribeTableReplicaAutoScaling,
    RegisterScalableTarget, PutScalingPolicy
  - repro: see the six evidence probes
  - handling: not handled in the controller (as of commit 34b85e6)
  - related: [DDB-TABLE-193](../table-policy-kinesis-autoscaling.md#ddb-table-193), [DDB-TABLE-197](../table-policy-kinesis-autoscaling.md#ddb-table-197), [DDB-TABLE-196](../table-policy-kinesis-autoscaling.md#ddb-table-196), [DDB-TABLE-189](../table-policy-kinesis-autoscaling.md#ddb-table-189), [DDB-TABLE-195](../table-policy-kinesis-autoscaling.md#ddb-table-195), [DDB-TABLE-292](../table-replicas.md#ddb-table-292) ·
    hypotheses: H-R-131, H-R-132, H-R-133, H-R-104, H-R-050 · evidence:
    table/sub-resources/replica-autoscaling-facade, table/round-trip/autoscaling-settings,
    table/error-taxonomy/autoscaling-validation, table/mutation-matrix/autoscaling-vs-throughput,
    table/dependencies/autoscaling-gsi-and-orphans, table/cross-region/replica-autoscaling-facades

## Notes

Confirms H-R-131 verdict (a). The ACK applicationautoscaling-controller (ScalableTarget
serviceNamespace=dynamodb resourceID=table/<name>[/index/<gsi>], ScalingPolicy) already owns 100% of this
state. What the DynamoDB Table controller DOES need (H-R-132): a read-side guard - treat
spec.provisionedThroughput (table and GSI) as unmanaged when an AAS target exists for the dimension, detected
via application-autoscaling:DescribeScalableTargets (works for regional tables;
DescribeTableReplicaAutoScaling does not) - plus two prerequisite rules: adding a replica or a GSI to a
PROVISIONED table requires write autoscaling on the table and every GSI first (ValidationException otherwise).
Legacy UpdateGlobalTableSettings autoscaling fields could not be tested (CreateGlobalTable 2017.11.29
rejected) and should be excluded from any GlobalTable spec (H-R-133).
