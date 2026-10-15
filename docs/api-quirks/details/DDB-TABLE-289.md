<!-- generated: ack-api-quirks dynamodb 2026-10-09 model=2012-08-10 controller=34b85e6 -->
# DDB-TABLE-289: GSI entries in DescribeTableReplicaAutoScaling: keys ['IndexName', 'IndexStatus', 'ProvisionedReadCapacityAutoScalingSettings', 'Provisio...
_Full entry and notes of one finding; its summary entry is in [table-replicas.md](../table-replicas.md).
Generated from ack-api-quirks `services/dynamodb` (model 2012-08-10, controller commit 34b85e6); edit the
finding in the lab, not here._

## Finding

- <a id="ddb-table-289"></a>**DDB-TABLE-289** `response-fidelity` · impact medium · unhandled (not handled in controller) · verified 2026-10-09
  **GSI entries in DescribeTableReplicaAutoScaling: keys ['IndexName', 'IndexStatus', 'ProvisionedReadCapacityAutoScalingSettings', 'Provisio...**
  Replicas[].GlobalSecondaryIndexes[] entry for gsi0 in us-west-2: {"IndexName": "gsi0", "IndexStatus":
  "ACTIVE", "ProvisionedReadCapacityAutoScalingSettings": {"AutoScalingDisabled": true, "ScalingPolicies":
  []}, "ProvisionedWriteCapacityAutoScalingSettings": {"MinimumUnits": 1, "MaximumUnits": 10,
  "AutoScalingRoleArn": "arn:aws:iam::<ACCOUNT>:role/<PRINCIPAL> in us-east-1: {"IndexName": "gsi0",
  "IndexStatus": "ACTIVE", "ProvisionedReadCapacityAutoScalingSettings": {"AutoScalingDisabled": true,
  "ScalingPolicies": []}, "ProvisionedWriteCapacityAutoScalingSettings": {"MinimumUnits": 1, "MaximumUnits":
  10, "AutoScalingRoleArn": "arn:aws:iam::<ACCOUNT>:role/<PRINCIPAL> After enabling all four dimensions:
  {"IndexName": "gsi0", "IndexStatus": "ACTIVE", "ProvisionedReadCapacityAutoScalingSettings":
  {"MinimumUnits": 1, "MaximumUnits": 10, "AutoScalingRoleArn": "arn:aws:iam::<ACCOUNT>:role/<PRINCIPAL>",
  "ScalingPolicies": [{"PolicyName": "DynamoDBReadCapacityUtilization:table/ackq-cf966e-as-gsi/index/gsi0",
  "TargetTrackingScalingPolicyConfiguration": {"TargetValue": 70.0}}]}, "ProvisionedWrite; AAS targets A
  [["/", "table:R", 1, 10], ["/", "table:W", 1, 10], ["/index/gsi0", "index:R", 1, 10], ["/index/gsi0",
  "index:W", 1, 12]], B [["/", "table:W", 1, 10], ["/index/gsi0", "index:W", 1, 12]]; policies [["/", "R",
  "DynamoDBReadCapacityUtilization:table/ac"], ["/", "W", "ackq-cf966e-ackq-cf966e-as-gsi-Units"],
  ["/index/gsi0", "R", "DynamoDBReadCapacityUtilization:table/ac"], ["/index/gsi0", "W",
  "DynamoDBWriteCapacityUtilization:table/a"]]; alarms per kind {"-AlarmHigh": 2, "-AlarmLow": 2,
  "-ProvisionedCapacityHigh": 2, "-ProvisionedCapacityLow": 2, "/index/gsi0-AlarmHigh": 2,
  "/index/gsi0-AlarmLow": 2, "/index/gsi0-ProvisionedCapacityHigh": 2, "/index/gsi0-ProvisionedCapacityLow":
  2}.
  - ACK: compare.nil_equals_zero_value, scope:skip · ops: DescribeTableReplicaAutoScaling,
    UpdateTableReplicaAutoScaling · fields: GlobalSecondaryIndexes, GlobalSecondaryIndexUpdates,
    ReplicaGlobalSecondaryIndexUpdates
  - repro: global PROVISIONED table with GSI -> DescribeTableReplicaAutoScaling -> enable table read, GSI read
    (ReplicaUpdates) and GSI write (GlobalSecondaryIndexUpdates)
  - handling: not handled in the controller (as of commit 34b85e6)
  - related: [DDB-TABLE-189](../table-policy-kinesis-autoscaling.md#ddb-table-189), [DDB-TABLE-195](../table-policy-kinesis-autoscaling.md#ddb-table-195), [DDB-TABLE-243](../table-policy-kinesis-autoscaling.md#ddb-table-243), [DDB-TABLE-232](../table-replicas.md#ddb-table-232), [DDB-TABLE-317](../table-policy-kinesis-autoscaling.md#ddb-table-317), [DDB-TABLE-237](../table-policy-kinesis-autoscaling.md#ddb-table-237) ·
    hypotheses: H-R-102, H-R-108 · evidence: table/dependencies/autoscaling-gsi-and-orphans

## Notes

H-R-102 for GSIs: never-configured GSI read renders as {AutoScalingDisabled:true, ScalingPolicies:[]} exactly
like the table level; GSI entries carry IndexStatus. GSI write updates go through GlobalSecondaryIndexUpdates
(all replicas), GSI read through ReplicaUpdates[].ReplicaGlobalSecondaryIndexUpdates (one region); AAS
resource id 'table/<name>/index/<gsi>' with dimensions dynamodb:index:Read/WriteCapacityUnits; policy names
'DynamoDB<Read|Write>CapacityUtilization:table/<name>/index/<gsi>'.
