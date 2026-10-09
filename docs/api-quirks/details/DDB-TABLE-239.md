<!-- generated: ack-api-quirks dynamodb 2026-10-09 model=2012-08-10 controller=34b85e6 -->
# DDB-TABLE-239: AutoScalingRoleArn other than the SLR is rejected: DynamoDB calls AAS as AWSServiceRoleForDynamoDBReplication, which lacks iam:PassRole
_Full entry and notes of one finding; its summary entry is in [table-replicas.md](../table-replicas.md).
Generated from ack-api-quirks `services/dynamodb` (model 2012-08-10, controller commit 34b85e6); edit the
finding in the lab, not here._

## Finding

- <a id="ddb-table-239"></a>**DDB-TABLE-239** `prerequisite` · impact medium · unhandled (not handled in controller) · verified 2026-10-09
  **AutoScalingRoleArn other than the SLR is rejected: DynamoDB calls AAS as AWSServiceRoleForDynamoDBReplication, which lacks iam:PassRole**
  Nonexistent role ARN -> {"operation": "update_table_replica_auto_scaling", "ok": false, "code":
  "ValidationException", "http_status": 400, "message": "User: arn:aws:sts::<ACCOUNT>:assumed-role/<PRINCIPAL>
  is not authorized to perform: iam:PassRole on resource: arn:aws:iam::<ACCOUNT>:role/<PRINCIPAL> because no
  identity-based policy allows the iam:PassRole action (Service: AWSApplicat", "latency_ms": 994,
  "client_side": false}. Explicit service-linked role ARN -> {"operation":
  "update_table_replica_auto_scaling", "ok": true, "code": null, "http_status": 200, "message": null,
  "latency_ms": 1138, "client_side": false}. Existing non-SLR role (Admin) -> {"operation":
  "update_table_replica_auto_scaling", "ok": false, "code": "ValidationException", "http_status": 400,
  "message": "User: arn:aws:sts::<ACCOUNT>:assumed-role/<PRINCIPAL> is not authorized to perform: iam:PassRole
  on resource: arn:aws:iam::<ACCOUNT>:role/<PRINCIPAL> because no identity-based policy allows the
  iam:PassRole action (Service: AWSApplicationAutoScaling; St", "latency_ms": 693, "client_side": false}.
  Describe role afterwards: arn:aws:iam::<ACCOUNT>:role/<PRINCIPAL> AAS target RoleARN:
  arn:aws:iam::<ACCOUNT>:role/<PRINCIPAL>
  - ACK: is_read_only, docs-only · ops: UpdateTableReplicaAutoScaling · fields: AutoScalingRoleArn
  - repro: UpdateTableReplicaAutoScaling read update with AutoScalingRoleArn variants
  - handling: not handled in the controller (as of commit 34b85e6)
  - related: [DDB-TABLE-237](../table-policy-kinesis-autoscaling.md#ddb-table-237), [DDB-TABLE-240](../table-policy-kinesis-autoscaling.md#ddb-table-240), [DDB-TABLE-191](../table-policy-kinesis-autoscaling.md#ddb-table-191), [DDB-TABLE-189](../table-policy-kinesis-autoscaling.md#ddb-table-189), [DDB-TABLE-196](../table-policy-kinesis-autoscaling.md#ddb-table-196) · hypotheses: H-S-041 ·
    evidence: table/round-trip/autoscaling-settings

## Notes

The error text names the principal 'arn:aws:sts::<ACCOUNT>:assumed-role/<PRINCIPAL>' and the AAS service - the
DynamoDB facade performs the Application Auto Scaling calls under DynamoDB's own service-linked role, not the
caller's identity (qualifies H-R-105). AutoScalingRoleArn is effectively read-only: only the SLR ARN (or
omission) succeeds.
