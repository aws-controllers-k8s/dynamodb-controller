<!-- generated: ack-api-quirks dynamodb 2026-10-09 model=2012-08-10 controller=34b85e6 -->
# DDB-TABLE-191: Service-linked role AWSServiceRoleForApplicationAutoScaling_DynamoDBTable appears as a side effect of global-table operations
_Full entry and notes of one finding; its summary entry is in
[table-policy-kinesis-autoscaling.md](../table-policy-kinesis-autoscaling.md). Generated from ack-api-quirks
`services/dynamodb` (model 2012-08-10, controller commit 34b85e6); edit the finding in the lab, not here._

## Finding

- <a id="ddb-table-191"></a>**DDB-TABLE-191** `prerequisite` · impact medium · unhandled (not handled in controller) · verified 2026-10-09
  **Service-linked role AWSServiceRoleForApplicationAutoScaling_DynamoDBTable appears as a side effect of global-table operations**
  iam:GetRole for the role returned NoSuchEntity at 00:16 UTC (account had never used DynamoDB autoscaling);
  at 00:27:16 UTC it existed, created while the only activity in the account was an earlier attempt of this
  probe that ran UpdateTable(ReplicaUpdates Create) on a PAY_PER_REQUEST table and the replica removal - no
  RegisterScalableTarget / UpdateTableReplicaAutoScaling call had succeeded yet (all were
  ResourceNotFoundException on regional tables). Every later Describe reports AutoScalingRoleArn =
  arn:aws:iam::<ACCOUNT>:role/<PRINCIPAL> targets registered via AAS without RoleARN get the same role.
  - ACK: docs-only · ops: UpdateTableReplicaAutoScaling, RegisterScalableTarget · fields: AutoScalingRoleArn
  - repro: iam get-role; register-scalable-target / UpdateTableReplicaAutoScaling; iam get-role
  - handling: not handled in the controller (as of commit 34b85e6)
  - related: [DDB-TABLE-237](../table-policy-kinesis-autoscaling.md#ddb-table-237), [DDB-TABLE-240](../table-policy-kinesis-autoscaling.md#ddb-table-240), [DDB-TABLE-239](../table-replicas.md#ddb-table-239), [DDB-TABLE-189](../table-policy-kinesis-autoscaling.md#ddb-table-189), [DDB-TABLE-196](../table-policy-kinesis-autoscaling.md#ddb-table-196) · hypotheses: H-R-105,
    H-S-041 · evidence: table/sub-resources/replica-autoscaling-facade

## Notes

H-R-105 partially confirmed: the SLR is created implicitly and before any explicit autoscaling call succeeds
(2019.11.21 replica add/remove is enough). IAM-restricted half of H-R-105 not run (admin caller). H-S-041:
AutoScalingRoleArn is always the SLR.
