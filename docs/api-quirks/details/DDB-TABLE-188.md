<!-- generated: ack-api-quirks dynamodb 2026-10-09 model=2012-08-10 controller=34b85e6 -->
# DDB-TABLE-188: Adding a replica to a PROVISIONED table requires write autoscaling first: 'Table write capacity should either be Pay-Per-Request or AutoS...
_Full entry and notes of one finding; its summary entry is in [table-replicas.md](../table-replicas.md).
Generated from ack-api-quirks `services/dynamodb` (model 2012-08-10, controller commit 34b85e6); edit the
finding in the lab, not here._

## Finding

- <a id="ddb-table-188"></a>**DDB-TABLE-188** `prerequisite` · impact high · SUSPECTED CONTROLLER BUG · verified 2026-10-09
  **Adding a replica to a PROVISIONED table requires write autoscaling first: 'Table write capacity should either be Pay-Per-Request or AutoS...**
  UpdateTable(ReplicaUpdates=[Create us-east-1]) on a PROVISIONED 1/1 regional table -> ValidationException
  'Table write capacity should either be Pay-Per-Request or AutoScaled.'. With only an AAS scalable target (no
  policy) on dynamodb:table:WriteCapacityUnits -> ValidationException. With target + target-tracking policy -> 200.
  UpdateTableReplicaAutoScaling cannot be used to satisfy this because it is itself rejected on a regional
  table, so the write autoscaling must be created through Application Auto Scaling (or the table must be
  PAY_PER_REQUEST: add replica -> 200).
  - ACK: custom_update, references, scope:skip · ops: UpdateTable, RegisterScalableTarget, PutScalingPolicy ·
    fields: ReplicaUpdates, BillingMode, ProvisionedThroughput
  - repro: CreateTable PROVISIONED + streams -> UpdateTable ReplicaUpdates Create -> ValidationException ->
    application-autoscaling register-scalable-target + put-scaling-policy -> retry
  - handling: suspected controller bug - see Handling gaps · tracked in https://github.com/aws-controllers-k8s/community/issues/2610 (not handled) · code refs: `5bbfe82`
  - related: [DDB-TABLE-249](../table-replicas.md#ddb-table-249) · hypotheses: H-R-033, H-R-131 · evidence:
    table/sub-resources/replica-autoscaling-facade

## Notes

Suspected controller bug confirmed by evidence: Suspicion confirmed: the prerequisite is server-enforced
(188/249), cannot be met through the DynamoDB API (UpdateTableReplicaAutoScaling -> RNF on a regional table,
187/188; a bare AAS target without a policy is insufficient, 188) and also covers every GSI write dimension
(288/291). The condition is recoverable without a spec change (register AAS target+policy, then retry; first
retry may 500, 249), so classifying the ValidationException as terminal wedges the CR. Follow-on: once
autoscaled, AAS rewrites provisioned throughput out-of-band (314/315), so spec.provisionedThroughput on a
PROVISIONED global table can never be stable.
