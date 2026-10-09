<!-- generated: ack-api-quirks dynamodb 2026-10-09 model=2012-08-10 controller=34b85e6 -->
# DDB-TABLE-322: Adding a PROVISIONED GSI to a PROVISIONED global table succeeds and DynamoDB auto-registers AAS write autoscaling for the new GSI
_Full entry and notes of one finding; its summary entry is in
[table-policy-kinesis-autoscaling.md](../table-policy-kinesis-autoscaling.md). Generated from ack-api-quirks
`services/dynamodb` (model 2012-08-10, controller commit 34b85e6); edit the finding in the lab, not here._

## Finding

- <a id="ddb-table-322"></a>**DDB-TABLE-322** `server-default` · impact high · unhandled (not handled in controller) · verified 2026-10-09
  **Adding a PROVISIONED GSI to a PROVISIONED global table succeeds and DynamoDB auto-registers AAS write autoscaling for the new GSI**
  UpdateTable(GlobalSecondaryIndexUpdates Create gsi1, ProvisionedThroughput 1/1) on a 2-replica PROVISIONED
  table whose table write capacity is autoscaled -> 200 (no autoscaling parameters exist on UpdateTable).
  While IndexStatus=CREATING an AAS scalable target table/<name>/index/gsi1 dynamodb:index:WriteCapacityUnits
  already existed in BOTH regions; once ACTIVE the GSI reads in DescribeTableReplicaAutoScaling as write
  {MinimumUnits 1, MaximumUnits 10, SLR role, policy
  'DynamoDBWriteCapacityUtilization:table/<name>/index/gsi1' TargetValue 70} and read
  {AutoScalingDisabled:true, ScalingPolicies:[]}. AAS RegisterScalableTarget for the not-yet-existing index id
  -> ValidationException 'DynamoDB index does not exist: table/<name>/index/gsi1'.
  - ACK: late_initialize, compare.is_ignored+delta_pre_compare, scope:skip · ops: UpdateTable,
    RegisterScalableTarget, PutScalingPolicy · fields: GlobalSecondaryIndexUpdates
  - repro: PROVISIONED global table -> UpdateTable add GSI (ValidationException) -> register-scalable-target
    table/<n>/index/<gsi> + put-scaling-policy -> UpdateTable add GSI
  - handling: not handled in the controller (as of commit 34b85e6)
  - related: [DDB-TABLE-249](../table-replicas.md#ddb-table-249), [DDB-TABLE-288](../table-replicas.md#ddb-table-288), [DDB-TABLE-291](../table-policy-kinesis-autoscaling.md#ddb-table-291), [DDB-TABLE-290](../table-policy-kinesis-autoscaling.md#ddb-table-290), [DDB-TABLE-194](../table-replicas.md#ddb-table-194), [DDB-TABLE-296](../table-replicas.md#ddb-table-296),
    [DDB-TABLE-251](../table-replicas.md#ddb-table-251), [DDB-TABLE-265](../table-replicas.md#ddb-table-265), [DDB-TABLE-294](../table-replicas.md#ddb-table-294), [DDB-TABLE-295](../table-replicas.md#ddb-table-295), [DDB-TABLE-297](../table-replicas.md#ddb-table-297) · hypotheses: H-R-108, H-R-131 ·
    evidence: table/creative/gsi-add-on-provisioned-global-table

## Notes

REFUTES the doc-based clause of H-R-108 ('a GSI added later gets NO autoscaling') for 2019.11.21 PROVISIONED
global tables: write autoscaling Min 1 Max 10 target 70 is created server-side (this is how DynamoDB keeps the
'GSI write capacity must be autoscaled' invariant). The earlier rejection seen in
table/dependencies/autoscaling-gsi-and-orphans was caused by an existing GSI whose write autoscaling had been
disabled. Backfill of the empty GSI took 535 s on the global table.

Contradiction with [DDB-TABLE-291](../table-policy-kinesis-autoscaling.md#ddb-table-291): 291 (dependencies run) recorded UpdateTable GSI Create on a PROVISIONED
global table -> ValidationException 'GSI write capacity should either be Pay-Per-Request or AutoScaled.'; 322
shows the same call succeeding and DynamoDB auto-registering write autoscaling for the new GSI Resolution:
already reconciled in 291's notes/title: the rejection was caused by the existing gsi0 whose write autoscaling
had just been disabled (290), and the message does not name the offending index. 322 canonical for the success
path; keep 291 for the 'any un-autoscaled GSI blocks every GSI UpdateTable' rule
