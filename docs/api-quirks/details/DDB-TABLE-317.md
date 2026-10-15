<!-- generated: ack-api-quirks dynamodb 2026-10-09 model=2012-08-10 controller=34b85e6 -->
# DDB-TABLE-317: Switching an autoscaled global table to PAY_PER_REQUEST keeps the AAS targets/policies; Describe shows Min/Max AND AutoScalingDisabled=true
_Full entry and notes of one finding; its summary entry is in
[table-policy-kinesis-autoscaling.md](../table-policy-kinesis-autoscaling.md). Generated from ack-api-quirks
`services/dynamodb` (model 2012-08-10, controller commit 34b85e6); edit the finding in the lab, not here._

## Finding

- <a id="ddb-table-317"></a>**DDB-TABLE-317** `requested-vs-effective` · impact medium · unhandled (not handled in controller) · verified 2026-10-09
  **Switching an autoscaled global table to PAY_PER_REQUEST keeps the AAS targets/policies; Describe shows Min/Max AND AutoScalingDisabled=true**
  UpdateTable BillingMode=PAY_PER_REQUEST -> 200, timeline [{"value": "UPDATING|rep:ACTIVE", "from_s": 0.15,
  "to_s": 218.12, "duration_s": 217.97}, {"value": "ACTIVE|rep:ACTIVE", "from_s": 218.12, "to_s": null,
  "duration_s": null}]. After ACTIVE: DescribeTableReplicaAutoScaling 200 read {"MinimumUnits": 10,
  "MaximumUnits": 20, "AutoScalingDisabled": true, "AutoScalingRoleArn":
  "arn:aws:iam::<ACCOUNT>:role/<PRINCIPAL>", "ScalingPolicies": [{"PolicyName":
  "DynamoDBReadCapacityUtilization:table/ackq-30f2d7-as-tp", "TargetTrackingScalingPolicyConfiguration":
  {"TargetValue": 70.0}}]} write {"MinimumUnits": 1, "MaximumUnits": 10, "AutoScalingDisabled": true,
  "AutoScalingRoleArn": "arn:aws:iam::<ACCOUNT>:role/<PRINCIPAL>", "ScalingPolicies": [{"PolicyName":
  "ackq-30f2d7-w0", "TargetTrackingScalingPolicyConfiguration": {"TargetValue": 70.0}}]}; AAS targets A
  [["ReadCapacityUnits", 10, 20], ["WriteCapacityUnits", 1, 10]] B [["WriteCapacityUnits", 1, 10]]; policies A
  [["WriteCapacityUnits", "ackq-30f2d7-w0"], ["ReadCapacityUnits",
  "DynamoDBReadCapacityUtilization:table/ackq-30f2d7-as-tp"]] B [["WriteCapacityUnits", "ackq-30f2d7-w0"]];
  alarms {"write": {"AlarmHigh": "OK", "AlarmLow": "ALARM", "ProvisionedCapacityHigh": "OK",
  "ProvisionedCapacityLow": "OK"}, "read": {"AlarmHigh": "OK", "AlarmLow": "ALARM", "ProvisionedCapacityHigh":
  "OK", "ProvisionedCapacityLow": "OK"}}; read activities since switch []. UpdateTableReplicaAutoScaling read
  on the PPR table -> {"operation": "update_table_replica_auto_scaling", "ok": false, "code":
  "ValidationException", "http_status": 400, "message": "Failed to update global table with name
  ‘ackq-30f2d7-as-tp‘. Replicas 'US-EAST-1', 'US-WEST-2' BillingMode is PayPerRequest. You must convert
  table's BillingMode to PROVISIONED to set parameters:
  'ReplicaProvisionedReadCapacityAutoScalingSettingsUpdate'.", "latency_ms": 697, "client_side": false};
  disable -> 200 (targets after [["WriteCapacityUnits", 1, 10]]).
  - ACK: custom_update, docs-only · ops: UpdateTable, DescribeTableReplicaAutoScaling,
    UpdateTableReplicaAutoScaling · fields: BillingMode, AutoScalingDisabled
  - repro: autoscaled PROVISIONED global table -> UpdateTable BillingMode=PAY_PER_REQUEST ->
    describe-scalable-targets, DescribeTableReplicaAutoScaling
  - handling: not handled in the controller (as of commit 34b85e6)
  - related: [DDB-TABLE-189](../table-policy-kinesis-autoscaling.md#ddb-table-189), [DDB-TABLE-195](../table-policy-kinesis-autoscaling.md#ddb-table-195), [DDB-TABLE-243](../table-policy-kinesis-autoscaling.md#ddb-table-243), [DDB-TABLE-232](../table-replicas.md#ddb-table-232), [DDB-TABLE-289](../table-replicas.md#ddb-table-289), [DDB-TABLE-237](../table-policy-kinesis-autoscaling.md#ddb-table-237),
    [DDB-TABLE-190](../table-replicas.md#ddb-table-190) · hypotheses: H-R-103 · evidence: table/mutation-matrix/autoscaling-vs-throughput

## Notes

H-R-103 partially confirmed: AAS targets/policies/alarms survive in both regions and
DescribeTableReplicaAutoScaling shows a THIRD shape {MinimumUnits, MaximumUnits, AutoScalingDisabled: true,
AutoScalingRoleArn, ScalingPolicies[...]} (disabled flag plus full settings) - unlike the 'never configured'
shape {AutoScalingDisabled:true, ScalingPolicies:[]}. The billing switch took 218 s of UPDATING and counted as
a decrease (NumberOfDecreasesToday 3->4); ProvisionedThroughput reads 0/0. On the PPR table a read update ->
ValidationException ('BillingMode is PayPerRequest. You must convert table's BillingMode to PROVISIONED ...')
while AutoScalingDisabled=true -> 200 and deregistered the AAS target. Switching back to PROVISIONED (24 h
rule) was not tested.
