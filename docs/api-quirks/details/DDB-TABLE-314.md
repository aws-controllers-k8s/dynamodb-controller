<!-- generated: ack-api-quirks dynamodb 2026-10-09 model=2012-08-10 controller=34b85e6 -->
# DDB-TABLE-314: Autoscaling Min above current capacity: AAS issues its own UpdateTable immediately (RCU 5->10 visible at 4 s, ACTIVE after 33 s)
_Full entry and notes of one finding; its summary entry is in
[table-policy-kinesis-autoscaling.md](../table-policy-kinesis-autoscaling.md). Generated from ack-api-quirks
`services/dynamodb` (model 2012-08-10, controller commit 34b85e6); edit the finding in the lab, not here._

## Finding

- <a id="ddb-table-314"></a>**DDB-TABLE-314** `async-state-machine` · impact high · unhandled (not handled in controller) · verified 2026-10-09
  **Autoscaling Min above current capacity: AAS issues its own UpdateTable immediately (RCU 5->10 visible at 4 s, ACTIVE after 33 s)**
  UpdateTableReplicaAutoScaling(read Min 10 Max 20) returned 200 with TableStatus UPDATING. DescribeTable
  transitions (t_s,status,rcu,wcu,decreases): [{"t_s": 0.2, "status": "UPDATING", "rcu": 5, "wcu": 5,
  "decreases": 0, "replica": {"us-east-1": ["ACTIVE", null]}}, {"t_s": 4.4, "status": "UPDATING", "rcu": 10,
  "wcu": 5, "decreases": 0, "replica": {"us-east-1": ["ACTIVE", {"ReadCapacityUnits": 5}]}}, {"t_s": 33.0,
  "status": "ACTIVE", "rcu": 10, "wcu": 5, "decreases": 0, "replica": {"us-east-1": ["ACTIVE",
  {"ReadCapacityUnits": 5}]}}]. Scaling activities: [{"start": "2026-10-09T00:48:36.763000+00:00", "end":
  "2026-10-09T00:49:07.252000+00:00", "status": "Successful", "desc": "Setting read capacity units to 10.",
  "cause": "minimum capacity was set to 10", "msg": "Successfully set read capacity units to 10. Change
  successfully fulfilled by dynamodb."}]. UpdateTable(DeletionProtectionEnabled) fired at the first UPDATING
  sample -> {"operation": "update_table", "ok": true, "code": null, "http_status": 200, "message": null,
  "latency_ms": 1022, "client_side": false}. Read alarm definitions: [{"name": "AlarmHigh", "metric":
  "ConsumedReadCapacityUnits", "metrics_expr": [], "stat": "Sum", "period": 60, "eval": 2, "datapoints": null,
  "op": "GreaterThanThreshold", "threshold": 210.0, "missing": null, "state": "INSUFFICIENT_DATA"}, {"name":
  "AlarmLow", "metric": "ConsumedReadCapacityUnits", "metrics_expr": [], "stat": "Sum", "period": 60, "eval":
  15, "datapoints": null, "op": "LessThanThreshold", "threshold": 150.0, "missing": null, "state":
  "INSUFFICIENT_DATA"}, {"name": "ProvisionedCapacityHigh", "metric": "ProvisionedReadCapacityUnits",
  "metrics_expr": [], "stat": "Average", "period": 300, "eval": 3, "datapoints": null, "op":
  "GreaterThanThreshold", "threshold": 5.0, "missing": null, "state": "INSUFFICIENT_DATA"}, {"name":
  "ProvisionedCapacityLow", "metric": "ProvisionedReadCapacityUnits", "metrics_expr": [], "stat": "Average",
  "period": 300, "eval": 3, "datapoints": null, "op.
  - ACK: requeue, compare.is_ignored+delta_pre_compare · ops: UpdateTableReplicaAutoScaling, UpdateTable,
    DescribeTable · fields: ProvisionedThroughput.ReadCapacityUnits, TableStatus
  - repro: RCU 5 table -> UpdateTableReplicaAutoScaling read Min 10 -> poll DescribeTable every 3 s +
    describe-scaling-activities
  - measurements: seconds_until_capacity_changed=4.4, elapsed_until_active_at_min_s=33.0
  - handling: not handled in the controller (as of commit 34b85e6)
  - related: [DDB-TABLE-238](../table-replicas.md#ddb-table-238), [DDB-TABLE-192](../table-replicas.md#ddb-table-192), [DDB-TABLE-198](../table-policy-kinesis-autoscaling.md#ddb-table-198), [DDB-TABLE-242](../table-policy-kinesis-autoscaling.md#ddb-table-242), [DDB-TABLE-195](../table-policy-kinesis-autoscaling.md#ddb-table-195), [DDB-TABLE-315](../table-policy-kinesis-autoscaling.md#ddb-table-315),
    [DDB-TABLE-256](../table-replicas.md#ddb-table-256), [DDB-TABLE-321](../table-replicas.md#ddb-table-321), [DDB-TABLE-306](../table-policy-kinesis-autoscaling.md#ddb-table-306), [DDB-TABLE-200](../table-replicas.md#ddb-table-200), [DDB-TABLE-296](../table-replicas.md#ddb-table-296), [DDB-TABLE-263](../table-global-tables.md#ddb-table-263), [DDB-TABLE-327](../table-replicas.md#ddb-table-327),
    [DDB-TABLE-251](../table-replicas.md#ddb-table-251) · hypotheses: H-R-106, H-S-045 · evidence: table/mutation-matrix/autoscaling-vs-throughput

## Notes

H-R-106 confirmed with faster timing than hypothesised: the scaling activity ('Setting read capacity units to
10', cause 'minimum capacity was set to 10') started in the same second as UpdateTableReplicaAutoScaling,
whose response already reported TableStatus UPDATING. The AAS-driven UPDATING lasted ~33 s. An UpdateTable
(DeletionProtectionEnabled) fired during that window was accepted (200); a ProvisionedThroughput UpdateTable 6
s after a later deletion-protection change got ResourceInUseException. H-S-045: the autoscaling call itself
does not flip TableStatus, the resulting AAS UpdateTable does. Alarms created per dimension: AlarmHigh
(Consumed > threshold, 2x60 s), AlarmLow (Consumed < threshold, 15x60 s), ProvisionedCapacityHigh/Low
(Provisioned*CapacityUnits avg over 3x300 s).

Contradiction with [DDB-TABLE-251](../table-replicas.md#ddb-table-251), [DDB-TABLE-321](../table-replicas.md#ddb-table-321): 251 claims provisioned RCU/WCU replicate between ALL regions
and overrides are the only per-region knob; 321/314 show an AAS-driven RCU change on the source NOT
replicating - the replica keeps the old value via a ProvisionedThroughputOverride the caller never set
Resolution: keep both; the likely discriminator is an AAS read target on the dimension (321/314 tables had
read autoscaling, 251's had write-only AAS) - needs a targeted re-probe. Controller consequence stands either
way: Replicas[].ProvisionedThroughputOverride can be server-owned

Contradiction with [DDB-TABLE-315](../table-policy-kinesis-autoscaling.md#ddb-table-315): 315's title says the manual RCU=5 UpdateTable is accepted, but its behavior
text shows that call failing with ResourceInUseException Resolution: title is correct: the recorded call
(00:49:32, 6 s after a DP=false change - cf. 314 notes) got ResourceInUseException and the retry at 00:50:03
succeeded (evidence row 136); transitions RCU 10->5, decreases 1, no scaling activity in 726 s support the
title. measurement seconds_until_reenforced=0.2 is spurious
