<!-- generated: ack-api-quirks dynamodb 2026-10-09 model=2012-08-10 controller=34b85e6 -->
# DDB-TABLE-315: Manual UpdateTable RCU below the autoscaling Min is accepted and NOT re-enforced by AAS within 12 min (no activity, no alarm)
_Full entry and notes of one finding; its summary entry is in
[table-policy-kinesis-autoscaling.md](../table-policy-kinesis-autoscaling.md). Generated from ack-api-quirks
`services/dynamodb` (model 2012-08-10, controller commit 34b85e6); edit the finding in the lab, not here._

## Finding

- <a id="ddb-table-315"></a>**DDB-TABLE-315** `requested-vs-effective` · impact high · unhandled (not handled in controller) · verified 2026-10-09
  **Manual UpdateTable RCU below the autoscaling Min is accepted and NOT re-enforced by AAS within 12 min (no activity, no alarm)**
  With read autoscaling Min 10 Max 20 and RCU 10, UpdateTable RCU=5 -> {"operation": "update_table", "ok":
  false, "code": "ResourceInUseException", "http_status": 400, "message": "The resource which you are
  attempting to change is in use.", "latency_ms": 567, "client_side": false}. Transitions: [{"t_s": 0.2,
  "status": "UPDATING", "rcu": 10, "wcu": 5, "decreases": 0, "replica": {"us-east-1": ["ACTIVE",
  {"ReadCapacityUnits": 5}]}}, {"t_s": 41.2, "status": "ACTIVE", "rcu": 5, "wcu": 5, "decreases": 1,
  "replica": {"us-east-1": ["ACTIVE", null]}}, {"t_s": 491.3, "status": "ACTIVE", "rcu": 5, "wcu": 1,
  "decreases": 2, "replica": {"us-east-1": ["ACTIVE", null]}}]. Scaling activities: []. Alarm states after:
  {"write": {"AlarmHigh": "OK", "AlarmLow": "ALARM", "ProvisionedCapacityHigh": "INSUFFICIENT_DATA",
  "ProvisionedCapacityLow": "OK"}, "read": {"AlarmHigh": "OK", "AlarmLow": "ALARM", "ProvisionedCapacityHigh":
  "OK", "ProvisionedCapacityLow": "OK"}}.
  - ACK: compare.is_ignored+delta_pre_compare, custom_update · ops: UpdateTable, DescribeTable · fields:
    ProvisionedThroughput.ReadCapacityUnits
  - repro: autoscaled read Min 10 -> UpdateTable RCU=5 -> poll DescribeTable 10 s /
    describe-scaling-activities 30 s
  - measurements: seconds_until_reenforced=0.2, watch_elapsed_s=726.4
  - handling: not handled in the controller (as of commit 34b85e6)
  - related: [DDB-TABLE-314](../table-policy-kinesis-autoscaling.md#ddb-table-314), [DDB-TABLE-195](../table-policy-kinesis-autoscaling.md#ddb-table-195), [DDB-TABLE-256](../table-replicas.md#ddb-table-256), [DDB-TABLE-321](../table-replicas.md#ddb-table-321), [DDB-TABLE-306](../table-policy-kinesis-autoscaling.md#ddb-table-306), [DDB-TABLE-200](../table-replicas.md#ddb-table-200),
    [DDB-TABLE-296](../table-replicas.md#ddb-table-296), [DDB-TABLE-263](../table-global-tables.md#ddb-table-263), [DDB-TABLE-327](../table-replicas.md#ddb-table-327) · hypotheses: H-R-117, H-R-132 · evidence:
    table/mutation-matrix/autoscaling-vs-throughput

## Notes

REFUTES H-R-117's 'AAS re-enforces the bounds without an alarm breach within ~10 minutes': with Min 10 and a
manual RCU=5, no scaling activity occurred in 726 s; ProvisionedCapacityLow stayed OK (threshold 5.0,
LessThanThreshold, 3x300 s) and AlarmLow was already in ALARM (idle table) but cannot scale below the current
value. Bounds are enforced only at registration/update time ('minimum capacity was set to N'). Meanwhile the
idle WRITE dimension (WCU 5, Min 1, zero traffic since creation) was scaled in to 1 by its AlarmLow ~10 min
after the policy was created (activity 00:58:09, policy created ~00:47:50) - idle tables DO scale in without
traffic. H-R-132: the manual value persisted, so a controller re-asserting spec.provisionedThroughput below
Min would 'win' until the next alarm-driven activity.

Contradiction with [DDB-TABLE-314](../table-policy-kinesis-autoscaling.md#ddb-table-314): 315's title says the manual RCU=5 UpdateTable is accepted, but its behavior
text shows that call failing with ResourceInUseException Resolution: title is correct: the recorded call
(00:49:32, 6 s after a DP=false change - cf. 314 notes) got ResourceInUseException and the retry at 00:50:03
succeeded (evidence row 136); transitions RCU 10->5, decreases 1, no scaling activity in 726 s support the
title. measurement seconds_until_reenforced=0.2 is spurious
