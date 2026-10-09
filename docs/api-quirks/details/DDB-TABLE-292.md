<!-- generated: ack-api-quirks dynamodb 2026-10-09 model=2012-08-10 controller=34b85e6 -->
# DDB-TABLE-292: DeleteTable (and replica removal) orphan AAS targets/policies/alarms; recreating the name does not re-arm them within 4 min
_Full entry and notes of one finding; its summary entry is in [table-replicas.md](../table-replicas.md).
Generated from ack-api-quirks `services/dynamodb` (model 2012-08-10, controller commit 34b85e6); edit the
finding in the lab, not here._

## Finding

- <a id="ddb-table-292"></a>**DDB-TABLE-292** `delete-semantics` · impact high · unhandled (not handled in controller) · verified 2026-10-09
  **DeleteTable (and replica removal) orphan AAS targets/policies/alarms; recreating the name does not re-arm them within 4 min**
  Before delete: {"targets_a": [["/", "table:R", 3, 10], ["/", "table:W", 1, 10], ["/index/gsi0", "index:R",
  1, 10]], "targets_b": [["/", "table:W", 1, 10]], "policies_a": [["/", "R",
  "DynamoDBReadCapacityUtilization:table/ac"], ["/", "W", "DynamoDBWriteCapacityUtilization:table/a"],
  ["/index/gsi0", "R", "DynamoDBReadCapacityUtilization:table/ac"]], "alarms": {"-AlarmHigh": 2, "-AlarmLow":
  2, "-ProvisionedCapacityHigh": 2, "-ProvisionedCapacityLow": 2, "/index/gsi0-AlarmHigh": 1,
  "/index/gsi0-AlarmLow": 1, "/ind. After removing the replica: {"targets_a": [["/", "table:R", 3, 10], ["/",
  "table:W", 1, 10], ["/index/gsi0", "index:R", 1, 10]], "targets_b": [["/", "table:W", 1, 10]], "alarms":
  {"-AlarmHigh": 2, "-AlarmLow": 2, "-ProvisionedCapacityHigh": 2, "-ProvisionedCapacityLow": 2,
  "/index/gsi0-AlarmHigh": 1, "/index/gsi0-AlarmLow": 1, "/index/gsi0-ProvisionedCapacityHigh": 1,
  "/index/gsi0-ProvisionedCapacityLow": 1}}. After DeleteTable (RNF reached) samples: [{"t_s": 0, "targets_a":
  [["/", "table:R", 3, 10], ["/", "table:W", 1, 10], ["/index/gsi0", "index:R", 1, 10]], "policies_a": [["/",
  "R", "DynamoDBReadCapacityUtilization:table/ac"], ["/", "W", "DynamoDBWriteCapacityUtilization:table/a"],
  ["/index/gsi0", "R", "DynamoDBReadCapacityUtilization:table/ac"]], "alarms": {"-AlarmHigh": 2, "-AlarmLow":
  2, "-ProvisionedCapacityHigh": 2, "-ProvisionedCapacityLow": 2, "/index/gsi0-AlarmHigh": 1,
  "/index/gsi0-AlarmLow": 1, "/index/gsi0-ProvisionedCapacityHigh": 1, "/index/gsi0-ProvisionedCapacityLow":
  1}}, {"t_s": 30, "targets_a": [["/", "table:R", 3, 10], ["/", "table:W", 1, 10], ["/index/gsi0", "index:R",
  1, 10]], "policies_a": [["/", "R", "DynamoDBReadCapacityUtilization:table/ac"], ["/", "W",
  "DynamoDBWriteCapacityUtilization:table/a"], ["/index/gsi0", "R",
  "DynamoDBReadCapacityUtilization:table/ac"]], "alarms": {"-AlarmHigh": 2, "-AlarmLow": 2,. Recreating the
  same table name (PROVISIONED 1/1, regional): RCU transitions over 4 min [{"t_s": 0.0, "status": "ACTIVE",
  "rcu": 1}]; read scaling activities [["2026-10-09T00:58:20.225000+00:00", "Successful", "Setting read
  capacity units to 3.", "Successfully set read capacity units to 3. Change successfully fulfilled by
  dynamodb."]]; DescribeTableReplicaAutoScaling on the recreated regional table -> ResourceNotFoundException.
  - ACK: pre-delete-cleanup, scope:skip · ops: DeleteTable, CreateTable, DescribeScalableTargets · fields:
    ProvisionedThroughput
  - repro: autoscaled table -> remove replica -> DeleteTable -> describe-scalable-targets at 0/30/90 s ->
    CreateTable same name -> poll ProvisionedThroughput
  - handling: not handled in the controller (as of commit 34b85e6)
  - related: [DDB-TABLE-293](../table-replicas.md#ddb-table-293), [DDB-TABLE-242](../table-policy-kinesis-autoscaling.md#ddb-table-242), [DDB-TABLE-198](../table-policy-kinesis-autoscaling.md#ddb-table-198), [DDB-TABLE-290](../table-policy-kinesis-autoscaling.md#ddb-table-290), [DDB-TABLE-323](../table-policy-kinesis-autoscaling.md#ddb-table-323), [DDB-TABLE-243](../table-policy-kinesis-autoscaling.md#ddb-table-243),
    [DDB-TABLEREPLICAAUTOSCALING-001](../table-replicas.md#ddb-tablereplicaautoscaling-001), [DDB-TABLE-193](../table-policy-kinesis-autoscaling.md#ddb-table-193), [DDB-TABLE-197](../table-policy-kinesis-autoscaling.md#ddb-table-197), [DDB-TABLE-196](../table-policy-kinesis-autoscaling.md#ddb-table-196), [DDB-TABLE-189](../table-policy-kinesis-autoscaling.md#ddb-table-189), [DDB-TABLE-195](../table-policy-kinesis-autoscaling.md#ddb-table-195)
    · hypotheses: H-R-131 · evidence: table/dependencies/autoscaling-gsi-and-orphans

## Notes

H-R-131 row 3 confirmed: 90 s after the table was gone all 3 targets, 3 policies and 12 alarms still existed
in us-west-2, and the replica region kept the table write target after the replica was removed (targets_b
[["/", "table:W", 1, 10]]). Recreating the same table name (regional, RCU 1) with an orphaned read target Min
3 produced no scaling activity within 4 min (RCU stayed 1; last activity [["2026-10-09T00:58:20.225000+00:00",
"Successful", "Setting read capacity units to 3.", "Successfully set read capacity units to 3. Change
successfully fulfill) and DescribeTableReplicaAutoScaling on the recreated regional table ->
ResourceNotFoundException. The orphans had to be deregistered explicitly (cleanup_targets {"a": [], "b": [],
"alarms": {}}).
