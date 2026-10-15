<!-- generated: ack-api-quirks dynamodb 2026-10-09 model=2012-08-10 controller=34b85e6 -->
# DDB-TABLE-288: Adding a replica to a PROVISIONED table with a GSI: table write autoscaling only -> ValidationException; with GSI write autoscaling -> 200
_Full entry and notes of one finding; its summary entry is in [table-replicas.md](../table-replicas.md).
Generated from ack-api-quirks `services/dynamodb` (model 2012-08-10, controller commit 34b85e6); edit the
finding in the lab, not here._

## Finding

- <a id="ddb-table-288"></a>**DDB-TABLE-288** `prerequisite` · impact high · tracked in GitHub issue (not handled) · verified 2026-10-09
  **Adding a replica to a PROVISIONED table with a GSI: table write autoscaling only -> ValidationException; with GSI write autoscaling -> 200**
  UpdateTable(ReplicaUpdates Create) with AAS write autoscaling on the table only -> {"operation":
  "update_table", "ok": false, "code": "ValidationException", "http_status": 400, "message": "GSI write
  capacity should either be Pay-Per-Request or AutoScaled.", "latency_ms": 1011, "client_side": false}. After
  also registering table/<name>/index/gsi0 dynamodb:index:WriteCapacityUnits + policy -> {"operation":
  "update_table", "ok": true, "code": null, "http_status": 200, "message": null, "latency_ms": 2577,
  "client_side": false}. AAS targets copied to us-east-1 after the replica became ACTIVE: [["/", "table:W", 1,
  10], ["/index/gsi0", "index:W", 1, 10]].
  - ACK: custom_update, references · ops: UpdateTable, RegisterScalableTarget · fields: ReplicaUpdates,
    GlobalSecondaryIndexes.ProvisionedThroughput
  - repro: PROVISIONED table + GSI -> AAS write autoscaling on table -> UpdateTable add replica -> add GSI
    write autoscaling -> retry
  - handling: tracked in https://github.com/aws-controllers-k8s/community/issues/2610 (not handled) · code refs: `5bbfe82`
  - related: [DDB-TABLE-249](../table-replicas.md#ddb-table-249), [DDB-TABLE-291](../table-policy-kinesis-autoscaling.md#ddb-table-291), [DDB-TABLE-322](../table-policy-kinesis-autoscaling.md#ddb-table-322), [DDB-TABLE-290](../table-policy-kinesis-autoscaling.md#ddb-table-290), [DDB-TABLE-194](../table-replicas.md#ddb-table-194) · hypotheses: H-R-108,
    H-R-131 · evidence: table/dependencies/autoscaling-gsi-and-orphans

## Notes

Every PROVISIONED write dimension (table AND each GSI) must have an AAS target+policy before
UpdateTable(ReplicaUpdates Create) is accepted: 'GSI write capacity should either be Pay-Per-Request or
AutoScaled.' After the replica is ACTIVE the write targets+policies exist in the replica region too (targets_b
[["/", "table:W", 1, 10], ["/index/gsi0", "index:W", 1, 10]]).
