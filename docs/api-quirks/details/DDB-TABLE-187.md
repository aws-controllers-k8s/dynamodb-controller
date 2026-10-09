<!-- generated: ack-api-quirks dynamodb 2026-10-09 model=2012-08-10 controller=34b85e6 -->
# DDB-TABLE-187: DescribeTableReplicaAutoScaling/UpdateTableReplicaAutoScaling fail on a regional table: RNF 'Global table ... does not exist'
_Full entry and notes of one finding; its summary entry is in [table-replicas.md](../table-replicas.md).
Generated from ack-api-quirks `services/dynamodb` (model 2012-08-10, controller commit 34b85e6); edit the
finding in the lab, not here._

## Finding

- <a id="ddb-table-187"></a>**DDB-TABLE-187** `prerequisite` · impact high · unhandled (not handled in controller) · verified 2026-10-09
  **DescribeTableReplicaAutoScaling/UpdateTableReplicaAutoScaling fail on a regional table: RNF 'Global table ... does not exist'**
  On an ACTIVE single-region PROVISIONED table (streams on, never had a replica)
  DescribeTableReplicaAutoScaling -> HTTP 400 ResourceNotFoundException 'Global table with name:
  'ackq-0ad4a5-as-prov' does not exist.'; UpdateTableReplicaAutoScaling (write) -> ResourceNotFoundException,
  (read via ReplicaUpdates own region) -> ResourceNotFoundException; same on a regional PAY_PER_REQUEST table
  (ResourceNotFoundException); same with an AAS write target already registered (ResourceNotFoundException).
  Right after UpdateTable(ReplicaUpdates Create) -> 200; polling at 5 s: [{"value":
  "UPDATING|rep:-|v:2019.11.21||ras:200:ACTIVE", "from_s": 0.26, "to_s": 5.83, "duration_s": 5.57}, {"value":
  "UPDATING|rep:CREATING|v:2019.11.21||ras:200:ACTIVE", "from_s": 5.83, "to_s": 17.09, "duration_s": 11.26},
  {"value": "ACTIVE|rep:ACTIVE|v:2019.11.21||ras:200:ACTIVE", "from_s": 17.09, "to_s": null, "duration_s":
  null}]. After removing the only replica (GlobalTableVersion <absent>, Replicas key present False) Describe
  -> ResourceNotFoundException, Update -> ResourceNotFoundException.
  - ACK: exceptions.404, terminal_codes, scope:skip · ops: DescribeTableReplicaAutoScaling,
    UpdateTableReplicaAutoScaling · fields: GlobalTableVersion, Replicas
  - repro: CreateTable (streams on, no replicas) -> DescribeTableReplicaAutoScaling (RNF) -> UpdateTable add
    replica -> poll -> remove replica -> Describe again
  - handling: not handled in the controller (as of commit 34b85e6)
  - related: [DDB-TABLE-232](../table-replicas.md#ddb-table-232), [DDB-TABLE-254](../table-replicas.md#ddb-table-254), [DDB-TABLE-231](../table-global-tables.md#ddb-table-231), [DDB-TABLE-253](../table-replicas.md#ddb-table-253), [DDB-TABLE-446](../service.md#ddb-table-446), [DDB-TABLE-306](../table-policy-kinesis-autoscaling.md#ddb-table-306),
    [DDB-TABLE-323](../table-policy-kinesis-autoscaling.md#ddb-table-323), [DDB-TABLE-226](../table-replicas.md#ddb-table-226), [DDB-TABLE-265](../table-replicas.md#ddb-table-265), [DDB-TABLE-308](../table-replicas.md#ddb-table-308), [DDB-TABLE-309](../table-replicas.md#ddb-table-309), [DDB-TABLE-329](../table-replicas.md#ddb-table-329), [DDB-TABLE-305](../table-replicas.md#ddb-table-305),
    [DDB-TABLE-249](../table-replicas.md#ddb-table-249), [DDB-TABLE-258](../table-global-tables.md#ddb-table-258) · hypotheses: H-S-040 · evidence:
    table/sub-resources/replica-autoscaling-facade

## Notes

REFUTES the single-region part of H-S-040: the API is gated on the table being a 2019.11.21 global table. The
RNF uses the same code as 'table missing' (ResourceNotFoundException, HTTP 400) and the same message template;
missing-table message: 'Global table with name: 'ackq-0ad4a5-missing' does not exist.'.
