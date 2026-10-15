<!-- generated: ack-api-quirks dynamodb 2026-10-09 model=2012-08-10 controller=34b85e6 -->
# DDB-TABLE-263: DeletionProtection on an MRSC member: table UPDATING ~38 s, not propagated to the replica, re-toggle throttled for 15 s
_Full entry and notes of one finding; its summary entry is in
[table-global-tables.md](../table-global-tables.md). Generated from ack-api-quirks `services/dynamodb` (model
2012-08-10, controller commit 34b85e6); edit the finding in the lab, not here._

## Finding

- <a id="ddb-table-263"></a>**DDB-TABLE-263** `cross-region` · impact medium · unhandled (not handled in controller) · verified 2026-10-09
  **DeletionProtection on an MRSC member: table UPDATING ~38 s, not propagated to the replica, re-toggle throttled for 15 s**
  UpdateTable DeletionProtectionEnabled=true on the us-west-2 member of an ACTIVE MRSC group returned
  TableStatus=UPDATING; DescribeTable stayed UPDATING for 38.5 s (replica and witness ACTIVE throughout). The
  replica in us-east-1 still reported DeletionProtectionEnabled=false afterwards (setting is per region). An
  immediate DeletionProtectionEnabled=false -> ResourceInUseException; after the table was ACTIVE again ->
  ThrottlingException "Deletion protection setting for table <name> modified within the previous 15000
  milliseconds. Please try again after <ts>" (HTTP 400). While the table was UPDATING the MRSC dissolve call
  also got ResourceInUseException.
  - ACK: requeue, e2e-timing, synced.when · ops: UpdateTable, DescribeTable · fields:
    DeletionProtectionEnabled
  - repro: MRSC group ACTIVE -> UpdateTable DeletionProtectionEnabled=true -> poll DescribeTable; UpdateTable
    DeletionProtectionEnabled=false
  - measurements: updating_after_deletion_protection_toggle_s=38.47,
    deletion_protection_retoggle_throttle_s=15
  - handling: not handled in the controller (as of commit 34b85e6)
  - related: [DDB-TABLE-306](../table-policy-kinesis-autoscaling.md#ddb-table-306), [DDB-TABLE-200](../table-replicas.md#ddb-table-200), [DDB-TABLE-296](../table-replicas.md#ddb-table-296), [DDB-TABLE-315](../table-policy-kinesis-autoscaling.md#ddb-table-315), [DDB-TABLE-327](../table-replicas.md#ddb-table-327), [DDB-TABLE-314](../table-policy-kinesis-autoscaling.md#ddb-table-314),
    [DDB-TABLE-297](../table-replicas.md#ddb-table-297), [DDB-TABLE-329](../table-replicas.md#ddb-table-329), [DDB-TABLE-311](../table-replicas.md#ddb-table-311), [DDB-TABLE-257](../table-global-tables.md#ddb-table-257), [DDB-TABLE-258](../table-global-tables.md#ddb-table-258), [DDB-TABLE-259](../table-global-tables.md#ddb-table-259), [DDB-TABLE-260](../table-global-tables.md#ddb-table-260),
    [DDB-TABLE-261](../table-global-tables.md#ddb-table-261), [DDB-TABLE-264](../table-global-tables.md#ddb-table-264), [DDB-TABLE-202](../table-global-tables.md#ddb-table-202) · hypotheses: H-R-020 · evidence:
    table/cross-region/mrsc-witness

## Notes

On a regional table deletion protection flips without an UPDATING phase in other probes; on a global table
member it is an asynchronous update that serialises with replica operations.

Contradiction with [DDB-TABLE-329](../table-replicas.md#ddb-table-329), [DDB-TABLE-327](../table-replicas.md#ddb-table-327), [DDB-TABLE-297](../table-replicas.md#ddb-table-297): 329 says the source's
DeletionProtectionEnabled=true 'propagated to the replica' and 327's note repeats it; 263 (MRSC,
base->replica) and 297 (EVENTUAL, replica->base) observe no propagation Resolution: 263/297 are right - DP is
per region. 329's own evidence.jsonl shows us-east-1 DeletionProtectionEnabled=false at +18 s, +80 s and +140
s after the source DP=true (02:06:06), and the replica-region DP=true call got ResourceInUseException; 327 set
DP in the replica region itself. 329's sentence and 327's note are wrong
