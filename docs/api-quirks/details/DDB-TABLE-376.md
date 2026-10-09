<!-- generated: ack-api-quirks dynamodb 2026-10-09 model=2012-08-10 controller=34b85e6 -->
# DDB-TABLE-376: GSI DELETING on a PROVISIONED table (~4.6 s): billing switch / table PT -> ResourceInUse (IOPS); DeleteTable refused; DP admitted
_Full entry and notes of one finding; its summary entry is in [table-indexes.md](../table-indexes.md).
Generated from ack-api-quirks `services/dynamodb` (model 2012-08-10, controller commit 34b85e6); edit the
finding in the lab, not here._

## Finding

- <a id="ddb-table-376"></a>**DDB-TABLE-376** `async-state-machine` · impact high · handled · verified 2026-10-09
  **GSI DELETING on a PROVISIONED table (~4.6 s): billing switch / table PT -> ResourceInUse (IOPS); DeleteTable refused; DP admitted**
  PROVISIONED 1/1 table with GSI gsi1 1/1: UpdateTable Delete gsi1 -> 200 OK (TableStatus=UPDATING,
  gsi1=DELETING). Fired right after: BillingMode=PAY_PER_REQUEST -> ResourceInUseException (HTTP 400) 'Attempt
  to change a resource which is still in use: Can't change table IOPS when an index is being deleted. Table:
  ackq-fbb48c-gsiracev Indexes: [gsi1]' @+0.06s [gsi1 was DELETING]; table PT 2/2 -> ResourceInUseException
  (HTTP 400) 'Attempt to change a resource which is still in use: Can't change table IOPS when an index is
  being deleted. Table: ackq-fbb48c-gsiracev Indexes: [gsi1]' @+0.09s [gsi1 was DELETING];
  BillingMode=PROVISIONED + PT 2/2 (no gsi1 entry) -> ResourceInUseException (HTTP 400) 'Attempt to change a
  resource which is still in use: Can't change table IOPS when an index is being deleted. Table:
  ackq-fbb48c-gsiracev Indexes: [gsi1]' @+0.11s [gsi1 was DELETING]; PT 2/2 + Update gsi1 2/2 ->
  ResourceInUseException (HTTP 400) 'Attempt to change a resource which is still in use: Index is being
  deleted. Table: ackq-fbb48c-gsiracev Index: gsi1' @+0.14s [gsi1 was DELETING]; DeleteTable ->
  ResourceInUseException (HTTP 400) 'Attempt to change a resource which is still in use: Cannot delete table
  while indexes are being created, updated, or deleted.' @+0.16s [gsi1 was DELETING];
  DeletionProtectionEnabled=true -> 200 OK (TableStatus=UPDATING, gsi1=DELETING) @+0.19s [gsi1 was DELETING].
  Timeline (TableStatus, gsi1 IndexStatus) at 0.5 s: [(('UPDATING', 'DELETING'), 4.62), (('ACTIVE', 'GONE'),
  None)]. Once gsi1 is gone: PT 2/2 -> 200 OK (TableStatus=UPDATING, gsi1=None) [gsi1 was GONE] (settle
  [(('UPDATING', 'GONE'), 1.01), (('ACTIVE', 'GONE'), None)]); BillingMode=PAY_PER_REQUEST -> 200 OK
  (TableStatus=UPDATING, gsi1=None) [gsi1 was GONE] (settle [(('UPDATING', 'GONE'), 107.36), (('ACTIVE',
  'GONE'), None)]).
  - ACK: updateable.when, deletable.when, requeue, one-per-reconcile · ops: UpdateTable, DeleteTable,
    DescribeTable · fields: BillingMode, ProvisionedThroughput, GlobalSecondaryIndexUpdates,
    DeletionProtectionEnabled
  - repro: PROVISIONED table + GSI gsi1: UpdateTable(Delete gsi1); immediately
    UpdateTable(BillingMode=PAY_PER_REQUEST) / PT 2/2 / DeleteTable / DP=true; repeat after gsi1 disappears
  - measurements: t2_gsi_deleting_s=4.6
  - handling: handled via `pkg/resource/table/hooks.go:226-238; pkg/resource/table/hooks_global_secondary_indexes.go:202-217`
  - related: [DDB-TABLE-163](../table-subresources.md#ddb-table-163), [DDB-TABLE-150](../table-indexes.md#ddb-table-150), [DDB-TABLE-152](../table-indexes.md#ddb-table-152), [DDB-TABLE-159](../table-indexes.md#ddb-table-159), [DDB-TABLE-135](../table-indexes.md#ddb-table-135), [DDB-TABLE-375](../table-indexes.md#ddb-table-375),
    [DDB-TABLE-462](../table-indexes.md#ddb-table-462), [DDB-TABLE-166](../table-indexes.md#ddb-table-166), [DDB-TABLE-458](../table-indexes.md#ddb-table-458), [DDB-TABLE-286](../table-streams-encryption-class.md#ddb-table-286), [DDB-TABLE-153](../table-indexes.md#ddb-table-153), [DDB-TABLE-164](../table-indexes.md#ddb-table-164), [DDB-TABLE-128](../table-indexes.md#ddb-table-128) ·
    hypotheses: H-T-023 · evidence: table/state-machine/gsi-delete-billing-race

## Notes

A GSI delete occupies the table for the whole IndexStatus=DELETING window (TableStatus=UPDATING for the same
4.6 s here). Table-level IOPS changes (PT, billing switch) and DeleteTable are ResourceInUseException with
index-specific messages; DeletionProtectionEnabled is admitted. Right after the entry vanished the PT change
(UPDATING 1 s) and the PROVISIONED->PAY_PER_REQUEST switch (UPDATING 107 s, this table's first switch) were
accepted. Related: [DDB-TABLE-150](../table-indexes.md#ddb-table-150) (DeleteTable refused while any GSI is in flight).
