<!-- generated: ack-api-quirks dynamodb 2026-10-09 model=2012-08-10 controller=34b85e6 -->
# DDB-TABLE-375: GSI DELETING on a PPR table (~2.6 s): BillingMode=PROVISIONED (+/- dying index) -> ResourceInUse IOPS/'Index is being deleted'; ODT refused
_Full entry and notes of one finding; its summary entry is in [table-indexes.md](../table-indexes.md).
Generated from ack-api-quirks `services/dynamodb` (model 2012-08-10, controller commit 34b85e6); edit the
finding in the lab, not here._

## Finding

- <a id="ddb-table-375"></a>**DDB-TABLE-375** `async-state-machine` · impact high · handled · verified 2026-10-09
  **GSI DELETING on a PPR table (~2.6 s): BillingMode=PROVISIONED (+/- dying index) -> ResourceInUse IOPS/'Index is being deleted'; ODT refused**
  PAY_PER_REQUEST table with GSI gsi1: UpdateTable Delete gsi1 -> 200 OK (TableStatus=UPDATING,
  gsi1=DELETING). Fired right after: BillingMode=PROVISIONED + table PT 1/1 (no gsi1 entry) ->
  ResourceInUseException (HTTP 400) 'Attempt to change a resource which is still in use: Can't change table
  IOPS when an index is being deleted. Table: ackq-fbb48c-gsiracep Indexes: [gsi1]' @+0.07s [gsi1 was
  DELETING]; the same + GlobalSecondaryIndexUpdates[Update gsi1 1/1] -> ResourceInUseException (HTTP 400)
  'Attempt to change a resource which is still in use: Index is being deleted. Table: ackq-fbb48c-gsiracep
  Index: gsi1' @+0.09s [gsi1 was DELETING]; BillingMode=PAY_PER_REQUEST re-send -> 200 OK
  (TableStatus=UPDATING, gsi1=ACTIVE) @+0.12s [gsi1 was DELETING]; OnDemandThroughput change ->
  ResourceInUseException (HTTP 400) 'Attempt to change a resource which is still in use: OnDemandThroughput
  cannot be updated while index deletion is in progress for indexes: [gsi1]' @+0.15s [gsi1 was DELETING].
  Timeline (TableStatus, gsi1 IndexStatus) at 0.5 s: [(('UPDATING', 'DELETING'), 2.56), (('ACTIVE', 'GONE'),
  None)] (gsi1 DELETING for 2.6 s). Once gsi1 is gone: BillingMode=PROVISIONED + PT 1/1 -> 200 OK
  (TableStatus=UPDATING, gsi1=None) [gsi1 was GONE] (settle [(('UPDATING', 'GONE'), 87.09), (('ACTIVE',
  'GONE'), None)]); Update of the vanished gsi1 -> ResourceNotFoundException (HTTP 400) 'Requested resource
  not found: Index gsi1 for table ackq-fbb48c-gsiracep' [gsi1 was GONE].
  - ACK: updateable.when, requeue, custom_update, one-per-reconcile · ops: UpdateTable, DescribeTable ·
    fields: BillingMode, ProvisionedThroughput, GlobalSecondaryIndexUpdates
  - repro: PPR table + GSI gsi1 ACTIVE: UpdateTable(Delete gsi1); immediately
    UpdateTable(BillingMode=PROVISIONED, PT 1/1) and the same with Update gsi1; repeat after gsi1 disappears
  - measurements: t1_gsi_deleting_s=2.6
  - handling: handled via `pkg/resource/table/hooks.go:226-238; pkg/resource/table/hooks_global_secondary_indexes.go:202-217`
  - related: [DDB-TABLE-152](../table-indexes.md#ddb-table-152), [DDB-TABLE-163](../table-subresources.md#ddb-table-163), [DDB-TABLE-149](../table-indexes.md#ddb-table-149), [DDB-TABLE-376](../table-indexes.md#ddb-table-376), [DDB-TABLE-462](../table-indexes.md#ddb-table-462), [DDB-TABLE-153](../table-indexes.md#ddb-table-153),
    [DDB-TABLE-164](../table-indexes.md#ddb-table-164), [DDB-TABLE-128](../table-indexes.md#ddb-table-128), [DDB-TABLE-135](../table-indexes.md#ddb-table-135), [DDB-TABLE-154](../table-throughput-billing.md#ddb-table-154), [DDB-TABLE-456](../service.md#ddb-table-456), [DDB-TABLE-458](../table-indexes.md#ddb-table-458), [DDB-TABLE-286](../table-streams-encryption-class.md#ddb-table-286),
    [DDB-TABLE-155](../table-indexes.md#ddb-table-155), [DDB-TABLE-165](../table-indexes.md#ddb-table-165), [DDB-TABLE-361](../table-streams-encryption-class.md#ddb-table-361) · hypotheses: H-T-023 · evidence:
    table/state-machine/gsi-delete-billing-race

## Notes

Qualifies H-T-023 / [DDB-TABLE-152](../table-indexes.md#ddb-table-152): while the index is still listed as DELETING the state check wins over the
per-index throughput validation - the controller gets ResourceInUseException ("Can't change table IOPS when an
index is being deleted ... Indexes: [gsi1]"), never 'ProvisionedThroughput must be specified for index';
naming the dying index in a GSI Update changes the message to 'Index is being deleted'. Once the entry is gone
the switch WITHOUT the index is clean (200, first switch of this table's life UPDATING 87 s) and an Update for
the vanished index is ResourceNotFoundException 'Index gsi1 for table X'. STALE RESPONSE: the
BillingMode=PAY_PER_REQUEST re-send accepted during the deletion echoed gsi1 with IndexStatus=ACTIVE although
DescribeTable said DELETING. A controller that removes a GSI and flips billing in consecutive reconciles must
wait for the index entry to disappear, not for TableStatus=ACTIVE alone.
