<!-- generated: ack-api-quirks dynamodb 2026-10-09 model=2012-08-10 controller=34b85e6 -->
# DDB-TABLE-456: UpdateTable: WarmThroughput={} / OnDemandThroughput={} is silently dropped (200, no-op) when any other member is sent; alone it is HTTP 500
_Full entry and notes of one finding; its summary entry is in [service.md](../service.md). Generated from
ack-api-quirks `services/dynamodb` (model 2012-08-10, controller commit 34b85e6); edit the finding in the lab,
not here._

## Finding

- <a id="ddb-table-456"></a>**DDB-TABLE-456** `request-validation` · impact high · unhandled (not handled in controller) · verified 2026-10-09
  **UpdateTable: WarmThroughput={} / OnDemandThroughput={} is silently dropped (200, no-op) when any other member is sent; alone it is HTTP 500**
  On an idle PAY_PER_REQUEST table: UpdateTable {WarmThroughput:{}} alone -> HTTP 500 InternalFailure (known),
  but {WarmThroughput:{}, DeletionProtectionEnabled:false} -> 200, {WarmThroughput:{}, TableClass:STANDARD} ->
  200 and {OnDemandThroughput:{}, BillingMode:PAY_PER_REQUEST} -> 200 with no change to the table and WITHOUT
  the usual 'WarmThroughput must be the only operation in the request' rejection: the empty struct is treated
  as absent for composition and ignored. Combined with ProvisionedThroughput on a PPR table the PT rule fires
  first (ValidationException). At GSI level the same shapes are validated properly:
  GlobalSecondaryIndexUpdates[Update{IndexName, WarmThroughput:{}}] -> ValidationException 'One or more
  parameter values were invalid: WarmThroughput must have at least one of ReadUnitsPerSecond or
  WriteUnitsPerSecond specified for index: gsi1' (also for a ghost index and even for a MISSING table - this
  validation precedes the existence check), Update{IndexName, OnDemandThroughput:{}} and Update{IndexName}
  alone -> 'The only Updates for index: gsi1 when TableThroughputMode is PAY_PER_REQUEST can be to
  OnDemandThroughput, WarmThroughput'; GSI OnDemandThroughput {-1,-1} on a real index -> 200 (clears, like the
  table). CreateTable with GlobalSecondaryIndexes[].WarmThroughput={} or OnDemandThroughput={} -> 200, index
  created with default WarmThroughput 12000/4000 and no OnDemandThroughput.
  - ACK: custom_update, compare.nil_equals_zero_value, requeue · ops: UpdateTable, CreateTable · fields:
    WarmThroughput, OnDemandThroughput, GlobalSecondaryIndexUpdates
  - repro: UpdateTable TableName=X WarmThroughput={} DeletionProtectionEnabled=false (200) vs UpdateTable
    TableName=X WarmThroughput={} (500); UpdateTable GlobalSecondaryIndexUpdates=[{Update:{IndexName:gsi1,
    WarmThroughput:{}}}]
  - handling: not handled in the controller (as of commit 34b85e6)
  - related: [DDB-TABLE-437](../table-throughput-billing.md#ddb-table-437), [DDB-TABLE-178](../table-throughput-billing.md#ddb-table-178), [DDB-TABLE-161](../table-subresources.md#ddb-table-161), [DDB-TABLE-433](../table-streams-encryption-class.md#ddb-table-433), [DDB-TABLE-043](../table-indexes.md#ddb-table-043), [DDB-TABLE-129](../table-indexes.md#ddb-table-129),
    [DDB-TABLE-359](../table-indexes.md#ddb-table-359), [DDB-TABLE-126](../table-indexes.md#ddb-table-126), [DDB-TABLE-448](../service.md#ddb-table-448), [DDB-TABLE-458](../table-indexes.md#ddb-table-458), [DDB-TABLE-128](../table-indexes.md#ddb-table-128), [DDB-TABLE-152](../table-indexes.md#ddb-table-152), [DDB-TABLE-133](../table-indexes.md#ddb-table-133),
    [DDB-TABLE-135](../table-indexes.md#ddb-table-135), [DDB-TABLE-154](../table-throughput-billing.md#ddb-table-154), [DDB-TABLE-153](../table-indexes.md#ddb-table-153), [DDB-TABLE-375](../table-indexes.md#ddb-table-375), [DDB-TABLE-286](../table-streams-encryption-class.md#ddb-table-286) · evidence:
    table/creative/degenerate-5xx-hunt

## Notes

Extends [DDB-TABLE-437](../table-throughput-billing.md#ddb-table-437)/178 (the {} -> 500 case) and [DDB-TABLE-161](../table-subresources.md#ddb-table-161) (ghost-GSI WarmThroughput 500, reproduced
here with valid values: gsi_update_warm_ghost_valid_values -> 500 InternalFailure, while ghost + {} -> 400
ValidationException). Controller consequence: a reconciler that materialises an empty
warmThroughput/onDemandThroughput struct next to another change gets a 200 and believes the whole update
applied; the throughput part never did. Live rows:
ut_odt_empty_plus_billing -> 200 OK
ut_warm_empty_plus_tableclass -> 200 OK
ut_tableclass_plus_warm_empty_gsi -> 400 ValidationException 'One or more parameter values were invalid:
WarmThroughput must have at least one of ReadUnitsPerSecond or WriteUnitsPerSecond specified for index: gsi1'
gsi_update_name_only_real -> 400 ValidationException 'One or more parameter values were invalid: The only
Updates for index: gsi1 when TableThroughputMode is PAY_PER_REQUEST can be to OnDemandThroughput,
WarmThroughput'
gsi_update_warm_empty_real -> 400 ValidationException 'One or more parameter values were invalid:
WarmThroughput must have at least one of ReadUnitsPerSecond or WriteUnitsPerSecond specified for index: gsi1'
gsi_update_odt_empty_real -> 400 ValidationException 'One or more parameter values were invalid: The only
Updates for index: gsi1 when TableThroughputMode is PAY_PER_REQUEST can be to OnDemandThroughput,
WarmThroughput'
gsi_update_warm_neg_real -> 400 ValidationException 'One or more parameter values were invalid: Requested
ReadUnitsPerSecond for WarmThroughput for index gsi1 is lower than current WarmThroughput, decreasing
WarmThroughput is not supported'
gsi_update_odt_neg_real -> 200 OK
gsi_update_odt_empty_ghost -> 400 ResourceNotFoundException 'Requested resource not found: Index ghost for
table ackq-220c6b-d5-gsi'
gsi_update_warm_ghost_valid_values -> 500 InternalFailure ''
gsi_update_warm_empty_ghost -> 400 ValidationException 'One or more parameter values were invalid:
WarmThroughput must have at least one of ReadUnitsPerSecond or WriteUnitsPerSecond specified for index: ghost'
gsi_update_warm_empty_ghost_missing_table -> 400 ValidationException 'One or more parameter values were
invalid: WarmThroughput must have at least one of ReadUnitsPerSecond or WriteUnitsPerSecond specified for
index: ghost'
gsi_update_pt_empty_real -> 400 ValidationException '2 validation errors detected: Value null at
'globalSecondaryIndexUpdates.1.member.update.provisionedThroughput.writeCapacityUnits' failed to satisfy
constraint: Member must not be null; Value null at
'globalSecondaryIndexUpdates.1.member.update.provisionedThro'
ct_gsi_warm_empty -> 200 OK
ct_gsi_odt_empty -> 200 OK
