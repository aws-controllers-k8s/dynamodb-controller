<!-- generated: ack-api-quirks dynamodb 2026-10-09 model=2012-08-10 controller=34b85e6 -->
# DDB-TABLE-359: GlobalSecondaryIndexUpdates=[]: alone (or with AttributeDefinitions) counts as absent; with any effective change -> 'List ... is empty'
_Full entry and notes of one finding; its summary entry is in [table-indexes.md](../table-indexes.md).
Generated from ack-api-quirks `services/dynamodb` (model 2012-08-10, controller commit 34b85e6); edit the
finding in the lab, not here._

## Finding

- <a id="ddb-table-359"></a>**DDB-TABLE-359** `request-validation` · impact high · handled · verified 2026-10-09
  **GlobalSecondaryIndexUpdates=[]: alone (or with AttributeDefinitions) counts as absent; with any effective change -> 'List ... is empty'**
  UpdateTable(GlobalSecondaryIndexUpdates=[]) alone -> ValidationException (HTTP 400) 'At least one of
  ProvisionedThroughput, BillingMode, ... or TableClass is required'. With DeletionProtectionEnabled toggle ->
  ValidationException (HTTP 400) 'One or more parameter values were invalid: List of
  GlobalSecondaryIndexUpdates is empty'. With BillingMode=PAY_PER_REQUEST re-send -> ValidationException (HTTP 400)
  'One or more parameter values were invalid: List of GlobalSecondaryIndexUpdates is empty'. With
  AttributeDefinitions=[pk S] -> ValidationException (HTTP 400) 'At least one of ProvisionedThroughput,
  BillingMode, ... or TableClass is required'. With OnDemandThroughput -> ValidationException (HTTP 400) 'One
  or more parameter values were invalid: List of GlobalSecondaryIndexUpdates is empty'.
  DeletionProtectionEnabled before/after the DP-carrier call: True/True; OnDemandThroughput after the series:
  None.
  - ACK: custom_update · ops: UpdateTable · fields: GlobalSecondaryIndexUpdates
  - repro: UpdateTable(TableName, GlobalSecondaryIndexUpdates=[]); UpdateTable(TableName,
    GlobalSecondaryIndexUpdates=[], DeletionProtectionEnabled=true)
  - handling: handled via `pkg/resource/table/hooks.go:385-398; 5bbfe82`
  - related: [DDB-TABLE-015](../table-indexes.md#ddb-table-015), [DDB-TABLE-160](../table-indexes.md#ddb-table-160), [DDB-TABLE-043](../table-indexes.md#ddb-table-043), [DDB-TABLE-129](../table-indexes.md#ddb-table-129), [DDB-TABLE-456](../service.md#ddb-table-456), [DDB-TABLE-126](../table-indexes.md#ddb-table-126),
    [DDB-TABLE-162](../table-indexes.md#ddb-table-162), [DDB-TABLE-127](../table-indexes.md#ddb-table-127), [DDB-TABLE-151](../table-indexes.md#ddb-table-151), [DDB-TABLE-357](../table-indexes.md#ddb-table-357) · hypotheses: H-T-018 · evidence:
    table/mutation-matrix/schema-immutability

## Notes

Resolves [DDB-TABLE-015](../table-indexes.md#ddb-table-015) vs [DDB-TABLE-160](../table-indexes.md#ddb-table-160): both are right - the empty list is treated as absent only for the 'at
least one parameter' check; once another parameter makes the request valid, the empty list itself is rejected
and the carrier change is NOT applied. Never serialize an empty GlobalSecondaryIndexUpdates list. Confirms
H-T-018 (qualified).
