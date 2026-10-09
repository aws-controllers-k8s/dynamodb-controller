<!-- generated: ack-api-quirks dynamodb 2026-10-09 model=2012-08-10 controller=34b85e6 -->
# DDB-TABLE-161: Unknown GSI in UpdateTable is ResourceNotFoundException with an 'Index' message; WarmThroughput on a ghost index returns HTTP 500
_Full entry and notes of one finding; its summary entry is in
[table-subresources.md](../table-subresources.md). Generated from ack-api-quirks `services/dynamodb` (model
2012-08-10, controller commit 34b85e6); edit the finding in the lab, not here._

## Finding

- <a id="ddb-table-161"></a>**DDB-TABLE-161** `error-code` · impact high · handled · verified 2026-10-08
  **Unknown GSI in UpdateTable is ResourceNotFoundException with an 'Index' message; WarmThroughput on a ghost index returns HTTP 500**
  GlobalSecondaryIndexUpdates[Delete {IndexName: ghost}] and [Update {IndexName: ghost,
  ProvisionedThroughput}] -> ResourceNotFoundException 'Requested resource not found: Index ghost for table
  ackq-...-gsi-gran' (also when mixed with valid Update entries, which are then not applied). A missing table
  gives 'Requested resource not found: Table: X not found' - the word after 'found:' discriminates. [Update
  {IndexName: ghost, WarmThroughput {12001,4001}}] -> HTTP 500 InternalFailure with no message (1.7 s).
  UpdateContributorInsights/DescribeContributorInsights IndexName=ghost -> ResourceNotFoundException
  'Requested resource not found: Index: ghost not found for table: X'. Query IndexName=ghost ->
  ValidationException 'The table does not have the specified index: ghost'. IndexNotFoundException was never
  returned.
  - ACK: exceptions.404, terminal_codes · ops: UpdateTable, UpdateContributorInsights,
    DescribeContributorInsights, Query · fields: GlobalSecondaryIndexUpdates.Delete.IndexName,
    GlobalSecondaryIndexUpdates.Update.IndexName
  - repro: UpdateTable GlobalSecondaryIndexUpdates=[{Update:{IndexName:ghost,
    WarmThroughput:{ReadUnitsPerSecond:12001,WriteUnitsPerSecond:4001}}}]
  - handling: handled via `generator.yaml:84-87; pkg/resource/table/sdk.go:83-86`
  - related: [DDB-TABLE-448](../service.md#ddb-table-448), [DDB-TABLE-456](../service.md#ddb-table-456), [DDB-TABLE-458](../table-indexes.md#ddb-table-458), [DDB-TABLE-380](../table-indexes.md#ddb-table-380), [DDB-TABLE-094](../table-subresources.md#ddb-table-094), [DDB-TABLE-137](../table-subresources.md#ddb-table-137),
    [DDB-TABLE-116](../table-subresources.md#ddb-table-116) · evidence: table/mutation-matrix/gsi-update-granularity

## Notes

Confirms H-T-125 (IndexNotFoundException is dead); partially refutes H-T-055 (Update on a ghost index is also
ResourceNotFoundException, not ValidationException). A controller mapping ResourceNotFoundException to 'table
gone' must inspect the message. The 500 for WarmThroughput on an unknown index looks like a service bug
(suspect-bug).
