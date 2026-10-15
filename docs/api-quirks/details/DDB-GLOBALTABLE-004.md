<!-- generated: ack-api-quirks dynamodb 2026-10-09 model=2012-08-10 controller=34b85e6 -->
# DDB-GLOBALTABLE-004: Legacy GlobalTable read/update APIs still answer - GlobalTableNotFoundException (HTTP 400) is the not-found signal; List returns []
_Full entry and notes of one finding; its summary entry is in
[table-global-tables.md](../table-global-tables.md). Generated from ack-api-quirks `services/dynamodb` (model
2012-08-10, controller commit 34b85e6); edit the finding in the lab, not here._

## Finding

- <a id="ddb-globaltable-004"></a>**DDB-GLOBALTABLE-004** `error-code` · impact medium · SUSPECTED CONTROLLER BUG · verified 2026-10-09
  **Legacy GlobalTable read/update APIs still answer - GlobalTableNotFoundException (HTTP 400) is the not-found signal; List returns []**
  For a plain regional table name and for a missing name alike: DescribeGlobalTable ->
  GlobalTableNotFoundException "Global table not found: Global table with name: '<name>' does not exist.";
  DescribeGlobalTableSettings, UpdateGlobalTableSettings (GlobalTableBillingMode or ReplicaSettingsUpdate) ->
  GlobalTableNotFoundException "Global table with name: '<name>' does not exist."; UpdateGlobalTable Delete ->
  GlobalTableNotFoundException; UpdateGlobalTable Create -> ValidationException "version 2017.11.29 is not
  supported" (checked before existence); UpdateGlobalTable ReplicaUpdates=[] -> ValidationException "One or
  more parameter values were invalid" (no detail). All are HTTP 400. ListGlobalTables returns 200 with
  GlobalTables=[] (no LastEvaluatedGlobalTableName key) for default, RegionName=us-east-1 and Limit=1;
  ListGlobalTables RegionName=us-fake-9 -> HTTP 500 InternalServerError "Internal server error".
  - ACK: exceptions.404, custom_find, docs-only · ops: DescribeGlobalTable, DescribeGlobalTableSettings,
    UpdateGlobalTable, UpdateGlobalTableSettings, ListGlobalTables
  - repro: Call each legacy GlobalTable API with GlobalTableName=<regional table> and =<missing name>;
    ListGlobalTables RegionName=us-fake-9
  - handling: suspected controller bug - see Handling gaps
  - related: [DDB-GLOBALTABLE-001](../table-global-tables.md#ddb-globaltable-001), [DDB-GLOBALTABLESETTINGS-001](../table-global-tables.md#ddb-globaltablesettings-001), [DDB-GLOBALTABLE-002](../table-global-tables.md#ddb-globaltable-002), [DDB-GLOBALTABLE-003](../table-global-tables.md#ddb-globaltable-003) ·
    hypotheses: H-R-049, H-R-045, H-R-040 · evidence: globaltable/round-trip/legacy-create

## Notes

A bogus RegionName on ListGlobalTables yields a 5xx (retryable-looking) rather than a ValidationException - a
controller List would retry forever on a typo. GlobalTableNotFoundException would be the exceptions.404 code
if the resource were ever implemented.

Suspected controller bug confirmed by evidence: UpdateGlobalTable with ReplicaUpdates=[] is rejected
server-side with ValidationException 'One or more parameter values were invalid' (the Go SDK would reject a
nil required member even earlier), so a ReplicaUpdates-less generated update can never succeed. Practically
moot: CreateGlobalTable is rejected everywhere ([DDB-GLOBALTABLE-002](../table-global-tables.md#ddb-globaltable-002)), so no GlobalTable CR reaches the update
path.
