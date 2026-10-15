<!-- generated: ack-api-quirks dynamodb 2026-10-09 model=2012-08-10 controller=34b85e6 -->
# DDB-TABLE-094: Contributor Insights is per table and per GSI; LSI, unknown index, missing table (incl. List) -> ResourceNotFoundException
_Full entry and notes of one finding; its summary entry is in
[table-subresources.md](../table-subresources.md). Generated from ack-api-quirks `services/dynamodb` (model
2012-08-10, controller commit 34b85e6); edit the finding in the lab, not here._

## Finding

- <a id="ddb-table-094"></a>**DDB-TABLE-094** `scope` · impact high · unhandled (not handled in controller) · verified 2026-10-08
  **Contributor Insights is per table and per GSI; LSI, unknown index, missing table (incl. List) -> ResourceNotFoundException**
  **Scope verdict: implement**
  Table ENABLE and GSI ENABLE back-to-back: 200 OK status=ENABLING / 200 OK status=ENABLING (both transition
  independently: table [{'value': 'ENABLING', 'from_s': 0.01, 'to_s': 1.03, 'duration_s': 1.02}, {'value':
  'ENABLED', 'from_s': 1.03, 'to_s': None, 'duration_s': None}], gsi [{'value': 'ENABLING', 'from_s': 0.01,
  'to_s': 1.03, 'duration_s': 1.02}, {'value': 'ENABLED', 'from_s': 1.03, 'to_s': None, 'duration_s': None}]).
  Describe(IndexName=LSI) -> ResourceNotFoundException (HTTP 400) 'Requested resource not found: Index: lsi1
  not found for table: ackq-244c78-i2'; Update(IndexName=LSI) -> ResourceNotFoundException (HTTP 400)
  'Requested resource not found: Index: lsi1 not found for table: ackq-244c78-i2'; Describe(bogus index) ->
  ResourceNotFoundException (HTTP 400) 'Requested resource not found: Index: nope not found for table:
  ackq-244c78-i2'; Update(bogus index) -> ResourceNotFoundException (HTTP 400) 'Requested resource not found:
  Index: nope not found for table: ackq-244c78-i2'; List(TableName=missing) -> ResourceNotFoundException.
  While the GSI table was CREATING: Describe(gsi) -> ResourceNotFoundException (HTTP 400) 'Requested resource
  not found: Table: ackq-244c78-i2 not found', Update(gsi) -> ResourceNotFoundException (HTTP 400) 'Requested
  resource not found: Table: ackq-244c78-i2 not found', Describe(table) -> ResourceNotFoundException (HTTP 400)
  'Requested resource not found: Table: ackq-244c78-i2 not found'. List(TableName) right after enabling:
  {'ok': True, 'NextToken': None, 'summaries': [{'TableName': 'ackq-244c78-i2', 'ContributorInsightsStatus':
  'ENABLING', 'ContributorInsightsMode': 'ACCESSED_AND_THROTTLED_KEYS'}, {'TableName': 'ackq-244c78-i2',
  'IndexName': 'gsi1', 'ContributorInsightsStatus': 'ENABLING', 'ContributorInsightsMode':
  'ACCESSED_AND_THROTTLED_KEYS'}]}.
  - ACK: custom_update, exceptions.404, terminal_codes · ops: UpdateContributorInsights,
    DescribeContributorInsights, ListContributorInsights · fields: IndexName
  - repro: table with GSI+LSI: ENABLE table, ENABLE gsi, ENABLE lsi, ENABLE bogus; List
  - handling: not handled in the controller (as of commit 34b85e6)
  - related: [DDB-TABLE-161](../table-subresources.md#ddb-table-161), [DDB-TABLE-380](../table-indexes.md#ddb-table-380), [DDB-TABLE-137](../table-subresources.md#ddb-table-137), [DDB-TABLE-116](../table-subresources.md#ddb-table-116), [DDB-TABLE-357](../table-indexes.md#ddb-table-357), [DDB-TABLE-133](../table-indexes.md#ddb-table-133),
    [DDB-TABLE-095](../table-subresources.md#ddb-table-095), [DDB-TABLE-344](../table-subresources.md#ddb-table-344), [DDB-TABLE-093](../table-subresources.md#ddb-table-093), [DDB-TABLE-112](../table-subresources.md#ddb-table-112) · evidence:
    table/sub-resources/insights-lifecycle

## Notes

Hypotheses: H-S-023, H-S-025, H-S-128. H-S-023 confirmed (LSI and bogus index -> ResourceNotFoundException
'Index: <name> not found for table: <t>'; List on a missing table -> ResourceNotFoundException, not an empty
page). H-S-025: while the table was CREATING every insights call (table-level and index-level, incl. List)
returned ResourceNotFoundException 'Requested resource not found: Table: <t> not found' - byte-identical to
the message for a deleted table. H-S-128 (small scale): table + GSI ENABLE fired back-to-back were both
accepted and transitioned independently.
