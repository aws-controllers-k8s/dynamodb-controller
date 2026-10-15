<!-- generated: ack-api-quirks dynamodb 2026-10-09 model=2012-08-10 controller=34b85e6 -->
# DDB-TABLE-095: ListContributorInsights omits DISABLED entries (fresh table -> empty), pages table first then GSIs, last page has no NextToken
_Full entry and notes of one finding; its summary entry is in
[table-subresources.md](../table-subresources.md). Generated from ack-api-quirks `services/dynamodb` (model
2012-08-10, controller commit 34b85e6); edit the finding in the lab, not here._

## Finding

- <a id="ddb-table-095"></a>**DDB-TABLE-095** `response-fidelity` · impact medium · unhandled (not handled in controller) · verified 2026-10-08
  **ListContributorInsights omits DISABLED entries (fresh table -> empty), pages table first then GSIs, last page has no NextToken**
  Fresh table: List(TableName) -> 200 with ContributorInsightsSummaries=[] (never-configured table is not
  listed). With table+GSI ENABLED, MaxResults=1: page 1 = [{TableName, ContributorInsightsStatus: ENABLED,
  ContributorInsightsMode}] + NextToken; page 2 = [{TableName, IndexName: gsi1, ...}] and no NextToken (2
  pages, no empty trailing page). After DISABLE on the table (GSI still ENABLED): List returns only the GSI
  entry - the DISABLED table entry disappears. A DISABLING entry is still listed. Account-wide List() with no
  TableName (MaxResults=100): 3 summaries, included the other probe table. Each summary carries
  ContributorInsightsMode while ENABLING/ENABLED/DISABLING.
  - ACK: custom_find, list_operation.match_fields · ops: ListContributorInsights · fields:
    ContributorInsightsSummaries, NextToken, MaxResults
  - repro: ListContributorInsights(TableName=<table with gsi>, MaxResults=1) loop
  - handling: not handled in the controller (as of commit 34b85e6)
  - related: [DDB-TABLE-094](../table-subresources.md#ddb-table-094), [DDB-TABLE-344](../table-subresources.md#ddb-table-344), [DDB-TABLE-093](../table-subresources.md#ddb-table-093), [DDB-TABLE-112](../table-subresources.md#ddb-table-112), [DDB-TABLE-116](../table-subresources.md#ddb-table-116) · evidence:
    table/sub-resources/insights-lifecycle

## Notes

Hypotheses: H-S-125. H-S-125 largely confirmed; the key addition is that List cannot be used to discover
DISABLED/never-configured tables or indexes - Describe per table/index is the only way.

Contradiction with [DDB-TABLE-344](../table-subresources.md#ddb-table-344): 344 states 'List(TableName=x) is always one page without NextToken'; 095
observed List(TableName, MaxResults=1) with table+GSI ENABLED return 2 pages joined by a NextToken (table
first, then the GSI, no empty trailing page) Resolution: keep both; 095 is canonical for per-table paging
(pages whenever enabled entries > MaxResults, never an empty page), 344 is canonical for the account-wide
empty-page behavior; 344's 'always one page' clause holds only when <= MaxResults entries are enabled
