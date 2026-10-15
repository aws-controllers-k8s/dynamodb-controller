<!-- generated: ack-api-quirks dynamodb 2026-10-09 model=2012-08-10 controller=34b85e6 -->
# DDB-TABLE-344: Account-wide ListContributorInsights(MaxResults=N) returns EMPTY pages with a NextToken (42 pages for 9 live table/GSI entries at N=1)
_Full entry and notes of one finding; its summary entry is in
[table-subresources.md](../table-subresources.md). Generated from ack-api-quirks `services/dynamodb` (model
2012-08-10, controller commit 34b85e6); edit the finding in the lab, not here._

## Finding

- <a id="ddb-table-344"></a>**DDB-TABLE-344** `response-fidelity` · impact medium · unhandled (not handled in controller) · verified 2026-10-09
  **Account-wide ListContributorInsights(MaxResults=N) returns EMPTY pages with a NextToken (42 pages for 9 live table/GSI entries at N=1)**
  ListContributorInsights without TableName iterates over an internal candidate set and emits a page per
  candidate slice even when every candidate is DISABLED: with 6 tables and 3 GSIs in the region and nothing
  enabled, MaxResults=1 -> 42 pages (all empty, each but the last with a NextToken), MaxResults=2 -> 21, 5 ->
  9, 100 -> 1 page, default -> 1 page. With 3 tables ENABLED, MaxResults=1 gave 41 pages with sizes [0, 0, 0,
  0, 0, 1, 0, 1, 0, 0]... (3 non-empty). The last page never carried a NextToken; List(TableName=x) is always
  one page without NextToken. The candidate count (42) exceeded the live tables+GSIs (9), i.e. it is not
  simply 'all tables'.
  - ACK: custom_find, list_operation.match_fields · ops: ListContributorInsights · fields: MaxResults,
    NextToken, ContributorInsightsSummaries
  - repro: ListContributorInsights(MaxResults=1) and follow NextToken until absent; count pages vs ListTables
  - measurements: pages_max_results_1=42, tables_in_region=6, gsis_in_region=3
  - handling: not handled in the controller (as of commit 34b85e6)
  - related: [DDB-TABLE-089](../table-subresources.md#ddb-table-089), [DDB-TABLE-094](../table-subresources.md#ddb-table-094), [DDB-TABLE-095](../table-subresources.md#ddb-table-095), [DDB-TABLE-096](../table-subresources.md#ddb-table-096), [DDB-TABLE-112](../table-subresources.md#ddb-table-112), [DDB-TABLE-340](../table-subresources.md#ddb-table-340),
    [DDB-TABLE-093](../table-subresources.md#ddb-table-093), [DDB-TABLE-116](../table-subresources.md#ddb-table-116) · hypotheses: H-S-125 · evidence: table/sub-resources/insights-list-ghosts

## Notes

REFUTES the H-S-125 clause 'never returns a NextToken that leads to an empty page'. A client that stops at the
first empty page (a common shortcut) silently misses entries; always loop until NextToken is absent, or use
MaxResults=100 (single page here). Status=DISABLED is never emitted, so the empty pages are the only trace of
DISABLED candidates.

Contradiction with [DDB-TABLE-095](../table-subresources.md#ddb-table-095): 344 states 'List(TableName=x) is always one page without NextToken'; 095
observed List(TableName, MaxResults=1) with table+GSI ENABLED return 2 pages joined by a NextToken (table
first, then the GSI, no empty trailing page) Resolution: keep both; 095 is canonical for per-table paging
(pages whenever enabled entries > MaxResults, never an empty page), 344 is canonical for the account-wide
empty-page behavior; 344's 'always one page' clause holds only when <= MaxResults entries are enabled
