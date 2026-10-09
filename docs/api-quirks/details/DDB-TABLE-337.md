<!-- generated: ack-api-quirks dynamodb 2026-10-09 model=2012-08-10 controller=34b85e6 -->
# DDB-TABLE-337: Insights ENABLE past the CW rule quota (100): 200 ENABLING, then FAILED in ~2s, FailureException LimitExceededException; no partial rules
_Full entry and notes of one finding; its summary entry is in
[table-subresources.md](../table-subresources.md). Generated from ack-api-quirks `services/dynamodb` (model
2012-08-10, controller commit 34b85e6); edit the finding in the lab, not here._

## Finding

- <a id="ddb-table-337"></a>**DDB-TABLE-337** `async-state-machine` · impact high · unhandled (not handled in controller) · verified 2026-10-09
  **Insights ENABLE past the CW rule quota (100): 200 ENABLING, then FAILED in ~2s, FailureException LimitExceededException; no partial rules**
  Account quota 'Number of Contributor Insights rules' (service-quotas monitoring/L-DBD11BCC) = 100.0. Each
  ENABLE on a hash-only table created 2 CloudWatch rules, each hash+range table 4 (deltas [(2, 2), (4, 4)]),
  all settling ENABLING->ENABLED in 1.05-3.07 s. At 98/100 rules, ENABLE on a 4-rule table was still accepted
  synchronously (200, ContributorInsightsStatus=ENABLING, mode echoed) but DescribeContributorInsights showed
  FAILED 2.02s later with FailureException={ExceptionName: 'LimitExceededException', ExceptionDescription:
  'Amazon CloudWatch Contributor Insights rule limit reached. Please disable Contributor Insights for other
  tables/indexes OR disable other CloudWatch Contributor Insights rules before retrying.'}; the response has
  no ContributorInsightsRuleList key and keeps ContributorInsightsMode. Rule creation is atomic: the
  CloudWatch count stayed 98 (no rule named after the table) and a 2-rule table enabled right afterwards went
  ENABLED (98->100), so a FAILED attempt consumes no quota. FAILED stayed FAILED for 20 s.
  - ACK: terminal_codes, synced.when, requeue · ops: UpdateContributorInsights, DescribeContributorInsights ·
    fields: ContributorInsightsStatus, FailureException, ContributorInsightsRuleList
  - repro: 25 hash+range PPR tables: UpdateContributorInsights(ENABLE) each -> 98 rules (1 hash-only + 24);
    ENABLE a 26th hash+range table; DescribeContributorInsights at 1/s
  - measurements: quota=100.0, rules_before_failed=98, enabling_s_before_failed=2.02,
    enable_settle_s_min=1.05, enable_settle_s_max=3.07, tables_enabled=25, create_31_tables_all_active_s=8.8
  - handling: not handled in the controller (as of commit 34b85e6)
  - related: [DDB-TABLE-089](../table-subresources.md#ddb-table-089), [DDB-TABLE-090](../table-subresources.md#ddb-table-090), [DDB-TABLE-092](../table-streams-encryption-class.md#ddb-table-092), [DDB-TABLE-093](../table-subresources.md#ddb-table-093), [DDB-TABLE-095](../table-subresources.md#ddb-table-095), [DDB-TABLE-097](../table-subresources.md#ddb-table-097),
    [DDB-TABLE-439](../table-subresources.md#ddb-table-439), [DDB-TABLE-338](../table-subresources.md#ddb-table-338), [DDB-TABLE-339](../table-subresources.md#ddb-table-339), [DDB-TABLE-340](../table-subresources.md#ddb-table-340), [DDB-TABLE-341](../table-subresources.md#ddb-table-341), [DDB-TABLE-342](../table-subresources.md#ddb-table-342) · hypotheses:
    H-S-127, H-S-022 · evidence: table/limits/insights-rule-quota

## Notes

H-S-127 LimitExceededException clause CONFIRMED (ExceptionName is the CloudWatch error name, description is a
DynamoDB-authored sentence, not the raw CloudWatch message). The quota is account-wide across tables and GSIs:
25 sort-keyed tables (or ~50 hash-only ones) exhaust it; the sync response cannot tell success from failure -
a controller must poll Describe and treat FAILED + LimitExceededException as a user-fixable terminal
condition.
