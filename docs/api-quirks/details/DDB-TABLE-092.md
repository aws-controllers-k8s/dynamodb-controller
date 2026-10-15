<!-- generated: ack-api-quirks dynamodb 2026-10-09 model=2012-08-10 controller=34b85e6 -->
# DDB-TABLE-092: ContributorInsightsMode changes via ENABLE (re-ENABLING); omitting mode reverts to ACCESSED_AND_THROTTLED_KEYS; DISABLE with mode accepted
_Full entry and notes of one finding; its summary entry is in
[table-streams-encryption-class.md](../table-streams-encryption-class.md). Generated from ack-api-quirks
`services/dynamodb` (model 2012-08-10, controller commit 34b85e6); edit the finding in the lab, not here._

## Finding

- <a id="ddb-table-092"></a>**DDB-TABLE-092** `update-granularity` · impact high · unhandled (not handled in controller) · verified 2026-10-08
  **ContributorInsightsMode changes via ENABLE (re-ENABLING); omitting mode reverts to ACCESSED_AND_THROTTLED_KEYS; DISABLE with mode accepted**
  Initial ENABLE (no mode) -> mode ACCESSED_AND_THROTTLED_KEYS, rules
  ['DynamoDBContributorInsights-PKC-ackq-244c78-i1-1791500895719',
  'DynamoDBContributorInsights-PKT-ackq-244c78-i1-1791500895719']. ENABLE mode=THROTTLED_KEYS on ENABLED table
  -> 200 OK status=ENABLING, transition [{'value': 'ENABLING', 'from_s': 0.01, 'to_s': 1.02, 'duration_s':
  1.01}, {'value': 'ENABLED', 'from_s': 1.02, 'to_s': None, 'duration_s': None}], rules after
  ['DynamoDBContributorInsights-PKT-ackq-244c78-i1-1791500895719']. ENABLE (no mode) afterwards -> 200 OK
  status=ENABLING, Describe mode=ACCESSED_AND_THROTTLED_KEYS, transition [{'value': 'ENABLING', 'from_s':
  0.01, 'to_s': 1.03, 'duration_s': 1.02}, {'value': 'ENABLED', 'from_s': 1.03, 'to_s': None, 'duration_s':
  None}]. ENABLE mode=ACCESSED_AND_THROTTLED_KEYS -> 200 OK status=ENABLED, transition [{'value': 'ENABLED',
  'from_s': 0.01, 'to_s': None, 'duration_s': None}]. DISABLE with mode=THROTTLED_KEYS -> 200 OK
  status=DISABLING.
  - ACK: custom_update, late_initialize, compare.is_ignored+delta_pre_compare · ops:
    UpdateContributorInsights, DescribeContributorInsights · fields: ContributorInsightsMode,
    ContributorInsightsRuleList
  - repro: ENABLE; ENABLE mode=THROTTLED_KEYS; ENABLE; ENABLE mode=ACCESSED_AND_THROTTLED_KEYS; DISABLE
    mode=THROTTLED_KEYS
  - handling: not handled in the controller (as of commit 34b85e6)
  - related: [DDB-TABLE-089](../table-subresources.md#ddb-table-089), [DDB-TABLE-090](../table-subresources.md#ddb-table-090), [DDB-TABLE-091](../table-subresources.md#ddb-table-091), [DDB-TABLE-341](../table-subresources.md#ddb-table-341), [DDB-TABLE-339](../table-subresources.md#ddb-table-339), [DDB-TABLE-343](../table-subresources.md#ddb-table-343),
    [DDB-TABLE-340](../table-subresources.md#ddb-table-340) · evidence: table/sub-resources/insights-lifecycle

## Notes

Hypotheses: H-S-021. H-S-021 partially confirmed: a mode change is done with ENABLE on an already-ENABLED
resource and re-transitions through ENABLING (~1 s), rule list shrinks 2->1 (hash-only) under THROTTLED_KEYS.
REFUTED clauses: ENABLE without a mode does NOT keep THROTTLED_KEYS - it reverts to the default
ACCESSED_AND_THROTTLED_KEYS (so a controller omitting mode flaps the user's setting); DISABLE with
ContributorInsightsMode set is accepted (200 DISABLING, response echoes the current mode, not the sent one),
not a ValidationException. ENABLE with a mode equal to the current one is a pure no-op (200 ENABLED, no
ENABLING).
