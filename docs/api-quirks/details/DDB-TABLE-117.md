<!-- generated: ack-api-quirks dynamodb 2026-10-09 model=2012-08-10 controller=34b85e6 -->
# DDB-TABLE-117: DP toggle never shows UPDATING (stays ACTIVE); stream enable gives a ~7 s UPDATING window in which all TTL/PITR/Insights calls are admitted
_Full entry and notes of one finding; its summary entry is in
[table-streams-encryption-class.md](../table-streams-encryption-class.md). Generated from ack-api-quirks
`services/dynamodb` (model 2012-08-10, controller commit 34b85e6); edit the finding in the lab, not here._

## Finding

- <a id="ddb-table-117"></a>**DDB-TABLE-117** `async-state-machine` · impact medium · unhandled (not handled in controller) · verified 2026-10-08
  **DP toggle never shows UPDATING (stays ACTIVE); stream enable gives a ~7 s UPDATING window in which all TTL/PITR/Insights calls are admitted**
  UpdateTable(DeletionProtectionEnabled=true) response TableStatus=ACTIVE, timeline [{'value': 'ACTIVE',
  'from_s': 0.01, 'to_s': None, 'duration_s': None}]; ops right after (table ACTIVE->ACTIVE):
  {'DescribeTimeToLive': 'OK', 'UpdateTimeToLive': "ValidationException (HTTP 400) 'TimeToLive is active on a
  different AttributeName: current AttributeName is ttl'", 'DescribeContinuousBackups': 'OK',
  'UpdateContinuousBackups': 'OK', 'DescribeContributorInsights': 'OK', 'UpdateContributorInsights': 'OK',
  'ListContributorInsights': 'OK'}. UpdateTable(StreamSpecification enable) response TableStatus=UPDATING,
  timeline [{'value': 'UPDATING', 'from_s': 0.01, 'to_s': 7.09, 'duration_s': 7.08}, {'value': 'ACTIVE',
  'from_s': 7.09, 'to_s': None, 'duration_s': None}]; ops right after (table UPDATING->UPDATING):
  {'DescribeTimeToLive': 'OK', 'UpdateTimeToLive': "ValidationException (HTTP 400) 'TimeToLive is active on a
  different AttributeName: current AttributeName is ttl'", 'DescribeContinuousBackups': 'OK',
  'UpdateContinuousBackups': 'OK', 'DescribeContributorInsights': 'OK', 'UpdateContributorInsights': 'OK',
  'ListContributorInsights': 'OK'}. (S1 already had TTL enabled: its UpdateTimeToLive result reflects the TTL
  cooldown, not table state.)
  - ACK: updateable.when, requeue · ops: UpdateTable, UpdateTimeToLive, UpdateContinuousBackups,
    UpdateContributorInsights
  - repro: UpdateTable(DeletionProtectionEnabled) / UpdateTable(StreamSpecification) then immediately call
    sub-resource APIs
  - handling: not handled in the controller (as of commit 34b85e6)
  - related: [DDB-TABLE-119](../table-streams-encryption-class.md#ddb-table-119), [DDB-TABLE-234](../table-policy-kinesis-autoscaling.md#ddb-table-234), [DDB-TABLE-287](../table-streams-encryption-class.md#ddb-table-287), [DDB-TABLE-450](../table-streams-encryption-class.md#ddb-table-450), [DDB-TABLE-460](../table-policy-kinesis-autoscaling.md#ddb-table-460), [DDB-TABLE-435](../table-streams-encryption-class.md#ddb-table-435),
    [DDB-TABLE-121](../table-throughput-billing.md#ddb-table-121), [DDB-TABLE-464](../service.md#ddb-table-464), [DDB-TABLE-445](../service.md#ddb-table-445), [DDB-TABLE-377](../service.md#ddb-table-377) · evidence:
    table/state-machine/subresource-admissibility

## Notes

Hypotheses: H-S-015, H-S-025. Hypotheses: H-S-015, H-S-025 (UPDATING does not block any TTL/PITR/Insights
call). UpdateTable(DeletionProtectionEnabled=true) returned TableStatus=ACTIVE and Describe never showed
UPDATING. Side observation: UpdateTable(DeletionProtectionEnabled=false) issued 2 s after the first
UpdateTable failed with ThrottlingException (HTTP 400), leaving protection on - a controller toggling two
table attributes in quick succession must expect this.
