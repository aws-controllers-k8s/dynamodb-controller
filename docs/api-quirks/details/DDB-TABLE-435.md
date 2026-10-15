<!-- generated: ack-api-quirks dynamodb 2026-10-09 model=2012-08-10 controller=34b85e6 -->
# DDB-TABLE-435: Fan-out of UpdateTable(stream)+TTL+PITR+Tag+Policy+Insights on ONE table at the same instant: no per-table conflict, all settings land
_Full entry and notes of one finding; its summary entry is in
[table-streams-encryption-class.md](../table-streams-encryption-class.md). Generated from ack-api-quirks
`services/dynamodb` (model 2012-08-10, controller commit 34b85e6); edit the finding in the lab, not here._

## Finding

- <a id="ddb-table-435"></a>**DDB-TABLE-435** `update-granularity` · impact medium · unhandled (not handled in controller) · verified 2026-10-09
  **Fan-out of UpdateTable(stream)+TTL+PITR+Tag+Policy+Insights on ONE table at the same instant: no per-table conflict, all settings land**
  Six different write APIs against the same fresh table fired within 0.12 s: UpdateTable(StreamSpecification
  enable) 200, UpdateTimeToLive(enable) 200, UpdateContinuousBackups(PITR on) 200, PutResourcePolicy 200,
  UpdateContributorInsights(ENABLE) 200, TagResource -> ThrottlingException 'The rate of control plane
  requests made by this account is too high' (the account limiter, not a per-table conflict; 200 on retry 2 s
  later). After the table settled every setting was in effect: StreamEnabled=true, TTL ENABLED, PITR ENABLED,
  policy readable, insights ENABLED, tags present. No ResourceInUseException or 'table is being updated'
  between the sub-resource APIs.
  - ACK: custom_update, e2e-timing · ops: UpdateTable, UpdateTimeToLive, UpdateContinuousBackups, TagResource,
    PutResourcePolicy, UpdateContributorInsights · fields: StreamSpecification, TimeToLiveSpecification,
    PointInTimeRecoverySpecification, Tags, ResourcePolicy, ContributorInsightsAction
  - repro: fresh ACTIVE table (>5 s old); 6 threads: update_table stream, update_time_to_live,
    update_continuous_backups, tag_resource, put_resource_policy, update_contributor_insights
  - measurements: burst_span_s=0.12, ok=5, throttled=1
  - handling: not handled in the controller (as of commit 34b85e6)
  - related: [DDB-TABLE-119](../table-streams-encryption-class.md#ddb-table-119), [DDB-TABLE-172](../service.md#ddb-table-172), [DDB-TABLE-206](../table-policy-kinesis-autoscaling.md#ddb-table-206), [DDB-TABLE-382](../table-streams-encryption-class.md#ddb-table-382), [DDB-TABLE-117](../table-streams-encryption-class.md#ddb-table-117), [DDB-TABLE-234](../table-policy-kinesis-autoscaling.md#ddb-table-234),
    [DDB-TABLE-287](../table-streams-encryption-class.md#ddb-table-287), [DDB-TABLE-450](../table-streams-encryption-class.md#ddb-table-450), [DDB-TABLE-460](../table-policy-kinesis-autoscaling.md#ddb-table-460), [DDB-TABLE-121](../table-throughput-billing.md#ddb-table-121), [DDB-TABLE-434](../service.md#ddb-table-434), [DDB-TABLE-383](../table-streams-encryption-class.md#ddb-table-383), [DDB-TABLE-445](../service.md#ddb-table-445) ·
    evidence: table/creative/throttle-exemptions

## Notes

Complements [DDB-TABLE-382](../table-streams-encryption-class.md#ddb-table-382) (UpdateTable itself is single-concern): the sub-resource APIs do not share a lock
with UpdateTable or with each other, so a controller can issue the table-level UpdateTable and all
sub-resource writes of one reconcile concurrently instead of serially, as long as it retries the account-level
ThrottlingException. Single trial; the per-resource locks that DO exist (tag write lock [DDB-TABLE-172](../service.md#ddb-table-172), policy
write window [DDB-TABLE-206](../table-policy-kinesis-autoscaling.md#ddb-table-206)) apply to a SECOND write of the same kind, not to the first fan-out.
