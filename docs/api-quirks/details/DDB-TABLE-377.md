<!-- generated: ack-api-quirks dynamodb 2026-10-09 model=2012-08-10 controller=34b85e6 -->
# DDB-TABLE-377: Per-table quotas/cooldowns die with the table: TableClass, SSE, decrease budget, TTL and DP cooldowns reset on same-name re-create
_Full entry and notes of one finding; its summary entry is in [service.md](../service.md). Generated from
ack-api-quirks `services/dynamodb` (model 2012-08-10, controller commit 34b85e6); edit the finding in the lab,
not here._

## Finding

- <a id="ddb-table-377"></a>**DDB-TABLE-377** `quota-limit` · impact medium · unhandled (not handled in controller) · verified 2026-10-09
  **Per-table quotas/cooldowns die with the table: TableClass, SSE, decrease budget, TTL and DP cooldowns reset on same-name re-create**
  Five tables, each driven into its per-table limit, deleted (DELETING 4.6-6.2 s) and re-created under the
  SAME name (same TableArn, new TableId): TableClass: IA, STD, 3rd -> LimitExceededException 'Updates to
  TableClass are limited to 2 times in 30 day(s)'; re-created table: IA OK, STD OK, 3rd LimitExceeded again
  (fresh budget). SSE: 4 flips OK, 5th -> LimitExceededException 'Encryption mode changes are limited in the
  24h window'; re-created table: flip OK. Provisioned decreases: 4 OK, 5th -> LimitExceededException;
  re-created table shows NumberOfDecreasesToday=0 and a decrease is accepted. TTL: enable OK, immediate
  disable -> ValidationException 'Time to live has been modified multiple times within a fixed interval';
  re-created table accepted TTL enable 11.4 s after the old incarnation's change. DeletionProtection: toggle
  on the re-created table 12.0 s after the old incarnation's toggle -> 200 (no 15 s ThrottlingException).
  WarmThroughput high-water mark (10/10 on the old PROVISIONED table) reads 5/5 on the re-created 5/5 table.
  - ACK: e2e-timing, docs-only · ops: UpdateTable, UpdateTimeToLive, DeleteTable, CreateTable · fields:
    TableClass, SSESpecification, ProvisionedThroughput, TimeToLiveSpecification, DeletionProtectionEnabled,
    WarmThroughput
  - repro: CreateTable X; UpdateTable TableClass=IA; TableClass=STANDARD; TableClass=IA (LimitExceeded);
    DeleteTable X; wait 404; CreateTable X; UpdateTable TableClass=IA -> 200
  - measurements: ttl_recreate_gap_s=11.4, dp_recreate_gap_s=12.0, delete_to_404_s=5.1,
    recreate_to_active_s=6.1
  - handling: not handled in the controller (as of commit 34b85e6)
  - related: [DDB-TABLE-283](../table-streams-encryption-class.md#ddb-table-283), [DDB-TABLE-142](../table-streams-encryption-class.md#ddb-table-142), [DDB-TABLE-184](../table-throughput-billing.md#ddb-table-184), [DDB-TABLE-143](../table-subresources.md#ddb-table-143), [DDB-TABLE-003](../table-streams-encryption-class.md#ddb-table-003), [DDB-TABLE-061](../table-throughput-billing.md#ddb-table-061),
    [DDB-TABLE-013](../service.md#ddb-table-013), [DDB-TABLE-214](../table-policy-kinesis-autoscaling.md#ddb-table-214), [DDB-TABLE-236](../table-policy-kinesis-autoscaling.md#ddb-table-236), [DDB-TABLE-097](../table-subresources.md#ddb-table-097), [DDB-TABLE-342](../table-subresources.md#ddb-table-342), [DDB-TABLE-374](../service.md#ddb-table-374), [DDB-TABLE-271](../table-policy-kinesis-autoscaling.md#ddb-table-271),
    [DDB-TABLE-436](../table-policy-kinesis-autoscaling.md#ddb-table-436), [DDB-TABLE-144](../table-subresources.md#ddb-table-144), [DDB-TABLE-145](../table-subresources.md#ddb-table-145), [DDB-TABLE-146](../table-subresources.md#ddb-table-146), [DDB-TABLE-147](../table-subresources.md#ddb-table-147), [DDB-TABLE-114](../service.md#ddb-table-114), [DDB-TABLE-117](../table-streams-encryption-class.md#ddb-table-117),
    [DDB-TABLE-464](../service.md#ddb-table-464), [DDB-TABLE-445](../service.md#ddb-table-445) · evidence: table/creative/name-keyed-cooldowns

## Notes

All budgets are keyed by the table incarnation (TableId), not by TableName/ARN. Consequences for a controller:
(1) e2e tests that delete and re-create a table name back-to-back never inherit an exhausted quota; (2) the
ONLY way out of a 30-day TableClass lock, a 31-min TTL cooldown or an exhausted SSE/decrease quota is to
re-create the table, which a controller must not do on its own (data loss) - surface the LimitExceeded as
terminal and document re-creation as the escape hatch; (3) WarmThroughput's never-decreases rule
([DDB-TABLE-061](../table-throughput-billing.md#ddb-table-061)) is also per incarnation.
