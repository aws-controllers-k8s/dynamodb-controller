<!-- generated: ack-api-quirks dynamodb 2026-10-09 model=2012-08-10 controller=34b85e6 -->
# DDB-TABLE-119: While TableStatus=UPDATING (stream toggle): Delete/TableClass/OnDemandThroughput rejected, DP/SSE/Backup/TTL/PITR/policy/Warm admitted
_Full entry and notes of one finding; its summary entry is in
[table-streams-encryption-class.md](../table-streams-encryption-class.md). Generated from ack-api-quirks
`services/dynamodb` (model 2012-08-10, controller commit 34b85e6); edit the finding in the lab, not here._

## Finding

- <a id="ddb-table-119"></a>**DDB-TABLE-119** `async-state-machine` · impact high · unhandled (not handled in controller) · verified 2026-10-08
  **While TableStatus=UPDATING (stream toggle): Delete/TableClass/OnDemandThroughput rejected, DP/SSE/Backup/TTL/PITR/policy/Warm admitted**
  Each op fired right after UpdateTable(StreamSpecification toggle) returned TableStatus=UPDATING (fresh ~4s
  window per op): {'delete': 'ResourceInUseException', 'dp_true': 'OK(UPDATING)', 'tableclass_ia':
  'ResourceInUseException', 'sse_toggle': 'OK(ACTIVE)', 'create_backup': 'OK', 'ttl_enable': 'OK',
  'pitr_enable': 'OK', 'put_resource_policy': 'OK', 'ondemand_throughput': 'ResourceInUseException',
  'warm_increase': 'OK(UPDATING)'}. Rejections: {'delete': 'Attempt to change a resource which is still in
  use: Cannot delete table while stream is being enabled/disabled.', 'tableclass_ia': "Attempt to change a
  resource which is still in use: Can't update table class when stream status is being updated. Table:",
  'ondemand_throughput': 'Attempt to change a resource which is still in use: OnDemandThroughput cannot be
  updated while stream status update is i'}.
  - ACK: updateable.when, requeue, synced.when · ops: UpdateTable, DeleteTable, CreateBackup,
    UpdateTimeToLive, UpdateContinuousBackups, PutResourcePolicy · fields: DeletionProtectionEnabled,
    TableClass, SSESpecification, OnDemandThroughput, WarmThroughput, StreamSpecification
  - repro: UpdateTable(StreamSpecification toggle); immediately issue the op; fresh toggle per op
  - handling: not handled in the controller (as of commit 34b85e6)
  - related: [DDB-TABLE-117](../table-streams-encryption-class.md#ddb-table-117), [DDB-TABLE-234](../table-policy-kinesis-autoscaling.md#ddb-table-234), [DDB-TABLE-287](../table-streams-encryption-class.md#ddb-table-287), [DDB-TABLE-450](../table-streams-encryption-class.md#ddb-table-450), [DDB-TABLE-460](../table-policy-kinesis-autoscaling.md#ddb-table-460), [DDB-TABLE-435](../table-streams-encryption-class.md#ddb-table-435),
    [DDB-TABLE-121](../table-throughput-billing.md#ddb-table-121), [DDB-TABLE-172](../service.md#ddb-table-172), [DDB-TABLE-348](../table-policy-kinesis-autoscaling.md#ddb-table-348), [DDB-TABLE-210](../table-policy-kinesis-autoscaling.md#ddb-table-210), [DDB-TABLE-432](../table-policy-kinesis-autoscaling.md#ddb-table-432) · evidence:
    table/state-machine/field-admissibility-while-updating

## Notes

Refines H-T-001: UPDATING is not a blanket ResourceInUseException. Rejection messages are field-specific
('Can't update table class when stream s...', 'OnDemandThroughput cannot be updated w...', 'Cannot delete
table while stream is being enabled/disabled'). An SSE switch, a WarmThroughput increase and a backup can all
start while the stream toggle is in flight, so several async phases can overlap.
