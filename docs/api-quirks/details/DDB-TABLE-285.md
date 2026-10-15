<!-- generated: ack-api-quirks dynamodb 2026-10-09 model=2012-08-10 controller=34b85e6 -->
# DDB-TABLE-285: DeletionProtection write during a TableClass switch does NOT reset TableStatus; attempt-1 anomaly traced to the SSE write
_Full entry and notes of one finding; its summary entry is in
[table-streams-encryption-class.md](../table-streams-encryption-class.md). Generated from ack-api-quirks
`services/dynamodb` (model 2012-08-10, controller commit 34b85e6); edit the finding in the lab, not here._

## Finding

- <a id="ddb-table-285"></a>**DDB-TABLE-285** `async-state-machine` · impact medium · unhandled (not handled in controller) · verified 2026-10-09
  **DeletionProtection write during a TableClass switch does NOT reset TableStatus; attempt-1 anomaly traced to the SSE write**
  UpdateTable(TableClass=IA) then UpdateTable(DeletionProtectionEnabled=true) at +0.05 s -> OK (response
  TableStatus=UPDATING). Dense DescribeTable (0.2 s) (status, class, LastUpdateDateTime, DP): [{'t_s': 0.06,
  'value': ['UPDATING', None, None, True]}, {'t_s': 5.38, 'value': ['ACTIVE', 'STANDARD_INFREQUENT_ACCESS',
  '2026-10-09 00:54:02.297000+00:00', True]}]. Same-value TableClass re-sends every 1 s until the class
  flipped: [(0.07, 'ResourceInUseException', 'UPDATING'), (1.14, 'ResourceInUseException', 'UPDATING'), (2.2,
  'ResourceInUseException', 'UPDATING'), (3.27, 'ResourceInUseException', 'UPDATING'), (4.33,
  'ResourceInUseException', 'UPDATING')]. Settled: {'updating_seen': False, 'status_updating_total_s': 0,
  'first_active_s': 0.01, 'class_flipped_s': 0.01, 'status_when_class_flipped': 'ACTIVE', 'distinct_values':
  [['ACTIVE', 'STANDARD_INFREQUENT_ACCESS', '2026-10-09 00:54:02.297000+00:00']], 'polls': 31}. Second real
  change afterwards: OK None. Attempt 1 (evidence.attempt1.jsonl, table ackq-60ca92-tc): DP accepted at +0.1
  s, DescribeTable ACTIVE at +0.5 s, TableClass STD/IA/STD/IA all accepted at +0.9/+1.0/+7.4/+13.9 s,
  TableClassSummary showed IA at 00:24:21.8 and STANDARD at 00:24:36.6 - the final class did not match the
  last accepted request (IA).
  - ACK: one-per-reconcile, requeue, synced.when · ops: UpdateTable, DescribeTable · fields: TableClass,
    DeletionProtectionEnabled, TableStatus, TableClassSummary
  - repro: UpdateTable(TableClass=IA); immediately UpdateTable(DeletionProtectionEnabled=true); DescribeTable
    every 0.2 s; re-send TableClass=IA every 1 s
  - handling: not handled in the controller (as of commit 34b85e6)
  - related: [DDB-TABLE-283](../table-streams-encryption-class.md#ddb-table-283), [DDB-TABLE-365](../table-streams-encryption-class.md#ddb-table-365), [DDB-TABLE-052](../table-streams-encryption-class.md#ddb-table-052), [DDB-TABLE-284](../table-streams-encryption-class.md#ddb-table-284), [DDB-TABLE-451](../table-streams-encryption-class.md#ddb-table-451), [DDB-TABLE-287](../table-streams-encryption-class.md#ddb-table-287),
    [DDB-TABLE-180](../table-streams-encryption-class.md#ddb-table-180), [DDB-TABLE-120](../table-streams-encryption-class.md#ddb-table-120), [DDB-TABLE-001](../table.md#ddb-table-001), [DDB-TABLE-002](../table-streams-encryption-class.md#ddb-table-002), [DDB-TABLE-370](../table-throughput-billing.md#ddb-table-370), [DDB-TABLE-369](../table-streams-encryption-class.md#ddb-table-369), [DDB-TABLE-065](../table-streams-encryption-class.md#ddb-table-065),
    [DDB-TABLE-459](../table-throughput-billing.md#ddb-table-459), [DDB-TABLE-054](../table-streams-encryption-class.md#ddb-table-054), [DDB-TABLE-010](../table.md#ddb-table-010) · hypotheses: H-T-001 · evidence:
    table/state-machine/table-class-switch

## Notes

Isolates the attempt-1 anomaly (evidence.attempt1.jsonl: DP+SSE interleaved, 5 TableClass changes accepted in
14 s, final class != last request): the DP write alone is harmless; the SSE write is the culprit - see
table/creative/tableclass-sse-race. Confidence high for the DP half.
