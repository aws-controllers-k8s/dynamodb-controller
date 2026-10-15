<!-- generated: ack-api-quirks dynamodb 2026-10-09 model=2012-08-10 controller=34b85e6 -->
# DDB-TABLE-120: While SSEDescription.Status=UPDATING (TableStatus ACTIVE): Delete rejected; DP, stream toggle and TableClass change admitted
_Full entry and notes of one finding; its summary entry is in
[table-streams-encryption-class.md](../table-streams-encryption-class.md). Generated from ack-api-quirks
`services/dynamodb` (model 2012-08-10, controller commit 34b85e6); edit the finding in the lab, not here._

## Finding

- <a id="ddb-table-120"></a>**DDB-TABLE-120** `async-state-machine` · impact high · handled · verified 2026-10-08
  **While SSEDescription.Status=UPDATING (TableStatus ACTIVE): Delete rejected; DP, stream toggle and TableClass change admitted**
  Ops fired right after UpdateTable(SSESpecification toggle) (TableStatus stays ACTIVE,
  SSEDescription.Status=UPDATING ~20s): {'delete': 'ResourceInUseException', 'dp_false': 'OK(ACTIVE)',
  'stream_toggle': 'OK(UPDATING)', 'tableclass_standard': 'OK(UPDATING)'}. Rejections: {'delete': 'Attempt to
  change a resource which is still in use: Table: ackq-8d9518-fadm is in the process of being updated.'}. The
  4th SSE toggle within ~9 minutes failed: LimitExceededException 'Subscriber limit exceeded: Encryption mode
  changes are limited in the 24h window ending at <ts>. After the first 4 change, each subsequent change in
  the same window can be performe[d]...'.
  - ACK: synced.when, deletable.when, requeue, terminal_codes · ops: UpdateTable, DeleteTable · fields:
    SSESpecification, SSEDescription.Status, DeletionProtectionEnabled, StreamSpecification, TableClass
  - repro: UpdateTable(SSESpecification{Enabled:true,SSEType:KMS}); immediately DeleteTable /
    UpdateTable(other field); repeat toggles >4 times in 24h
  - handling: handled via `pkg/resource/table/hooks.go:434-473; test/e2e/tests/test_table.py:630-673`
  - related: [DDB-TABLE-283](../table-streams-encryption-class.md#ddb-table-283), [DDB-TABLE-365](../table-streams-encryption-class.md#ddb-table-365), [DDB-TABLE-052](../table-streams-encryption-class.md#ddb-table-052), [DDB-TABLE-284](../table-streams-encryption-class.md#ddb-table-284), [DDB-TABLE-285](../table-streams-encryption-class.md#ddb-table-285), [DDB-TABLE-451](../table-streams-encryption-class.md#ddb-table-451),
    [DDB-TABLE-287](../table-streams-encryption-class.md#ddb-table-287), [DDB-TABLE-180](../table-streams-encryption-class.md#ddb-table-180), [DDB-TABLE-001](../table.md#ddb-table-001), [DDB-TABLE-002](../table-streams-encryption-class.md#ddb-table-002), [DDB-TABLE-370](../table-throughput-billing.md#ddb-table-370), [DDB-TABLE-369](../table-streams-encryption-class.md#ddb-table-369), [DDB-TABLE-065](../table-streams-encryption-class.md#ddb-table-065),
    [DDB-TABLE-459](../table-throughput-billing.md#ddb-table-459), [DDB-TABLE-054](../table-streams-encryption-class.md#ddb-table-054), [DDB-TABLE-010](../table.md#ddb-table-010), [DDB-TABLE-069](../table-streams-encryption-class.md#ddb-table-069), [DDB-TABLE-066](../table-throughput-billing.md#ddb-table-066), [DDB-TABLE-334](../table-streams-encryption-class.md#ddb-table-334), [DDB-TABLE-082](../table-streams-encryption-class.md#ddb-table-082),
    [DDB-TABLE-078](../table-streams-encryption-class.md#ddb-table-078), [DDB-TABLE-141](../table-streams-encryption-class.md#ddb-table-141), [DDB-TABLE-140](../table-streams-encryption-class.md#ddb-table-140), [DDB-TABLE-371](../table-streams-encryption-class.md#ddb-table-371), [DDB-TABLE-079](../table-streams-encryption-class.md#ddb-table-079), [DDB-TABLE-081](../table-streams-encryption-class.md#ddb-table-081), [DDB-TABLE-142](../table-streams-encryption-class.md#ddb-table-142) ·
    evidence: table/state-machine/field-admissibility-while-updating

## Notes

Two findings in one window: (1) TableStatus=ACTIVE is not 'quiet' - a stream toggle started during an SSE
update made both UPDATING at once; (2) SSE mode flips are quota-limited to 4 per rolling 24h per table
(LimitExceededException, not ValidationException), so a controller flapping SSE settings locks itself out for
a day.

Contradiction with [DDB-TABLE-140](../table-streams-encryption-class.md#ddb-table-140), [DDB-TABLE-081](../table-streams-encryption-class.md#ddb-table-081), [DDB-TABLE-141](../table-streams-encryption-class.md#ddb-table-141), [DDB-TABLE-142](../table-streams-encryption-class.md#ddb-table-142): SSE quota boundary: 081/141/142
observe 4 accepted changes and the 5th rejected ('Number of updates today: 4'); 140's T1 (created with a CMK,
first change CMK->CMK by ARN) had 5 accepted and the 6th rejected ('Number of updates today: 5', verified in
sse-kms-quota evidence). 120's behavior says 'the 4th SSE toggle failed' but its evidence shows a phase-A
enable plus three phase-B toggles succeeded before the 5th call failed with 'Number of updates today: 4' -
consistent with 081, wrong count in the text Resolution: keep all; 081 canonical (4 then one per 6 h, window
anchored ~9 s after the first change); 140-T1's extra accepted change is unexplained - controllers should
budget for 4
