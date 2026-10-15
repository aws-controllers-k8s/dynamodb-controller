<!-- generated: ack-api-quirks dynamodb 2026-10-09 model=2012-08-10 controller=34b85e6 -->
# DDB-TABLE-287: SSE write during a TableClass switch flips TableStatus to ACTIVE at once; a 2nd TableClass change is then accepted but silently lost
_Full entry and notes of one finding; its summary entry is in
[table-streams-encryption-class.md](../table-streams-encryption-class.md). Generated from ack-api-quirks
`services/dynamodb` (model 2012-08-10, controller commit 34b85e6); edit the finding in the lab, not here._

## Finding

- <a id="ddb-table-287"></a>**DDB-TABLE-287** `async-state-machine` · impact high · unhandled (not handled in controller) · verified 2026-10-09
  **SSE write during a TableClass switch flips TableStatus to ACTIVE at once; a 2nd TableClass change is then accepted but silently lost**
  Table 'sse': UpdateTable(TableClass=IA) -> UPDATING; UpdateTable(SSESpecification{Enabled:true,SSEType:KMS})
  at +0.16 s -> OK with TableStatus=ACTIVE; DescribeTable at +0.17 s: ACTIVE, TableClassSummary absent,
  SSEDescription.Status=UPDATING. UpdateTable(TableClass=STANDARD) at +0.19 s -> OK (response UPDATING)
  although the IA job was still running; UpdateTable(OnDemandThroughput) at +0.22 s -> OK (normally
  ResourceInUseException during a TableClass switch). TableClassSummary showed IA at +9.1 s
  (LastUpdateDateTime 01:00:38) and never STANDARD: the accepted STANDARD request was silently dropped. A
  further TableClass=STANDARD request afterwards -> OK and applied (so the dropped request did not count
  against the 2-per-30-days quota). Controls on sibling tables: TTL write / PITR write / no write ->
  TableStatus stayed UPDATING, every TableClass re-send was ResourceInUseException ('Can't update table class
  when a table class update is in progress') until the class flipped at 4.6/4.7/4.0 s; in the no-write control
  TableClassSummary showed the new class ~0.2 s before TableStatus returned to ACTIVE.
  - ACK: one-per-reconcile, synced.when, requeue, custom_update · ops: UpdateTable, UpdateTimeToLive,
    UpdateContinuousBackups, DescribeTable · fields: TableClass, SSESpecification, TableStatus,
    TableClassSummary
  - repro: UpdateTable(TableClass=IA); UpdateTable(SSESpecification{Enabled:true,SSEType:KMS}) 0.1 s later;
    DescribeTable every 0.2 s; UpdateTable(TableClass=STANDARD) every 0.5 s
  - handling: not handled in the controller (as of commit 34b85e6)
  - related: [DDB-TABLE-283](../table-streams-encryption-class.md#ddb-table-283), [DDB-TABLE-365](../table-streams-encryption-class.md#ddb-table-365), [DDB-TABLE-052](../table-streams-encryption-class.md#ddb-table-052), [DDB-TABLE-284](../table-streams-encryption-class.md#ddb-table-284), [DDB-TABLE-285](../table-streams-encryption-class.md#ddb-table-285), [DDB-TABLE-451](../table-streams-encryption-class.md#ddb-table-451),
    [DDB-TABLE-180](../table-streams-encryption-class.md#ddb-table-180), [DDB-TABLE-120](../table-streams-encryption-class.md#ddb-table-120), [DDB-TABLE-286](../table-streams-encryption-class.md#ddb-table-286), [DDB-TABLE-458](../table-indexes.md#ddb-table-458), [DDB-TABLE-462](../table-indexes.md#ddb-table-462), [DDB-TABLE-175](../table-indexes.md#ddb-table-175), [DDB-TABLE-450](../table-streams-encryption-class.md#ddb-table-450),
    [DDB-TABLE-117](../table-streams-encryption-class.md#ddb-table-117), [DDB-TABLE-119](../table-streams-encryption-class.md#ddb-table-119), [DDB-TABLE-234](../table-policy-kinesis-autoscaling.md#ddb-table-234), [DDB-TABLE-460](../table-policy-kinesis-autoscaling.md#ddb-table-460), [DDB-TABLE-435](../table-streams-encryption-class.md#ddb-table-435), [DDB-TABLE-121](../table-throughput-billing.md#ddb-table-121) · hypotheses:
    H-T-001, H-T-013 · evidence: table/creative/tableclass-sse-race

## Notes

Follow-up of table/state-machine/table-class-switch attempt 1 (five TableClass changes accepted in 14 s after
a DP+SSE combo). The SSE path (which keeps TableStatus=ACTIVE by design) overwrites the table-level UPDATING
marker set by the asynchronous TableClass job. A controller that sends TableClass and SSESpecification changes
in back-to-back UpdateTable calls can observe a premature ACTIVE, get a second TableClass change accepted and
silently lost, and must re-compare TableClassSummary.TableClass after every SSE change.
