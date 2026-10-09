<!-- generated: ack-api-quirks dynamodb 2026-10-09 model=2012-08-10 controller=34b85e6 -->
# DDB-TABLE-017: UpdateTable(DeletionProtectionEnabled) is synchronous: response TableStatus=ACTIVE, DP=False; DescribeTable immediately agrees (False)
_Full entry and notes of one finding; its summary entry is in
[table-streams-encryption-class.md](../table-streams-encryption-class.md). Generated from ack-api-quirks
`services/dynamodb` (model 2012-08-10, controller commit 34b85e6); edit the finding in the lab, not here._

## Finding

- <a id="ddb-table-017"></a>**DDB-TABLE-017** `async-state-machine` · impact medium · handled · verified 2026-10-08
  **UpdateTable(DeletionProtectionEnabled) is synchronous: response TableStatus=ACTIVE, DP=False; DescribeTable immediately agrees (False)**
  On a quiet ACTIVE table UpdateTable(DeletionProtectionEnabled=false) returned HTTP 200 with
  TableDescription.TableStatus=ACTIVE and DeletionProtectionEnabled=False; DescribeTable immediately
  afterwards: TableStatus=ACTIVE, DeletionProtectionEnabled=False. Latency 16 ms.
  - ACK: synced.when, none · ops: UpdateTable, DescribeTable · fields: DeletionProtectionEnabled
  - repro: UpdateTable(DeletionProtectionEnabled=false) on ACTIVE table; inspect response; DescribeTable
  - handling: handled via `pkg/resource/table/hooks.go:729-731; pkg/resource/table/hooks.go:427-429`
  - related: [DDB-TABLE-003](../table-streams-encryption-class.md#ddb-table-003), [DDB-TABLE-176](../table-streams-encryption-class.md#ddb-table-176), [DDB-TABLE-438](../table-streams-encryption-class.md#ddb-table-438), [DDB-TABLE-053](../service.md#ddb-table-053) · evidence:
    table/error-taxonomy/missing-table-noop-update-dp

## Notes

H-T-010 confirmed. Note the 15s per-table cooldown between DP flips (ThrottlingException).
