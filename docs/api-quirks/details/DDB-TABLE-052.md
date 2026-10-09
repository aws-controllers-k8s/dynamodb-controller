<!-- generated: ack-api-quirks dynamodb 2026-10-09 model=2012-08-10 controller=34b85e6 -->
# DDB-TABLE-052: TableClass STANDARD -> STANDARD_INFREQUENT_ACCESS: re-send while UPDATING -> ResourceInUse, DP admitted; 31.4 s is a polling upper bound
_Full entry and notes of one finding; its summary entry is in
[table-streams-encryption-class.md](../table-streams-encryption-class.md). Generated from ack-api-quirks
`services/dynamodb` (model 2012-08-10, controller commit 34b85e6); edit the finding in the lab, not here._

## Finding

- <a id="ddb-table-052"></a>**DDB-TABLE-052** `async-state-machine` · impact medium · handled · verified 2026-10-08
  **TableClass STANDARD -> STANDARD_INFREQUENT_ACCESS: re-send while UPDATING -> ResourceInUse, DP admitted; 31.4 s is a polling upper bound**
  UpdateTable TableClass=STANDARD_INFREQUENT_ACCESS on an empty PPR table -> OK (TableStatus=UPDATING). Status
  1s later: UPDATING. Re-send while UPDATING -> ResourceInUseException: 'Attempt to change a resource which is
  still in use: Can't update table class when a table class update is in progress. Table: ackq-8c14eb-mm-tc
  TableClassUpdateInProgress: STANDARD_INFREQUENT_ACCESS'. Different field (DeletionProtectionEnabled) while
  UPDATING -> OK (TableStatus=UPDATING). Final: {"elapsed_before_wait_s": 31.4, "finished": true,
  "total_updating_s_upper_bound": 31.4, "TableClassSummary": {"TableClass": "STANDARD_INFREQUENT_ACCESS",
  "LastUpdateDateTime": "2026-10-08 23:07:26.734000+00:00"}, "TableStatus": "ACTIVE"}. Re-send after ACTIVE ->
  OK (TableStatus=UPDATING).
  - ACK: requeue, updateable.when, custom_update · ops: UpdateTable, DescribeTable · fields: TableClass,
    TableClassSummary
  - repro: PPR table; UpdateTable TableClass=STANDARD_INFREQUENT_ACCESS; poll TableStatus
  - measurements: tableclass_updating_s=31.4
  - handling: handled via `pkg/resource/table/hooks.go:421-425; test/e2e/tests/test_table.py:675-714`
  - related: [DDB-TABLE-283](../table-streams-encryption-class.md#ddb-table-283), [DDB-TABLE-365](../table-streams-encryption-class.md#ddb-table-365), [DDB-TABLE-284](../table-streams-encryption-class.md#ddb-table-284), [DDB-TABLE-285](../table-streams-encryption-class.md#ddb-table-285), [DDB-TABLE-451](../table-streams-encryption-class.md#ddb-table-451), [DDB-TABLE-287](../table-streams-encryption-class.md#ddb-table-287),
    [DDB-TABLE-180](../table-streams-encryption-class.md#ddb-table-180), [DDB-TABLE-120](../table-streams-encryption-class.md#ddb-table-120), [DDB-TABLE-001](../table.md#ddb-table-001), [DDB-TABLE-002](../table-streams-encryption-class.md#ddb-table-002), [DDB-TABLE-370](../table-throughput-billing.md#ddb-table-370), [DDB-TABLE-369](../table-streams-encryption-class.md#ddb-table-369), [DDB-TABLE-065](../table-streams-encryption-class.md#ddb-table-065),
    [DDB-TABLE-459](../table-throughput-billing.md#ddb-table-459), [DDB-TABLE-054](../table-streams-encryption-class.md#ddb-table-054), [DDB-TABLE-010](../table.md#ddb-table-010), [DDB-TABLE-018](../table-streams-encryption-class.md#ddb-table-018) · evidence:
    table/mutation-matrix/stream-protection-throughput

## Notes

Hypotheses: H-T-033.

Contradiction with [DDB-TABLE-284](../table-streams-encryption-class.md#ddb-table-284), [DDB-TABLE-365](../table-streams-encryption-class.md#ddb-table-365), [DDB-TABLE-180](../table-streams-encryption-class.md#ddb-table-180), [DDB-TABLE-018](../table-streams-encryption-class.md#ddb-table-018): 052 reports TableClass UPDATING
31.4 s; 284 (0.5 s polling) measured 4.09/3.58 s, 365 6.1/4.0 s, 180 6.07 s, 018 6.08 s. 284's notes reconcile
it: 052's number is 'elapsed_before_wait_s'/'total_updating_s_upper_bound' from coarse polling, not the switch
duration Resolution: keep both; 284 canonical for the duration; retitle 052
