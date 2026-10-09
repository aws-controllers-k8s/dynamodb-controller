<!-- generated: ack-api-quirks dynamodb 2026-10-09 model=2012-08-10 controller=34b85e6 -->
# DDB-TABLE-284: TableClass switch: UPDATING ~3.6-4.1 s; TableClassSummary flips with ACTIVE; UpdateTable echoes the OLD summary; Delete refused until ACTIVE
_Full entry and notes of one finding; its summary entry is in
[table-streams-encryption-class.md](../table-streams-encryption-class.md). Generated from ack-api-quirks
`services/dynamodb` (model 2012-08-10, controller commit 34b85e6); edit the finding in the lab, not here._

## Finding

- <a id="ddb-table-284"></a>**DDB-TABLE-284** `async-state-machine` · impact medium · handled · verified 2026-10-09
  **TableClass switch: UPDATING ~3.6-4.1 s; TableClassSummary flips with ACTIVE; UpdateTable echoes the OLD summary; Delete refused until ACTIVE**
  0.5 s DescribeTable polling on empty PPR tables. Clean switches STD->IA and IA->STD: TableStatus=UPDATING
  for 4.09 s and 3.58 s; TableClassSummary.TableClass/LastUpdateDateTime showed the new class in the same poll
  in which TableStatus returned to ACTIVE (in table/creative/tableclass-sse-race the new class was visible
  ~0.2 s before ACTIVE). Sequences: [UPDATING, absent] -> [ACTIVE, IA, ts1]; [UPDATING, IA, ts1] -> [ACTIVE,
  STANDARD, ts2]. UpdateTable responses: TableStatus=UPDATING with TableClassSummary = the PREVIOUS summary
  (absent before the first change, IA/ts1 when switching back) - the response never shows the requested class.
  Baseline before any change: TableClassSummary absent (== STANDARD). Table 'del': UpdateTable(TableClass=IA)
  then DeleteTable every second -> ResourceInUseException 'Table ... is in the process of being updated' at
  +0.05/+1.1/+2.1/+3.1/+4.1 s, accepted at +5.2 s (first poll showing ACTIVE); table gone 5 s later.
  - ACK: synced.when, deletable.when, requeue, custom_update · ops: UpdateTable, DescribeTable, DeleteTable ·
    fields: TableClass, TableClassSummary.TableClass, TableClassSummary.LastUpdateDateTime, TableStatus
  - repro: PPR table; UpdateTable TableClass=...; DescribeTable every 0.5 s; separately DeleteTable every 1 s
    after a switch
  - measurements: switch_std_to_ia_updating_s=4.09, switch_ia_to_std_updating_s=3.58,
    switch_with_sse_interleaved_class_flipped_s=3.59, delete_admitted_after_s=5.19, delete_gone_after_s=5.07
  - handling: handled via `pkg/resource/table/hooks.go:306; pkg/resource/table/hooks.go:325-335; generator.yaml:12; templates/hooks/table/sdk_read_one_post_set_output.go.tpl:46-50; pkg/resource/table/hooks.go:421-425; test/e2e/tests/test_table.py:675-714`
  - related: [DDB-TABLE-026](../table-streams-encryption-class.md#ddb-table-026), [DDB-TABLE-177](../table-streams-encryption-class.md#ddb-table-177), [DDB-TABLE-365](../table-streams-encryption-class.md#ddb-table-365), [DDB-TABLE-180](../table-streams-encryption-class.md#ddb-table-180), [DDB-TABLE-019](../table-streams-encryption-class.md#ddb-table-019), [DDB-TABLE-283](../table-streams-encryption-class.md#ddb-table-283),
    [DDB-TABLE-052](../table-streams-encryption-class.md#ddb-table-052), [DDB-TABLE-285](../table-streams-encryption-class.md#ddb-table-285), [DDB-TABLE-451](../table-streams-encryption-class.md#ddb-table-451), [DDB-TABLE-287](../table-streams-encryption-class.md#ddb-table-287), [DDB-TABLE-120](../table-streams-encryption-class.md#ddb-table-120), [DDB-TABLE-069](../table-streams-encryption-class.md#ddb-table-069), [DDB-TABLE-065](../table-streams-encryption-class.md#ddb-table-065),
    [DDB-TABLE-066](../table-throughput-billing.md#ddb-table-066), [DDB-TABLE-002](../table-streams-encryption-class.md#ddb-table-002), [DDB-TABLE-369](../table-streams-encryption-class.md#ddb-table-369), [DDB-TABLE-370](../table-throughput-billing.md#ddb-table-370), [DDB-TABLE-001](../table.md#ddb-table-001), [DDB-TABLE-334](../table-streams-encryption-class.md#ddb-table-334), [DDB-TABLE-060](../table-throughput-billing.md#ddb-table-060),
    [DDB-TABLE-179](../table-throughput-billing.md#ddb-table-179), [DDB-TABLE-371](../table-streams-encryption-class.md#ddb-table-371), [DDB-TABLE-140](../table-streams-encryption-class.md#ddb-table-140), [DDB-TABLE-064](../table-throughput-billing.md#ddb-table-064), [DDB-TABLE-183](../table-throughput-billing.md#ddb-table-183), [DDB-TABLE-156](../table-throughput-billing.md#ddb-table-156), [DDB-TABLE-018](../table-streams-encryption-class.md#ddb-table-018) ·
    evidence: table/state-machine/table-class-switch

## Notes

Wave-1's 31.4 s was an upper bound from coarse polling; the switch itself takes a few seconds on an empty
table. Compare TableClassSummary.TableClass (absent == STANDARD) with the desired class rather than waiting on
TableStatus alone.

Contradiction with [DDB-TABLE-052](../table-streams-encryption-class.md#ddb-table-052), [DDB-TABLE-365](../table-streams-encryption-class.md#ddb-table-365), [DDB-TABLE-180](../table-streams-encryption-class.md#ddb-table-180), [DDB-TABLE-018](../table-streams-encryption-class.md#ddb-table-018): 052 reports TableClass UPDATING
31.4 s; 284 (0.5 s polling) measured 4.09/3.58 s, 365 6.1/4.0 s, 180 6.07 s, 018 6.08 s. 284's notes reconcile
it: 052's number is 'elapsed_before_wait_s'/'total_updating_s_upper_bound' from coarse polling, not the switch
duration Resolution: keep both; 284 canonical for the duration; retitle 052
