<!-- generated: ack-api-quirks dynamodb 2026-10-09 model=2012-08-10 controller=34b85e6 -->
# DDB-TABLE-330: After EnableKey the data plane works within <7 min but TableStatus stays INACCESSIBLE_ENCRYPTION_CREDENTIALS for 57 min
_Full entry and notes of one finding; its summary entry is in
[table-streams-encryption-class.md](../table-streams-encryption-class.md). Generated from ack-api-quirks
`services/dynamodb` (model 2012-08-10, controller commit 34b85e6); edit the finding in the lab, not here._

## Finding

- <a id="ddb-table-330"></a>**DDB-TABLE-330** `eventual-consistency` · impact medium · partially handled · verified 2026-10-09
  **After EnableKey the data plane works within <7 min but TableStatus stays INACCESSIBLE_ENCRYPTION_CREDENTIALS for 57 min**
  Table 'traf' of table/state-machine/kms-inaccessible-lifecycle: CMK disabled 00:19:32, TableStatus flagged
  INACCESSIBLE_ENCRYPTION_CREDENTIALS at 01:00:55, key re-enabled 01:35:08 UTC. GetItem(ConsistentRead) +
  DescribeTable every 15 s from 01:41:55 (+407 s) onward: GetItem succeeded on EVERY sample from the first one
  (+407 s) while TableStatus stayed INACCESSIBLE_ENCRYPTION_CREDENTIALS with InaccessibleEncryptionDateTime
  still set; TableStatus flipped to ACTIVE and InaccessibleEncryptionDateTime disappeared in the same poll at
  +3410 s (02:31:57). Several accepted UpdateTable calls on this table in the meantime (TableClass,
  BillingMode, stream, DP, WarmThroughput, OnDemandThroughput - see
  table/mutation-matrix/inaccessible-update-table) each produced a transient UPDATING that returned to
  INACCESSIBLE, not ACTIVE, and did not accelerate the recovery. A sibling table on another re-enabled key
  with zero traffic ('quiet') recovered in 18 min; the data-plane polling did not speed up the status flip.
  - ACK: synced.when, requeue · ops: GetItem, DescribeTable · fields: TableStatus,
    SSEDescription.InaccessibleEncryptionDateTime
  - repro: INACCESSIBLE table; kms EnableKey; GetItem + DescribeTable every 15 s
  - measurements: status_active_after_enable_s=3410, first_observed_get_item_ok_after_enable_s=407
  - handling: partially handled via `pkg/resource/table/hooks.go:60-65; pkg/resource/table/hooks.go:203-208` - see Handling gaps
  - related: [DDB-TABLE-331](../table-streams-encryption-class.md#ddb-table-331), [DDB-TABLE-335](../table-streams-encryption-class.md#ddb-table-335), [DDB-TABLE-332](../table-streams-encryption-class.md#ddb-table-332), [DDB-TABLE-334](../table-streams-encryption-class.md#ddb-table-334) · hypotheses: H-T-105, H-T-102 ·
    evidence: table/consistency-windows/inaccessible-recovery-lag

## Notes

H-T-105 recovery half: TableStatus/InaccessibleEncryptionDateTime are refreshed by a slow periodic check
(12-80 min observed for both directions in this session), not by data-plane or control-plane activity; a
controller should treat INACCESSIBLE_ENCRYPTION_CREDENTIALS as 'requeue with a long backoff' and must not
infer anything about the key from a successful GetItem. H-T-102: a table can be fully readable while still
flagged.

Partially handled in controller via: [GT-DDB-004](../service.md#gt-ddb-004) (controller hooks catalog entry): TerminalStatuses = [ARCHIVING, DELETING] -> customUpdateTable
sets ACK.Terminal and stops (hooks.go:60-65, 203-208); ARCHIVED is treated as synced (generator.ya (scorer
verdict: partial match)
