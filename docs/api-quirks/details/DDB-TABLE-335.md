<!-- generated: ack-api-quirks dynamodb 2026-10-09 model=2012-08-10 controller=34b85e6 -->
# DDB-TABLE-335: Re-enabling the CMK: ACTIVE again after 18 / 44 / 57 min (3 tables), InaccessibleEncryptionDateTime cleared; CancelKeyDeletion alone no help
_Full entry and notes of one finding; its summary entry is in
[table-streams-encryption-class.md](../table-streams-encryption-class.md). Generated from ack-api-quirks
`services/dynamodb` (model 2012-08-10, controller commit 34b85e6); edit the finding in the lab, not here._

## Finding

- <a id="ddb-table-335"></a>**DDB-TABLE-335** `async-state-machine` · impact high · partially handled · verified 2026-10-09
  **Re-enabling the CMK: ACTIVE again after 18 / 44 / 57 min (3 tables), InaccessibleEncryptionDateTime cleared; CancelKeyDeletion alone no help**
  EnableKey on the disabled CMKs (30 s polling): 'quiet' ACTIVE after 1087.7 s (18.1 min); 'traf' after 3410 s
  (56.8 min, measured by table/consistency-windows/inaccessible-recovery-lag because it exceeded this probe's
  45-min recovery budget); 'pend' (ScheduleKeyDeletion'd key): CancelKeyDeletion leaves KeyState=Disabled and
  the table stayed INACCESSIBLE_ENCRYPTION_CREDENTIALS for the following 45.3 min; EnableKey then -> ACTIVE
  after 2654.2 s (44.2 min). In every case the sequence was INACCESSIBLE_ENCRYPTION_CREDENTIALS -> ACTIVE in
  one step with SSEDescription.InaccessibleEncryptionDateTime removed in the same poll (cleared, not frozen);
  SSEDescription afterwards is {Status: ENABLED, SSEType: KMS, KMSMasterKeyArn: <same key>}. No DynamoDB API
  call was needed.
  - ACK: synced.when, requeue, is_read_only · ops: DescribeTable · fields: TableStatus,
    SSEDescription.InaccessibleEncryptionDateTime
  - repro: INACCESSIBLE table -> kms EnableKey (or CancelKeyDeletion then EnableKey) -> DescribeTable every
    30s
  - measurements: recover_quiet_s=1087.7, recover_traf_s=3410, recover_pend_after_enable_s=2654.2,
    pend_cancel_only_no_recovery_observed_min=45.3
  - handling: partially handled via `pkg/resource/table/hooks.go:60-65; pkg/resource/table/hooks.go:203-208` - see Handling gaps
  - related: [DDB-TABLE-331](../table-streams-encryption-class.md#ddb-table-331), [DDB-TABLE-330](../table-streams-encryption-class.md#ddb-table-330), [DDB-TABLE-332](../table-streams-encryption-class.md#ddb-table-332), [DDB-TABLE-334](../table-streams-encryption-class.md#ddb-table-334) · hypotheses: H-T-105 · evidence:
    table/state-machine/kms-inaccessible-lifecycle

## Notes

H-T-105 recovery half confirmed except for the '<30 min' bound: 18-57 min observed, i.e. the same slow
periodic check as detection. A controller can only requeue; InaccessibleEncryptionDateTime must not be
persisted as a permanent field. The 'archival clock restarts' half is untested.

Partially handled in controller via: [GT-DDB-004](../service.md#gt-ddb-004) (controller hooks catalog entry): TerminalStatuses = [ARCHIVING, DELETING] -> customUpdateTable
sets ACK.Terminal and stops (hooks.go:60-65, 203-208); ARCHIVED is treated as synced (generator.ya (scorer
verdict: partial match)
