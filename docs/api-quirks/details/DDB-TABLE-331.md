<!-- generated: ack-api-quirks dynamodb 2026-10-09 model=2012-08-10 controller=34b85e6 -->
# DDB-TABLE-331: CMK disabled -> INACCESSIBLE_ENCRYPTION_CREDENTIALS after 13-43 min, pending-deletion key after 75 min; data plane fails after ~5 min
_Full entry and notes of one finding; its summary entry is in
[table-streams-encryption-class.md](../table-streams-encryption-class.md). Generated from ack-api-quirks
`services/dynamodb` (model 2012-08-10, controller commit 34b85e6); edit the finding in the lab, not here._

## Finding

- <a id="ddb-table-331"></a>**DDB-TABLE-331** `async-state-machine` · impact high · partially handled · verified 2026-10-09
  **CMK disabled -> INACCESSIBLE_ENCRYPTION_CREDENTIALS after 13-43 min, pending-deletion key after 75 min; data plane fails after ~5 min**
  Five PPR tables on four CMKs, DescribeTable every 30 s. Minutes from the KMS action to the first poll
  showing TableStatus=INACCESSIBLE_ENCRYPTION_CREDENTIALS: DisableKey, zero traffic ('quiet'): 12.6;
  DisableKey, GetItem every 30 s ('traf'): 41.7; sibling table on the same disabled key, no traffic ('del'):
  36.2; RevokeGrant on the table's grants, key Enabled ('grants'): 43.2; ScheduleKeyDeletion(7 days) ('pend'):
  75.1 (InaccessibleEncryptionDateTime 01:34:36 for an action at 00:19:32). The two tables sharing one key
  flipped 5.5 min apart, so detection is per table, not per key, and traffic does not accelerate it. In every
  case the sequence was ACTIVE -> INACCESSIBLE_ENCRYPTION_CREDENTIALS with SSEDescription.Status still
  ENABLED, KMSMasterKeyArn unchanged and SSEDescription.InaccessibleEncryptionDateTime added (its value is
  20-30 s before the first poll that showed the status, i.e. it is the detection time, not the KMS action
  time); ArchivalSummary absent. Data plane on 'traf': GetItem kept succeeding for 5.5 min after DisableKey
  (cached data key), then failed on every call (120/120) with ValidationException 'KMS key disabled error:
  com.amazonaws.services.kms.model.DisabledException: arn:aws:kms:...:key/... is disabled. (Service: AWSKMS;
  Status Code: 400; Error Code: DisabledException; ...)' while TableStatus stayed ACTIVE for another 36 min.
  - ACK: synced.when, requeue, terminal_codes · ops: DescribeTable, GetItem · fields: TableStatus,
    SSEDescription.Status, SSEDescription.InaccessibleEncryptionDateTime
  - repro: CreateTable(SSESpecification KMS CMK) -> ACTIVE -> kms DisableKey | ScheduleKeyDeletion(7d) ->
    DescribeTable every 30s
  - measurements: detect_quiet_s=754.6, detect_traf_s=2504.5, detect_del_s=2172.7, detect_grants_s=2595.0,
    detect_pend_s=4504, get_item_first_failure_s=332.2
  - handling: partially handled via `pkg/resource/table/hooks.go:60-65; pkg/resource/table/hooks.go:203-208` - see Handling gaps
  - related: [DDB-TABLE-330](../table-streams-encryption-class.md#ddb-table-330), [DDB-TABLE-335](../table-streams-encryption-class.md#ddb-table-335), [DDB-TABLE-332](../table-streams-encryption-class.md#ddb-table-332), [DDB-TABLE-334](../table-streams-encryption-class.md#ddb-table-334) · hypotheses: H-T-101, H-T-102,
    H-T-105 · evidence: table/state-machine/kms-inaccessible-lifecycle

## Notes

H-T-101 first half: confirmed for the status/InaccessibleEncryptionDateTime shape, but the '<30 min' bound is
refuted (13-75 min, apparently a slow per-table periodic check). H-T-102: the only reliable signal that the
key is unusable is the data plane (fails within ~5 min) - TableStatus lags by up to 70 min and
SSEDescription.Status never leaves ENABLED. H-T-105 pending-deletion variant: behaves like a disabled key but
was the slowest to be detected.

Partially handled in controller via: [GT-DDB-004](../service.md#gt-ddb-004) (controller hooks catalog entry): TerminalStatuses = [ARCHIVING, DELETING] -> customUpdateTable
sets ACK.Terminal and stops (hooks.go:60-65, 203-208); ARCHIVED is treated as synced (generator.ya (scorer
verdict: partial match)
