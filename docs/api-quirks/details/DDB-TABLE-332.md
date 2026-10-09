<!-- generated: ack-api-quirks dynamodb 2026-10-09 model=2012-08-10 controller=34b85e6 -->
# DDB-TABLE-332: CMK disabled while CREATING: table stuck CREATING ~59 min then silently vanishes; revoked grants -> INACCESSIBLE in 43 min, repairable
_Full entry and notes of one finding; its summary entry is in
[table-streams-encryption-class.md](../table-streams-encryption-class.md). Generated from ack-api-quirks
`services/dynamodb` (model 2012-08-10, controller commit 34b85e6); edit the finding in the lab, not here._

## Finding

- <a id="ddb-table-332"></a>**DDB-TABLE-332** `async-state-machine` · impact medium · partially handled · verified 2026-10-09
  **CMK disabled while CREATING: table stuck CREATING ~59 min then silently vanishes; revoked grants -> INACCESSIBLE in 43 min, repairable**
  'creating': CreateTable(CMK) returned TableStatus=CREATING; DisableKey ~0.3 s later. DescribeTable (30 s
  polls) showed CREATING for 59.3 min (a plain PPR table normally takes ~8 s), then ResourceNotFoundException:
  the table was removed by the service with no API error ever returned to the caller and never reached ACTIVE
  or INACCESSIBLE_ENCRYPTION_CREDENTIALS. Re-enabling the key 16 min after the disappearance did not bring
  anything back. 'grants': the CMK had two DynamoDB-created grants (GranteePrincipal
  dynamodb.us-west-2.amazonaws.com, Operations
  Decrypt/Encrypt/GenerateDataKey/ReEncryptFrom/ReEncryptTo/RetireGrant/DescribeKey, EncryptionContextSubset
  aws:dynamodb:tableName + aws:dynamodb:subscriberId); RevokeGrant on both (key left Enabled) -> ACTIVE ->
  INACCESSIBLE_ENCRYPTION_CREDENTIALS after 43.2 min with InaccessibleEncryptionDateTime set. In that state
  UpdateTable(SSESpecification{Enabled,KMS,KMSMasterKeyId=<another enabled CMK>}) -> 200 (TableStatus stayed
  INACCESSIBLE, SSEDescription.Status=UPDATING) and 30 s later the table was ACTIVE with
  InaccessibleEncryptionDateTime cleared and the new key in place.
  - ACK: synced.when, requeue, terminal_codes · ops: CreateTable, DescribeTable, UpdateTable · fields:
    TableStatus, SSESpecification.KMSMasterKeyId
  - repro: CreateTable with CMK then kms DisableKey within 1s; separately kms ListGrants/RevokeGrant for the
    table's grants; poll DescribeTable
  - measurements: creating_vanished_after_s=3560.3, grants_detect_s=2595.0,
    grants_recover_via_sse_switch_s=30.4
  - handling: partially handled via `pkg/resource/table/hooks.go:60-65; pkg/resource/table/hooks.go:203-208` - see Handling gaps
  - related: [DDB-TABLE-331](../table-streams-encryption-class.md#ddb-table-331), [DDB-TABLE-330](../table-streams-encryption-class.md#ddb-table-330), [DDB-TABLE-335](../table-streams-encryption-class.md#ddb-table-335), [DDB-TABLE-334](../table-streams-encryption-class.md#ddb-table-334) · hypotheses: H-T-148, H-T-143 ·
    evidence: table/state-machine/kms-inaccessible-lifecycle

## Notes

H-T-148: refuted in detail - the table neither reaches ACTIVE nor INACCESSIBLE; it is garbage-collected after
~1 h, so a controller waiting for ACTIVE after CreateTable must also exit on ResourceNotFoundException (the
table it created is gone) and report the KMS cause. H-T-143: confirmed that revoking the grants flips the
table within ~45 min; its contrarian 'never recoverable' branch is refuted: because the old key itself is
still usable, UpdateTable to another CMK re-encrypts and heals the table in 30 s (contrast: with the old key
DISABLED the same call is ValidationException 'KMS key disabled error', see the op-matrix finding).

Partially handled in controller via: [GT-DDB-004](../service.md#gt-ddb-004) (controller hooks catalog entry): TerminalStatuses = [ARCHIVING, DELETING] -> customUpdateTable
sets ACK.Terminal and stops (hooks.go:60-65, 203-208); ARCHIVED is treated as synced (generator.ya (scorer
verdict: partial match)
