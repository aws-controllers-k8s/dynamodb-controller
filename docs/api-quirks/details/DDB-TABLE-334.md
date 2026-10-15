<!-- generated: ack-api-quirks dynamodb 2026-10-09 model=2012-08-10 controller=34b85e6 -->
# DDB-TABLE-334: DeleteTable on a table in INACCESSIBLE_ENCRYPTION_CREDENTIALS -> 200, TableStatus=DELETING, gone after 8 s
_Full entry and notes of one finding; its summary entry is in
[table-streams-encryption-class.md](../table-streams-encryption-class.md). Generated from ack-api-quirks
`services/dynamodb` (model 2012-08-10, controller commit 34b85e6); edit the finding in the lab, not here._

## Finding

- <a id="ddb-table-334"></a>**DDB-TABLE-334** `delete-semantics` · impact high · partially handled · verified 2026-10-09
  **DeleteTable on a table in INACCESSIBLE_ENCRYPTION_CREDENTIALS -> 200, TableStatus=DELETING, gone after 8 s**
  State before: {'status': 'INACCESSIBLE_ENCRYPTION_CREDENTIALS', 'sse_status': 'ENABLED', 'inaccessible_dt':
  '2026-10-09 00:55:39.143000+00:00', 'archival': None}. DeleteTable: {'ok': True, 'code': None,
  'http_status': 200, 'full_message': None}. Response TableStatus=DELETING, SSEDescription={'Status':
  'ENABLED', 'SSEType': 'KMS', 'KMSMasterKeyArn':
  'arn:aws:kms:us-west-2:<ACCOUNT>:key/74e12eb3-f7c5-41b7-a8c9-901ca9fdc45b',
  'InaccessibleEncryptionDateTime': '2026-10-09 00:55:39.143000+00:00'}. Removal timeline: [{'value':
  'DELETING', 'from_s': 0.01, 'to_s': 8.08, 'duration_s': 8.07}, {'value': 'ERR:ResourceNotFoundException',
  'from_s': 8.08, 'to_s': None, 'duration_s': None}]. The response echoes SSEDescription with
  InaccessibleEncryptionDateTime still present.
  - ACK: deletable.when, custom_delete · ops: DeleteTable, DescribeTable · fields: TableStatus
  - repro: CMK table, kms DisableKey, wait for INACCESSIBLE_ENCRYPTION_CREDENTIALS, DeleteTable, poll
    DescribeTable until ResourceNotFoundException
  - handling: partially handled via `pkg/resource/table/hooks.go:60-65; pkg/resource/table/hooks.go:203-208` - see Handling gaps
  - related: [DDB-TABLE-069](../table-streams-encryption-class.md#ddb-table-069), [DDB-TABLE-120](../table-streams-encryption-class.md#ddb-table-120), [DDB-TABLE-065](../table-streams-encryption-class.md#ddb-table-065), [DDB-TABLE-066](../table-throughput-billing.md#ddb-table-066), [DDB-TABLE-002](../table-streams-encryption-class.md#ddb-table-002), [DDB-TABLE-284](../table-streams-encryption-class.md#ddb-table-284),
    [DDB-TABLE-369](../table-streams-encryption-class.md#ddb-table-369), [DDB-TABLE-370](../table-throughput-billing.md#ddb-table-370), [DDB-TABLE-001](../table.md#ddb-table-001), [DDB-TABLE-331](../table-streams-encryption-class.md#ddb-table-331), [DDB-TABLE-330](../table-streams-encryption-class.md#ddb-table-330), [DDB-TABLE-335](../table-streams-encryption-class.md#ddb-table-335), [DDB-TABLE-332](../table-streams-encryption-class.md#ddb-table-332) ·
    hypotheses: H-T-104 · evidence: table/state-machine/kms-inaccessible-lifecycle

## Notes

H-T-104 first half confirmed. The ARCHIVING/ARCHIVED halves are untested (7-day path).

Partially handled in controller via: [GT-DDB-004](../service.md#gt-ddb-004) (controller hooks catalog entry): TerminalStatuses = [ARCHIVING, DELETING] -> customUpdateTable
sets ACK.Terminal and stops (hooks.go:60-65, 203-208); ARCHIVED is treated as synced (generator.ya (scorer
verdict: partial match)
