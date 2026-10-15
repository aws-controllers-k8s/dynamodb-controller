<!-- generated: ack-api-quirks dynamodb 2026-10-09 model=2012-08-10 controller=34b85e6 -->
# DDB-TABLE-336: UNTESTED: 7-day archival path (ARCHIVING/ARCHIVED, ArchivalSummary, system backup) after INACCESSIBLE_ENCRYPTION_CREDENTIALS
_Full entry and notes of one finding; its summary entry is in
[table-streams-encryption-class.md](../table-streams-encryption-class.md). Generated from ack-api-quirks
`services/dynamodb` (model 2012-08-10, controller commit 34b85e6); edit the finding in the lab, not here._

## Finding

- <a id="ddb-table-336"></a>**DDB-TABLE-336** `async-state-machine` · impact medium · unhandled (not handled in controller) · verified 2026-10-09 · status: unverified
  **UNTESTED: 7-day archival path (ARCHIVING/ARCHIVED, ArchivalSummary, system backup) after INACCESSIBLE_ENCRYPTION_CREDENTIALS**
  Not exercised (needs the key to stay unusable for >7 days). Known from this probe: INACCESSIBLE is reached
  in 12.6 min and recovery takes 18-57 min; ArchivalSummary was absent in every DescribeTable during
  INACCESSIBLE ({'quiet': None, 'traf': None, 'del': None, 'pend': None, 'creating': None, 'grants': None}).
  AWS docs state the table moves to ARCHIVING then ARCHIVED with
  ArchivalSummary{ArchivalReason=INACCESSIBLE_ENCRYPTION_CREDENTIALS, ArchivalDateTime, ArchivalBackupArn} and
  that 'operations are not allowed until archival is complete'; whether DeleteTable is admitted during
  ARCHIVING, whether ARCHIVED tables keep their name, and whether the backup is restorable remain unverified.
  - ACK: synced.when, terminal_codes, docs-only · ops: DescribeTable, DeleteTable, DescribeBackup · fields:
    TableStatus, ArchivalSummary
  - repro: Keep a CMK disabled for >7 days after INACCESSIBLE_ENCRYPTION_CREDENTIALS; poll DescribeTable
    hourly
  - handling: not handled in the controller (as of commit 34b85e6)
  - related: [DDB-BACKUP-015](../backup.md#ddb-backup-015), [DDB-TABLE-167](../table-subresources.md#ddb-table-167), [DDB-BACKUP-014](../backup.md#ddb-backup-014), [DDB-BACKUP-006](../backup.md#ddb-backup-006) · hypotheses: H-T-101, H-T-104,
    H-T-106, H-T-107, H-T-147 · evidence: table/state-machine/kms-inaccessible-lifecycle

## Notes

Untested in this session (budget). Hypotheses H-T-106, H-T-107, H-T-147 and the archival halves of
H-T-101/H-T-104/H-T-136 stay unverified.
