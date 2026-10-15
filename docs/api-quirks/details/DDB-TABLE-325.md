<!-- generated: ack-api-quirks dynamodb 2026-10-09 model=2012-08-10 controller=34b85e6 -->
# DDB-TABLE-325: While INACCESSIBLE_ENCRYPTION_CREDENTIALS, UpdateTable BillingMode/TableClass/OnDemand/Warm/stream/DP are all accepted and applied
_Full entry and notes of one finding; its summary entry is in
[table-streams-encryption-class.md](../table-streams-encryption-class.md). Generated from ack-api-quirks
`services/dynamodb` (model 2012-08-10, controller commit 34b85e6); edit the finding in the lab, not here._

## Finding

- <a id="ddb-table-325"></a>**DDB-TABLE-325** `async-state-machine` · impact high · unhandled (not handled in controller) · verified 2026-10-09
  **While INACCESSIBLE_ENCRYPTION_CREDENTIALS, UpdateTable BillingMode/TableClass/OnDemand/Warm/stream/DP are all accepted and applied**
  Target: CMK-encrypted PPR table whose key had been disabled ~65 min earlier
  (TableStatus=INACCESSIBLE_ENCRYPTION_CREDENTIALS, SSEDescription.Status=ENABLED; KMS key verified Disabled
  throughout). UpdateTable with one field at a time: WarmThroughput 12001/4001 -> OK (status stays
  INACCESSIBLE); OnDemandThroughput -> OK; TableClass=STANDARD_INFREQUENT_ACCESS -> OK, TableStatus=UPDATING
  for 4 s then back to INACCESSIBLE_ENCRYPTION_CREDENTIALS (not ACTIVE); BillingMode=PROVISIONED 1/1 -> OK,
  UPDATING 78.5 s then INACCESSIBLE again; StreamSpecification disable -> OK, UPDATING 4 s;
  DeletionProtectionEnabled=false -> OK. DescribeTable afterwards shows every change applied (PROVISIONED, IA,
  stream absent, DP false). Only KMS-dependent calls fail in this state (SSESpecification changes,
  UpdateTimeToLive, CreateBackup, UpdateContributorInsights, GetItem/PutItem -> ValidationException 'KMS key
  disabled error: ...DisabledException', see table/state-machine/kms-inaccessible-lifecycle).
  - ACK: updateable.when, synced.when, terminal_codes · ops: UpdateTable, DescribeTable · fields: BillingMode,
    TableClass, OnDemandThroughput, WarmThroughput, StreamSpecification, DeletionProtectionEnabled
  - repro: CMK table; kms DisableKey; wait for INACCESSIBLE_ENCRYPTION_CREDENTIALS; UpdateTable with each
    field alone
  - handling: not handled in the controller (as of commit 34b85e6)
  - related: [DDB-TABLE-333](../table-streams-encryption-class.md#ddb-table-333) · hypotheses: H-T-103 · evidence: table/mutation-matrix/inaccessible-update-table

## Notes

REFUTES H-T-103 (every UpdateTable rejected while INACCESSIBLE): only SSESpecification changes are refused;
all other table-level updates go through, each with a transient UPDATING that returns to
INACCESSIBLE_ENCRYPTION_CREDENTIALS rather than ACTIVE. A controller must therefore not use
TableStatus==ACTIVE as its post-update settle condition for such tables, and must expect INACCESSIBLE as a
terminal value of the UPDATING phase. Completes the matrix of table/state-machine/kms-inaccessible-lifecycle,
where BillingMode/TableClass/OnDemandThroughput were ResourceInUse only because a stream enable was in flight.
