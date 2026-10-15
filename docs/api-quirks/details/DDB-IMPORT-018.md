<!-- generated: ack-api-quirks dynamodb 2026-10-09 model=2012-08-10 controller=34b85e6 -->
# DDB-IMPORT-018: Concurrent ImportTable calls for the same TableName are all accepted; losers end FAILED with FailureCode=TableAlreadyExists
_Full entry and notes of one finding; its summary entry is in [import.md](../import.md). Generated from
ack-api-quirks `services/dynamodb` (model 2012-08-10, controller commit 34b85e6); edit the finding in the lab,
not here._

## Finding

- <a id="ddb-import-018"></a>**DDB-IMPORT-018** `idempotency` · impact high · unhandled (not handled in controller) · verified 2026-10-09
  **Concurrent ImportTable calls for the same TableName are all accepted; losers end FAILED with FailureCode=TableAlreadyExists**
  Three ImportTable calls for one TableName within 1s (token T, no token, token T2) all returned 200 with
  distinct ImportArns while none of the tables existed yet. Outcome per call: {'1_first': ('FAILED',
  'TableAlreadyExists', '2026-10-09T00:25:48.913000+00:00'), '5_no_token_same_table_name': ('COMPLETED', None,
  '2026-10-09T00:26:40.259000+00:00'), '6_other_token_same_table_name': ('FAILED', 'TableAlreadyExists',
  '2026-10-09T00:25:23.904000+00:00')}. Winner: ['5_no_token_same_table_name']. ImportTable does not reserve
  the table name synchronously.
  - ACK: custom_create, terminal_codes, synced.when · ops: ImportTable, DescribeImport · fields:
    TableCreationParameters.TableName, ImportTableDescription.FailureCode
  - repro: ImportTable x3 for the same TableName within 1s -> poll DescribeImport for each to terminal
  - handling: not handled in the controller (as of commit 34b85e6)
  - related: [DDB-TABLE-219](../table-restore.md#ddb-table-219), [DDB-TABLE-275](../table-restore.md#ddb-table-275), [DDB-TABLE-100](../table-restore.md#ddb-table-100), [DDB-TABLE-217](../table-restore.md#ddb-table-217), [DDB-TABLE-444](../service.md#ddb-table-444), [DDB-IMPORT-019](../import.md#ddb-import-019),
    [DDB-IMPORT-017](../import.md#ddb-import-017), [DDB-IMPORT-001](../import.md#ddb-import-001) · hypotheses: H-B-036, H-B-018 · evidence: import/idempotency/client-token

## Notes

Refutes the synchronous ResourceInUseException expectation for the CREATING-via-another-import case; the
first-submitted import is not guaranteed to win.

Contradiction with [DDB-TABLE-444](../service.md#ddb-table-444), [DDB-IMPORT-019](../import.md#ddb-import-019): 444 classes 'Table already exists: <TBL>' as the ImportTable
duplicate response for CREATING/ACTIVE/DELETING alike (PERMANENT); 018/019 show ImportTable into a name whose
import is still pre-visible (~30 s, DescribeTable ResourceNotFound) is accepted with a NEW ImportArn and the
loser fails asynchronously with FailureCode=TableAlreadyExists - the name is not reserved synchronously
Resolution: keep both; 444's clause holds for ACTIVE/DELETING (019) and once a CREATING table is visible;
018/019 bound the exception window and add the async failure path
