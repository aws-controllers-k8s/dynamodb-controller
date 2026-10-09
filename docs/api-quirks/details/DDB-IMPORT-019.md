<!-- generated: ack-api-quirks dynamodb 2026-10-09 model=2012-08-10 controller=34b85e6 -->
# DDB-IMPORT-019: ImportTable into a taken TableName: ACTIVE/DELETING -> ResourceInUseException; CREATING via another import -> accepted (new ImportArn)
_Full entry and notes of one finding; its summary entry is in [import.md](../import.md). Generated from
ack-api-quirks `services/dynamodb` (model 2012-08-10, controller commit 34b85e6); edit the finding in the lab,
not here._

## Finding

- <a id="ddb-import-019"></a>**DDB-IMPORT-019** `error-code` · impact high · unhandled (not handled in controller) · verified 2026-10-09
  **ImportTable into a taken TableName: ACTIVE/DELETING -> ResourceInUseException; CREATING via another import -> accepted (new ImportArn)**
  ImportTable(TableName=<ACTIVE table>) -> ResourceInUseException (HTTP 400) 'Table already exists:
  ackq-641e07-tbl-b'. Same TableName while another import is creating it: no token -> 200 NEW import ARN
  (IN_PROGRESS) ''; other token -> 200 NEW import ARN (IN_PROGRESS). TableName of a table in DELETING ->
  ResourceInUseException 'Table already exists: ackq-641e07-tbl-b'; after it is gone -> 200 NEW import ARN
  (IN_PROGRESS). Token-less replay after the import COMPLETED -> ResourceInUseException.
  - ACK: terminal_codes, custom_create, exceptions.404 · ops: ImportTable, CreateTable, DeleteTable · fields:
    TableCreationParameters.TableName
  - repro: CreateTable TB -> ImportTable(TableName=TB); DeleteTable TB -> ImportTable(TableName=TB) while
    DELETING and after gone
  - handling: not handled in the controller (as of commit 34b85e6)
  - related: [DDB-TABLE-219](../table-restore.md#ddb-table-219), [DDB-TABLE-275](../table-restore.md#ddb-table-275), [DDB-TABLE-100](../table-restore.md#ddb-table-100), [DDB-TABLE-217](../table-restore.md#ddb-table-217), [DDB-TABLE-444](../service.md#ddb-table-444), [DDB-IMPORT-018](../import.md#ddb-import-018),
    [DDB-IMPORT-017](../import.md#ddb-import-017), [DDB-IMPORT-001](../import.md#ddb-import-001) · hypotheses: H-B-036, H-B-018 · evidence: import/idempotency/client-token

## Notes

Contradiction with [DDB-TABLE-444](../service.md#ddb-table-444), [DDB-IMPORT-018](../import.md#ddb-import-018): 444 classes 'Table already exists: <TBL>' as the ImportTable
duplicate response for CREATING/ACTIVE/DELETING alike (PERMANENT); 018/019 show ImportTable into a name whose
import is still pre-visible (~30 s, DescribeTable ResourceNotFound) is accepted with a NEW ImportArn and the
loser fails asynchronously with FailureCode=TableAlreadyExists - the name is not reserved synchronously
Resolution: keep both; 444's clause holds for ACTIVE/DELETING (019) and once a CREATING table is visible;
018/019 bound the exception window and add the async failure path
