<!-- generated: ack-api-quirks dynamodb 2026-10-09 model=2012-08-10 controller=34b85e6 -->
# DDB-IMPORT-006: DeleteTable on an importing table: ResourceInUseException while CREATING; accepted once ACTIVE ~6s before the import flips COMPLETED
_Full entry and notes of one finding; its summary entry is in [import.md](../import.md). Generated from
ack-api-quirks `services/dynamodb` (model 2012-08-10, controller commit 34b85e6); edit the finding in the lab,
not here._

## Finding

- <a id="ddb-import-006"></a>**DDB-IMPORT-006** `delete-semantics` · impact high · handled · verified 2026-10-09
  **DeleteTable on an importing table: ResourceInUseException while CREATING; accepted once ACTIVE ~6s before the import flips COMPLETED**
  DeleteTable attempts from t+30s every 15s returned (elapsed_s, code, ImportStatus, TableStatus): [(30.8,
  'ResourceInUseException', 'IN_PROGRESS', 'CREATING'), (49.4, 'ResourceInUseException', 'IN_PROGRESS',
  'CREATING'), (68.0, 'ResourceInUseException', 'IN_PROGRESS', 'CREATING'), (86.4, 'ResourceInUseException',
  'IN_PROGRESS', 'CREATING'), (104.8, 'ResourceInUseException', 'IN_PROGRESS', 'CREATING'), (123.1,
  'ResourceInUseException', 'IN_PROGRESS', 'CREATING'), (141.5, 'ResourceInUseException', 'IN_PROGRESS',
  'CREATING'), (159.9, 'ResourceInUseException', 'IN_PROGRESS', 'CREATING'), (178.3, 'ResourceInUseException',
  'IN_PROGRESS', 'CREATING'), (196.9, 'ResourceInUseException', 'IN_PROGRESS', 'CREATING'), (215.2, 'ok',
  'IN_PROGRESS', 'ACTIVE')]. Distinct (ImportStatus, TableStatus) pairs observed for the import:
  [['IN_PROGRESS', 'ERR:ResourceNotFoundException'], ['IN_PROGRESS', 'ERR:ResourceNotFoundException'],
  ['IN_PROGRESS', 'CREATING'], ['IN_PROGRESS', 'CREATING'], ['IN_PROGRESS', 'CREATING'], ['IN_PROGRESS',
  'CREATING'], ['IN_PROGRESS', 'CREATING'], ['IN_PROGRESS', 'ACTIVE'], ['COMPLETED', 'DELETING']]; final
  DescribeImport status=COMPLETED FailureCode=None FailureMessage=; final
  DescribeTable=ResourceNotFoundException.
  - ACK: custom_delete, deletable.when, terminal_codes · ops: ImportTable, DeleteTable, DescribeImport ·
    fields: ImportTableDescription.ImportStatus, ImportTableDescription.FailureCode
  - repro: ImportTable (30 MB) -> from t+30s DeleteTable(target) every 15s -> poll
    DescribeImport/DescribeTable to terminal
  - measurements: t3_delete_accepted_at_s=215.2
  - handling: handled via `generator.yaml:104-109; pkg/resource/table/hooks.go:72-93`
  - related: [DDB-TABLE-277](../table-restore.md#ddb-table-277), [DDB-TABLE-298](../table-replicas.md#ddb-table-298) · hypotheses: H-B-034 · evidence: import/state-machine/lifecycle

## Notes

There is no cancel API for imports. CANCELLING/CANCELLED was NOT reached: the table turns ACTIVE a few seconds
before ImportStatus becomes COMPLETED and DeleteTable in that window is accepted while the import still ends
COMPLETED (ImportedItemCount=28500). H-B-034 refuted for the 'accepted while CREATING' part.
