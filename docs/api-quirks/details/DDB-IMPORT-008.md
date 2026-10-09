<!-- generated: ack-api-quirks dynamodb 2026-10-09 model=2012-08-10 controller=34b85e6 -->
# DDB-IMPORT-008: Import records outlive the imported table: DescribeImport/ListImports(TableArn) after DeleteTable
_Full entry and notes of one finding; its summary entry is in [import.md](../import.md). Generated from
ack-api-quirks `services/dynamodb` (model 2012-08-10, controller commit 34b85e6); edit the finding in the lab,
not here._

## Finding

- <a id="ddb-import-008"></a>**DDB-IMPORT-008** `identity` · impact medium · unhandled (not handled in controller) · verified 2026-10-09
  **Import records outlive the imported table: DescribeImport/ListImports(TableArn) after DeleteTable**
  After the imported table was deleted (DescribeTable -> ResourceNotFoundException), DescribeImport -> ok
  (status COMPLETED, TableArn/TableId still present), ListImports(TableArn) -> 1 entries, paginated
  ListImports() still contains the import: True.
  - ACK: custom_find, exceptions.404 · ops: DeleteTable, DescribeImport, ListImports
  - repro: ImportTable -> COMPLETED -> DeleteTable -> wait ResourceNotFoundException -> DescribeImport /
    ListImports(TableArn)
  - handling: not handled in the controller (as of commit 34b85e6)
  - related: [DDB-TABLE-216](../table-restore.md#ddb-table-216), [DDB-IMPORT-005](../import.md#ddb-import-005), [DDB-TABLE-278](../table-restore.md#ddb-table-278), [DDB-EXPORT-020](../export.md#ddb-export-020), [DDB-EXPORT-022](../export.md#ddb-export-022) · hypotheses:
    H-B-142 · evidence: import/state-machine/lifecycle

## Notes

Contradiction with [DDB-EXPORT-020](../export.md#ddb-export-020), [DDB-EXPORT-022](../export.md#ddb-export-022): 020's title says export records are 'keyed by the
name-based TableArn', yet its behavior shows ListExports(TableArn) -> 0 entries immediately after DeleteTable
and still 0 after a same-name re-create (same ARN string, new TableId); 022 confirms ListExports(TableArn) ->
0 after DeleteTable while DescribeExport still works; 008 shows the opposite for imports:
ListImports(TableArn) still returns the import after DeleteTable Resolution: retitle 020 (title_fixes); keep
022/008; an Export reconciler must ReadOne via DescribeExport(ExportArn) and never rely on
ListExports(TableArn) after the source is gone
