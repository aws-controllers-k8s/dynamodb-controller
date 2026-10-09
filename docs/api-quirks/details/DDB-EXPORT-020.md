<!-- generated: ack-api-quirks dynamodb 2026-10-09 model=2012-08-10 controller=34b85e6 -->
# DDB-EXPORT-020: Export records outlive the source table (DescribeExport ok) but ListExports(TableArn) returns 0 once it is deleted or re-created by name
_Full entry and notes of one finding; its summary entry is in [export.md](../export.md). Generated from
ack-api-quirks `services/dynamodb` (model 2012-08-10, controller commit 34b85e6); edit the finding in the lab,
not here._

## Finding

- <a id="ddb-export-020"></a>**DDB-EXPORT-020** `identity` · impact medium · unhandled (not handled in controller) · verified 2026-10-09
  **Export records outlive the source table (DescribeExport ok) but ListExports(TableArn) returns 0 once it is deleted or re-created by name**
  After DeleteTable: DescribeExport -> ok (TableArn/TableId still present: True), ListExports(TableArn) -> 0
  entries. After re-creating a table with the same name (new TableId 5dec6036-34c3-43dd-ae21-b7de6c44e66c vs
  export TableId 8dee4e27-6be3-4358-9987-8b55b06a732e) ListExports(TableArn) -> 0 entries; ExportSummary
  carries TableId: False.
  - ACK: custom_find, list_operation.match_fields · ops: DeleteTable, DescribeExport, ListExports, CreateTable
  - repro: Export -> COMPLETED -> DeleteTable -> DescribeExport/ListExports(TableArn) -> CreateTable same name
    -> ListExports(TableArn)
  - handling: not handled in the controller (as of commit 34b85e6)
  - related: [DDB-TABLE-216](../table-restore.md#ddb-table-216), [DDB-IMPORT-005](../import.md#ddb-import-005), [DDB-TABLE-278](../table-restore.md#ddb-table-278), [DDB-IMPORT-008](../import.md#ddb-import-008), [DDB-EXPORT-013](../export.md#ddb-export-013), [DDB-TABLE-454](../table-subresources.md#ddb-table-454),
    [DDB-EXPORT-021](../export.md#ddb-export-021), [DDB-EXPORT-022](../export.md#ddb-export-022) · hypotheses: H-B-142, H-B-038 · evidence: export/state-machine/lifecycle

## Notes

Contradiction with [DDB-EXPORT-022](../export.md#ddb-export-022), [DDB-IMPORT-008](../import.md#ddb-import-008): 020's title says export records are 'keyed by the
name-based TableArn', yet its behavior shows ListExports(TableArn) -> 0 entries immediately after DeleteTable
and still 0 after a same-name re-create (same ARN string, new TableId); 022 confirms ListExports(TableArn) ->
0 after DeleteTable while DescribeExport still works; 008 shows the opposite for imports:
ListImports(TableArn) still returns the import after DeleteTable Resolution: retitle 020 (title_fixes); keep
022/008; an Export reconciler must ReadOne via DescribeExport(ExportArn) and never rely on
ListExports(TableArn) after the source is gone
