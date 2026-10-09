<!-- generated: ack-api-quirks dynamodb 2026-10-09 model=2012-08-10 controller=34b85e6 -->
# DDB-EXPORT-013: Deleting the source ~1 s after ExportTableToPointInTime is accepted: DeleteTable 200; export outcome unobserved (poll throttled)
_Full entry and notes of one finding; its summary entry is in [export.md](../export.md). Generated from
ack-api-quirks `services/dynamodb` (model 2012-08-10, controller commit 34b85e6); edit the finding in the lab,
not here._

## Finding

- <a id="ddb-export-013"></a>**DDB-EXPORT-013** `delete-semantics` · impact medium · unhandled (not handled in controller) · verified 2026-10-09
  **Deleting the source ~1 s after ExportTableToPointInTime is accepted: DeleteTable 200; export outcome unobserved (poll throttled)**
  ExportTableToPointInTime(T2) then DeleteTable(T2) ~1s later -> ok (TableStatus DELETING). Table gone after
  15.8s while the export was ERR:ThrottlingException. Export final: None, ItemCount=None, FailureMessage='';
  DescribeExport still returns TableArn/TableId: False.
  - ACK: pre-delete-cleanup, references, docs-only · ops: ExportTableToPointInTime, DeleteTable,
    DescribeExport
  - repro: Export table T2 -> DeleteTable T2 immediately -> poll DescribeExport to terminal
  - handling: not handled in the controller (as of commit 34b85e6)
  - related: [DDB-TABLE-454](../table-subresources.md#ddb-table-454), [DDB-EXPORT-020](../export.md#ddb-export-020), [DDB-EXPORT-021](../export.md#ddb-export-021), [DDB-EXPORT-022](../export.md#ddb-export-022) · hypotheses: H-B-027 · evidence:
    export/idempotency/client-token

## Notes

Contradiction with [DDB-TABLE-454](../table-subresources.md#ddb-table-454): 013 titles 'export ends None' for an export whose source was deleted ~1 s
after acceptance, but its own behavior shows the poll loop was throttled (ERR:ThrottlingException) and no
terminal status was ever read; 454 watched an export started +0.04 s into DELETING reach COMPLETED with
ItemCount=1 and the manifest written after the table was gone Resolution: keep both; 454 is canonical; 013's
outcome is unobserved, not None (retitled in title_fixes)
