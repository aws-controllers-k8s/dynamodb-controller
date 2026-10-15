<!-- generated: ack-api-quirks dynamodb 2026-10-09 model=2012-08-10 controller=34b85e6 -->
# DDB-TABLE-122: During DELETING, CreateBackup and an identical policy re-Put are 200 for ~1.6 s; a changed policy and every UpdateTable -> ResourceInUse
_Full entry and notes of one finding; its summary entry is in
[table-policy-kinesis-autoscaling.md](../table-policy-kinesis-autoscaling.md). Generated from ack-api-quirks
`services/dynamodb` (model 2012-08-10, controller commit 34b85e6); edit the finding in the lab, not here._

## Finding

- <a id="ddb-table-122"></a>**DDB-TABLE-122** `delete-semantics` · impact medium · unhandled (not handled in controller) · verified 2026-10-08
  **During DELETING, CreateBackup and an identical policy re-Put are 200 for ~1.6 s; a changed policy and every UpdateTable -> ResourceInUse**
  Ops issued while DescribeTable reported TableStatus=DELETING: {'stream_toggle': 'ResourceInUseException',
  'tableclass_ia': 'ResourceInUseException', 'sse_toggle': 'ResourceInUseException', 'create_backup': 'OK',
  'put_resource_policy': 'OK', 'ondemand_throughput': 'ResourceInUseException', 'warm_increase_again':
  'ResourceInUseException', 'dp_true': 'ResourceInUseException'}. CreateBackup returned BackupStatus=CREATING
  and the backup became AVAILABLE (0 bytes) after the table was gone; PutResourcePolicy returned a RevisionId.
  Backups later deleted: {'delete_backup.bc9bd6a9': 'OK', 'delete_backup.2e7ca1ed': 'OK'}.
  - ACK: pre-delete-cleanup, deletable.when · ops: CreateBackup, PutResourcePolicy, UpdateTable, DeleteTable
  - repro: DeleteTable; immediately CreateBackup(TableName) and PutResourcePolicy(ResourceArn)
  - handling: not handled in the controller (as of commit 34b85e6)
  - related: [DDB-TABLE-236](../table-policy-kinesis-autoscaling.md#ddb-table-236), [DDB-TABLE-374](../service.md#ddb-table-374), [DDB-TABLE-097](../table-subresources.md#ddb-table-097), [DDB-TABLE-453](../table-subresources.md#ddb-table-453), [DDB-TABLE-088](../table-subresources.md#ddb-table-088), [DDB-TABLE-235](../table-policy-kinesis-autoscaling.md#ddb-table-235),
    [DDB-TABLE-342](../table-subresources.md#ddb-table-342), [DDB-TABLE-214](../table-policy-kinesis-autoscaling.md#ddb-table-214) · evidence: table/state-machine/field-admissibility-while-updating

## Notes

A Backup resource reconciled in the same instant a Table is being deleted can succeed and outlive the table;
the controller cannot assume DELETING blocks all writes.

Contradiction with [DDB-TABLE-236](../table-policy-kinesis-autoscaling.md#ddb-table-236): 122 says PutResourcePolicy succeeds (200, RevisionId) during DELETING; 236
says ResourceInUseException 'Table is being deleted'. [DDB-TABLE-374](../service.md#ddb-table-374) reconciles: an IDENTICAL re-Put
(RevisionId no-op) is 200 for the first ~1.63 s of DELETING, a CHANGED document is ResourceInUse from +0.03 s
in 3/3 trials Resolution: keep both; 374 is canonical; 122's title overgeneralizes (see title_fixes)
