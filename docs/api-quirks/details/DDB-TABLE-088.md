<!-- generated: ack-api-quirks dynamodb 2026-10-09 model=2012-08-10 controller=34b85e6 -->
# DDB-TABLE-088: SYSTEM backup on delete of a PITR table is suppressed when UpdateContinuousBackups(disable) is issued while DELETING
_Full entry and notes of one finding; its summary entry is in
[table-subresources.md](../table-subresources.md). Generated from ack-api-quirks `services/dynamodb` (model
2012-08-10, controller commit 34b85e6); edit the finding in the lab, not here._

## Finding

- <a id="ddb-table-088"></a>**DDB-TABLE-088** `delete-semantics` · impact medium · unhandled (not handled in controller) · verified 2026-10-08
  **SYSTEM backup on delete of a PITR table is suppressed when UpdateContinuousBackups(disable) is issued while DELETING**
  ListBackups(BackupType=SYSTEM) after DeleteTable: PITR-on table -> None; PITR-off table -> None.
  DeleteBackup on the system backup -> n/a.
  - ACK: pre-delete-cleanup, docs-only · ops: DeleteTable, ListBackups, DeleteBackup
  - repro: enable PITR; DeleteTable; poll ListBackups(BackupType=SYSTEM) 5 min
  - handling: not handled in the controller (as of commit 34b85e6)
  - related: [DDB-TABLE-113](../table-subresources.md#ddb-table-113), [DDB-TABLE-122](../table-policy-kinesis-autoscaling.md#ddb-table-122), [DDB-TABLE-236](../table-policy-kinesis-autoscaling.md#ddb-table-236), [DDB-TABLE-374](../service.md#ddb-table-374), [DDB-TABLE-097](../table-subresources.md#ddb-table-097), [DDB-TABLE-453](../table-subresources.md#ddb-table-453),
    [DDB-TABLE-235](../table-policy-kinesis-autoscaling.md#ddb-table-235), [DDB-TABLE-342](../table-subresources.md#ddb-table-342), [DDB-TABLE-214](../table-policy-kinesis-autoscaling.md#ddb-table-214) · evidence: table/sub-resources/pitr-lifecycle

## Notes

Hypotheses: H-S-131. Re-interpreted after table/creative/pitr-delete-system-backup: this probe (and
table/error-taxonomy/subresource-errors) issued UpdateContinuousBackups(PointInTimeRecoveryEnabled=false)
while the table was DELETING (accepted, 200) and no SYSTEM backup ever appeared; a plain delete of a PITR
table produces '<table>$DeletedTableBackup' within ~8 s. So the 'no backup' here is a real quirk, not a
refutation.
