<!-- generated: ack-api-quirks dynamodb 2026-10-09 model=2012-08-10 controller=34b85e6 -->
# DDB-TABLE-447: TableNotFoundException catalogue: three texts ('Table not found: <name>' / ': <ARN>' / bare) from the backup, PITR and export families...
_Full entry and notes of one finding; its summary entry is in [service.md](../service.md). Generated from
ack-api-quirks `services/dynamodb` (model 2012-08-10, controller commit 34b85e6); edit the finding in the lab,
not here._

## Finding

- <a id="ddb-table-447"></a>**DDB-TABLE-447** `error-code` · impact medium · unhandled (not handled in controller) · verified 2026-10-09
  **TableNotFoundException catalogue: three texts ('Table not found: <name>' / ': <ARN>' / bare) from the backup, PITR and export families...**
  TableNotFoundException (HTTP 400) is used only by the continuous-backups/backup/restore/export family.
  Texts: 'Table not found: <TBL>' (name-based calls), 'Table not found: <ARN>' (ExportTableToPointInTime
  echoes the ARN), bare 'Table not found' (RestoreTableToPointInTime with SourceTableArn, us-east-1). It is
  PERMANENT (missing table) when the table does not exist, but TRANSIENT-WAIT-FOR-STATE when the table is
  CREATING (CreateBackup/UpdateContinuousBackups right after CreateTable) and when a restore target is still
  CREATING - the text is identical in both cases, so a reconciler must consult DescribeTable before giving up.
  - ACK: exceptions.404, requeue · ops: CreateBackup, DescribeContinuousBackups, UpdateContinuousBackups,
    RestoreTableToPointInTime, ExportTableToPointInTime
  - repro: DescribeContinuousBackups/CreateBackup on a missing name; CreateBackup right after CreateTable;
    ExportTableToPointInTime with a missing ARN
  - handling: not handled in the controller (as of commit 34b85e6)
  - related: [DDB-TABLE-014](../table-policy-kinesis-autoscaling.md#ddb-table-014), [DDB-TABLE-074](../service.md#ddb-table-074), [DDB-BACKUP-001](../backup.md#ddb-backup-001), [DDB-TABLE-217](../table-restore.md#ddb-table-217), [DDB-TABLE-070](../service.md#ddb-table-070), [DDB-TABLE-098](../service.md#ddb-table-098),
    [DDB-TABLE-298](../table-replicas.md#ddb-table-298), [DDB-IMPORT-001](../import.md#ddb-import-001), [DDB-BACKUP-002](../backup.md#ddb-backup-002), [DDB-EXPORT-014](../export.md#ddb-export-014), [DDB-EXPORT-001](../export.md#ddb-export-001), [DDB-EXPORT-002](../export.md#ddb-export-002),
    [DDB-BACKUP-012](../backup.md#ddb-backup-012), [DDB-TABLE-274](../table-restore.md#ddb-table-274), [DDB-TABLE-273](../table-restore.md#ddb-table-273), [DDB-TABLE-272](../table-restore.md#ddb-table-272) · evidence:
    table/creative/error-message-regions

## Notes

Classes: RETRY = TRANSIENT-RETRY, clears by itself (typical wait given); WAIT = TRANSIENT-WAIT-FOR-STATE,
clears when the named status changes; SPEC = PERMANENT-SPEC, user must change the spec / fix an external
dependency; QUOTA = PERMANENT-QUOTA, needs a quota increase or the stated 1h/24h/30d window. Messages mined
from 119 past evidence.jsonl files (9562 error records, 29 codes; mine_errors.py -> mined-catalogue.txt in
this probe dir) plus this probe's live runs in us-west-2 and us-east-1; <TBL>/<ARN>/<TS>/<N>/<REGION>/<IDX>
mark request-specific tokens. Machine-readable form: catalogue.py in the probe directory.

CATALOGUE (TableNotFoundException):
- [WAIT ~PERMANENT if gone; WAIT (TableStatus=ACTIVE) while CREATING - identical text] Table not found: <TBL>
(ops: CreateBackup,DescribeContinuousBackups,UpdateContinuousBackups,RestoreTableToPointInTime)
- [SPEC] Table not found: <ARN> (ops: ExportTableToPointInTime; echoes the ARN that was sent)
- [SPEC] Table not found (ops: RestoreTableToPointInTime; bare text (SourceTableArn form, us-east-1))

LIVE this run (region/label: http code latency 'message'):
us-east-1/tnf_describe_cb: 400 TableNotFoundException 75ms 'Table not found: ackq-e3283a-emr-e1-missing'
us-west-2/tnf_describe_cb: 400 TableNotFoundException 10ms 'Table not found: ackq-e3283a-emr-w2-missing'
us-east-1/tnf_create_backup: 400 TableNotFoundException 63ms 'Table not found: ackq-e3283a-emr-e1-missing'
us-west-2/tnf_create_backup: 400 TableNotFoundException 6ms 'Table not found: ackq-e3283a-emr-w2-missing'
us-east-1/tnf_export_arn: 400 TableNotFoundException 111ms 'Table not found:
arn:aws:dynamodb:us-east-1:<ACCOUNT>:table/ackq-e3283a-emr-e1-missing'
us-west-2/tnf_export_arn: 400 TableNotFoundException 59ms 'Table not found:
arn:aws:dynamodb:us-west-2:<ACCOUNT>:table/ackq-e3283a-emr-w2-missing'
us-east-1/creating_create_backup: 400 TableNotFoundException 65ms 'Table not found: ackq-e3283a-emr-e1'
us-west-2/creating_create_backup: 400 TableNotFoundException 8ms 'Table not found: ackq-e3283a-emr-w2'
us-east-1/creating_update_cb_disable: 400 TableNotFoundException 84ms 'Table not found: ackq-e3283a-emr-e1'
us-west-2/creating_update_cb_disable: 400 TableNotFoundException 19ms 'Table not found: ackq-e3283a-emr-w2'
us-east-1/active_update_cb_disable: 400 ContinuousBackupsUnavailableException 84ms 'Backups are being enabled
for the table: ackq-e3283a-emr-e1. Please retry later'
us-west-2/active_update_cb_disable: 400 ContinuousBackupsUnavailableException 25ms 'Backups are being enabled
for the table: ackq-e3283a-emr-w2. Please retry later'
us-east-1/active_create_backup_early: 400 ContinuousBackupsUnavailableException 206ms 'Backups are being
enabled for the table: ackq-e3283a-emr-e1. Please retry later'
us-west-2/active_create_backup_early: 400 ContinuousBackupsUnavailableException 82ms 'Backups are being
enabled for the table: ackq-e3283a-emr-w2. Please retry later'

Contradiction with [DDB-EXPORT-014](../export.md#ddb-export-014), [DDB-BACKUP-002](../backup.md#ddb-backup-002): 014's title claims UpdateContinuousBackups right after
ACTIVE fails with ContinuousBackupsUnavailableException, but its data is attempts=1, window 0.0 s, code null
on both tables (no failure at all); 002 observed the error at +0.1 s and +3.2 s with success at +6.23 s for
CreateBackup and reports 3-6 s for UpdateContinuousBackups; 447's live rows reproduce it for both ops in two
regions Resolution: keep both; 002 canonical for the code/message; the post-ACTIVE window is 0-6 s and not
deterministic, so a controller must requeue on the code rather than sleep a fixed time (014 retitled)
