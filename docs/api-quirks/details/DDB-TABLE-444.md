<!-- generated: ack-api-quirks dynamodb 2026-10-09 model=2012-08-10 controller=34b85e6 -->
# DDB-TABLE-444: ResourceInUseException catalogue: 40+ texts, all transient except 'Table already exists'; global tables return a terse detail-free variant
_Full entry and notes of one finding; its summary entry is in [service.md](../service.md). Generated from
ack-api-quirks `services/dynamodb` (model 2012-08-10, controller commit 34b85e6); edit the finding in the lab,
not here._

## Finding

- <a id="ddb-table-444"></a>**DDB-TABLE-444** `error-code` · impact high · unhandled (not handled in controller) · verified 2026-10-09
  **ResourceInUseException catalogue: 40+ texts, all transient except 'Table already exists'; global tables return a terse detail-free variant**
  ResourceInUseException (HTTP 400) is TRANSIENT-WAIT-FOR-STATE for every text except 'Table already exists:
  <TBL>' (CreateTable/ImportTable duplicate: PERMANENT, adopt or rename) and 'Global table with name: '<TBL>'
  already exists with replicas in regions: ...' (PERMANENT-SPEC). Regional-table texts are 'Attempt to change
  a resource which is still in use: <detail>' where <detail> names the blocking state ('Table is being
  created: <TBL>', 'Table is being deleted: <TBL>', 'Table: <TBL> is in the process of being updated.', 'Table
  IOPS are currently being updated...', 'Cannot delete table while indexes are being created, updated, or
  deleted.', 'Index creation is in resource allocation phase. Retry deletion during backfilling phase or when
  the index is active...', 'Table tags are being updated: <TBL>' (~2 s), 'Table is pending previous
  resource-based policy update: <TBL>' (~2 s), 'Server-Side Encryption is still being updated', ...).
  UpdateTimeToLive uses a different word order ('Table <TBL> is being created') and sometimes the bare prefix
  'Attempt to change a resource which is still in use'. Tables that are members of a global table (replicas
  present) return the detail-free 'The resource which you are attempting to change is in use.' for UpdateTable
  and DeleteTable (27 records, all in replica probes), so the blocking state cannot be read from the message
  there.
  - ACK: requeue, synced.when, deletable.when · ops: UpdateTable, DeleteTable, CreateTable, TagResource,
    PutResourcePolicy, UpdateTimeToLive
  - repro: UpdateTable/DeleteTable in each state; see catalogue.py (mined from state-machine, cross-region and
    dependencies probes)
  - handling: not handled in the controller (as of commit 34b85e6)
  - related: [DDB-TABLE-005](../table.md#ddb-table-005), [DDB-TABLE-001](../table.md#ddb-table-001), [DDB-TABLE-150](../table-indexes.md#ddb-table-150), [DDB-TABLE-200](../table-replicas.md#ddb-table-200), [DDB-TABLE-219](../table-restore.md#ddb-table-219), [DDB-TABLE-275](../table-restore.md#ddb-table-275),
    [DDB-TABLE-100](../table-restore.md#ddb-table-100), [DDB-TABLE-217](../table-restore.md#ddb-table-217), [DDB-IMPORT-019](../import.md#ddb-import-019), [DDB-IMPORT-018](../import.md#ddb-import-018) · evidence:
    table/creative/error-message-regions

## Notes

Classes: RETRY = TRANSIENT-RETRY, clears by itself (typical wait given); WAIT = TRANSIENT-WAIT-FOR-STATE,
clears when the named status changes; SPEC = PERMANENT-SPEC, user must change the spec / fix an external
dependency; QUOTA = PERMANENT-QUOTA, needs a quota increase or the stated 1h/24h/30d window. Messages mined
from 119 past evidence.jsonl files (9562 error records, 29 codes; mine_errors.py -> mined-catalogue.txt in
this probe dir) plus this probe's live runs in us-west-2 and us-east-1; <TBL>/<ARN>/<TS>/<N>/<REGION>/<IDX>
mark request-specific tokens. Machine-readable form: catalogue.py in the probe directory.

CATALOGUE (ResourceInUseException):
- [SPEC] Table already exists: <TBL> (ops: CreateTable,ImportTable; duplicate create in
CREATING/ACTIVE/DELETING alike: adopt or rename; PERMANENT)
- [SPEC] Global table with name: '<TBL>' already exists with replicas in regions: '<REGION>, <REGION>'. (ops:
UpdateTable)
- [WAIT ~TableStatus=ACTIVE (~5 s no-index, minutes with GSIs/restores)] Attempt to change a resource which is
still in use: Table is being created: <TBL> (ops: CreateTable,UpdateTable,DeleteTable,TagResource)
- [WAIT ~TableStatus=ACTIVE] Attempt to change a resource which is still in use: Table <TBL> is being created
(ops: UpdateTimeToLive; different word order from the UpdateTable text)
- [WAIT ~TableStatus=ACTIVE] Attempt to change a resource which is still in use (ops: UpdateTimeToLive; bare
prefix, no detail)
- [WAIT ~table gone (~6 s) -> ResourceNotFound; then re-create if desired] Attempt to change a resource which
is still in use: Table is being deleted: <TBL> (ops: PutResourcePolicy,DeleteTable,TagResource,UpdateTable)
- [WAIT ~restore target CREATING -> ACTIVE/gone] Attempt to change a resource which is still in use: Table is
being used: <TBL> (ops: CreateTable)
- [WAIT ~replica CREATING/DELETING finishes] Attempt to change a resource which is still in use: Table: <TBL>
is being used. (ops: DeleteTable)
- [WAIT ~TableStatus=ACTIVE] Attempt to change a resource which is still in use: Table: <TBL> is in the
process of being updated. (ops: DeleteTable)
- [WAIT ~TableStatus=ACTIVE (~1-130 s)] Attempt to change a resource which is still in use: Table IOPS are
currently being updated. Table: <TBL> (ops: UpdateTable)
- [WAIT ~TableStatus=ACTIVE (~5-7 s)] Attempt to change a resource which is still in use: Can't change table
IOPS when stream status is being updated. Table: <TBL> Stream: <TS> Operation: ENABLE (ops: UpdateTable;
family 'Can't <X> when stream status is being updated ... Operation: ENABLE|DISABLE' (table class, index
create/delete); carries the stream label timestamp)
- [WAIT ~TableStatus=ACTIVE] Attempt to change a resource which is still in use: Can't enable or disable
stream while table IOPS are being updated. Table: <TBL> (ops: UpdateTable; family 'Can't <X> while table IOPS
are being updated' (stream, index create/delete))
- [WAIT ~TableStatus=ACTIVE] Attempt to change a resource which is still in use: A stream status change is
currently in progress. Table: <TBL> Stream: <TS> Operation: ENABLE (ops: UpdateTable)
- [WAIT ~TableStatus=ACTIVE] Attempt to change a resource which is still in use: Cannot delete table while
stream is being enabled/disabled. (ops: DeleteTable)
- [WAIT ~TableStatus=ACTIVE (~4 s)] Attempt to change a resource which is still in use: Can't update table
class when a table class update is in progress. Table: <TBL> TableClassUpdateInProgress: <CLASS> (ops:
UpdateTable; family 'Can't <X> when a table class update is in progress' (stream, index create/delete) and
'OnDemandThroughput cannot be updated while TableClass update is in progress...')
- [WAIT ~TableStatus=ACTIVE (up to ~130 s)] Attempt to change a resource which is still in use:
OnDemandThroughput cannot be updated while BillingMode update is in progress (ops: UpdateTable; family
'OnDemandThroughput cannot be updated while <stream status update|index deletion> is in progress...')
- [WAIT ~SSEDescription.Status=ENABLED (~22 s); TableStatus stays ACTIVE] Attempt to change a resource which
is still in use: Server-Side Encryption is still being updated (ops: UpdateTable)
- [WAIT ~all IndexStatus=ACTIVE (backfill 7-16 min)] Attempt to change a resource which is still in use:
Cannot delete table while indexes are being created, updated, or deleted. (ops: DeleteTable)
- [WAIT ~Backfilling=true (~25-55 s)] Attempt to change a resource which is still in use: Index creation is in
resource allocation phase. Retry deletion during backfilling phase or when the index is active. Table: <TBL>
Index: <IDX> (ops: UpdateTable)
- [WAIT ~Backfilling=true] Attempt to change a resource which is still in use: Index is being created but is
not backfilling yet. Table: <TBL> Index: <IDX> (ops: UpdateTable; double space)
- [WAIT ~IndexStatus=ACTIVE] Attempt to change a resource which is still in use: Can't change table IOPS when
an index is being created. Table: <TBL> Indexes: [<IDX>] (ops: UpdateTable; also 'Can't change stream status
when an index is being created', 'Can't change table IOPS when an index is being deleted')
- [WAIT ~index gone (~5 s)] Attempt to change a resource which is still in use: Index is being deleted. Table:
<TBL> Index: <IDX> (ops: UpdateTable; also 'Index is being updated. Table: <TBL> Index: <IDX>')
- [RETRY ~~2 s] Attempt to change a resource which is still in use: Table tags are being updated: <TBL> (ops:
DeleteTable; same lock is LimitExceededException for TagResource/UntagResource)
- [RETRY ~~1-2 s, then ThrottlingException until 15 s] Attempt to change a resource which is still in use:
Table is pending previous resource-based policy update: <TBL> (ops: PutResourcePolicy,DeleteResourcePolicy;
also 'Stream is pending previous resource-based policy update: <TS>')
- [RETRY ~~1-2 s] Attempt to change a resource which is still in use: Table: <TBL> has a pending
resource-based policy update. (ops: DeleteTable)
- [WAIT ~TableStatus=ACTIVE] Table needs to be in ACTIVE status to modify deletion protection setting. (ops:
UpdateTable)
- [WAIT ~TableStatus and every ReplicaStatus = ACTIVE] The resource which you are attempting to change is in
use. (ops: UpdateTable,DeleteTable; GLOBAL TABLES ONLY: detail-free variant returned instead of every 'Attempt
to change ...' text above (27 records, all on tables with replicas))

LIVE this run (region/label: http code latency 'message'):
us-east-1/creating_update_dp: 400 ResourceInUseException 70ms 'Attempt to change a resource which is still in
use: Table is being created: ackq-e3283a-emr-e1'
us-west-2/creating_update_dp: 400 ResourceInUseException 11ms 'Attempt to change a resource which is still in
use: Table is being created: ackq-e3283a-emr-w2'
us-east-1/creating_dup_create: 400 ResourceInUseException 70ms 'Attempt to change a resource which is still in
use: Table is being created: ackq-e3283a-emr-e1'
us-west-2/creating_dup_create: 400 ResourceInUseException 11ms 'Attempt to change a resource which is still in
use: Table is being created: ackq-e3283a-emr-w2'
us-east-1/creating_update_ttl: 400 ResourceNotFoundException 64ms 'Requested resource not found: Table:
ackq-e3283a-emr-e1 not found'
us-west-2/creating_update_ttl: 400 ResourceNotFoundException 6ms 'Requested resource not found: Table:
ackq-e3283a-emr-w2 not found'
us-east-1/riu_dup_create_active: 400 ResourceInUseException 69ms 'Table already exists: ackq-e3283a-emr-e1'
us-west-2/riu_dup_create_active: 400 ResourceInUseException 11ms 'Table already exists: ackq-e3283a-emr-w2'
us-east-1/riu_policy_put_pending: 400 ResourceInUseException 117ms 'Attempt to change a resource which is
still in use: Table is pending previous resource-based policy update: ackq-e3283a-emr-e1'
us-west-2/riu_policy_put_pending: 400 ResourceInUseException 62ms 'Attempt to change a resource which is still
in use: Table is pending previous resource-based policy update: ackq-e3283a-emr-w2'
us-east-1/riu_delete_during_tag_lock: 400 ResourceInUseException 68ms 'Attempt to change a resource which is
still in use: Table: ackq-e3283a-emr-e1 has a pending resource-based policy update.'
us-west-2/riu_delete_during_tag_lock: 400 ResourceInUseException 11ms 'Attempt to change a resource which is
still in use: Table: ackq-e3283a-emr-w2 has a pending resource-based policy update.'

The detail-free 'The resource which you are attempting to change is in use.' variant: 27 records, every one on
a table that had replicas (probes table/cross-region/*, table/dependencies/replica-*,
table/state-machine/replica-create-timeline, table/mutation-matrix/replica-overrides and
autoscaling-vs-throughput); never seen on a regional table.

Contradiction with [DDB-IMPORT-018](../import.md#ddb-import-018), [DDB-IMPORT-019](../import.md#ddb-import-019): 444 classes 'Table already exists: <TBL>' as the
ImportTable duplicate response for CREATING/ACTIVE/DELETING alike (PERMANENT); 018/019 show ImportTable into a
name whose import is still pre-visible (~30 s, DescribeTable ResourceNotFound) is accepted with a NEW
ImportArn and the loser fails asynchronously with FailureCode=TableAlreadyExists - the name is not reserved
synchronously Resolution: keep both; 444's clause holds for ACTIVE/DELETING (019) and once a CREATING table is
visible; 018/019 bound the exception window and add the async failure path
