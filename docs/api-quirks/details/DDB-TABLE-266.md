<!-- generated: ack-api-quirks dynamodb 2026-10-09 model=2012-08-10 controller=34b85e6 -->
# DDB-TABLE-266: DeleteTable on a table with replicas is rejected while a replica it sourced (<24h) exists; the stated reason is the 24h source-region rule
_Full entry and notes of one finding; its summary entry is in [table-replicas.md](../table-replicas.md).
Generated from ack-api-quirks `services/dynamodb` (model 2012-08-10, controller commit 34b85e6); edit the
finding in the lab, not here._

## Finding

- <a id="ddb-table-266"></a>**DDB-TABLE-266** `delete-semantics` · impact high · handled · verified 2026-10-09
  **DeleteTable on a table with replicas is rejected while a replica it sourced (<24h) exists; the stated reason is the 24h source-region rule**
  3-region group (us-east-1 added from us-west-2 at T+0, eu-west-1 added from the us-east-1 endpoint at
  T+11min). DeleteTable in us-west-2 while both replicas were ACTIVE -> ValidationException (HTTP 400)
  'Replica cannot be deleted because it has acted as a source region for new replica(s) being added to the
  table in the last 24 hours.' The same message was returned 13 min later while the last remaining replica
  (us-east-1, sourced from us-west-2) was DELETING. Base-table deletion only succeeded once Replicas[] was
  empty. DeleteTable issued in us-east-1 (a replica, while itself DELETING) -> ResourceInUseException 'The
  resource which you are attempting to change is in use.'
  - ACK: custom_delete, pre-delete-cleanup, deletable.when, requeue · ops: DeleteTable · fields: Replicas
  - repro: 3-region table -> DeleteTable in the base region
  - handling: handled via `templates/hooks/table/sdk_delete_pre_build_request.go.tpl:8-22; pkg/resource/table/hooks_replica_updates.go:256-263`
  - related: [DDB-TABLE-262](../table-replicas.md#ddb-table-262), [DDB-TABLE-267](../table-replicas.md#ddb-table-267), [DDB-TABLE-265](../table-replicas.md#ddb-table-265), [DDB-TABLE-297](../table-replicas.md#ddb-table-297), [DDB-TABLE-260](../table-global-tables.md#ddb-table-260), [DDB-TABLE-442](../service.md#ddb-table-442),
    [DDB-TABLE-312](../table-replicas.md#ddb-table-312), [DDB-TABLE-311](../table-replicas.md#ddb-table-311) · hypotheses: H-R-010, H-R-011 · evidence:
    table/cross-region/multi-replica-delete-semantics

## Notes

Confirms H-R-011 in practice for any group whose replicas were added from the base within the last 24h (i.e.
the normal controller flow); refutes H-R-010 for that window. Whether DeleteTable on a base succeeds (and
orphans replicas) once every replica it sourced is older than 24h remains untested (needs a day-old group).
The finalizer must remove replicas first and must expect the 'source region ... last 24 hours'
ValidationException as a retry-after-replica-removal condition, not a terminal error.

Contradiction with [DDB-TABLE-442](../service.md#ddb-table-442), [DDB-TABLE-312](../table-replicas.md#ddb-table-312), [DDB-TABLE-311](../table-replicas.md#ddb-table-311), [DDB-TABLE-267](../table-replicas.md#ddb-table-267): 442 classifies 'Operation
cannot be performed while replica server-side encryption status is in UPDATING state' as WAIT-FOR-STATE
ReplicaStatus and the 24 h source-region text as PERMANENT-QUOTA 24 h; 312/311 show the SSE text is returned
for a disabled KMS key with nothing visible changing in DescribeTable, and 266/267 show the 24 h text clears
as soon as the sourced replica is removed Resolution: keep 442 as the catalogue; 312/311 canonical for the SSE
text (external dependency: EnableKey, not a state wait) and 266/267 for the 24 h text (retry after replica
removal, not a quota window)
