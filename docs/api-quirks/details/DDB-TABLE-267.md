<!-- generated: ack-api-quirks dynamodb 2026-10-09 model=2012-08-10 controller=34b85e6 -->
# DDB-TABLE-267: Source-region rule: a replica that was the endpoint/source of another replica added <24h ago cannot be removed until that replica is gone
_Full entry and notes of one finding; its summary entry is in [table-replicas.md](../table-replicas.md).
Generated from ack-api-quirks `services/dynamodb` (model 2012-08-10, controller commit 34b85e6); edit the
finding in the lab, not here._

## Finding

- <a id="ddb-table-267"></a>**DDB-TABLE-267** `delete-semantics` · impact high · handled · verified 2026-10-09
  **Source-region rule: a replica that was the endpoint/source of another replica added <24h ago cannot be removed until that replica is gone**
  With eu-west-1 (added from the us-east-1 endpoint) ACTIVE: ReplicaUpdates=[Delete us-east-1] from us-west-2
  -> ValidationException 'Replica cannot be deleted because it has acted as a source region for new replica(s)
  being added to the table in the last 24 hours.' ReplicaUpdates=[Delete eu-west-1] -> 200 (gone after ~2 min:
  A UPDATING 31s -> eu-west-1 DELETING ~65s -> ResourceNotFoundException at 121s). 13 minutes later, with
  eu-west-1 gone, Delete us-east-1 -> 200 (accepted). While that delete ran: ReplicaUpdates=[Delete us-west-2]
  issued at us-east-1 -> ResourceInUseException; DeleteTable in us-east-1 -> ResourceInUseException;
  DeleteTable in us-west-2 -> the 24h-source ValidationException (us-east-1 was sourced from us-west-2 <24h
  ago and still existed). Same message seen for ReplicaUpdates.Delete of the base region issued from a replica
  while that replica was still CREATING (table/state-machine/replica-creating-side-ops).
  - ACK: custom_delete, pre-delete-cleanup, terminal_codes, requeue · ops: UpdateTable, DeleteTable · fields:
    ReplicaUpdates.Delete, Replicas
  - repro: Create replica B from A; Create replica C from B's endpoint; try to remove B (or A) within 24h
  - measurements: eu_west_1_delete_total_s=121.0
  - handling: handled via `templates/hooks/table/sdk_delete_pre_build_request.go.tpl:8-22; pkg/resource/table/hooks_replica_updates.go:256-263`
  - related: [DDB-TABLE-204](../table-replicas.md#ddb-table-204), [DDB-TABLE-293](../table-replicas.md#ddb-table-293), [DDB-TABLE-308](../table-replicas.md#ddb-table-308), [DDB-TABLE-222](../table-replicas.md#ddb-table-222), [DDB-TABLE-297](../table-replicas.md#ddb-table-297), [DDB-TABLE-261](../table-global-tables.md#ddb-table-261),
    [DDB-TABLE-231](../table-global-tables.md#ddb-table-231), [DDB-TABLE-266](../table-replicas.md#ddb-table-266), [DDB-TABLE-262](../table-replicas.md#ddb-table-262), [DDB-TABLE-265](../table-replicas.md#ddb-table-265), [DDB-TABLE-260](../table-global-tables.md#ddb-table-260), [DDB-TABLE-442](../service.md#ddb-table-442), [DDB-TABLE-312](../table-replicas.md#ddb-table-312),
    [DDB-TABLE-311](../table-replicas.md#ddb-table-311) · hypotheses: H-R-053, H-R-010, H-R-011, H-R-006 · evidence:
    table/cross-region/multi-replica-delete-semantics

## Notes

Qualifies H-R-053: deleting every replica in one call is impossible (one action per call) AND the order
matters - replicas must be removed leaf-first (most recently added / those not used as a source first); a
Delete rejected with the 24h-source message becomes acceptable as soon as the replicas it sourced are gone. A
controller removing N replicas needs N sequential UpdateTable calls with waits and must treat this
ValidationException as 'retry later', not terminal.

Contradiction with [DDB-TABLE-442](../service.md#ddb-table-442), [DDB-TABLE-312](../table-replicas.md#ddb-table-312), [DDB-TABLE-311](../table-replicas.md#ddb-table-311), [DDB-TABLE-266](../table-replicas.md#ddb-table-266): 442 classifies 'Operation
cannot be performed while replica server-side encryption status is in UPDATING state' as WAIT-FOR-STATE
ReplicaStatus and the 24 h source-region text as PERMANENT-QUOTA 24 h; 312/311 show the SSE text is returned
for a disabled KMS key with nothing visible changing in DescribeTable, and 266/267 show the 24 h text clears
as soon as the sourced replica is removed Resolution: keep 442 as the catalogue; 312/311 canonical for the SSE
text (external dependency: EnableKey, not a state wait) and 266/267 for the 24 h text (retry after replica
removal, not a quota window)
