<!-- generated: ack-api-quirks dynamodb 2026-10-09 model=2012-08-10 controller=34b85e6 -->
# DDB-TABLE-257: MRSC create shape rules - STRONG needs 3 regions in ONE UpdateTable (2 replica Creates, or 1 Create + 1 witness); witness needs STRONG
_Full entry and notes of one finding; its summary entry is in
[table-global-tables.md](../table-global-tables.md). Generated from ack-api-quirks `services/dynamodb` (model
2012-08-10, controller commit 34b85e6); edit the finding in the lab, not here._

## Finding

- <a id="ddb-table-257"></a>**DDB-TABLE-257** `request-validation` · impact high · handled · verified 2026-10-09
  **MRSC create shape rules - STRONG needs 3 regions in ONE UpdateTable (2 replica Creates, or 1 Create + 1 witness); witness needs STRONG**
  On a regional table, all synchronous HTTP 400 ValidationException: STRONG + ReplicaUpdates=[Create
  us-east-1] -> "Unsupported replica count for global tables with MultiRegionConsistency set to STRONG.";
  STRONG + GlobalTableWitnessUpdates only -> the generic "At least one of ProvisionedThroughput, ... or
  TableClass is required" (witness updates are not counted as a mutation); replica + witness without
  MultiRegionConsistency, or with EVENTUAL -> "MultiRegionConsistency must be set as STRONG when
  GlobalTableWitnessUpdates parameter is present."; witness in the replica's region -> "Cannot target multiple
  regions with the same action"; witness in the table's own region -> "Cannot add a witness in the same region
  as an existing replica when creating a global table with MultiRegionConsistency set to STRONG."; replica or
  witness in us-west-1 -> "Unsupported Region(s) specified for global tables with MultiRegionConsistency set
  to STRONG: [us-west-1]."; two witnesses -> "Value '[GlobalTableWitnessGroupUpdate(...)]' at
  'globalTableWitnessUpdates' failed to satisfy constraint: Member must have length less than or equal to 1";
  adding DeletionProtectionEnabled to the call -> "Requests that modify replicas or witnesses must not also
  modify other fields". Accepted: STRONG + [Create us-east-1] + witness us-east-2; STRONG + [Create us-east-1,
  Create us-east-2]; and (attempt 1) STRONG + [Create eu-west-1] + witness us-east-2.
  - ACK: custom_update, one-per-reconcile, terminal_codes · ops: UpdateTable · fields: MultiRegionConsistency,
    ReplicaUpdates, GlobalTableWitnessUpdates
  - repro: CreateTable (streams NEW_AND_OLD_IMAGES) -> UpdateTable with each listed
    MultiRegionConsistency/ReplicaUpdates/GlobalTableWitnessUpdates combination
  - handling: handled via `generator.yaml:13-14; e4a4d8c`
  - related: [DDB-TABLE-224](../table-replicas.md#ddb-table-224), [DDB-TABLE-265](../table-replicas.md#ddb-table-265), [DDB-TABLE-258](../table-global-tables.md#ddb-table-258), [DDB-TABLE-223](../table-replicas.md#ddb-table-223), [DDB-TABLE-259](../table-global-tables.md#ddb-table-259), [DDB-TABLE-260](../table-global-tables.md#ddb-table-260),
    [DDB-TABLE-261](../table-global-tables.md#ddb-table-261), [DDB-TABLE-264](../table-global-tables.md#ddb-table-264), [DDB-TABLE-202](../table-global-tables.md#ddb-table-202), [DDB-TABLE-263](../table-global-tables.md#ddb-table-263) · hypotheses: H-R-021, H-R-020, H-R-001 ·
    evidence: table/cross-region/mrsc-witness

## Notes

H-R-021 mostly confirmed (3-region minimum; witness or 2 creates; one witness) but the "restricted region set"
part is refuted - a eu-west-1 replica with a us-east-2 witness was accepted; us-west-1 is simply not
MRSC-capable. Because STRONG groups must be born complete, the one-replica-per-reconcile pattern cannot build
an MRSC table: the controller must send the whole initial membership in one call.
