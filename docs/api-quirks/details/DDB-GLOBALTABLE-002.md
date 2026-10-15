<!-- generated: ack-api-quirks dynamodb 2026-10-09 model=2012-08-10 controller=34b85e6 -->
# DDB-GLOBALTABLE-002: CreateGlobalTable (legacy 2017.11.29) is rejected everywhere in 2026 - 'global tables version 2017.11.29 is not supported'
_Full entry and notes of one finding; its summary entry is in
[table-global-tables.md](../table-global-tables.md). Generated from ack-api-quirks `services/dynamodb` (model
2012-08-10, controller commit 34b85e6); edit the finding in the lab, not here._

## Finding

- <a id="ddb-globaltable-002"></a>**DDB-GLOBALTABLE-002** `scope` · impact high · handled · verified 2026-10-09
  **CreateGlobalTable (legacy 2017.11.29) is rejected everywhere in 2026 - 'global tables version 2017.11.29 is not supported'** (hypothesis refuted; behavior confirmed)
  **Scope verdict: skip:deprecated**
  With identical, empty, PAY_PER_REQUEST, stream-enabled (NEW_AND_OLD_IMAGES) tables ACTIVE in us-west-2 and
  us-east-1, CreateGlobalTable ReplicationGroup=[us-west-2, us-east-1] fails synchronously with HTTP 400
  ValidationException "One or more parameter values were invalid: DynamoDB global tables version 2017.11.29 is
  not supported. We recommend using DynamoDB global tables version 2019.11.21, instead of version 2017.11.29
  (Legacy)." from the us-west-2, us-east-1, us-east-2 and eu-west-1 endpoints. From ap-south-1 the message is
  region-specific and enumerates the 11 regions where the legacy version was ever supported ("not supported in
  this region 'ap-south-1', supported regions are : [eu-west-2, eu-west-1, ap-southeast-1, ap-southeast-2,
  ap-northeast-2, eu-central-1, ap-northeast-1, us-east-1, us-east-2, us-west-1, us-west-2]") yet the create
  is rejected in those regions too. Nothing is created (DescribeGlobalTable -> GlobalTableNotFoundException);
  the regional tables are untouched.
  - ACK: scope:skip, ignore.resource · ops: CreateGlobalTable
  - repro: CreateTable x2 (same name, streams NEW_AND_OLD_IMAGES) -> wait ACTIVE -> CreateGlobalTable
    ReplicationGroup=[us-west-2, us-east-1]
  - handling: handled via `test/e2e/tests/test_global_table.py:82; 089de59`
  - related: [DDB-GLOBALTABLE-005](../table-global-tables.md#ddb-globaltable-005), [DDB-GLOBALTABLE-001](../table-global-tables.md#ddb-globaltable-001), [DDB-GLOBALTABLE-004](../table-global-tables.md#ddb-globaltable-004), [DDB-GLOBALTABLESETTINGS-001](../table-global-tables.md#ddb-globaltablesettings-001),
    [DDB-GLOBALTABLE-003](../table-global-tables.md#ddb-globaltable-003) · hypotheses: H-R-035, H-R-051, H-R-037, H-R-039, H-R-040, H-R-041, H-R-042, H-R-043,
    H-R-044 · evidence: globaltable/round-trip/legacy-create

## Notes

Refutes H-R-035 (status=refuted refers to the hypothesis; the rejection itself was observed 5x). Settles
H-R-051: a GlobalTable CRD cannot create anything in 2026 and can only adopt pre-existing legacy groups (none
can be made for testing) -> drop it in favour of Table.spec.replicas. H-R-037/039/040/041/042/043/044
(duplicate codes, ARN shape, delete path, status timeline, settings propagation) are untestable as a
consequence. UpdateGlobalTable Create returns the same message even for a missing GlobalTableName.
