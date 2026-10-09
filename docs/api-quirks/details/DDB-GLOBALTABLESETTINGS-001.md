<!-- generated: ack-api-quirks dynamodb 2026-10-09 model=2012-08-10 controller=34b85e6 -->
# DDB-GLOBALTABLESETTINGS-001: GlobalTableSettings has no reachable instance in 2026 - legacy-only, no create/delete, GlobalTableNotFoundException for every table
_Full entry and notes of one finding; its summary entry is in
[table-global-tables.md](../table-global-tables.md). Generated from ack-api-quirks `services/dynamodb` (model
2012-08-10, controller commit 34b85e6); edit the finding in the lab, not here._

## Finding

- <a id="ddb-globaltablesettings-001"></a>**DDB-GLOBALTABLESETTINGS-001** `scope` · impact high · handled · verified 2026-10-09
  **GlobalTableSettings has no reachable instance in 2026 - legacy-only, no create/delete, GlobalTableNotFoundException for every table**
  **Scope verdict: skip:deprecated**
  DescribeGlobalTableSettings and UpdateGlobalTableSettings (GlobalTableBillingMode or ReplicaSettingsUpdate)
  return HTTP 400 GlobalTableNotFoundException "Global table with name: '<name>' does not exist." for a plain
  regional table, for a missing name, and for 2019.11.21 global tables (EVENTUAL and STRONG, from the source
  or a replica region). The only operation that could create a legacy global table for them to describe,
  CreateGlobalTable, is rejected everywhere with "version 2017.11.29 is not supported". The API has no
  Create/Delete for settings; its leaves are just the regional tables' billing/capacity/auto-scaling settings.
  - ACK: scope:skip, ignore.resource · ops: DescribeGlobalTableSettings, UpdateGlobalTableSettings,
    CreateGlobalTable
  - repro: DescribeGlobalTableSettings / UpdateGlobalTableSettings with a regional, missing and 2019.11.21
    table name
  - handling: handled via `generator.yaml:126-129; pkg/resource/global_table/sdk.go:83-85`
  - related: [DDB-GLOBALTABLE-001](../table-global-tables.md#ddb-globaltable-001), [DDB-GLOBALTABLE-004](../table-global-tables.md#ddb-globaltable-004), [DDB-GLOBALTABLE-002](../table-global-tables.md#ddb-globaltable-002), [DDB-GLOBALTABLE-003](../table-global-tables.md#ddb-globaltable-003) · hypotheses:
    H-R-049, H-R-045 · evidence: globaltable/round-trip/legacy-create,
    table/cross-region/mrec-replica-updates, table/cross-region/mrsc-witness

## Notes

Confirms H-R-049 (not a standalone CRD) with the stronger conclusion that it should be skipped outright rather
than folded into a GlobalTable resource, since that parent cannot be created either. The 2019.11.21
equivalents are Table.spec.billingMode / provisionedThroughput and
ReplicaUpdates.*.ProvisionedThroughputOverride.
