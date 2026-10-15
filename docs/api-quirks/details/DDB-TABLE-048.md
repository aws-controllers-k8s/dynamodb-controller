<!-- generated: ack-api-quirks dynamodb 2026-10-09 model=2012-08-10 controller=34b85e6 -->
# DDB-TABLE-048: GlobalTableSettingsReplicationMode on a regional table: ENABLED accepted, DISABLED accepted, ENABLED_WITH_OVERRIDES ValidationException
_Full entry and notes of one finding; its summary entry is in
[table-global-tables.md](../table-global-tables.md). Generated from ack-api-quirks `services/dynamodb` (model
2012-08-10, controller commit 34b85e6); edit the finding in the lab, not here._

## Finding

- <a id="ddb-table-048"></a>**DDB-TABLE-048** `request-validation` · impact medium · unhandled (not handled in controller) · verified 2026-10-08
  **GlobalTableSettingsReplicationMode on a regional table: ENABLED accepted, DISABLED accepted, ENABLED_WITH_OVERRIDES ValidationException**
  UpdateTable GlobalTableSettingsReplicationMode=ENABLED -> OK (changed paths
  ["GlobalTableSettingsReplicationMode"]); re-sent ENABLED -> OK. DISABLED -> OK; re-sent -> OK.
  ENABLED_WITH_OVERRIDES -> ValidationException 'Value 'ENABLED_WITH_OVERRIDES' at
  'GlobalTableSettingsReplicationMode' failed to satisfy constraint: Only ENABLED and DISABLED settings
  replication a'.
  - ACK: custom_update, scope:defer · ops: UpdateTable, DescribeTable · fields:
    GlobalTableSettingsReplicationMode
  - repro: PAY_PER_REQUEST regional table; UpdateTable GlobalTableSettingsReplicationMode=ENABLED, again,
    DISABLED, ENABLED_WITH_OVERRIDES
  - measurements: updating_s_enabled=0.0
  - handling: not handled in the controller (as of commit 34b85e6)
  - related: [DDB-TABLE-320](../table-global-tables.md#ddb-table-320), [DDB-TABLE-231](../table-global-tables.md#ddb-table-231), [DDB-TABLE-201](../table-global-tables.md#ddb-table-201), [DDB-TABLE-229](../table-global-tables.md#ddb-table-229), [DDB-TABLE-251](../table-replicas.md#ddb-table-251) · evidence:
    table/mutation-matrix/stream-protection-throughput

## Notes

Hypotheses: H-T-146.

Contradiction with [DDB-TABLE-320](../table-global-tables.md#ddb-table-320): 320: GlobalTableSettingsReplicationMode 'appears only while the table has
replicas; absent on regional tables'; 048: UpdateTable GlobalTableSettingsReplicationMode=ENABLED/DISABLED on
a regional table -> 200 and DescribeTable shows the changed path Resolution: not contradictory - 320 describes
the default (its notes say setting it was not attempted); 048 shows it is caller-settable on a regional table
while the server default ENABLED_WITH_OVERRIDES cannot be sent. Title fix for 320
