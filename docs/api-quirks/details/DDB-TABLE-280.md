<!-- generated: ack-api-quirks dynamodb 2026-10-09 model=2012-08-10 controller=34b85e6 -->
# DDB-TABLE-280: Index overrides must match a source index by KeySchema+Projection but may RENAME it; throughput overrides follow the effective billing mode
_Full entry and notes of one finding; its summary entry is in [table-indexes.md](../table-indexes.md).
Generated from ack-api-quirks `services/dynamodb` (model 2012-08-10, controller commit 34b85e6); edit the
finding in the lab, not here._

## Finding

- <a id="ddb-table-280"></a>**DDB-TABLE-280** `request-validation` · impact high · unhandled (not handled in controller) · verified 2026-10-09
  **Index overrides must match a source index by KeySchema+Projection but may RENAME it; throughput overrides follow the effective billing mode**
  GlobalSecondaryIndexOverride=[{IndexName:'gsi-new', same KeySchema/Projection as gsi1, 5/5}] and
  LocalSecondaryIndexOverride=[{IndexName:'lsi-new', same keys/projection as lsi1}] were accepted (200) and
  the restored tables carried indexes named gsi-new / lsi-new (the original names were gone). Overrides with a
  different KeySchema or Projection -> ValidationException 'Index <n> does not match a secondary index that
  existed in the source table and cannot be created during the restore operation'. A GSI override without
  ProvisionedThroughput on a PROVISIONED restore -> 'Must specify provisioned throughput for index gsi1'; with
  ProvisionedThroughput when BillingModeOverride=PAY_PER_REQUEST -> 'Cannot override ProvisionedThroughput if
  BillingMode is overridden to PAY_PER_REQUEST for index gsi1'; OnDemandThroughputOverride on a PROVISIONED
  backup -> 'Cannot override MaxReadRequestUnits for OnDemandThroughput unless BillingModeOverride is
  PAY_PER_REQUEST'; ProvisionedThroughputOverride 0/0 is rejected client-side (min 1).
  - ACK: custom_create, terminal_codes · ops: RestoreTableFromBackup · fields: GlobalSecondaryIndexOverride,
    LocalSecondaryIndexOverride, ProvisionedThroughputOverride, OnDemandThroughputOverride,
    BillingModeOverride
  - repro: RestoreTableFromBackup with each override variant on a PROVISIONED backup that has gsi1 (gk) and
    lsi1 (pk+lsk)
  - handling: not handled in the controller (as of commit 34b85e6)
  - related: [DDB-TABLE-273](../table-restore.md#ddb-table-273), [DDB-TABLE-279](../table-restore.md#ddb-table-279), [DDB-TABLE-281](../table-indexes.md#ddb-table-281), [DDB-BACKUP-021](../backup.md#ddb-backup-021), [DDB-BACKUP-017](../backup.md#ddb-backup-017) · hypotheses:
    H-B-138, H-B-104 · evidence: table/round-trip/restore-overrides

## Notes

Refines H-B-138: 'cannot create new indexes' is enforced on keys/projection, not names - a Table spec whose
GSI names differ from the backup's is accepted and silently yields renamed indexes. H-B-104(c) confirmed
(OnDemandThroughputOverride on a PROVISIONED backup is a synchronous ValidationException).
