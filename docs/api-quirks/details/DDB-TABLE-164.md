<!-- generated: ack-api-quirks dynamodb 2026-10-09 model=2012-08-10 controller=34b85e6 -->
# DDB-TABLE-164: Re-sending identical ProvisionedThroughput (table or GSI) is a ValidationException; identical DeletionProtection and partial PT changes pass
_Full entry and notes of one finding; its summary entry is in [table-indexes.md](../table-indexes.md).
Generated from ack-api-quirks `services/dynamodb` (model 2012-08-10, controller commit 34b85e6); edit the
finding in the lab, not here._

## Finding

- <a id="ddb-table-164"></a>**DDB-TABLE-164** `idempotency` · impact high · SUSPECTED CONTROLLER BUG · verified 2026-10-08
  **Re-sending identical ProvisionedThroughput (table or GSI) is a ValidationException; identical DeletionProtection and partial PT changes pass**
  UpdateTable GlobalSecondaryIndexUpdates[Update gsi1 2/2] when gsi1 is already 2/2 -> ValidationException
  'The provisioned throughput for the index gsi1 will not change. The requested value equals the current
  value. Current ReadCapacityUnits provisioned for index gsi1: 2. Requested ReadCapacityUnits: 2. Current
  WriteCapacityUnits ...'. Table ProvisionedThroughput identical -> 'The provisioned throughput for the table
  will not change. ...'. BillingMode=PROVISIONED alone on a PROVISIONED table -> 'ProvisionedThroughput must
  be specified when BillingMode is PROVISIONED'; BillingMode=PROVISIONED + identical throughput -> the 'will
  not change' error. Changing only WriteCapacityUnits (2/2 -> 2/3) is accepted. DeletionProtectionEnabled=true
  re-sent on a protected table -> 200.
  - ACK: custom_update, compare.is_ignored+delta_pre_compare · ops: UpdateTable · fields:
    ProvisionedThroughput, GlobalSecondaryIndexUpdates.Update.ProvisionedThroughput, BillingMode,
    DeletionProtectionEnabled
  - repro: PROVISIONED table 1/1 with gsi1 2/2; UpdateTable GlobalSecondaryIndexUpdates=[Update gsi1 2/2]
  - handling: suspected controller bug - see Handling gaps
  - related: [DDB-TABLE-058](../table-throughput-billing.md#ddb-table-058), [DDB-TABLE-060](../table-throughput-billing.md#ddb-table-060), [DDB-TABLE-370](../table-throughput-billing.md#ddb-table-370), [DDB-TABLE-057](../table-throughput-billing.md#ddb-table-057), [DDB-TABLE-061](../table-throughput-billing.md#ddb-table-061), [DDB-TABLE-063](../table-throughput-billing.md#ddb-table-063),
    [DDB-TABLE-152](../table-indexes.md#ddb-table-152), [DDB-TABLE-153](../table-indexes.md#ddb-table-153), [DDB-TABLE-375](../table-indexes.md#ddb-table-375), [DDB-TABLE-376](../table-indexes.md#ddb-table-376), [DDB-TABLE-128](../table-indexes.md#ddb-table-128), [DDB-TABLE-135](../table-indexes.md#ddb-table-135), [DDB-TABLE-154](../table-throughput-billing.md#ddb-table-154),
    [DDB-TABLE-155](../table-indexes.md#ddb-table-155), [DDB-TABLE-286](../table-streams-encryption-class.md#ddb-table-286) · evidence: table/mutation-matrix/gsi-update-granularity

## Notes

Confirms H-T-030; the controller must diff per index before emitting Update actions.

Suspected controller bug confirmed by evidence: The fallback the controller takes for a changed
Projection/KeySchema - a throughput-only UpdateGlobalSecondaryIndexAction - is not a harmless no-op: when the
PT is unchanged DynamoDB rejects it with ValidationException 'The provisioned throughput for the index X will
not change' ([DDB-TABLE-164](../table-indexes.md#ddb-table-164)), so the delta persists and every reconcile errors; under PPR a PT Update is
rejected too ('The only Updates for index ... can be to OnDemandThroughput, WarmThroughput', [DDB-TABLE-152](../table-indexes.md#ddb-table-152)).
The correct path - delete then recreate under the same name - works once the entry is gone ([DDB-TABLE-166](../table-indexes.md#ddb-table-166), 507
s on an empty table). The immutability itself is model-level (the action has no Projection/KeySchema members)
and was not separately probed.
