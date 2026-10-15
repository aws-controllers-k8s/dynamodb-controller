<!-- generated: ack-api-quirks dynamodb 2026-10-09 model=2012-08-10 controller=34b85e6 -->
# DDB-TABLE-357: KeySchema and LSIs are structurally immutable: no UpdateTable member (SDK ParamValidationError); unknown wire members are ignored
_Full entry and notes of one finding; its summary entry is in [table-indexes.md](../table-indexes.md).
Generated from ack-api-quirks `services/dynamodb` (model 2012-08-10, controller commit 34b85e6); edit the
finding in the lab, not here._

## Finding

- <a id="ddb-table-357"></a>**DDB-TABLE-357** `immutable-field` · impact high · handled · verified 2026-10-09
  **KeySchema and LSIs are structurally immutable: no UpdateTable member (SDK ParamValidationError); unknown wire members are ignored**
  The UpdateTable input shape has no KeySchema or LocalSecondaryIndexes member (create-only members:
  ['GlobalSecondaryIndexes', 'GlobalTableSourceArn', 'KeySchema', 'LocalSecondaryIndexes', 'ResourcePolicy',
  'Tags']); boto3 rejects them client-side with ParamValidationError ('Unknown parameter in input:
  "KeySchema", must be one of: AttributeDefinitions, TableName, BillingMode, ...') and no request is sent.
  Injected into the raw JSON body as the only change, KeySchema -> ValidationException (HTTP 400) 'At least
  one of ProvisionedThroughput, BillingMode, ... or TableClass is required'; LocalSecondaryIndexes ->
  ValidationException (HTTP 400) 'At least one of ProvisionedThroughput, BillingMode, ... or TableClass is
  required'; a bogus member -> ValidationException (HTTP 400) 'At least one of ProvisionedThroughput,
  BillingMode, ... or TableClass is required'; KeySchema as a non-list -> ValidationException (HTTP 400) 'At
  least one of ProvisionedThroughput, BillingMode, ... or TableClass is required'. Injected next to
  DeletionProtectionEnabled=true -> 200 OK (TableStatus=ACTIVE) and DescribeTable KeySchema/LSIs
  unchanged=True.
  - ACK: is_immutable, custom_update · ops: UpdateTable, DescribeTable · fields: KeySchema,
    LocalSecondaryIndexes, AttributeDefinitions
  - repro: ACTIVE table (pk HASH, sk RANGE, 1 LSI): boto3 update_table(KeySchema=...) -> ParamValidationError;
    inject {'KeySchema': [...]} into the serialized body via a before-call hook -> see response
  - handling: handled via `generator.yaml:55-58; pkg/resource/table/hooks.go:621-627; generator.yaml:66-69; pkg/resource/table/hooks.go:815-880`
  - related: [DDB-TABLE-137](../table-subresources.md#ddb-table-137), [DDB-TABLE-162](../table-indexes.md#ddb-table-162), [DDB-TABLE-127](../table-indexes.md#ddb-table-127), [DDB-TABLE-151](../table-indexes.md#ddb-table-151), [DDB-TABLE-359](../table-indexes.md#ddb-table-359), [DDB-TABLE-133](../table-indexes.md#ddb-table-133),
    [DDB-TABLE-094](../table-subresources.md#ddb-table-094) · hypotheses: H-T-066, H-T-045 · evidence: table/mutation-matrix/schema-immutability

## Notes

Confirms H-T-066/H-T-045 structural part: a KeySchema or LSI diff can only be reconciled by recreate; the
controller must detect it itself (terminal condition) because the API offers no call that would even fail for
it. LocalSecondaryIndexDescription keys: ['IndexArn', 'IndexName', 'IndexSizeBytes', 'ItemCount', 'KeySchema',
'Projection'] (lacks ['IndexStatus', 'ProvisionedThroughput', 'OnDemandThroughput', 'WarmThroughput',
'Backfilling']).
