<!-- generated: ack-api-quirks dynamodb 2026-10-09 model=2012-08-10 controller=34b85e6 -->
# DDB-TABLE-042: KeySchema is order-sensitive (HASH must be first); duplicate AttributeDefinitions and unused/missing definitions are rejected
_Full entry and notes of one finding; its summary entry is in
[table-throughput-billing.md](../table-throughput-billing.md). Generated from ack-api-quirks
`services/dynamodb` (model 2012-08-10, controller commit 34b85e6); edit the finding in the lab, not here._

## Finding

- <a id="ddb-table-042"></a>**DDB-TABLE-042** `request-validation` · impact medium · handled · verified 2026-10-08
  **KeySchema is order-sensitive (HASH must be first); duplicate AttributeDefinitions and unused/missing definitions are rejected** (hypothesis refuted; behavior confirmed)
  KeySchema [RANGE, HASH] order -> ValidationException: 'Invalid KeySchema: The first KeySchemaElement is not
  a HASH key type'. two HASH -> ValidationException: 'Invalid KeySchema: The second KeySchemaElement is not a
  RANGE key type'. RANGE only -> ValidationException: 'Invalid KeySchema: The first KeySchemaElement is not a
  HASH key type'. three elements -> ValidationException: '1 validation error detected: Value
  '[com.amazonaws.dynamodb.v20120810.KeySchemaElement@ad28eb1d,
  com.amazonaws.dynamodb.v20120810.KeySchemaElement@38b9654b,
  com.amazonaws.dynamodb.v20120810.KeySchemaElement@38bb13cd]' at 'keySchema' faile. [] ->
  ValidationException: '1 validation error detected: Value '[]' at 'keySchema' failed to satisfy constraint:
  Member must have length greater than or equal to 1'. missing -> ValidationException: '1 validation error
  detected: Value null at 'keySchema' failed to satisfy constraint: Member must not be null'. attribute-name
  case mismatch (PK vs pk) -> ValidationException: 'One or more parameter values were invalid: Some index key
  attributes are not defined in AttributeDefinitions. Keys: [PK], AttributeDefinitions: [pk]'. duplicate
  AttributeDefinitions same type -> ValidationException: 'Attribute Name is duplicated: pk'. duplicate
  different types -> ValidationException: 'Attribute Name is duplicated: pk'. unused attribute ->
  ValidationException: 'One or more parameter values were invalid: Number of attributes in KeySchema does not
  exactly match number of attributes defined in AttributeDefinitions'. AttributeDefinitions [] ->
  ValidationException: 'Invalid KeySchema: Some index key attribute have no definition'. missing ->
  ValidationException: '1 validation error detected: Value null at 'attributeDefinitions' failed to satisfy
  constraint: Member must not be null'. wrong attribute -> ValidationException: 'One or more parameter values
  were invalid: Some index key attributes are not defined in AttributeDefinitions. Keys: [pk],
  AttributeDefinitions: [other]'. reversed AttributeDefinitions order -> OK (Describe keeps sent order:
  [{"AttributeName": "pk", "AttributeType": "S"}, {"AttributeName": "sk", "AttributeType": "N"}]).
  - ACK: compare.is_ignored+delta_pre_compare, custom_create · ops: CreateTable, DescribeTable · fields:
    KeySchema, AttributeDefinitions
  - repro: CreateTable with the listed KeySchema / AttributeDefinitions shapes; DescribeTable the accepted
    ones
  - handling: handled via `generator.yaml:88-90; pkg/resource/table/sdk.go:1234-1250`
  - related: [DDB-TABLE-041](../service.md#ddb-table-041), [DDB-TABLE-364](../table-throughput-billing.md#ddb-table-364) · evidence: table/weird-inputs/create-validation

## Notes

Hypotheses: H-T-042. H-T-042 partially refuted: the order-insensitivity claim is wrong (RANGE listed before
HASH is a ValidationException), the duplicate-definition rejection is confirmed. AttributeDefinitions order IS
free and is echoed back as sent, so a controller must compare it order-insensitively. The 3-element KeySchema
message leaks Java class names (com.amazonaws.dynamodb.v20120810.KeySchemaElement@...).
