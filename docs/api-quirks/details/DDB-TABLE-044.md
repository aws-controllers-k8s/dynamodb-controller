<!-- generated: ack-api-quirks dynamodb 2026-10-09 model=2012-08-10 controller=34b85e6 -->
# DDB-TABLE-044: SSESpecification without the Enabled member is accepted: {} -> no SSEDescription, {SSEType:KMS[,KMSMasterKeyId]} -> KMS encryption enabled
_Full entry and notes of one finding; its summary entry is in
[table-streams-encryption-class.md](../table-streams-encryption-class.md). Generated from ack-api-quirks
`services/dynamodb` (model 2012-08-10, controller commit 34b85e6); edit the finding in the lab, not here._

## Finding

- <a id="ddb-table-044"></a>**DDB-TABLE-044** `request-validation` · impact medium · handled · verified 2026-10-08
  **SSESpecification without the Enabled member is accepted: {} -> no SSEDescription, {SSEType:KMS[,KMSMasterKeyId]} -> KMS encryption enabled**
  CreateTable SSESpecification={} -> OK (Describe SSEDescription: "<absent>"). SSESpecification={SSEType:KMS}
  -> OK (Describe: {"Status": "ENABLED", "SSEType": "KMS", "KMSMasterKeyArn":
  "arn:aws:kms:us-west-2:<ACCOUNT>:key/301f0bb6-edf0-476a-9525-14f2ee58148a"}). SSESpecification={SSEType:KMS,
  KMSMasterKeyId:alias/aws/dynamodb} -> OK (Describe: {"Status": "ENABLED", "SSEType": "KMS",
  "KMSMasterKeyArn": "arn:aws:kms:us-west-2:<ACCOUNT>:key/301f0bb6-edf0-476a-9525-14f2ee58148a"}).
  - ACK: custom_create, compare.nil_equals_zero_value · ops: CreateTable, DescribeTable · fields:
    SSESpecification.Enabled, SSESpecification.SSEType
  - repro: CreateTable PAY_PER_REQUEST with SSESpecification={SSEType:'KMS'} (no Enabled); DescribeTable
  - handling: handled via `generator.yaml:10-11; generator.yaml:70-72; pkg/resource/table/hooks.go:446-465; test/e2e/tests/test_table.py:641-673`
  - related: [DDB-TABLE-027](../table-streams-encryption-class.md#ddb-table-027), [DDB-TABLE-036](../table-streams-encryption-class.md#ddb-table-036), [DDB-TABLE-031](../table-streams-encryption-class.md#ddb-table-031), [DDB-TABLE-079](../table-streams-encryption-class.md#ddb-table-079), [DDB-TABLE-140](../table-streams-encryption-class.md#ddb-table-140) · evidence:
    table/weird-inputs/create-validation

## Notes

Hypotheses: H-T-027. A nil Enabled pointer with SSEType set turns encryption on; Enabled is effectively
inferred from SSEType.
