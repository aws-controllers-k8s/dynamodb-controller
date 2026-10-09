<!-- generated: ack-api-quirks dynamodb 2026-10-09 model=2012-08-10 controller=34b85e6 -->
# DDB-TABLE-110: CreateTable with invalid or >50 tags fails with ValidationException and creates nothing; CreateTable Tags=[] is accepted
_Full entry and notes of one finding; its summary entry is in [table.md](../table.md). Generated from
ack-api-quirks `services/dynamodb` (model 2012-08-10, controller commit 34b85e6); edit the finding in the lab,
not here._

## Finding

- <a id="ddb-table-110"></a>**DDB-TABLE-110** `request-validation` · impact medium · handled · verified 2026-10-08
  **CreateTable with invalid or >50 tags fails with ValidationException and creates nothing; CreateTable Tags=[] is accepted**
  CreateTable Tags=[{aws:foo}] -> ValidationException 'Tag Key cannot be prefixed with aws:, Key: aws:foo';
  key 129 chars -> ValidationException 'The Tag Key provided is invalid, Key:
  xxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxx'; 51 tags -> ValidationException 'Number of
  Tags exceed the current limit for the provided ResourceArn'; duplicate keys -> ValidationException (stored:
  None); Tags=[] -> 200; '#' in key -> ValidationException; 50 tags -> 200 (50 tags visible 0.01 s after
  ACTIVE). DescribeTable 1s after each rejected create -> {'aws_prefix': 'ResourceNotFoundException',
  'key_129': 'ResourceNotFoundException', '51_tags': 'ResourceNotFoundException', 'dup_keys':
  'ResourceNotFoundException', 'hash_char': 'ResourceNotFoundException'}.
  - ACK: terminal_codes, tags.custom-sync · ops: CreateTable · fields: Tags
  - repro: CreateTable with each Tags payload (boto3 parameter_validation=False); DescribeTable after 1s
  - handling: handled via `templates/hooks/table/sdk_create_post_set_output.go.tpl:1-4; bcd26e1`
  - related: [DDB-TABLE-101](../service.md#ddb-table-101), [DDB-TABLE-104](../table.md#ddb-table-104), [DDB-TABLE-105](../service.md#ddb-table-105), [DDB-TABLE-106](../service.md#ddb-table-106), [DDB-TABLE-107](../service.md#ddb-table-107), [DDB-TABLE-108](../service.md#ddb-table-108),
    [DDB-TABLE-109](../service.md#ddb-table-109), [DDB-TABLE-368](../service.md#ddb-table-368), [DDB-TABLE-372](../service.md#ddb-table-372) · evidence: table/tags/validation-upsert-limits

## Notes

Confirms the no-orphan part of H-T-133 and refutes its LimitExceededException claim for 51 tags
(ValidationException). Asymmetry: an empty Tags list is accepted by CreateTable but rejected by TagResource
('Atleast one Tag needs to be provided as Input.'). 50 tags on CreateTable are visible the moment the table is
ACTIVE.
