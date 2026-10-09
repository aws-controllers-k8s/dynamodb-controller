<!-- generated: ack-api-quirks dynamodb 2026-10-09 model=2012-08-10 controller=34b85e6 -->
# DDB-TABLE-328: Multi-region KMS key: the other region's ARN of the same MRK is rejected (Invalid arn); bare mrk-id/local ARN/alias accepted
_Full entry and notes of one finding; its summary entry is in
[table-streams-encryption-class.md](../table-streams-encryption-class.md). Generated from ack-api-quirks
`services/dynamodb` (model 2012-08-10, controller commit 34b85e6); edit the finding in the lab, not here._

## Finding

- <a id="ddb-table-328"></a>**DDB-TABLE-328** `request-validation` · impact medium · unhandled (not handled in controller) · verified 2026-10-09
  **Multi-region KMS key: the other region's ARN of the same MRK is rejected (Invalid arn); bare mrk-id/local ARN/alias accepted**
  MRK primary in us-west-2 (KeyId mrk-67c1526d2d8940a3947df29d0358c35a), replica in us-east-1 with the
  identical KeyId (replica key Enabled 60 s after ReplicateKey). CreateTable in us-west-2 with KMSMasterKeyId =
  {'replica_region_arn': ('526d2d8940a3947df29d0358c35a', 'ValidationException'), 'bare_mrk_id':
  ('526d2d8940a3947df29d0358c35a', 'OK'), 'local_arn': ('526d2d8940a3947df29d0358c35a', 'OK'), 'alias':
  ('alias/ackq-e0ee5d-mrk', 'OK')}. DescribeTable KMSMasterKeyArn for the accepted forms: {'bare_mrk_id':
  'arn:aws:kms:us-west-2:<ACCOUNT>:key/mrk-67c1526d2d8940a3947df29d0358c35a', 'local_arn':
  'arn:aws:kms:us-west-2:<ACCOUNT>:key/mrk-67c1526d2d8940a3947df29d0358c35a', 'alias':
  'arn:aws:kms:us-west-2:<ACCOUNT>:key/mrk-67c1526d2d8940a3947df29d0358c35a'}.
  ReplicaUpdates.Create{RegionName:us-east-1, KMSMasterKeyId=...}: {'primary_region_arn':
  'ValidationException', 'bare_mrk_id': 'OK'}. Rejection messages: {'replica_region_arn': 'KMS validation
  error: com.amazonaws.services.kms.model.NotFoundException: Invalid arn us-east-1 (Service: AWSKMS; Status
  Code: 400; Error Code: NotFoundException; Request ID: aa0f5abe-4311-4c12-9df3-bd70475f1913; Proxy: null)',
  'replica_primary_region_arn': 'KMS validation error for region us-east-1:
  com.amazonaws.services.kms.model.NotFoundException: Invalid arn us-west-2 (Service: AWSKMS; Status Code:
  400; Error Code: NotFoundException; Request ID: f4de86ee-5e2e-4fb5-b677-f24cad8a4d7c; Proxy: null)'}.
  Replica created with the bare_mrk_id form; source Replicas[].KMSMasterKeyId=None; replica-region
  DescribeTable SSEDescription=None.
  - ACK: terminal_codes, references, custom_update · ops: CreateTable, UpdateTable, DescribeTable · fields:
    SSESpecification.KMSMasterKeyId, ReplicaUpdates.Create.KMSMasterKeyId, Replicas.KMSMasterKeyId
  - repro: kms CreateKey MultiRegion=true + ReplicateKey; CreateTable with each KMSMasterKeyId form;
    UpdateTable ReplicaUpdates.Create with each form
  - handling: not handled in the controller (as of commit 34b85e6)
  - related: [DDB-TABLE-313](../table-replicas.md#ddb-table-313), [DDB-TABLE-309](../table-replicas.md#ddb-table-309), [DDB-TABLE-295](../table-replicas.md#ddb-table-295), [DDB-TABLE-312](../table-replicas.md#ddb-table-312), [DDB-TABLE-310](../table-replicas.md#ddb-table-310), [DDB-TABLE-311](../table-replicas.md#ddb-table-311),
    [DDB-TABLE-329](../table-replicas.md#ddb-table-329), [DDB-TABLE-327](../table-replicas.md#ddb-table-327) · hypotheses: H-T-110 · evidence: table/cross-region/multi-region-kms-replica

## Notes

H-T-110 cross-region half: DynamoDB region-checks the ARN literally even for a multi-region key that exists in
both regions (CreateTable: 'KMS validation error: ...NotFoundException: Invalid arn us-east-1';
ReplicaUpdates.Create: 'KMS validation error for region us-east-1: ...Invalid arn us-west-2'). A bare
'mrk-...' key id is resolved in the target region, so the same spec value works for the table and for every
replica (DescribeTable then reports the region-local ARN: source SSEDescription.KMSMasterKeyArn=us-west-2 ARN,
Replicas[].KMSMasterKeyId / replica-region SSEDescription = us-east-1 ARN). Cross-account keys were not tested
(single account).
