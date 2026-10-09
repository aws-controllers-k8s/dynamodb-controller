<!-- generated: ack-api-quirks dynamodb 2026-10-09 model=2012-08-10 controller=34b85e6 -->
# DDB-TABLE-065: SSE switch to KMS: TableStatus stays ACTIVE, SSEDescription.Status=UPDATING ~22s; Delete/SSE change rejected meanwhile
_Full entry and notes of one finding; its summary entry is in
[table-streams-encryption-class.md](../table-streams-encryption-class.md). Generated from ack-api-quirks
`services/dynamodb` (model 2012-08-10, controller commit 34b85e6); edit the finding in the lab, not here._

## Finding

- <a id="ddb-table-065"></a>**DDB-TABLE-065** `async-state-machine` · impact medium · handled · verified 2026-10-08
  **SSE switch to KMS: TableStatus stays ACTIVE, SSEDescription.Status=UPDATING ~22s; Delete/SSE change rejected meanwhile**
  UpdateTable(SSESpecification{Enabled:true,SSEType:KMS}) response: TableStatus=ACTIVE,
  SSEDescription={'Status': 'UPDATING'}. DescribeTable timeline (status, sse status, kms arn present): [(0.01,
  {'status': 'ACTIVE', 'sse': 'UPDATING', 'kms_arn': False}), (10.16, {'status': 'ACTIVE', 'sse': 'UPDATING',
  'kms_arn': True}), (22.33, {'status': 'ACTIVE', 'sse': 'ENABLED', 'kms_arn': True})]. Final SSEDescription:
  {'Status': 'ENABLED', 'SSEType': 'KMS', 'KMSMasterKeyArn':
  'arn:aws:kms:us-west-2:<ACCOUNT>:key/301f0bb6-edf0-476a-9525-14f2ee58148a'}. Re-sending the same
  SSESpecification -> OK ''. Disabling (Enabled:false): ResourceInUseException Attempt to change a resource
  which is still in use: Server-Side Encryption is still being updated; timeline []; final SSEDescription:
  None. During the switch: DeleteTable -> ResourceInUseException, UpdateTable(DP) -> OK(ACTIVE).
  - ACK: synced.when, requeue · ops: UpdateTable, DescribeTable · fields: SSESpecification, SSEDescription
  - repro: UpdateTable(SSESpecification{Enabled:true,SSEType:KMS}); poll DescribeTable; then
    SSESpecification{Enabled:false}
  - measurements: sse_kms_updating_s=0, sse_disable_updating_s=null
  - handling: handled via `generator.yaml:10-11; generator.yaml:70-72; pkg/resource/table/hooks.go:434-473; test/e2e/tests/test_table.py:630-673`
  - related: [DDB-TABLE-001](../table.md#ddb-table-001), [DDB-TABLE-002](../table-streams-encryption-class.md#ddb-table-002), [DDB-TABLE-370](../table-throughput-billing.md#ddb-table-370), [DDB-TABLE-369](../table-streams-encryption-class.md#ddb-table-369), [DDB-TABLE-052](../table-streams-encryption-class.md#ddb-table-052), [DDB-TABLE-285](../table-streams-encryption-class.md#ddb-table-285),
    [DDB-TABLE-120](../table-streams-encryption-class.md#ddb-table-120), [DDB-TABLE-459](../table-throughput-billing.md#ddb-table-459), [DDB-TABLE-054](../table-streams-encryption-class.md#ddb-table-054), [DDB-TABLE-010](../table.md#ddb-table-010), [DDB-TABLE-069](../table-streams-encryption-class.md#ddb-table-069), [DDB-TABLE-066](../table-throughput-billing.md#ddb-table-066), [DDB-TABLE-284](../table-streams-encryption-class.md#ddb-table-284),
    [DDB-TABLE-334](../table-streams-encryption-class.md#ddb-table-334), [DDB-TABLE-082](../table-streams-encryption-class.md#ddb-table-082), [DDB-TABLE-078](../table-streams-encryption-class.md#ddb-table-078), [DDB-TABLE-141](../table-streams-encryption-class.md#ddb-table-141), [DDB-TABLE-140](../table-streams-encryption-class.md#ddb-table-140), [DDB-TABLE-371](../table-streams-encryption-class.md#ddb-table-371), [DDB-TABLE-079](../table-streams-encryption-class.md#ddb-table-079),
    [DDB-TABLE-081](../table-streams-encryption-class.md#ddb-table-081), [DDB-TABLE-142](../table-streams-encryption-class.md#ddb-table-142), [DDB-TABLE-018](../table-streams-encryption-class.md#ddb-table-018) · evidence: table/state-machine/billing-sse-warm-throughput

## Notes

H-T-013 PARTIALLY REFUTED: SSEDescription.Status goes UPDATING (KMSMasterKeyArn appears after ~10s, ENABLED at
~22s) but TableStatus never left ACTIVE. While SSE is UPDATING: DeleteTable -> ResourceInUseException,
SSESpecification{Enabled:false} -> ResourceInUseException 'Server-Side Encryption is still being updated', yet
re-sending the identical SSESpecification -> 200 and UpdateTable(DeletionProtectionEnabled) -> 200.

Contradiction with [DDB-TABLE-018](../table-streams-encryption-class.md#ddb-table-018), [DDB-TABLE-141](../table-streams-encryption-class.md#ddb-table-141), [DDB-TABLE-082](../table-streams-encryption-class.md#ddb-table-082), [DDB-TABLE-078](../table-streams-encryption-class.md#ddb-table-078): 018 title generalizes 'SSE
re-sends -> ValidationException' from its single Enabled:false cell ('Table is already encrypted by default');
141/082/078 show that re-sending {Enabled:true}, SSEType-only, alias or key-id returns 200, re-encrypts for
~21 s and burns one of the 4 daily SSE changes; only Enabled:false and the exact-ARN form are
ValidationException, and 065 shows an identical re-send during the SSE job is also 200 Resolution: keep all;
141/082 canonical for the SSE re-send rule; retitle 018
