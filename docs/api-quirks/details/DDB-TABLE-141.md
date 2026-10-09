<!-- generated: ack-api-quirks dynamodb 2026-10-09 model=2012-08-10 controller=34b85e6 -->
# DDB-TABLE-141: AWS-managed-key table: re-sending Enabled:true/SSEType:KMS/alias/aws/dynamodb re-encrypts (~21s) and burns quota; only ARN is a no-op
_Full entry and notes of one finding; its summary entry is in
[table-streams-encryption-class.md](../table-streams-encryption-class.md). Generated from ack-api-quirks
`services/dynamodb` (model 2012-08-10, controller commit 34b85e6); edit the finding in the lab, not here._

## Finding

- <a id="ddb-table-141"></a>**DDB-TABLE-141** `idempotency` · impact high · tracked in GitHub issue (not handled) · verified 2026-10-08
  **AWS-managed-key table: re-sending Enabled:true/SSEType:KMS/alias/aws/dynamodb re-encrypts (~21s) and burns quota; only ARN is a no-op** (hypothesis refuted; behavior confirmed)
  T2 created with SSESpecification{Enabled:true} (KMSMasterKeyArn = alias/aws/dynamodb key: True). Re-send
  {Enabled:true} -> OK (resp TableStatus=ACTIVE, resp SSE={"Status": "UPDATING", "SSEType": "KMS",
  "KMSMasterKeyArn": "arn:aws:kms:us-west-2:<ACCOUNT>:key/301f0bb6-edf0-476a-9525-14f2ee58148a"}; 21.31s via
  ACTIVE/UPDATING/14f2ee58148a->ACTIVE/ENABLED/14f2ee58148a; after={"Status": "ENABLED", "SSEType": "KMS",
  "KMSMasterKeyArn": "arn:aws:kms:us-west-2:<ACCOUNT>:key/301f0bb6-edf0-476a-9525-14f2ee58148a"}).
  {Enabled:true,SSEType:KMS} -> OK (resp TableStatus=ACTIVE, resp SSE={"Status": "UPDATING", "SSEType": "KMS",
  "KMSMasterKeyArn": "arn:aws:kms:us-west-2:<ACCOUNT>:key/301f0bb6-edf0-476a-9525-14f2ee58148a"}; 21.31s via
  ACTIVE/UPDATING/14f2ee58148a->ACTIVE/ENABLED/14f2ee58148a; after={"Status": "ENABLED", "SSEType": "KMS",
  "KMSMasterKeyArn": "arn:aws:kms:us-west-2:<ACCOUNT>:key/301f0bb6-edf0-476a-9525-14f2ee58148a"}).
  {..,KMSMasterKeyId:alias/aws/dynamodb} -> OK (resp TableStatus=ACTIVE, resp SSE={"Status": "UPDATING",
  "SSEType": "KMS", "KMSMasterKeyArn":
  "arn:aws:kms:us-west-2:<ACCOUNT>:key/301f0bb6-edf0-476a-9525-14f2ee58148a"}; 21.32s via
  ACTIVE/UPDATING/14f2ee58148a->ACTIVE/ENABLED/14f2ee58148a; after={"Status": "ENABLED", "SSEType": "KMS",
  "KMSMasterKeyArn": "arn:aws:kms:us-west-2:<ACCOUNT>:key/301f0bb6-edf0-476a-9525-14f2ee58148a"}).
  {..,KMSMasterKeyId:<aws key ARN>} -> ValidationException: 'One or more parameter values were invalid: Table
  is already encrypted with given KMSMasterKeyId. Use KMSMasterKeyId parameter if you want to change Master
  Key'. Enabled:false -> OK (resp TableStatus=ACTIVE, resp SSE={"Status": "UPDATING", "SSEType": "KMS",
  "KMSMasterKeyArn": "arn:aws:kms:us-west-2:<ACCOUNT>:key/301f0bb6-edf0-476a-9525-14f2ee58148a"}; 21.3s via
  ACTIVE/UPDATING/14f2ee58148a->ACTIVE/UPDATING/->ACTIVE/<absent>/None; after="<absent>"). Enabled:true ->
  LimitExceededException: 'Subscriber limit exceeded: Encryption mode changes are limited in the 24h window
  ending at 2026-10-09T23:25:27.800Z. After the first 4 change, each subsequent change in the same window can
  be performed at most once every 21600 seconds. Number of updates today: 4. Last change at
  2026-10-08T23:26:32.1'. -> CMK K1 -> LimitExceededException: 'Subscriber limit exceeded: Encryption mode
  changes are limited in the 24h window ending at 2026-10-09T23:25:27.800Z. After the first 4 change, each
  subsequent change in the same window can be performed at most once every 21600 seconds. Number of updates
  today: 4. Last change at 2026-10-08T23:26:32.1'.
  - ACK: compare.is_ignored+delta_pre_compare, custom_update · ops: UpdateTable, DescribeTable · fields:
    SSESpecification.Enabled, SSESpecification.SSEType, SSESpecification.KMSMasterKeyId
  - repro: CreateTable SSESpecification{Enabled:true}; UpdateTable with each equivalent AWS-managed spelling
  - handling: tracked in https://github.com/aws-controllers-k8s/community/issues/2136 (not handled) · code refs: `generator.yaml:73-77; pkg/resource/table/hooks.go:603-611; pkg/resource/table/hooks.go:583-619; pkg/resource/table/hooks.go:446-465; test/e2e/tests/test_table.py:641-673`
  - related: [DDB-TABLE-082](../table-streams-encryption-class.md#ddb-table-082), [DDB-TABLE-078](../table-streams-encryption-class.md#ddb-table-078), [DDB-TABLE-140](../table-streams-encryption-class.md#ddb-table-140), [DDB-TABLE-371](../table-streams-encryption-class.md#ddb-table-371), [DDB-TABLE-065](../table-streams-encryption-class.md#ddb-table-065), [DDB-TABLE-079](../table-streams-encryption-class.md#ddb-table-079),
    [DDB-TABLE-081](../table-streams-encryption-class.md#ddb-table-081), [DDB-TABLE-142](../table-streams-encryption-class.md#ddb-table-142), [DDB-TABLE-120](../table-streams-encryption-class.md#ddb-table-120), [DDB-TABLE-018](../table-streams-encryption-class.md#ddb-table-018) · evidence: table/mutation-matrix/sse-kms-quota

## Notes

Hypotheses: H-T-112. REFUTED for the re-send claim: UpdateTable SSESpecification{Enabled:true} on a table
already using the AWS managed key returns 200 but starts a ~21s SSEDescription.Status=UPDATING cycle and
counts as one of the 4 encryption changes allowed per 24h; the same for {Enabled:true,SSEType:KMS} and
KMSMasterKeyId=alias/aws/dynamodb. Only KMSMasterKeyId=<resolved aws/dynamodb key ARN> yields the
ValidationException no-op 'Table is already encrypted with given KMSMasterKeyId'. Confirmed: KMSMasterKeyArn
equals the account's alias/aws/dynamodb key ARN, and explicit alias/aws/dynamodb produces an identical
SSEDescription.

Contradiction with [DDB-TABLE-018](../table-streams-encryption-class.md#ddb-table-018), [DDB-TABLE-082](../table-streams-encryption-class.md#ddb-table-082), [DDB-TABLE-078](../table-streams-encryption-class.md#ddb-table-078), [DDB-TABLE-065](../table-streams-encryption-class.md#ddb-table-065): 018 title generalizes 'SSE
re-sends -> ValidationException' from its single Enabled:false cell ('Table is already encrypted by default');
141/082/078 show that re-sending {Enabled:true}, SSEType-only, alias or key-id returns 200, re-encrypts for
~21 s and burns one of the 4 daily SSE changes; only Enabled:false and the exact-ARN form are
ValidationException, and 065 shows an identical re-send during the SSE job is also 200 Resolution: keep all;
141/082 canonical for the SSE re-send rule; retitle 018

Contradiction with [DDB-TABLE-140](../table-streams-encryption-class.md#ddb-table-140), [DDB-TABLE-081](../table-streams-encryption-class.md#ddb-table-081), [DDB-TABLE-142](../table-streams-encryption-class.md#ddb-table-142), [DDB-TABLE-120](../table-streams-encryption-class.md#ddb-table-120): SSE quota boundary: 081/141/142
observe 4 accepted changes and the 5th rejected ('Number of updates today: 4'); 140's T1 (created with a CMK,
first change CMK->CMK by ARN) had 5 accepted and the 6th rejected ('Number of updates today: 5', verified in
sse-kms-quota evidence). 120's behavior says 'the 4th SSE toggle failed' but its evidence shows a phase-A
enable plus three phase-B toggles succeeded before the 5th call failed with 'Number of updates today: 4' -
consistent with 081, wrong count in the text Resolution: keep all; 081 canonical (4 then one per 6 h, window
anchored ~9 s after the first change); 140-T1's extra accepted change is unexplained - controllers should
budget for 4
