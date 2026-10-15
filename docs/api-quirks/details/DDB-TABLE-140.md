<!-- generated: ack-api-quirks dynamodb 2026-10-09 model=2012-08-10 controller=34b85e6 -->
# DDB-TABLE-140: SSE transitions CMK->CMK, CMK->AWS managed, AWS managed->off->on, off->CMK: phases, durations and which re-sends are no-ops
_Full entry and notes of one finding; its summary entry is in
[table-streams-encryption-class.md](../table-streams-encryption-class.md). Generated from ack-api-quirks
`services/dynamodb` (model 2012-08-10, controller commit 34b85e6); edit the finding in the lab, not here._

## Finding

- <a id="ddb-table-140"></a>**DDB-TABLE-140** `async-state-machine` · impact high · handled · verified 2026-10-08
  **SSE transitions CMK->CMK, CMK->AWS managed, AWS managed->off->on, off->CMK: phases, durations and which re-sends are no-ops**
  T1 (CMK K1): K1->K2 by ARN -> OK (resp TableStatus=ACTIVE, resp SSE={"Status": "UPDATING", "SSEType": "KMS",
  "KMSMasterKeyArn": "arn:aws:kms:us-west-2:<ACCOUNT>:key/8963cda5-0fac-4af1-8635-0af67d16fe14"}; 22.31s via
  ACTIVE/UPDATING/0af67d16fe14->ACTIVE/UPDATING/07e049320145->ACTIVE/ENABLED/07e049320145; after={"Status":
  "ENABLED", "SSEType": "KMS", "KMSMasterKeyArn":
  "arn:aws:kms:us-west-2:<ACCOUNT>:key/5494d1e8-65e0-4fdc-9814-07e049320145"}). ->Enabled:true (AWS managed)
  -> OK (resp TableStatus=ACTIVE, resp SSE={"Status": "UPDATING", "SSEType": "KMS", "KMSMasterKeyArn":
  "arn:aws:kms:us-west-2:<ACCOUNT>:key/5494d1e8-65e0-4fdc-9814-07e049320145"}; 21.29s via
  ACTIVE/UPDATING/07e049320145->ACTIVE/UPDATING/14f2ee58148a->ACTIVE/ENABLED/14f2ee58148a; after={"Status":
  "ENABLED", "SSEType": "KMS", "KMSMasterKeyArn":
  "arn:aws:kms:us-west-2:<ACCOUNT>:key/301f0bb6-edf0-476a-9525-14f2ee58148a"}). Enabled:true again -> OK (resp
  TableStatus=ACTIVE, resp SSE={"Status": "UPDATING", "SSEType": "KMS", "KMSMasterKeyArn":
  "arn:aws:kms:us-west-2:<ACCOUNT>:key/301f0bb6-edf0-476a-9525-14f2ee58148a"}; 21.31s via
  ACTIVE/UPDATING/14f2ee58148a->ACTIVE/ENABLED/14f2ee58148a; after={"Status": "ENABLED", "SSEType": "KMS",
  "KMSMasterKeyArn": "arn:aws:kms:us-west-2:<ACCOUNT>:key/301f0bb6-edf0-476a-9525-14f2ee58148a"}).
  alias/aws/dynamodb explicit -> OK (resp TableStatus=ACTIVE, resp SSE={"Status": "UPDATING", "SSEType":
  "KMS", "KMSMasterKeyArn": "arn:aws:kms:us-west-2:<ACCOUNT>:key/301f0bb6-edf0-476a-9525-14f2ee58148a"};
  21.31s via ACTIVE/UPDATING/14f2ee58148a->ACTIVE/ENABLED/14f2ee58148a; after={"Status": "ENABLED", "SSEType":
  "KMS", "KMSMasterKeyArn": "arn:aws:kms:us-west-2:<ACCOUNT>:key/301f0bb6-edf0-476a-9525-14f2ee58148a"}).
  Enabled:false -> OK (resp TableStatus=ACTIVE, resp SSE={"Status": "UPDATING", "SSEType": "KMS",
  "KMSMasterKeyArn": "arn:aws:kms:us-west-2:<ACCOUNT>:key/301f0bb6-edf0-476a-9525-14f2ee58148a"}; 21.3s via
  ACTIVE/UPDATING/14f2ee58148a->ACTIVE/UPDATING/->ACTIVE/<absent>/None; after="<absent>"). Enabled:true after
  disable -> LimitExceededException: 'Subscriber limit exceeded: Encryption mode changes are limited in the
  24h window ending at 2026-10-09T23:23:37.938Z. After the first 4 change, each subsequent change in the same
  window can be performed at most once every 21600 seconds. Number of updates today: 5. Last change at
  2026-10-08T23:25:04.5'. T3 (never encrypted with a specified key): Enabled:true -> OK (resp
  TableStatus=ACTIVE, resp SSE={"Status": "UPDATING"}; 22.32s via
  ACTIVE/UPDATING/->ACTIVE/UPDATING/14f2ee58148a->ACTIVE/ENABLED/14f2ee58148a; after={"Status": "ENABLED",
  "SSEType": "KMS", "KMSMasterKeyArn":
  "arn:aws:kms:us-west-2:<ACCOUNT>:key/301f0bb6-edf0-476a-9525-14f2ee58148a"}). Enabled:false -> OK (resp
  TableStatus=ACTIVE, resp SSE={"Status": "UPDATING", "SSEType": "KMS", "KMSMasterKeyArn":
  "arn:aws:kms:us-west-2:<ACCOUNT>:key/301f0bb6-edf0-476a-9525-14f2ee58148a"}; 22.32s via
  ACTIVE/UPDATING/14f2ee58148a->ACTIVE/UPDATING/->ACTIVE/<absent>/None; after="<absent>"). CMK K1 -> OK (resp
  TableStatus=ACTIVE, resp SSE={"Status": "UPDATING"}; 22.33s via
  ACTIVE/UPDATING/->ACTIVE/UPDATING/0af67d16fe14->ACTIVE/ENABLED/0af67d16fe14; after={"Status": "ENABLED",
  "SSEType": "KMS", "KMSMasterKeyArn":
  "arn:aws:kms:us-west-2:<ACCOUNT>:key/8963cda5-0fac-4af1-8635-0af67d16fe14"}). Enabled:false -> OK (resp
  TableStatus=ACTIVE, resp SSE={"Status": "UPDATING", "SSEType": "KMS", "KMSMasterKeyArn":
  "arn:aws:kms:us-west-2:<ACCOUNT>:key/8963cda5-0fac-4af1-8635-0af67d16fe14"}; 23.35s via
  ACTIVE/UPDATING/0af67d16fe14->ACTIVE/UPDATING/->ACTIVE/<absent>/None; after="<absent>").
  - ACK: requeue, synced.when, custom_update · ops: UpdateTable, DescribeTable · fields: SSESpecification,
    SSEDescription.Status, SSEDescription.KMSMasterKeyArn
  - repro: see behavior; poll DescribeTable at 1s after each UpdateTable
  - measurements: create-t1=8.12, create-t2=0.01, create-t3=0.01, t1-k1-to-k2-arn=22.31,
    t1-k2-to-aws-managed=21.29, t1-aws-managed-resend=21.31, t1-aws-alias-explicit=21.31, t1-disable=21.3,
    t2-resend-enabled-true=21.31, t2-enabled-true-type-kms=21.31, t2-aws-alias-explicit=21.32,
    t2-disable=21.3, t3-enable-aws-managed=22.32, t3-disable=22.32, t3-cmk-k1=22.33, t3-disable-2=23.35
  - handling: handled via `pkg/resource/table/hooks.go:434-473; test/e2e/tests/test_table.py:630-673`
  - related: [DDB-TABLE-060](../table-throughput-billing.md#ddb-table-060), [DDB-TABLE-370](../table-throughput-billing.md#ddb-table-370), [DDB-TABLE-284](../table-streams-encryption-class.md#ddb-table-284), [DDB-TABLE-179](../table-throughput-billing.md#ddb-table-179), [DDB-TABLE-066](../table-throughput-billing.md#ddb-table-066), [DDB-TABLE-371](../table-streams-encryption-class.md#ddb-table-371),
    [DDB-TABLE-064](../table-throughput-billing.md#ddb-table-064), [DDB-TABLE-183](../table-throughput-billing.md#ddb-table-183), [DDB-TABLE-180](../table-streams-encryption-class.md#ddb-table-180), [DDB-TABLE-177](../table-streams-encryption-class.md#ddb-table-177), [DDB-TABLE-156](../table-throughput-billing.md#ddb-table-156), [DDB-TABLE-082](../table-streams-encryption-class.md#ddb-table-082), [DDB-TABLE-078](../table-streams-encryption-class.md#ddb-table-078),
    [DDB-TABLE-141](../table-streams-encryption-class.md#ddb-table-141), [DDB-TABLE-065](../table-streams-encryption-class.md#ddb-table-065), [DDB-TABLE-079](../table-streams-encryption-class.md#ddb-table-079), [DDB-TABLE-081](../table-streams-encryption-class.md#ddb-table-081), [DDB-TABLE-142](../table-streams-encryption-class.md#ddb-table-142), [DDB-TABLE-120](../table-streams-encryption-class.md#ddb-table-120), [DDB-TABLE-027](../table-streams-encryption-class.md#ddb-table-027),
    [DDB-TABLE-036](../table-streams-encryption-class.md#ddb-table-036), [DDB-TABLE-044](../table-streams-encryption-class.md#ddb-table-044), [DDB-TABLE-031](../table-streams-encryption-class.md#ddb-table-031) · evidence: table/mutation-matrix/sse-kms-quota

## Notes

Hypotheses: H-T-112, H-T-034. Phase shapes: CMK->CMK: SSE Status UPDATING with the OLD key ARN, then UPDATING
with the NEW key ARN, then ENABLED (~22s). -> AWS owned (Enabled:false): UPDATING old key -> {Status:UPDATING}
without SSEType/KMSMasterKeyArn -> SSEDescription absent (~21s). AWS owned -> AWS managed/CMK: response
SSEDescription={Status:UPDATING} only; DescribeTable {Status:UPDATING} -> UPDATING + key ARN -> ENABLED
(~22s). TableStatus stays ACTIVE throughout every SSE change.

Contradiction with [DDB-TABLE-081](../table-streams-encryption-class.md#ddb-table-081), [DDB-TABLE-141](../table-streams-encryption-class.md#ddb-table-141), [DDB-TABLE-142](../table-streams-encryption-class.md#ddb-table-142), [DDB-TABLE-120](../table-streams-encryption-class.md#ddb-table-120): SSE quota boundary: 081/141/142
observe 4 accepted changes and the 5th rejected ('Number of updates today: 4'); 140's T1 (created with a CMK,
first change CMK->CMK by ARN) had 5 accepted and the 6th rejected ('Number of updates today: 5', verified in
sse-kms-quota evidence). 120's behavior says 'the 4th SSE toggle failed' but its evidence shows a phase-A
enable plus three phase-B toggles succeeded before the 5th call failed with 'Number of updates today: 4' -
consistent with 081, wrong count in the text Resolution: keep all; 081 canonical (4 then one per 6 h, window
anchored ~9 s after the first change); 140-T1's extra accepted change is unexplained - controllers should
budget for 4
