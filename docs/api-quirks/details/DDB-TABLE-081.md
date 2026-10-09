<!-- generated: ack-api-quirks dynamodb 2026-10-09 model=2012-08-10 controller=34b85e6 -->
# DDB-TABLE-081: SSE/KMS changes are quota-limited per table: 4 per 24h window, then one per 6h; excess fails with LimitExceededException (HTTP 400)
_Full entry and notes of one finding; its summary entry is in
[table-streams-encryption-class.md](../table-streams-encryption-class.md). Generated from ack-api-quirks
`services/dynamodb` (model 2012-08-10, controller commit 34b85e6); edit the finding in the lab, not here._

## Finding

- <a id="ddb-table-081"></a>**DDB-TABLE-081** `quota-limit` · impact high · tracked in GitHub issue (not handled) · verified 2026-10-08
  **SSE/KMS changes are quota-limited per table: 4 per 24h window, then one per 6h; excess fails with LimitExceededException (HTTP 400)**
  After four SSESpecification changes on one table within minutes (three no-op re-sends of the same key by
  alias/key-id plus one Enabled:false), every further UpdateTable with SSESpecification failed with
  LimitExceededException (HTTP 400): 'Subscriber limit exceeded: Encryption mode changes are limited in the
  24h window ending at 2026-10-09T23:13:00.402Z. After the first 4 change, each subsequent change in the same
  window can be performed at most once every 21600 seconds. Number of updates today: 4. Last change at <ts>'.
  The window started at the first change (table creation time + ~0), not at midnight. Requests rejected by
  this quota include ones that would otherwise be ValidationException (bad keys), so the quota check runs
  before key validation.
  - ACK: requeue, terminal_codes, custom_update, compare.is_ignored+delta_pre_compare · ops: UpdateTable ·
    fields: SSESpecification, SSESpecification.KMSMasterKeyId
  - repro: CMK table; UpdateTable SSESpecification with the same key as alias x3 (each triggers a change) +
    Enabled:false; then any further SSESpecification update
  - measurements: changes_before_limit=4, window_h=24, subsequent_min_interval_s=21600
  - handling: tracked in https://github.com/aws-controllers-k8s/community/issues/2136 (not handled) · code refs: `generator.yaml:73-77; pkg/resource/table/hooks.go:603-611; pkg/resource/table/hooks.go:583-619`
  - related: [DDB-TABLE-142](../table-streams-encryption-class.md#ddb-table-142), [DDB-TABLE-082](../table-streams-encryption-class.md#ddb-table-082), [DDB-TABLE-078](../table-streams-encryption-class.md#ddb-table-078), [DDB-TABLE-141](../table-streams-encryption-class.md#ddb-table-141), [DDB-TABLE-140](../table-streams-encryption-class.md#ddb-table-140), [DDB-TABLE-371](../table-streams-encryption-class.md#ddb-table-371),
    [DDB-TABLE-065](../table-streams-encryption-class.md#ddb-table-065), [DDB-TABLE-079](../table-streams-encryption-class.md#ddb-table-079), [DDB-TABLE-120](../table-streams-encryption-class.md#ddb-table-120) · evidence: table/mutation-matrix/sse-kms

## Notes

A controller that re-sends KMSMasterKeyId as an alias or key id on each reconcile burns the quota in four
reconciles and is then locked out for up to 24h.

Contradiction with [DDB-TABLE-140](../table-streams-encryption-class.md#ddb-table-140), [DDB-TABLE-141](../table-streams-encryption-class.md#ddb-table-141), [DDB-TABLE-142](../table-streams-encryption-class.md#ddb-table-142), [DDB-TABLE-120](../table-streams-encryption-class.md#ddb-table-120): SSE quota boundary: 081/141/142
observe 4 accepted changes and the 5th rejected ('Number of updates today: 4'); 140's T1 (created with a CMK,
first change CMK->CMK by ARN) had 5 accepted and the 6th rejected ('Number of updates today: 5', verified in
sse-kms-quota evidence). 120's behavior says 'the 4th SSE toggle failed' but its evidence shows a phase-A
enable plus three phase-B toggles succeeded before the 5th call failed with 'Number of updates today: 4' -
consistent with 081, wrong count in the text Resolution: keep all; 081 canonical (4 then one per 6 h, window
anchored ~9 s after the first change); 140-T1's extra accepted change is unexplained - controllers should
budget for 4
