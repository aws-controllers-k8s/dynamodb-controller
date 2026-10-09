<!-- generated: ack-api-quirks dynamodb 2026-10-09 model=2012-08-10 controller=34b85e6 -->
# DDB-TABLE-029: StreamSpecification round-trip: absent when never enabled; {StreamEnabled:false} sent at create -> observed shape recorded
_Full entry and notes of one finding; its summary entry is in
[table-streams-encryption-class.md](../table-streams-encryption-class.md). Generated from ack-api-quirks
`services/dynamodb` (model 2012-08-10, controller commit 34b85e6); edit the finding in the lab, not here._

## Finding

- <a id="ddb-table-029"></a>**DDB-TABLE-029** `response-fidelity` · impact medium · handled · verified 2026-10-08
  **StreamSpecification round-trip: absent when never enabled; {StreamEnabled:false} sent at create -> observed shape recorded**
  DescribeTable StreamSpecification per shape: {"min": "<absent>", "ppr": "<absent>", "prov-explicit":
  "<absent>", "full-prov": {"StreamEnabled": true, "StreamViewType": "NEW_AND_OLD_IMAGES"}, "full-ppr":
  {"StreamEnabled": true, "StreamViewType": "KEYS_ONLY"}, "sse-false": "<absent>", "sse-type-only":
  "<absent>", "sse-awsalias": "<absent>", "sse-keyid": "<absent>", "sse-keyarn": "<absent>", "sse-labalias":
  "<absent>", "stream-new": {"StreamEnabled": true, "StreamViewType": "NEW_IMAGE"}, "stream-old":
  {"StreamEnabled": true, "StreamViewType": "OLD_IMAGE"}, "prov-warm": "<absent>"}. LatestStreamArn present:
  {"min": false, "ppr": false, "prov-explicit": false, "full-prov": true, "full-ppr": true, "sse-false":
  false, "sse-type-only": false, "sse-awsalias": false, "sse-keyid": false, "sse-keyarn": false,
  "sse-labalias": false, "stream-new": true, "stream-old": true, "prov-warm": false}.
  - ACK: compare.is_ignored+delta_pre_compare · ops: CreateTable, DescribeTable · fields: StreamSpecification,
    LatestStreamArn, LatestStreamLabel
  - repro: CreateTable without StreamSpecification / with {StreamEnabled:false} / with each StreamViewType;
    DescribeTable
  - handling: handled via `test/e2e/table.py:88-100; pkg/resource/table/sdk.go:369-380`
  - related: [DDB-TABLE-050](../table-streams-encryption-class.md#ddb-table-050), [DDB-TABLE-035](../table-streams-encryption-class.md#ddb-table-035), [DDB-TABLE-018](../table-streams-encryption-class.md#ddb-table-018), [DDB-TABLE-002](../table-streams-encryption-class.md#ddb-table-002), [DDB-TABLE-040](../table-throughput-billing.md#ddb-table-040), [DDB-TABLE-036](../table-streams-encryption-class.md#ddb-table-036) ·
    evidence: table/round-trip/full-fields

## Notes

Hypotheses: H-T-036. Create-side only; the disable-after-enable part of H-T-036 is in the mutation-matrix
probes.
