<!-- generated: ack-api-quirks dynamodb 2026-10-09 model=2012-08-10 controller=34b85e6 -->
# DDB-TABLE-208: GetResourcePolicy returns a canonicalized document (minified, 1-element Action -> string, account id -> root ARN); byte-stable if canonical
_Full entry and notes of one finding; its summary entry is in
[table-policy-kinesis-autoscaling.md](../table-policy-kinesis-autoscaling.md). Generated from ack-api-quirks
`services/dynamodb` (model 2012-08-10, controller commit 34b85e6); edit the finding in the lab, not here._

## Finding

- <a id="ddb-table-208"></a>**DDB-TABLE-208** `normalization` · impact high · handled · verified 2026-10-09
  **GetResourcePolicy returns a canonicalized document (minified, 1-element Action -> string, account id -> root ARN); byte-stable if canonical**
  Pretty-printed Put (331 bytes) reads back as 234 bytes: byte-identical=False, json-equal=False (the
  1-element Action list came back as a string). Minified Put byte-identical=False; reordered keys
  byte-identical=False (keys come back as Version/Statement/Sid/Effect/Principal/Action/Resource); Action as
  string byte-identical=False (type str); Principal '<account-id>' reads back as {'AWS':
  'arn:aws:iam::<ACCOUNT>:root'}; a 19456-byte whitespace-padded document reads back as 228 bytes. Example
  read-back:
  {"Version":"2012-10-17","Statement":[{"Sid":"AckqAllowRead","Effect":"Allow","Principal":{"AWS":"arn:aws:iam::<ACCOUNT>:root"},"Action":"dynamodb:GetItem","Resource":"arn:aws:dynamodb:us-west-2:<ACCOUNT>:table/ackq-71f899-rp"}]}
  - ACK: is_document, is_iam_policy, compare.is_ignored+delta_pre_compare · ops: PutResourcePolicy,
    GetResourcePolicy · fields: ResourcePolicy, Policy
  - repro: Put each formatting variant; GetResourcePolicy until RevisionId matches; compare bytes
  - measurements: get_lag_s_after_first_put=2.04, pretty_sent_len=331, returned_len=234
  - handling: handled via `generator.yaml:32-37; pkg/resource/table/hooks_resource_policy.go:30-137; pkg/resource/table/hooks_resource_policy.go:139-177; pkg/resource/table/hooks_resource_policy_test.go:25-235`
  - related: [DDB-TABLE-244](../table-policy-kinesis-autoscaling.md#ddb-table-244), [DDB-TABLE-245](../table-policy-kinesis-autoscaling.md#ddb-table-245), [DDB-TABLE-246](../table-policy-kinesis-autoscaling.md#ddb-table-246), [DDB-TABLE-248](../table-policy-kinesis-autoscaling.md#ddb-table-248), [DDB-TABLE-346](../table-policy-kinesis-autoscaling.md#ddb-table-346), [DDB-TABLE-233](../table-policy-kinesis-autoscaling.md#ddb-table-233),
    [DDB-TABLE-351](../table-policy-kinesis-autoscaling.md#ddb-table-351), [DDB-TABLE-207](../table-policy-kinesis-autoscaling.md#ddb-table-207), [DDB-TABLE-352](../table-policy-kinesis-autoscaling.md#ddb-table-352), [DDB-TABLE-353](../table-policy-kinesis-autoscaling.md#ddb-table-353), [DDB-TABLE-354](../table-policy-kinesis-autoscaling.md#ddb-table-354), [DDB-TABLE-355](../table-policy-kinesis-autoscaling.md#ddb-table-355), [DDB-TABLE-356](../table-policy-kinesis-autoscaling.md#ddb-table-356) ·
    hypotheses: H-S-028 · evidence: table/sub-resources/resource-policy

## Notes

H-S-028 REFUTED: the service canonicalizes IAM-style; a string comparison of spec vs Get sees permanent drift
for pretty-printed/list-form specs - compare parsed+canonicalized JSON or track RevisionId.

Contradiction with [DDB-TABLE-351](../table-policy-kinesis-autoscaling.md#ddb-table-351): 208 says the Get document is 'never byte-identical' to the Put; 351 shows
the canonical form is a byte-stable fixpoint (a document sent minified in Version/Statement //
Sid,Effect,Principal,Action,Resource,Condition order reads back byte-identical, and re-Putting the Get output
is a no-op for 19/20 variants). 208 only tested non-canonical inputs Resolution: keep both; 351 is canonical;
fix 208's title
