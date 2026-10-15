<!-- generated: ack-api-quirks dynamodb 2026-10-09 model=2012-08-10 controller=34b85e6 -->
# DDB-TABLE-355: Lenient parsing: Effect allow -> Allow (not rejected); Statement object -> 1-element list; unicode escapes decoded; absent Sid stays absent
_Full entry and notes of one finding; its summary entry is in
[table-policy-kinesis-autoscaling.md](../table-policy-kinesis-autoscaling.md). Generated from ack-api-quirks
`services/dynamodb` (model 2012-08-10, controller commit 34b85e6); edit the finding in the lab, not here._

## Finding

- <a id="ddb-table-355"></a>**DDB-TABLE-355** `normalization` · impact medium · unhandled (not handled in controller) · verified 2026-10-09
  **Lenient parsing: Effect allow -> Allow (not rejected); Statement object -> 1-element list; unicode escapes decoded; absent Sid stays absent**
  "Effect": "allow" -> Allow (rewritten 200, not rejected). "Statement": {...} (object) -> list with 1
  element. Sid sent as "V15Esc" reads back as V15Esc (JSON-equal, not byte-equal). A statement without Sid
  reads back without Sid (['Effect', 'Principal', 'Action', 'Resource']) - no synthetic Sid.
  Tabs/newlines/spaces are stripped (270 bytes -> 225 bytes).
  - ACK: is_iam_policy, compare.is_ignored+delta_pre_compare · ops: PutResourcePolicy, GetResourcePolicy ·
    fields: Policy.Statement.Effect, Policy.Statement, Policy.Statement.Sid
  - repro: Put each variant; Get; compare
  - handling: not handled in the controller (as of commit 34b85e6)
  - related: [DDB-TABLE-207](../table-policy-kinesis-autoscaling.md#ddb-table-207), [DDB-TABLE-208](../table-policy-kinesis-autoscaling.md#ddb-table-208), [DDB-TABLE-211](../table-policy-kinesis-autoscaling.md#ddb-table-211), [DDB-TABLE-244](../table-policy-kinesis-autoscaling.md#ddb-table-244), [DDB-TABLE-351](../table-policy-kinesis-autoscaling.md#ddb-table-351), [DDB-TABLE-352](../table-policy-kinesis-autoscaling.md#ddb-table-352),
    [DDB-TABLE-353](../table-policy-kinesis-autoscaling.md#ddb-table-353), [DDB-TABLE-354](../table-policy-kinesis-autoscaling.md#ddb-table-354), [DDB-TABLE-356](../table-policy-kinesis-autoscaling.md#ddb-table-356) · hypotheses: H-S-028 · evidence:
    table/round-trip/policy-canonicalization

## Notes

Refutes the brief's expectation that a lowercase Effect is rejected. A naive string/JSON diff of spec vs Get
flags all of these as drift; a canonicalizer must title-case Effect, wrap a bare Statement object, and compare
decoded strings.
