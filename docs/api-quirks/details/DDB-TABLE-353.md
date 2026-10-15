<!-- generated: ack-api-quirks dynamodb 2026-10-09 model=2012-08-10 controller=34b85e6 -->
# DDB-TABLE-353: Action/Resource rewrites: 1-element list -> string; multi-element lists verbatim (order, duplicates kept); lowercase action stored as-is
_Full entry and notes of one finding; its summary entry is in
[table-policy-kinesis-autoscaling.md](../table-policy-kinesis-autoscaling.md). Generated from ack-api-quirks
`services/dynamodb` (model 2012-08-10, controller commit 34b85e6); edit the finding in the lab, not here._

## Finding

- <a id="ddb-table-353"></a>**DDB-TABLE-353** `normalization` · impact medium · unhandled (not handled in controller) · verified 2026-10-09
  **Action/Resource rewrites: 1-element list -> string; multi-element lists verbatim (order, duplicates kept); lowercase action stored as-is**
  Action ["dynamodb:GetItem"] -> dynamodb:GetItem and Resource [<arn>] -> string (str). Action
  ["dynamodb:Query","dynamodb:GetItem"] (reverse-lexical) -> ['dynamodb:Query', 'dynamodb:GetItem'] (not
  sorted); Action ["dynamodb:GetItem","dynamodb:GetItem"] -> ['dynamodb:GetItem', 'dynamodb:GetItem'] (NOT
  de-duplicated, unlike Principal); Resource [<arn>/index/*, <arn>] -> ['table/ackq-9bc2f6-pc0/index/*',
  'table/ackq-9bc2f6-pc0'] (order kept). Action "dynamodb:getitem" (lowercase) is accepted and stored verbatim
  (dynamodb:getitem); NotAction is preserved (dynamodb:DeleteTable). Condition {StringEquals:
  {aws:PrincipalAccount: ["<id>"]}} -> {'StringEquals': {'aws:PrincipalAccount': '<ACCOUNT>'}} (1-element list
  -> string, same rule).
  - ACK: is_iam_policy, compare.is_ignored+delta_pre_compare · ops: PutResourcePolicy, GetResourcePolicy ·
    fields: Policy.Statement.Action, Policy.Statement.Resource, Policy.Statement.Condition
  - repro: Put variants with list/string Action, Resource, Condition values; Get; compare
  - handling: not handled in the controller (as of commit 34b85e6)
  - related: [DDB-TABLE-207](../table-policy-kinesis-autoscaling.md#ddb-table-207), [DDB-TABLE-208](../table-policy-kinesis-autoscaling.md#ddb-table-208), [DDB-TABLE-211](../table-policy-kinesis-autoscaling.md#ddb-table-211), [DDB-TABLE-244](../table-policy-kinesis-autoscaling.md#ddb-table-244), [DDB-TABLE-351](../table-policy-kinesis-autoscaling.md#ddb-table-351), [DDB-TABLE-352](../table-policy-kinesis-autoscaling.md#ddb-table-352),
    [DDB-TABLE-354](../table-policy-kinesis-autoscaling.md#ddb-table-354), [DDB-TABLE-355](../table-policy-kinesis-autoscaling.md#ddb-table-355), [DDB-TABLE-356](../table-policy-kinesis-autoscaling.md#ddb-table-356) · hypotheses: H-S-028 · evidence:
    table/round-trip/policy-canonicalization

## Notes

The rewrite is purely structural (singleton list -> scalar) - no sorting, no case folding, no de-duplication
of Action/Resource lists - so a canonicalizer must do exactly that and nothing more.
