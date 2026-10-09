<!-- generated: ack-api-quirks dynamodb 2026-10-09 model=2012-08-10 controller=34b85e6 -->
# DDB-TABLE-356: PutResourcePolicy rejections (ValidationException 400): duplicate Sids, bare-string account Principal, Allow to *, non-existent principal
_Full entry and notes of one finding; its summary entry is in
[table-policy-kinesis-autoscaling.md](../table-policy-kinesis-autoscaling.md). Generated from ack-api-quirks
`services/dynamodb` (model 2012-08-10, controller commit 34b85e6); edit the finding in the lab, not here._

## Finding

- <a id="ddb-table-356"></a>**DDB-TABLE-356** `request-validation` · impact medium · unhandled (not handled in controller) · verified 2026-10-09
  **PutResourcePolicy rejections (ValidationException 400): duplicate Sids, bare-string account Principal, Allow to *, non-existent principal**
  Two statements with the same Sid -> ValidationException 'One or more parameter values were invalid: Invalid
  policy document: The Statement Ids in the policy are not unique'. Principal given as a bare account-id
  string -> ValidationException 'One or more parameter values were invalid: Invalid policy document: Syntax
  error at position (1,94)'. Principal "*" or {"AWS":"*"} with Effect Allow -> ValidationException
  'Resource-based policy grants unbounded access in one or more elements. Please revise the policy to ensure
  least-privilege form of access'. Principal list containing arn:aws:iam::<ACCOUNT>:root ->
  ValidationException 'One or more parameter values were invalid: Invalid principal in policy document.'. All
  rejections are synchronous and leave the existing policy and RevisionId untouched.
  - ACK: terminal_codes, is_iam_policy · ops: PutResourcePolicy · fields: Policy
  - repro: Put each invalid variant on an ACTIVE table
  - handling: not handled in the controller (as of commit 34b85e6)
  - related: [DDB-TABLE-207](../table-policy-kinesis-autoscaling.md#ddb-table-207), [DDB-TABLE-208](../table-policy-kinesis-autoscaling.md#ddb-table-208), [DDB-TABLE-211](../table-policy-kinesis-autoscaling.md#ddb-table-211), [DDB-TABLE-244](../table-policy-kinesis-autoscaling.md#ddb-table-244), [DDB-TABLE-351](../table-policy-kinesis-autoscaling.md#ddb-table-351), [DDB-TABLE-352](../table-policy-kinesis-autoscaling.md#ddb-table-352),
    [DDB-TABLE-353](../table-policy-kinesis-autoscaling.md#ddb-table-353), [DDB-TABLE-354](../table-policy-kinesis-autoscaling.md#ddb-table-354), [DDB-TABLE-355](../table-policy-kinesis-autoscaling.md#ddb-table-355) · hypotheses: H-S-028 · evidence:
    table/round-trip/policy-canonicalization

## Notes

ValidationException is shared with the >20480-byte case ([DDB-TABLE-211](../table-policy-kinesis-autoscaling.md#ddb-table-211)); the message text is the only way to
distinguish a syntax error from the least-privilege guardrail. Deny-to-* policies were not re-tested here
(accepted in [DDB-TABLE-210](../table-policy-kinesis-autoscaling.md#ddb-table-210)).
