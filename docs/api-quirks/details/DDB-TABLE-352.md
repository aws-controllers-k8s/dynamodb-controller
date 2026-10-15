<!-- generated: ack-api-quirks dynamodb 2026-10-09 model=2012-08-10 controller=34b85e6 -->
# DDB-TABLE-352: Principal rewrites: {AWS: account-id} -> root ARN; duplicate [id, ARN] -> one string; Allow * rejected (unbounded access); bare id rejected
_Full entry and notes of one finding; its summary entry is in
[table-policy-kinesis-autoscaling.md](../table-policy-kinesis-autoscaling.md). Generated from ack-api-quirks
`services/dynamodb` (model 2012-08-10, controller commit 34b85e6); edit the finding in the lab, not here._

## Finding

- <a id="ddb-table-352"></a>**DDB-TABLE-352** `normalization` · impact high · unhandled (not handled in controller) · verified 2026-10-09
  **Principal rewrites: {AWS: account-id} -> root ARN; duplicate [id, ARN] -> one string; Allow * rejected (unbounded access); bare id rejected**
  Principal {"AWS": "<account-id>"} reads back as {'AWS': 'arn:aws:iam::<ACCOUNT>:root'}. Principal {"AWS":
  ["<account-id>", "arn:aws:iam::<account-id>:root"]} (same principal twice) reads back collapsed to the
  STRING {'AWS': 'arn:aws:iam::<ACCOUNT>:root'} (list -> string after de-duplication). Principal "*" ->
  ValidationException 'Resource-based policy grants unbounded access in one or more elements. Please revise
  the policy to ensure least-privilege form of access'; {"AWS": "*"} -> ValidationException 'Resource-based
  policy grants unbounded access in one or more elements. Please revise the policy to ensure least-privilege
  form of access' (an Allow to everyone is refused by a DynamoDB guardrail, not by IAM syntax). Principal
  "<account-id>" as a bare string -> ValidationException 'One or more parameter values were invalid: Invalid
  policy document: Syntax error at position (1,94)'. A list naming own root plus arn:aws:iam::<ACCOUNT>:root
  -> ValidationException 'One or more parameter values were invalid: Invalid principal in policy document.'
  (non-existent account; IAM validates principals exist).
  - ACK: is_iam_policy, compare.is_ignored+delta_pre_compare, terminal_codes · ops: PutResourcePolicy,
    GetResourcePolicy · fields: Policy.Statement.Principal
  - repro: Put each Principal form; Get after ~2 s; compare
  - handling: not handled in the controller (as of commit 34b85e6)
  - related: [DDB-TABLE-207](../table-policy-kinesis-autoscaling.md#ddb-table-207), [DDB-TABLE-208](../table-policy-kinesis-autoscaling.md#ddb-table-208), [DDB-TABLE-211](../table-policy-kinesis-autoscaling.md#ddb-table-211), [DDB-TABLE-244](../table-policy-kinesis-autoscaling.md#ddb-table-244), [DDB-TABLE-351](../table-policy-kinesis-autoscaling.md#ddb-table-351), [DDB-TABLE-353](../table-policy-kinesis-autoscaling.md#ddb-table-353),
    [DDB-TABLE-354](../table-policy-kinesis-autoscaling.md#ddb-table-354), [DDB-TABLE-355](../table-policy-kinesis-autoscaling.md#ddb-table-355), [DDB-TABLE-356](../table-policy-kinesis-autoscaling.md#ddb-table-356) · hypotheses: H-S-028 · evidence:
    table/round-trip/policy-canonicalization

## Notes

Account ids must be expanded to root ARNs and principal lists de-duplicated/collapsed before comparing with
Get; the 'unbounded access' ValidationException is terminal (user must fix the spec) and is specific to
Allow-* resource policies.

Contradiction with [DDB-TABLE-207](../table-policy-kinesis-autoscaling.md#ddb-table-207): 207 says 'Principal as bare account id instead of root ARN' is accepted and
equivalent (same RevisionId); 352 and [DDB-TABLE-356](../table-policy-kinesis-autoscaling.md#ddb-table-356) say a bare account-id Principal is rejected with
ValidationException 'Syntax error at position (1,94)'. Not a real conflict: 207/208 used {"AWS":
"<account-id>"} (accepted, rewritten to root ARN), 352/356 used Principal: "<account-id>" without the AWS
wrapper Resolution: keep both; 352 is canonical for the taxonomy; read 207's 'bare account id' as '{AWS:
account-id}'
