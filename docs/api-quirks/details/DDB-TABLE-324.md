<!-- generated: ack-api-quirks dynamodb 2026-10-09 model=2012-08-10 controller=34b85e6 -->
# DDB-TABLE-324: Policy enforcement lags the API by ~221-237s for every change after the first (GetResourcePolicy converges in ~2s; first Put: 2s)
_Full entry and notes of one finding; its summary entry is in
[table-policy-kinesis-autoscaling.md](../table-policy-kinesis-autoscaling.md). Generated from ack-api-quirks
`services/dynamodb` (model 2012-08-10, controller commit 34b85e6); edit the finding in the lab, not here._

## Finding

- <a id="ddb-table-324"></a>**DDB-TABLE-324** `eventual-consistency` · impact high · unhandled (not handled in controller) · verified 2026-10-09
  **Policy enforcement lags the API by ~221-237s for every change after the first (GetResourcePolicy converges in ~2s; first Put: 2s)**
  Throwaway table, policy = Deny * dynamodb:DescribeTable; DescribeTable + GetResourcePolicy polled every 1s.
  Sequence of changes -> (seconds until DescribeTable reflected the change, seconds until GetResourcePolicy
  showed the new RevisionId/PolicyNotFound): [('deny', 2.05, 2.05), ('delete', 228.08, 2.04), ('deny', 236.53,
  1.03), ('replace-with-allow', 236.59, 2.04), ('deny', 221.17, 2.04), ('delete', 237.23, 1.02), ('deny',
  236.53, 3.07), ('replace-with-allow', 236.66, 3.06)]. The first Deny ever applied to the table was enforced
  after 2.05s; every later change (Delete of the Deny, a new Deny, replacing the Deny with an Allow-only
  document) took 221-237s to be enforced although GetResourcePolicy reflected each change within ~2s. Deny
  message: 'User: arn:aws:sts::<ACCOUNT>:assumed-role/<PRINCIPAL> is not authorized to perform:
  dynamodb:DescribeTable on resource: arn:aws:dynamodb:us-west-'.
  - ACK: e2e-timing, requeue, docs-only · ops: PutResourcePolicy, DeleteResourcePolicy, GetResourcePolicy ·
    fields: ResourcePolicy
  - repro: Put Deny-* on dynamodb:DescribeTable; poll DescribeTable + GetResourcePolicy at 1s; Delete; poll;
    Put Deny; Put Allow-only; poll; repeat
  - measurements: first_deny_enforced_s=2.05, later_changes_enforced_s.n=7,
    later_changes_enforced_s.min=221.17, later_changes_enforced_s.median=236.53,
    later_changes_enforced_s.max=237.23, get_converged_s.n=4, get_converged_s.min=1.03,
    get_converged_s.median=2.04, get_converged_s.max=3.07
  - handling: not handled in the controller (as of commit 34b85e6)
  - related: [DDB-TABLE-210](../table-policy-kinesis-autoscaling.md#ddb-table-210), [DDB-TABLE-270](../table-policy-kinesis-autoscaling.md#ddb-table-270), [DDB-TABLE-431](../table-streams-encryption-class.md#ddb-table-431), [DDB-TABLE-432](../table-policy-kinesis-autoscaling.md#ddb-table-432) · hypotheses: H-S-007, H-S-029 ·
    evidence: table/creative/policy-enforcement-lag

## Notes

Consistent with a ~4-minute authorization cache per table that is primed on first evaluation: once primed,
neither tightening nor loosening the policy takes effect for ~220-240s, so 'RevisionId converged' (2s) must
not be read as 'policy in effect'. Explains the apparent non-enforcement of a Deny-DeleteResourcePolicy policy
16s after Put in table/sub-resources/resource-policy and its enforcement at 60s (and the still-denied Delete
16s after the Deny was replaced) in table/creative/policy-kinesis-followups. e2e tests asserting access right
after Put/Delete will flake for minutes.
