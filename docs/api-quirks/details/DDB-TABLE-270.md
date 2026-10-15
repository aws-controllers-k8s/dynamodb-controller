<!-- generated: ack-api-quirks dynamodb 2026-10-09 model=2012-08-10 controller=34b85e6 -->
# DDB-TABLE-270: A policy denying only dynamodb:DeleteResourcePolicy is accepted without Confirm; 60s later the caller's Delete -> AccessDeniedException
_Full entry and notes of one finding; its summary entry is in
[table-policy-kinesis-autoscaling.md](../table-policy-kinesis-autoscaling.md). Generated from ack-api-quirks
`services/dynamodb` (model 2012-08-10, controller commit 34b85e6); edit the finding in the lab, not here._

## Finding

- <a id="ddb-table-270"></a>**DDB-TABLE-270** `request-validation` · impact medium · unhandled (not handled in controller) · verified 2026-10-09
  **A policy denying only dynamodb:DeleteResourcePolicy is accepted without Confirm; 60s later the caller's Delete -> AccessDeniedException**
  PutResourcePolicy(Deny Principal * on dynamodb:DeleteResourcePolicy + Allow GetItem) without
  ConfirmRemoveSelfResourceAccess -> 200 OK. After 60s: Get -> 200 OK; DeleteResourcePolicy ->
  AccessDeniedException (HTTP 400) 'User: arn:aws:sts::<ACCOUNT>:assumed-role/<PRINCIPAL> is not authorized to
  perform: dynamodb:DeleteResourcePolicy on resource: arn:aws:dynamodb:us-west-2:0'. Replacing it with Put ->
  200 OK; Delete after replacement -> AccessDeniedException (HTTP 400) 'User:
  arn:aws:sts::<ACCOUNT>:assumed-role/<PRINCIPAL> is not authorized to perform: dynamodb:DeleteResourcePolicy
  on resource: arn:aws:dynamodb:us-west-2:0'.
  - ACK: custom_field, terminal_codes · ops: PutResourcePolicy, DeleteResourcePolicy · fields: Policy,
    ConfirmRemoveSelfResourceAccess
  - repro: Put Deny-* on dynamodb:DeleteResourcePolicy only; wait 60s; DeleteResourcePolicy
  - handling: not handled in the controller (as of commit 34b85e6)
  - related: [DDB-TABLE-210](../table-policy-kinesis-autoscaling.md#ddb-table-210), [DDB-TABLE-324](../table-policy-kinesis-autoscaling.md#ddb-table-324), [DDB-TABLE-431](../table-streams-encryption-class.md#ddb-table-431), [DDB-TABLE-432](../table-policy-kinesis-autoscaling.md#ddb-table-432) · hypotheses: H-S-029 · evidence:
    table/creative/policy-kinesis-followups

## Notes

Resolves the surprise from table/sub-resources/resource-policy (where the same Deny looked unenforced 16s
after Put): the self-lockout check only guards PutResourcePolicy, and the Deny on DeleteResourcePolicy IS
enforced once propagated (AccessDeniedException after 60s). Enforcement lags the API view: even after the Deny
was REPLACED by an Allow-only document, Delete 16s later was still denied - a Put that removes a Deny does not
immediately restore access (see table/creative/policy-enforcement-lag for timings). A controller can still
recover by Put-replacing the policy and waiting.

Contradiction with [DDB-TABLE-210](../table-policy-kinesis-autoscaling.md#ddb-table-210): 210 reports a Deny on DeleteResourcePolicy alone as accepted and NOT
enforced (the caller's Delete still 200 at 16 s); 270 shows the same Deny enforced (AccessDeniedException) at
60 s and still enforced 16 s after being replaced. [DDB-TABLE-324](../table-policy-kinesis-autoscaling.md#ddb-table-324) reconciles: changes after the first policy
evaluation take ~220-237 s to be enforced (authorization cache), so 210's 16 s check was inside the lag
Resolution: keep both; 324 is canonical for the lag, 270 for the Deny-Delete outcome; 210's 'Deny-Delete ok'
must not be read as 'harmless' (title fix)
