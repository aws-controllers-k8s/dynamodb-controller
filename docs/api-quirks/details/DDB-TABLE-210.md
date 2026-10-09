<!-- generated: ack-api-quirks dynamodb 2026-10-09 model=2012-08-10 controller=34b85e6 -->
# DDB-TABLE-210: Lockout check guards only PutResourcePolicy: Deny on Put -> AccessDenied unless ConfirmRemoveSelfResourceAccess; Deny-Delete enforced later
_Full entry and notes of one finding; its summary entry is in
[table-policy-kinesis-autoscaling.md](../table-policy-kinesis-autoscaling.md). Generated from ack-api-quirks
`services/dynamodb` (model 2012-08-10, controller commit 34b85e6); edit the finding in the lab, not here._

## Finding

- <a id="ddb-table-210"></a>**DDB-TABLE-210** `request-validation` · impact high · unhandled (not handled in controller) · verified 2026-10-09
  **Lockout check guards only PutResourcePolicy: Deny on Put -> AccessDenied unless ConfirmRemoveSelfResourceAccess; Deny-Delete enforced later**
  Deny * on Put+DeleteResourcePolicy without ConfirmRemoveSelfResourceAccess -> AccessDeniedException (HTTP 400)
  'The new resource policy will not allow you to update the resource policy in the future.'; Confirm=false ->
  AccessDeniedException (HTTP 400) 'The new resource policy will not allow you to update the resource policy
  in the future.'; Deny only PutResourcePolicy -> AccessDeniedException (HTTP 400) 'The new resource policy
  will not allow you to update the resource policy in the future.'; Deny Put only for the caller's role via
  aws:PrincipalArn condition -> AccessDeniedException (HTTP 400) 'The new resource policy will not allow you
  to update the resource policy in the future.'. Deny only DeleteResourcePolicy (Principal *) -> 200 OK and
  16s later the caller's DeleteResourcePolicy still -> 200 OK. Deny for a non-existent account principal ->
  ValidationException (HTTP 400) 'One or more parameter values were invalid: Invalid principal in policy
  document.'. With ConfirmRemoveSelfResourceAccess=true (throwaway table) -> 200 OK; 16s later Get -> 200, Put
  -> AccessDeniedException (HTTP 400) 'User: arn:aws:sts::<ACCOUNT>:assumed-role/<PRINCIPAL> is not authorized
  to perform: dynamodb:PutResourcePolicy on resource: arn:aws:dynamodb:us-w', Put+Confirm ->
  AccessDeniedException (HTTP 400) 'User: arn:aws:sts::<ACCOUNT>:assumed-role/<PRINCIPAL> is not authorized to
  perform: dynamodb:PutResourcePolicy on resource: arn:aws:dynamodb:us-w', Delete -> AccessDeniedException
  (HTTP 400) 'User: arn:aws:sts::<ACCOUNT>:assumed-role/<PRINCIPAL> is not authorized to perform:
  dynamodb:DeleteResourcePolicy on resource: arn:aws:dynamodb:u' ('with an explicit deny in a resource-based
  policy'), DescribeTable/TagResource -> 200, DeleteTable -> accepted (a later manual DeleteTable succeeded;
  the in-probe attempt hit the tag write lock).
  - ACK: terminal_codes, custom_field, is_iam_policy · ops: PutResourcePolicy, DeleteResourcePolicy · fields:
    Policy, ConfirmRemoveSelfResourceAccess
  - repro: Put Deny-* on dynamodb:PutResourcePolicy with and without ConfirmRemoveSelfResourceAccess; Put
    Deny-* on dynamodb:DeleteResourcePolicy only; then Delete
  - handling: not handled in the controller (as of commit 34b85e6)
  - related: [DDB-TABLE-270](../table-policy-kinesis-autoscaling.md#ddb-table-270), [DDB-TABLE-324](../table-policy-kinesis-autoscaling.md#ddb-table-324), [DDB-TABLE-431](../table-streams-encryption-class.md#ddb-table-431), [DDB-TABLE-432](../table-policy-kinesis-autoscaling.md#ddb-table-432), [DDB-TABLE-172](../service.md#ddb-table-172), [DDB-TABLE-348](../table-policy-kinesis-autoscaling.md#ddb-table-348),
    [DDB-TABLE-119](../table-streams-encryption-class.md#ddb-table-119) · hypotheses: H-S-029 · evidence: table/sub-resources/resource-policy

## Notes

H-S-029 lockout clause: code is AccessDeniedException (hyp said ValidationException). The check simulates the
caller against the new document (a condition-scoped Deny on the caller's role is caught). A confirmed lockout
is irreversible for a non-root caller but does not block DeleteTable. Surprise: Deny on DeleteResourcePolicy
alone neither triggers the check nor (within 16s) blocked the caller's Delete.

Contradiction with [DDB-TABLE-270](../table-policy-kinesis-autoscaling.md#ddb-table-270): 210 reports a Deny on DeleteResourcePolicy alone as accepted and NOT
enforced (the caller's Delete still 200 at 16 s); 270 shows the same Deny enforced (AccessDeniedException) at
60 s and still enforced 16 s after being replaced. [DDB-TABLE-324](../table-policy-kinesis-autoscaling.md#ddb-table-324) reconciles: changes after the first policy
evaluation take ~220-237 s to be enforced (authorization cache), so 210's 16 s check was inside the lag
Resolution: keep both; 324 is canonical for the lag, 270 for the Deny-Delete outcome; 210's 'Deny-Delete ok'
must not be read as 'harmless' (title fix)
