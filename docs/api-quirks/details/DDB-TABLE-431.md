<!-- generated: ack-api-quirks dynamodb 2026-10-09 model=2012-08-10 controller=34b85e6 -->
# DDB-TABLE-431: Resource-policy Deny on dynamodb:DeleteTable blocks the owner's DeleteTable (AccessDenied, beats DP) and outlives its removal by ~226 s
_Full entry and notes of one finding; its summary entry is in
[table-streams-encryption-class.md](../table-streams-encryption-class.md). Generated from ack-api-quirks
`services/dynamodb` (model 2012-08-10, controller commit 34b85e6); edit the finding in the lab, not here._

## Finding

- <a id="ddb-table-431"></a>**DDB-TABLE-431** `delete-semantics` · impact high · unhandled (not handled in controller) · verified 2026-10-09
  **Resource-policy Deny on dynamodb:DeleteTable blocks the owner's DeleteTable (AccessDenied, beats DP) and outlives its removal by ~226 s**
  PutResourcePolicy {Deny, Principal:'*', Action:dynamodb:DeleteTable} on an ACTIVE table (same account, Admin
  role): DeleteTable -> AccessDeniedException (HTTP 400) 'User: arn:aws:sts::<acct>:assumed-role/<PRINCIPAL>
  is not authorized to perform: dynamodb:DeleteTable on resource: <arn> with an explicit deny in a
  resource-based policy', enforced 0-2 s after Put. With DeletionProtectionEnabled=true AND the deny, the
  AccessDeniedException is returned (not the DP ValidationException); turning DP off (UpdateTable allowed)
  changes nothing. DescribeTable shows no hint (DeletionProtectionEnabled=false, TableStatus=ACTIVE).
  DeleteResourcePolicy within 15 s of the Put -> ThrottlingException 'Resource-based policy for table <name>
  modified within the previous 15000 milliseconds'; once removed (GetResourcePolicy 200/PolicyNotFound at
  once) the caller's DeleteTable kept failing with AccessDeniedException for 226.4 s (46 attempts at 5 s)
  before the first 200.
  - ACK: pre-delete-cleanup, terminal_codes, requeue · ops: DeleteTable, PutResourcePolicy,
    DeleteResourcePolicy, UpdateTable · fields: ResourcePolicy, DeletionProtectionEnabled
  - repro: CreateTable (DP=true); PutResourcePolicy Deny dynamodb:DeleteTable Principal *; DeleteTable ->
    AccessDenied; UpdateTable DP=false; DeleteTable -> AccessDenied; DeleteResourcePolicy; DeleteTable every 5
    s
  - measurements: deny_enforced_after_put_s=2.0, delete_allowed_after_policy_removal_s=226.4,
    delete_allowed_after_policy_removal_attempts=46
  - handling: not handled in the controller (as of commit 34b85e6)
  - related: [DDB-TABLE-324](../table-policy-kinesis-autoscaling.md#ddb-table-324), [DDB-TABLE-270](../table-policy-kinesis-autoscaling.md#ddb-table-270), [DDB-TABLE-016](../table-streams-encryption-class.md#ddb-table-016), [DDB-TABLE-206](../table-policy-kinesis-autoscaling.md#ddb-table-206), [DDB-TABLE-210](../table-policy-kinesis-autoscaling.md#ddb-table-210), [DDB-TABLE-432](../table-policy-kinesis-autoscaling.md#ddb-table-432) ·
    evidence: table/creative/policy-denies-finalizer

## Notes

A user-managed (or spec-managed) resourcePolicy is a second, invisible deletion protection: the finalizer's
DeleteTable returns AccessDeniedException, which a controller should treat as terminal-with-message rather
than requeue blindly. Recovery needs DeleteResourcePolicy (possible as long as the policy does not also deny
it, cf. [DDB-TABLE-270](../table-policy-kinesis-autoscaling.md#ddb-table-270)) followed by ~4 min of retries - the authorization cache of [DDB-TABLE-324](../table-policy-kinesis-autoscaling.md#ddb-table-324) applies to
DeleteTable as well. In the first attempt the table disappeared ~60 s after the removal while this caller was
still denied at 55 s; the deleter could not be identified (another session in the shared account) - consistent
with a per-caller cache but not proven.
