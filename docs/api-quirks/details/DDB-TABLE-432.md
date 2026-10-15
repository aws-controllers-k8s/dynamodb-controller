<!-- generated: ack-api-quirks dynamodb 2026-10-09 model=2012-08-10 controller=34b85e6 -->
# DDB-TABLE-432: Deny dynamodb:UpdateTable in the table policy is per-action: UpdateTable (name/ARN, no-op/real) AccessDenied; TTL/PITR/Tag/Delete succeed
_Full entry and notes of one finding; its summary entry is in
[table-policy-kinesis-autoscaling.md](../table-policy-kinesis-autoscaling.md). Generated from ack-api-quirks
`services/dynamodb` (model 2012-08-10, controller commit 34b85e6); edit the finding in the lab, not here._

## Finding

- <a id="ddb-table-432"></a>**DDB-TABLE-432** `error-code` · impact medium · unhandled (not handled in controller) · verified 2026-10-09
  **Deny dynamodb:UpdateTable in the table policy is per-action: UpdateTable (name/ARN, no-op/real) AccessDenied; TTL/PITR/Tag/Delete succeed**
  Resource policy {Deny, Principal:'*', Action:dynamodb:UpdateTable}: a same-value
  UpdateTable(DeletionProtectionEnabled=false) turned from 200 to AccessDeniedException 4.1 s after Put; a
  real change (StreamSpecification enable) and UpdateTable with the table ARN as TableName ->
  AccessDeniedException too. In the same second DescribeTable, UpdateTimeToLive(enable), TagResource,
  UpdateContinuousBackups and GetResourcePolicy -> 200, and DeleteTable was admitted (ResourceInUseException
  only because of the tag write lock, then 200).
  - ACK: terminal_codes, requeue · ops: UpdateTable, UpdateTimeToLive, TagResource, UpdateContinuousBackups,
    DeleteTable · fields: ResourcePolicy
  - repro: PutResourcePolicy Deny dynamodb:UpdateTable; UpdateTable DP=<same>; UpdateTimeToLive; TagResource;
    DeleteTable
  - measurements: deny_enforced_after_put_s=4.1
  - handling: not handled in the controller (as of commit 34b85e6)
  - related: [DDB-TABLE-324](../table-policy-kinesis-autoscaling.md#ddb-table-324), [DDB-TABLE-023](../table-streams-encryption-class.md#ddb-table-023), [DDB-TABLE-210](../table-policy-kinesis-autoscaling.md#ddb-table-210), [DDB-TABLE-270](../table-policy-kinesis-autoscaling.md#ddb-table-270), [DDB-TABLE-431](../table-streams-encryption-class.md#ddb-table-431), [DDB-TABLE-172](../service.md#ddb-table-172),
    [DDB-TABLE-348](../table-policy-kinesis-autoscaling.md#ddb-table-348), [DDB-TABLE-119](../table-streams-encryption-class.md#ddb-table-119), [DDB-TABLE-011](../service.md#ddb-table-011) · evidence: table/creative/policy-denies-finalizer

## Notes

AccessDeniedException on UpdateTable is not a credentials problem the controller can retry away: it is the
table's own policy. A controller should surface it as a terminal condition and keep syncing the sub-resources
that are still permitted (TTL, PITR, tags, insights, policy itself), instead of aborting the whole reconcile
on the first error.
