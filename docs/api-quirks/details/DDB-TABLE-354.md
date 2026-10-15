<!-- generated: ack-api-quirks dynamodb 2026-10-09 model=2012-08-10 controller=34b85e6 -->
# DDB-TABLE-354: Version omitted -> Get adds Version 2008-10-17 (not 2012), yet omitted vs explicit 2008 are DIFFERENT documents for idempotency (ping-pong)
_Full entry and notes of one finding; its summary entry is in
[table-policy-kinesis-autoscaling.md](../table-policy-kinesis-autoscaling.md). Generated from ack-api-quirks
`services/dynamodb` (model 2012-08-10, controller commit 34b85e6); edit the finding in the lab, not here._

## Finding

- <a id="ddb-table-354"></a>**DDB-TABLE-354** `normalization` · impact high · unhandled (not handled in controller) · verified 2026-10-09
  **Version omitted -> Get adds Version 2008-10-17 (not 2012), yet omitted vs explicit 2008 are DIFFERENT documents for idempotency (ping-pong)**
  A policy sent without Version reads back with 2008-10-17 (IAM's legacy default; explicit 2008-10-17 and
  2012-10-17 are preserved). But the two forms are not equivalent to the RevisionId/idempotency check: Put(no
  Version) -> rev 1791521505559; Put(the Get output with 2008-10-17) 16 s later -> NEW rev 1791521523543 (a
  real change, not the usual no-op; inside the 15 s window it is even ThrottlingException); Put(no Version)
  again -> new rev again (True); Put(no Version) twice -> no-op (True). So the Get output is NOT a fixpoint
  for a Version-less spec: a controller that re-Puts what it read (or compares spec vs Get) will churn
  RevisionIds every reconcile and hit the 15 s ThrottlingException.
  - ACK: is_iam_policy, compare.is_ignored+delta_pre_compare, annotation-shadow-state · ops:
    PutResourcePolicy, GetResourcePolicy · fields: Policy.Version, RevisionId
  - repro: Put({Statement:[...]}) ; Get -> Version 2008-10-17 ; 16 s ; Put(Get output) -> new RevisionId ; 16
    s ; Put(no Version) -> new RevisionId ; Put(no Version) -> same
  - handling: not handled in the controller (as of commit 34b85e6)
  - related: [DDB-TABLE-207](../table-policy-kinesis-autoscaling.md#ddb-table-207), [DDB-TABLE-208](../table-policy-kinesis-autoscaling.md#ddb-table-208), [DDB-TABLE-211](../table-policy-kinesis-autoscaling.md#ddb-table-211), [DDB-TABLE-244](../table-policy-kinesis-autoscaling.md#ddb-table-244), [DDB-TABLE-207](../table-policy-kinesis-autoscaling.md#ddb-table-207), [DDB-TABLE-351](../table-policy-kinesis-autoscaling.md#ddb-table-351),
    [DDB-TABLE-352](../table-policy-kinesis-autoscaling.md#ddb-table-352), [DDB-TABLE-353](../table-policy-kinesis-autoscaling.md#ddb-table-353), [DDB-TABLE-355](../table-policy-kinesis-autoscaling.md#ddb-table-355), [DDB-TABLE-356](../table-policy-kinesis-autoscaling.md#ddb-table-356) · hypotheses: H-S-028 · evidence:
    table/round-trip/policy-canonicalization

## Notes

Qualifies [DDB-TABLE-207](../table-policy-kinesis-autoscaling.md#ddb-table-207) (equivalent re-Puts are no-ops): equivalence is computed on the submitted document
with Version treated as a literal field, while Get fills the default. Controllers should always send an
explicit Version (2012-10-17) and treat a missing Version in the spec as 2008-10-17 when diffing - or better,
default it to 2012-10-17 before Put (policy variables need it).
