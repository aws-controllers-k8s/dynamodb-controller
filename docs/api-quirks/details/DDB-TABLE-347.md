<!-- generated: ack-api-quirks dynamodb 2026-10-09 model=2012-08-10 controller=34b85e6 -->
# DDB-TABLE-347: ExpectedRevisionId = RevisionId just returned by Put is never PolicyNotFound: ResourceInUse (~0.6-1.5s), Throttling until 15.0s, then OK
_Full entry and notes of one finding; its summary entry is in
[table-policy-kinesis-autoscaling.md](../table-policy-kinesis-autoscaling.md). Generated from ack-api-quirks
`services/dynamodb` (model 2012-08-10, controller commit 34b85e6); edit the finding in the lab, not here._

## Finding

- <a id="ddb-table-347"></a>**DDB-TABLE-347** `error-code` · impact high · unhandled (not handled in controller) · verified 2026-10-09
  **ExpectedRevisionId = RevisionId just returned by Put is never PolicyNotFound: ResourceInUse (~0.6-1.5s), Throttling until 15.0s, then OK**
  Right after PutResourcePolicy returned RevisionId R (Get still PolicyNotFoundException), Put(other,
  ExpectedRevisionId=R) at offsets 0..20 s gave ['ResourceInUseException->ThrottlingException->OK'] with first
  success at [15.088, 15.103] s; Delete(ExpectedRevisionId=R) gave
  ['ResourceInUseException->ThrottlingException->OK'], first success at [15.039, 15.04] s. Issuing the Put the
  instant Get first showed R ([1.308, 1.747] s) gave ['ResourceInUseException->ThrottlingException->OK',
  'ThrottlingException->OK'] - i.e. read visibility does not unlock writes; the per-table 15 s cooldown does.
  ResourceInUseException message: 'Table ... is pending previous resource-based policy update';
  ThrottlingException: 'modified within the previous 15000 milliseconds. Please try again after <ts>'. No
  PolicyNotFoundException was returned at any offset, so the revision check already sees the new revision
  while Get does not.
  - ACK: requeue, terminal_codes, custom_update · ops: PutResourcePolicy, DeleteResourcePolicy · fields:
    ExpectedRevisionId, RevisionId
  - repro: Put -> R; Put(other, ExpectedRevisionId=R) at 0, 0.1, 0.25 ... 20 s until OK; same with Delete;
    same starting when Get first returns R
  - measurements: put_expected_first_ok_s=[15.088, 15.103], delete_expected_first_ok_s=[15.039, 15.04],
    first_visible_s=[1.308, 1.747]
  - handling: not handled in the controller (as of commit 34b85e6)
  - related: [DDB-TABLE-206](../table-policy-kinesis-autoscaling.md#ddb-table-206), [DDB-TABLE-209](../table-policy-kinesis-autoscaling.md#ddb-table-209), [DDB-TABLE-244](../table-policy-kinesis-autoscaling.md#ddb-table-244), [DDB-TABLE-245](../table-policy-kinesis-autoscaling.md#ddb-table-245), [DDB-TABLE-246](../table-policy-kinesis-autoscaling.md#ddb-table-246), [DDB-TABLE-248](../table-policy-kinesis-autoscaling.md#ddb-table-248),
    [DDB-TABLE-205](../table-policy-kinesis-autoscaling.md#ddb-table-205), [DDB-TABLE-247](../table-policy-kinesis-autoscaling.md#ddb-table-247), [DDB-TABLE-464](../service.md#ddb-table-464), [DDB-TABLE-445](../service.md#ddb-table-445), [DDB-TABLE-348](../table-policy-kinesis-autoscaling.md#ddb-table-348), [DDB-TABLE-213](../table-streams-encryption-class.md#ddb-table-213), [DDB-TABLE-214](../table-policy-kinesis-autoscaling.md#ddb-table-214) ·
    hypotheses: H-S-108, H-S-027, H-S-006 · evidence: table/consistency-windows/policy-stale-read-sequence

## Notes

Qualifies [DDB-TABLE-206](../table-policy-kinesis-autoscaling.md#ddb-table-206)/209: a controller that chains Put -> Put(ExpectedRevisionId=<returned>) within 15 s
must treat ResourceInUseException and ThrottlingException as retry-after (both carry no revision information);
PolicyNotFoundException in that window would mean a genuinely different revision (someone else wrote).
