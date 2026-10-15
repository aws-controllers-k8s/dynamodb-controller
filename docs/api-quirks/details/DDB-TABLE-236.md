<!-- generated: ack-api-quirks dynamodb 2026-10-09 model=2012-08-10 controller=34b85e6 -->
# DDB-TABLE-236: DELETING: policy/Kinesis reads 200; Put/Delete policy -> ResourceInUse; Enable/Disable -> Validation; gone -> ResourceNotFound, no ghosts
_Full entry and notes of one finding; its summary entry is in
[table-policy-kinesis-autoscaling.md](../table-policy-kinesis-autoscaling.md). Generated from ack-api-quirks
`services/dynamodb` (model 2012-08-10, controller commit 34b85e6); edit the finding in the lab, not here._

## Finding

- <a id="ddb-table-236"></a>**DDB-TABLE-236** `error-code` · impact high · unhandled (not handled in controller) · verified 2026-10-09
  **DELETING: policy/Kinesis reads 200; Put/Delete policy -> ResourceInUse; Enable/Disable -> Validation; gone -> ResourceNotFound, no ghosts**
  While TableStatus=DELETING (table with a policy and an ACTIVE destination): GetResourcePolicy 200 OK (old
  RevisionId); DescribeKinesisStreamingDestination 200 OK (entry ACTIVE then UPDATING); PutResourcePolicy
  ResourceInUseException (HTTP 400) 'Attempt to change a resource which is still in use: Table is being
  deleted: ackq-f57f9f-pk-a'; DeleteResourcePolicy ResourceInUseException (HTTP 400) 'Attempt to change a
  resource which is still in use: Table is being deleted: ackq-f57f9f-pk-a';
  EnableKinesisStreamingDestination(same stream) ValidationException (HTTP 400) 'Table is not in a valid state
  to enable Kinesis Streaming Destination: EnableKinesisStreamingDestination must be DISABLED or ENABLE_FAILED
  to perform '; Enable(other stream) ValidationException (HTTP 400) 'Table is not in a valid state to enable
  Kinesis Streaming Destination: EnableKinesisStreamingDestination must be DISABLED or ENABLE_FAILED to
  perform '; UpdateKinesisStreamingDestination 200 OK; DisableKinesisStreamingDestination ValidationException
  (HTTP 400) 'Table is not in a valid state to enable Kinesis Streaming Destination:
  KinesisStreamingDestination must be ACTIVE to perform DISABLE operation.'. DELETING lasted ~5.43s. From the
  first DescribeTable ResourceNotFoundException (t=5.43s) on, 60 more seconds of 1s polling produced 0 ghost
  200s: GetResourcePolicy -> ResourceNotFoundException 'Requested resource not found: Table: ackq-f57f9f-pk-a
  not found' and DescribeKinesisStreamingDestination -> ResourceNotFoundException 'Requested resource not
  found: Table: ackq-f57f9f-pk-a not found' from the very first poll.
  - ACK: exceptions.404, terminal_codes, deletable.when · ops: GetResourcePolicy,
    DescribeKinesisStreamingDestination, PutResourcePolicy, DeleteResourcePolicy,
    EnableKinesisStreamingDestination, DisableKinesisStreamingDestination, UpdateKinesisStreamingDestination
  - repro: table with policy + ACTIVE destination: DeleteTable; every 1s call Get/DescribeKinesis (+ each
    mutator once while DELETING); continue 60s after ResourceNotFoundException
  - measurements: ghost_reads_after_gone=0, deleting_duration_s=5.43
  - handling: not handled in the controller (as of commit 34b85e6)
  - related: [DDB-TABLE-122](../table-policy-kinesis-autoscaling.md#ddb-table-122), [DDB-TABLE-374](../service.md#ddb-table-374), [DDB-TABLE-097](../table-subresources.md#ddb-table-097), [DDB-TABLE-453](../table-subresources.md#ddb-table-453), [DDB-TABLE-088](../table-subresources.md#ddb-table-088), [DDB-TABLE-235](../table-policy-kinesis-autoscaling.md#ddb-table-235),
    [DDB-TABLE-342](../table-subresources.md#ddb-table-342), [DDB-TABLE-214](../table-policy-kinesis-autoscaling.md#ddb-table-214), [DDB-TABLE-271](../table-policy-kinesis-autoscaling.md#ddb-table-271), [DDB-TABLE-436](../table-policy-kinesis-autoscaling.md#ddb-table-436), [DDB-TABLE-377](../service.md#ddb-table-377), [DDB-TABLE-299](../table-policy-kinesis-autoscaling.md#ddb-table-299), [DDB-TABLE-300](../table-policy-kinesis-autoscaling.md#ddb-table-300),
    [DDB-TABLE-301](../table-policy-kinesis-autoscaling.md#ddb-table-301), [DDB-TABLE-302](../table-policy-kinesis-autoscaling.md#ddb-table-302), [DDB-TABLE-304](../table-policy-kinesis-autoscaling.md#ddb-table-304), [DDB-TABLE-269](../table-policy-kinesis-autoscaling.md#ddb-table-269), [DDB-TABLE-268](../table-policy-kinesis-autoscaling.md#ddb-table-268), [DDB-TABLE-319](../table-policy-kinesis-autoscaling.md#ddb-table-319), [DDB-TABLE-303](../table-policy-kinesis-autoscaling.md#ddb-table-303),
    [DDB-TABLE-234](../table-policy-kinesis-autoscaling.md#ddb-table-234), [DDB-TABLE-460](../table-policy-kinesis-autoscaling.md#ddb-table-460) · hypotheses: H-S-118, H-S-119, H-S-120 · evidence:
    table/state-machine/policy-kinesis-admissibility

## Notes

H-S-118 partially confirmed: Get and Describe kinesis return 200 during DELETING, but the destination shows
UPDATING (not DISABLING). H-S-119 REFUTED for policy/kinesis: no ghost 200s after the table is gone, and both
use ResourceNotFoundException 'Requested resource not found: Table: X not found'. H-S-120 partially confirmed:
Put/Delete policy -> ResourceInUseException 'Table is being deleted'; Enable/Disable -> ValidationException
(not ResourceInUse); UpdateKinesisStreamingDestination -> 200 (!) on a DELETING table.

Contradiction with [DDB-TABLE-122](../table-policy-kinesis-autoscaling.md#ddb-table-122): 122 says PutResourcePolicy succeeds (200, RevisionId) during DELETING; 236
says ResourceInUseException 'Table is being deleted'. [DDB-TABLE-374](../service.md#ddb-table-374) reconciles: an IDENTICAL re-Put
(RevisionId no-op) is 200 for the first ~1.63 s of DELETING, a CHANGED document is ResourceInUse from +0.03 s
in 3/3 trials Resolution: keep both; 374 is canonical; 122's title overgeneralizes (see title_fixes)
