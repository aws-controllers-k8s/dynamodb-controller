<!-- generated: ack-api-quirks dynamodb 2026-10-09 model=2012-08-10 controller=34b85e6 -->
# DDB-TABLE-173: With SDK standard retries (3 attempts) a back-to-back TagResource+UntagResource pair still fails 4/5 times (LimitExceededException ~1.7 s)
_Full entry and notes of one finding; its summary entry is in [service.md](../service.md). Generated from
ack-api-quirks `services/dynamodb` (model 2012-08-10, controller commit 34b85e6); edit the finding in the lab,
not here._

## Finding

- <a id="ddb-table-173"></a>**DDB-TABLE-173** `quota-limit` · impact high · unhandled (not handled in controller) · verified 2026-10-08
  **With SDK standard retries (3 attempts) a back-to-back TagResource+UntagResource pair still fails 4/5 times (LimitExceededException ~1.7 s)**
  Using botocore standard retry mode with total_max_attempts=3 (same shape as aws-sdk-go-v2's default retryer,
  which also treats LimitExceededException as a throttle), TagResource immediately followed by UntagResource
  on the same table: the second call ended in LimitExceededException 'Table tags are being updated' in 4/5
  runs after exhausting its retries (wall-clock 1.55-1.75 s), and succeeded in 1/5 (after 2 retries, 0.94 s).
  In an earlier run with one extra attempt available the pair succeeded 4/5 times at 1.9-5.3 s wall-clock.
  Unlocked tag calls take 10-50 ms.
  - ACK: tags.custom-sync, requeue, e2e-timing · ops: TagResource, UntagResource
  - repro: boto3 Config(retries={'max_attempts': 3, 'mode': 'standard'}); TagResource then UntagResource
    back-to-back; 5 runs
  - measurements: second_call_wall_ms=[1550, 1736, 1748, 1738, 944], success=1, n=5
  - handling: not handled in the controller (as of commit 34b85e6)
  - related: [DDB-TABLE-103](../service.md#ddb-table-103), [DDB-TABLE-440](../table.md#ddb-table-440), [DDB-TABLE-441](../table.md#ddb-table-441), [DDB-TABLE-130](../service.md#ddb-table-130), [DDB-TABLE-104](../table.md#ddb-table-104), [DDB-TABLE-108](../service.md#ddb-table-108) ·
    evidence: table/tags/write-lock-characterization

## Notes

Shows what a controller using default SDK retries actually experiences: the jittered exponential backoff of 3
attempts (~0.1-2 s total) is shorter than the ~1.7 s lock most of the time, so the second tag mutation of a
reconcile fails outright and must be requeued; each reconcile that needs both an untag and a tag will
therefore take at least two reconcile rounds unless the controller sleeps/polls ListTagsOfResource between the
two calls.
