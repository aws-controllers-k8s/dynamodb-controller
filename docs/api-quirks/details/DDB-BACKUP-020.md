<!-- generated: ack-api-quirks dynamodb 2026-10-09 model=2012-08-10 controller=34b85e6 -->
# DDB-BACKUP-020: DescribeBackup and ListBackups throttle with ThrottlingException 'Rate exceeded' (HTTP 400), bursty bucket: ~10/s describe, 5/s list
_Full entry and notes of one finding; its summary entry is in [backup.md](../backup.md). Generated from
ack-api-quirks `services/dynamodb` (model 2012-08-10, controller commit 34b85e6); edit the finding in the lab,
not here._

## Finding

- <a id="ddb-backup-020"></a>**DDB-BACKUP-020** `quota-limit` · impact high · unhandled (not handled in controller) · verified 2026-10-09
  **DescribeBackup and ListBackups throttle with ThrottlingException 'Rate exceeded' (HTTP 400), bursty bucket: ~10/s describe, 5/s list**
  30 concurrent DescribeBackup calls (0.23 s) and 60 within 2 s all returned 200, but 40 sequential calls
  fired back-to-back got 13 successes and then 27x ThrottlingException (HTTP 400, 'Rate exceeded') starting at
  0.165 s. 12 sequential ListBackups calls: exactly 5 succeeded, the remaining 7 (from 0.111 s) returned
  ThrottlingException 'Rate exceeded'; 12 concurrent ListBackups after a 3 s pause all succeeded. 25
  DeleteBackup calls (12 sequential in 0.38 s + 13 concurrent) all succeeded. A DescribeBackup right after the
  ListBackups throttle succeeded (per-operation buckets). LimitExceededException was never returned for reads.
  - ACK: requeue, none · ops: DescribeBackup, ListBackups, DeleteBackup
  - repro: burst DescribeBackup x30 concurrent / x40 sequential; ListBackups x12 sequential; DeleteBackup x12
  - measurements: describe_sequential_ok_before_throttle=13, describe_first_throttle_at_s=0.165,
    list_ok_before_throttle=5, list_first_throttle_at_s=0.111
  - handling: not handled in the controller (as of commit 34b85e6)
  - related: [DDB-EXPORT-007](../export.md#ddb-export-007), [DDB-BACKUP-014](../backup.md#ddb-backup-014), [DDB-BACKUP-019](../backup.md#ddb-backup-019) · hypotheses: H-B-009, H-B-031 · evidence:
    backup/limits/burst

## Notes

H-B-009 partially confirmed (small concurrent bursts pass; sustained >10/s does not). H-B-031 refuted for
DescribeBackup: the code is ThrottlingException, not LimitExceededException. A controller polling many Backup
resources needs client-side pacing (the harness clients have retries disabled; SDK default retries would mask
this as latency).
