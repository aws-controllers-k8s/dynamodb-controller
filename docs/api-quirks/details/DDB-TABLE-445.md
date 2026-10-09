<!-- generated: ack-api-quirks dynamodb 2026-10-09 model=2012-08-10 controller=34b85e6 -->
# DDB-TABLE-445: ThrottlingException catalogue: 'Rate exceeded' / account control-plane rate (back off) vs per-table 15 s cooldowns with embedded retry-after
_Full entry and notes of one finding; its summary entry is in [service.md](../service.md). Generated from
ack-api-quirks `services/dynamodb` (model 2012-08-10, controller commit 34b85e6); edit the finding in the lab,
not here._

## Finding

- <a id="ddb-table-445"></a>**DDB-TABLE-445** `error-code` · impact high · unhandled (not handled in controller) · verified 2026-10-09
  **ThrottlingException catalogue: 'Rate exceeded' / account control-plane rate (back off) vs per-table 15 s cooldowns with embedded retry-after**
  Five ThrottlingException (HTTP 400) texts, all TRANSIENT-RETRY. Rate limits: 'Rate exceeded'
  (per-connection/per-API read buckets, retry in ~1 s) and 'The rate of control plane requests made by this
  account is too high' (account mutation limiter, ~1 s). Per-table cooldowns that are NOT rate limits:
  'Deletion protection setting for table <TBL> modified within the previous 15000 milliseconds. Please try
  again after <TS>' and 'Resource-based policy for table <TBL> modified within the previous 15000
  milliseconds. Please try again after <TS>.' (also '...for stream <label>...'). The cooldown texts carry an
  ISO-8601 retry-after timestamp with millisecond precision, so exact or suffix matching breaks on every
  occurrence while the prefix 'Deletion protection setting for table' / 'Resource-based policy for' is stable;
  the timestamp can be parsed to compute the requeue delay (<= 15 s).
  - ACK: requeue, terminal_codes · ops: UpdateTable, PutResourcePolicy, DeleteResourcePolicy,
    DescribeTimeToLive, TagResource, CreateTable, DeleteTable
  - repro: UpdateTable(DeletionProtectionEnabled) twice within 15 s; PutResourcePolicy twice within 15 s
  - handling: not handled in the controller (as of commit 34b85e6)
  - related: [DDB-TABLE-003](../table-streams-encryption-class.md#ddb-table-003), [DDB-TABLE-206](../table-policy-kinesis-autoscaling.md#ddb-table-206), [DDB-TABLE-053](../service.md#ddb-table-053), [DDB-TABLE-403](../service.md#ddb-table-403), [DDB-TABLE-247](../table-policy-kinesis-autoscaling.md#ddb-table-247), [DDB-TABLE-347](../table-policy-kinesis-autoscaling.md#ddb-table-347),
    [DDB-TABLE-464](../service.md#ddb-table-464), [DDB-TABLE-348](../table-policy-kinesis-autoscaling.md#ddb-table-348), [DDB-TABLE-213](../table-streams-encryption-class.md#ddb-table-213), [DDB-TABLE-205](../table-policy-kinesis-autoscaling.md#ddb-table-205), [DDB-TABLE-099](../service.md#ddb-table-099), [DDB-TABLE-360](../table-subresources.md#ddb-table-360), [DDB-TABLE-434](../service.md#ddb-table-434),
    [DDB-TABLE-435](../table-streams-encryption-class.md#ddb-table-435), [DDB-TABLE-383](../table-streams-encryption-class.md#ddb-table-383), [DDB-TABLE-117](../table-streams-encryption-class.md#ddb-table-117), [DDB-TABLE-377](../service.md#ddb-table-377) · evidence:
    table/creative/error-message-regions

## Notes

Classes: RETRY = TRANSIENT-RETRY, clears by itself (typical wait given); WAIT = TRANSIENT-WAIT-FOR-STATE,
clears when the named status changes; SPEC = PERMANENT-SPEC, user must change the spec / fix an external
dependency; QUOTA = PERMANENT-QUOTA, needs a quota increase or the stated 1h/24h/30d window. Messages mined
from 119 past evidence.jsonl files (9562 error records, 29 codes; mine_errors.py -> mined-catalogue.txt in
this probe dir) plus this probe's live runs in us-west-2 and us-east-1; <TBL>/<ARN>/<TS>/<N>/<REGION>/<IDX>
mark request-specific tokens. Machine-readable form: catalogue.py in the probe directory.

CATALOGUE (ThrottlingException):
- [RETRY ~~1 s (bursty token bucket, per API / per HTTP connection)] Rate exceeded (ops:
DescribeTimeToLive,DescribeExport,DescribeBackup,DeleteBackup,ListBackups,DescribeLimits,DescribeContinuousBackups)
- [RETRY ~~1 s (account-wide mutation limiter ~1 call/s)] The rate of control plane requests made by this
account is too high (ops: UpdateTable,DeleteTable,TagResource,CreateTable)
- [RETRY ~<= 15 s; retry-after timestamp embedded (ms precision)] Deletion protection setting for table <TBL>
modified within the previous 15000 milliseconds. Please try again after <TS> (ops: UpdateTable)
- [RETRY ~<= 15 s; retry-after timestamp embedded] Resource-based policy for table <TBL> modified within the
previous 15000 milliseconds. Please try again after <TS>. (ops: PutResourcePolicy,DeleteResourcePolicy;
trailing period, unlike the DP text)
- [RETRY ~<= 15 s] Resource-based policy for stream <TS> modified within the previous 15000 milliseconds.
Please try again after <TS>. (ops: PutResourcePolicy,DeleteResourcePolicy)

LIVE this run (region/label: http code latency 'message'):
us-east-1/dp_first: 200 OK 112ms
us-west-2/dp_first: 200 OK 15ms
us-east-1/thr_dp_second: 400 ThrottlingException 69ms 'Deletion protection setting for table
ackq-e3283a-emr-e1 modified within the previous 15000 milliseconds. Please try again after
2026-10-09T05:53:25.330Z'
us-west-2/thr_dp_second: 400 ThrottlingException 12ms 'Deletion protection setting for table
ackq-e3283a-emr-w2 modified within the previous 15000 milliseconds. Please try again after
2026-10-09T05:52:48.688Z'
us-east-1/policy_put: 200 OK 176ms
us-west-2/policy_put: 200 OK 108ms
us-east-1/riu_policy_put_pending: 400 ResourceInUseException 117ms 'Attempt to change a resource which is
still in use: Table is pending previous resource-based policy update: ackq-e3283a-emr-e1'
us-west-2/riu_policy_put_pending: 400 ResourceInUseException 62ms 'Attempt to change a resource which is still
in use: Table is pending previous resource-based policy update: ackq-e3283a-emr-w2'
us-east-1/thr_policy_put_15s: 400 ThrottlingException 128ms 'Resource-based policy for table
ackq-e3283a-emr-e1 modified within the previous 15000 milliseconds. Please try again after
2026-10-09T05:53:25.594Z.'
us-west-2/thr_policy_put_15s: 400 ThrottlingException 58ms 'Resource-based policy for table ackq-e3283a-emr-w2
modified within the previous 15000 milliseconds. Please try again after 2026-10-09T05:52:48.794Z.'
us-east-1/thr_policy_delete_15s: 400 ThrottlingException 80ms 'Resource-based policy for table
ackq-e3283a-emr-e1 modified within the previous 15000 milliseconds. Please try again after
2026-10-09T05:53:25.594Z.'
us-west-2/thr_policy_delete_15s: 400 ThrottlingException 15ms 'Resource-based policy for table
ackq-e3283a-emr-w2 modified within the previous 15000 milliseconds. Please try again after
2026-10-09T05:52:48.794Z.'
us-east-1/thr_ttl_rate_exceeded: 400 ThrottlingException 63ms 'Rate exceeded'
us-west-2/thr_ttl_rate_exceeded: 400 ThrottlingException 6ms 'Rate exceeded'
