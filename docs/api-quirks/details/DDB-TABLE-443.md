<!-- generated: ack-api-quirks dynamodb 2026-10-09 model=2012-08-10 controller=34b85e6 -->
# DDB-TABLE-443: LimitExceededException message catalogue: 3 transient texts (tag lock ~2 s, one online index, hourly decrease) vs 24h/30d/account quotas
_Full entry and notes of one finding; its summary entry is in [service.md](../service.md). Generated from
ack-api-quirks `services/dynamodb` (model 2012-08-10, controller commit 34b85e6); edit the finding in the lab,
not here._

## Finding

- <a id="ddb-table-443"></a>**DDB-TABLE-443** `error-code` · impact high · unhandled (not handled in controller) · verified 2026-10-09
  **LimitExceededException message catalogue: 3 transient texts (tag lock ~2 s, one online index, hourly decrease) vs 24h/30d/account quotas**
  LimitExceededException (HTTP 400) texts fall into four classes. TRANSIENT-RETRY: 'Subscriber limit exceeded:
  Table tags are being updated: <TBL>' (~1.5-3 s). TRANSIENT-WAIT-FOR-STATE: 'Subscriber limit exceeded: Only
  1 online index can be created or deleted simultaneously per table' (IndexStatus ACTIVE), 'Subscriber limit
  exceeded: Only 50 restore operations can be done simultaneously' (restores finish). PERMANENT-QUOTA with a
  stated wait: 'Provisioned throughput decreases are limited within a given UTC day... at most once every 3600
  seconds...' (1 h / 00:00 UTC), 'Encryption mode changes are limited in the 24h window ending at <TS>... once
  every 21600 seconds... Next changes can be made at <TS>.' (6 h), 'Updates to TableClass are limited to 2
  times in 30 day(s).' (30 d, also 'Limit exceeded for replica in <REGION>. Updates to TableClass...'), 'The
  requested ReadCapacityUnits, N, is above the per table maximum for the account in <REGION>. Per table
  maximum: 40000...', 'This request would have caused the ReadCapacityUnits limit to be exceeded for the
  account in <REGION>...', '...exceeds TableMaxReadCapacityUnits of the account in region <REGION>' (quota
  increase). PERMANENT-SPEC: 'Subscriber limit exceeded: Number of global secondary indexes exceeds per-table
  limit of 20'. Decrease/encryption texts embed the next allowed time; TableClass does not.
  - ACK: terminal_codes, requeue · ops: UpdateTable, TagResource, UntagResource, RestoreTableFromBackup,
    CreateTable · fields: TableClass, SSESpecification, ProvisionedThroughput, Tags
  - repro: TagResource then UntagResource immediately; three TableClass switches on one table; see
    catalogue.py
  - handling: not handled in the controller (as of commit 34b85e6)
  - related: [DDB-TABLE-171](../table-throughput-billing.md#ddb-table-171), [DDB-TABLE-103](../service.md#ddb-table-103), [DDB-TABLE-283](../table-streams-encryption-class.md#ddb-table-283), [DDB-TABLE-081](../table-streams-encryption-class.md#ddb-table-081), [DDB-TABLE-282](../table-restore.md#ddb-table-282) · evidence:
    table/creative/error-message-regions

## Notes

Classes: RETRY = TRANSIENT-RETRY, clears by itself (typical wait given); WAIT = TRANSIENT-WAIT-FOR-STATE,
clears when the named status changes; SPEC = PERMANENT-SPEC, user must change the spec / fix an external
dependency; QUOTA = PERMANENT-QUOTA, needs a quota increase or the stated 1h/24h/30d window. Messages mined
from 119 past evidence.jsonl files (9562 error records, 29 codes; mine_errors.py -> mined-catalogue.txt in
this probe dir) plus this probe's live runs in us-west-2 and us-east-1; <TBL>/<ARN>/<TS>/<N>/<REGION>/<IDX>
mark request-specific tokens. Machine-readable form: catalogue.py in the probe directory.

CATALOGUE (LimitExceededException):
- [RETRY ~~1.5-3 s (until ListTagsOfResource reflects the previous write)] Subscriber limit exceeded: Table
tags are being updated: <TBL> (ops: TagResource,UntagResource)
- [WAIT ~IndexStatus of the in-flight GSI = ACTIVE / gone (minutes)] Subscriber limit exceeded: Only 1 online
index can be created or deleted simultaneously per table (ops: UpdateTable)
- [WAIT ~other restores reach ACTIVE] Subscriber limit exceeded: Only 50 restore operations can be done
simultaneously (ops: RestoreTableFromBackup)
- [QUOTA ~3600 s after the last decrease or 00:00 UTC] Subscriber limit exceeded: Provisioned throughput
decreases are limited within a given UTC day. After the first 4 decreases, each subsequent decrease in the
same UTC day can be performed at most once every 3600 seconds. Number of decreases today: <N>. Last decrease
at <weekday, date at time> (ops: UpdateTable; embeds a human-formatted timestamp)
- [QUOTA ~until the embedded 'Next changes can be made at' timestamp (<= 6 h)] Subscriber limit exceeded:
Encryption mode changes are limited in the 24h window ending at <TS>. After the first 4 change, each
subsequent change in the same window can be performed at most once every 21600 seconds. Number of updates
today: <N>. Last change at <TS>. Next changes can be made at <TS>. (ops: UpdateTable)
- [QUOTA ~30 days (or re-create the table)] Subscriber limit exceeded: Updates to TableClass are limited to 2
times in 30 day(s). (ops: UpdateTable; no-op re-sends are also rejected once spent)
- [QUOTA ~30 days] Limit exceeded for replica in <REGION>. Updates to TableClass are limited to 2 times in 30
day(s) (ops: UpdateTable)
- [SPEC] Subscriber limit exceeded: Number of global secondary indexes exceeds per-table limit of 20 (ops:
UpdateTable; same rule is ValidationException at CreateTable)
- [QUOTA ~quota increase] The requested ReadCapacityUnits, <N>, is above the per table maximum for the account
in <REGION>. Per table maximum: 40000. Refer to the Amazon DynamoDB Developer Guide for current limits and how
to request higher limits. (ops: CreateTable,UpdateTable; also '... for index <IDX>, <N>, is above the per
index maximum ...')
- [QUOTA ~quota increase or free capacity] This request would have caused the ReadCapacityUnits limit to be
exceeded for the account in <REGION>. Current ReadCapacityUnits reserved by the account: <N>. Limit: <N>.
Requested: <N>. Refer to ... (ops: CreateTable)
- [QUOTA ~quota increase] Subscriber limit exceeded: Requested MaxReadRequestUnits for OnDemandThroughput for
table exceeds TableMaxReadCapacityUnits of the account in region <REGION> (ops: UpdateTable; also 'for index :
<IDX>' and 'Requested ReadUnitsPerSecond for WarmThroughput for index <IDX> exceeds ...')

LIVE this run (region/label: http code latency 'message'):
us-east-1/lee_tag_lock: 400 LimitExceededException 78ms 'Subscriber limit exceeded: Table tags are being
updated: ackq-e3283a-emr-e1'
us-west-2/lee_tag_lock: 400 LimitExceededException 15ms 'Subscriber limit exceeded: Table tags are being
updated: ackq-e3283a-emr-w2'
us-east-1/tableclass_1: 200 OK 112ms
us-west-2/tableclass_1: 200 OK 37ms
us-east-1/tableclass_2: 200 OK 92ms
us-west-2/tableclass_2: 200 OK 32ms
us-east-1/tableclass_3: 400 LimitExceededException 71ms 'Subscriber limit exceeded: Updates to TableClass are
limited to 2 times in 30 day(s).'
us-west-2/tableclass_3: 400 LimitExceededException 15ms 'Subscriber limit exceeded: Updates to TableClass are
limited to 2 times in 30 day(s).'

Extends [DDB-TABLE-171](../table-throughput-billing.md#ddb-table-171) (index/throughput texts) with the tag-lock, TableClass, encryption-window,
replica-TableClass and restore-concurrency texts and an explicit class per text.
