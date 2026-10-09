<!-- generated: ack-api-quirks dynamodb 2026-10-09 model=2012-08-10 controller=34b85e6 -->
# DDB-TABLE-434: Account control-plane limiter: async UpdateTable/TagResource/DeleteTable 3-5 of 10 concurrent throttled; no-op UpdateTable, DP, TTL, reads 0
_Full entry and notes of one finding; its summary entry is in [service.md](../service.md). Generated from
ack-api-quirks `services/dynamodb` (model 2012-08-10, controller commit 34b85e6); edit the finding in the lab,
not here._

## Finding

- <a id="ddb-table-434"></a>**DDB-TABLE-434** `quota-limit` · impact medium · unhandled (not handled in controller) · verified 2026-10-09
  **Account control-plane limiter: async UpdateTable/TagResource/DeleteTable 3-5 of 10 concurrent throttled; no-op UpdateTable, DP, TTL, reads 0**
  10 idle PAY_PER_REQUEST tables, one 10-thread burst per step with settle gaps (span 0.02-0.18 s each): REAL
  UpdateTable(StreamSpecification enable) 7 OK / 3 ThrottlingException 'The rate of control plane requests
  made by this account is too high'; no-op UpdateTable(BillingMode=PAY_PER_REQUEST) 10 OK;
  UpdateTable(DeletionProtectionEnabled=true) 10 OK and (=false) 10 OK; TagResource 7 OK / 3
  ThrottlingException (same message); UpdateTimeToLive(enable) 10 OK; 40 concurrent reads (DescribeTable,
  ListTagsOfResource, DescribeContinuousBackups, DescribeTimeToLive) 40 OK; DeleteTable 5 OK / 5
  ThrottlingException, all 5 succeeded on one retry 1 s later. Throttled calls return in ~35-170 ms.
  - ACK: requeue, e2e-timing · ops: UpdateTable, TagResource, DeleteTable, UpdateTimeToLive, DescribeTable,
    ListTagsOfResource, DescribeContinuousBackups, DescribeTimeToLive · fields: StreamSpecification,
    DeletionProtectionEnabled, BillingMode, Tags
  - repro: 10 tables; ThreadPoolExecutor(10) UpdateTable stream enable; wait; ThreadPoolExecutor(10)
    UpdateTable DP=true; ThreadPoolExecutor(10) TagResource; ThreadPoolExecutor(10) DeleteTable
  - measurements: real_update_throttled_of_10=3, noop_update_throttled_of_10=0, dp_toggle_throttled_of_10=0,
    tag_throttled_of_10=3, ttl_throttled_of_10=0, reads_throttled_of_40=0, delete_throttled_of_10=5
  - handling: not handled in the controller (as of commit 34b85e6)
  - related: [DDB-TABLE-132](../service.md#ddb-table-132), [DDB-TABLE-053](../service.md#ddb-table-053), [DDB-TABLE-130](../service.md#ddb-table-130), [DDB-TABLE-099](../service.md#ddb-table-099), [DDB-TABLE-435](../table-streams-encryption-class.md#ddb-table-435), [DDB-TABLE-383](../table-streams-encryption-class.md#ddb-table-383),
    [DDB-TABLE-445](../service.md#ddb-table-445) · evidence: table/creative/throttle-exemptions

## Notes

Refines [DDB-TABLE-053](../service.md#ddb-table-053)/132/130: the account limiter is charged only by operations that start an asynchronous
table job (stream/IOPS/billing changes, create, delete) and by TagResource; synchronous metadata flips
(DeletionProtection), no-op re-sends, the TTL sub-resource API and all reads are exempt at 10 concurrent
calls. Many controllers starting at once will therefore see ThrottlingException mainly on their first real
UpdateTable/TagResource/CreateTable/DeleteTable of each table and one retry suffices.
