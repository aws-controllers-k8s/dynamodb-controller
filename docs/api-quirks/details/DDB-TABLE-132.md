<!-- generated: ack-api-quirks dynamodb 2026-10-09 model=2012-08-10 controller=34b85e6 -->
# DDB-TABLE-132: 10 concurrent CreateTable or DeleteTable (no indexes): 4/10 get ThrottlingException (account control-plane rate); 50 DescribeTable fine
_Full entry and notes of one finding; its summary entry is in [service.md](../service.md). Generated from
ack-api-quirks `services/dynamodb` (model 2012-08-10, controller commit 34b85e6); edit the finding in the lab,
not here._

## Finding

- <a id="ddb-table-132"></a>**DDB-TABLE-132** `quota-limit` · impact medium · unhandled (not handled in controller) · verified 2026-10-08
  **10 concurrent CreateTable or DeleteTable (no indexes): 4/10 get ThrottlingException (account control-plane rate); 50 DescribeTable fine**
  With SDK retries fully disabled (botocore total_max_attempts=1), 10 concurrent CreateTable calls for
  single-key PAY_PER_REQUEST tables (span 185 ms) returned 6x 200 and 4x ThrottlingException (HTTP 400, 'The
  rate of control plane requests made by this account is too high'); 10 concurrent DeleteTable likewise 6x 200
  / 4x ThrottlingException. No LimitExceededException and no ResourceInUseException. With a single SDK retry
  (botocore max_attempts=1, which still retries once) all 10 creates and deletes succeed (first run: 3-4 of 10
  needed the retry). 50 concurrent DescribeTable: 50x 200, max latency 63 ms, in both runs. All 10 tables were
  ACTIVE 6.7-7.7 s after the burst and gone 5.7-6.8 s after the delete burst.
  - ACK: requeue, e2e-timing · ops: CreateTable, DeleteTable, DescribeTable
  - repro: ThreadPoolExecutor(10) CreateTable; ThreadPoolExecutor(50) DescribeTable; ThreadPoolExecutor(10)
    DeleteTable
  - measurements: create_burst_rejected_noretry=4, delete_burst_rejected_noretry=4, describe_burst_rejected=0,
    create_burst_retry_needed_run1=3, all_active_after_s=7.67, all_gone_after_s=5.7
  - handling: not handled in the controller (as of commit 34b85e6)
  - related: [DDB-TABLE-053](../service.md#ddb-table-053), [DDB-TABLE-130](../service.md#ddb-table-130), [DDB-TABLE-441](../table.md#ddb-table-441), [DDB-TABLE-131](../table.md#ddb-table-131), [DDB-TABLE-051](../table-throughput-billing.md#ddb-table-051) · evidence:
    table/limits/rate-bursts

## Notes

Refines H-T-131: for index-less tables the constraint on concurrent CreateTable is the account-wide
control-plane rate (ThrottlingException), not a LimitExceededException/serialization rule, and it is cheap to
ride out with one retry. A controller starting with many Table CRs will see ThrottlingException from
CreateTable/DeleteTable on the first attempt roughly 40% of the time at 10 concurrent calls.
