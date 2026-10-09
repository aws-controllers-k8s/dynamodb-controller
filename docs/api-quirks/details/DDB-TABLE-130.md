<!-- generated: ack-api-quirks dynamodb 2026-10-09 model=2012-08-10 controller=34b85e6 -->
# DDB-TABLE-130: Tag API bursts: account rate limit surfaces as ThrottlingException 'rate of control plane requests ... too high'; ListTags 15/s fine
_Full entry and notes of one finding; its summary entry is in [service.md](../service.md). Generated from
ack-api-quirks `services/dynamodb` (model 2012-08-10, controller commit 34b85e6); edit the finding in the lab,
not here._

## Finding

- <a id="ddb-table-130"></a>**DDB-TABLE-130** `quota-limit` · impact high · unhandled (not handled in controller) · verified 2026-10-08
  **Tag API bursts: account rate limit surfaces as ThrottlingException 'rate of control plane requests ... too high'; ListTags 15/s fine**
  With SDK retries disabled, 15 concurrent TagResource calls over 10 ACTIVE tables (fired within 1.1 s)
  returned 10x 200, 1x ThrottlingException (HTTP 400, 'The rate of control plane requests made by this account
  is too high') on a table that received only one call, and 4x LimitExceededException 'Subscriber limit
  exceeded: Table tags are being updated: <name>' on the tables that received two calls (the per-table tag
  lock, not a rate limit). 15 concurrent UntagResource: 10x 200 + 5x LimitExceededException (all on doubly-hit
  tables), no ThrottlingException. 15 concurrent ListTagsOfResource (nominal limit 10/s): 15x 200 in 26 ms.
  Every rejected call succeeded when retried sequentially 1 s later. Successful burst calls had latencies of
  0.1-1.1 s.
  - ACK: requeue, tags.custom-sync, terminal_codes · ops: TagResource, ListTagsOfResource, UntagResource
  - repro: 10 ACTIVE tables; ThreadPoolExecutor(15) firing TagResource with distinct keys; repeat for
    ListTagsOfResource and UntagResource
  - measurements: tag_burst_rejected=5, list_burst_rejected=0, untag_burst_rejected=5, tag_burst_span_ms=1105,
    tag_burst_latency_max_ms=1095, throttling_exception_count=1, per_table_lock_count_tag=4,
    per_table_lock_count_untag=5
  - handling: not handled in the controller (as of commit 34b85e6)
  - related: [DDB-TABLE-053](../service.md#ddb-table-053), [DDB-TABLE-132](../service.md#ddb-table-132), [DDB-TABLE-441](../table.md#ddb-table-441), [DDB-TABLE-131](../table.md#ddb-table-131), [DDB-TABLE-051](../table-throughput-billing.md#ddb-table-051), [DDB-TABLE-103](../service.md#ddb-table-103),
    [DDB-TABLE-173](../service.md#ddb-table-173), [DDB-TABLE-440](../table.md#ddb-table-440), [DDB-TABLE-104](../table.md#ddb-table-104), [DDB-TABLE-108](../service.md#ddb-table-108) · evidence: table/limits/rate-bursts

## Notes

Confirms the ThrottlingException-not-LimitExceededException part of H-T-131 for the account-wide control-plane
rate; the message ('The rate of control plane requests made by this account is too high') differs from the
text hypothesised. CAVEAT: this burst ran with botocore max_attempts=1, which still performs ONE retry, so the
counts are lower bounds: 4 of the 10 'successful' TagResource calls and 4 of the 10 UntagResource calls
succeeded only on their retry (evidence retry_attempts=1), i.e. 9/15 first attempts failed in each burst. The
tag bursts were not repeated without retries to spare the shared account limit. A controller reconciling many
tables concurrently sees BOTH codes from the tag APIs: ThrottlingException (account rate, retry with backoff)
and LimitExceededException 'Table tags are being updated' (per-table lock, ~1.6-1.8 s). Both are HTTP 400.
ListTagsOfResource tolerated 15 concurrent calls (0 failures, no retries needed).
