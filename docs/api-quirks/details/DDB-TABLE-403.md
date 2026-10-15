<!-- generated: ack-api-quirks dynamodb 2026-10-09 model=2012-08-10 controller=34b85e6 -->
# DDB-TABLE-403: DescribeTimeToLive throttling is per HTTP connection: ~4 calls then 'Rate exceeded' on one connection; fresh connections never; refill ~2 s
_Full entry and notes of one finding; its summary entry is in [service.md](../service.md). Generated from
ack-api-quirks `services/dynamodb` (model 2012-08-10, controller commit 34b85e6); edit the finding in the lab,
not here._

## Finding

- <a id="ddb-table-403"></a>**DDB-TABLE-403** `quota-limit` · impact medium · handled · verified 2026-10-09
  **DescribeTimeToLive throttling is per HTTP connection: ~4 calls then 'Rate exceeded' on one connection; fresh connections never; refill ~2 s**
  Sequential, no retries. ttl.same_conn_nogap: 30 (120.5/s) -> {'OK': 4, 'ThrottlingException': 26}, first
  throttle at #4, pattern ....TTTTTTTTTTTTTTTTTTTTTTTTTT. After the throttled burst, polling the SAME
  connection every 0.5 s: first success after 2.06 s (pattern TTTT.TTT.TTT.TT). ttl.fresh_conn_nogap: 30
  (27.1/s) -> {'OK': 30}, first throttle at #30, pattern ...............................
  ttl.same_conn_10_per_s: 30 (9.2/s) -> {'OK': 5, 'ThrottlingException': 25}, first throttle at #4, pattern
  ....TTTTTTTTTTTTTTT.TTTTTTTTTT. ttl.same_conn_4_per_s: 20 (3.8/s) -> {'OK': 6, 'ThrottlingException': 14},
  first throttle at #4, pattern ....TTTTTTT.TTTTTTT.. Comparison on one connection, no gap:
  cb.same_conn_nogap: 30 (79.6/s) -> {'OK': 13, 'ThrottlingException': 17}, first throttle at #11, pattern
  ...........TTTTTTTTTTT..TTTTTT; tags.same_conn_nogap_100: 100 (184.2/s) -> {'OK': 100}, first throttle at
  #100, pattern
  ..................................................................................................... 30
  concurrent DescribeTimeToLive sharing one client/pool: {'OK': 30}.
  - ACK: requeue, e2e-timing · ops: DescribeTimeToLive, DescribeContinuousBackups, ListTagsOfResource
  - repro: one boto3 client (keep-alive): 30x DescribeTimeToLive back-to-back; then poll every 0.5 s; then 30x
    with a new client per call; then paced 10/s and 4/s
  - measurements: ttl_same_conn_ok_before_throttle=4, ttl_refill_first_ok_s=2.06, ttl_fresh_conn_throttled=0,
    ttl_10_per_s_throttled=25, ttl_4_per_s_throttled=14, cb_same_conn_throttled=17
  - handling: handled via `templates/hooks/table/sdk_read_one_post_set_output.go.tpl:57-72; bcd26e1`
  - related: [DDB-TABLE-099](../service.md#ddb-table-099), [DDB-TABLE-360](../table-subresources.md#ddb-table-360), [DDB-TABLE-445](../service.md#ddb-table-445) · hypotheses: H-T-131 · evidence:
    table/limits/read-api-bursts

## Notes

Explains [DDB-TABLE-099](../service.md#ddb-table-099)/360: the DescribeTimeToLive limiter is a small per-connection (or per front-end host)
token bucket - burst ~4, refill ~1 token per 2 s - not an account-wide 10/s: 30 calls over fresh connections
all passed while 4/s on one keep-alive connection throttled from the 5th call on. DescribeContinuousBackups
has a larger bucket (~11) on the same connection; ListTagsOfResource showed no per-connection limit (100 calls
in 0.54 s). A controller polling DescribeTimeToLive over one keep-alive connection must pace it below ~0.5/s
or treat ThrottlingException 'Rate exceeded' (HTTP 400) as a short requeue; the 1/s ThrottlingExceptions
reported by earlier probes were this bucket, not shared account load. Qualifies H-T-131 (ListTagsOfResource
10/s not enforced per table/connection).
