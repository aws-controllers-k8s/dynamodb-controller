<!-- generated: ack-api-quirks dynamodb 2026-10-09 model=2012-08-10 controller=34b85e6 -->
# DDB-TABLE-346: GetResourcePolicy at 100 ms after a write flips exactly once, never back: PNF->NEW (fresh), OLD->NEW (replace), OLD->PNF (delete); 6/6
_Full entry and notes of one finding; its summary entry is in
[table-policy-kinesis-autoscaling.md](../table-policy-kinesis-autoscaling.md). Generated from ack-api-quirks
`services/dynamodb` (model 2012-08-10, controller commit 34b85e6); edit the finding in the lab, not here._

## Finding

- <a id="ddb-table-346"></a>**DDB-TABLE-346** `eventual-consistency` · impact high · unhandled (not handled in controller) · verified 2026-10-09
  **GetResourcePolicy at 100 ms after a write flips exactly once, never back: PNF->NEW (fresh), OLD->NEW (replace), OLD->PNF (delete); 6/6**
  Get every 100 ms for 5 s after each write (6 trials per phase, 3 tables, 46-47 polls/trial, Get latency 8.5
  ms, no throttling at 10 Get/s). Fresh Put: pattern ['PNF->NEW'] in all trials; first Get at ~0.1 s was
  already PolicyNotFoundException; the switch to the new RevisionId happened once at 1.79 s (min 1.385, max
  2.278) with no intermediate 200 of another revision. Replace: ['OLD->NEW'] in all trials - the OLD
  document/RevisionId is served with HTTP 200 until 1.577 s (max 2.295), then the new one;
  PolicyNotFoundException never appeared in between. Delete: ['OLD->PNF'] in all trials; the deleted document
  is served until 1.368 s (max 2.025), then PolicyNotFoundException for the rest of the 5 s. No trial flapped
  back (non-monotonic sequences: 0/0/0), so the first observation of the new state is final.
  DeleteResourcePolicy echoed the removed RevisionId in 6 trials.
  - ACK: requeue, late_initialize, compare.is_ignored+delta_pre_compare · ops: PutResourcePolicy,
    GetResourcePolicy, DeleteResourcePolicy · fields: ResourcePolicy, RevisionId
  - repro: Put; Get every 100 ms for 5 s; 16 s later Put(other); same; 16 s later Delete; same; x6
  - measurements: fresh_first_new_s.n=6, fresh_first_new_s.min=1.385, fresh_first_new_s.median=1.79,
    fresh_first_new_s.max=2.278, replace_last_old_s.n=6, replace_last_old_s.min=1.184,
    replace_last_old_s.median=1.577, replace_last_old_s.max=2.295, delete_last_old_s.n=6,
    delete_last_old_s.min=1.148, delete_last_old_s.median=1.368, delete_last_old_s.max=2.025,
    get_latency_ms.n=6, get_latency_ms.min=8.0, get_latency_ms.median=8.5, get_latency_ms.max=9.0
  - handling: not handled in the controller (as of commit 34b85e6)
  - related: [DDB-TABLE-206](../table-policy-kinesis-autoscaling.md#ddb-table-206), [DDB-TABLE-209](../table-policy-kinesis-autoscaling.md#ddb-table-209), [DDB-TABLE-244](../table-policy-kinesis-autoscaling.md#ddb-table-244), [DDB-TABLE-245](../table-policy-kinesis-autoscaling.md#ddb-table-245), [DDB-TABLE-246](../table-policy-kinesis-autoscaling.md#ddb-table-246), [DDB-TABLE-248](../table-policy-kinesis-autoscaling.md#ddb-table-248),
    [DDB-TABLE-205](../table-policy-kinesis-autoscaling.md#ddb-table-205), [DDB-TABLE-233](../table-policy-kinesis-autoscaling.md#ddb-table-233), [DDB-TABLE-208](../table-policy-kinesis-autoscaling.md#ddb-table-208), [DDB-TABLE-351](../table-policy-kinesis-autoscaling.md#ddb-table-351) · hypotheses: H-S-007, H-S-107, H-S-108,
    H-S-044 · evidence: table/consistency-windows/policy-stale-read-sequence

## Notes

Confirms H-S-007/H-S-107/H-S-108 and refutes H-S-044 at 100 ms resolution; the previously reported 200 ms
windows ([DDB-TABLE-244](../table-policy-kinesis-autoscaling.md#ddb-table-244)..246) are reproduced with the exact sequence: a single monotonic switch ~1.3-2.4 s
after the write. A controller can therefore poll until the RevisionId returned by Put is visible and trust it
(no need to wait for 'stability').
