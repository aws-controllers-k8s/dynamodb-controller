<!-- generated: ack-api-quirks dynamodb 2026-10-09 model=2012-08-10 controller=34b85e6 -->
# DDB-TABLE-103: Per-table tag write lock: 2nd TagResource/UntagResource within ~1-3 s -> LimitExceededException 'Table tags are being updated'
_Full entry and notes of one finding; its summary entry is in [service.md](../service.md). Generated from
ack-api-quirks `services/dynamodb` (model 2012-08-10, controller commit 34b85e6); edit the finding in the lab,
not here._

## Finding

- <a id="ddb-table-103"></a>**DDB-TABLE-103** `quota-limit` · impact high · SUSPECTED CONTROLLER BUG · verified 2026-10-08
  **Per-table tag write lock: 2nd TagResource/UntagResource within ~1-3 s -> LimitExceededException 'Table tags are being updated'**
  Any TagResource or UntagResource issued right after a successful TagResource/UntagResource on the same table
  is rejected with LimitExceededException (HTTP 400) 'Subscriber limit exceeded: Table tags are being updated:
  <name>' - 15/15 trials here (each trial even included botocore's one implicit retry ~1 s later, which also
  failed). Retried every 100 ms the second write was accepted after p50 1.6-2.4 s, max 3.4 s from the first
  call (tag->tag 1.1-3.1 s; tag->untag 2.0-3.4 s; untag->tag 1.4-2.4 s). Measured with retries fully disabled
  in table/tags/write-lock-characterization, the lock is held for ~1.6-1.8 s after the write returns and is
  released when ListTagsOfResource reflects the change. DeleteTable issued right after TagResource ->
  ResourceInUseException 'Attempt to change a resource which is still in use: Table tags are being updated:
  <name>', accepted after 1.7-1.8 s. UpdateTable (DeletionProtectionEnabled) right after TagResource is NOT
  blocked (200), and TagResource right after UpdateTable is not blocked either. The lock also explains 'lost'
  untag+retag sequences: the re-tag is rejected synchronously, leaving the key missing (5/5 trials).
  - ACK: tags.custom-sync, requeue, one-per-reconcile · ops: TagResource, UntagResource, DeleteTable,
    UpdateTable
  - repro: TagResource {a:1}; immediately TagResource {b:1} (boto3 Config retries max_attempts=1); repeat
    every 100 ms until 200. Also TagResource then DeleteTable.
  - measurements: lock_window_p50_s=1.58, lock_window_max_s=3.366, delete_after_tag_blocked_s=1.796,
    untag_then_retag_lost=5
  - handling: suspected controller bug - see Handling gaps
  - related: [DDB-TABLE-006](../table.md#ddb-table-006), [DDB-TABLE-063](../table-throughput-billing.md#ddb-table-063), [DDB-TABLE-381](../table-throughput-billing.md#ddb-table-381), [DDB-TABLE-005](../table.md#ddb-table-005), [DDB-TABLE-101](../service.md#ddb-table-101), [DDB-TABLE-104](../table.md#ddb-table-104),
    [DDB-TABLE-102](../service.md#ddb-table-102), [DDB-TABLE-105](../service.md#ddb-table-105), [DDB-TABLE-108](../service.md#ddb-table-108), [DDB-TABLE-173](../service.md#ddb-table-173), [DDB-TABLE-440](../table.md#ddb-table-440), [DDB-TABLE-441](../table.md#ddb-table-441), [DDB-TABLE-130](../service.md#ddb-table-130) ·
    evidence: table/tags/state-gating-and-lag

## Notes

Not the documented 5/s account rate limit: it is per table and fires on the 2nd call. botocore and
aws-sdk-go-v2 standard retry modes classify LimitExceededException as a throttle and retry with short jittered
backoff, which does NOT reliably outlast the ~1.7 s lock (see the follow-up finding: 3 attempts -> 1/5
success). The ACK tag sync pattern 'UntagResource removed keys, then TagResource added/changed keys' will
usually fail on the second call; a controller must either merge the diff into one call per reconcile (one
TagResource, or one UntagResource) and requeue for the other half, or requeue on LimitExceededException
containing 'Table tags are being updated', and must also expect DeleteTable to be rejected with
ResourceInUseException for ~2 s after a tag write.

Contradiction with [DDB-TABLE-440](../table.md#ddb-table-440), [DDB-TABLE-441](../table.md#ddb-table-441): 103 states 'Any TagResource or UntagResource issued right
after a successful TagResource/UntagResource ... is rejected with LimitExceededException' (15/15); 440 shows
an identical replay or a superset containing the in-flight tags passes (200) during the lock, and 441 shows
no-op writes (identical set, absent key) never take the lock - 103's 15 trials were all effective,
non-superset writes Resolution: keep all; 103 canonical for the lock, 440/441 refine the admission test to
'effective write not containing the in-flight tags'

Suspected controller bug confirmed by evidence: Not flagged SUSPECT in the digest; evidence-driven. The
mechanism issues one TagResource then one UntagResource in the same reconcile on the assumption that the only
limit is 5 calls/s per account. 103 shows a per-table lock held ~1.7 s after any tag write rejecting the next
write with LimitExceededException 'Table tags are being updated' (15/15; aws-sdk-go-v2 standard retry outlasts
it only ~1/5), so any mixed add+remove delta errors on the second call most of the time (self-heals on requeue
but emits spurious errors), and DeleteTable within ~2 s of a tag write -> ResourceInUseException.
