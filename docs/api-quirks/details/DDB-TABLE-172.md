<!-- generated: ack-api-quirks dynamodb 2026-10-09 model=2012-08-10 controller=34b85e6 -->
# DDB-TABLE-172: Tag write lock: only effective changes take it; held ~1.6-1.8 s until ListTags reflects the change; blocks DeleteTable, no other mutation
_Full entry and notes of one finding; its summary entry is in [service.md](../service.md). Generated from
ack-api-quirks `services/dynamodb` (model 2012-08-10, controller commit 34b85e6); edit the finding in the lab,
not here._

## Finding

- <a id="ddb-table-172"></a>**DDB-TABLE-172** `quota-limit` · impact high · unhandled (not handled in controller) · verified 2026-10-09, re-verified
  **Tag write lock: only effective changes take it; held ~1.6-1.8 s until ListTags reflects the change; blocks DeleteTable, no other mutation**
  Measured with SDK retries fully disabled. A no-op UntagResource (absent key) and a no-op TagResource (same
  key and value) do NOT take the lock (an immediately following TagResource is accepted), but a real
  UntagResource does (an immediately following no-op UntagResource -> LimitExceededException 'Subscriber limit
  exceeded: Table tags are being updated'). ListTagsOfResource is never blocked. After a 1-tag TagResource the
  lock was held 1.58 s / 1.80 s, after a 45-tag TagResource 1.80 s (probe: no-op UntagResource every 200 ms);
  the new tags appeared in ListTagsOfResource at the same poll (1.58 s / 1.80 s) or one poll later (2.02 s),
  i.e. the lock releases when the change becomes readable. Mutations issued 50 ms after a TagResource were all
  accepted: UpdateTable (ProvisionedThroughput) 200, UpdateTimeToLive 200, UpdateContinuousBackups 200,
  CreateBackup 200; only DeleteTable is rejected (ResourceInUseException 'Attempt to change a resource which
  is still in use: Table tags are being updated', accepted after 1.7 s). Reverse direction: TagResource right
  after UpdateTable (table UPDATING) -> 200; TagResource right after UpdateTimeToLive -> 200.
  - ACK: tags.custom-sync, requeue, one-per-reconcile · ops: TagResource, UntagResource, UpdateTable,
    UpdateTimeToLive, UpdateContinuousBackups, CreateBackup, DeleteTable
  - repro: TagResource; every 200 ms UntagResource [absent-key] until 200 while polling ListTagsOfResource;
    TagResource then each other mutation after 50 ms
  - measurements: lock_1tag_s=1.577, lock_1tag_again_s=1.799, lock_45tags_s=1.795,
    release_minus_visible_max_s=0.221, delete_after_tag_accepted_s=1.725
  - handling: not handled in the controller (as of commit 34b85e6)
  - related: [DDB-TABLE-348](../table-policy-kinesis-autoscaling.md#ddb-table-348), [DDB-TABLE-119](../table-streams-encryption-class.md#ddb-table-119), [DDB-TABLE-210](../table-policy-kinesis-autoscaling.md#ddb-table-210), [DDB-TABLE-432](../table-policy-kinesis-autoscaling.md#ddb-table-432) · evidence:
    table/tags/write-lock-characterization, table/creative/reverify-set-b

## Notes

Follow-up to the per-table tag lock finding. Practical consequences: (1) the controller's tag diff must be
applied as at most one tag mutation per ~2 s per table; (2) tag writes may be interleaved freely with
UpdateTable/TTL/PITR/backup calls; (3) a finalizer must tolerate ResourceInUseException from DeleteTable for
~2 s after its last tag write; (4) because the lock releases together with read visibility, 'wait until
ListTagsOfResource shows the change' is a sufficient readiness signal for the next tag write. Side
observation: UpdateTimeToLive toggled twice within a short interval fails with ValidationException 'Time to
live has been modified multiple times within a fixed interval'.
