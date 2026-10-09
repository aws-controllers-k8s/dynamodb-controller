<!-- generated: ack-api-quirks dynamodb 2026-10-09 model=2012-08-10 controller=34b85e6 -->
# DDB-TABLE-440: Tag write lock (~1.5 s) admits only a TagResource that includes the in-flight tags (replay or full set); other Tag/Untag -> LimitExceeded
_Full entry and notes of one finding; its summary entry is in [table.md](../table.md). Generated from
ack-api-quirks `services/dynamodb` (model 2012-08-10, controller commit 34b85e6); edit the finding in the lab,
not here._

## Finding

- <a id="ddb-table-440"></a>**DDB-TABLE-440** `quota-limit` · impact medium · unhandled (not handled in controller) · verified 2026-10-09
  **Tag write lock (~1.5 s) admits only a TagResource that includes the in-flight tags (replay or full set); other Tag/Untag -> LimitExceeded**
  Pairs of back-to-back calls on one table (second call within ms of an effective TagResource): identical
  replay {A} after {A} -> 200; {A} after {B} (A already committed, i.e. a no-op against the committed set) ->
  LimitExceededException 'Subscriber limit exceeded: Table tags are being updated: <name>'; {A,B,C} after {C}
  -> 200; the full desired set {base..., A, B, C, D} after {D} -> 200; UntagResource of an absent key after
  {E} -> LimitExceededException; {F:y} after {F:x} -> LimitExceededException; ListTagsOfResource -> 200.
  Polling the rejected shape at 0.2 s after an effective write: LimitExceededException until 1.52 s, then 200.
  - ACK: tags.custom-sync, requeue · ops: TagResource, UntagResource, ListTagsOfResource · fields: Tags
  - repro: TagResource {B}; immediately TagResource {A} (A already present) -> LimitExceededException; vs
    TagResource {C}; immediately TagResource {A,B,C} -> 200
  - measurements: lock_duration_for_rejected_request_s=1.52, trials_per_shape=1
  - handling: not handled in the controller (as of commit 34b85e6)
  - related: [DDB-TABLE-103](../service.md#ddb-table-103), [DDB-TABLE-172](../service.md#ddb-table-172), [DDB-TABLE-173](../service.md#ddb-table-173), [DDB-TABLE-441](../table.md#ddb-table-441), [DDB-TABLE-130](../service.md#ddb-table-130), [DDB-TABLE-104](../table.md#ddb-table-104),
    [DDB-TABLE-108](../service.md#ddb-table-108) · evidence: table/creative/noop-tag-storm

## Notes

Refines [DDB-TABLE-172](../service.md#ddb-table-172) ('only effective changes take the lock'): the lock is taken by the effective write, but
the admission test for the NEXT call is whether it contains the in-flight request's key/value pairs, not
whether it is a no-op against the committed set. For a controller this means: write the complete desired tag
set in ONE TagResource (a superset passes even while the previous write is in flight) and never follow a
TagResource with an UntagResource in the same reconcile without a ~2 s wait ([DDB-TABLE-173](../service.md#ddb-table-173)). Single trial per
shape.

Contradiction with [DDB-TABLE-103](../service.md#ddb-table-103), [DDB-TABLE-441](../table.md#ddb-table-441): 103 states 'Any TagResource or UntagResource issued right
after a successful TagResource/UntagResource ... is rejected with LimitExceededException' (15/15); 440 shows
an identical replay or a superset containing the in-flight tags passes (200) during the lock, and 441 shows
no-op writes (identical set, absent key) never take the lock - 103's 15 trials were all effective,
non-superset writes Resolution: keep all; 103 canonical for the lock, 440/441 refine the admission test to
'effective write not containing the in-flight tags'
