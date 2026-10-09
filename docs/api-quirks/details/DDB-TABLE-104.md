<!-- generated: ack-api-quirks dynamodb 2026-10-09 model=2012-08-10 controller=34b85e6 -->
# DDB-TABLE-104: DELETING: tagging backend forgets the table ~1 s after DeleteTable, ~5 s before DescribeTable 404s; UPDATING admits tag ops
_Full entry and notes of one finding; its summary entry is in [table.md](../table.md). Generated from
ack-api-quirks `services/dynamodb` (model 2012-08-10, controller commit 34b85e6); edit the finding in the lab,
not here._

## Finding

- <a id="ddb-table-104"></a>**DDB-TABLE-104** `async-state-machine` · impact medium · handled · verified 2026-10-08
  **DELETING: tagging backend forgets the table ~1 s after DeleteTable, ~5 s before DescribeTable 404s; UPDATING admits tag ops**
  While UPDATING (provisioned throughput increase, 2.2 s window): TagResource at +0.05 s -> 200,
  ListTagsOfResource -> 200; the following UntagResource/TagResource -> LimitExceededException "Table tags are
  being updated" (the per-table lock, not the table state). After DeleteTable (200, TableStatus DELETING for
  6.0 s): at +0.9 s TagResource -> ResourceInUseException "Attempt to change a resource which is still in use:
  Table is being deleted: <name>" while ListTagsOfResource still returned the full tag set (200); from +1.6 s
  onward, while DescribeTable still reported DELETING, TagResource, UntagResource and ListTagsOfResource all
  returned ResourceNotFoundException ("Requested resource not found: ResourceArn: ... not found"). After the
  DescribeTable 404 (+6.0 s) all three stayed ResourceNotFoundException for the 15 s observed (no 200 ever
  reappeared, tag set not readable via the ARN of a deleted table).
  - ACK: tags.custom-sync, synced.when, deletable.when · ops: TagResource, UntagResource, ListTagsOfResource,
    DeleteTable, UpdateTable
  - repro: UpdateTable ProvisionedThroughput 1/1->2/2 then tag ops every 0.3 s while UPDATING; DeleteTable
    then tag/untag/list every 0.7 s until 15 s after DescribeTable 404
  - measurements: updating_window_s=2.203, deleting_window_s=6.003, tag_api_404_after_delete_s=1.6,
    list_tags_200_after_404_s=0
  - handling: handled via `pkg/resource/table/hooks.go:175-196; 888fee6`
  - related: [DDB-TABLE-006](../table.md#ddb-table-006), [DDB-TABLE-008](../table.md#ddb-table-008), [DDB-TABLE-076](../table.md#ddb-table-076), [DDB-TABLE-063](../table-throughput-billing.md#ddb-table-063), [DDB-TABLE-103](../service.md#ddb-table-103), [DDB-TABLE-381](../table-throughput-billing.md#ddb-table-381),
    [DDB-TABLE-005](../table.md#ddb-table-005), [DDB-TABLE-101](../service.md#ddb-table-101), [DDB-TABLE-110](../table.md#ddb-table-110), [DDB-TABLE-173](../service.md#ddb-table-173), [DDB-TABLE-440](../table.md#ddb-table-440), [DDB-TABLE-441](../table.md#ddb-table-441), [DDB-TABLE-130](../service.md#ddb-table-130),
    [DDB-TABLE-108](../service.md#ddb-table-108), [DDB-TABLE-071](../service.md#ddb-table-071), [DDB-TABLE-184](../table-throughput-billing.md#ddb-table-184) · evidence: table/tags/state-gating-and-lag

## Notes

Partially confirms H-T-064: ListTagsOfResource does keep serving the tag set for the first second of DELETING,
but it switches to ResourceNotFoundException long before DescribeTable does, so a finalizer must not use
tag-API 404s as the "table is gone" signal and should skip tag reconciliation as soon as deletion starts.
H-T-061 for UPDATING is refuted (TagResource is admitted while UPDATING).
