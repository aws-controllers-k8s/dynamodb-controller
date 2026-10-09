<!-- generated: ack-api-quirks dynamodb 2026-10-09 model=2012-08-10 controller=34b85e6 -->
# DDB-TABLE-101: Tag APIs during CREATING: ResourceNotFoundException, then ResourceInUseException just before ACTIVE; tags unreadable until ACTIVE
_Full entry and notes of one finding; its summary entry is in [service.md](../service.md). Generated from
ack-api-quirks `services/dynamodb` (model 2012-08-10, controller commit 34b85e6); edit the finding in the lab,
not here._

## Finding

- <a id="ddb-table-101"></a>**DDB-TABLE-101** `async-state-machine` · impact high · handled · verified 2026-10-08
  **Tag APIs during CREATING: ResourceNotFoundException, then ResourceInUseException just before ACTIVE; tags unreadable until ACTIVE**
  CreateTable returns TableDescription.TableArn immediately, but TagResource, UntagResource and
  ListTagsOfResource with that ARN return ResourceNotFoundException (HTTP 400, "Requested resource not found:
  ResourceArn: arn:...:table/<name> not found") for ~5-6 s of the ~7 s CREATING window (8/9 attempts at 0.7 s
  spacing). In the last ~1 s before ACTIVE the table becomes known to the tagging backend: TagResource ->
  ResourceInUseException "Attempt to change a resource which is still in use: Table is being created: <name>",
  UntagResource -> 200, ListTagsOfResource -> 200 with an empty set. Tags supplied in CreateTable are likewise
  unreadable (ListTagsOfResource 404 x8) until the table is ACTIVE, at which point they appear together with
  the ACTIVE status (no further lag, 2 runs). TagResource issued the instant DescribeTable first reports
  ACTIVE succeeds (200). The CreateTable response does not echo Tags.
  - ACK: tags.custom-sync, synced.when, exceptions.404 · ops: CreateTable, TagResource, UntagResource,
    ListTagsOfResource · fields: Tags, ResourceArn
  - repro: CreateTable (with or without Tags); immediately and every 0.7 s call
    TagResource/UntagResource/ListTagsOfResource with TableDescription.TableArn until DescribeTable reports
    ACTIVE
  - measurements: creating_window_s=6.769, tag_api_404_until_s=5.3, create_tags_visible_s=4.448,
    active_at_s=4.448
  - handling: handled via `pkg/resource/table/hooks.go:182-186; pkg/resource/table/hooks_resource_policy.go:60-63; pkg/resource/table/hooks_tags.go:138-168; pkg/resource/table/hooks.go:549-553; test/e2e/tests/test_table.py:277-349; e953ae5; templates/hooks/table/sdk_create_post_set_output.go.tpl:1-4; bcd26e1`
  - related: [DDB-TABLE-006](../table.md#ddb-table-006), [DDB-TABLE-063](../table-throughput-billing.md#ddb-table-063), [DDB-TABLE-103](../service.md#ddb-table-103), [DDB-TABLE-381](../table-throughput-billing.md#ddb-table-381), [DDB-TABLE-005](../table.md#ddb-table-005), [DDB-TABLE-104](../table.md#ddb-table-104),
    [DDB-TABLE-110](../table.md#ddb-table-110), [DDB-TABLE-071](../service.md#ddb-table-071) · evidence: table/tags/state-gating-and-lag

## Notes

Refutes the ResourceInUseException-only part of H-T-061 and the "visible as soon as DescribeTable succeeds"
part of H-T-051. A controller must not treat ResourceNotFoundException from the tag APIs as "table gone" while
the table is CREATING, and must defer tag reconciliation (including read-back of CreateTable tags) until
ACTIVE; passing Tags on CreateTable is the only way to have tags during creation.
