<!-- generated: ack-api-quirks dynamodb 2026-10-09 model=2012-08-10 controller=34b85e6 -->
# DDB-TABLE-378: Each stream enable mints a new LatestStreamArn; ListStreams(TableName) still lists DISABLED streams of flaps and deleted incarnations
_Full entry and notes of one finding; its summary entry is in
[table-streams-encryption-class.md](../table-streams-encryption-class.md). Generated from ack-api-quirks
`services/dynamodb` (model 2012-08-10, controller commit 34b85e6); edit the finding in the lab, not here._

## Finding

- <a id="ddb-table-378"></a>**DDB-TABLE-378** `identity` · impact medium · unhandled (not handled in controller) · verified 2026-10-09
  **Each stream enable mints a new LatestStreamArn; ListStreams(TableName) still lists DISABLED streams of flaps and deleted incarnations**
  Table re-created under the same name with StreamSpecification enabled: LatestStreamArn differs from the old
  incarnation's (label = enable timestamp), dynamodbstreams:DescribeStream on the old ARN still returns 200
  with StreamStatus=DISABLED and TableName=<name>, and ListStreams(TableName=<name>) lists BOTH ARNs (old
  first-deleted one included). On a single incarnation, enable -> disable -> enable yields a third ARN;
  ListStreams(TableName) then returns 3 streams (2 DISABLED, 1 ENABLED). DescribeTable.LatestStreamArn is the
  only pointer to the live stream.
  - ACK: is_read_only, docs-only · ops: UpdateTable, DescribeTable, DescribeStream, ListStreams · fields:
    LatestStreamArn, LatestStreamLabel, StreamSpecification
  - repro: CreateTable with StreamSpecification -> DeleteTable -> CreateTable same name with
    StreamSpecification -> dynamodbstreams ListStreams TableName=<name>; or UpdateTable stream on/off/on
  - measurements: streams_listed_after_recreate=2, streams_listed_after_one_flap=3
  - handling: not handled in the controller (as of commit 34b85e6)
  - related: [DDB-TABLE-013](../service.md#ddb-table-013), [DDB-TABLE-050](../table-streams-encryption-class.md#ddb-table-050), [DDB-TABLE-367](../table-streams-encryption-class.md#ddb-table-367), [DDB-TABLE-362](../table-streams-encryption-class.md#ddb-table-362), [DDB-TABLE-180](../table-streams-encryption-class.md#ddb-table-180), [DDB-TABLE-004](../table-streams-encryption-class.md#ddb-table-004),
    [DDB-TABLE-373](../service.md#ddb-table-373), [DDB-TABLE-071](../service.md#ddb-table-071), [DDB-TABLE-213](../table-streams-encryption-class.md#ddb-table-213) · evidence: table/creative/name-keyed-cooldowns,
    table/creative/noop-resend-storm

## Notes

Extends [DDB-TABLE-367](../table-streams-encryption-class.md#ddb-table-367) (flap on one incarnation, found independently in parallel by xs-identity-tags-streams)
with the name-reuse case: ListStreams(TableName) also returns the DISABLED stream of a DELETED previous
incarnation of the same name, so a controller that resolves 'the table's stream' by name after a
delete/re-create can bind to a dead stream. Extends [DDB-TABLE-050](../table-streams-encryption-class.md#ddb-table-050) (LatestStreamArn survives disable): after a
re-enable the ARN changes, so a status.latestStreamARN captured once is stale after any stream flap or table
re-creation, and consumers (Lambda ESM, pipes) bound to the old ARN silently see a DISABLED stream. A
controller must refresh LatestStreamArn from DescribeTable on every reconcile and must not resolve 'the
table's stream' via ListStreams(TableName).
