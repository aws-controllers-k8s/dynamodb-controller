<!-- generated: ack-api-quirks dynamodb 2026-10-09 model=2012-08-10 controller=34b85e6 -->
# DDB-TABLE-362: Re-enabling a stream mints a NEW LatestStreamArn/Label; the response already carries it and the old ARN is gone from DescribeTable
_Full entry and notes of one finding; its summary entry is in
[table-streams-encryption-class.md](../table-streams-encryption-class.md). Generated from ack-api-quirks
`services/dynamodb` (model 2012-08-10, controller commit 34b85e6); edit the finding in the lab, not here._

## Finding

- <a id="ddb-table-362"></a>**DDB-TABLE-362** `identity` · impact medium · unhandled (not handled in controller) · verified 2026-10-09
  **Re-enabling a stream mints a NEW LatestStreamArn/Label; the response already carries it and the old ARN is gone from DescribeTable**
  Enable {true, NEW_IMAGE} -> LatestStreamArn .../stream/2026-10-09T04:48:48.824. Disable -> DescribeTable
  keeps the same LatestStreamArn ([DDB-TABLE-050](../table-streams-encryption-class.md#ddb-table-050)). Re-enable {true, NEW_IMAGE} -> the UpdateTable response and
  DescribeTable at T+0 carry a DIFFERENT LatestStreamArn .../stream/2026-10-09T04:48:57.132 (label = enable
  timestamp); the old ARN is no longer referenced anywhere in DescribeTable. UPDATING windows: enable 3-4 s,
  disable 4 s, re-enable 4 s; no cooldown between the three calls.
  - ACK: is_read_only, references, docs-only · ops: UpdateTable, DescribeTable · fields: StreamSpecification,
    LatestStreamArn, LatestStreamLabel
  - repro: UpdateTable StreamSpecification enable -> disable -> enable; compare LatestStreamArn across the
    three ACTIVE states.
  - measurements: enable_updating_s=3.0, disable_updating_s=4.1, reenable_updating_s=4.1
  - handling: not handled in the controller (as of commit 34b85e6)
  - related: [DDB-TABLE-050](../table-streams-encryption-class.md#ddb-table-050), [DDB-TABLE-180](../table-streams-encryption-class.md#ddb-table-180), [DDB-TABLE-367](../table-streams-encryption-class.md#ddb-table-367), [DDB-TABLE-378](../table-streams-encryption-class.md#ddb-table-378), [DDB-TABLE-004](../table-streams-encryption-class.md#ddb-table-004), [DDB-TABLE-373](../service.md#ddb-table-373),
    [DDB-TABLE-013](../service.md#ddb-table-013), [DDB-TABLE-071](../service.md#ddb-table-071), [DDB-TABLE-213](../table-streams-encryption-class.md#ddb-table-213) · evidence: table/creative/xs-response-echo,
    table/creative/xs-identity-tags-streams

## Notes

Extends [DDB-TABLE-050](../table-streams-encryption-class.md#ddb-table-050): the stream is not a toggle but a sequence of distinct stream resources. Any dependent
that stored status.latestStreamARN (Lambda EventSourceMapping, Kinesis adapter consumers, a referencing ACK
resource) is silently detached by a disable/enable flap of StreamSpecification; a controller should surface
latestStreamARN as read-only status and expect it to change on every re-enable. Fate of the old stream: see
the ListStreams finding from xs-identity-tags-streams.
