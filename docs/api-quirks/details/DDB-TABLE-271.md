<!-- generated: ack-api-quirks dynamodb 2026-10-09 model=2012-08-10 controller=34b85e6 -->
# DDB-TABLE-271: Same-name re-create: policy/Kinesis reads show no leak of the old table's state (0 stale reads at 0.5s polling, CreateTable..ACTIVE+10s)
_Full entry and notes of one finding; its summary entry is in
[table-policy-kinesis-autoscaling.md](../table-policy-kinesis-autoscaling.md). Generated from ack-api-quirks
`services/dynamodb` (model 2012-08-10, controller commit 34b85e6); edit the finding in the lab, not here._

## Finding

- <a id="ddb-table-271"></a>**DDB-TABLE-271** `identity` · impact medium · unhandled (not handled in controller) · verified 2026-10-09
  **Same-name re-create: policy/Kinesis reads show no leak of the old table's state (0 stale reads at 0.5s polling, CreateTable..ACTIVE+10s)**
  Old table R: policy 200 OK, destination [('-fu-g2', 'ACTIVE')]; DeleteTable -> gone. CreateTable(R) ->
  CREATING. Polling DescribeTable + GetResourcePolicy + DescribeKinesisStreamingDestination every 0.5s:
  timeline (table, get, kinesis) [(['CREATING', 'ERR:ResourceNotFoundException',
  'ERR:ResourceNotFoundException'], 0.0, 1.05), (['CREATING', 'ERR:PolicyNotFoundException', 'EMPTY'], 1.58,
  4.21), (['ACTIVE', 'ERR:PolicyNotFoundException', 'EMPTY'], 4.73, 14.75)]; ACTIVE at 4.73s; reads returning
  the old RevisionId or the old destination: 0 (first: None).
  - ACK: none, custom_find · ops: CreateTable, GetResourcePolicy, DescribeKinesisStreamingDestination
  - repro: table with policy + ACTIVE destination: DeleteTable; wait gone; CreateTable same name; poll the two
    reads every 0.5s
  - handling: not handled in the controller (as of commit 34b85e6)
  - related: [DDB-TABLE-214](../table-policy-kinesis-autoscaling.md#ddb-table-214), [DDB-TABLE-236](../table-policy-kinesis-autoscaling.md#ddb-table-236), [DDB-TABLE-097](../table-subresources.md#ddb-table-097), [DDB-TABLE-342](../table-subresources.md#ddb-table-342), [DDB-TABLE-374](../service.md#ddb-table-374), [DDB-TABLE-436](../table-policy-kinesis-autoscaling.md#ddb-table-436),
    [DDB-TABLE-377](../service.md#ddb-table-377) · hypotheses: H-S-121 · evidence: table/creative/policy-kinesis-followups

## Notes

H-S-121 clean-slate clause confirmed; transient-leak clause refuted (no leak seen).
