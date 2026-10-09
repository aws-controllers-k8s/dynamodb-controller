<!-- generated: ack-api-quirks dynamodb 2026-10-09 model=2012-08-10 controller=34b85e6 -->
# DDB-TABLE-050: Stream view type cannot be changed in place; disable requires no StreamViewType; LatestStreamArn survives disable
_Full entry and notes of one finding; its summary entry is in
[table-streams-encryption-class.md](../table-streams-encryption-class.md). Generated from ack-api-quirks
`services/dynamodb` (model 2012-08-10, controller commit 34b85e6); edit the finding in the lab, not here._

## Finding

- <a id="ddb-table-050"></a>**DDB-TABLE-050** `update-granularity` · impact high · SUSPECTED CONTROLLER BUG · verified 2026-10-09, re-verified
  **Stream view type cannot be changed in place; disable requires no StreamViewType; LatestStreamArn survives disable**
  Enable {true,NEW_IMAGE} -> OK (TableStatus=UPDATING), UPDATING 4.05s. Re-send same -> ValidationException:
  'Table already has an enabled stream: TableName: ackq-8c14eb-mm-a'. Change to NEW_AND_OLD_IMAGES while
  enabled -> ValidationException: 'Table already has an enabled stream: TableName: ackq-8c14eb-mm-a'. {true}
  without view type -> ValidationException: 'One or more parameter values were invalid: If stream is being
  enabled then UpdateViewType is required'. Disable with {false,KEYS_ONLY} -> ValidationException: 'One or
  more parameter values were invalid: If stream is being disabled, then UpdateViewType must not be specified'.
  Disable {false} -> OK (TableStatus=UPDATING), UPDATING 5.07s; DescribeTable afterwards:
  {"StreamSpecification": "<absent>", "LatestStreamArn":
  "arn:aws:dynamodb:us-west-2:<ACCOUNT>:table/ackq-8c14eb-mm-a/stream/2026-10-08T23:07:31.275",
  "LatestStreamLabel": "2026-10-08T23:07:31.275"}. Disable again -> ValidationException: 'Table has no stream
  to disable: TableName: ackq-8c14eb-mm-a'. Re-enable -> OK (TableStatus=UPDATING), UPDATING 5.06s; new
  LatestStreamArn differs: True. Old stream DescribeStream: {"ok": true, "code": null, "status": "DISABLED"}.
  - ACK: custom_update, requeue, compare.is_ignored+delta_pre_compare · ops: UpdateTable, DescribeTable ·
    fields: StreamSpecification.StreamEnabled, StreamSpecification.StreamViewType, LatestStreamArn,
    LatestStreamLabel
  - repro: PPR table; UpdateTable stream enable NEW_IMAGE; re-send; change view type; disable with view type;
    disable; disable again; re-enable NEW_AND_OLD_IMAGES
  - measurements: stream_enable_updating_s=4.05, stream_disable_updating_s=5.07,
    stream_reenable_updating_s=5.06
  - handling: suspected controller bug - see Handling gaps
  - related: [DDB-TABLE-035](../table-streams-encryption-class.md#ddb-table-035), [DDB-TABLE-018](../table-streams-encryption-class.md#ddb-table-018), [DDB-TABLE-002](../table-streams-encryption-class.md#ddb-table-002), [DDB-TABLE-029](../table-streams-encryption-class.md#ddb-table-029), [DDB-TABLE-040](../table-throughput-billing.md#ddb-table-040), [DDB-TABLE-036](../table-streams-encryption-class.md#ddb-table-036),
    [DDB-TABLE-362](../table-streams-encryption-class.md#ddb-table-362), [DDB-TABLE-367](../table-streams-encryption-class.md#ddb-table-367), [DDB-TABLE-378](../table-streams-encryption-class.md#ddb-table-378), [DDB-TABLE-180](../table-streams-encryption-class.md#ddb-table-180), [DDB-TABLE-004](../table-streams-encryption-class.md#ddb-table-004), [DDB-TABLE-373](../service.md#ddb-table-373), [DDB-TABLE-013](../service.md#ddb-table-013),
    [DDB-TABLE-071](../service.md#ddb-table-071), [DDB-TABLE-213](../table-streams-encryption-class.md#ddb-table-213) · evidence: table/mutation-matrix/stream-protection-throughput,
    table/creative/reverify-set-b

## Notes

Hypotheses: H-T-025, H-T-026, H-T-036.

Suspected controller bug confirmed by evidence: AWS side of the suspicion verified: after disabling,
DescribeTable omits StreamSpecification entirely (observed nil) and a repeated StreamEnabled=false is rejected
with ValidationException 'Table has no stream to disable' (050); never-enabled tables also report no
StreamSpecification (029). A spec with streamEnabled=false therefore diffs against nil and each re-send is a
terminal ValidationException - a pre-compare normalization (nil observed == {StreamEnabled:false}) is needed.
