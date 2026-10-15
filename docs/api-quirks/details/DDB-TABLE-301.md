<!-- generated: ack-api-quirks dynamodb 2026-10-09 model=2012-08-10 controller=34b85e6 -->
# DDB-TABLE-301: Disable: ~2s DISABLING, then a DISABLED entry lingers (>=171s); re-Enable flips it (no duplicate); Disable w/ config -> ValidationException
_Full entry and notes of one finding; its summary entry is in
[table-policy-kinesis-autoscaling.md](../table-policy-kinesis-autoscaling.md). Generated from ack-api-quirks
`services/dynamodb` (model 2012-08-10, controller commit 34b85e6); edit the finding in the lab, not here._

## Finding

- <a id="ddb-table-301"></a>**DDB-TABLE-301** `delete-semantics` · impact high · unhandled (not handled in controller) · verified 2026-10-09
  **Disable: ~2s DISABLING, then a DISABLED entry lingers (>=171s); re-Enable flips it (no duplicate); Disable w/ config -> ValidationException**
  Disable(K1, EnableKinesisStreamingConfiguration={MICROSECOND}) -> ValidationException (HTTP 400) 'Stream
  cannot be disabled using EnableKinesisStreamingConfiguration parameters'. Disable(K1) -> 200 OK
  DestinationStatus=DISABLING. A second Disable issued during DISABLING -> 200 OK (it blocked 1460 ms and
  returned DISABLING); Update during DISABLING -> ValidationException (HTTP 400) 'Table is not in a valid
  state to enable Kinesis Streaming Destination: Kinesis streaming is not in ACTIVE state. Updates are only
  allowed in ACTIVE st'. The first Describe after the second Disable already showed DISABLED (DISABLING lasts
  ~1-2s). The DISABLED entry stayed listed at every 10s sample for 171s: [(0, 'ks1=DISABLED'), (30,
  'ks1=DISABLED'), (60, 'ks1=DISABLED'), (90, 'ks1=DISABLED'), (121, 'ks1=DISABLED'), (151, 'ks1=DISABLED')].
  On the DISABLED entry: Update -> ValidationException (HTTP 400) 'Table is not in a valid state to enable
  Kinesis Streaming Destination: Kinesis streaming is not in ACTIVE state. Updates are only allowed in ACTIVE
  st'; Disable -> ValidationException (HTTP 400) 'Table is not in a valid state to enable Kinesis Streaming
  Destination: KinesisStreamingDestination must be ACTIVE to perform DISABLE operation.'. Re-Enable(K1) -> 200
  OK ENABLING; Describe 0.049s later: [('ks1', 'ENABLING')]; ENABLING lasted 3.03s; entries after: [('ks1',
  'ACTIVE', None)] (count for K1 = 1, i.e. the DISABLED entry flipped rather than a second entry being added).
  - ACK: custom_delete, compare.is_ignored+delta_pre_compare, requeue, list_operation.match_fields,
    custom_field · ops: DisableKinesisStreamingDestination, DescribeKinesisStreamingDestination,
    EnableKinesisStreamingDestination, UpdateKinesisStreamingDestination · fields:
    KinesisDataStreamDestinations[].DestinationStatus, EnableKinesisStreamingConfiguration
  - repro: Enable; wait ACTIVE; Disable with EnableKinesisStreamingConfiguration; Disable; Disable again
    immediately; poll 1/s; keep describing for 3 min; Update/Disable the DISABLED entry; Enable again
  - measurements: disabled_entry_still_listed_after_s=171, re_enabling_duration_s=3.03,
    second_disable_latency_ms=1460
  - handling: not handled in the controller (as of commit 34b85e6)
  - related: [DDB-TABLE-299](../table-policy-kinesis-autoscaling.md#ddb-table-299), [DDB-TABLE-300](../table-policy-kinesis-autoscaling.md#ddb-table-300), [DDB-TABLE-302](../table-policy-kinesis-autoscaling.md#ddb-table-302), [DDB-TABLE-304](../table-policy-kinesis-autoscaling.md#ddb-table-304), [DDB-TABLE-269](../table-policy-kinesis-autoscaling.md#ddb-table-269), [DDB-TABLE-268](../table-policy-kinesis-autoscaling.md#ddb-table-268),
    [DDB-TABLE-319](../table-policy-kinesis-autoscaling.md#ddb-table-319), [DDB-TABLE-303](../table-policy-kinesis-autoscaling.md#ddb-table-303), [DDB-TABLE-235](../table-policy-kinesis-autoscaling.md#ddb-table-235), [DDB-TABLE-236](../table-policy-kinesis-autoscaling.md#ddb-table-236), [DDB-TABLE-234](../table-policy-kinesis-autoscaling.md#ddb-table-234), [DDB-TABLE-460](../table-policy-kinesis-autoscaling.md#ddb-table-460) · hypotheses:
    H-S-008, H-S-038, H-S-036, H-S-115 · evidence: table/sub-resources/kinesis-destination

## Notes

H-S-008 DISABLED-lingers clause confirmed (>= 3 min; a controller must filter DISABLED/ENABLE_FAILED entries
out of the desired-state diff). H-S-038 'Disable accepts and ignores EnableKinesisStreamingConfiguration'
REFUTED (ValidationException 'Stream cannot be disabled using EnableKinesisStreamingConfiguration
parameters'). H-S-036 'Update on DISABLED -> ResourceNotFoundException' refuted (ValidationException). H-S-115
duplicate-entry clause REFUTED (single entry flips).
