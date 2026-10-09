<!-- generated: ack-api-quirks dynamodb 2026-10-09 model=2012-08-10 controller=34b85e6 -->
# DDB-TABLE-304: Describe lags Disable ~100ms (still ACTIVE after Disable returned DISABLING); Enable 10ms after Disable rejected, ~600ms after accepted
_Full entry and notes of one finding; its summary entry is in
[table-policy-kinesis-autoscaling.md](../table-policy-kinesis-autoscaling.md). Generated from ack-api-quirks
`services/dynamodb` (model 2012-08-10, controller commit 34b85e6); edit the finding in the lab, not here._

## Finding

- <a id="ddb-table-304"></a>**DDB-TABLE-304** `stale-response` · impact medium · unhandled (not handled in controller) · verified 2026-10-09
  **Describe lags Disable ~100ms (still ACTIVE after Disable returned DISABLING); Enable 10ms after Disable rejected, ~600ms after accepted**
  Disable(K1) -> 200 OK DestinationStatus=DISABLING; Enable(K1) 11ms later -> ValidationException (HTTP 400)
  'Table is not in a valid state to enable Kinesis Streaming Destination: EnableKinesisStreamingDestination
  must be DISABLED or ENABLE_FAILED to perform '; four Describes 46-93ms after the Disable all still showed
  ACTIVE; a further Disable 100ms later -> 200 DISABLING and the entry read DISABLED 2.2s later. In an earlier
  run of this probe an Enable issued ~630ms after Disable was ACCEPTED (200 ENABLING) and the entry went
  ENABLING->ACTIVE, i.e. the DISABLING->DISABLED transition had completed internally before Describe reflected
  it.
  - ACK: requeue, synced.when, e2e-timing · ops: DisableKinesisStreamingDestination,
    DescribeKinesisStreamingDestination, EnableKinesisStreamingDestination · fields:
    KinesisDataStreamDestinations[].DestinationStatus
  - repro: Enable; wait ACTIVE; Disable; Enable immediately; Describe within 100ms; (variant) Disable then
    Enable after ~600ms
  - measurements: stale_active_after_disable_ms=93
  - handling: not handled in the controller (as of commit 34b85e6)
  - related: [DDB-TABLE-269](../table-policy-kinesis-autoscaling.md#ddb-table-269), [DDB-TABLE-299](../table-policy-kinesis-autoscaling.md#ddb-table-299), [DDB-TABLE-300](../table-policy-kinesis-autoscaling.md#ddb-table-300), [DDB-TABLE-301](../table-policy-kinesis-autoscaling.md#ddb-table-301), [DDB-TABLE-302](../table-policy-kinesis-autoscaling.md#ddb-table-302), [DDB-TABLE-268](../table-policy-kinesis-autoscaling.md#ddb-table-268),
    [DDB-TABLE-319](../table-policy-kinesis-autoscaling.md#ddb-table-319), [DDB-TABLE-303](../table-policy-kinesis-autoscaling.md#ddb-table-303), [DDB-TABLE-235](../table-policy-kinesis-autoscaling.md#ddb-table-235), [DDB-TABLE-236](../table-policy-kinesis-autoscaling.md#ddb-table-236), [DDB-TABLE-234](../table-policy-kinesis-autoscaling.md#ddb-table-234), [DDB-TABLE-460](../table-policy-kinesis-autoscaling.md#ddb-table-460) · hypotheses:
    H-S-130, H-S-033 · evidence: table/sub-resources/kinesis-destination

## Notes

Qualifies H-S-130 for Kinesis: a sub-second stale read exists after Disable (not after Enable/Update). A
disable->enable switch loop must poll Describe until DISABLED before Enabling, and must not treat a stale
ACTIVE as 'disable failed'.
