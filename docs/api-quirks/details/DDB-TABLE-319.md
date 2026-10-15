<!-- generated: ack-api-quirks dynamodb 2026-10-09 model=2012-08-10 controller=34b85e6 -->
# DDB-TABLE-319: Deleted Kinesis stream: destination stays ACTIVE (293s watched, no description) and Update still succeeds; Disable+recreate+Enable works
_Full entry and notes of one finding; its summary entry is in
[table-policy-kinesis-autoscaling.md](../table-policy-kinesis-autoscaling.md). Generated from ack-api-quirks
`services/dynamodb` (model 2012-08-10, controller commit 34b85e6); edit the finding in the lab, not here._

## Finding

- <a id="ddb-table-319"></a>**DDB-TABLE-319** `prerequisite` · impact high · unhandled (not handled in controller) · verified 2026-10-09
  **Deleted Kinesis stream: destination stays ACTIVE (293s watched, no description) and Update still succeeds; Disable+recreate+Enable works**
  Enable(E3) -> ACTIVE; DeleteStream(E3) -> 200; the stream was gone from Kinesis at t=10s but
  DescribeKinesisStreamingDestination kept reporting ['self:e3=ACTIVE'] at every 10s sample for 293s with no
  DestinationStatusDescription. UpdateKinesisStreamingDestination(precision) on the dead destination -> 200
  OK, UPDATING for 123.55s, then ACTIVE with the new precision. Disable -> 200 OK (Describe right after still
  ACTIVE for ~100ms; see the Describe-lag finding). Recreating a stream with the same name (identical ARN) and
  Enable 6s later -> 200 OK, ENABLING 2.02s, then ACTIVE (precision reset: [('self:e3', 'ACTIVE', None)]).
  Deleting the stream DURING ENABLING instead: Enable(E1) -> 200 ENABLING, DeleteStream 50ms later -> 200;
  ENABLING lasted 17.19s (vs 2-7s normally) and ended as ENABLE_FAILED 'User does not have a permission to use
  kinesis stream'.
  - ACK: references, annotation-shadow-state, custom_update, pre-delete-cleanup · ops:
    EnableKinesisStreamingDestination, DescribeKinesisStreamingDestination, UpdateKinesisStreamingDestination,
    DisableKinesisStreamingDestination · fields: KinesisDataStreamDestinations[].DestinationStatus,
    KinesisDataStreamDestinations[].DestinationStatusDescription
  - repro: Enable; wait ACTIVE; DeleteStream; Describe every 10s for 5 min; Update; Disable; recreate the
    stream; Enable. Separately: Enable then DeleteStream within 100ms; poll
  - measurements: broken_watch_s=293, stream_gone_at_s=10, update_on_dead_destination_updating_s=123.55,
    enabling_when_stream_deleted_mid_flight_s=17.19
  - handling: not handled in the controller (as of commit 34b85e6)
  - related: [DDB-TABLE-299](../table-policy-kinesis-autoscaling.md#ddb-table-299), [DDB-TABLE-300](../table-policy-kinesis-autoscaling.md#ddb-table-300), [DDB-TABLE-301](../table-policy-kinesis-autoscaling.md#ddb-table-301), [DDB-TABLE-302](../table-policy-kinesis-autoscaling.md#ddb-table-302), [DDB-TABLE-304](../table-policy-kinesis-autoscaling.md#ddb-table-304), [DDB-TABLE-269](../table-policy-kinesis-autoscaling.md#ddb-table-269),
    [DDB-TABLE-268](../table-policy-kinesis-autoscaling.md#ddb-table-268), [DDB-TABLE-303](../table-policy-kinesis-autoscaling.md#ddb-table-303), [DDB-TABLE-235](../table-policy-kinesis-autoscaling.md#ddb-table-235), [DDB-TABLE-236](../table-policy-kinesis-autoscaling.md#ddb-table-236), [DDB-TABLE-234](../table-policy-kinesis-autoscaling.md#ddb-table-234), [DDB-TABLE-460](../table-policy-kinesis-autoscaling.md#ddb-table-460) · hypotheses:
    H-S-035, H-S-034, H-S-038 · evidence: table/error-taxonomy/kinesis-destination-errors

## Notes

H-S-035 confirmed: DestinationStatus is not a health signal; the controller must cross-check the Kinesis
stream (DescribeStreamSummary) itself. H-S-034's ENABLE_FAILED recipe (stream deleted mid-ENABLING) confirmed,
but with the misleading 'permission' description. H-S-038's 'Disable of ENABLE_FAILED -> DISABLING' is refuted
elsewhere (ValidationException); here Disable of a dead-but-ACTIVE destination succeeds.
