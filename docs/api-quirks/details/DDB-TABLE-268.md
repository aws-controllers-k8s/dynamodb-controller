<!-- generated: ack-api-quirks dynamodb 2026-10-09 model=2012-08-10 controller=34b85e6 -->
# DDB-TABLE-268: ENABLE_FAILED entries accumulate (one per bad ARN, ~2.02s after Enable); Enable is rejected only while another entry is in flight/ACTIVE
_Full entry and notes of one finding; its summary entry is in
[table-policy-kinesis-autoscaling.md](../table-policy-kinesis-autoscaling.md). Generated from ack-api-quirks
`services/dynamodb` (model 2012-08-10, controller commit 34b85e6); edit the finding in the lab, not here._

## Finding

- <a id="ddb-table-268"></a>**DDB-TABLE-268** `async-state-machine` · impact high · unhandled (not handled in controller) · verified 2026-10-09
  **ENABLE_FAILED entries accumulate (one per bad ARN, ~2.02s after Enable); Enable is rejected only while another entry is in flight/ACTIVE**
  Enable(nonexistent stream ARN n1) -> 200 OK DestinationStatus=ENABLING; timeline [('-none1=ENABLING', 2.02),
  ('-none1=ENABLE_FAILED', None)]; entry [{'stream': '-none1', 'status': 'ENABLE_FAILED', 'desc': 'User does
  not have a permission to use kinesis stream', 'precision': None}]. Enable(n1) again on its ENABLE_FAILED
  entry -> 200 OK; entries for n1 afterwards: 1 (flipped, not duplicated). Enable(n2) while n1 is
  ENABLE_FAILED -> 200 OK; entries [('-none2', 'ENABLE_FAILED'), ('-none1', 'ENABLE_FAILED')]. On the
  ENABLE_FAILED entry: Update -> ValidationException (HTTP 400) 'Table is not in a valid state to enable
  Kinesis Streaming Destination: Kinesis streaming is not in ACTIVE state. Updates are only allowed in ACTIVE
  state. TableName: ackq'; Disable -> ValidationException (HTTP 400) 'Table is not in a valid state to enable
  Kinesis Streaming Destination: KinesisStreamingDestination must be ACTIVE to perform DISABLE operation.'
  (timeline []; entries after [('-none2', 'ENABLE_FAILED', 'User does not have a permission to use kinesis
  stream'), ('-none1', 'ENABLE_FAILED', 'User does not have a permission to use kinesis stream')]).
  Enable(valid G1) alongside the failed entries -> 200 OK (timeline
  [('-fu-g1=ENABLING,-none2=ENABLE_FAILED,-none1=ENABLE_FAILED', 7.08),
  ('-fu-g1=ACTIVE,-none2=ENABLE_FAILED,-none1=ENABLE_FAILED', None)]). While G1 is ACTIVE: Enable(bad n3) ->
  ValidationException (HTTP 400) 'Table is not in a valid state to enable Kinesis Streaming Destination:
  EnableKinesisStreamingDestination must be DISABLED or ENABLE_FAILED to perform ENABLE operation.';
  Enable(valid G2) -> ValidationException (HTTP 400) 'Table is not in a valid state to enable Kinesis
  Streaming Destination: EnableKinesisStreamingDestination must be DISABLED or ENABLE_FAILED to perform ENABLE
  operation.'. After Disable(G1): Enable(n3) -> ValidationException (HTTP 400) 'Table is not in a valid state
  to enable Kinesis Streaming Destination: EnableKinesisStreamingDestination must be DISABLED or ENABLE_FAILED
  to perform ENABLE operation.' (entries [('-fu-g1', 'ACTIVE'), ('-none2', 'ENABLE_FAILED'), ('-none1',
  'ENABLE_FAILED')]). Re-Enable(G1) with the failed/disabled entries present -> ValidationException (HTTP 400)
  'Table is not in a valid state to enable Kinesis Streaming Destination: EnableKinesisStreamingDestination
  must be DISABLED or ENABLE_FAILED to perform ENABLE operation.'; final entries [('-fu-g1', 'ACTIVE'),
  ('-none2', 'ENABLE_FAILED'), ('-none1', 'ENABLE_FAILED')].
  - ACK: custom_update, list_operation.match_fields, requeue, terminal_codes, annotation-shadow-state · ops:
    EnableKinesisStreamingDestination, DescribeKinesisStreamingDestination,
    DisableKinesisStreamingDestination, UpdateKinesisStreamingDestination · fields:
    KinesisDataStreamDestinations[].DestinationStatus,
    KinesisDataStreamDestinations[].DestinationStatusDescription
  - repro: Enable(bogus ARN) -> poll -> ENABLE_FAILED; Enable(bogus) again; Enable(bogus2); Update/Disable the
    failed entry; Enable(valid); Enable(bogus3) while ACTIVE
  - measurements: enabling_to_enable_failed_s=2.02
  - handling: not handled in the controller (as of commit 34b85e6)
  - related: [DDB-TABLE-299](../table-policy-kinesis-autoscaling.md#ddb-table-299), [DDB-TABLE-300](../table-policy-kinesis-autoscaling.md#ddb-table-300), [DDB-TABLE-301](../table-policy-kinesis-autoscaling.md#ddb-table-301), [DDB-TABLE-302](../table-policy-kinesis-autoscaling.md#ddb-table-302), [DDB-TABLE-304](../table-policy-kinesis-autoscaling.md#ddb-table-304), [DDB-TABLE-269](../table-policy-kinesis-autoscaling.md#ddb-table-269),
    [DDB-TABLE-319](../table-policy-kinesis-autoscaling.md#ddb-table-319), [DDB-TABLE-303](../table-policy-kinesis-autoscaling.md#ddb-table-303), [DDB-TABLE-235](../table-policy-kinesis-autoscaling.md#ddb-table-235), [DDB-TABLE-236](../table-policy-kinesis-autoscaling.md#ddb-table-236), [DDB-TABLE-234](../table-policy-kinesis-autoscaling.md#ddb-table-234), [DDB-TABLE-460](../table-policy-kinesis-autoscaling.md#ddb-table-460) · hypotheses:
    H-S-034, H-S-037, H-S-038, H-S-113 · evidence: table/creative/policy-kinesis-followups

## Notes

H-S-037's 'ENABLE_FAILED entry for the same StreamArn blocks re-Enable with ResourceInUseException' is refuted
(re-Enable is accepted and flips the entry). The admissibility rule is: a table may have at most one entry in
ENABLING/ACTIVE/UPDATING/DISABLING; any number of DISABLED/ENABLE_FAILED entries linger. Caveat: the A7/A8
Enables were issued 50-90ms after Disable(G1) returned DISABLING while Describe still reported G1 ACTIVE (see
the Describe-lag finding), so they measure 'Enable while DISABLING' (rejected), not 'Enable after DISABLED'. A
controller must filter the list by status and read DestinationStatusDescription to surface the async failure.

Contradiction with [DDB-TABLE-299](../table-policy-kinesis-autoscaling.md#ddb-table-299): 299 states 'one destination per table'; 268 and [DDB-TABLE-301](../table-policy-kinesis-autoscaling.md#ddb-table-301) show several
DISABLED/ENABLE_FAILED entries coexisting with one ACTIVE entry (list of 3), re-Enable flipping an existing
entry, and Enable rejected only while another entry is ENABLING/ACTIVE/UPDATING/DISABLING Resolution: keep
both; 268's rule 'at most one live entry, any number of DISABLED/ENABLE_FAILED' is canonical; fix 299's title
