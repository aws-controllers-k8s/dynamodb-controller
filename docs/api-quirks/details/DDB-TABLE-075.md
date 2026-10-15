<!-- generated: ack-api-quirks dynamodb 2026-10-09 model=2012-08-10 controller=34b85e6 -->
# DDB-TABLE-075: DescribeTable right after CreateTable never returned ResourceNotFoundException (0/10 creates, 100 ms polling); first 200 within 90 ms
_Full entry and notes of one finding; its summary entry is in [service.md](../service.md). Generated from
ack-api-quirks `services/dynamodb` (model 2012-08-10, controller commit 34b85e6); edit the finding in the lab,
not here._

## Finding

- <a id="ddb-table-075"></a>**DDB-TABLE-075** `eventual-consistency` · impact high · handled · verified 2026-10-08
  **DescribeTable right after CreateTable never returned ResourceNotFoundException (0/10 creates, 100 ms polling); first 200 within 90 ms** (hypothesis refuted; behavior confirmed)
  Across 10 CreateTable calls (PAY_PER_REQUEST, single key, no indexes), DescribeTable polled every 100 ms for
  5 s from the moment the create response arrived never returned ResourceNotFoundException; the first
  DescribeTable succeeded 7-83 ms after the create response (p50 49 ms) and already reported
  TableStatus=CREATING. CREATING lasted 1.0-2.0 s (p50 2.0 s) for these tables. CreateTable's own response
  already carries TableStatus=CREATING and the TableArn.
  - ACK: requeue, exceptions.404 · ops: CreateTable, DescribeTable
  - repro: CreateTable; DescribeTable in a 100ms loop for 5s; repeat 10x
  - measurements: create_samples=10, rnf_polls=0, samples_with_rnf=0, first_describe_ok_p50_s=0.049,
    first_describe_ok_max_s=0.083, creating_p50_s=2.02, creating_max_s=2.02
  - handling: handled via `test/e2e/table.py:240-269; b323c3d`
  - related: [DDB-TABLE-076](../table.md#ddb-table-076), [DDB-TABLE-181](../table.md#ddb-table-181) · evidence: table/consistency-windows/create-delete-visibility

## Notes

Refutes H-T-050 at n=10 for simple tables: the documented 'DescribeTable immediately after CreateTable might
return ResourceNotFoundException' window was not observable even at 100 ms resolution. Treating a 404 right
after create as 'not yet' is still cheap insurance, but a controller does not need a long grace period.
