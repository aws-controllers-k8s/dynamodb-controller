<!-- generated: ack-api-quirks dynamodb 2026-10-09 model=2012-08-10 controller=34b85e6 -->
# DDB-TABLE-051: OnDemandThroughput on UpdateTable: partial merge, 0 rejected, -1 clears one member; what DescribeTable shows after clearing
_Full entry and notes of one finding; its summary entry is in
[table-throughput-billing.md](../table-throughput-billing.md). Generated from ack-api-quirks
`services/dynamodb` (model 2012-08-10, controller commit 34b85e6); edit the finding in the lab, not here._

## Finding

- <a id="ddb-table-051"></a>**DDB-TABLE-051** `update-granularity` · impact high · unhandled (not handled in controller) · verified 2026-10-08
  **OnDemandThroughput on UpdateTable: partial merge, 0 rejected, -1 clears one member; what DescribeTable shows after clearing**
  Set {1000,500} -> OK (TableStatus=ACTIVE); Describe {"MaxReadRequestUnits": 1000, "MaxWriteRequestUnits":
  500}. Re-send same -> OK (TableStatus=ACTIVE). {MaxReadRequestUnits:2000} alone -> ThrottlingException: 'The
  rate of control plane requests made by this account is too high'; Describe {"MaxReadRequestUnits": 1000,
  "MaxWriteRequestUnits": 500}. {MaxWriteRequestUnits:0} -> ValidationException: 'One or more parameter values
  were invalid: Requested MaxWriteRequestUnits for OnDemandThroughput for table is outside of valid range'.
  {MaxWriteRequestUnits:-1} -> OK (TableStatus=ACTIVE); Describe {"MaxReadRequestUnits": 1000,
  "MaxWriteRequestUnits": -1}. {MaxReadRequestUnits:-1} -> OK (TableStatus=ACTIVE); Describe
  {"MaxReadRequestUnits": -1}. {-1,-1} again -> ThrottlingException: 'The rate of control plane requests made
  by this account is too high'; Describe {"MaxReadRequestUnits": -1}. {} -> InternalFailure: ''.
  {MaxReadRequestUnits:-2} -> ValidationException: 'One or more parameter values were invalid: Requested
  MaxReadRequestUnits for OnDemandThroughput for table is outside of valid range'.
  - ACK: custom_update, compare.is_ignored+delta_pre_compare, compare.nil_equals_zero_value · ops:
    UpdateTable, DescribeTable · fields: OnDemandThroughput.MaxReadRequestUnits,
    OnDemandThroughput.MaxWriteRequestUnits
  - repro: PPR table; UpdateTable OnDemandThroughput {1000,500}; {Read:2000}; {Write:0}; {Write:-1};
    {Read:-1}; {-1,-1}; {}; DescribeTable after each
  - measurements: odt-set-both=0.0, odt-resend-same=0.0, odt-write-minus1=0.0, odt-read-minus1=0.0
  - handling: not handled in the controller (as of commit 34b85e6)
  - related: [DDB-TABLE-053](../service.md#ddb-table-053), [DDB-TABLE-130](../service.md#ddb-table-130), [DDB-TABLE-132](../service.md#ddb-table-132), [DDB-TABLE-441](../table.md#ddb-table-441), [DDB-TABLE-131](../table.md#ddb-table-131), [DDB-TABLE-178](../table-throughput-billing.md#ddb-table-178),
    [DDB-TABLE-182](../table-throughput-billing.md#ddb-table-182) · evidence: table/mutation-matrix/stream-protection-throughput

## Notes

Hypotheses: H-T-121, H-T-039.

Contradiction with [DDB-TABLE-182](../table-throughput-billing.md#ddb-table-182), [DDB-TABLE-178](../table-throughput-billing.md#ddb-table-178): 051 reports DescribeTable showing MaxWriteRequestUnits=-1 /
{MaxReadRequestUnits:-1} after clearing (title: 'what DescribeTable shows after clearing'); 182 (6 reads over
6 s, all orders) and 178 show -1 is visible only in the UpdateTable response and for ~1 s in DescribeTable,
after which the cleared member is omitted and the struct is absent when both are cleared Resolution: keep all;
182 is canonical for the representation, 051's -1 reads are the transient first-second view

Contradiction with [DDB-TABLE-178](../table-throughput-billing.md#ddb-table-178), [DDB-TABLE-053](../service.md#ddb-table-053): 051: {MaxReadRequestUnits:2000} alone -> ThrottlingException
and {-1,-1} again -> ThrottlingException; 178 re-ran both -> 200 (single-member merge to {2000,500}; {-1,-1}
echoed). 053 explains: those were account control-plane rate hits from concurrent probes, not
OnDemandThroughput semantics Resolution: keep both; 178 is canonical for merge semantics, 051 retains the 0 /
-2 -> ValidationException boundaries
