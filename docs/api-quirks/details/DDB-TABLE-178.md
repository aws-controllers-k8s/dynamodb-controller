<!-- generated: ack-api-quirks dynamodb 2026-10-09 model=2012-08-10 controller=34b85e6 -->
# DDB-TABLE-178: OnDemandThroughput UpdateTable: single-member merge, -1 clears a member (echoed as -1), {} -> HTTP 500 InternalFailure
_Full entry and notes of one finding; its summary entry is in
[table-throughput-billing.md](../table-throughput-billing.md). Generated from ack-api-quirks
`services/dynamodb` (model 2012-08-10, controller commit 34b85e6); edit the finding in the lab, not here._

## Finding

- <a id="ddb-table-178"></a>**DDB-TABLE-178** `update-granularity` · impact high · handled · verified 2026-10-08
  **OnDemandThroughput UpdateTable: single-member merge, -1 clears a member (echoed as -1), {} -> HTTP 500 InternalFailure**
  {1000,500} -> OK response={"TableStatus": "ACTIVE", "OnDemandThroughput": {"MaxReadRequestUnits": 1000,
  "MaxWriteRequestUnits": 500}}; -> Describe {"MaxReadRequestUnits": 1000, "MaxWriteRequestUnits": 500}.
  {Read:2000} -> Describe {"MaxReadRequestUnits": 2000, "MaxWriteRequestUnits": 500}. {Read:-1} -> Describe
  {"MaxWriteRequestUnits": 500}. {Write:-1} -> Describe "<absent>". {-1,-1} again -> OK
  response={"TableStatus": "ACTIVE", "OnDemandThroughput": {"MaxReadRequestUnits": -1, "MaxWriteRequestUnits":
  -1}}; de -> Describe "<absent>". {} -> InternalFailure (HTTP 500): '' / InternalFailure (HTTP 500): ''.
  {Write:700} -> Describe {"MaxWriteRequestUnits": 700}. {Write:700} again -> OK response={"TableStatus":
  "ACTIVE", "OnDemandThroughput": {"MaxWriteRequestUnits": 700}}; describe transitions={"timed.
  - ACK: custom_update, compare.is_ignored+delta_pre_compare, compare.nil_equals_zero_value · ops:
    UpdateTable, DescribeTable · fields: OnDemandThroughput.MaxReadRequestUnits,
    OnDemandThroughput.MaxWriteRequestUnits
  - repro: PPR table; UpdateTable OnDemandThroughput {1000,500}; {Read:2000}; {Read:-1}; {Write:-1}; {};
    DescribeTable after each
  - handling: handled via `test/e2e/tests/test_table.py:37-42; test/e2e/tests/test_table.py:544-556; generator.yaml:88-90; pkg/resource/table/sdk.go:1234-1250`
  - related: [DDB-TABLE-051](../table-throughput-billing.md#ddb-table-051), [DDB-TABLE-182](../table-throughput-billing.md#ddb-table-182), [DDB-TABLE-053](../service.md#ddb-table-053) · evidence:
    table/response-fidelity/create-update-response

## Notes

H-T-121 and H-T-039 partial. The -1 values seen in DescribeTable right after an update are transient (about
1s, eventually consistent); the steady-state representation omits cleared members and omits the struct when
both are cleared (see table/response-fidelity/odt-minus1-representation). OnDemandThroughput={} on UpdateTable
returned HTTP 500 InternalFailure 4/4 times across two runs (CreateTable accepts {}).

Contradiction with [DDB-TABLE-051](../table-throughput-billing.md#ddb-table-051), [DDB-TABLE-182](../table-throughput-billing.md#ddb-table-182): 051 reports DescribeTable showing MaxWriteRequestUnits=-1 /
{MaxReadRequestUnits:-1} after clearing (title: 'what DescribeTable shows after clearing'); 182 (6 reads over
6 s, all orders) and 178 show -1 is visible only in the UpdateTable response and for ~1 s in DescribeTable,
after which the cleared member is omitted and the struct is absent when both are cleared Resolution: keep all;
182 is canonical for the representation, 051's -1 reads are the transient first-second view

Contradiction with [DDB-TABLE-051](../table-throughput-billing.md#ddb-table-051), [DDB-TABLE-053](../service.md#ddb-table-053): 051: {MaxReadRequestUnits:2000} alone -> ThrottlingException
and {-1,-1} again -> ThrottlingException; 178 re-ran both -> 200 (single-member merge to {2000,500}; {-1,-1}
echoed). 053 explains: those were account control-plane rate hits from concurrent probes, not
OnDemandThroughput semantics Resolution: keep both; 178 is canonical for merge semantics, 051 retains the 0 /
-2 -> ValidationException boundaries
