<!-- generated: ack-api-quirks dynamodb 2026-10-09 model=2012-08-10 controller=34b85e6 -->
# DDB-TABLE-182: Clearing OnDemandThroughput with -1: -1 visible ~1s in DescribeTable, then the member is omitted; struct absent when both cleared
_Full entry and notes of one finding; its summary entry is in
[table-throughput-billing.md](../table-throughput-billing.md). Generated from ack-api-quirks
`services/dynamodb` (model 2012-08-10, controller commit 34b85e6); edit the finding in the lab, not here._

## Finding

- <a id="ddb-table-182"></a>**DDB-TABLE-182** `response-fidelity` · impact high · unhandled (not handled in controller) · verified 2026-10-09, re-verified
  **Clearing OnDemandThroughput with -1: -1 visible ~1s in DescribeTable, then the member is omitted; struct absent when both cleared**
  DescribeTable OnDemandThroughput (distinct values over 6 reads/6s) per state: {"a1-set":
  ["{\"MaxReadRequestUnits\": 1000, \"MaxWriteRequestUnits\": 500}"], "a2-after-clear-read":
  ["{\"MaxReadRequestUnits\": -1, \"MaxWriteRequestUnits\": 500}", "{\"MaxWriteRequestUnits\": 500}"],
  "a3-after-clear-write": ["\"<absent>\""], "b1-set": ["{\"MaxReadRequestUnits\": 1000,
  \"MaxWriteRequestUnits\": 500}"], "b2-after-clear-write": ["{\"MaxReadRequestUnits\": 1000}"],
  "b3-after-clear-read": ["\"<absent>\""], "c1-set": ["{\"MaxReadRequestUnits\": 1000,
  \"MaxWriteRequestUnits\": 500}"], "c2-after-clear-both": ["\"<absent>\""], "c3-after-clear-both-again":
  ["\"<absent>\""], "d1-set-read-only": ["{\"MaxReadRequestUnits\": 300}"], "d2-after-clear-read":
  ["\"<absent>\""]}. UpdateTable response OnDemandThroughput per call: {"a1-set-1000-500":
  {"MaxReadRequestUnits": 1000, "MaxWriteRequestUnits": 500}, "a2-clear-read": {"MaxReadRequestUnits": -1,
  "MaxWriteRequestUnits": 500}, "a3-clear-write": {"MaxWriteRequestUnits": -1}, "b1-set-1000-500":
  {"MaxReadRequestUnits": 1000, "MaxWriteRequestUnits": 500}, "b2-clear-write": {"MaxReadRequestUnits": 1000,
  "MaxWriteRequestUnits": -1}, "b3-clear-read": {"MaxReadRequestUnits": -1}, "c1-set-1000-500":
  {"MaxReadRequestUnits": 1000, "MaxWriteRequestUnits": 500}, "c2-clear-both": {"MaxReadRequestUnits": -1,
  "MaxWriteRequestUnits": -1}, "c3-clear-both-again": {"MaxReadRequestUnits": -1, "MaxWriteRequestUnits": -1},
  "d1-set-read-only-300": {"MaxReadRequestUnits": 300}, "d2-clear-read": {"MaxReadRequestUnits": -1}}.
  - ACK: compare.is_ignored+delta_pre_compare, compare.nil_equals_zero_value, custom_update · ops:
    UpdateTable, DescribeTable · fields: OnDemandThroughput.MaxReadRequestUnits,
    OnDemandThroughput.MaxWriteRequestUnits
  - repro: PPR table; UpdateTable OnDemandThroughput {1000,500}; {MaxReadRequestUnits:-1}; DescribeTable x6;
    then {MaxWriteRequestUnits:-1}; DescribeTable x6 (and the other orders)
  - measurements: minus1_visible_in_describe_s=1.0
  - handling: not handled in the controller (as of commit 34b85e6)
  - related: [DDB-TABLE-051](../table-throughput-billing.md#ddb-table-051), [DDB-TABLE-178](../table-throughput-billing.md#ddb-table-178), [DDB-TABLE-053](../service.md#ddb-table-053) · evidence:
    table/response-fidelity/odt-minus1-representation, table/creative/reverify-set-b

## Notes

Hypotheses: H-T-121, H-T-039. REFUTES the 'DescribeTable keeps returning -1' claim of both: -1 is visible only
in the UpdateTable response and in DescribeTable for roughly the first second (eventually consistent read),
after which the cleared member is omitted ({MaxWriteRequestUnits:500} after clearing read;
{MaxReadRequestUnits:1000} after clearing write) and the whole OnDemandThroughput struct is absent once both
are cleared. Clearing order does not matter. Re-sending {-1,-1} when nothing is set is accepted (200) and
echoes {-1,-1}. A controller must map nil <-> -1 (never compare -1 to a Describe value) and treat an absent
struct/member as 'unlimited'.

Contradiction with [DDB-TABLE-051](../table-throughput-billing.md#ddb-table-051), [DDB-TABLE-178](../table-throughput-billing.md#ddb-table-178): 051 reports DescribeTable showing MaxWriteRequestUnits=-1 /
{MaxReadRequestUnits:-1} after clearing (title: 'what DescribeTable shows after clearing'); 182 (6 reads over
6 s, all orders) and 178 show -1 is visible only in the UpdateTable response and for ~1 s in DescribeTable,
after which the cleared member is omitted and the struct is absent when both are cleared Resolution: keep all;
182 is canonical for the representation, 051's -1 reads are the transient first-second view
