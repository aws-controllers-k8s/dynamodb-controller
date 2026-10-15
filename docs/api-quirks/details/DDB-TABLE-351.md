<!-- generated: ack-api-quirks dynamodb 2026-10-09 model=2012-08-10 controller=34b85e6 -->
# DDB-TABLE-351: Policy canonical form is a byte-stable fixpoint (minified; Version,Statement / Sid,Effect,Principal,Action,Resource,Condition); re-Put no-op
_Full entry and notes of one finding; its summary entry is in
[table-policy-kinesis-autoscaling.md](../table-policy-kinesis-autoscaling.md). Generated from ack-api-quirks
`services/dynamodb` (model 2012-08-10, controller commit 34b85e6); edit the finding in the lab, not here._

## Finding

- <a id="ddb-table-351"></a>**DDB-TABLE-351** `normalization` · impact high · unhandled (not handled in controller) · verified 2026-10-09
  **Policy canonical form is a byte-stable fixpoint (minified; Version,Statement / Sid,Effect,Principal,Action,Resource,Condition); re-Put no-op**
  20/25 formatting variants were accepted; every read-back was minified (no whitespace outside strings) with
  top-level keys ['Version', 'Statement'] and statement keys in the order ['Sid', 'Effect', 'Principal',
  'Action', 'Resource', 'Condition'] (Condition last, NotAction in Action's slot). A document SENT in that
  form read back byte-identical (True), and the Get output of a scrambled-key variant Put on a fresh table
  read back byte-identical (True); a pretty-printed re-Put of it was a no-op (same RevisionId: True).
  Re-Putting the Get output inside the 15 s window returned the same RevisionId (no-op) for 19/20 accepted
  variants; the exception is 'Version omitted' (see the Version finding). Get showed the new RevisionId
  1.07-2.35 s after Put.
  - ACK: is_document, is_iam_policy, compare.is_ignored+delta_pre_compare · ops: PutResourcePolicy,
    GetResourcePolicy · fields: Policy, RevisionId
  - repro: Put each variant on a hash-only PPR table; Get until RevisionId matches; compare bytes; re-Put the
    Get output; Put the Get output on a fresh table
  - measurements: variants_accepted=20, reput_noop=19, get_lag_s_min=1.07, get_lag_s_max=2.35
  - handling: not handled in the controller (as of commit 34b85e6)
  - related: [DDB-TABLE-207](../table-policy-kinesis-autoscaling.md#ddb-table-207), [DDB-TABLE-208](../table-policy-kinesis-autoscaling.md#ddb-table-208), [DDB-TABLE-211](../table-policy-kinesis-autoscaling.md#ddb-table-211), [DDB-TABLE-244](../table-policy-kinesis-autoscaling.md#ddb-table-244), [DDB-TABLE-245](../table-policy-kinesis-autoscaling.md#ddb-table-245), [DDB-TABLE-246](../table-policy-kinesis-autoscaling.md#ddb-table-246),
    [DDB-TABLE-248](../table-policy-kinesis-autoscaling.md#ddb-table-248), [DDB-TABLE-346](../table-policy-kinesis-autoscaling.md#ddb-table-346), [DDB-TABLE-233](../table-policy-kinesis-autoscaling.md#ddb-table-233), [DDB-TABLE-352](../table-policy-kinesis-autoscaling.md#ddb-table-352), [DDB-TABLE-353](../table-policy-kinesis-autoscaling.md#ddb-table-353), [DDB-TABLE-354](../table-policy-kinesis-autoscaling.md#ddb-table-354), [DDB-TABLE-355](../table-policy-kinesis-autoscaling.md#ddb-table-355),
    [DDB-TABLE-356](../table-policy-kinesis-autoscaling.md#ddb-table-356) · hypotheses: H-S-028 · evidence: table/round-trip/policy-canonicalization

## Notes

Refines [DDB-TABLE-208](../table-policy-kinesis-autoscaling.md#ddb-table-208) ('never byte-identical'): the service's canonical form IS reproducible, so a controller
can canonicalize the spec (minify, fixed key order, the rewrites below) and string-compare, or simply compare
parsed JSON after applying the rewrites. Variants NOT no-op on re-Put: ['version_omitted'].

Contradiction with [DDB-TABLE-208](../table-policy-kinesis-autoscaling.md#ddb-table-208): 208 says the Get document is 'never byte-identical' to the Put; 351 shows
the canonical form is a byte-stable fixpoint (a document sent minified in Version/Statement //
Sid,Effect,Principal,Action,Resource,Condition order reads back byte-identical, and re-Putting the Get output
is a no-op for 19/20 variants). 208 only tested non-canonical inputs Resolution: keep both; 351 is canonical;
fix 208's title
