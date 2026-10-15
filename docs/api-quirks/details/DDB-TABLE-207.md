<!-- generated: ack-api-quirks dynamodb 2026-10-09 model=2012-08-10 controller=34b85e6 -->
# DDB-TABLE-207: RevisionId is the epoch-ms change time; equivalent re-Puts (whitespace/key order/list-vs-string/account-id) are no-ops, same id
_Full entry and notes of one finding; its summary entry is in
[table-policy-kinesis-autoscaling.md](../table-policy-kinesis-autoscaling.md). Generated from ack-api-quirks
`services/dynamodb` (model 2012-08-10, controller commit 34b85e6); edit the finding in the lab, not here._

## Finding

- <a id="ddb-table-207"></a>**DDB-TABLE-207** `idempotency` · impact high · handled · verified 2026-10-09
  **RevisionId is the epoch-ms change time; equivalent re-Puts (whitespace/key order/list-vs-string/account-id) are no-ops, same id**
  PutResourcePolicy(pretty-printed) -> rev A. Re-Put identical -> same id (True). Minified -> same (True).
  Keys reordered -> same (True). Action as plain string instead of 1-element list -> same (True). Principal as
  bare account id instead of root ARN -> same (True). All of these were accepted INSIDE the 15s window that
  rejects different documents, i.e. equivalence is decided on the canonical form. Re-Put of document A after a
  different policy -> same id as the first A: False (ids are change timestamps, not content hashes). Ids are
  13-digit strings, strictly increasing in change order: True.
  - ACK: compare.is_ignored+delta_pre_compare, is_document, annotation-shadow-state · ops: PutResourcePolicy ·
    fields: ResourcePolicy, RevisionId
  - repro: Put(pretty); Put(pretty); Put(minified); Put(reordered); Put(action-string); Put(account-id
    principal); wait 16s; Put(other); wait 16s; Put(pretty) - compare RevisionIds
  - measurements: revision_id_len=13
  - handling: handled via `test/e2e/tests/test_table.py:1139-1180; pkg/resource/table/hooks_resource_policy.go:30-45; pkg/resource/table/hooks_resource_policy.go:139-177; pkg/resource/table/hooks_resource_policy_test.go:25-235`
  - related: [DDB-TABLE-208](../table-policy-kinesis-autoscaling.md#ddb-table-208), [DDB-TABLE-351](../table-policy-kinesis-autoscaling.md#ddb-table-351), [DDB-TABLE-352](../table-policy-kinesis-autoscaling.md#ddb-table-352), [DDB-TABLE-353](../table-policy-kinesis-autoscaling.md#ddb-table-353), [DDB-TABLE-354](../table-policy-kinesis-autoscaling.md#ddb-table-354), [DDB-TABLE-355](../table-policy-kinesis-autoscaling.md#ddb-table-355),
    [DDB-TABLE-356](../table-policy-kinesis-autoscaling.md#ddb-table-356), [DDB-TABLE-085](../table-subresources.md#ddb-table-085), [DDB-TABLE-091](../table-subresources.md#ddb-table-091), [DDB-TABLE-341](../table-subresources.md#ddb-table-341), [DDB-TABLE-147](../table-subresources.md#ddb-table-147), [DDB-TABLE-300](../table-policy-kinesis-autoscaling.md#ddb-table-300), [DDB-TABLE-383](../table-streams-encryption-class.md#ddb-table-383),
    [DDB-TABLE-205](../table-policy-kinesis-autoscaling.md#ddb-table-205) · hypotheses: H-S-006, H-S-112, H-S-028 · evidence: table/sub-resources/resource-policy

## Notes

H-S-006 'whitespace-only change yields a new RevisionId' REFUTED; 'byte-identical re-Put returns the same id'
confirmed. H-S-112: numerically monotonic in practice (timestamps), but string-compare is still the only
documented contract.

Contradiction with [DDB-TABLE-352](../table-policy-kinesis-autoscaling.md#ddb-table-352): 207 says 'Principal as bare account id instead of root ARN' is accepted and
equivalent (same RevisionId); 352 and [DDB-TABLE-356](../table-policy-kinesis-autoscaling.md#ddb-table-356) say a bare account-id Principal is rejected with
ValidationException 'Syntax error at position (1,94)'. Not a real conflict: 207/208 used {"AWS":
"<account-id>"} (accepted, rewritten to root ARN), 352/356 used Principal: "<account-id>" without the AWS
wrapper Resolution: keep both; 352 is canonical for the taxonomy; read 207's 'bare account id' as '{AWS:
account-id}'
