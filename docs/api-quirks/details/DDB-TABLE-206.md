<!-- generated: ack-api-quirks dynamodb 2026-10-09 model=2012-08-10 controller=34b85e6 -->
# DDB-TABLE-206: Policy writes serialized per resource: ResourceInUseException while applying (~2s), then ThrottlingException until 15s after the change
_Full entry and notes of one finding; its summary entry is in
[table-policy-kinesis-autoscaling.md](../table-policy-kinesis-autoscaling.md). Generated from ack-api-quirks
`services/dynamodb` (model 2012-08-10, controller commit 34b85e6); edit the finding in the lab, not here._

## Finding

- <a id="ddb-table-206"></a>**DDB-TABLE-206** `quota-limit` · impact high · unhandled (not handled in controller) · verified 2026-10-09
  **Policy writes serialized per resource: ResourceInUseException while applying (~2s), then ThrottlingException until 15s after the change**
  After a Put changed the policy (RevisionId R = epoch ms of the change; R - caller wall clock at the call =
  69 ms), a Put of a DIFFERENT document 2.4s later -> ThrottlingException (HTTP 400) 'Resource-based policy
  for table ackq-71f899-rp modified within the previous 15000 milliseconds. Please try again after
  2026-10-09T00:29:34.602Z.'; a Delete 2.4s later -> ThrottlingException (HTTP 400) 'Resource-based policy for
  table ackq-71f899-rp modified within the previous 15000 milliseconds. Please try again after
  2026-10-09T00:29:34.602Z.'; the message's 'try again after' timestamp is exactly R+15000 ms. Retrying every
  0.5s, the first success came 15.30s after R (attempt at 14.75s still failed). Equivalent-document re-Puts
  inside the window succeed as no-ops (same RevisionId). With ExpectedRevisionId=current inside the window the
  code is instead ResourceInUseException ('Attempt to change a resource which is still in use: Table is
  pending previous resource-based policy update: ac'); with a stale ExpectedRevisionId ->
  PolicyNotFoundException (revision check first). The window also applies after a Delete (Put 0.07s after
  Delete -> ThrottlingException (HTTP 400) 'Resource-based policy for table ackq-71f899-rp modified within the
  previous 15000 milliseconds. Please try again after 2026-10-09T00:30:37.353Z.') and independently per slot:
  the stream ARN's slot gave ThrottlingException 'Resource-based policy for stream <label> modified within the
  previous 15000 milliseconds' with first success after 15.56s, while a table-slot write 1.6s after the stream
  write succeeded.
  - ACK: requeue, terminal_codes, one-per-reconcile, e2e-timing · ops: PutResourcePolicy, DeleteResourcePolicy
    · fields: ResourcePolicy, RevisionId, ExpectedRevisionId
  - repro: Put(p1); Put(p2) immediately -> ThrottlingException 'modified within the previous 15000
    milliseconds. Please try again after <ts>'; Put(p2, ExpectedRevisionId=rev1) -> ResourceInUseException;
    retry until success
  - measurements: cooldown_first_success_s=15.3, try_again_minus_revision_ms=15000,
    stream_slot_first_success_s=15.56
  - handling: not handled in the controller (as of commit 34b85e6)
  - related: [DDB-TABLE-247](../table-policy-kinesis-autoscaling.md#ddb-table-247), [DDB-TABLE-347](../table-policy-kinesis-autoscaling.md#ddb-table-347), [DDB-TABLE-464](../service.md#ddb-table-464), [DDB-TABLE-445](../service.md#ddb-table-445), [DDB-TABLE-348](../table-policy-kinesis-autoscaling.md#ddb-table-348), [DDB-TABLE-213](../table-streams-encryption-class.md#ddb-table-213),
    [DDB-TABLE-205](../table-policy-kinesis-autoscaling.md#ddb-table-205) · hypotheses: H-S-027, H-S-006, H-S-112 · evidence: table/sub-resources/resource-policy

## Notes

Qualifies H-S-027. Overlapping writes are rejected in two phases. Phase 1 (~0-2s, while the previous write is
still applying and Get still returns the old state) -> ResourceInUseException 'Attempt to change a resource
which is still in use: Table|Stream is pending previous resource-based policy update' (seen 0.07-0.2s after
Put/Delete in table/consistency-windows/resource-policy-windows, and 1.7s after a stream Put). Phase 2 (until
RevisionId+15000ms) -> ThrottlingException 'modified within the previous 15000 milliseconds. Please try again
after <ts>' (seen at 2.4s and 13-14.75s). Both HTTP 400, both retry-after. With ExpectedRevisionId the
revision check runs first (PolicyNotFoundException on mismatch). Do not treat this ThrottlingException as a
generic rate limit - retrying before the timestamp is futile.
