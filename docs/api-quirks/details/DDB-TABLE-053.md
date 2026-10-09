<!-- generated: ack-api-quirks dynamodb 2026-10-09 model=2012-08-10 controller=34b85e6 -->
# DDB-TABLE-053: Account-level control-plane rate limit surfaces as ThrottlingException (HTTP 400) on UpdateTable at ~1 call/s across concurrent clients
_Full entry and notes of one finding; its summary entry is in [service.md](../service.md). Generated from
ack-api-quirks `services/dynamodb` (model 2012-08-10, controller commit 34b85e6); edit the finding in the lab,
not here._

## Finding

- <a id="ddb-table-053"></a>**DDB-TABLE-053** `quota-limit` · impact medium · unhandled (not handled in controller) · verified 2026-10-08
  **Account-level control-plane rate limit surfaces as ThrottlingException (HTTP 400) on UpdateTable at ~1 call/s across concurrent clients**
  During the OnDemandThroughput sequence two UpdateTable calls failed with ThrottlingException (HTTP 400):
  'The rate of control plane requests made by this account is too high' while this probe issued ~1
  UpdateTable/s and other probes ran in the same account. The same code is used for the per-table
  DeletionProtection cooldown, so ThrottlingException on UpdateTable has two distinct causes.
  - ACK: requeue, terminal_codes · ops: UpdateTable
  - repro: Issue UpdateTable calls at ~1/s while other control-plane traffic is active in the account
  - handling: not handled in the controller (as of commit 34b85e6)
  - related: [DDB-TABLE-003](../table-streams-encryption-class.md#ddb-table-003), [DDB-TABLE-176](../table-streams-encryption-class.md#ddb-table-176), [DDB-TABLE-017](../table-streams-encryption-class.md#ddb-table-017), [DDB-TABLE-438](../table-streams-encryption-class.md#ddb-table-438), [DDB-TABLE-130](../service.md#ddb-table-130), [DDB-TABLE-132](../service.md#ddb-table-132),
    [DDB-TABLE-441](../table.md#ddb-table-441), [DDB-TABLE-131](../table.md#ddb-table-131), [DDB-TABLE-051](../table-throughput-billing.md#ddb-table-051), [DDB-TABLE-178](../table-throughput-billing.md#ddb-table-178), [DDB-TABLE-182](../table-throughput-billing.md#ddb-table-182) · evidence:
    table/mutation-matrix/stream-protection-throughput

## Notes

The two throttled calls (odt-partial-read-2000, odt-both-minus1-again) were re-run successfully in
table/response-fidelity/create-update-response.

Contradiction with [DDB-TABLE-051](../table-throughput-billing.md#ddb-table-051), [DDB-TABLE-178](../table-throughput-billing.md#ddb-table-178): 051: {MaxReadRequestUnits:2000} alone -> ThrottlingException
and {-1,-1} again -> ThrottlingException; 178 re-ran both -> 200 (single-member merge to {2000,500}; {-1,-1}
echoed). 053 explains: those were account control-plane rate hits from concurrent probes, not
OnDemandThroughput semantics Resolution: keep both; 178 is canonical for merge semantics, 051 retains the 0 /
-2 -> ValidationException boundaries
