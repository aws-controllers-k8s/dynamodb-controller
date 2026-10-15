<!-- generated: ack-api-quirks dynamodb 2026-10-09 model=2012-08-10 controller=34b85e6 -->
# Table billing mode and throughput (provisioned, on-demand, warm)
_Billing-mode switches, provisioned/on-demand/warm throughput rules, decrease budgets, key schema immutability._
Generated from ack-api-quirks `services/dynamodb` (render date in the marker above); model 2012-08-10 (service/dynamodb v1.39.8); controller commit 34b85e6; evidence: `services/dynamodb/probes/<probe id>/` in the lab repo.

## Overview

<!-- preserved:start id=overview -->
This document covers BillingMode with ProvisionedThroughput and OnDemandThroughput, WarmThroughput, the provisioned-decrease budget and the create-time key schema (KeySchema/AttributeDefinitions) - the capacity-shaped fields of CreateTable/UpdateTable/DescribeTable. The most surprising facts are that a billing switch holds TableStatus=UPDATING for 60-170 s with no 24 h quota, that WarmThroughput is always present, never decreases and takes 4.5-8.5 min to raise while TableStatus stays ACTIVE, and that reversing an in-flight PAY_PER_REQUEST -> PROVISIONED switch returns 200 and is silently dropped ([DDB-TABLE-183](#ddb-table-183), [DDB-TABLE-179](#ddb-table-179), [DDB-TABLE-452](#ddb-table-452)).

### Rules a reconciler must respect
- Billing switch: PAY_PER_REQUEST -> PROVISIONED needs ProvisionedThroughput in the same call (nothing is estimated or defaulted, the values are applied verbatim); PROVISIONED -> PAY_PER_REQUEST must not carry ProvisionedThroughput but may carry OnDemandThroughput; same-day reversals are accepted and no once-per-24 h rejection was ever observed ([DDB-TABLE-056](#ddb-table-056), [DDB-TABLE-412](#ddb-table-412), [DDB-TABLE-183](#ddb-table-183)).
- Controller coverage of the switch is thin: the hooks catalog records that e2e exercises only PAY_PER_REQUEST -> PROVISIONED, the reverse being commented out for quota reasons ([GT-DDB-021](service.md#gt-ddb-021) (controller hooks catalog entry)), and that billing/on-demand changes are covered by 600 s e2e waits plus a 10 s requeue until ACTIVE ([GT-DDB-022](service.md#gt-ddb-022) (controller hooks catalog entry)); the dropped reversal of an in-flight switch (200, no effect) has no handling at all ([DDB-TABLE-056](#ddb-table-056), [DDB-TABLE-183](#ddb-table-183), [DDB-TABLE-452](#ddb-table-452)).
- Re-sends are not uniform: identical ProvisionedThroughput (alone or with BillingMode=PROVISIONED) is ValidationException 'will not change', BillingMode=PROVISIONED alone is ValidationException 'ProvisionedThroughput must be specified', while BillingMode=PAY_PER_REQUEST re-sent is a 200 whose response says UPDATING although nothing happens - diff before sending and never persist the UpdateTable response ([DDB-TABLE-057](#ddb-table-057), [DDB-TABLE-060](#ddb-table-060)).
- Reversal and overlap: during a PPR -> PROVISIONED switch a BillingMode=PAY_PER_REQUEST call is 200 yet swallowed (table ends PROVISIONED); during PROVISIONED -> PPR the reverse call (it carries PT) is ResourceInUseException 'Table IOPS are currently being updated'; even the ~1.3 s PT window rejects a second PT change, a billing switch and DeleteTable, and DeleteTable is ResourceInUseException for the whole throughput/billing UPDATING window - wait for ACTIVE between calls and re-Describe afterwards ([DDB-TABLE-452](#ddb-table-452), [DDB-TABLE-370](#ddb-table-370), [DDB-TABLE-063](#ddb-table-063)).
- Decrease budget: 4 decreases per table per UTC day (both dimensions in one call count once), then exactly one more 3600 s after the LAST accepted decrease (not at the top of the hour); the 5th is LimitExceededException naming the next allowed time, rejected attempts do not move NumberOfDecreasesToday/LastDecreaseDateTime, the counter resets at 00:00 UTC rather than 24 h after the first decrease, and the UpdateTable response reports the OLD counter - requeue at LastDecreaseDateTime+3600 s or parse 'Next decrease can be made at' ([DDB-TABLE-184](#ddb-table-184), [DDB-TABLE-326](#ddb-table-326), [DDB-TABLE-185](#ddb-table-185)).
- The hooks catalog records terminal_codes = InvalidParameter/ValidationException only (generator.yaml:88-90; [GT-DDB-073](service.md#gt-ddb-073) (controller hooks catalog entry)), so LimitExceededException is requeued on the generic cadence instead of at LastDecreaseDateTime+3600 s, and a stale full PT struct retried after ResourceInUseException can burn a decrease: ProvisionedThroughput must carry both RCU and WCU, so a writer that loses a race and retries its stale struct overwrites the other writer's dimension and, if lower, spends a decrease - re-read DescribeTable before every retry ([DDB-TABLE-381](#ddb-table-381), [DDB-TABLE-184](#ddb-table-184)).
- WarmThroughput is always present (12000/4000 default for PAY_PER_REQUEST; tracks the highest RCU/WCU ever provisioned on PROVISIONED; 12000/4000 after a switch to PPR and kept after switching back), never decreases (ValidationException 'decreasing WarmThroughput is not supported'), accepts equal values and single-member structs (the 'back to 12000/4000 -> OK' chain of [DDB-TABLE-049](#ddb-table-049) ran while a 12001/4001 increase was still in flight, so 12000/4000 was equal to the effective value, not a decrease), and an increase is async with TableStatus ACTIVE - only WarmThroughput.Status=UPDATING signals it and the job is last-writer-wins; a nil spec diffs against 12000/4000 and a lower spec is unsatisfiable, so treat nil as ignore and never send a lower value ([DDB-TABLE-032](#ddb-table-032), [DDB-TABLE-061](#ddb-table-061), [DDB-TABLE-179](#ddb-table-179), [DDB-TABLE-459](#ddb-table-459)).
- OnDemandThroughput merges member by member; -1 clears a member (echoed as -1 in the response and for ~1 s in DescribeTable, then omitted; the struct is absent once both are cleared); 0 and -2 are ValidationException; {} alone is HTTP 500 InternalFailure (deterministic, never escaped by a 5xx retry loop); it is absent unless set and only valid on PAY_PER_REQUEST - map nil <-> -1 and treat absent as unlimited ([DDB-TABLE-178](#ddb-table-178), [DDB-TABLE-182](#ddb-table-182), [DDB-TABLE-051](#ddb-table-051), [DDB-TABLE-028](#ddb-table-028)).
- CreateTable shape: no BillingMode means PROVISIONED and both RCU/WCU are required; PAY_PER_REQUEST + PT and PROVISIONED + ODT are ValidationException; 0/-1 fail the >= 1 constraint; WarmThroughput below the on-demand initial throughput (PPR) or below RCU/WCU (PROVISIONED) is rejected; partial and {} ODT/Warm structs are accepted at create although {} is a 500 on UpdateTable ([DDB-TABLE-024](#ddb-table-024), [DDB-TABLE-038](#ddb-table-038), [DDB-TABLE-039](#ddb-table-039)).
- Echo shape: BillingModeSummary is absent on tables created PROVISIONED (present with LastUpdateToPayPerRequestDateTime once the table was ever PPR); PPR reports ProvisionedThroughput 0/0 with NumberOfDecreasesToday; the CreateTable response lacks WarmThroughput (unless sent) and LastUpdateToPayPerRequestDateTime; PT, billing and Warm responses echo the OLD values with UPDATING ([DDB-TABLE-028](#ddb-table-028), [DDB-TABLE-033](#ddb-table-033), [DDB-TABLE-060](#ddb-table-060), [DDB-TABLE-064](#ddb-table-064), [DDB-TABLE-055](#ddb-table-055)).
- Key schema is create-only and byte-exact: KeySchema is order-sensitive (HASH first, max 2 elements), AttributeDefinitions must equal the exact set of key attributes (duplicates, unused and case-mismatched entries rejected; order is free and echoed as sent, so compare order-insensitively), attribute names are not trimmed or normalized (' pk' and 'pk' are two attributes), and a duplicate CreateTable is ResourceInUseException in every state with no TableAlreadyExists code ([DDB-TABLE-042](#ddb-table-042), [DDB-TABLE-364](#ddb-table-364); [DDB-TABLE-005](table.md#ddb-table-005), [DDB-TABLE-010](table.md#ddb-table-010), table.md).

### Timing you should expect
- Billing switch PAY_PER_REQUEST -> PROVISIONED: UPDATING 60.5-98.2 s (n=5); PROVISIONED -> PAY_PER_REQUEST 128.6-171.6 s on tables created PROVISIONED (n=4) but 4.1-5.1 s on tables that were PAY_PER_REQUEST before (n=4) ([DDB-TABLE-056](#ddb-table-056), [DDB-TABLE-062](#ddb-table-062), [DDB-TABLE-064](#ddb-table-064), [DDB-TABLE-183](#ddb-table-183), [DDB-TABLE-067](#ddb-table-067), [DDB-TABLE-452](#ddb-table-452)).
- ProvisionedThroughput change: UPDATING 1.0-2.0 s (increase ~2 s, decrease ~1 s; n>=8); the 5th-decrease rejection answers in ~18 ms and the refill lands at +3600.1 s ([DDB-TABLE-060](#ddb-table-060), [DDB-TABLE-370](#ddb-table-370), [DDB-TABLE-058](#ddb-table-058), [DDB-TABLE-184](#ddb-table-184), [DDB-TABLE-326](#ddb-table-326)).
- WarmThroughput first increase: 271.6-500.9 s (n=3) with TableStatus ACTIVE throughout; a re-raise or single-member write ~2 s ([DDB-TABLE-179](#ddb-table-179), [DDB-TABLE-066](#ddb-table-066), [DDB-TABLE-121](#ddb-table-121)).
- OnDemandThroughput writes are synchronous (TableStatus stays ACTIVE), the -1 sentinel is visible ~1 s; CreateTable of every shape reached ACTIVE within ~9.5 s ([DDB-TABLE-178](#ddb-table-178), [DDB-TABLE-182](#ddb-table-182), [DDB-TABLE-033](#ddb-table-033)).

### Known handling gaps in the controller
- No finding rendered in this document is stored as suspect-bug or partial; the controller consequences above (thin e2e coverage of the switch, LimitExceededException not scheduled on LastDecreaseDateTime, WarmThroughput/OnDemandThroughput sentinel mapping, the deterministic HTTP 500) are derived from entries stored as handled or unhandled.

### Where to look next
- The UpdateTable single-concern matrix ([DDB-TABLE-433](table-streams-encryption-class.md#ddb-table-433), [DDB-TABLE-382](table-streams-encryption-class.md#ddb-table-382), table-streams-encryption-class.md) and what is admitted during a billing-switch UPDATING window ([DDB-TABLE-369](table-streams-encryption-class.md#ddb-table-369), table-streams-encryption-class.md - rendered there because its title names streams); a billing switch needs a throughput Update per GSI, separate per-GSI decrease budgets, KeySchema/LSIs have no UpdateTable member ([DDB-TABLE-152](table-indexes.md#ddb-table-152), [DDB-TABLE-157](table-indexes.md#ddb-table-157), [DDB-TABLE-357](table-indexes.md#ddb-table-357), table-indexes.md).
- HTTP 500 catalogue and the silently dropped {} members ([DDB-TABLE-448](service.md#ddb-table-448), [DDB-TABLE-456](service.md#ddb-table-456), service.md). Evidence: services/dynamodb/probes/table/{mutation-matrix,round-trip,weird-inputs,limits,state-machine,creative}/.

Entries below are generated from the lab findings; low-impact items are in the appendix, long notes under details/.
<!-- preserved:end -->

## At a glance

- canonical findings: 53 (high 23 / medium 17 / low 13); duplicates folded into the appendix: 5
- handling: handled 18 · partial 0 · tracked 0 · unhandled 34 · suspect-bug 0 · n-a 1 (tracked = handled/partial whose reference is an open GitHub issue; counted as not handled)
- re-verified: 1 · last_verified: 2026-10-08..2026-10-09 · model: 2012-08-10 (service/dynamodb v1.39.8)
- categories: other 9, request-validation 9, async-state-machine 8, quota-limit 8, response-fidelity 4,
  idempotency 3, server-default 3, update-granularity 3, stale-response 2, error-code 1, normalization 1,
  requested-vs-effective 1, shape-mismatch 1

## Operations

| operation | kind | required inputs | declared error shapes | paginated |
| --- | --- | --- | --- | --- |
| CreateTable | create | TableName | ResourceInUseException, LimitExceededException, InternalServerError | no |
| DeleteTable | delete | TableName | ResourceInUseException, ResourceNotFoundException, LimitExceededException, InternalServerError | no |
| DescribeLimits | read | - | InternalServerError | no |
| DescribeTable | read | TableName | ResourceNotFoundException, InternalServerError | no |
| UpdateTable | update | TableName | ResourceInUseException, ResourceNotFoundException, LimitExceededException, InternalServerError | no |

## State machine

- **IndexStatus**: CREATING, UPDATING, DELETING, ACTIVE (transitional: CREATING, UPDATING, DELETING)
- **ReplicaStatus**: CREATING, CREATION_FAILED, UPDATING, DELETING, ACTIVE, REGION_DISABLED,
  INACCESSIBLE_ENCRYPTION_CREDENTIALS, ARCHIVING, ARCHIVED, REPLICATION_NOT_AUTHORIZED (transitional:
  CREATING, UPDATING, DELETING, ARCHIVING)
- **SSEStatus**: ENABLING, ENABLED, DISABLING, DISABLED, UPDATING (transitional: ENABLING, DISABLING,
  UPDATING)
- **TableStatus**: CREATING, UPDATING, DELETING, ACTIVE, INACCESSIBLE_ENCRYPTION_CREDENTIALS, ARCHIVING,
  ARCHIVED, REPLICATION_NOT_AUTHORIZED (transitional: CREATING, UPDATING, DELETING, ARCHIVING)
- **WitnessStatus**: CREATING, DELETING, ACTIVE (transitional: CREATING, DELETING)

- <a id="ddb-table-058"></a>**DDB-TABLE-058** `async-state-machine` · impact medium · unhandled (not handled in controller) · verified 2026-10-08
  **Provisioned capacity up/down: UPDATING durations, NumberOfDecreasesToday/LastDecreaseDateTime bookkeeping, partial and zero values**
  PT 1/1 -> 2/2: OK (response TableStatus=UPDATING, UPDATING 2.02s); Describe {"TableStatus": "ACTIVE",
  "BillingModeSummary": "<absent>", "ProvisionedThroughput": {"LastIncreaseDateTime": "2026-10-08
  23:09:25.303000+00:00", "NumberOfDecreasesToday": 0, "ReadCapacityUnits": 2, "WriteCapacityUnits": 2},
  "OnDemandThroughput": "<absent>", "WarmThroughput": {"ReadUnitsPerSecond": 2, "WriteUnitsPerSecond": 2,
  "Status": "ACTIVE"}, "TableClassSummary": "<absent>"}. 2/2 -> 3/3 (with BillingMode): OK (response
  TableStatus=UPDATING, UPDATING 2.02s). {ReadCapacityUnits:4} only -> ValidationException: '1 validation
  error detected: Value null at 'provisionedThroughput.writeCapacityUnits' failed to satisfy constraint:
  Member must not be null'. 3/3 -> 1/1: OK (response TableStatus=UPDATING, UPDATING 1.01s); Describe
  {"TableStatus": "ACTIVE", "BillingModeSummary": "<absent>", "ProvisionedThroughput":
  {"LastIncreaseDateTime": "2026-10-08 23:09:27.894000+00:00", "LastDecreaseDateTime": "2026-10-08
  23:09:29.159000+00:00", "NumberOfDecreasesToday": 1, "ReadCapacityUnits": 1, "WriteCapacityUnits": 1},
  "OnDemandThroughput": "<absent>", "WarmThroughput": {"ReadUnitsPerSecond": 3, "WriteUnitsPerSecond": 3,
  "Status": "ACTIVE"}, "TableClassSummary": "<absent>"}. 1/1 -> read 2 / write 1 (mixed): OK (response
  TableStatus=UPDATING, UPDATING 1.01s); Describe {"TableStatus": "ACTIVE", "BillingModeSummary": "<absent>",
  "ProvisionedThroughput": {"LastIncreaseDateTime": "2026-10-08 23:09:30.195000+00:00",
  "LastDecreaseDateTime": "2026-10-08 23:09:29.159000+00:00", "NumberOfDecreasesToday": 1,
  "ReadCapacityUnits": 2, "WriteCapacityUnits": 1}, "OnDemandThroughput": "<absent>", "WarmThroughput":
  {"ReadUnitsPerSecond": 3, "WriteUnitsPerSecond": 3, "Status": "ACTIVE"}, "TableClassSummary": "<absent>"}.
  0/0 -> ValidationException: '2 validation errors detected: Value '0' at
  'provisionedThroughput.writeCapacityUnits' failed to satisfy constraint: Member must have value greater than
  or equal to 1; Value '0' at 'provisionedThroughput.readCapacityUnits'. C: 1/1 -> 5/5: OK (response
  TableStatus=UPDATING, UPDATING 1.01s); 5/5 -> 2/2: OK (response TableStatus=UPDATING, UPDATING 2.02s);
  Describe {"TableStatus": "ACTIVE", "BillingModeSummary": {"BillingMode": "PROVISIONED",
  "LastUpdateToPayPerRequestDateTime": "2026-10-08 23:09:17.982000+00:00"}, "ProvisionedThroughput":
  {"LastIncreaseDateTime": "2026-10-08 23:14:52.322000+00:00", "LastDecreaseDateTime": "2026-10-08
  23:14:53.990000+00:00", "NumberOfDecreasesToday": 1, "ReadCapacityUnits": 2, "WriteCapacityUnits": 2},
  "OnDemandThroughput": "<absent>", "WarmThroughput": {"ReadUnitsPerSecond": 12000, "WriteUnitsPerSecond":
  4000, "Status": "ACTIVE"}, "TableClassSummary": "<absent>"}.
  - ACK: requeue, e2e-timing, compare.is_ignored+delta_pre_compare · ops: UpdateTable, DescribeTable · fields:
    ProvisionedThroughput.ReadCapacityUnits, ProvisionedThroughput.WriteCapacityUnits,
    ProvisionedThroughput.NumberOfDecreasesToday
  - repro: PROVISIONED table; UpdateTable ProvisionedThroughput up, same, down, mixed; DescribeTable after
    each
  - measurements: d-pt-up-2-2=2.02, d-billing-prov-plus-pt-up-3-3=2.02, d-pt-down-1-1=1.01,
    d-pt-mixed-read-up-write-down=1.01, c-pt-up-5-5=1.01, c-pt-down-2-2=2.02
  - handling: not handled in the controller (as of commit 34b85e6)
  - related: [DDB-TABLE-025](#ddb-table-025), [DDB-TABLE-055](#ddb-table-055), [DDB-TABLE-183](#ddb-table-183), [DDB-TABLE-033](#ddb-table-033), [DDB-TABLE-060](#ddb-table-060), [DDB-TABLE-064](#ddb-table-064),
    [DDB-TABLE-370](#ddb-table-370), [DDB-TABLE-057](#ddb-table-057), [DDB-TABLE-061](#ddb-table-061), [DDB-TABLE-063](#ddb-table-063), [DDB-TABLE-164](table-indexes.md#ddb-table-164) · evidence:
    table/mutation-matrix/billing-capacity

- <a id="ddb-table-063"></a>**DDB-TABLE-063** `async-state-machine` · impact high · unhandled (not handled in controller) · verified 2026-10-08
  **DeleteTable while UPDATING (throughput change / billing switch) -> ResourceInUseException; UPDATING ~2s / ~129s**
  UpdateTable(ProvisionedThroughput 1/1->2/2) response TableStatus=UPDATING; DeleteTable right after ->
  ResourceInUseException 'Attempt to change a resource which is still in use: Table: ackq-8f0165-upd is in the
  process of being updated.'; UpdateTable(DeletionProtectionEnabled same value) -> OK(UPDATING). UPDATING
  lasted 2.03 s and the new RCU appeared in DescribeTable timeline: [(('UPDATING', 1), 0.01), (('ACTIVE', 2),
  2.04)]. Re-sending the same throughput -> ValidationException 'The provisioned throughput for the table will
  not change. The requested value equals the current value. Current ReadCapa'.
  - ACK: deletable.when, requeue, updateable.when · ops: UpdateTable, DeleteTable · fields:
    ProvisionedThroughput
  - repro: PROVISIONED table; UpdateTable(RCU 1->2); immediately DeleteTable
  - measurements: throughput_updating_s=2.03, creating_duration_s_provisioned=4.05
  - handling: not handled in the controller (as of commit 34b85e6)
  - related: [DDB-TABLE-006](table.md#ddb-table-006), [DDB-TABLE-103](service.md#ddb-table-103), [DDB-TABLE-381](#ddb-table-381), [DDB-TABLE-005](table.md#ddb-table-005), [DDB-TABLE-101](service.md#ddb-table-101), [DDB-TABLE-104](table.md#ddb-table-104),
    [DDB-TABLE-184](#ddb-table-184), [DDB-TABLE-058](#ddb-table-058), [DDB-TABLE-060](#ddb-table-060), [DDB-TABLE-370](#ddb-table-370), [DDB-TABLE-057](#ddb-table-057), [DDB-TABLE-061](#ddb-table-061), [DDB-TABLE-164](table-indexes.md#ddb-table-164) ·
    evidence: table/state-machine/billing-sse-warm-throughput

- <a id="ddb-table-064"></a>**DDB-TABLE-064** `async-state-machine` · impact high · handled · verified 2026-10-08
  **PROVISIONED->PAY_PER_REQUEST keeps the table UPDATING ~129s while BillingModeSummary already says PAY_PER_REQUEST**
  UpdateTable(BillingMode=PAY_PER_REQUEST) response: TableStatus=UPDATING, BillingModeSummary={'BillingMode':
  'PAY_PER_REQUEST'}, ProvisionedThroughput={'LastIncreaseDateTime': datetime.datetime(2026, 10, 8, 23, 8, 48,
  363000, tzinfo=tzlocal()), 'LastDecreaseDateTime': datetime.datetime(2026, 10, 8, 23, 8, 49, 404000,
  tzinfo=tzlocal()), 'NumberOfDecreasesToday': 0, 'ReadCapacityUnits': 0, 'WriteCapacityUnits': 0}.
  DescribeTable timeline of (status, billing, rcu, warm): [(0.01, {'status': 'UPDATING', 'billing':
  'PAY_PER_REQUEST', 'rcu': 0, 'warm': (2, 'ACTIVE')}), (128.79, {'status': 'ACTIVE', 'billing':
  'PAY_PER_REQUEST', 'rcu': 0, 'warm': (12000, 'ACTIVE')})]. After ACTIVE: {'BillingModeSummary':
  {'BillingMode': 'PAY_PER_REQUEST', 'LastUpdateToPayPerRequestDateTime': datetime.datetime(2026, 10, 8, 23,
  10, 58, 74000, tzinfo=tzlocal())}, 'ProvisionedThroughput': {'LastIncreaseDateTime': datetime.datetime(2026,
  10, 8, 23, 8, 48, 363000, tzinfo=tzlocal()), 'NumberOfDecreasesToday': 0, 'ReadCapacityUnits': 0,
  'WriteCapacityUnits': 0}, 'WarmThroughput': {'ReadUnitsPerSecond': 12000, 'WriteUnitsPerSecond': 4000,
  'Status': 'ACTIVE'}, 'OnDemandThroughput': None}. Immediate switch back to PROVISIONED -> OK ''. During the
  switch: DeleteTable -> ResourceInUseException, UpdateTable(DP) -> OK(UPDATING).
  - ACK: synced.when, requeue, terminal_codes · ops: UpdateTable, DescribeTable · fields: BillingMode,
    BillingModeSummary, ProvisionedThroughput
  - repro: PROVISIONED table -> UpdateTable(BillingMode=PAY_PER_REQUEST); poll DescribeTable 1/s; then
    UpdateTable(BillingMode=PROVISIONED)
  - measurements: billing_updating_s=128.78
  - handling: handled via `pkg/resource/table/hooks.go:306; pkg/resource/table/hooks.go:325-335; test/e2e/tests/test_table.py:37-42; test/e2e/tests/test_table.py:544-556`
  - related: [DDB-TABLE-056](#ddb-table-056), [DDB-TABLE-062](#ddb-table-062), [DDB-TABLE-067](#ddb-table-067), [DDB-TABLE-183](#ddb-table-183), [DDB-TABLE-369](table-streams-encryption-class.md#ddb-table-369), [DDB-TABLE-452](#ddb-table-452),
    [DDB-TABLE-055](#ddb-table-055), [DDB-TABLE-433](table-streams-encryption-class.md#ddb-table-433), [DDB-TABLE-156](#ddb-table-156), [DDB-TABLE-057](#ddb-table-057), [DDB-TABLE-025](#ddb-table-025), [DDB-TABLE-058](#ddb-table-058), [DDB-TABLE-033](#ddb-table-033),
    [DDB-TABLE-060](#ddb-table-060), [DDB-TABLE-370](#ddb-table-370), [DDB-TABLE-284](table-streams-encryption-class.md#ddb-table-284), [DDB-TABLE-179](#ddb-table-179), [DDB-TABLE-066](#ddb-table-066), [DDB-TABLE-371](table-streams-encryption-class.md#ddb-table-371), [DDB-TABLE-140](table-streams-encryption-class.md#ddb-table-140),
    [DDB-TABLE-180](table-streams-encryption-class.md#ddb-table-180), [DDB-TABLE-177](table-streams-encryption-class.md#ddb-table-177) · evidence: table/state-machine/billing-sse-warm-throughput
  - notes: H-T-012: compare billing_updating_s with throughput_updating_s=2.03. BillingModeSummary reflects
    the target mode as soon as the response, before ACTIVE. H-T-012 CONFIRMED (128.8s vs 2.0s for a throughput
    change; response and every poll already reported PAY_PER_REQUEST). SURPRISE: immediate switch...
  - full notes: [details/DDB-TABLE-064.md](details/DDB-TABLE-064.md)

- <a id="ddb-table-121"></a>**DDB-TABLE-121** `async-state-machine` · impact medium · unhandled (not handled in controller) · verified 2026-10-08
  **WarmThroughput increase duration is not predictable: 272s and 409s for +1000/+1000, but ~2s when values were already raised once**
  On three tables a WarmThroughput increase from the 12000/4000 default to 13000/5000 kept
  WarmThroughput.Status=UPDATING for 271.6s and 409.0s (TableStatus ACTIVE throughout). A further increase
  13000/5000 -> 14000/6000 on the same table returned Status=UPDATING with the old numbers but DescribeTable
  2s later showed 14000/6000 Status=ACTIVE, and DeleteTable was then admitted (TableStatus->DELETING). After
  the table was deleted its backups stayed AVAILABLE and GetResourcePolicy -> ResourceNotFoundException.
  - ACK: synced.when, requeue, e2e-timing · ops: UpdateTable, DescribeTable, DeleteTable · fields:
    WarmThroughput, WarmThroughput.Status
  - repro: PPR table; UpdateTable(WarmThroughput 13000/5000); poll until Status=ACTIVE;
    UpdateTable(WarmThroughput 14000/6000); DescribeTable 2s later
  - handling: not handled in the controller (as of commit 34b85e6)
  - related: [DDB-TABLE-179](#ddb-table-179), [DDB-TABLE-066](#ddb-table-066), [DDB-TABLE-459](#ddb-table-459), [DDB-TABLE-049](#ddb-table-049), [DDB-TABLE-061](#ddb-table-061), [DDB-TABLE-059](#ddb-table-059),
    [DDB-TABLE-069](table-streams-encryption-class.md#ddb-table-069), [DDB-TABLE-117](table-streams-encryption-class.md#ddb-table-117), [DDB-TABLE-119](table-streams-encryption-class.md#ddb-table-119), [DDB-TABLE-234](table-policy-kinesis-autoscaling.md#ddb-table-234), [DDB-TABLE-287](table-streams-encryption-class.md#ddb-table-287), [DDB-TABLE-450](table-streams-encryption-class.md#ddb-table-450), [DDB-TABLE-460](table-policy-kinesis-autoscaling.md#ddb-table-460),
    [DDB-TABLE-435](table-streams-encryption-class.md#ddb-table-435) · evidence: table/state-machine/field-admissibility-while-updating
  - notes: Polling with a fixed backoff tuned to minutes would waste a reconcile on the fast case; use the
    Status field, not elapsed time. Phase C of this probe therefore ran against a DELETING table (recorded as
    such in result.yaml; CreateBackup and PutResourcePolicy were still accepted during DELETING).

- <a id="ddb-table-179"></a>**DDB-TABLE-179** `async-state-machine` · impact high · unhandled (not handled in controller) · verified 2026-10-08
  **WarmThroughput increase is async (~6.5 min) with TableStatus ACTIVE: only WarmThroughput.Status=UPDATING signals it; decrease rejected**
  Before: {"ReadUnitsPerSecond": 12000, "WriteUnitsPerSecond": 4000, "Status": "ACTIVE"}. UpdateTable
  WarmThroughput +1/+1 -> OK response={"TableStatus": "ACTIVE", "WarmThroughput": {"ReadUnitsPerSecond":
  12000, "WriteUnitsPerSecond": 4000, "Status": "UPDATING"}}; describe transitions={"timed_out": false,
  "transitions": [{"at_s": 0.01, "value": {"TableStatus": "ACTIVE", "WarmThroughput": {"ReadUnitsPerSecond":
  12000, "WriteUnitsPerSecond": 4000, "Status": "UPDATING"}}}, {"at_s": 388.78, "value": {"TableStatus":
  "ACTIVE", "WarmThroughput": {"ReadUnitsPerSecond": 12001, "WriteUnitsPerSecond": 4001, "Status":
  "ACTIVE"}}}]}. Re-send same -> OK response={"TableStatus": "ACTIVE", "WarmThroughput":
  {"ReadUnitsPerSecond": 12001, "WriteUnitsPerSecond": 4001, "Status": "UPDATING"}}; describe
  transitions={"timed_out": true, "transitions": [{"at_s": 0.01, "value": {"TableStatus": "ACTIVE",
  "WarmThroughput": {"ReadUnitsPerSecond": 12001, "Write. Decrease back -> ValidationException (HTTP 400):
  'One or more parameter values were invalid: Requested ReadUnitsPerSecond for WarmThroughput for table is
  lower than current WarmThroughput, decreasing WarmThroughput is not supported'. {WriteUnitsPerSecond:+2}
  only -> OK response={"TableStatus": "ACTIVE", "WarmThroughput": {"ReadUnitsPerSecond": 12001,
  "WriteUnitsPerSecond": 4001, "Status": "UPDATING"}}; describe transitions={"timed_out": false,
  "transitions": [{"at_s": 0.01, "value": {"TableStatus": "ACTIVE", "WarmThroughput": {"ReadUnitsPerSecond":
  12001, "WriteUnitsPerSecond": 4001, "Status": "UPDATING"}}}, {"at_s": 2.02, "value": {"TableStatus":
  "ACTIVE", "WarmThroughput": {"ReadUnitsPerSecond": 12001, "WriteUnitsPerSecond": 4002, "Status":
  "ACTIVE"}}}]}.
  - ACK: synced.when, requeue, compare.is_ignored+delta_pre_compare, custom_update · ops: UpdateTable,
    DescribeTable · fields: WarmThroughput.ReadUnitsPerSecond, WarmThroughput.WriteUnitsPerSecond,
    WarmThroughput.Status
  - repro: PPR table; UpdateTable WarmThroughput 12001/4001; poll DescribeTable WarmThroughput.Status; then
    12000/4000
  - measurements: warm_increase_completion_s=388.8, warm_increase_completion_s_first_run=500.9,
    warm_partial_write_increase_completion_s=2.0
  - handling: not handled in the controller (as of commit 34b85e6)
  - related: [DDB-TABLE-066](#ddb-table-066), [DDB-TABLE-459](#ddb-table-459), [DDB-TABLE-049](#ddb-table-049), [DDB-TABLE-061](#ddb-table-061), [DDB-TABLE-059](#ddb-table-059), [DDB-TABLE-069](table-streams-encryption-class.md#ddb-table-069),
    [DDB-TABLE-121](#ddb-table-121), [DDB-TABLE-060](#ddb-table-060), [DDB-TABLE-370](#ddb-table-370), [DDB-TABLE-284](table-streams-encryption-class.md#ddb-table-284), [DDB-TABLE-371](table-streams-encryption-class.md#ddb-table-371), [DDB-TABLE-140](table-streams-encryption-class.md#ddb-table-140), [DDB-TABLE-064](#ddb-table-064),
    [DDB-TABLE-183](#ddb-table-183), [DDB-TABLE-180](table-streams-encryption-class.md#ddb-table-180), [DDB-TABLE-177](table-streams-encryption-class.md#ddb-table-177), [DDB-TABLE-156](#ddb-table-156) · evidence:
    table/response-fidelity/create-update-response
  - notes: A +1/+1 increase from 12000/4000 completed after 388.8s (and 500.9s in an earlier run);
    DescribeTable kept the OLD values with Status=UPDATING until completion, TableStatus never left ACTIVE.
    Re-sending the same values is accepted (200, Status=UPDATING briefly ~2s). Decrease ->
    ValidationException...
  - full notes: [details/DDB-TABLE-179.md](details/DDB-TABLE-179.md)

- <a id="ddb-table-370"></a>**DDB-TABLE-370** `async-state-machine` · impact high · handled · verified 2026-10-09
  **ProvisionedThroughput-only UPDATING lasts ~1.3s; a second PT change / billing switch / DeleteTable inside it -> ResourceInUseException**
  Trigger PT 2/2 (from 1/1) -> 200 OK (TableStatus=UPDATING). Immediately: pt_change_again ->
  ResourceInUseException (HTTP 400) 'Attempt to change a resource which is still in use: Table IOPS are
  currently being updated. Table: ackq-90be14-upd2' @+0.06s; billing_switch_ppr -> ResourceInUseException
  (HTTP 400) 'Attempt to change a resource which is still in use: Table IOPS are currently being updated.
  Table: ackq-90be14-upd2' @+0.08s; delete_table -> ResourceInUseException (HTTP 400) 'Attempt to change a
  resource which is still in use: Table: ackq-90be14-upd2 is in the process of being updated.' @+0.1s;
  dp_toggle -> 200 OK (TableStatus=ACTIVE) @+13.72s. Second trial: trigger PT 3/3 (response echoes the OLD
  throughput 2/2, TableStatus=UPDATING) then PT 4/4 at +0.05s -> ResourceInUseException (HTTP 400) 'Attempt to
  change a resource which is still in use: Table IOPS are currently being updated. Table: ackq-90be14-upd2';
  UPDATING timeline (0.25 s polling) [(['UPDATING', 2], 1.3), (['ACTIVE', 3], None)]; the same PT change
  re-sent right after ACTIVE -> 200 OK (TableStatus=UPDATING) (timeline [(['UPDATING', 3], 2.34), (['ACTIVE',
  4], None)]).
  - ACK: updateable.when, requeue, synced.when · ops: UpdateTable, DeleteTable · fields:
    ProvisionedThroughput, BillingMode
  - repro: PROVISIONED table: UpdateTable(PT n+1/n+1); immediately UpdateTable(PT n+2/n+2),
    UpdateTable(BillingMode=PAY_PER_REQUEST), DeleteTable; poll DescribeTable at 0.25 s
  - measurements: pt_updating_s_trial1=null, pt_updating_s_trial2=1.3
  - handling: handled via `pkg/resource/table/hooks.go:81-84; pkg/resource/table/hooks.go:198-202`
  - related: [DDB-TABLE-063](#ddb-table-063), [DDB-TABLE-164](table-indexes.md#ddb-table-164), [DDB-TABLE-060](#ddb-table-060), [DDB-TABLE-058](#ddb-table-058), [DDB-TABLE-057](#ddb-table-057), [DDB-TABLE-061](#ddb-table-061),
    [DDB-TABLE-001](table.md#ddb-table-001), [DDB-TABLE-002](table-streams-encryption-class.md#ddb-table-002), [DDB-TABLE-369](table-streams-encryption-class.md#ddb-table-369), [DDB-TABLE-052](table-streams-encryption-class.md#ddb-table-052), [DDB-TABLE-285](table-streams-encryption-class.md#ddb-table-285), [DDB-TABLE-120](table-streams-encryption-class.md#ddb-table-120), [DDB-TABLE-065](table-streams-encryption-class.md#ddb-table-065),
    [DDB-TABLE-459](#ddb-table-459), [DDB-TABLE-054](table-streams-encryption-class.md#ddb-table-054), [DDB-TABLE-010](table.md#ddb-table-010), [DDB-TABLE-069](table-streams-encryption-class.md#ddb-table-069), [DDB-TABLE-066](#ddb-table-066), [DDB-TABLE-284](table-streams-encryption-class.md#ddb-table-284), [DDB-TABLE-334](table-streams-encryption-class.md#ddb-table-334),
    [DDB-TABLE-179](#ddb-table-179), [DDB-TABLE-371](table-streams-encryption-class.md#ddb-table-371), [DDB-TABLE-140](table-streams-encryption-class.md#ddb-table-140), [DDB-TABLE-064](#ddb-table-064), [DDB-TABLE-183](#ddb-table-183), [DDB-TABLE-180](table-streams-encryption-class.md#ddb-table-180), [DDB-TABLE-177](table-streams-encryption-class.md#ddb-table-177),
    [DDB-TABLE-156](#ddb-table-156) · hypotheses: H-T-001, H-T-002 · evidence: table/state-machine/updating-second-mutation
  - notes: A controller that splits one desired state into several UpdateTable calls (billing, throughput,
    warm, on-demand) must wait for TableStatus=ACTIVE between them; even the ~2 s PT window is enough to get
    ResourceInUseException on the follow-up call.

- <a id="ddb-table-410"></a>**DDB-TABLE-410** `async-state-machine` · impact high · unhandled (not handled in controller) · verified 2026-10-09
  **Doc claim C042 PARTLY: UpdateTable is asynchronous and flips TableStatus ACTIVE->UPDATING while executing**
  Only some UpdateTable changes flip TableStatus to UPDATING (BillingMode, ProvisionedThroughput, stream
  toggle, TableClass, replica changes, the 25-55 s resource-allocation phase of a GSI add). Others complete
  with TableStatus=ACTIVE throughout: DeletionProtectionEnabled and table-level OnDemandThroughput are
  synchronous (response already ACTIVE, DescribeTable agrees); SSESpecification runs ~22 s signalled only by
  SSEDescription.Status=UPDATING; WarmThroughput runs ~6.5 min signalled only by
  WarmThroughput.Status=UPDATING; GSI throughput/OnDemandThroughput updates flip only IndexStatus; a GSI add
  returns the table to ACTIVE while the index backfills 7-16 min; and no-op re-sends (same
  BillingMode/TableClass) return TableStatus=UPDATING in the response while DescribeTable is ACTIVE at once.
  DeleteTable is nevertheless rejected during the SSE/Warm/GSI phases even though TableStatus says ACTIVE.
  - ACK: synced.when, requeue, docs-only · ops: UpdateTable, DescribeTable · fields: TableStatus,
    SSEDescription.Status, WarmThroughput.Status, GlobalSecondaryIndexes.IndexStatus
  - handling: not handled in the controller (as of commit 34b85e6)
  - related: [DDB-TABLE-017](table-streams-encryption-class.md#ddb-table-017), [DDB-TABLE-065](table-streams-encryption-class.md#ddb-table-065), [DDB-TABLE-179](#ddb-table-179), [DDB-TABLE-135](table-indexes.md#ddb-table-135), [DDB-TABLE-148](table-indexes.md#ddb-table-148), [DDB-TABLE-177](table-streams-encryption-class.md#ddb-table-177),
    [DDB-TABLE-069](table-streams-encryption-class.md#ddb-table-069) · hypotheses: C042 · evidence: table/error-taxonomy/missing-table-noop-update-dp,
    table/state-machine/billing-sse-warm-throughput, table/response-fidelity/create-update-response,
    table/round-trip/gsi-lsi-describe, table/state-machine/gsi-lifecycle, service/static/doc-claims-2
  - notes: VERDICT: PARTLY - TableStatus=UPDATING is neither necessary (DP/ODT synchronous; SSE, Warm, GSI
    phases keep ACTIVE) nor sufficient (no-op responses say UPDATING) as an 'UpdateTable in progress' signal

- <a id="ddb-table-459"></a>**DDB-TABLE-459** `async-state-machine` · impact medium · unhandled (not handled in controller) · verified 2026-10-09
  **WarmThroughput job is last-writer-wins: a higher request 1 s into a running Warm update is accepted and applied; no write resets its Status**
  PAY_PER_REQUEST tables: UpdateTable(WarmThroughput 13000/5000) at t0 (TableStatus stays ACTIVE,
  WarmThroughput.Status=UPDATING, values still 12000/4000); write at +0.1 s (SSESpecification KMS -> 200 /
  DeletionProtection -> 200 / OnDemandThroughput 100/100 -> 200 / TagResource -> 200);
  UpdateTable(WarmThroughput 14000/6000) at +1.0 s -> 200 in all 5 cells (response Status UPDATING, old
  values). WarmThroughput.Status stayed UPDATING continuously and flipped to ACTIVE once, after 441-514 s,
  with the values of the SECOND request (14000/6000) in every cell including the no-write control. No write
  reset the Status early; the overlapping SSE change finished (ENABLED) at +82 s inside the Warm window.
  - ACK: synced.when, requeue, custom_update · ops: UpdateTable, DescribeTable · fields: WarmThroughput,
    SSESpecification, DeletionProtectionEnabled, OnDemandThroughput
  - repro: UpdateTable(WarmThroughput 13000/5000); 1 s later UpdateTable(WarmThroughput 14000/6000) -> 200;
    DescribeTable until WarmThroughput.Status=ACTIVE (~8 min): 14000/6000
  - measurements: warm_updating_s=[441.46, 451.46, 473.59, 493.76, 513.88]
  - handling: not handled in the controller (as of commit 34b85e6)
  - related: [DDB-TABLE-066](#ddb-table-066), [DDB-TABLE-121](#ddb-table-121), [DDB-TABLE-179](#ddb-table-179), [DDB-TABLE-049](#ddb-table-049), [DDB-TABLE-061](#ddb-table-061), [DDB-TABLE-059](#ddb-table-059),
    [DDB-TABLE-069](table-streams-encryption-class.md#ddb-table-069), [DDB-TABLE-001](table.md#ddb-table-001), [DDB-TABLE-002](table-streams-encryption-class.md#ddb-table-002), [DDB-TABLE-370](#ddb-table-370), [DDB-TABLE-369](table-streams-encryption-class.md#ddb-table-369), [DDB-TABLE-052](table-streams-encryption-class.md#ddb-table-052), [DDB-TABLE-285](table-streams-encryption-class.md#ddb-table-285),
    [DDB-TABLE-120](table-streams-encryption-class.md#ddb-table-120), [DDB-TABLE-065](table-streams-encryption-class.md#ddb-table-065), [DDB-TABLE-054](table-streams-encryption-class.md#ddb-table-054), [DDB-TABLE-010](table.md#ddb-table-010) · evidence: table/creative/clobber-gsi-warm
  - notes: Extends [DDB-TABLE-066](#ddb-table-066) (equal values accepted during a Warm update) with a real second change: it is
    accepted and wins, so unlike the TableClass ([DDB-TABLE-287](table-streams-encryption-class.md#ddb-table-287)) and billing ([DDB-TABLE-452](#ddb-table-452)) cases there is no
    lost write here. OnDemandThroughput was admitted during the Warm job (it is ResourceInUse...
  - full notes: [details/DDB-TABLE-459.md](details/DDB-TABLE-459.md)

## Idempotency

- <a id="ddb-table-047"></a>**DDB-TABLE-047** `idempotency` · impact high · unhandled (not handled in controller) · verified 2026-10-08
  **Re-sending unchanged values via UpdateTable: per-field no-op vs ValidationException matrix (PAY_PER_REQUEST table)**
  AttributeDefinitions only (same) -> ValidationException 'At least one of ProvisionedThroughput, BillingMode,
  UpdateStreamEnabled, GlobalSecondaryIndexUpdates, SSESpecification, ReplicaUpdates, MultiAccountRe'.
  BillingMode=PAY_PER_REQUEST (same) -> OK. TableClass=STANDARD when TableClassSummary absent -> OK.
  StreamSpecification{false} when no stream -> ValidationException 'Table has no stream to disable: TableName:
  ackq-8c14eb-mm-a'. SSESpecification{Enabled:false} when SSE absent -> ValidationException 'One or more
  parameter values were invalid: Table is already encrypted by default'. DeletionProtectionEnabled=true then
  re-sent -> OK / ThrottlingException 'Deletion protection setting for table ackq-8c14eb-mm-a modified within
  the previous 15000 milliseconds. Please try again after 2026-10-08T23:07:38.207'.
  DeletionProtectionEnabled=false then re-sent -> ThrottlingException 'Deletion protection setting for table
  ackq-8c14eb-mm-a modified within the previous 15000 milliseconds. Please try again after
  2026-10-08T23:07:38.207' / ThrottlingException 'Deletion protection setting for table ackq-8c14eb-mm-a
  modified within the previous 15000 milliseconds. Please try again after 2026-10-08T23:07:38.207'.
  - ACK: custom_update, compare.is_ignored+delta_pre_compare · ops: UpdateTable · fields:
    AttributeDefinitions, BillingMode, TableClass, StreamSpecification, SSESpecification,
    DeletionProtectionEnabled
  - repro: PAY_PER_REQUEST table; UpdateTable each field alone with its current value, twice
  - measurements: matrix:matrix.single.empty-update=0.0, matrix:matrix.single.attrdefs-only-same=0.0,
    matrix:matrix.single.billing-same-ppr=0.0, matrix:matrix.single.tableclass-standard-when-unset=0.0,
    matrix:matrix.single.stream-disable-when-absent=0.0, matrix:matrix.single.sse-disable-when-absent=0.0,
    matrix:matrix.single.dp-true=0.0, matrix:matrix.single.dp-false=0.0, matrix:matrix.single.warm-up=0.0,
    matrix:matrix.single.warm-partial-read=0.0, matrix:matrix.single.warm-down-to-default=0.0,
    matrix:matrix.single.gtsrm-enabled=0.0, matrix:matrix.single.gtsrm-disabled=0.0,
    matrix:matrix.single.gtsrm-overrides=0.0
  - handling: not handled in the controller (as of commit 34b85e6)
  - related: [DDB-TABLE-015](table-indexes.md#ddb-table-015), [DDB-TABLE-160](table-indexes.md#ddb-table-160), [DDB-TABLE-437](#ddb-table-437), [DDB-TABLE-449](service.md#ddb-table-449) · evidence:
    table/mutation-matrix/stream-protection-throughput
  - notes: Hypotheses: H-T-033, H-T-036, H-T-029.

- <a id="ddb-table-057"></a>**DDB-TABLE-057** `idempotency` · impact high · handled · verified 2026-10-08
  **Re-sending the current BillingMode / ProvisionedThroughput via UpdateTable: which combinations are no-ops vs ValidationException**
  PROVISIONED 1/1 table: BillingMode=PROVISIONED alone -> ValidationException: 'One or more parameter values
  were invalid: ProvisionedThroughput must be specified when BillingMode is PROVISIONED'. PT 1/1 (same) ->
  ValidationException: 'The provisioned throughput for the table will not change. The requested value equals
  the current value. Current ReadCapacityUnits provisioned for the table: 1. Requested ReadCapacityUnits: 1.
  Current WriteCapacityUnits p'. BillingMode=PROVISIONED + PT 1/1 (same) -> ValidationException: 'The
  provisioned throughput for the table will not change. The requested value equals the current value. Current
  ReadCapacityUnits provisioned for the table: 1. Requested ReadCapacityUnits: 1. Current WriteCapacityUnits
  p'. PT 2/2 then PT 2/2 again -> ValidationException: 'The provisioned throughput for the table will not
  change. The requested value equals the current value. Current ReadCapacityUnits provisioned for the table: 2.
  Requested ReadCapacityUnits: 2. Current WriteCapacityUnits p'. BillingMode=PROVISIONED + PT 3/3 (change with
  mode) -> OK (response TableStatus=UPDATING, UPDATING 2.02s). After switching C to PROVISIONED:
  BillingMode+PT re-sent -> ValidationException: 'The provisioned throughput for the table will not change.
  The requested value equals the current value. Current ReadCapacityUnits provisioned for the table: 1.
  Requested ReadCapacityUnits: 1. Current WriteCapacityUnits p'; BillingMode alone -> ValidationException:
  'One or more parameter values were invalid: ProvisionedThroughput must be specified when BillingMode is
  PROVISIONED'. PPR table: BillingMode=PAY_PER_REQUEST re-sent -> OK (response TableStatus=UPDATING, UPDATING
  0.0s).
  - ACK: custom_update, compare.is_ignored+delta_pre_compare · ops: UpdateTable · fields: BillingMode,
    ProvisionedThroughput
  - repro: PROVISIONED table; UpdateTable with the same BillingMode and/or the same ProvisionedThroughput
  - handling: handled via `pkg/resource/table/hooks.go:338-432; generator.yaml:91-92`
  - related: [DDB-TABLE-369](table-streams-encryption-class.md#ddb-table-369), [DDB-TABLE-452](#ddb-table-452), [DDB-TABLE-156](#ddb-table-156), [DDB-TABLE-064](#ddb-table-064), [DDB-TABLE-177](table-streams-encryption-class.md#ddb-table-177), [DDB-TABLE-019](table-streams-encryption-class.md#ddb-table-019),
    [DDB-TABLE-018](table-streams-encryption-class.md#ddb-table-018), [DDB-TABLE-365](table-streams-encryption-class.md#ddb-table-365), [DDB-TABLE-183](#ddb-table-183), [DDB-TABLE-358](#ddb-table-358), [DDB-TABLE-283](table-streams-encryption-class.md#ddb-table-283), [DDB-TABLE-024](#ddb-table-024), [DDB-TABLE-038](#ddb-table-038),
    [DDB-TABLE-039](#ddb-table-039), [DDB-TABLE-056](#ddb-table-056), [DDB-TABLE-059](#ddb-table-059), [DDB-TABLE-433](table-streams-encryption-class.md#ddb-table-433), [DDB-TABLE-058](#ddb-table-058), [DDB-TABLE-060](#ddb-table-060), [DDB-TABLE-370](#ddb-table-370),
    [DDB-TABLE-061](#ddb-table-061), [DDB-TABLE-063](#ddb-table-063), [DDB-TABLE-164](table-indexes.md#ddb-table-164) · evidence: table/mutation-matrix/billing-capacity
  - notes: Hypotheses: H-T-029.
  - full notes: [details/DDB-TABLE-057.md](details/DDB-TABLE-057.md)

- <a id="ddb-table-156"></a>**DDB-TABLE-156** `idempotency` · impact medium · handled · verified 2026-10-08
  **Re-sending BillingMode=PAY_PER_REQUEST on a PAY_PER_REQUEST table is a 200 no-op: response says UPDATING, DescribeTable stays ACTIVE**
  UpdateTable BillingMode=PAY_PER_REQUEST on a table already in PAY_PER_REQUEST returns 200 with
  TableStatus=UPDATING in the response (no 'will not change' error, unlike re-sending identical
  ProvisionedThroughput). Combined with DeletionProtectionEnabled in the same call it fails with
  ValidationException 'DeletionProtection modification must be the only operation in the request'.
  - ACK: compare.is_ignored+delta_pre_compare, custom_update · ops: UpdateTable · fields: BillingMode
  - repro: PAY_PER_REQUEST table; UpdateTable BillingMode=PAY_PER_REQUEST
  - handling: handled via `pkg/resource/table/hooks.go:338-432; generator.yaml:91-92`
  - related: [DDB-TABLE-369](table-streams-encryption-class.md#ddb-table-369), [DDB-TABLE-452](#ddb-table-452), [DDB-TABLE-057](#ddb-table-057), [DDB-TABLE-064](#ddb-table-064), [DDB-TABLE-177](table-streams-encryption-class.md#ddb-table-177), [DDB-TABLE-019](table-streams-encryption-class.md#ddb-table-019),
    [DDB-TABLE-018](table-streams-encryption-class.md#ddb-table-018), [DDB-TABLE-365](table-streams-encryption-class.md#ddb-table-365), [DDB-TABLE-183](#ddb-table-183), [DDB-TABLE-358](#ddb-table-358), [DDB-TABLE-283](table-streams-encryption-class.md#ddb-table-283), [DDB-TABLE-060](#ddb-table-060), [DDB-TABLE-370](#ddb-table-370),
    [DDB-TABLE-284](table-streams-encryption-class.md#ddb-table-284), [DDB-TABLE-179](#ddb-table-179), [DDB-TABLE-066](#ddb-table-066), [DDB-TABLE-371](table-streams-encryption-class.md#ddb-table-371), [DDB-TABLE-140](table-streams-encryption-class.md#ddb-table-140), [DDB-TABLE-180](table-streams-encryption-class.md#ddb-table-180), [DDB-TABLE-433](table-streams-encryption-class.md#ddb-table-433),
    [DDB-TABLE-056](#ddb-table-056), [DDB-TABLE-174](table-indexes.md#ddb-table-174), [DDB-TABLE-199](table-replicas.md#ddb-table-199), [DDB-TABLE-224](table-replicas.md#ddb-table-224), [DDB-TABLE-162](table-indexes.md#ddb-table-162), [DDB-TABLE-127](table-indexes.md#ddb-table-127) · evidence:
    table/mutation-matrix/gsi-billing-throughput
  - notes: Complements H-T-030: a controller that echoes billingMode on every reconcile causes a spurious
    UPDATING cycle.
  - full notes: [details/DDB-TABLE-156.md](details/DDB-TABLE-156.md)

## Errors

- <a id="ddb-table-171"></a>**DDB-TABLE-171** `error-code` · impact high · unhandled (not handled in controller) · verified 2026-10-08
  **LimitExceededException message catalogue for index/throughput operations: retryable vs terminal variants share one code**
  Observed LimitExceededException messages (all HTTP 400): retryable - 'Subscriber limit exceeded: Only 1
  online index can be created or deleted simultaneously per table' (clears when the index settles) and
  'Subscriber limit exceeded: Provisioned throughput decreases are limited within a given UTC day. After the
  first 4 decreases, each subsequent decrease in the same UTC day can be performed at most once every 3600
  seconds. Number of decreases today: 4. Last decrease at ...' (clears after an hour); terminal - 'Subscriber
  limit exceeded: Number of global secondary indexes exceeds per-table limit of 20', 'The requested
  ReadCapacityUnits, N, is above the per table maximum for the account in REGION. Per table maximum: 40000.
  ...', '...for index gsi1, N, is above the per index maximum...', 'Subscriber limit exceeded: Requested
  MaxReadRequestUnits for OnDemandThroughput for table exceeds TableMaxReadCapacityUnits of the account in
  region REGION' (and 'for index : gsi1'), '...ReadUnitsPerSecond for WarmThroughput for index gsi1 exceeds
  TableMaxReadCapacityUnits...', 'This request would have caused the ReadCapacityUnits limit to be exceeded
  for the account in REGION. Current ReadCapacityUnits reserved by the account: N. Limit: 80000. Requested:
  M'. Latencies for these rejections are 0.8-2.9 s (vs ~10 ms for ValidationException). No 'Too many
  operations for a given subscriber' variant was triggered.
  - ACK: terminal_codes, requeue · ops: CreateTable, UpdateTable · fields: GlobalSecondaryIndexUpdates,
    ProvisionedThroughput, OnDemandThroughput, WarmThroughput
  - repro: see table/mutation-matrix/gsi-update-granularity, table/limits/gsi-decrease-budget,
    table/mutation-matrix/gsi-billing-throughput, this probe
  - handling: not handled in the controller (as of commit 34b85e6)
  - related: [DDB-TABLE-159](table-indexes.md#ddb-table-159), [DDB-TABLE-157](table-indexes.md#ddb-table-157), [DDB-TABLE-154](#ddb-table-154), [DDB-TABLE-170](table-indexes.md#ddb-table-170), [DDB-TABLE-169](table-indexes.md#ddb-table-169) · evidence:
    table/limits/gsi-concurrency-quotas
  - notes: Dry tabulation for H-T-139 from this shard's probes; the controller must classify by message
    substring.

## Request validation

- <a id="ddb-table-024"></a>**DDB-TABLE-024** `request-validation` · impact medium · handled · verified 2026-10-08
  **CreateTable without BillingMode defaults to PROVISIONED and requires ProvisionedThroughput**
  CreateTable with only TableName/AttributeDefinitions/KeySchema (no BillingMode, no ProvisionedThroughput)
  fails with ValidationException (HTTP 400): 'One or more parameter values were invalid: ReadCapacityUnits and
  WriteCapacityUnits must both be specified when BillingMode is PROVISIONED'. With ProvisionedThroughput 1/1
  and still no BillingMode the table is created as PROVISIONED.
  - ACK: custom_create, docs-only · ops: CreateTable · fields: BillingMode, ProvisionedThroughput
  - repro: CreateTable(TableName, AttributeDefinitions, KeySchema) with no BillingMode and no
    ProvisionedThroughput
  - handling: handled via `generator.yaml:7-9; templates/hooks/table/sdk_read_one_post_set_output.go.tpl:51-55`
  - related: [DDB-TABLE-038](#ddb-table-038), [DDB-TABLE-039](#ddb-table-039), [DDB-TABLE-056](#ddb-table-056), [DDB-TABLE-057](#ddb-table-057), [DDB-TABLE-059](#ddb-table-059), [DDB-TABLE-018](table-streams-encryption-class.md#ddb-table-018),
    [DDB-TABLE-433](table-streams-encryption-class.md#ddb-table-433) · evidence: table/round-trip/full-fields
  - notes: Hypotheses: H-T-044. Confirms the first half of H-T-044.

- <a id="ddb-table-038"></a>**DDB-TABLE-038** `request-validation` · impact high · unhandled (not handled in controller) · verified 2026-10-08
  **Throughput fields sent with the wrong BillingMode or out of range: CreateTable codes and messages**
  PAY_PER_REQUEST + ProvisionedThroughput 1/1 -> ValidationException: 'One or more parameter values were
  invalid: Neither ReadCapacityUnits nor WriteCapacityUnits can be specified when BillingMode is
  PAY_PER_REQUEST'. PAY_PER_REQUEST + ProvisionedThroughput 0/0 -> ValidationException: '2 validation errors
  detected: Value '0' at 'provisionedThroughput.writeCapacityUnits' failed to satisfy constraint: Member must
  have value greater than or equal to 1; Value '0' at 'provisionedThroughput.readCapacityUnits' failed to
  satisfy constraint: Member must have value greater than or equal to '. PROVISIONED + OnDemandThroughput ->
  ValidationException: 'One or more parameter values were invalid: MaxReadRequestUnits for OnDemandThroughput
  cannot be specified when table BillingMode is PROVISIONED.'. PROVISIONED 0/0 -> ValidationException: '2
  validation errors detected: Value '0' at 'provisionedThroughput.writeCapacityUnits' failed to satisfy
  constraint: Member must have value greater than or equal to 1; Value '0' at
  'provisionedThroughput.readCapacityUnits' failed to satisfy constraint: Member must have value greater than
  or equal to '. PROVISIONED -1/-1 -> ValidationException: '2 validation errors detected: Value '-1' at
  'provisionedThroughput.writeCapacityUnits' failed to satisfy constraint: Member must have value greater than
  or equal to 1; Value '-1' at 'provisionedThroughput.readCapacityUnits' failed to satisfy constraint: Member
  must have value greater than or equal t'. PROVISIONED read-only -> ValidationException: '1 validation error
  detected: Value null at 'provisionedThroughput.writeCapacityUnits' failed to satisfy constraint: Member must
  not be null'. PROVISIONED {} -> ValidationException: '2 validation errors detected: Value null at
  'provisionedThroughput.writeCapacityUnits' failed to satisfy constraint: Member must not be null; Value null
  at 'provisionedThroughput.readCapacityUnits' failed to satisfy constraint: Member must not be null'. no
  BillingMode + 0/0 -> ValidationException: '2 validation errors detected: Value '0' at
  'provisionedThroughput.writeCapacityUnits' failed to satisfy constraint: Member must have value greater than
  or equal to 1; Value '0' at 'provisionedThroughput.readCapacityUnits' failed to satisfy constraint: Member
  must have value greater than or equal to '.
  - ACK: custom_create, compare.nil_equals_zero_value · ops: CreateTable · fields: BillingMode,
    ProvisionedThroughput, OnDemandThroughput
  - repro: CreateTable PAY_PER_REQUEST with ProvisionedThroughput; CreateTable PROVISIONED with
    OnDemandThroughput; zero/negative capacities
  - handling: not handled in the controller (as of commit 34b85e6)
  - related: [DDB-TABLE-024](#ddb-table-024), [DDB-TABLE-039](#ddb-table-039), [DDB-TABLE-056](#ddb-table-056), [DDB-TABLE-057](#ddb-table-057), [DDB-TABLE-059](#ddb-table-059), [DDB-TABLE-018](table-streams-encryption-class.md#ddb-table-018),
    [DDB-TABLE-433](table-streams-encryption-class.md#ddb-table-433) · evidence: table/weird-inputs/create-validation
  - notes: Hypotheses: H-T-044.

- <a id="ddb-table-039"></a>**DDB-TABLE-039** `request-validation` · impact medium · unhandled (not handled in controller) · verified 2026-10-08
  **OnDemandThroughput and WarmThroughput edge values at CreateTable (0, -1, -2, one member, empty struct, below defaults)**
  OnDemandThroughput 0/0 -> ValidationException: 'One or more parameter values were invalid: Requested
  MaxReadRequestUnits for OnDemandThroughput for table is outside of valid range'. -1/-1 ->
  ValidationException: 'One or more parameter values were invalid: Requested MaxReadRequestUnits for
  OnDemandThroughput for table is outside of valid range'. -2/-2 -> ValidationException: 'One or more
  parameter values were invalid: Requested MaxReadRequestUnits for OnDemandThroughput for table is outside of
  valid range'. read-only -> ACCEPTED -> Describe {"MaxReadRequestUnits": 100}. {} -> ACCEPTED. WarmThroughput
  1/1 -> ValidationException: 'One or more parameter values were invalid: Requested ReadUnitsPerSecond for
  WarmThroughput for table is lower than initial throughput for OnDemand. See:
  https://docs.aws.amazon.com/amazondynamodb/latest/developerguide/on-demand-capacity-mode.html#on-demand-capacity-mode-initial'.
  0/0 -> ValidationException: 'One or more parameter values were invalid: Requested ReadUnitsPerSecond for
  WarmThroughput for table is lower than initial throughput for OnDemand. See:
  https://docs.aws.amazon.com/amazondynamodb/latest/developerguide/on-demand-capacity-mode.html#on-demand-capacity-mode-initial'.
  read-only -> ACCEPTED -> Describe {"ReadUnitsPerSecond": 12001, "WriteUnitsPerSecond": 4000, "Status":
  "ACTIVE"}. {} -> ACCEPTED. PROVISIONED 10/10 with WarmThroughput 5/5 -> ValidationException: 'One or more
  parameter values were invalid: Requested ReadUnitsPerSecond for WarmThroughput for table is lower than
  ReadCapacityUnits of ProvisionedThroughput'.
  - ACK: custom_create, compare.nil_equals_zero_value · ops: CreateTable · fields: OnDemandThroughput,
    WarmThroughput
  - repro: CreateTable PAY_PER_REQUEST with the listed OnDemandThroughput / WarmThroughput values
  - handling: not handled in the controller (as of commit 34b85e6)
  - related: [DDB-TABLE-045](#ddb-table-045), [DDB-TABLE-032](#ddb-table-032), [DDB-TABLE-028](#ddb-table-028), [DDB-TABLE-061](#ddb-table-061), [DDB-TABLE-068](#ddb-table-068), [DDB-TABLE-033](#ddb-table-033),
    [DDB-TABLE-059](#ddb-table-059), [DDB-TABLE-024](#ddb-table-024), [DDB-TABLE-038](#ddb-table-038), [DDB-TABLE-056](#ddb-table-056), [DDB-TABLE-057](#ddb-table-057), [DDB-TABLE-018](table-streams-encryption-class.md#ddb-table-018), [DDB-TABLE-433](table-streams-encryption-class.md#ddb-table-433) ·
    evidence: table/weird-inputs/create-validation
  - notes: Hypotheses: H-T-121, H-T-039.

- <a id="ddb-table-042"></a>**DDB-TABLE-042** `request-validation` · impact medium · handled · verified 2026-10-08
  **KeySchema is order-sensitive (HASH must be first); duplicate AttributeDefinitions and unused/missing definitions are rejected** (hypothesis refuted; behavior confirmed)
  KeySchema [RANGE, HASH] order -> ValidationException: 'Invalid KeySchema: The first KeySchemaElement is not
  a HASH key type'. two HASH -> ValidationException: 'Invalid KeySchema: The second KeySchemaElement is not a
  RANGE key type'. RANGE only -> ValidationException: 'Invalid KeySchema: The first KeySchemaElement is not a
  HASH key type'. three elements -> ValidationException: '1 validation error detected: Value
  '[com.amazonaws.dynamodb.v20120810.KeySchemaElement@ad28eb1d,
  com.amazonaws.dynamodb.v20120810.KeySchemaElement@38b9654b,
  com.amazonaws.dynamodb.v20120810.KeySchemaElement@38bb13cd]' at 'keySchema' faile. [] ->
  ValidationException: '1 validation error detected: Value '[]' at 'keySchema' failed to satisfy constraint:
  Member must have length greater than or equal to 1'. missing -> ValidationException: '1 validation error
  detected: Value null at 'keySchema' failed to satisfy constraint: Member must not be null'. attribute-name
  case mismatch (PK vs pk) -> ValidationException: 'One or more parameter values were invalid: Some index key
  attributes are not defined in AttributeDefinitions. Keys: [PK], AttributeDefinitions: [pk]'. duplicate
  AttributeDefinitions same type -> ValidationException: 'Attribute Name is duplicated: pk'. duplicate
  different types -> ValidationException: 'Attribute Name is duplicated: pk'. unused attribute ->
  ValidationException: 'One or more parameter values were invalid: Number of attributes in KeySchema does not
  exactly match number of attributes defined in AttributeDefinitions'. AttributeDefinitions [] ->
  ValidationException: 'Invalid KeySchema: Some index key attribute have no definition'. missing ->
  ValidationException: '1 validation error detected: Value null at 'attributeDefinitions' failed to satisfy
  constraint: Member must not be null'. wrong attribute -> ValidationException: 'One or more parameter values
  were invalid: Some index key attributes are not defined in AttributeDefinitions. Keys: [pk],
  AttributeDefinitions: [other]'. reversed AttributeDefinitions order -> OK (Describe keeps sent order:
  [{"AttributeName": "pk", "AttributeType": "S"}, {"AttributeName": "sk", "AttributeType": "N"}]).
  - ACK: compare.is_ignored+delta_pre_compare, custom_create · ops: CreateTable, DescribeTable · fields:
    KeySchema, AttributeDefinitions
  - repro: CreateTable with the listed KeySchema / AttributeDefinitions shapes; DescribeTable the accepted
    ones
  - handling: handled via `generator.yaml:88-90; pkg/resource/table/sdk.go:1234-1250`
  - related: [DDB-TABLE-041](service.md#ddb-table-041), [DDB-TABLE-364](#ddb-table-364) · evidence: table/weird-inputs/create-validation
  - notes: Hypotheses: H-T-042. H-T-042 partially refuted: the order-insensitivity claim is wrong (RANGE
    listed before HASH is a ValidationException), the duplicate-definition rejection is confirmed.
    AttributeDefinitions order IS free and is echoed back as sent, so a controller must compare it...
  - full notes: [details/DDB-TABLE-042.md](details/DDB-TABLE-042.md)

- <a id="ddb-table-059"></a>**DDB-TABLE-059** `request-validation` · impact high · unhandled (not handled in controller) · verified 2026-10-08
  **UpdateTable: OnDemandThroughput on PROVISIONED and ProvisionedThroughput on PPR rejected; WarmThroughput below current Warm value rejected**
  PROVISIONED table: OnDemandThroughput -> ValidationException: 'One or more parameter values were invalid:
  MaxReadRequestUnits for OnDemandThroughput cannot be specified when the table BillingMode is PROVISIONED'.
  WarmThroughput 1/1 (below RCU/WCU) -> ValidationException: 'One or more parameter values were invalid:
  Requested ReadUnitsPerSecond for WarmThroughput for table is lower than current WarmThroughput, decreasing
  WarmThroughput is not supported'. WarmThroughput 50/50 (above) -> OK (response TableStatus=ACTIVE, UPDATING
  0.0s); Describe {"TableStatus": "ACTIVE", "BillingModeSummary": "<absent>", "ProvisionedThroughput":
  {"LastIncreaseDateTime": "2026-10-08 23:09:30.195000+00:00", "LastDecreaseDateTime": "2026-10-08
  23:09:29.159000+00:00", "NumberOfDecreasesToday": 1, "ReadCapacityUnits": 2, "WriteCapacityUnits": 1},
  "OnDemandThroughput": "<absent>", "WarmThroughput": {"ReadUnitsPerSecond": 3, "WriteUnitsPerSecond": 3,
  "Status": "UPDATING"}, "TableClassSummary": "<absent>"}. PPR table: ProvisionedThroughput 1/1 ->
  ValidationException: 'One or more parameter values were invalid: Neither ReadCapacityUnits nor
  WriteCapacityUnits can be specified when BillingMode is PAY_PER_REQUEST'. C after switch to PROVISIONED:
  OnDemandThroughput -> ValidationException: 'One or more parameter values were invalid: MaxReadRequestUnits
  for OnDemandThroughput cannot be specified when the table BillingMode is PROVISIONED'.
  - ACK: custom_update, compare.is_ignored+delta_pre_compare · ops: UpdateTable · fields: OnDemandThroughput,
    ProvisionedThroughput, WarmThroughput
  - repro: PROVISIONED table: UpdateTable OnDemandThroughput{100,100}; PPR table: UpdateTable
    ProvisionedThroughput{1,1}
  - handling: not handled in the controller (as of commit 34b85e6)
  - related: [DDB-TABLE-179](#ddb-table-179), [DDB-TABLE-066](#ddb-table-066), [DDB-TABLE-459](#ddb-table-459), [DDB-TABLE-049](#ddb-table-049), [DDB-TABLE-061](#ddb-table-061), [DDB-TABLE-069](table-streams-encryption-class.md#ddb-table-069),
    [DDB-TABLE-121](#ddb-table-121), [DDB-TABLE-032](#ddb-table-032), [DDB-TABLE-028](#ddb-table-028), [DDB-TABLE-068](#ddb-table-068), [DDB-TABLE-033](#ddb-table-033), [DDB-TABLE-039](#ddb-table-039), [DDB-TABLE-045](#ddb-table-045),
    [DDB-TABLE-024](#ddb-table-024), [DDB-TABLE-038](#ddb-table-038), [DDB-TABLE-056](#ddb-table-056), [DDB-TABLE-057](#ddb-table-057), [DDB-TABLE-018](table-streams-encryption-class.md#ddb-table-018), [DDB-TABLE-433](table-streams-encryption-class.md#ddb-table-433) · evidence:
    table/mutation-matrix/billing-capacity
  - notes: Contradiction with [DDB-TABLE-049](#ddb-table-049), [DDB-TABLE-179](#ddb-table-179), [DDB-TABLE-066](#ddb-table-066), [DDB-TABLE-061](#ddb-table-061): 049 title/behavior:
    WarmThroughput 'back to 12000/4000 -> OK' (a decrease accepted); 179/066/061/059: any value below the
    current WarmThroughput is ValidationException 'decreasing WarmThroughput is not supported'. 049's...
  - full notes: [details/DDB-TABLE-059.md](details/DDB-TABLE-059.md)

- <a id="ddb-table-358"></a>**DDB-TABLE-358** `request-validation` · impact medium · handled · verified 2026-10-09
  **AttributeDefinitions re-typing the key (pk S->N) via UpdateTable without an index change: accepted 200; DescribeTable keeps S**
  AttributeDefinitions=[pk N] as the only parameter -> ValidationException (HTTP 400) 'At least one of
  ProvisionedThroughput, BillingMode, ... or TableClass is required'. AttributeDefinitions=[pk N, sk N] +
  DeletionProtectionEnabled=false -> 200 OK (TableStatus=ACTIVE); DescribeTable afterwards: [['lsk', 'N'],
  ['pk', 'S'], ['sk', 'S']]. AttributeDefinitions=[newattr B, lsk S] + BillingMode=PAY_PER_REQUEST re-send ->
  200 OK (TableStatus=UPDATING) (timeline [('ACTIVE', None)]); DescribeTable afterwards: [['lsk', 'N'], ['pk',
  'S'], ['sk', 'S']]. AttributeDefinitions=[] + DeletionProtectionEnabled=true -> 200 OK (TableStatus=ACTIVE).
  AttributeDefinitions=[pk S] (correct type) alone -> ValidationException (HTTP 400) 'At least one of
  ProvisionedThroughput, BillingMode, ... or TableClass is required'.
  - ACK: is_immutable, compare.is_ignored+delta_pre_compare · ops: UpdateTable, DescribeTable · fields:
    AttributeDefinitions
  - repro: ACTIVE PPR table pk S: UpdateTable(AttributeDefinitions=[{pk,N},{sk,N}],
    DeletionProtectionEnabled=false); DescribeTable
  - handling: handled via `generator.yaml:55-58; pkg/resource/table/hooks.go:621-627`
  - related: [DDB-TABLE-162](table-indexes.md#ddb-table-162), [DDB-TABLE-127](table-indexes.md#ddb-table-127), [DDB-TABLE-177](table-streams-encryption-class.md#ddb-table-177), [DDB-TABLE-019](table-streams-encryption-class.md#ddb-table-019), [DDB-TABLE-156](#ddb-table-156), [DDB-TABLE-057](#ddb-table-057),
    [DDB-TABLE-018](table-streams-encryption-class.md#ddb-table-018), [DDB-TABLE-365](table-streams-encryption-class.md#ddb-table-365), [DDB-TABLE-183](#ddb-table-183), [DDB-TABLE-283](table-streams-encryption-class.md#ddb-table-283), [DDB-TABLE-433](table-streams-encryption-class.md#ddb-table-433), [DDB-TABLE-056](#ddb-table-056), [DDB-TABLE-174](table-indexes.md#ddb-table-174),
    [DDB-TABLE-199](table-replicas.md#ddb-table-199), [DDB-TABLE-224](table-replicas.md#ddb-table-224) · hypotheses: H-T-066 · evidence: table/mutation-matrix/schema-immutability
  - notes: Refutes the ValidationException expectation of H-T-066 for the no-index path too: the service
    neither applies nor rejects a conflicting key type; AttributeDefinitions on UpdateTable is only consulted
    for GSI Create. Attribute-type drift in the spec must be flagged by the controller as recreate-only.

- <a id="ddb-table-437"></a>**DDB-TABLE-437** `request-validation` · impact medium · unhandled (not handled in controller) · verified 2026-10-09
  **Empty-struct UpdateTable members: WarmThroughput {} and OnDemandThroughput {} -> HTTP 500 InternalFailure...**
  Idle PAY_PER_REQUEST and PROVISIONED tables, members sent alone (client-side validation disabled where
  botocore would refuse): WarmThroughput={} -> HTTP 500 InternalFailure on both tables (OnDemandThroughput={}
  likewise on both). StreamSpecification={} / {StreamViewType only} -> ValidationException 'Value null at
  streamSpecification.streamEnabled'; {StreamEnabled:true} without view type -> 'If stream is being enabled
  then UpdateViewType is required'; {StreamEnabled:false} with no stream -> 'Table has no stream to disable'.
  ProvisionedThroughput={} or with ONE member -> ValidationException naming the null member (no partial
  merge). GlobalSecondaryIndexUpdates [{}] -> 'One of ...Update, ...Create, ...Delete must be specified',
  [{Update:{}}]/[{Create:{}}]/[{Delete:{}}] -> 'Value null at ...indexName', [{Update:{IndexName}}] ->
  ResourceNotFoundException. ReplicaUpdates [] -> 'Member must have length greater than or equal to 1', [{}]
  -> 'There are no actions specified in the Replica Update Action'. TableClass='' / BillingMode='' -> enum
  ValidationException. SSESpecification={} on an AWS-owned-key table -> 'Table is already encrypted by
  default'; {Enabled:true} (no SSEType) -> 200 and the table re-encrypts with the AWS-managed key
  (SSEDescription.Status UPDATING ~22 s, SSE quota consumed); {SSEType:KMS} alone on the now-KMS table -> 200
  and another ~22 s re-encryption with no visible change; {Enabled:false, SSEType:KMS} -> 'SSEType can not be
  specified if Enabled is false'. AttributeDefinitions alone -> 'At least one of ...' (ignored member).
  - ACK: terminal_codes, custom_update, compare.nil_equals_zero_value · ops: UpdateTable · fields:
    WarmThroughput, OnDemandThroughput, StreamSpecification, ProvisionedThroughput,
    GlobalSecondaryIndexUpdates, ReplicaUpdates, SSESpecification, TableClass, BillingMode,
    AttributeDefinitions
  - repro: UpdateTable TableName=<idle table> WarmThroughput={} -> 500; UpdateTable
    SSESpecification={Enabled:true} -> 200 + 22 s re-encryption
  - measurements: sse_enabled_only_reencrypt_s=22.2
  - handling: not handled in the controller (as of commit 34b85e6)
  - related: [DDB-TABLE-178](#ddb-table-178), [DDB-TABLE-161](table-subresources.md#ddb-table-161), [DDB-TABLE-141](table-streams-encryption-class.md#ddb-table-141), [DDB-TABLE-044](table-streams-encryption-class.md#ddb-table-044), [DDB-TABLE-164](table-indexes.md#ddb-table-164), [DDB-TABLE-015](table-indexes.md#ddb-table-015),
    [DDB-TABLE-047](#ddb-table-047), [DDB-TABLE-160](table-indexes.md#ddb-table-160), [DDB-TABLE-449](service.md#ddb-table-449), [DDB-TABLE-203](table-replicas.md#ddb-table-203), [DDB-TABLE-223](table-replicas.md#ddb-table-223), [DDB-TABLE-307](table-replicas.md#ddb-table-307), [DDB-TABLE-225](table-replicas.md#ddb-table-225),
    [DDB-TABLE-308](table-replicas.md#ddb-table-308), [DDB-TABLE-294](table-replicas.md#ddb-table-294) · evidence: table/creative/empty-struct-updates
  - notes: Extends [DDB-TABLE-178](#ddb-table-178) (OnDemandThroughput {} -> 500) to WarmThroughput: a controller that
    materialises spec.warmThroughput/onDemandThroughput as an empty struct (nil members) gets a 500 that is
    permanent for that request and would be retried forever. All other nil-member shapes are ordinary...
  - full notes: [details/DDB-TABLE-437.md](details/DDB-TABLE-437.md)

## Update granularity and ordering

- <a id="ddb-table-049"></a>**DDB-TABLE-049** `update-granularity` · impact medium · handled · verified 2026-10-08
  **WarmThroughput UpdateTable chain inside one in-flight increase: increase, partial struct, re-sends and 'back to 12000/4000' all 200**
  WarmThroughput 12001/4001 -> OK (changed ["WarmThroughput.Status"]); re-sent -> OK.
  {ReadUnitsPerSecond:12002} only -> OK (changed []); re-sent -> OK. back to 12000/4000 -> OK; re-sent -> OK.
  - ACK: custom_update, requeue · ops: UpdateTable, DescribeTable · fields: WarmThroughput
  - repro: PAY_PER_REQUEST table; UpdateTable WarmThroughput 12001/4001; again; {Read:12002}; 12000/4000
  - measurements: warm-up=0.0, warm-partial-read=0.0, warm-down-to-default=0.0
  - handling: handled via `pkg/resource/table/hooks.go:338-432; generator.yaml:91-92`
  - related: [DDB-TABLE-179](#ddb-table-179), [DDB-TABLE-066](#ddb-table-066), [DDB-TABLE-459](#ddb-table-459), [DDB-TABLE-061](#ddb-table-061), [DDB-TABLE-059](#ddb-table-059), [DDB-TABLE-069](table-streams-encryption-class.md#ddb-table-069),
    [DDB-TABLE-121](#ddb-table-121) · evidence: table/mutation-matrix/stream-protection-throughput
  - notes: Contradiction with [DDB-TABLE-179](#ddb-table-179), [DDB-TABLE-066](#ddb-table-066), [DDB-TABLE-061](#ddb-table-061), [DDB-TABLE-059](#ddb-table-059): 049 title/behavior:
    WarmThroughput 'back to 12000/4000 -> OK' (a decrease accepted); 179/066/061/059: any value below the
    current WarmThroughput is ValidationException 'decreasing WarmThroughput is not supported'. 049's...
  - full notes: [details/DDB-TABLE-049.md](details/DDB-TABLE-049.md)

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
  - related: [DDB-TABLE-053](service.md#ddb-table-053), [DDB-TABLE-130](service.md#ddb-table-130), [DDB-TABLE-132](service.md#ddb-table-132), [DDB-TABLE-441](table.md#ddb-table-441), [DDB-TABLE-131](table.md#ddb-table-131), [DDB-TABLE-178](#ddb-table-178),
    [DDB-TABLE-182](#ddb-table-182) · evidence: table/mutation-matrix/stream-protection-throughput
  - notes: Hypotheses: H-T-121, H-T-039.
  - full notes: [details/DDB-TABLE-051.md](details/DDB-TABLE-051.md)

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
  - related: [DDB-TABLE-051](#ddb-table-051), [DDB-TABLE-182](#ddb-table-182), [DDB-TABLE-053](service.md#ddb-table-053) · evidence:
    table/response-fidelity/create-update-response
  - notes: H-T-121 and H-T-039 partial. The -1 values seen in DescribeTable right after an update are
    transient (about 1s, eventually consistent); the steady-state representation omits cleared members and
    omits the struct when both are cleared (see table/response-fidelity/odt-minus1-representation)....
  - full notes: [details/DDB-TABLE-178.md](details/DDB-TABLE-178.md)

## Field behavior (defaults, normalization, shapes, immutability)

- <a id="ddb-table-028"></a>**DDB-TABLE-028** `server-default` · impact high · unhandled (not handled in controller) · verified 2026-10-08
  **PAY_PER_REQUEST tables report ProvisionedThroughput 0/0 and default WarmThroughput; OnDemandThroughput absent unless sent**
  DescribeTable ProvisionedThroughput per shape: {"min": {"NumberOfDecreasesToday": 0, "ReadCapacityUnits": 1,
  "WriteCapacityUnits": 1}, "ppr": {"NumberOfDecreasesToday": 0, "ReadCapacityUnits": 0, "WriteCapacityUnits":
  0}, "prov-explicit": {"NumberOfDecreasesToday": 0, "ReadCapacityUnits": 1, "WriteCapacityUnits": 1},
  "full-prov": {"NumberOfDecreasesToday": 0, "ReadCapacityUnits": 5, "WriteCapacityUnits": 5}, "full-ppr":
  {"NumberOfDecreasesToday": 0, "ReadCapacityUnits": 0, "WriteCapacityUnits": 0}, "sse-false":
  {"NumberOfDecreasesToday": 0, . WarmThroughput: {"min": {"ReadUnitsPerSecond": 1, "WriteUnitsPerSecond": 1,
  "Status": "ACTIVE"}, "ppr": {"ReadUnitsPerSecond": 12000, "WriteUnitsPerSecond": 4000, "Status": "ACTIVE"},
  "prov-explicit": {"ReadUnitsPerSecond": 1, "WriteUnitsPerSecond": 1, "Status": "ACTIVE"}, "full-prov":
  {"ReadUnitsPerSecond": 5, "WriteUnitsPerSecond": 5, "Status": "ACTIVE"}, "full-ppr": {"ReadUnitsPerSecond":
  12001, "WriteUnitsPerSecond": 4001, "Status": "ACTIVE"}, "sse-false": {"ReadUnitsPerSecond": 12000,
  "WriteUnitsPerSecond". OnDemandThroughput: {"min": "<absent>", "ppr": "<absent>", "prov-explicit":
  "<absent>", "full-prov": "<absent>", "full-ppr": {"MaxReadRequestUnits": 100, "MaxWriteRequestUnits": 100},
  "sse-false": "<absent>", "sse-type-only": "<absent>", "sse-awsalias": "<absent>", "sse-keyid": "<absent>",
  "sse-keyarn": "<absent>", "ss.
  - ACK: compare.nil_equals_zero_value, compare.is_ignored+delta_pre_compare · ops: CreateTable, DescribeTable
    · fields: ProvisionedThroughput, WarmThroughput, OnDemandThroughput
  - repro: CreateTable PAY_PER_REQUEST (with and without OnDemandThroughput / WarmThroughput); DescribeTable
  - handling: not handled in the controller (as of commit 34b85e6)
  - related: [DDB-TABLE-032](#ddb-table-032), [DDB-TABLE-061](#ddb-table-061), [DDB-TABLE-068](#ddb-table-068), [DDB-TABLE-033](#ddb-table-033), [DDB-TABLE-039](#ddb-table-039), [DDB-TABLE-045](#ddb-table-045),
    [DDB-TABLE-059](#ddb-table-059) · evidence: table/round-trip/full-fields
  - notes: Hypotheses: H-T-039. H-T-039 create-side part (absent unless set); the -1 sentinel part is in
    table/mutation-matrix/stream-protection-throughput.

- <a id="ddb-table-032"></a>**DDB-TABLE-032** `server-default` · impact high · unhandled (not handled in controller) · verified 2026-10-08
  **WarmThroughput always present in DescribeTable: 12000/4000 default for PAY_PER_REQUEST, equals RCU/WCU for PROVISIONED**
  DescribeTable.WarmThroughput per shape: {"min": {"ReadUnitsPerSecond": 1, "WriteUnitsPerSecond": 1,
  "Status": "ACTIVE"}, "ppr": {"ReadUnitsPerSecond": 12000, "WriteUnitsPerSecond": 4000, "Status": "ACTIVE"},
  "prov-explicit": {"ReadUnitsPerSecond": 1, "WriteUnitsPerSecond": 1, "Status": "ACTIVE"}, "full-prov":
  {"ReadUnitsPerSecond": 5, "WriteUnitsPerSecond": 5, "Status": "ACTIVE"}, "full-ppr": {"ReadUnitsPerSecond":
  12001, "WriteUnitsPerSecond": 4001, "Status": "ACTIVE"}, "sse-false": {"ReadUnitsPerSecond": 12000,
  "WriteUnitsPerSecond": 4000, "Status": "ACTIVE"}, "sse-type-only": {"ReadUnitsPerSecond": 12000,
  "WriteUnitsPerSecond": 4000, "Status": "ACTIVE"}, "sse-awsalias": {"ReadUnitsPerSecond": 12000,
  "WriteUnitsPerSecond": 4000,. For PROVISIONED tables it reports ReadUnitsPerSecond/WriteUnitsPerSecond equal
  to the provisioned RCU/WCU (1/1 for min, 5/5 for full-prov) and the CreateTable response omits
  WarmThroughput entirely for them; for PAY_PER_REQUEST the default is 12000/4000 with Status=ACTIVE. Explicit
  WarmThroughput at create (full-ppr 12001/4001) shows Status=UPDATING in the CreateTable response. prov-warm
  result: {"ReadUnitsPerSecond": 100, "WriteUnitsPerSecond": 100, "Status": "ACTIVE"}.
  - ACK: compare.is_ignored+delta_pre_compare, late_initialize, is_read_only · ops: CreateTable, DescribeTable
    · fields: WarmThroughput
  - repro: CreateTable PROVISIONED 1/1 without WarmThroughput; DescribeTable -> WarmThroughput{1,1,ACTIVE};
    CreateTable PAY_PER_REQUEST -> WarmThroughput{12000,4000,ACTIVE}
  - handling: not handled in the controller (as of commit 34b85e6)
  - related: [DDB-TABLE-028](#ddb-table-028), [DDB-TABLE-061](#ddb-table-061), [DDB-TABLE-068](#ddb-table-068), [DDB-TABLE-033](#ddb-table-033), [DDB-TABLE-039](#ddb-table-039), [DDB-TABLE-045](#ddb-table-045),
    [DDB-TABLE-059](#ddb-table-059) · evidence: table/round-trip/full-fields
  - notes: A spec with warmThroughput nil would diff against 12000/4000 (or RCU/WCU) unless ignored when nil.

- <a id="ddb-table-061"></a>**DDB-TABLE-061** `server-default` · impact high · unhandled (not handled in controller) · verified 2026-10-08
  **WarmThroughput on PROVISIONED tables tracks the highest RCU/WCU ever provisioned, never decreases; 12000/4000 after switch to PPR**
  PROVISIONED 1/1 -> WarmThroughput 1/1; after 2/2 -> 2/2; after 3/3 -> 3/3; after decreasing to 1/1 and then
  read 2/write 1 WarmThroughput stayed 3/3. UpdateTable WarmThroughput 1/1 -> ValidationException 'Requested
  ReadUnitsPerSecond for WarmThroughput for table is lower than current WarmThroughput, decreasing
  WarmThroughput is not supported'; 50/50 accepted (TableStatus stays ACTIVE, WarmThroughput.Status=UPDATING).
  After switching to PAY_PER_REQUEST WarmThroughput is 12000/4000 and stays 12000/4000 after switching back to
  PROVISIONED 1/1.
  - ACK: compare.is_ignored+delta_pre_compare, is_read_only, custom_update · ops: UpdateTable, DescribeTable ·
    fields: WarmThroughput.ReadUnitsPerSecond, WarmThroughput.WriteUnitsPerSecond
  - repro: PROVISIONED 1/1 table; UpdateTable PT 2/2, 3/3, 1/1; DescribeTable WarmThroughput after each;
    UpdateTable WarmThroughput 1/1
  - handling: not handled in the controller (as of commit 34b85e6)
  - related: [DDB-TABLE-068](#ddb-table-068), [DDB-TABLE-179](#ddb-table-179), [DDB-TABLE-066](#ddb-table-066), [DDB-TABLE-459](#ddb-table-459), [DDB-TABLE-049](#ddb-table-049), [DDB-TABLE-059](#ddb-table-059),
    [DDB-TABLE-069](table-streams-encryption-class.md#ddb-table-069), [DDB-TABLE-121](#ddb-table-121), [DDB-TABLE-032](#ddb-table-032), [DDB-TABLE-028](#ddb-table-028), [DDB-TABLE-033](#ddb-table-033), [DDB-TABLE-039](#ddb-table-039), [DDB-TABLE-045](#ddb-table-045),
    [DDB-TABLE-058](#ddb-table-058), [DDB-TABLE-060](#ddb-table-060), [DDB-TABLE-370](#ddb-table-370), [DDB-TABLE-057](#ddb-table-057), [DDB-TABLE-063](#ddb-table-063), [DDB-TABLE-164](table-indexes.md#ddb-table-164) · evidence:
    table/mutation-matrix/billing-capacity
  - notes: Any spec.warmThroughput lower than the current server value is unsatisfiable; the controller must
    treat WarmThroughput as monotonic.
  - full notes: [details/DDB-TABLE-061.md](details/DDB-TABLE-061.md)

- <a id="ddb-table-316"></a>**DDB-TABLE-316** `requested-vs-effective` · impact high · unhandled (not handled in controller) · verified 2026-10-09
  **Manual RCU raised inside the bounds on an idle table was scaled back to Min by the already-firing AlarmLow after 82 s**
  UpdateTable RCU=15 (bounds 10-20, read AlarmLow already in ALARM because the table had been idle >15 min) ->
  200; table ACTIVE at RCU 15 after 41 s; at 82 s AAS started 'Setting read capacity units to 10' (cause:
  AlarmLow in state ALARM triggered policy DynamoDBReadCapacityUtilization:table/<name>) and the table was
  ACTIVE at RCU 10 at 92 s (NumberOfDecreasesToday 3). A GetItem trickle (1 per 5 s) was running. The write
  dimension (WCU 5 re-applied by the same UpdateTable) kept AlarmLow in ALARM. After the scale-in all four
  read alarms went INSUFFICIENT_DATA briefly.
  - ACK: compare.is_ignored+delta_pre_compare, docs-only · ops: UpdateTable, DescribeTable · fields:
    ProvisionedThroughput
  - repro: autoscaled read 10-20 -> UpdateTable RCU=15 -> trickle reads -> poll for ~20 min; watch write
    dimension with no traffic
  - measurements: seconds_until_scale_in=82.2, seconds_until_active_at_min=92.4,
    idle_write_scale_in_after_policy_create_s=620
  - handling: not handled in the controller (as of commit 34b85e6)
  - hypotheses: H-R-117, H-R-132 · evidence: table/mutation-matrix/autoscaling-vs-throughput
  - notes: H-R-117 'inside bounds is left alone until an alarm fires' confirmed literally, but on an idle
    table the scale-in alarm is permanently in ALARM, so any manual value above Min is reverted within ~1.5
    min. Combined with the previous finding: on an idle table the only stable manual values are <= Min...
  - full notes: [details/DDB-TABLE-316.md](details/DDB-TABLE-316.md)

## Response fidelity and consistency

- <a id="ddb-table-025"></a>**DDB-TABLE-025** `response-fidelity` · impact high · handled · verified 2026-10-08
  **BillingModeSummary is omitted for PROVISIONED tables, present for PAY_PER_REQUEST tables**
  DescribeTable BillingModeSummary presence by create shape: {"min": false, "ppr": true, "prov-explicit":
  false, "full-prov": false, "full-ppr": true, "sse-false": true, "sse-type-only": true, "sse-awsalias": true,
  "sse-keyid": true, "sse-keyarn": true, "sse-labalias": true, "stream-new": true, "stream-old": true,
  "prov-warm": false}. Raw values: {"min": "<absent>", "ppr": {"BillingMode": "PAY_PER_REQUEST",
  "LastUpdateToPayPerRequestDateTime": "2026-10-08 22:58:10.537000+00:00"}, "prov-explicit": "<absent>",
  "full-prov": "<absent>", "full-ppr": {"BillingMode": "PAY_PER_REQUEST", "LastUpdateToPayPerRequestDateTime":
  "2026-10-08 22:58:10.772000+00:00"}, "sse-false": {"BillingMode": "PAY_PER_REQUEST",
  "LastUpdateToPayPerRequestDateTime": "2026-10-08 22:58:10.849000+00:00"}, "sse-type-only": {"BillingMode":
  "PAY_PER_REQUEST", "LastUpdateToPayPerRequestDateTime": "2026-10-08 22:58:10.879000+00:00"}, "sse-awsalias":
  {"BillingMode": "PAY_PER_.
  - ACK: compare.is_ignored+delta_pre_compare, late_initialize · ops: CreateTable, DescribeTable · fields:
    BillingMode, BillingModeSummary
  - repro: CreateTable (no BillingMode, PT 1/1) and CreateTable(BillingMode=PROVISIONED) and
    CreateTable(PAY_PER_REQUEST); DescribeTable each
  - handling: handled via `generator.yaml:7-9; templates/hooks/table/sdk_read_one_post_set_output.go.tpl:51-55`
  - related: [DDB-TABLE-055](#ddb-table-055), [DDB-TABLE-183](#ddb-table-183), [DDB-TABLE-058](#ddb-table-058), [DDB-TABLE-033](#ddb-table-033), [DDB-TABLE-060](#ddb-table-060), [DDB-TABLE-064](#ddb-table-064) ·
    evidence: table/round-trip/full-fields
  - notes: Hypotheses: H-T-031. H-T-031 create-side part. min/prov-explicit are PROVISIONED
    (implicit/explicit); the post-switch part is in table/mutation-matrix/billing-capacity.

- <a id="ddb-table-033"></a>**DDB-TABLE-033** `response-fidelity` · impact medium · handled · verified 2026-10-08
  **CreateTable response lacks fields DescribeTable adds later: WarmThroughput (unless sent at create) and LastUpdateToPayPerRequestDateTime**
  Keys only in DescribeTable vs the CreateTable response, per shape: {"min": {"only_in_describe":
  ["WarmThroughput"], "only_in_create_response": []}, "ppr": {"only_in_describe": ["WarmThroughput"],
  "only_in_create_response": []}, "prov-explicit": {"only_in_describe": ["WarmThroughput"],
  "only_in_create_response": []}, "full-prov": {"only_in_describe": ["WarmThroughput"],
  "only_in_create_response": []}, "full-ppr": {"only_in_describe": [], "only_in_create_response": []},
  "sse-false": {"only_in_describe": ["WarmThroughput"], "only_in_create_response": []}, "sse-type-only":
  {"only_in_describe": ["WarmThroughput"], "only_in_create_response": []}, "sse-awsalias": {".
  BillingModeSummary in the CreateTable response is {BillingMode} only; DescribeTable adds
  LastUpdateToPayPerRequestDateTime. All tables reached ACTIVE within ~9.49s of CreateTable.
  - ACK: post-create-nudge, late_initialize · ops: CreateTable, DescribeTable · fields: WarmThroughput,
    BillingModeSummary
  - repro: CreateTable (PROVISIONED 1/1); compare response TableDescription keys with DescribeTable keys once
    ACTIVE
  - measurements: create_to_all_active_s=9.49
  - handling: handled via `pkg/resource/table/hooks.go:306; pkg/resource/table/hooks.go:325-335`
  - related: [DDB-TABLE-025](#ddb-table-025), [DDB-TABLE-055](#ddb-table-055), [DDB-TABLE-183](#ddb-table-183), [DDB-TABLE-058](#ddb-table-058), [DDB-TABLE-060](#ddb-table-060), [DDB-TABLE-064](#ddb-table-064),
    [DDB-TABLE-032](#ddb-table-032), [DDB-TABLE-028](#ddb-table-028), [DDB-TABLE-061](#ddb-table-061), [DDB-TABLE-068](#ddb-table-068), [DDB-TABLE-039](#ddb-table-039), [DDB-TABLE-045](#ddb-table-045), [DDB-TABLE-059](#ddb-table-059) ·
    evidence: table/round-trip/full-fields

- <a id="ddb-table-055"></a>**DDB-TABLE-055** `response-fidelity` · impact high · handled · verified 2026-10-08
  **BillingModeSummary after switching: PPR->PROVISIONED and PROVISIONED->PPR (presence, LastUpdateToPayPerRequestDateTime, 0/0 throughput)**
  Table C created PAY_PER_REQUEST: {"TableStatus": "ACTIVE", "BillingModeSummary": {"BillingMode":
  "PAY_PER_REQUEST", "LastUpdateToPayPerRequestDateTime": "2026-10-08 23:09:17.982000+00:00"},
  "ProvisionedThroughput": {"NumberOfDecreasesToday": 0, "ReadCapacityUnits": 0, "WriteCapacityUnits": 0},
  "OnDemandThroughput": "<absent>", "WarmThroughput": {"ReadUnitsPerSecond": 12000, "WriteUnitsPerSecond":
  4000, "Status": "ACTIVE"}, "TableClassSummary": "<absent>"}. After UpdateTable(BillingMode=PROVISIONED, PT
  1/1): {"TableStatus": "ACTIVE", "BillingModeSummary": {"BillingMode": "PROVISIONED",
  "LastUpdateToPayPerRequestDateTime": "2026-10-08 23:09:17.982000+00:00"}, "ProvisionedThroughput":
  {"NumberOfDecreasesToday": 0, "ReadCapacityUnits": 1, "WriteCapacityUnits": 1}, "OnDemandThroughput":
  "<absent>", "WarmThroughput": {"ReadUnitsPerSecond": 12000, "WriteUnitsPerSecond": 4000, "Status":
  "ACTIVE"}, "TableClassSummary": "<absent>"}. Table D created PROVISIONED: {"TableStatus": "ACTIVE",
  "BillingModeSummary": "<absent>", "ProvisionedThroughput": {"NumberOfDecreasesToday": 0,
  "ReadCapacityUnits": 1, "WriteCapacityUnits": 1}, "OnDemandThroughput": "<absent>", "WarmThroughput":
  {"ReadUnitsPerSecond": 1, "WriteUnitsPerSecond": 1, "Status": "ACTIVE"}, "TableClassSummary": "<absent>"}.
  After UpdateTable(BillingMode=PAY_PER_REQUEST): {"TableStatus": "ACTIVE", "BillingModeSummary":
  {"BillingMode": "PAY_PER_REQUEST", "LastUpdateToPayPerRequestDateTime": "2026-10-08 23:11:49.509000+00:00"},
  "ProvisionedThroughput": {"LastIncreaseDateTime": "2026-10-08 23:09:30.195000+00:00",
  "LastDecreaseDateTime": "2026-10-08 23:09:29.159000+00:00", "NumberOfDecreasesToday": 1,
  "ReadCapacityUnits": 0, "WriteCapacityUnits": 0}, "OnDemandThroughput": {"MaxReadRequestUnits": 100,
  "MaxWriteRequestUnits": 100}, "WarmThroughput": {"ReadUnitsPerSecond": 12000, "WriteUnitsPerSecond": 4000,
  "Status": "ACTIVE"}, "TableClassSummary": "<absent>"}.
  - ACK: compare.is_ignored+delta_pre_compare, late_initialize · ops: UpdateTable, DescribeTable · fields:
    BillingMode, BillingModeSummary, ProvisionedThroughput, WarmThroughput
  - repro: CreateTable PAY_PER_REQUEST; UpdateTable BillingMode=PROVISIONED+PT; DescribeTable. CreateTable
    PROVISIONED; UpdateTable BillingMode=PAY_PER_REQUEST; DescribeTable
  - measurements: ppr_to_prov_updating_s=98.19, prov_to_ppr_updating_s=139.64
  - handling: handled via `generator.yaml:7-9; templates/hooks/table/sdk_read_one_post_set_output.go.tpl:51-55`
  - related: [DDB-TABLE-056](#ddb-table-056), [DDB-TABLE-062](#ddb-table-062), [DDB-TABLE-064](#ddb-table-064), [DDB-TABLE-067](#ddb-table-067), [DDB-TABLE-183](#ddb-table-183), [DDB-TABLE-369](table-streams-encryption-class.md#ddb-table-369),
    [DDB-TABLE-452](#ddb-table-452), [DDB-TABLE-433](table-streams-encryption-class.md#ddb-table-433), [DDB-TABLE-025](#ddb-table-025), [DDB-TABLE-058](#ddb-table-058), [DDB-TABLE-033](#ddb-table-033), [DDB-TABLE-060](#ddb-table-060) · evidence:
    table/mutation-matrix/billing-capacity
  - notes: Hypotheses: H-T-031.

- <a id="ddb-table-060"></a>**DDB-TABLE-060** `stale-response` · impact medium · handled · verified 2026-10-08
  **UpdateTable response echoes the OLD ProvisionedThroughput values (with TableStatus=UPDATING and a new LastIncreaseDateTime)**
  UpdateTable ProvisionedThroughput 1/1 -> 2/2 returned TableStatus=UPDATING with
  ProvisionedThroughput{ReadCapacityUnits:1, WriteCapacityUnits:1, LastIncreaseDateTime:<now>}; DescribeTable
  showed 2/2 only after UPDATING ended (~1-2s). Same for 1/1 -> 5/5. Billing-mode switch responses carry
  BillingModeSummary{BillingMode:<new>} without LastUpdateToPayPerRequestDateTime, which DescribeTable adds
  later.
  - ACK: synced.when, requeue, post-create-nudge · ops: UpdateTable, DescribeTable · fields:
    ProvisionedThroughput.ReadCapacityUnits, ProvisionedThroughput.WriteCapacityUnits, BillingModeSummary
  - repro: PROVISIONED 1/1 table; UpdateTable ProvisionedThroughput 2/2; compare response
    TableDescription.ProvisionedThroughput with DescribeTable after ACTIVE
  - measurements: capacity_increase_updating_s=2.0, capacity_decrease_updating_s=1.0
  - handling: handled via `pkg/resource/table/hooks.go:306; pkg/resource/table/hooks.go:325-335`
  - related: [DDB-TABLE-025](#ddb-table-025), [DDB-TABLE-055](#ddb-table-055), [DDB-TABLE-183](#ddb-table-183), [DDB-TABLE-058](#ddb-table-058), [DDB-TABLE-033](#ddb-table-033), [DDB-TABLE-064](#ddb-table-064),
    [DDB-TABLE-370](#ddb-table-370), [DDB-TABLE-057](#ddb-table-057), [DDB-TABLE-061](#ddb-table-061), [DDB-TABLE-063](#ddb-table-063), [DDB-TABLE-164](table-indexes.md#ddb-table-164), [DDB-TABLE-284](table-streams-encryption-class.md#ddb-table-284), [DDB-TABLE-179](#ddb-table-179),
    [DDB-TABLE-066](#ddb-table-066), [DDB-TABLE-371](table-streams-encryption-class.md#ddb-table-371), [DDB-TABLE-140](table-streams-encryption-class.md#ddb-table-140), [DDB-TABLE-180](table-streams-encryption-class.md#ddb-table-180), [DDB-TABLE-177](table-streams-encryption-class.md#ddb-table-177), [DDB-TABLE-156](#ddb-table-156) · evidence:
    table/mutation-matrix/billing-capacity

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
  - related: [DDB-TABLE-051](#ddb-table-051), [DDB-TABLE-178](#ddb-table-178), [DDB-TABLE-053](service.md#ddb-table-053) · evidence:
    table/response-fidelity/odt-minus1-representation, table/creative/reverify-set-b
  - notes: Hypotheses: H-T-121, H-T-039. REFUTES the 'DescribeTable keeps returning -1' claim of both: -1 is
    visible only in the UpdateTable response and in DescribeTable for roughly the first second (eventually
    consistent read), after which the cleared member is omitted ({MaxWriteRequestUnits:500} after...
  - full notes: [details/DDB-TABLE-182.md](details/DDB-TABLE-182.md)

- <a id="ddb-table-452"></a>**DDB-TABLE-452** `stale-response` · impact high · unhandled (not handled in controller) · verified 2026-10-09
  **Reversing an in-flight billing switch: PPR->PROV then 'back to PPR' = 200 but dropped; PROV->PPR then 'back to PROV' = ResourceInUse**
  Table A (created PAY_PER_REQUEST): UpdateTable(PROVISIONED 1/1) -> UPDATING; while UPDATING: +1 s
  BillingMode=PAY_PER_REQUEST -> OK (resp UPDATING/PAY_PER_REQUEST rcu=0); +3 s PROVISIONED 2/2 ->
  ResourceInUseException; +6 s PAY_PER_REQUEST -> OK (resp UPDATING/PAY_PER_REQUEST rcu=0). ACTIVE after 92.23
  s; state 60 s later: {"t_s": 92.23, "status": "ACTIVE", "bm": "PROVISIONED", "bm_ts": "2026-10-09
  05:57:32.214000+00:00", "rcu": 1, "wcu": 1, "warm": "12000/4000"}. Retry PAY_PER_REQUEST once ACTIVE -> OK,
  timeline [{"t_s": 0.04, "status": "UPDATING", "bm": "PAY_PER_REQUEST", "bm_ts": "2026-10-09
  05:57:32.214000+00:00", "rcu": 0, "wcu": 0, "warm": "12000/4000"}, {"t_s": 4.13, "status": "UPDATING", "bm":
  "PAY_PER_REQUEST", "bm_ts": "2026-10-09 06:00:21.474000+00:00", "rcu": 0, "wcu": 0, "warm": "12000/4000"},
  {"t_s": 4.64, "status": "ACTIVE", "bm": "PAY_PER_REQUEST", "bm_ts": "2026-10-09 06:00:21.474000+00:00",
  "rcu": 0, "wcu": 0, "warm": "12000/4000"}]. Table B (created PROVISIONED 1/1): UpdateTable(PAY_PER_REQUEST)
  -> UPDATING; while UPDATING: +1 s PROVISIONED 1/1 -> ResourceInUseException; +3 s PROVISIONED 2/2 ->
  ResourceInUseException; +6 s PROVISIONED 1/1 -> ResourceInUseException. ACTIVE after 128.63 s; state 60 s
  later: {"t_s": 128.63, "status": "ACTIVE", "bm": "PAY_PER_REQUEST", "bm_ts": "2026-10-09
  05:59:52.468000+00:00", "rcu": 0, "wcu": 0, "warm": "12000/4000"}. Retry PROVISIONED 1/1 once ACTIVE -> OK,
  timeline [{"t_s": 0.04, "status": "UPDATING", "bm": "PROVISIONED", "bm_ts": "2026-10-09
  05:59:52.468000+00:00", "rcu": 1, "wcu": 1, "warm": "12000/4000"}, {"t_s": 54.78, "status": "ACTIVE", "bm":
  "PROVISIONED", "bm_ts": "2026-10-09 05:59:52.468000+00:00", "rcu": 1, "wcu": 1, "warm": "12000/4000"}].
  Messages: {"diffpt": "Attempt to change a resource which is still in use: Table IOPS are currently being
  updated. Table: ackq-71fb57-br-b", "rev1": "Attempt to change a resource which is still in use: Table IOPS
  are currently being updated. Table: ackq-71fb57-br-b", "rev2": "Attempt to change a resource which is still
  in use: Table IOPS are currently being updated. Table: ackq-71fb57-br-b"}
  - ACK: synced.when, requeue, custom_update, one-per-reconcile · ops: UpdateTable, DescribeTable · fields:
    BillingMode, ProvisionedThroughput, BillingModeSummary, TableStatus
  - repro: CreateTable PPR; UpdateTable(BillingMode=PROVISIONED 1/1); 1 s later
    UpdateTable(BillingMode=PAY_PER_REQUEST); watch DescribeTable until ACTIVE + 60 s
  - handling: not handled in the controller (as of commit 34b85e6)
  - related: [DDB-TABLE-156](#ddb-table-156), [DDB-TABLE-057](#ddb-table-057), [DDB-TABLE-019](table-streams-encryption-class.md#ddb-table-019), [DDB-TABLE-064](#ddb-table-064), [DDB-TABLE-369](table-streams-encryption-class.md#ddb-table-369), [DDB-TABLE-056](#ddb-table-056),
    [DDB-TABLE-062](#ddb-table-062), [DDB-TABLE-067](#ddb-table-067), [DDB-TABLE-183](#ddb-table-183), [DDB-TABLE-055](#ddb-table-055), [DDB-TABLE-433](table-streams-encryption-class.md#ddb-table-433), [DDB-TABLE-177](table-streams-encryption-class.md#ddb-table-177) · evidence:
    table/creative/billing-reversal-noop
  - notes: Follow-up of table/creative/clobber-matrix where 9/9 PPR->PROVISIONED cells (incl. the no-write
    control) accepted a reverse PAY_PER_REQUEST request at +1 s with 200 and finished PROVISIONED. The
    reversal is evidently classified as a no-op re-send of the still-committed mode ([DDB-TABLE-156](#ddb-table-156)) rather...
  - full notes: [details/DDB-TABLE-452.md](details/DDB-TABLE-452.md)

## Quotas and rate limits

- <a id="ddb-table-056"></a>**DDB-TABLE-056** `quota-limit` · impact high · handled · verified 2026-10-08
  **Billing-mode switch rules: PPR->PROVISIONED requires ProvisionedThroughput; switching back the same day is accepted**
  C (PPR): BillingMode=PROVISIONED alone -> ValidationException: 'One or more parameter values were invalid:
  ProvisionedThroughput must be specified when BillingMode is PROVISIONED'. ProvisionedThroughput alone while
  PPR -> ValidationException: 'One or more parameter values were invalid: Neither ReadCapacityUnits nor
  WriteCapacityUnits can be specified when BillingMode is PAY_PER_REQUEST'. BillingMode=PROVISIONED + PT 1/1
  -> OK (response TableStatus=UPDATING, UPDATING 98.19s). Later BillingMode=PAY_PER_REQUEST (same day) -> OK
  (response TableStatus=UPDATING, UPDATING 4.06s). D (PROVISIONED): BillingMode=PAY_PER_REQUEST + PT ->
  ValidationException: 'One or more parameter values were invalid: ProvisionedThroughput cannot be specified
  when BillingMode is PAY_PER_REQUEST'. + OnDemandThroughput -> OK (response TableStatus=UPDATING, UPDATING
  139.64s). alone -> None: ''. Later BillingMode=PROVISIONED + PT (same day) -> OK (response
  TableStatus=UPDATING, UPDATING 82.96s).
  - ACK: custom_update, terminal_codes, requeue · ops: UpdateTable · fields: BillingMode,
    ProvisionedThroughput, OnDemandThroughput
  - repro: PPR table: UpdateTable BillingMode=PROVISIONED (no PT); then with PT; then
    BillingMode=PAY_PER_REQUEST again
  - handling: handled via `test/e2e/tests/test_table.py:558-575`
  - related: [DDB-TABLE-062](#ddb-table-062), [DDB-TABLE-064](#ddb-table-064), [DDB-TABLE-067](#ddb-table-067), [DDB-TABLE-183](#ddb-table-183), [DDB-TABLE-369](table-streams-encryption-class.md#ddb-table-369), [DDB-TABLE-452](#ddb-table-452),
    [DDB-TABLE-055](#ddb-table-055), [DDB-TABLE-433](table-streams-encryption-class.md#ddb-table-433), [DDB-TABLE-024](#ddb-table-024), [DDB-TABLE-038](#ddb-table-038), [DDB-TABLE-039](#ddb-table-039), [DDB-TABLE-057](#ddb-table-057), [DDB-TABLE-059](#ddb-table-059),
    [DDB-TABLE-018](table-streams-encryption-class.md#ddb-table-018), [DDB-TABLE-156](#ddb-table-156), [DDB-TABLE-358](#ddb-table-358), [DDB-TABLE-174](table-indexes.md#ddb-table-174), [DDB-TABLE-199](table-replicas.md#ddb-table-199), [DDB-TABLE-224](table-replicas.md#ddb-table-224), [DDB-TABLE-162](table-indexes.md#ddb-table-162),
    [DDB-TABLE-127](table-indexes.md#ddb-table-127) · evidence: table/mutation-matrix/billing-capacity
  - notes: Hypotheses: H-T-044.

- <a id="ddb-table-154"></a>**DDB-TABLE-154** `quota-limit` · impact medium · handled · verified 2026-10-08
  **OnDemandThroughput caps: above-quota values are synchronous LimitExceededException; -1 is accepted on UpdateTable and echoed literally**
  UpdateTable OnDemandThroughput MaxReadRequestUnits=40001 (TableMaxReadCapacityUnits=40000) ->
  LimitExceededException 'Subscriber limit exceeded: Requested MaxReadRequestUnits for OnDemandThroughput for
  table exceeds TableMaxReadCapacityUnits of the account in region us-west-2' (2.6 s latency); GSI variant:
  '...for OnDemandThroughput for index : gsi1 exceeds TableMaxReadCapacityUnits...'. Same shape for GSI
  WarmThroughput above the quota ('Requested ReadUnitsPerSecond for WarmThroughput for index gsi1 exceeds
  TableMaxReadCapacityUnits...'). UpdateTable OnDemandThroughput {-1,-1} (documented 'remove cap') returns 200
  and DescribeTable then reports OnDemandThroughput {MaxReadRequestUnits: -1, MaxWriteRequestUnits: -1} (not
  absent); a partial GSI update {MaxReadRequestUnits: 6000} keeps the previous write cap (6000/5000).
  Re-sending an identical table OnDemandThroughput is accepted (200). One UpdateTable in this burst was
  throttled: ThrottlingException 'The rate of control plane requests made by this account is too high'.
  - ACK: terminal_codes, compare.nil_equals_zero_value · ops: UpdateTable, DescribeTable · fields:
    OnDemandThroughput, GlobalSecondaryIndexUpdates.Update.OnDemandThroughput,
    GlobalSecondaryIndexUpdates.Update.WarmThroughput
  - repro: UpdateTable OnDemandThroughput={MaxReadRequestUnits:40001,MaxWriteRequestUnits:1000}; UpdateTable
    OnDemandThroughput={-1,-1}; DescribeTable
  - handling: handled via `generator.yaml:1-6; generator.yaml:15`
  - related: [DDB-TABLE-171](#ddb-table-171), [DDB-TABLE-159](table-indexes.md#ddb-table-159), [DDB-TABLE-157](table-indexes.md#ddb-table-157), [DDB-TABLE-170](table-indexes.md#ddb-table-170), [DDB-TABLE-169](table-indexes.md#ddb-table-169), [DDB-TABLE-135](table-indexes.md#ddb-table-135),
    [DDB-TABLE-134](table-indexes.md#ddb-table-134), [DDB-TABLE-138](table-indexes.md#ddb-table-138), [DDB-TABLE-155](table-indexes.md#ddb-table-155), [DDB-TABLE-153](table-indexes.md#ddb-table-153), [DDB-TABLE-128](table-indexes.md#ddb-table-128), [DDB-TABLE-456](service.md#ddb-table-456), [DDB-TABLE-375](table-indexes.md#ddb-table-375),
    [DDB-TABLE-458](table-indexes.md#ddb-table-458), [DDB-TABLE-286](table-streams-encryption-class.md#ddb-table-286), [DDB-TABLE-164](table-indexes.md#ddb-table-164) · evidence: table/mutation-matrix/gsi-billing-throughput
  - notes: Confirms H-T-122 and H-T-120 (GSI side). -1 must be modelled as a legitimate observed value, not as
    'unset'.

- <a id="ddb-table-183"></a>**DDB-TABLE-183** `quota-limit` · impact medium · handled · verified 2026-10-09
  **BillingMode flipped PROVISIONED->PAY_PER_REQUEST->PROVISIONED->PAY_PER_REQUEST within 4 minutes: no once-per-24h rejection** (hypothesis refuted; behavior confirmed)
  Fresh PROVISIONED 1/1 table (DescribeTable shows no BillingModeSummary at all): UpdateTable
  BillingMode=PAY_PER_REQUEST -> 200, TableStatus UPDATING for 171.6 s; the UpdateTable response echoes
  ProvisionedThroughput 0/0 and a LastDecreaseDateTime stamp, BillingModeSummary {BillingMode:
  PAY_PER_REQUEST} without LastUpdateToPayPerRequestDateTime, which appears only once ACTIVE. Re-sending
  BillingMode=PAY_PER_REQUEST while already PPR -> 200 (no-op, status UPDATING in the response). UpdateTable
  back to PROVISIONED 1/1 two minutes later -> 200, UPDATING 60.5 s, BillingModeSummary keeps
  LastUpdateToPayPerRequestDateTime of the earlier switch. A second switch to PAY_PER_REQUEST one minute after
  that -> 200 again. No LimitExceededException or ValidationException at any point.
  - ACK: none, e2e-timing · ops: UpdateTable, DescribeTable · fields: BillingMode,
    BillingModeSummary.LastUpdateToPayPerRequestDateTime, ProvisionedThroughput
  - repro: CreateTable PROVISIONED 1/1; UpdateTable PAY_PER_REQUEST; wait ACTIVE; UpdateTable PROVISIONED 1/1;
    wait ACTIVE; UpdateTable PAY_PER_REQUEST
  - measurements: to_ppr_updating_s=171.61, back_to_provisioned_updating_s=60.53
  - handling: handled via `generator.yaml:7-9; templates/hooks/table/sdk_read_one_post_set_output.go.tpl:51-55; test/e2e/tests/test_table.py:558-575; test/e2e/tests/test_table.py:37-42; test/e2e/tests/test_table.py:544-556`
  - related: [DDB-TABLE-067](#ddb-table-067), [DDB-TABLE-056](#ddb-table-056), [DDB-TABLE-062](#ddb-table-062), [DDB-TABLE-064](#ddb-table-064), [DDB-TABLE-369](table-streams-encryption-class.md#ddb-table-369), [DDB-TABLE-452](#ddb-table-452),
    [DDB-TABLE-055](#ddb-table-055), [DDB-TABLE-433](table-streams-encryption-class.md#ddb-table-433), [DDB-TABLE-177](table-streams-encryption-class.md#ddb-table-177), [DDB-TABLE-019](table-streams-encryption-class.md#ddb-table-019), [DDB-TABLE-156](#ddb-table-156), [DDB-TABLE-057](#ddb-table-057), [DDB-TABLE-018](table-streams-encryption-class.md#ddb-table-018),
    [DDB-TABLE-365](table-streams-encryption-class.md#ddb-table-365), [DDB-TABLE-358](#ddb-table-358), [DDB-TABLE-283](table-streams-encryption-class.md#ddb-table-283), [DDB-TABLE-025](#ddb-table-025), [DDB-TABLE-058](#ddb-table-058), [DDB-TABLE-033](#ddb-table-033), [DDB-TABLE-060](#ddb-table-060),
    [DDB-TABLE-370](#ddb-table-370), [DDB-TABLE-284](table-streams-encryption-class.md#ddb-table-284), [DDB-TABLE-179](#ddb-table-179), [DDB-TABLE-066](#ddb-table-066), [DDB-TABLE-371](table-streams-encryption-class.md#ddb-table-371), [DDB-TABLE-140](table-streams-encryption-class.md#ddb-table-140), [DDB-TABLE-180](table-streams-encryption-class.md#ddb-table-180) ·
    evidence: table/limits/provisioned-decrease-billing-flip
  - notes: Refutes H-T-046 (the once-per-24h billing-mode switch rule is not enforced in this account/region
    as of this run). The expensive part is the duration: switching to PAY_PER_REQUEST kept the table UPDATING
    for ~3 minutes versus ~2 s for a throughput change, so a controller flip-flopping billing mode...
  - full notes: [details/DDB-TABLE-183.md](details/DDB-TABLE-183.md)

- <a id="ddb-table-184"></a>**DDB-TABLE-184** `quota-limit` · impact high · handled · verified 2026-10-09
  **Provisioned decreases: 4 accepted back-to-back, 5th -> LimitExceededException naming the next allowed time; counted per call/table**
  Fresh PROVISIONED 10/10 table: ProvisionedThroughput has NumberOfDecreasesToday=0 and no
  LastDecreaseDateTime / LastIncreaseDateTime members. Four decreases (10/10->9/9 both dimensions, then WCU
  9->8->7->6) issued ~2 s apart, each waiting ACTIVE (UPDATING 2.0 s each), were accepted; the UpdateTable
  response reports the OLD units and the OLD NumberOfDecreasesToday (0,1,2,3) but a NEW LastDecreaseDateTime,
  while DescribeTable after ACTIVE shows the new units and the counter 1,2,3,4. The 5th decrease ->
  LimitExceededException HTTP 400 "Subscriber limit exceeded: Provisioned throughput decreases are limited
  within a given UTC day. After the first 4 decreases, each subsequent decrease in the same UTC day can be
  performed at most once every 3600 seconds. Number of decreases today: 4. Last decrease at Thursday, October
  8, 2026 at 11:20:48 PM Coordinated Universal Time. Next decrease can be made at Friday, October 9, 2026 at
  12:00:00 AM Coordinated Universal Time." An immediate retry and a mixed RCU-down/WCU-up request get the same
  error; an increase (both dimensions up) is accepted. Decreasing both RCU and WCU in one call counted as one
  decrease. Another table (separate probe) was unaffected: the budget is per table.
  - ACK: terminal_codes, requeue, one-per-reconcile · ops: UpdateTable, DescribeTable · fields:
    ProvisionedThroughput, ProvisionedThroughput.NumberOfDecreasesToday,
    ProvisionedThroughput.LastDecreaseDateTime, ProvisionedThroughput.LastIncreaseDateTime
  - repro: CreateTable PROVISIONED 10/10; UpdateTable decreasing WCU by 1 five times, waiting ACTIVE between
    calls
  - measurements: updating_s_per_decrease=2.02, decreases_before_reject=4
  - handling: handled via `pkg/resource/table/hooks.go:81-84; pkg/resource/table/hooks.go:198-202; pkg/resource/table/hooks.go:306; pkg/resource/table/hooks.go:325-335; generator.yaml:88-90; pkg/resource/table/sdk.go:1234-1250`
  - related: [DDB-TABLE-185](#ddb-table-185), [DDB-TABLE-326](#ddb-table-326), [DDB-TABLE-381](#ddb-table-381), [DDB-TABLE-063](#ddb-table-063), [DDB-TABLE-104](table.md#ddb-table-104) · evidence:
    table/limits/provisioned-decrease-billing-flip
  - notes: Confirms H-T-047 and the first half of H-T-113 (4 immediate decreases, then one per 3600 s,
    next-allowed time in the message). The message is the only machine-readable source of the next-allowed
    timestamp; NumberOfDecreasesToday in the UpdateTable response lags by one (stale-response), so a...
  - full notes: [details/DDB-TABLE-184.md](details/DDB-TABLE-184.md)

- <a id="ddb-table-185"></a>**DDB-TABLE-185** `quota-limit` · impact medium · unhandled (not handled in controller) · verified 2026-10-09
  **NumberOfDecreasesToday resets to 0 exactly at 00:00:00 UTC (not 24 h after the first decrease); LastDecreaseDateTime is kept**
  With the daily budget exhausted (NumberOfDecreasesToday=4, 5th decrease rejected at 23:20 UTC),
  DescribeTable sampled every 5-10 s showed 4 through 23:59:55 UTC and 0 at the 00:00:00 UTC sample;
  LastDecreaseDateTime (23:20:48) and LastIncreaseDateTime were NOT cleared by the reset. Three further
  decreases at 00:00:46, 00:00:48 and 00:00:50 UTC were all accepted (counter 1,2,3), i.e. the full budget of
  4 immediate decreases is restored at midnight UTC, 40 minutes after the quota had been exhausted; the error
  message had correctly announced "Next decrease can be made at ... 12:00:00 AM Coordinated Universal Time"
  rather than the hourly 3600 s refill.
  - ACK: requeue, terminal_codes · ops: UpdateTable, DescribeTable · fields:
    ProvisionedThroughput.NumberOfDecreasesToday, ProvisionedThroughput.LastDecreaseDateTime
  - repro: Exhaust 4 decreases before midnight UTC; DescribeTable every 5 s across 00:00:00 UTC; decrease
    three more times
  - measurements: reset_observed_at_utc_s_after_midnight=0, post_reset_decreases_accepted=3
  - handling: not handled in the controller (as of commit 34b85e6)
  - related: [DDB-TABLE-184](#ddb-table-184), [DDB-TABLE-326](#ddb-table-326), [DDB-TABLE-381](#ddb-table-381) · evidence:
    table/limits/provisioned-decrease-billing-flip
  - notes: Confirms the UTC-midnight reset and the never-cleared LastDecreaseDateTime parts of H-T-113. The
    hourly refill (one extra decrease at the top of the next hour) could not be observed separately because
    the first boundary after exhaustion was midnight, which restores the full budget. A controller can...
  - full notes: [details/DDB-TABLE-185.md](details/DDB-TABLE-185.md)

- <a id="ddb-table-326"></a>**DDB-TABLE-326** `quota-limit` · impact high · handled · verified 2026-10-09
  **After 4 decreases, exactly one more is admitted 3600 s after the last one (not at the top of the hour); 6th -> LimitExceeded again**
  PROVISIONED 10/10 table, RCU 10->9->8->7->6 in 4 calls (UPDATING 1-2 s each). 5th decrease ->
  LimitExceededException HTTP 400: 'Subscriber limit exceeded: Provisioned throughput decreases are limited
  within a given UTC day. After the first 4 decreases, each subsequent decrease in the same UTC day can be
  performed at most once every 3600 seconds. Number of decreases today: 4. Last decrease at Friday, October 9,
  2026 at 12:21:34 AM Coordinated Universal Time. Next decrease can be made at Friday, October 9, 2026 at
  1:21:34 AM Coordinated Universal Time'. The same decrease retried at T_last+5,10,...,55 min, at the top of
  the hour (01:00:20), and every minute from +56 min: all LimitExceededException up to T_last+3540 s; admitted
  at T_last+3600 s (01:21:34). NumberOfDecreasesToday then 5, LastDecreaseDateTime moved to 01:21:36. Failed
  attempts never moved NumberOfDecreasesToday or LastDecreaseDateTime. Immediately after the admitted decrease
  a further decrease -> LimitExceededException ('Number of decreases today: 5 ... Next decrease can be made at
  ... 2:21:36 AM'), still rejected 5 min later: one token per 3600 s measured from the LAST ACCEPTED decrease,
  no bucket refill.
  - ACK: requeue, terminal_codes, custom_update · ops: UpdateTable, DescribeTable · fields:
    ProvisionedThroughput, ProvisionedThroughput.NumberOfDecreasesToday,
    ProvisionedThroughput.LastDecreaseDateTime
  - repro: PROVISIONED table; 5 consecutive RCU decreases; retry the 5th every 5 min then every minute from
    +56 min; on success retry immediately
  - measurements: refill_after_last_decrease_s=3600.1, fifth_rejection_latency_ms=18,
    last_failed_attempt_since_last_decrease_s=3540.1
  - handling: handled via `pkg/resource/table/hooks.go:81-84; pkg/resource/table/hooks.go:198-202; generator.yaml:88-90; pkg/resource/table/sdk.go:1234-1250`
  - related: [DDB-TABLE-184](#ddb-table-184), [DDB-TABLE-185](#ddb-table-185), [DDB-TABLE-381](#ddb-table-381) · hypotheses: H-T-113, H-T-047 · evidence:
    table/limits/decrease-hourly-refill
  - notes: Confirms the hourly-refill half of H-T-113 as 'once per 3600 s after the last accepted decrease'
    (the 4-then-LimitExceeded half and the 00:00 UTC reset were measured in
    table/limits/provisioned-decrease-billing-flip). The next-allowed time is only available by parsing the
    message ('Next decrease can...
  - full notes: [details/DDB-TABLE-326.md](details/DDB-TABLE-326.md)

- <a id="ddb-table-381"></a>**DDB-TABLE-381** `quota-limit` · impact medium · unhandled (not handled in controller) · verified 2026-10-09
  **Two writers racing full ProvisionedThroughput structs: loser gets ResourceInUseException; its stale retry is a budget-burning decrease**
  PROVISIONED table at 6/6 with NumberOfDecreasesToday=4. Writer A: UpdateTable ProvisionedThroughput {20,6}
  -> 200 (TableStatus UPDATING ~1.0 s). Writer B 0.3 s later: {6,20} -> ResourceInUseException 'Attempt to
  change a resource which is still in use: Table IOPS are currently being updated. Table: <name>'. Final state
  {20,6}. B's retry after ACTIVE with the same stale struct -> LimitExceededException 'Provisioned throughput
  decreases are limited within a given UTC day ... Number of decreases today: 4' because re-sending the stale
  RCU=6 is a decrease of A's 20. With budget left, the retry would have silently reverted A's change.
  - ACK: requeue, custom_update · ops: UpdateTable · fields: ProvisionedThroughput
  - repro: PROVISIONED table; thread A UpdateTable PT {20,w}; thread B at +0.3 s UpdateTable PT {r,20}; wait
    ACTIVE; B re-sends
  - measurements: writer_b_delay_s=0.3, updating_window_s=1.01
  - handling: not handled in the controller (as of commit 34b85e6)
  - related: [DDB-TABLE-184](#ddb-table-184), [DDB-TABLE-163](table-subresources.md#ddb-table-163), [DDB-TABLE-006](table.md#ddb-table-006), [DDB-TABLE-063](#ddb-table-063), [DDB-TABLE-103](service.md#ddb-table-103), [DDB-TABLE-005](table.md#ddb-table-005),
    [DDB-TABLE-101](service.md#ddb-table-101), [DDB-TABLE-104](table.md#ddb-table-104), [DDB-TABLE-185](#ddb-table-185), [DDB-TABLE-326](#ddb-table-326) · evidence: table/creative/update-atomicity
  - notes: There is no optimistic concurrency on UpdateTable (no revision/ETag); the only protection is the
    ~1-2 s UPDATING window. Because ProvisionedThroughput must carry both RCU and WCU, a controller that
    computes the struct from a stale read and retries after ResourceInUseException will overwrite the...
  - full notes: [details/DDB-TABLE-381.md](details/DDB-TABLE-381.md)

- <a id="ddb-table-393"></a>**DDB-TABLE-393** `quota-limit` · impact medium · unhandled (not handled in controller) · verified 2026-10-09
  **Doc claim C015 PARTLY: 'the only quota is the aggregate account capacity' - the per-table/per-index 40k cap is enforced too**
  CreateTable/UpdateTable with 40001 RCU on one table fails with LimitExceededException 'above the per table
  maximum for the account ... Per table maximum: 40000' although the account aggregate (80000) has room; the
  per-index cap is checked separately and GSI capacity is not aggregated into the table cap ([DDB-TABLE-170](table-indexes.md#ddb-table-170);
  quotas via DescribeLimits in [DDB-TABLE-131](table.md#ddb-table-131)). Back-to-back provisioned increases (1->2->3, 1->5) were
  accepted with ~1-2 s UPDATING each ([DDB-TABLE-058](#ddb-table-058)).
  - ACK: terminal_codes · ops: UpdateTable, CreateTable, DescribeLimits · fields: ProvisionedThroughput
  - handling: not handled in the controller (as of commit 34b85e6)
  - related: [DDB-TABLE-170](table-indexes.md#ddb-table-170), [DDB-TABLE-131](table.md#ddb-table-131), [DDB-TABLE-058](#ddb-table-058) · evidence: table/limits/gsi-concurrency-quotas,
    table/limits/rate-bursts, service/static/doc-claims-1
  - notes: VERDICT: PARTLY - the aggregate account quota is not the only one: TableMaxRead/WriteCapacityUnits
    applies per table and per index; no rapid-increase cooldown was seen for small steps

## Handling gaps (bugs to file)

None recorded for this document's findings; see [service.md 'Handling gaps summary'](service.md#handling-gaps-summary) for the service-wide list.

## E2E timing

Values are seconds unless the key says otherwise; n = trials behind the numbers ('1 run' when the finding records none).

| finding | what | measurements | n |
| --- | --- | --- | --- |
| [DDB-TABLE-033](#ddb-table-033) | CreateTable response lacks fields DescribeTable adds later: WarmThroughput (unless sent at create) and LastUpdateToPayPerRequestDateTime | create_to_all_active_s=9.49 | 1 run |
| [DDB-TABLE-055](#ddb-table-055) | BillingModeSummary after switching: PPR->PROVISIONED and PROVISIONED->PPR (presence, LastUpdateToPayPerRequestDateTime, 0/0 throughput) | ppr_to_prov_updating_s=98.19, prov_to_ppr_updating_s=139.64 | 1 run |
| [DDB-TABLE-058](#ddb-table-058) | Provisioned capacity up/down: UPDATING durations, NumberOfDecreasesToday/LastDecreaseDateTime bookkeeping, partial and zero values | d-pt-up-2-2=2.02, d-billing-prov-plus-pt-up-3-3=2.02, d-pt-down-1-1=1.01, d-pt-mixed-read-up-write-down=1.01, c-pt-up-5-5=1.01, c-pt-down-2-2=2.02 | 1 run |
| [DDB-TABLE-060](#ddb-table-060) | UpdateTable response echoes the OLD ProvisionedThroughput values (with TableStatus=UPDATING and a new LastIncreaseDateTime) | capacity_increase_updating_s=2.0, capacity_decrease_updating_s=1.0 | 1 run |
| [DDB-TABLE-063](#ddb-table-063) | DeleteTable while UPDATING (throughput change / billing switch) -> ResourceInUseException; UPDATING ~2s / ~129s | throughput_updating_s=2.03, creating_duration_s_provisioned=4.05 | 1 run |
| [DDB-TABLE-064](#ddb-table-064) | PROVISIONED->PAY_PER_REQUEST keeps the table UPDATING ~129s while BillingModeSummary already says PAY_PER_REQUEST | billing_updating_s=128.78 | 1 run |
| [DDB-TABLE-179](#ddb-table-179) | WarmThroughput increase is async (~6.5 min) with TableStatus ACTIVE: only WarmThroughput.Status=UPDATING signals it; decrease rejected | warm_increase_completion_s=388.8, warm_increase_completion_s_first_run=500.9, warm_partial_write_increase_completion_s=2.0 | 1 run |
| [DDB-TABLE-182](#ddb-table-182) | Clearing OnDemandThroughput with -1: -1 visible ~1s in DescribeTable, then the member is omitted; struct absent when both cleared | minus1_visible_in_describe_s=1.0 | 1 run |
| [DDB-TABLE-183](#ddb-table-183) | BillingMode flipped PROVISIONED->PAY_PER_REQUEST->PROVISIONED->PAY_PER_REQUEST within 4 minutes: no once-per-24h rejection | to_ppr_updating_s=171.61, back_to_provisioned_updating_s=60.53 | 1 run |
| [DDB-TABLE-184](#ddb-table-184) | Provisioned decreases: 4 accepted back-to-back, 5th -> LimitExceededException naming the next allowed time; counted per call/table | updating_s_per_decrease=2.02, decreases_before_reject=4 | 1 run |
| [DDB-TABLE-185](#ddb-table-185) | NumberOfDecreasesToday resets to 0 exactly at 00:00:00 UTC (not 24 h after the first decrease); LastDecreaseDateTime is kept | reset_observed_at_utc_s_after_midnight=0, post_reset_decreases_accepted=3 | 1 run |
| [DDB-TABLE-316](#ddb-table-316) | Manual RCU raised inside the bounds on an idle table was scaled back to Min by the already-firing AlarmLow after 82 s | seconds_until_scale_in=82.2, seconds_until_active_at_min=92.4, idle_write_scale_in_after_policy_create_s=620 | 1 run |
| [DDB-TABLE-326](#ddb-table-326) | After 4 decreases, exactly one more is admitted 3600 s after the last one (not at the top of the hour); 6th -> LimitExceeded again | refill_after_last_decrease_s=3600.1, fifth_rejection_latency_ms=18, last_failed_attempt_since_last_decrease_s=3540.1 | 1 run |
| [DDB-TABLE-370](#ddb-table-370) | ProvisionedThroughput-only UPDATING lasts ~1.3s; a second PT change / billing switch / DeleteTable inside it -> ResourceInUseException | pt_updating_s_trial1=null, pt_updating_s_trial2=1.3 | 1 run |
| [DDB-TABLE-381](#ddb-table-381) | Two writers racing full ProvisionedThroughput structs: loser gets ResourceInUseException; its stale retry is a budget-burning decrease | writer_b_delay_s=0.3, updating_window_s=1.01 | 1 run |
| [DDB-TABLE-437](#ddb-table-437) | Empty-struct UpdateTable members: WarmThroughput {} and OnDemandThroughput {} -> HTTP 500 InternalFailure... | sse_enabled_only_reencrypt_s=22.2 | 1 run |
| [DDB-TABLE-459](#ddb-table-459) | WarmThroughput job is last-writer-wins: a higher request 1 s into a running Warm update is accepted and applied; no write resets its Status | warm_updating_s=[441.46, 451.46, 473.59, 493.76, 513.88] | 1 run |

## Open questions

- [DDB-TABLE-390](#ddb-table-390) (unverified) - Doc claim C007 UNTESTABLE: OnDemandThroughput 'sets the maximum read/write
  units' - the cap is data-plane; only accept/echo is testable: VERDICT: UNTESTABLE - enforcement of the cap
  is data-plane; control-plane acceptance/echo is confirmed by the related findings
- [DDB-TABLE-423](#ddb-table-423) (unverified) - Doc claim C056 UNTESTABLE: ReadCapacityUnits = max strongly consistent reads/s
  before DynamoDB returns ThrottlingException: VERDICT: UNTESTABLE - data-plane throttling behavior; not
  exercised (the lab stays on the control plane)
- [DDB-TABLE-424](#ddb-table-424) (unverified) - Doc claim C057 UNTESTABLE: WriteCapacityUnits = max writes/s before DynamoDB
  returns ThrottlingException: VERDICT: UNTESTABLE - data-plane throttling behavior; not exercised
- [DDB-TABLE-425](#ddb-table-425) (unverified) - Doc claim C058 UNTESTABLE: 50 RCU provide 100 eventually consistent reads per
  second: VERDICT: UNTESTABLE - data-plane capacity accounting; not exercised

<!-- preserved:start id=open-questions -->
<!-- open questions and follow-up experiments; survives re-renders -->
<!-- preserved:end -->

## Appendix: low-impact and duplicate findings

| id | category | impact | status | title | related | duplicate_of |
| --- | --- | --- | --- | --- | --- | --- |
| <a id="ddb-table-040"></a>**DDB-TABLE-040** | request-validation | low | confirmed | Enum values are case-sensitive; unknown/lowercase values fail CreateTable with ValidationException (all table enums) | [DDB-TABLE-050](table-streams-encryption-class.md#ddb-table-050), [DDB-TABLE-035](table-streams-encryption-class.md#ddb-table-035), [DDB-TABLE-018](table-streams-encryption-class.md#ddb-table-018), [DDB-TABLE-002](table-streams-encryption-class.md#ddb-table-002), [DDB-TABLE-029](table-streams-encryption-class.md#ddb-table-029), [DDB-TABLE-036](table-streams-encryption-class.md#ddb-table-036) | - |
| <a id="ddb-table-045"></a>**DDB-TABLE-045** | response-fidelity | medium | confirmed | Partial OnDemandThroughput/WarmThroughput structs are accepted at CreateTable and echoed partially (single member) in DescribeTable | [DDB-TABLE-032](#ddb-table-032), [DDB-TABLE-028](#ddb-table-028), [DDB-TABLE-061](#ddb-table-061), [DDB-TABLE-068](#ddb-table-068), [DDB-TABLE-033](#ddb-table-033), [DDB-TABLE-039](#ddb-table-039), [DDB-TABLE-059](#ddb-table-059) | [DDB-TABLE-039](#ddb-table-039) |
| <a id="ddb-table-062"></a>**DDB-TABLE-062** | async-state-machine | medium | confirmed | Billing-mode switch durations are asymmetric and a same-day reverse switch was accepted (no 24h quota error observed) | [DDB-TABLE-056](#ddb-table-056), [DDB-TABLE-064](#ddb-table-064), [DDB-TABLE-067](#ddb-table-067), [DDB-TABLE-183](#ddb-table-183), [DDB-TABLE-369](table-streams-encryption-class.md#ddb-table-369), [DDB-TABLE-452](#ddb-table-452), [DDB-TABLE-055](#ddb-table-055), [DDB-TABLE-433](table-streams-encryption-class.md#ddb-table-433) | [DDB-TABLE-056](#ddb-table-056) |
| <a id="ddb-table-066"></a>**DDB-TABLE-066** | async-state-machine | high | confirmed | WarmThroughput increase: response shows OLD numbers + Status=UPDATING, TableStatus stays ACTIVE ~272s; equal values accepted | [DDB-TABLE-179](#ddb-table-179), [DDB-TABLE-459](#ddb-table-459), [DDB-TABLE-049](#ddb-table-049), [DDB-TABLE-061](#ddb-table-061), [DDB-TABLE-059](#ddb-table-059), [DDB-TABLE-069](table-streams-encryption-class.md#ddb-table-069), [DDB-TABLE-121](#ddb-table-121), [DDB-TABLE-120](table-streams-encryption-class.md#ddb-table-120), [DDB-TABLE-065](table-streams-encryption-class.md#ddb-table-065), [DDB-TABLE-002](table-streams-encryption-class.md#ddb-table-002), [DDB-TABLE-284](table-streams-encryption-class.md#ddb-table-284), [DDB-TABLE-369](table-streams-encryption-class.md#ddb-table-369), [DDB-TABLE-370](#ddb-table-370), [DDB-TABLE-001](table.md#ddb-table-001), [DDB-TABLE-334](table-streams-encryption-class.md#ddb-table-334), [DDB-TABLE-060](#ddb-table-060), [DDB-TABLE-371](table-streams-encryption-class.md#ddb-table-371), [DDB-TABLE-140](table-streams-encryption-class.md#ddb-table-140), [DDB-TABLE-064](#ddb-table-064), [DDB-TABLE-183](#ddb-table-183), [DDB-TABLE-180](table-streams-encryption-class.md#ddb-table-180), [DDB-TABLE-177](table-streams-encryption-class.md#ddb-table-177), [DDB-TABLE-156](#ddb-table-156) | [DDB-TABLE-179](#ddb-table-179) |
| <a id="ddb-table-067"></a>**DDB-TABLE-067** | quota-limit | low | confirmed | Switching BillingMode back and forth within minutes is accepted (no 24h cooldown observed on a fresh table) | [DDB-TABLE-056](#ddb-table-056), [DDB-TABLE-062](#ddb-table-062), [DDB-TABLE-064](#ddb-table-064), [DDB-TABLE-183](#ddb-table-183), [DDB-TABLE-369](table-streams-encryption-class.md#ddb-table-369), [DDB-TABLE-452](#ddb-table-452), [DDB-TABLE-055](#ddb-table-055), [DDB-TABLE-433](table-streams-encryption-class.md#ddb-table-433) | [DDB-TABLE-183](#ddb-table-183) |
| <a id="ddb-table-068"></a>**DDB-TABLE-068** | server-default | medium | confirmed | WarmThroughput on a PROVISIONED table mirrors its ProvisionedThroughput (1/1) and jumps to 12000/4000 on switch to PAY_PER_REQUEST | [DDB-TABLE-032](#ddb-table-032), [DDB-TABLE-028](#ddb-table-028), [DDB-TABLE-061](#ddb-table-061), [DDB-TABLE-033](#ddb-table-033), [DDB-TABLE-039](#ddb-table-039), [DDB-TABLE-045](#ddb-table-045), [DDB-TABLE-059](#ddb-table-059) | [DDB-TABLE-061](#ddb-table-061) |
| <a id="ddb-table-363"></a>**DDB-TABLE-363** | shape-mismatch | low | confirmed | DeleteTable returns a reduced TableDescription (no KeySchema, AttributeDefinitions, indexes, CreationDateTime, WarmThroughput) | [DDB-TABLE-006](table.md#ddb-table-006), [DDB-TABLE-004](table-streams-encryption-class.md#ddb-table-004), [DDB-TABLE-138](table-indexes.md#ddb-table-138), [DDB-TABLE-134](table-indexes.md#ddb-table-134) | - |
| <a id="ddb-table-364"></a>**DDB-TABLE-364** | normalization | low | confirmed | Key attribute names are not normalized: whitespace, newline, tab, emoji, dots and 255 chars round-trip byte-identically | [DDB-TABLE-041](service.md#ddb-table-041), [DDB-TABLE-042](#ddb-table-042), [DDB-TABLE-125](table-indexes.md#ddb-table-125) | - |
| <a id="ddb-table-390"></a>**DDB-TABLE-390** | other | low | unverified | Doc claim C007 UNTESTABLE: OnDemandThroughput 'sets the maximum read/write units' - the cap is data-plane; only accept/echo is testable | [DDB-TABLE-028](#ddb-table-028), [DDB-TABLE-045](#ddb-table-045), [DDB-TABLE-154](#ddb-table-154), [DDB-TABLE-128](table-indexes.md#ddb-table-128) | - |
| <a id="ddb-table-411"></a>**DDB-TABLE-411** | other | low | confirmed | Doc claim C043 TRUE: Switching PAY_PER_REQUEST -> PROVISIONED requires initial provisioned capacity values | [DDB-TABLE-056](#ddb-table-056), [DDB-TABLE-152](table-indexes.md#ddb-table-152) | - |
| <a id="ddb-table-412"></a>**DDB-TABLE-412** | request-validation | low | confirmed | Doc claim C044 FALSE: Initial provisioned capacity is 'estimated from the last 30 minutes of consumption' on a PPR->PROVISIONED switch | [DDB-TABLE-056](#ddb-table-056), [DDB-TABLE-055](#ddb-table-055) | - |
| <a id="ddb-table-415"></a>**DDB-TABLE-415** | other | low | confirmed | Doc claim C047 TRUE: UpdateTable OnDemandThroughput updates the table's on-demand read/write maxima | [DDB-TABLE-051](#ddb-table-051), [DDB-TABLE-178](#ddb-table-178), [DDB-TABLE-154](#ddb-table-154), [DDB-TABLE-135](table-indexes.md#ddb-table-135), [DDB-TABLE-182](#ddb-table-182) | - |
| <a id="ddb-table-420"></a>**DDB-TABLE-420** | other | low | confirmed | Doc claim C053 TRUE: OnDemandThroughput.MaxReadRequestUnits is the table's maximum read request units | [DDB-TABLE-045](#ddb-table-045), [DDB-TABLE-178](#ddb-table-178), [DDB-TABLE-154](#ddb-table-154) | - |
| <a id="ddb-table-421"></a>**DDB-TABLE-421** | other | low | confirmed | Doc claim C054 TRUE: OnDemandThroughput.MaxWriteRequestUnits is the table's maximum write request units | [DDB-TABLE-051](#ddb-table-051), [DDB-TABLE-178](#ddb-table-178), [DDB-TABLE-182](#ddb-table-182) | - |
| <a id="ddb-table-423"></a>**DDB-TABLE-423** | other | low | unverified | Doc claim C056 UNTESTABLE: ReadCapacityUnits = max strongly consistent reads/s before DynamoDB returns ThrottlingException | - | - |
| <a id="ddb-table-424"></a>**DDB-TABLE-424** | other | low | unverified | Doc claim C057 UNTESTABLE: WriteCapacityUnits = max writes/s before DynamoDB returns ThrottlingException | - | - |
| <a id="ddb-table-425"></a>**DDB-TABLE-425** | other | low | unverified | Doc claim C058 UNTESTABLE: 50 RCU provide 100 eventually consistent reads per second | - | - |
| <a id="ddb-table-429"></a>**DDB-TABLE-429** | other | low | confirmed | Doc claim C062 TRUE: TableDescription.OnDemandThroughput reports the on-demand table's read/write maxima | [DDB-TABLE-028](#ddb-table-028), [DDB-TABLE-045](#ddb-table-045), [DDB-TABLE-182](#ddb-table-182), [DDB-TABLE-153](table-indexes.md#ddb-table-153) | - |

## Supplementary notes

<!-- preserved:start -->
<!-- preserved:end -->
