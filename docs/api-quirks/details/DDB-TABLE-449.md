<!-- generated: ack-api-quirks dynamodb 2026-10-09 model=2012-08-10 controller=34b85e6 -->
# DDB-TABLE-449: Error texts are identical in us-west-2 and us-east-1 after removing resource names/timestamps (58/58 triggers)...
_Full entry and notes of one finding; its summary entry is in [service.md](../service.md). Generated from
ack-api-quirks `services/dynamodb` (model 2012-08-10, controller commit 34b85e6); edit the finding in the lab,
not here._

## Finding

- <a id="ddb-table-449"></a>**DDB-TABLE-449** `error-code` · impact high · unhandled (not handled in controller) · verified 2026-10-09
  **Error texts are identical in us-west-2 and us-east-1 after removing resource names/timestamps (58/58 triggers)...**
  The same 58 error triggers were fired in us-west-2 and us-east-1: 58/58 returned the same code AND the same
  text after normalising table names/ARNs/timestamps (differences, if any, listed in notes). Request-specific
  noise found in otherwise stable texts: resource names and ARNs (most texts), ISO-8601 millisecond timestamps
  (DP and policy 15 s cooldowns, SSE 24h window, system-backup expiry, PITR window), 52-char request ids
  (Java-SDK suffix on global-table errors), region names (quota texts), and the UpdateTable 'At least one of
  ... is required' list, which omits whichever listed member the request already contained
  (MultiRegionConsistency disappears when it was sent) and names three members that are not in the public SDK
  model (MultiAccountReplicaReady, ReplicaTransitRoleArn, UpdateStreamEnabled). Safe strategy: match on a
  stable prefix (or a short invariant phrase), never on the full text or suffix.
  - ACK: terminal_codes, requeue · ops: UpdateTable, DescribeTable, TagResource, PutResourcePolicy,
    UpdateTimeToLive, CreateTable
  - repro: see probe.py: same trigger list against one table per region; compare after norm()
  - handling: not handled in the controller (as of commit 34b85e6)
  - related: [DDB-TABLE-015](../table-indexes.md#ddb-table-015), [DDB-TABLE-047](../table-throughput-billing.md#ddb-table-047), [DDB-TABLE-160](../table-indexes.md#ddb-table-160), [DDB-TABLE-437](../table-throughput-billing.md#ddb-table-437) · evidence:
    table/creative/error-message-regions

## Notes

Per-label comparison (code, http, raw_identical, same_after_norm, noise, latency) is in result.yaml
region_compare; differences: {}. Required-list texts: {"us-west-2": {"only_name": "At least one of
ProvisionedThroughput, BillingMode, UpdateStreamEnabled, GlobalSecondaryIndexUpdates, SSESpecification,
ReplicaUpdates, MultiAccountReplicaReady, ReplicaTransitRoleArn, MultiRegionConsistency,
DeletionProtectionEnabled, OnDemandThroughput, WarmThroughput or TableClass is required", "mrc_only": "At
least one of ProvisionedThroughput, BillingMode, UpdateStreamEnabled, GlobalSecondaryIndexUpdates,
SSESpecification, ReplicaUpdates, MultiAccountReplicaReady, ReplicaTransitRoleArn, DeletionProtectionEnabled,
OnDemandThroughput, WarmThroughput or TableClass is required"}, "us-east-1": {"only_name": "At least one of
ProvisionedThroughput, BillingMode, UpdateStreamEnabled, GlobalSecondaryIndexUpdates, SSESpecification,
ReplicaUpdates, MultiAccountReplicaReady, ReplicaTransitRoleArn, MultiRegionConsistency,
DeletionProtectionEnabled, OnDemandThroughput, WarmThroughput or TableClass is required", "mrc_only": "At
least one of ProvisionedThroughput, BillingMode, UpdateStreamEnabled, GlobalSecondaryIndexUpdates,
SSESpecification, ReplicaUpdates, MultiAccountReplicaReady, ReplicaTransitRoleArn, DeletionProtectionEnabled,
OnDemandThroughput, WarmThrough
