<!-- generated: ack-api-quirks dynamodb 2026-10-09 model=2012-08-10 controller=34b85e6 -->
# DDB-TABLE-251: Manual ProvisionedThroughput changes in any region replicate group-wide (no override materialized); per-region RCU only via overrides
_Full entry and notes of one finding; its summary entry is in [table-replicas.md](../table-replicas.md).
Generated from ack-api-quirks `services/dynamodb` (model 2012-08-10, controller commit 34b85e6); edit the
finding in the lab, not here._

## Finding

- <a id="ddb-table-251"></a>**DDB-TABLE-251** `cross-region` · impact high · handled · verified 2026-10-09
  **Manual ProvisionedThroughput changes in any region replicate group-wide (no override materialized); per-region RCU only via overrides**
  UpdateTable ProvisionedThroughput RCU5/WCU10 on the base -> 200; after 38s the us-east-1 table also reports
  5/10 (A UPDATING 34s, B UPDATING 3s longer). UpdateTable issued directly in us-east-1 with RCU 7 / WCU 10 ->
  200, and the BASE table's RCU became 7 as well (A.Replicas[].ProvisionedThroughputOverride stays absent).
  UpdateTable in us-east-1 WCU 20 -> 200 and the base's WCU became 20. BillingMode=PAY_PER_REQUEST on the base
  with a replica -> 200; both regions switched (GSIs UPDATING for ~5 min). The replica entry shows
  GlobalTableSettingsReplicationMode=ENABLED_WITH_OVERRIDES.
  - ACK: compare.is_ignored+delta_pre_compare, custom_update, requeue, annotation-shadow-state · ops:
    UpdateTable, DescribeTable · fields: ProvisionedThroughput, Replicas.ProvisionedThroughputOverride,
    BillingMode
  - repro: PROVISIONED table + replica: UpdateTable ProvisionedThroughput in each region; DescribeTable both
    regions
  - measurements: wcu_propagation_total_s=39.1
  - handling: handled via `pkg/resource/table/hooks_replica_updates.go:39-45; pkg/resource/table/hooks_replica_updates.go:166-171`
  - related: [DDB-TABLE-048](../table-global-tables.md#ddb-table-048), [DDB-TABLE-320](../table-global-tables.md#ddb-table-320), [DDB-TABLE-231](../table-global-tables.md#ddb-table-231), [DDB-TABLE-201](../table-global-tables.md#ddb-table-201), [DDB-TABLE-229](../table-global-tables.md#ddb-table-229), [DDB-TABLE-250](../table-replicas.md#ddb-table-250),
    [DDB-TABLE-252](../table-replicas.md#ddb-table-252), [DDB-TABLE-321](../table-replicas.md#ddb-table-321), [DDB-TABLE-294](../table-replicas.md#ddb-table-294), [DDB-TABLE-223](../table-replicas.md#ddb-table-223), [DDB-TABLE-307](../table-replicas.md#ddb-table-307), [DDB-TABLE-296](../table-replicas.md#ddb-table-296), [DDB-TABLE-265](../table-replicas.md#ddb-table-265),
    [DDB-TABLE-295](../table-replicas.md#ddb-table-295), [DDB-TABLE-297](../table-replicas.md#ddb-table-297), [DDB-TABLE-322](../table-policy-kinesis-autoscaling.md#ddb-table-322), [DDB-TABLE-314](../table-policy-kinesis-autoscaling.md#ddb-table-314) · hypotheses: H-R-018 · evidence:
    table/cross-region/provisioned-replica

## Notes

Refutes the 'read-side is regional' half of H-R-018: with the default settings-replication mode a regional RCU
change does NOT materialise as a ProvisionedThroughputOverride in the other region, it is replicated into the
other region's own ProvisionedThroughput. A controller comparing its spec against DescribeTable will see
out-of-band changes made in ANY region as drift on the base table and revert them group-wide. Per-region read
capacity requires ReplicaUpdates.Update.ProvisionedThroughputOverride.

Contradiction with [DDB-TABLE-321](../table-replicas.md#ddb-table-321), [DDB-TABLE-314](../table-policy-kinesis-autoscaling.md#ddb-table-314): 251 claims provisioned RCU/WCU replicate between ALL regions
and overrides are the only per-region knob; 321/314 show an AAS-driven RCU change on the source NOT
replicating - the replica keeps the old value via a ProvisionedThroughputOverride the caller never set
Resolution: keep both; the likely discriminator is an AAS read target on the dimension (321/314 tables had
read autoscaling, 251's had write-only AAS) - needs a targeted re-probe. Controller consequence stands either
way: Replicas[].ProvisionedThroughputOverride can be server-owned
