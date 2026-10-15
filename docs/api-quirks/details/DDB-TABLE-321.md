<!-- generated: ack-api-quirks dynamodb 2026-10-09 model=2012-08-10 controller=34b85e6 -->
# DDB-TABLE-321: AAS read scaling of the source region pins the replica's read capacity via Replicas[].ProvisionedThroughputOverride
_Full entry and notes of one finding; its summary entry is in [table-replicas.md](../table-replicas.md).
Generated from ack-api-quirks `services/dynamodb` (model 2012-08-10, controller commit 34b85e6); edit the
finding in the lab, not here._

## Finding

- <a id="ddb-table-321"></a>**DDB-TABLE-321** `cross-region` · impact medium · SUSPECTED CONTROLLER BUG · verified 2026-10-09
  **AAS read scaling of the source region pins the replica's read capacity via Replicas[].ProvisionedThroughputOverride**
  After Application Auto Scaling raised the source table's ReadCapacityUnits (1->2 in us-west-2; 5->10 in
  table/mutation-matrix/autoscaling-vs-throughput), DescribeTable showed the us-east-1 replica with
  ProvisionedThroughputOverride {ReadCapacityUnits: <old value>} (1 resp. 5) that the caller never set; before
  the scaling activity the replica had no override. Write capacity is table-wide and needs no override.
  - ACK: compare.is_ignored+delta_pre_compare, docs-only · ops: DescribeTable · fields:
    Replicas[].ProvisionedThroughputOverride.ReadCapacityUnits
  - repro: 2-replica PROVISIONED table -> register AAS read target with MinCapacity above current RCU in
    region A -> DescribeTable in A
  - handling: suspected controller bug - see Handling gaps
  - related: [DDB-TABLE-314](../table-policy-kinesis-autoscaling.md#ddb-table-314), [DDB-TABLE-195](../table-policy-kinesis-autoscaling.md#ddb-table-195), [DDB-TABLE-315](../table-policy-kinesis-autoscaling.md#ddb-table-315), [DDB-TABLE-256](../table-replicas.md#ddb-table-256), [DDB-TABLE-250](../table-replicas.md#ddb-table-250), [DDB-TABLE-252](../table-replicas.md#ddb-table-252),
    [DDB-TABLE-251](../table-replicas.md#ddb-table-251), [DDB-TABLE-294](../table-replicas.md#ddb-table-294), [DDB-TABLE-229](../table-global-tables.md#ddb-table-229), [DDB-TABLE-223](../table-replicas.md#ddb-table-223), [DDB-TABLE-307](../table-replicas.md#ddb-table-307) · hypotheses: H-S-039, H-R-033 ·
    evidence: table/sub-resources/replica-autoscaling-facade, table/mutation-matrix/autoscaling-vs-throughput

## Notes

A spec that omits replica overrides would see server-materialized overrides after any read scaling event in
another region.

Contradiction with [DDB-TABLE-251](../table-replicas.md#ddb-table-251), [DDB-TABLE-314](../table-policy-kinesis-autoscaling.md#ddb-table-314): 251 claims provisioned RCU/WCU replicate between ALL regions
and overrides are the only per-region knob; 321/314 show an AAS-driven RCU change on the source NOT
replicating - the replica keeps the old value via a ProvisionedThroughputOverride the caller never set
Resolution: keep both; the likely discriminator is an AAS read target on the dimension (321/314 tables had
read autoscaling, 251's had write-only AAS) - needs a targeted re-probe. Controller consequence stands either
way: Replicas[].ProvisionedThroughputOverride can be server-owned

Suspected controller bug confirmed by evidence: The GT's example is inverted but the loop is real:
DescribeTable echoes EVERY table GSI under Replicas[].GlobalSecondaryIndexes with IndexName (+WarmThroughput)
and no override (296/252), and AAS read scaling injects a ProvisionedThroughputOverride the spec never
declared (321); both are observed-vs-desired differences that updateReplicaUpdate() cannot express as a valid
action (RegionName-only Update is rejected: 294), so the hook returns an empty update and requeues with
requeueWaitReplicasActive forever. Also: a same-value override Update is accepted and costs ~34s UPDATING
(294), so even an expressible 'fix' churns.
