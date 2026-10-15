<!-- generated: ack-api-quirks dynamodb 2026-10-09 model=2012-08-10 controller=34b85e6 -->
# DDB-TABLE-286: While a TableClass switch is UPDATING: stream/OnDemandThroughput/TableClass ResourceInUse; SSE, DP, tags, TTL, PITR, backup, policy OK
_Full entry and notes of one finding; its summary entry is in
[table-streams-encryption-class.md](../table-streams-encryption-class.md). Generated from ack-api-quirks
`services/dynamodb` (model 2012-08-10, controller commit 34b85e6); edit the finding in the lab, not here._

## Finding

- <a id="ddb-table-286"></a>**DDB-TABLE-286** `async-state-machine` · impact high · unhandled (not handled in controller) · verified 2026-10-09
  **While a TableClass switch is UPDATING: stream/OnDemandThroughput/TableClass ResourceInUse; SSE, DP, tags, TTL, PITR, backup, policy OK**
  Ops fired 0-0.66 s after UpdateTable(TableClass=IA) returned TableStatus=UPDATING (DescribeTable view just
  before each op: ["['ACTIVE', None, None]", "['UPDATING', None, None]"]): {'tableclass_resend_same':
  'ResourceInUseException', 'tableclass_reverse': 'ResourceInUseException', 'stream_enable':
  'ResourceInUseException', 'ondemand_throughput': 'ResourceInUseException', 'sse_aws_managed': 'OK(ACTIVE)',
  'tag_resource': 'OK', 'ttl_enable': 'OK', 'pitr_enable': 'OK', 'create_backup': 'OK', 'put_resource_policy':
  'OK', 'dp_true': 'OK(ACTIVE)'}. Rejection messages: {'tableclass_resend_same': "Attempt to change a resource
  which is still in use: Can't update table class when a table class update is in progress. Table:
  ackq-61b715-tc-ops TableClassUpdateInProgress: STANDARD_INFREQUENT_ACCESS", 'tableclass_reverse': "Attempt
  to change a resource which is still in use: Can't update table class when a table class update is in
  progress. Table: ackq-61b715-tc-ops TableClassUpdateInProgress: STANDARD_INFREQUENT_ACCESS",
  'stream_enable': "Attempt to change a resource which is still in use: Can't change stream status when a
  table class update is in progress. Table: ackq-61b715-tc-ops TableClassUpdateInProgress:
  STANDARD_INFREQUENT_ACCESS", 'ondemand_throughput': 'Attempt to change a resource which is still in use:
  OnDemandThroughput cannot be updated while TableClass update is in progress. TableClassUpdateInProgress:
  STANDARD_INFREQUENT_ACCESS'}. DescribeTable after the switch settled: {'DeletionProtectionEnabled': True,
  'StreamSpecification': None, 'SSEDescription': {'Status': 'ENABLED', 'SSEType': 'KMS', 'KMSMasterKeyArn':
  'arn:aws:kms:us-west-2:<ACCOUNT>:key/301f0bb6-edf0-476a-9525-14f2ee58148a'}, 'OnDemandThroughput': None,
  'GSIs': [], 'TableClassSummary': {'TableClass': 'STANDARD_INFREQUENT_ACCESS', 'LastUpdateDateTime':
  '2026-10-09 00:55:02.345000+00:00'}}. Switch timing with the ops interleaved: UPDATING 0 s, class flipped at
  3.59 s.
  - ACK: updateable.when, deletable.when, requeue, synced.when · ops: UpdateTable, DeleteTable, TagResource,
    UpdateTimeToLive, UpdateContinuousBackups, CreateBackup, PutResourcePolicy · fields: TableClass,
    DeletionProtectionEnabled, StreamSpecification, SSESpecification, OnDemandThroughput,
    GlobalSecondaryIndexUpdates
  - repro: UpdateTable(TableClass=STANDARD_INFREQUENT_ACCESS) on a PPR table; immediately issue each op once
  - handling: not handled in the controller (as of commit 34b85e6)
  - related: [DDB-TABLE-166](../table-indexes.md#ddb-table-166), [DDB-TABLE-163](../table-subresources.md#ddb-table-163), [DDB-TABLE-376](../table-indexes.md#ddb-table-376), [DDB-TABLE-458](../table-indexes.md#ddb-table-458), [DDB-TABLE-175](../table-indexes.md#ddb-table-175), [DDB-TABLE-174](../table-indexes.md#ddb-table-174),
    [DDB-TABLE-382](../table-streams-encryption-class.md#ddb-table-382), [DDB-TABLE-462](../table-indexes.md#ddb-table-462), [DDB-TABLE-287](../table-streams-encryption-class.md#ddb-table-287), [DDB-TABLE-450](../table-streams-encryption-class.md#ddb-table-450), [DDB-TABLE-135](../table-indexes.md#ddb-table-135), [DDB-TABLE-154](../table-throughput-billing.md#ddb-table-154), [DDB-TABLE-456](../service.md#ddb-table-456),
    [DDB-TABLE-128](../table-indexes.md#ddb-table-128), [DDB-TABLE-153](../table-indexes.md#ddb-table-153), [DDB-TABLE-375](../table-indexes.md#ddb-table-375), [DDB-TABLE-164](../table-indexes.md#ddb-table-164), [DDB-TABLE-155](../table-indexes.md#ddb-table-155) · hypotheses: H-T-001, H-T-002 ·
    evidence: table/state-machine/table-class-switch

## Notes

TableClass flavour of the per-field UPDATING admissibility matrix (wave 1 covered the stream-toggle and SSE
flavours; wave 1 also showed GSI Create is ResourceInUse during a TableClass switch, so it was not
re-attempted here). CAUTION: the SSE write at +0.2 s was accepted AND flipped TableStatus to ACTIVE
immediately (every later op saw ACTIVE) - see table/creative/tableclass-sse-race; the DP write is accepted
with the table already ACTIVE. DeleteTable during a switch: ResourceInUseException until the class flips (see
the timing finding).
