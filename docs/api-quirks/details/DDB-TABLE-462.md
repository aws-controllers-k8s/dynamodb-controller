<!-- generated: ack-api-quirks dynamodb 2026-10-09 model=2012-08-10 controller=34b85e6 -->
# DDB-TABLE-462: SSE write does not clobber throughput changes, GSI deletes or PROV->PPR switches: its response says ACTIVE, DescribeTable keeps UPDATING
_Full entry and notes of one finding; its summary entry is in [table-indexes.md](../table-indexes.md).
Generated from ack-api-quirks `services/dynamodb` (model 2012-08-10, controller commit 34b85e6); edit the
finding in the lab, not here._

## Finding

- <a id="ddb-table-462"></a>**DDB-TABLE-462** `stale-response` · impact medium · unhandled (not handled in controller) · verified 2026-10-09
  **SSE write does not clobber throughput changes, GSI deletes or PROV->PPR switches: its response says ACTIVE, DescribeTable keeps UPDATING**
  PROVISIONED 1/1 tables, UpdateTable(SSESpecification Enabled/KMS) 0.1 s after the job, DescribeTable every
  0.2 s. pt (ProvisionedThroughput 2/2, ~1.5 s): pt: job -> UPDATING; SSE at +0.2 s -> 200 (response
  TableStatus ACTIVE); DescribeTable right after: UPDATING; follow-up at +0.61 s -> ResourceInUseException
  'Attempt to change a resource which is still in use: Table IOPS are currently being updated. Tab'; job
  indicator settled at +1.45 s; final {'status': 'ACTIVE', 'rcu': 2, 'bm': None, 'gsi': None, 'sse':
  'ENABLED'}. gd (GSI Delete on a PROVISIONED table): gd: job -> UPDATING; SSE at +0.2 s -> 200 (response
  TableStatus ACTIVE); DescribeTable right after: UPDATING; follow-up at +1.02 s -> ResourceInUseException
  'Attempt to change a resource which is still in use: Can't change table IOPS when an index is be'; job
  indicator settled at +3.34 s; final {'status': 'ACTIVE', 'rcu': 1, 'bm': None, 'gsi': None, 'sse':
  'ENABLED'}. b2 (BillingMode PAY_PER_REQUEST, ~107 s): b2: job -> UPDATING; SSE at +0.2 s -> 200 (response
  TableStatus ACTIVE); DescribeTable right after: UPDATING; follow-up at +1.02 s -> ResourceInUseException
  'Attempt to change a resource which is still in use: Table IOPS are currently being updated. Tab'; job
  indicator settled at +107.15 s; final {'status': 'ACTIVE', 'rcu': 0, 'bm': 'PAY_PER_REQUEST', 'gsi': None,
  'sse': 'ENABLED'}. In every cell the SSE write's own UpdateTable response carried TableStatus=ACTIVE while
  DescribeTable stayed UPDATING and the follow-up was refused; the SSE job itself ran to ENABLED alongside the
  table job.
  - ACK: synced.when, one-per-reconcile, requeue · ops: UpdateTable, DescribeTable · fields: SSESpecification,
    TableStatus, ProvisionedThroughput, GlobalSecondaryIndexUpdates, BillingMode
  - repro: UpdateTable(job); UpdateTable(SSESpecification KMS) at +0.1 s; follow-up UpdateTable at +0.6/+1.0
    s; DescribeTable every 0.2 s
  - handling: not handled in the controller (as of commit 34b85e6)
  - related: [DDB-TABLE-450](../table-streams-encryption-class.md#ddb-table-450), [DDB-TABLE-287](../table-streams-encryption-class.md#ddb-table-287), [DDB-TABLE-370](../table-throughput-billing.md#ddb-table-370), [DDB-TABLE-376](../table-indexes.md#ddb-table-376), [DDB-TABLE-452](../table-throughput-billing.md#ddb-table-452), [DDB-TABLE-163](../table-subresources.md#ddb-table-163),
    [DDB-TABLE-375](../table-indexes.md#ddb-table-375), [DDB-TABLE-152](../table-indexes.md#ddb-table-152), [DDB-TABLE-286](../table-streams-encryption-class.md#ddb-table-286), [DDB-TABLE-458](../table-indexes.md#ddb-table-458), [DDB-TABLE-175](../table-indexes.md#ddb-table-175), [DDB-TABLE-165](../table-indexes.md#ddb-table-165), [DDB-TABLE-361](../table-streams-encryption-class.md#ddb-table-361),
    [DDB-TABLE-155](../table-indexes.md#ddb-table-155) · evidence: table/creative/clobber-iops-sse

## Notes

Closes the SSE column of the clobber matrix: with table/creative/clobber-matrix (stream enable, PPR->PROV) and
clobber-gsi-warm (GSI add) the premature-ACTIVE reset of [DDB-TABLE-287](../table-streams-encryption-class.md#ddb-table-287) is confined to the TableClass switch.
The SSE response's TableStatus=ACTIVE is a stale-response hazard everywhere (it never reflects a concurrent
table job), so a controller must not read TableStatus from the UpdateTable(SSESpecification) response.
