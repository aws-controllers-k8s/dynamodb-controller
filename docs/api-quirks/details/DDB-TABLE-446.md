<!-- generated: ack-api-quirks dynamodb 2026-10-09 model=2012-08-10 controller=34b85e6 -->
# DDB-TABLE-446: ResourceNotFoundException catalogue: the word after 'not found:'...
_Full entry and notes of one finding; its summary entry is in [service.md](../service.md). Generated from
ack-api-quirks `services/dynamodb` (model 2012-08-10, controller commit 34b85e6); edit the finding in the lab,
not here._

## Finding

- <a id="ddb-table-446"></a>**DDB-TABLE-446** `error-code` · impact high · unhandled (not handled in controller) · verified 2026-10-09
  **ResourceNotFoundException catalogue: the word after 'not found:'...**
  ResourceNotFoundException (HTTP 400) has ~8 text families: 'Requested resource not found: Table: <TBL> not
  found' (table gone or CREATING: WAIT or re-create), 'Requested resource not found: ResourceArn: <ARN> not
  found' (tag APIs; also returned while the table is CREATING -> WAIT-FOR-STATE), 'Requested resource not
  found: Index <name> for table <TBL>' (UpdateTable GSI Update/Delete: PERMANENT-SPEC) vs 'Requested resource
  not found: Index: <name> not found for table: <TBL>' (Contributor Insights, also while the index is
  CREATING), 'Requested resource not found: Stream: <label> not found for Table: <TBL>' (policy APIs on a
  stream ARN), "Global table with name: '<TBL>' does not exist." (DescribeTableReplicaAutoScaling on a
  regional table: PERMANENT), 'Failed to update settings for global table with name: ... because a replica
  does not exist in regions / the global secondary indexes with names ... do not exist' (PERMANENT-SPEC),
  'Stream <TBL> under account <ACCT> not found.' (Kinesis, different API), bare 'Requested resource not found'
  (data plane). On global tables the same text may arrive wrapped as '... not found (Service:
  AmazonDynamoDBv2; Status Code: 400; Error Code: ResourceNotFoundException; Request ID: <52 chars>; Proxy:
  null)', i.e. with a per-request id, so only prefix matching is safe.
  - ACK: exceptions.404, requeue · ops: DescribeTable, UpdateTable, TagResource, ListTagsOfResource,
    DescribeContributorInsights, GetResourcePolicy, DescribeTableReplicaAutoScaling,
    UpdateTableReplicaAutoScaling
  - repro: DescribeTable missing; UpdateTable GSI Update IndexName=ghost; DescribeContributorInsights
    IndexName=ghost; DescribeTableReplicaAutoScaling on a regional table
  - handling: not handled in the controller (as of commit 34b85e6)
  - related: [DDB-TABLE-070](../service.md#ddb-table-070), [DDB-TABLE-161](../table-subresources.md#ddb-table-161), [DDB-TABLE-098](../service.md#ddb-table-098), [DDB-TABLE-101](../service.md#ddb-table-101), [DDB-TABLE-187](../table-replicas.md#ddb-table-187), [DDB-TABLE-232](../table-replicas.md#ddb-table-232),
    [DDB-TABLE-254](../table-replicas.md#ddb-table-254), [DDB-TABLE-231](../table-global-tables.md#ddb-table-231), [DDB-TABLE-253](../table-replicas.md#ddb-table-253) · evidence: table/creative/error-message-regions

## Notes

Classes: RETRY = TRANSIENT-RETRY, clears by itself (typical wait given); WAIT = TRANSIENT-WAIT-FOR-STATE,
clears when the named status changes; SPEC = PERMANENT-SPEC, user must change the spec / fix an external
dependency; QUOTA = PERMANENT-QUOTA, needs a quota increase or the stated 1h/24h/30d window. Messages mined
from 119 past evidence.jsonl files (9562 error records, 29 codes; mine_errors.py -> mined-catalogue.txt in
this probe dir) plus this probe's live runs in us-west-2 and us-east-1; <TBL>/<ARN>/<TS>/<N>/<REGION>/<IDX>
mark request-specific tokens. Machine-readable form: catalogue.py in the probe directory.

CATALOGUE (ResourceNotFoundException):
- [WAIT ~PERMANENT if the table is gone; WAIT while a restore/import target is still CREATING (same text)]
Requested resource not found: Table: <TBL> not found (ops:
DescribeTable,UpdateTable,DeleteTable,DescribeTimeToLive,UpdateTimeToLive,*ContributorInsights,*KinesisStreamingDestination,*ResourcePolicy)
- [WAIT ~PERMANENT if gone; WAIT (TableStatus=ACTIVE) right after CreateTable and ~1 s after DeleteTable
starts] Requested resource not found: ResourceArn: <ARN> not found (ops:
TagResource,UntagResource,ListTagsOfResource)
- [SPEC] Requested resource not found: Index <IDX> for table <TBL> (ops: UpdateTable; GSI Update/Delete on an
unknown index (no colon after Index))
- [WAIT ~IndexStatus=ACTIVE if the GSI is CREATING; PERMANENT for LSIs/unknown names] Requested resource not
found: Index: <IDX> not found for table: <TBL> (ops: DescribeContributorInsights,UpdateContributorInsights)
- [SPEC] Requested resource not found: Stream: <TS> not found for Table: <TBL> (ops:
GetResourcePolicy,PutResourcePolicy,DeleteResourcePolicy; stream ARN with an unknown label)
- [SPEC] Global table with name: '<TBL>' does not exist. (ops:
DescribeTableReplicaAutoScaling,UpdateTableReplicaAutoScaling; regional table or missing table; same text as
GlobalTableNotFoundException for the legacy APIs)
- [SPEC] Failed to update settings for global table with name: ‘<TBL>’ because a replica does not exist in
regions: ‘[<REGION>]’. (ops: UpdateTableReplicaAutoScaling; also '... because the global secondary indexes
with names: ‘[<IDX>]’ do not exist.' and '... a global secondary index with name: ‘<IDX>’ does not exist in
region: ‘<REGION>’.' (WAIT while the GSI is CREATING))
- [WAIT] Requested resource not found: Table: <TBL> not found (Service: AmazonDynamoDBv2; Status Code: 400;
Error Code: ResourceNotFoundException; Request ID: <REQID>; Proxy: null) (ops:
DescribeTableReplicaAutoScaling,UpdateTable; GLOBAL TABLES: Java-SDK suffix with a per-request id)
- [SPEC] Stream <TBL> under account <ACCT> not found. (ops: DescribeStreamSummary,DeleteStream; Kinesis API,
not DynamoDB)
- [SPEC] Requested resource not found (ops: PutItem,GetItem; data plane: bare text)

LIVE this run (region/label: http code latency 'message'):
us-east-1/rnf_describe: 400 ResourceNotFoundException 70ms 'Requested resource not found: Table:
ackq-e3283a-emr-e1-missing not found'
us-west-2/rnf_describe: 400 ResourceNotFoundException 11ms 'Requested resource not found: Table:
ackq-e3283a-emr-w2-missing not found'
us-east-1/rnf_tags_arn: 400 ResourceNotFoundException 65ms 'Requested resource not found: ResourceArn:
arn:aws:dynamodb:us-east-1:<ACCOUNT>:table/ackq-e3283a-emr-e1-missing not found'
us-west-2/rnf_tags_arn: 400 ResourceNotFoundException 7ms 'Requested resource not found: ResourceArn:
arn:aws:dynamodb:us-west-2:<ACCOUNT>:table/ackq-e3283a-emr-w2-missing not found'
us-east-1/rnf_policy_missing: 400 ResourceNotFoundException 65ms 'Requested resource not found: Table:
ackq-e3283a-emr-e1-missing not found'
us-west-2/rnf_policy_missing: 400 ResourceNotFoundException 7ms 'Requested resource not found: Table:
ackq-e3283a-emr-w2-missing not found'
us-east-1/rnf_ttl_missing: 400 ResourceNotFoundException 67ms 'Requested resource not found: Table:
ackq-e3283a-emr-e1-missing not found'
us-west-2/rnf_ttl_missing: 400 ResourceNotFoundException 10ms 'Requested resource not found: Table:
ackq-e3283a-emr-w2-missing not found'
us-east-1/rnf_gsi_ghost_update: 400 ResourceNotFoundException 73ms 'Requested resource not found: Index ghost
for table ackq-e3283a-emr-e1'
us-west-2/rnf_gsi_ghost_update: 400 ResourceNotFoundException 12ms 'Requested resource not found: Index ghost
for table ackq-e3283a-emr-w2'
us-east-1/rnf_insights_ghost: 400 ResourceNotFoundException 65ms 'Requested resource not found: Index: ghost
not found for table: ackq-e3283a-emr-e1'
us-west-2/rnf_insights_ghost: 400 ResourceNotFoundException 7ms 'Requested resource not found: Index: ghost
not found for table: ackq-e3283a-emr-w2'
us-east-1/rnf_autoscaling: 400 ResourceNotFoundException 88ms 'Global table with name: 'ackq-e3283a-emr-e1'
does not exist.'
us-west-2/rnf_autoscaling: 400 ResourceNotFoundException 56ms 'Global table with name: 'ackq-e3283a-emr-w2'
does not exist.'
us-east-1/creating_tag: 400 ResourceNotFoundException 216ms 'Requested resource not found: ResourceArn:
arn:aws:dynamodb:us-east-1:<ACCOUNT>:table/ackq-e3283a-emr-e1 not found'
us-west-2/creating_tag: 400 ResourceNotFoundException 40ms 'Requested resource not found: ResourceArn:
arn:aws:dynamodb:us-west-2:<ACCOUNT>:table/ackq-e3283a-emr-w2 not found'
us-east-1/creating_update_ttl: 400 ResourceNotFoundException 64ms 'Requested resource not found: Table:
ackq-e3283a-emr-e1 not found'
us-west-2/creating_update_ttl: 400 ResourceNotFoundException 6ms 'Requested resource not found: Table:
ackq-e3283a-emr-w2 not found'
us-east-1/ise_warm_empty_missing_table: 400 ResourceNotFoundException 69ms 'Requested resource not found:
Table: ackq-e3283a-emr-e1-missing not found'
us-west-2/ise_warm_empty_missing_table: 400 ResourceNotFoundException 12ms 'Requested resource not found:
Table: ackq-e3283a-emr-w2-missing not found'

Java-SDK-suffixed variant seen on DescribeTableReplicaAutoScaling and UpdateTable(ReplicaUpdates) against a
global table whose member was gone (probe table/dependencies/replica-prerequisites 2026-10-09T00:38). The TTL
cooldown ValidationException got the same suffix on a global table
(table/state-machine/replica-create-timeline).
