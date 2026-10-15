<!-- generated: ack-api-quirks dynamodb 2026-10-09 model=2012-08-10 controller=34b85e6 -->
# DDB-TABLE-070: Not-found is HTTP 400 ResourceNotFoundException for table ops; sub-resource ops use other codes
_Full entry and notes of one finding; its summary entry is in [service.md](../service.md). Generated from
ack-api-quirks `services/dynamodb` (model 2012-08-10, controller commit 34b85e6); edit the finding in the lab,
not here._

## Finding

- <a id="ddb-table-070"></a>**DDB-TABLE-070** `error-code` · impact high · unhandled (not handled in controller) · verified 2026-10-08
  **Not-found is HTTP 400 ResourceNotFoundException for table ops; sub-resource ops use other codes**
  For a table name that does not exist, DescribeTable, UpdateTable (DeletionProtectionEnabled or
  ProvisionedThroughput), DeleteTable, DescribeTimeToLive, UpdateTimeToLive, DescribeContributorInsights,
  DescribeKinesisStreamingDestination and GetResourcePolicy return ResourceNotFoundException with HTTP 400 and
  message 'Requested resource not found: Table: <name> not found';
  TagResource/UntagResource/ListTagsOfResource with the corresponding ARN return ResourceNotFoundException
  'Requested resource not found: ResourceArn: <arn> not found'. No operation returned HTTP 404. UpdateTable
  with an invalid shape (WriteCapacityUnits=0) on a missing table returns ValidationException, i.e. request
  validation runs before the existence check. DescribeContinuousBackups and CreateBackup return
  TableNotFoundException 'Table not found: <name>' instead; DescribeTableReplicaAutoScaling returns
  ResourceNotFoundException with the message "Global table with name: '<name>' does not exist."
  - ACK: exceptions.404, terminal_codes · ops: DescribeTable, UpdateTable, DeleteTable, TagResource,
    UntagResource, ListTagsOfResource, DescribeContinuousBackups, DescribeTimeToLive,
    DescribeContributorInsights
  - repro: Each op with TableName/ARN of a table that never existed
  - handling: not handled in the controller (as of commit 34b85e6)
  - related: [DDB-TABLE-074](../service.md#ddb-table-074), [DDB-TABLE-014](../table-policy-kinesis-autoscaling.md#ddb-table-014), [DDB-TABLE-098](../service.md#ddb-table-098), [DDB-TABLE-447](../service.md#ddb-table-447) · evidence:
    table/error-taxonomy/not-found-codes

## Notes

Confirms the HTTP-400 part of H-T-054 and the ValidationException-for-malformed-ARN part; the codes are not
uniform across API families (see the TableNotFoundException finding). Validation-before-existence means a
controller cannot infer 'table gone' from a failed UpdateTable without checking the code.
