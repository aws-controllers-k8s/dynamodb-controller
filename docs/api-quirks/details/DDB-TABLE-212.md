<!-- generated: ack-api-quirks dynamodb 2026-10-09 model=2012-08-10 controller=34b85e6 -->
# DDB-TABLE-212: ResourceArn forms: bare name/other account/region/partition/index -> ValidationException; missing or wrong-case table -> ResourceNotFound
_Full entry and notes of one finding; its summary entry is in
[table-policy-kinesis-autoscaling.md](../table-policy-kinesis-autoscaling.md). Generated from ack-api-quirks
`services/dynamodb` (model 2012-08-10, controller commit 34b85e6); edit the finding in the lab, not here._

## Finding

- <a id="ddb-table-212"></a>**DDB-TABLE-212** `identity` · impact medium · unhandled (not handled in controller) · verified 2026-10-09
  **ResourceArn forms: bare name/other account/region/partition/index -> ValidationException; missing or wrong-case table -> ResourceNotFound**
  GetResourcePolicy / PutResourcePolicy / DeleteResourcePolicy by ResourceArn form: bare table name ->
  ValidationException (HTTP 400) 'One or more parameter values were invalid: ARNs must start with 'arn:':
  ackq-71f899-rp' / ValidationException (HTTP 400) 'One or more parameter values were invalid: ARNs must start
  with 'arn:': ackq-71f899-rp' / skipped (put ok); ARN of a missing table (same account) ->
  ResourceNotFoundException (HTTP 400) 'Requested resource not found: Table: ackq-71f899-missing not found' /
  ResourceNotFoundException (HTTP 400) 'Requested resource not found: Table: ackq-71f899-missing not found' /
  skipped (put ok); ARN with another account id -> ValidationException (HTTP 400) 'This action is only
  supported by accounts that match the resource owner’s account.' / ValidationException (HTTP 400) 'This
  action is only supported by accounts that match the resource owner’s account.' / skipped (put ok); ARN in
  the other region with this table's name -> ValidationException (HTTP 400) 'One or more parameter values were
  invalid: Invalid resource Arn: arn:aws:dynamodb:us-east-1:<ACCOUNT>:table/ackq-71f899-rp' /
  ValidationException (HTTP 400) 'One or more parameter values were invalid: Invalid resource Arn:
  arn:aws:dynamodb:us-east-1:<ACCOUNT>:table/ackq-71f899-rp' / skipped (put ok); aws-cn partition ->
  ValidationException (HTTP 400) 'One or more parameter values were invalid: Invalid resource Arn:
  arn:aws-cn:dynamodb:us-west-2:<ACCOUNT>:table/ackq-71f899-rp' / ValidationException (HTTP 400) 'One or more
  parameter values were invalid: Invalid resource Arn:
  arn:aws-cn:dynamodb:us-west-2:<ACCOUNT>:table/ackq-71f899-rp' / skipped (put ok); upper-cased name ->
  ResourceNotFoundException (HTTP 400) 'Requested resource not found: Table: ACKQ-71F899-RP not found' /
  ResourceNotFoundException (HTTP 400) 'Requested resource not found: Table: ACKQ-71F899-RP not found' /
  skipped (put ok); GSI ARN -> ValidationException (HTTP 400) 'One or more parameter values were invalid:
  Invalid resource arn provided, only table or stream is accepted. Provided resource arn:
  arn:aws:dynamodb:us-west-2:<ACCOUNT>' / ValidationException (HTTP 400) 'One or more parameter values were
  invalid: Invalid resource arn provided, only table or stream is accepted. Provided resource arn:
  arn:aws:dynamodb:us-west-2:<ACCOUNT>' / skipped (put ok); bogus stream label -> ResourceNotFoundException
  (HTTP 400) 'Requested resource not found: Stream: 2000-01-01T00:00:00.000 not found for Table:
  ackq-71f899-rp' / ValidationException (HTTP 400) 'One or more parameter values were invalid: Invalid policy
  document: The relative-id "table/ackq-71f899-rp" is invalid for ARN "arn:aws:dynamodb:us-west-2:<ACCOUNT>' /
  skipped (put ok); backup-shaped ARN -> ValidationException (HTTP 400) 'One or more parameter values were
  invalid: Provided Arn is not a DynamoDB resource arn:
  arn:aws:dynamodb:us-west-2:<ACCOUNT>:table/ackq-71f899-rp/backup/0170' / ValidationException (HTTP 400) 'One
  or more parameter values were invalid: Provided Arn is not a DynamoDB resource arn:
  arn:aws:dynamodb:us-west-2:<ACCOUNT>:table/ackq-71f899-rp/backup/0170' / skipped (put ok); real stream ARN
  (LatestStreamArn) -> PolicyNotFoundException (HTTP 400) 'Resource-based policy not found for the provided
  ResourceArn: arn:aws:dynamodb:us-west-2:<ACCOUNT>:table/ackq-71f899-rp/stream/2026-10-09T00:29:04.041' / 200
  OK / skipped (put ok).
  - ACK: exceptions.404, terminal_codes, is_arn_primary_key · ops: GetResourcePolicy, PutResourcePolicy,
    DeleteResourcePolicy · fields: ResourceArn
  - repro: Get/Put/Delete with each ResourceArn form on an ACTIVE table that has a GSI and a stream
  - handling: not handled in the controller (as of commit 34b85e6)
  - related: [DDB-TABLE-136](../service.md#ddb-table-136), [DDB-TABLE-211](../table-policy-kinesis-autoscaling.md#ddb-table-211) · hypotheses: H-S-111, H-S-031 · evidence:
    table/sub-resources/resource-policy

## Notes

H-S-111: bare name -> ValidationException and missing table -> ResourceNotFoundException confirmed; other
account -> ValidationException 'This action is only supported by accounts that match the resource owner's
account' (hyp: AccessDeniedException) refuted. H-S-031: index ARN -> ValidationException 'only table or stream
is accepted' confirmed; a bogus stream label -> ResourceNotFoundException naming the stream.
