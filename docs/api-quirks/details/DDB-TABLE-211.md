<!-- generated: ack-api-quirks dynamodb 2026-10-09 model=2012-08-10 controller=34b85e6 -->
# DDB-TABLE-211: PutResourcePolicy validation: Resource may name another table/*/index (accepted); >20480 bytes -> ValidationException, not LimitExceeded
_Full entry and notes of one finding; its summary entry is in
[table-policy-kinesis-autoscaling.md](../table-policy-kinesis-autoscaling.md). Generated from ack-api-quirks
`services/dynamodb` (model 2012-08-10, controller commit 34b85e6); edit the finding in the lab, not here._

## Finding

- <a id="ddb-table-211"></a>**DDB-TABLE-211** `request-validation` · impact medium · unhandled (not handled in controller) · verified 2026-10-09
  **PutResourcePolicy validation: Resource may name another table/*/index (accepted); >20480 bytes -> ValidationException, not LimitExceeded**
  Statement Resource = another table's ARN -> 200 OK; Resource='*' -> 200 OK; Resource = this table's GSI ARN
  -> 200 OK; Resource omitted -> ValidationException (HTTP 400) 'One or more parameter values were invalid:
  Invalid policy document: Missing required field Resource'; Resource = this table's stream ARN with a table
  action -> ValidationException (HTTP 400) 'One or more parameter values were invalid: Invalid policy
  document: The relative-id "table/ackq-71f899-rp/stream/2026-10-09T00:29:04.041" is invalid for ARN "ar';
  stream actions (DescribeStream/GetRecords) in a table policy -> ValidationException (HTTP 400) 'One or more
  parameter values were invalid: Invalid policy document: The following action names are invalid:
  "dynamodb:GetRecords", "dynamodb:DescribeStream"'. Size: 21504 bytes (whitespace-padded) ->
  ValidationException (HTTP 400) 'One or more parameter values were invalid: Maximum policy size of 20480
  bytes exceeded'; 19456 bytes -> 200 (reads back as 228 bytes); 110 statements (29080 bytes) ->
  ValidationException (HTTP 400) 'One or more parameter values were invalid: Maximum policy size of 20480
  bytes exceeded'. Malformed JSON -> ValidationException (HTTP 400) 'One or more parameter values were
  invalid: Invalid policy document: This policy contains invalid Json'; empty string -> ValidationException
  (HTTP 400) 'One or more parameter values were invalid: Invalid policy document: This policy contains invalid
  Json'; empty Statement list -> ValidationException (HTTP 400) 'One or more parameter values were invalid:
  Invalid policy document: Could not parse the policy: Statement is empty!'.
  - ACK: terminal_codes, is_iam_policy · ops: PutResourcePolicy · fields: Policy
  - repro: Put with Resource=other table ARN; Put 21KB whitespace-padded; Put '{not json'; Put with
    dynamodb:DescribeStream action on the table ARN
  - handling: not handled in the controller (as of commit 34b85e6)
  - related: [DDB-TABLE-136](../service.md#ddb-table-136), [DDB-TABLE-212](../table-policy-kinesis-autoscaling.md#ddb-table-212) · hypotheses: H-S-029 · evidence:
    table/sub-resources/resource-policy

## Notes

H-S-029 REFUTED on two clauses: a Resource naming a different table is accepted (no cross-check against the
target), and >20KB is ValidationException 'Maximum policy size of 20480 bytes exceeded', not
LimitExceededException. Whitespace counts toward the limit (docs) but is stripped on read-back.
