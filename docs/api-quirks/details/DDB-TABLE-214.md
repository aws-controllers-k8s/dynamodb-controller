<!-- generated: ack-api-quirks dynamodb 2026-10-09 model=2012-08-10 controller=34b85e6 -->
# DDB-TABLE-214: Policy APIs on a deleted table's ARN: Get/Put/Delete -> ResourceNotFoundException (Delete is not idempotent across table deletion)
_Full entry and notes of one finding; its summary entry is in
[table-policy-kinesis-autoscaling.md](../table-policy-kinesis-autoscaling.md). Generated from ack-api-quirks
`services/dynamodb` (model 2012-08-10, controller commit 34b85e6); edit the finding in the lab, not here._

## Finding

- <a id="ddb-table-214"></a>**DDB-TABLE-214** `delete-semantics` · impact medium · handled · verified 2026-10-09
  **Policy APIs on a deleted table's ARN: Get/Put/Delete -> ResourceNotFoundException (Delete is not idempotent across table deletion)**
  3s after DescribeTable first returned ResourceNotFoundException: GetResourcePolicy(table ARN) ->
  ResourceNotFoundException (HTTP 400) 'Requested resource not found: Table: ackq-71f899-rp not found';
  PutResourcePolicy -> ResourceNotFoundException (HTTP 400) 'Requested resource not found: Table:
  ackq-71f899-rp not found'; DeleteResourcePolicy -> ResourceNotFoundException (HTTP 400) 'Requested resource
  not found: Table: ackq-71f899-rp not found'; GetResourcePolicy(old stream ARN) -> PolicyNotFoundException
  (HTTP 400) 'Resource-based policy not found for the provided ResourceArn:
  arn:aws:dynamodb:us-west-2:<ACCOUNT>:table/ackq-71f899-rp/stream/2026-10-09T00:29:04.041'.
  - ACK: exceptions.404, terminal_codes · ops: GetResourcePolicy, PutResourcePolicy, DeleteResourcePolicy
  - repro: DeleteTable; wait ResourceNotFoundException; Get/Put/Delete policy by the old ARN
  - handling: handled via `pkg/resource/table/hooks_resource_policy.go:97-103; pkg/resource/table/hooks_resource_policy.go:129-132`
  - related: [DDB-TABLE-122](../table-policy-kinesis-autoscaling.md#ddb-table-122), [DDB-TABLE-236](../table-policy-kinesis-autoscaling.md#ddb-table-236), [DDB-TABLE-374](../service.md#ddb-table-374), [DDB-TABLE-097](../table-subresources.md#ddb-table-097), [DDB-TABLE-453](../table-subresources.md#ddb-table-453), [DDB-TABLE-088](../table-subresources.md#ddb-table-088),
    [DDB-TABLE-235](../table-policy-kinesis-autoscaling.md#ddb-table-235), [DDB-TABLE-342](../table-subresources.md#ddb-table-342), [DDB-TABLE-271](../table-policy-kinesis-autoscaling.md#ddb-table-271), [DDB-TABLE-436](../table-policy-kinesis-autoscaling.md#ddb-table-436), [DDB-TABLE-377](../service.md#ddb-table-377), [DDB-TABLE-205](../table-policy-kinesis-autoscaling.md#ddb-table-205), [DDB-TABLE-209](../table-policy-kinesis-autoscaling.md#ddb-table-209),
    [DDB-TABLE-246](../table-policy-kinesis-autoscaling.md#ddb-table-246), [DDB-TABLE-347](../table-policy-kinesis-autoscaling.md#ddb-table-347) · hypotheses: H-S-111, H-S-119 · evidence:
    table/sub-resources/resource-policy
