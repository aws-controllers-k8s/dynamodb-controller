<!-- generated: ack-api-quirks dynamodb 2026-10-09 model=2012-08-10 controller=34b85e6 -->
# DDB-TABLE-213: The stream ARN is an independent policy slot (own RevisionId, own 15s window); Get(table ARN) ignores it -> PolicyNotFoundException
_Full entry and notes of one finding; its summary entry is in
[table-streams-encryption-class.md](../table-streams-encryption-class.md). Generated from ack-api-quirks
`services/dynamodb` (model 2012-08-10, controller commit 34b85e6); edit the finding in the lab, not here._

## Finding

- <a id="ddb-table-213"></a>**DDB-TABLE-213** `sub-resource-api` · impact medium · unhandled (not handled in controller) · verified 2026-10-09
  **The stream ARN is an independent policy slot (own RevisionId, own 15s window); Get(table ARN) ignores it -> PolicyNotFoundException**
  PutResourcePolicy(ResourceArn=LatestStreamArn, Resource=stream ARN, stream actions) -> 200 rev S, readable
  via Get(stream ARN) after 1.54s. GetResourcePolicy(table ARN) at that time -> PolicyNotFoundException (HTTP 400)
  'Resource-based policy not found for the provided ResourceArn:
  arn:aws:dynamodb:us-west-2:<ACCOUNT>:table/ackq-71f899-rp'. PutResourcePolicy(table ARN) 1.6s after the
  stream write -> 200 OK (not blocked by the stream slot's window). A second write to the stream slot:
  ThrottlingException until 15.56s after the first, then 200 with a new id; the table policy stayed unchanged
  (True). Delete(stream ARN) -> throttled for ~15s after the stream change, then 200 echoing the removed id
  (True); Get(stream) -> PolicyNotFound 1.03s later. Delete(table ARN) -> 200 OK.
  - ACK: scope:defer, custom_field, ignore.field_paths, requeue · ops: PutResourcePolicy, GetResourcePolicy,
    DeleteResourcePolicy · fields: ResourceArn, ResourcePolicy
  - repro: table with streams: Put on LatestStreamArn; Get(table ARN); Put(table ARN); Put(stream ARN) again
    until accepted; Delete(stream ARN) until accepted
  - measurements: stream_second_write_first_success_s=15.56, stream_delete_attempts=15
  - handling: not handled in the controller (as of commit 34b85e6)
  - related: [DDB-TABLE-050](../table-streams-encryption-class.md#ddb-table-050), [DDB-TABLE-362](../table-streams-encryption-class.md#ddb-table-362), [DDB-TABLE-367](../table-streams-encryption-class.md#ddb-table-367), [DDB-TABLE-378](../table-streams-encryption-class.md#ddb-table-378), [DDB-TABLE-180](../table-streams-encryption-class.md#ddb-table-180), [DDB-TABLE-004](../table-streams-encryption-class.md#ddb-table-004),
    [DDB-TABLE-373](../service.md#ddb-table-373), [DDB-TABLE-013](../service.md#ddb-table-013), [DDB-TABLE-071](../service.md#ddb-table-071), [DDB-TABLE-206](../table-policy-kinesis-autoscaling.md#ddb-table-206), [DDB-TABLE-247](../table-policy-kinesis-autoscaling.md#ddb-table-247), [DDB-TABLE-347](../table-policy-kinesis-autoscaling.md#ddb-table-347), [DDB-TABLE-464](../service.md#ddb-table-464),
    [DDB-TABLE-445](../service.md#ddb-table-445), [DDB-TABLE-348](../table-policy-kinesis-autoscaling.md#ddb-table-348), [DDB-TABLE-205](../table-policy-kinesis-autoscaling.md#ddb-table-205) · hypotheses: H-S-031, H-S-027 · evidence:
    table/sub-resources/resource-policy

## Notes

H-S-031 stream-slot clause confirmed. A Table controller that only manages the table-ARN policy never sees
stream policies; a stream policy must use stream actions (table actions -> ValidationException 'relative-id
... stream').
