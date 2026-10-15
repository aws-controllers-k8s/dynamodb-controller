<!-- generated: ack-api-quirks dynamodb 2026-10-09 model=2012-08-10 controller=34b85e6 -->
# DDB-TABLE-374: DELETING timeline per API: changed policy Put ResourceInUse from +0.03 s; identical re-Put/CreateBackup 200 until ~1.6 s; backends 404 first
_Full entry and notes of one finding; its summary entry is in [service.md](../service.md). Generated from
ack-api-quirks `services/dynamodb` (model 2012-08-10, controller commit 34b85e6); edit the finding in the lab,
not here._

## Finding

- <a id="ddb-table-374"></a>**DDB-TABLE-374** `delete-semantics` · impact high · unhandled (not handled in controller) · verified 2026-10-09
  **DELETING timeline per API: changed policy Put ResourceInUse from +0.03 s; identical re-Put/CreateBackup 200 until ~1.6 s; backends 404 first**
  One table per API; DeleteTable, then the API every 200 ms with DescribeTable in the same slot. Format:
  outcome [TableStatus seen] from-to s x calls. PutResourcePolicy with a NEW document, no prior policy, trial
  1: gone at 6.03s; ResourceInUseException HTTP 400 [DELETING] 0.03-4.63s x24 | ResourceNotFoundException HTTP
  400 [DELETING] 4.83-5.83s x6 | ResourceNotFoundException HTTP 400 [ERR:ResourceNotFoundException] 6.03-7.03s
  x6. Trial 2: gone at 7.43s; ResourceInUseException HTTP 400 [DELETING] 0.03-6.03s x31 |
  ResourceNotFoundException HTTP 400 [DELETING] 6.23-7.23s x6 | ResourceNotFoundException HTTP 400
  [ERR:ResourceNotFoundException] 7.43-8.43s x6. New document over an existing policy: gone at 5.64s;
  ResourceInUseException HTTP 400 [DELETING] 0.04-4.24s x22 | ResourceNotFoundException HTTP 400 [DELETING]
  4.44-5.44s x6 | ResourceNotFoundException HTTP 400 [ERR:ResourceNotFoundException] 5.64-6.64s x6. IDENTICAL
  re-Put of the existing document: gone at 4.83s; OK [DELETING] 0.03-1.63s x9 | ResourceInUseException HTTP
  400 [DELETING] 1.83-4.63s x15 | ResourceNotFoundException HTTP 400 [ERR:ResourceNotFoundException]
  4.83-5.83s x6. CreateBackup: gone at 5.63s; OK [DELETING] 0.03-1.63s x9 | TableNotFoundException HTTP 400
  [DELETING] 1.83-5.43s x19 | TableNotFoundException HTTP 400 [ERR:ResourceNotFoundException] 5.63-6.63s x6.
  TagResource re-sending the existing key/value: gone at 5.03s; OK [DELETING] 0.03-1.03s x6 |
  ResourceNotFoundException HTTP 400 [DELETING] 1.23-4.83s x19 | ResourceNotFoundException HTTP 400
  [ERR:ResourceNotFoundException] 5.03-6.03s x6. TagResource with a changing value: gone at 5.83s;
  ResourceInUseException HTTP 400 [DELETING] 0.03-1.63s x9 | ResourceNotFoundException HTTP 400 [DELETING]
  1.83-5.63s x20 | ResourceNotFoundException HTTP 400 [ERR:ResourceNotFoundException] 5.83-6.83s x6. Messages:
  policy {'ResourceInUseException': 'Attempt to change a resource which is still in use: Table is being
  deleted: ackq-53f6bd-del-p1', 'ResourceNotFoundException': 'Requested resource not found: Table:
  ackq-53f6bd-del-p1 not found'}; backup {'TableNotFoundException': 'Table not found: ackq-53f6bd-del-b'};
  identical tag {'ResourceNotFoundException': 'Requested resource not found: ResourceArn:
  arn:aws:dynamodb:us-west-2:<ACCOUNT>:table/ackq-53f6bd-del-t not found'}; changing tag
  {'ResourceInUseException': 'Attempt to change a resource which is still in use: Table is being deleted:
  ackq-53f6bd-del-t2', 'ResourceNotFoundException': 'Requested resource not found: ResourceArn:
  arn:aws:dynamodb:us-west-2:<ACCOUNT>:table/ackq-53f6bd-del-t2 not found'}. After the tables were gone (first
  run): GetResourcePolicy on p1/p1b/p2 -> ResourceNotFoundException 'Requested resource not found: Table: X
  not found'; the 9 backups created in the first 1.6 s of B's DELETING were AVAILABLE and deletable
  (DeleteBackup 9x 200). The second run re-created the p1/p1b/p2 names: GetResourcePolicy then returned
  PolicyNotFoundException (no policy leaked from the deleted incarnation).
  - ACK: deletable.when, pre-delete-cleanup, requeue · ops: DeleteTable, PutResourcePolicy, CreateBackup,
    TagResource, DescribeTable, GetResourcePolicy
  - repro: PPR table; DeleteTable; loop every 200 ms: <API> + DescribeTable until ResourceNotFoundException +1
    s; one table per API variant (new/identical policy doc, CreateBackup, same/changing tag)
  - measurements: p1_resource_in_use_s=[0.03, 4.63], p1_rnf_from_s=4.83, p1_table_gone_s=6.03,
    p1b_resource_in_use_s=[0.03, 6.03], p1b_table_gone_s=7.43, p2_resource_in_use_s=[0.04, 4.24],
    p2_table_gone_s=5.64, p3_identical_put_ok_s=[0.03, 1.63], p3_table_gone_s=4.83, b_backup_ok_s=[0.03,
    1.63], b_table_not_found_from_s=1.83, b_table_gone_s=5.63, t_identical_tag_ok_s=[0.03, 1.03],
    t_rnf_from_s=1.23, t_table_gone_s=5.03, t2_changing_tag_ok_s=null, t2_resource_in_use_s=[0.03, 1.63],
    t2_table_gone_s=5.83, backups_created_during_deleting=9
  - handling: not handled in the controller (as of commit 34b85e6)
  - related: [DDB-TABLE-122](../table-policy-kinesis-autoscaling.md#ddb-table-122), [DDB-TABLE-236](../table-policy-kinesis-autoscaling.md#ddb-table-236), [DDB-TABLE-104](../table.md#ddb-table-104), [DDB-TABLE-118](../service.md#ddb-table-118), [DDB-BACKUP-001](../backup.md#ddb-backup-001), [DDB-BACKUP-007](../backup.md#ddb-backup-007),
    [DDB-TABLE-207](../table-policy-kinesis-autoscaling.md#ddb-table-207), [DDB-TABLE-097](../table-subresources.md#ddb-table-097), [DDB-TABLE-453](../table-subresources.md#ddb-table-453), [DDB-TABLE-088](../table-subresources.md#ddb-table-088), [DDB-TABLE-235](../table-policy-kinesis-autoscaling.md#ddb-table-235), [DDB-TABLE-342](../table-subresources.md#ddb-table-342), [DDB-TABLE-214](../table-policy-kinesis-autoscaling.md#ddb-table-214),
    [DDB-TABLE-271](../table-policy-kinesis-autoscaling.md#ddb-table-271), [DDB-TABLE-436](../table-policy-kinesis-autoscaling.md#ddb-table-436), [DDB-TABLE-377](../service.md#ddb-table-377) · hypotheses: H-S-120, H-B-005, H-T-064 · evidence:
    table/consistency-windows/deleting-admissibility-timeline

## Notes

Reconciles [DDB-TABLE-122](../table-policy-kinesis-autoscaling.md#ddb-table-122) (Put 200 during DELETING) with [DDB-TABLE-236](../table-policy-kinesis-autoscaling.md#ddb-table-236) (Put -> ResourceInUseException): a Put
that CHANGES the policy is rejected with 'Table is being deleted' from the first 200 ms slot in 3/3 trials,
while an IDENTICAL re-Put (RevisionId no-op) returns 200 for the first ~1.6 s (P3) - that is the shape
[DDB-TABLE-122](../table-policy-kinesis-autoscaling.md#ddb-table-122) used, so both findings are right. The policy and tag backends forget the table
(ResourceNotFound) ~1 s BEFORE DescribeTable stops returning DELETING; CreateBackup flips to
TableNotFoundException ~4 s before. Each 200 CreateBackup leaves a backup that outlives the table. Confirms
the ResourceInUse part of H-S-120 for PutResourcePolicy; refutes H-B-005 (CreateBackup during DELETING is 200
then TableNotFoundException, never TableInUseException); qualifies H-T-064 (TagResource: 200 for ~1 s, then
RNF).
