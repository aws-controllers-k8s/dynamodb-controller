<!-- generated: ack-api-quirks dynamodb 2026-10-09 model=2012-08-10 controller=34b85e6 -->
# DDB-TABLE-333: INACCESSIBLE_ENCRYPTION_CREDENTIALS: reads, tags, PITR, policy, DP/stream updates OK; data plane, TTL, backup, insights, SSE changes fail
_Full entry and notes of one finding; its summary entry is in
[table-streams-encryption-class.md](../table-streams-encryption-class.md). Generated from ack-api-quirks
`services/dynamodb` (model 2012-08-10, controller commit 34b85e6); edit the finding in the lab, not here._

## Finding

- <a id="ddb-table-333"></a>**DDB-TABLE-333** `async-state-machine` · impact high · unhandled (not handled in controller) · verified 2026-10-09
  **INACCESSIBLE_ENCRYPTION_CREDENTIALS: reads, tags, PITR, policy, DP/stream updates OK; data plane, TTL, backup, insights, SSE changes fail**
  Table in INACCESSIBLE_ENCRYPTION_CREDENTIALS (ListTables still lists it). OK: ListTagsOfResource,
  DescribeTimeToLive, DescribeContinuousBackups, DescribeKinesisStreamingDestination,
  DescribeContributorInsights, ListBackups, GetResourcePolicy (PolicyNotFoundException as for any table
  without a policy), TagResource (UntagResource 0.1 s later -> LimitExceededException 'Table tags are being
  updated' = the usual tag write lock), UpdateContinuousBackups(PITR on), PutResourcePolicy,
  UpdateTable(DeletionProtectionEnabled=true), UpdateTable(StreamSpecification enable) -> TableStatus UPDATING
  10 s then back to INACCESSIBLE_ENCRYPTION_CREDENTIALS (not ACTIVE). Rejected with ValidationException 'KMS
  key disabled error: com.amazonaws.services.kms.model.DisabledException: <key arn> is disabled. (Service:
  AWSKMS; Status Code: 400; Error Code: DisabledException ...)': GetItem (consistent and eventually
  consistent), PutItem, UpdateTimeToLive, CreateBackup, UpdateContributorInsights, and every SSESpecification
  change (to another enabled CMK, to the AWS managed key alias/aws/dynamodb, to the AWS owned key
  Enabled=false) - the error names the OLD disabled key; re-sending the same key -> ValidationException 'Table
  is already encrypted with given KMSMasterKey'. BillingMode/TableClass/OnDemandThroughput/WarmThroughput were
  ResourceInUseException here only because the stream enable was still in flight; re-tested alone they are all
  accepted (table/mutation-matrix/inaccessible-update-table).
  - ACK: synced.when, updateable.when, terminal_codes, tags.custom-sync, requeue · ops: DescribeTable,
    ListTagsOfResource, TagResource, UntagResource, UpdateTable, UpdateTimeToLive, UpdateContinuousBackups,
    CreateBackup, PutResourcePolicy, UpdateContributorInsights, GetItem, PutItem · fields: TableStatus,
    SSESpecification, DeletionProtectionEnabled, StreamSpecification, BillingMode, TableClass,
    OnDemandThroughput
  - repro: CMK table -> kms DisableKey -> wait for INACCESSIBLE_ENCRYPTION_CREDENTIALS -> issue each
    read/mutation once
  - handling: not handled in the controller (as of commit 34b85e6)
  - related: [DDB-TABLE-325](../table-streams-encryption-class.md#ddb-table-325) · hypotheses: H-T-102, H-T-103, H-T-136 · evidence:
    table/state-machine/kms-inaccessible-lifecycle

## Notes

H-T-102 confirmed (reads work, data plane fails with a ValidationException carrying the KMS DisabledException
text - no dedicated code). H-T-103 REFUTED: only SSE changes (and KMS-touching sub-resources
TTL/backup/insights) are refused; DP, streams, PITR, tags, policy, billing, class, throughput changes all go
through - but the self-heal via a different key is indeed impossible while the old key is disabled
(ValidationException naming the old key). H-T-136 confirmed for the INACCESSIBLE state (ARCHIVING/ARCHIVED
untested).
