<!-- generated: ack-api-quirks dynamodb 2026-10-09 model=2012-08-10 controller=34b85e6 -->
# DDB-TABLE-437: Empty-struct UpdateTable members: WarmThroughput {} and OnDemandThroughput {} -> HTTP 500 InternalFailure...
_Full entry and notes of one finding; its summary entry is in
[table-throughput-billing.md](../table-throughput-billing.md). Generated from ack-api-quirks
`services/dynamodb` (model 2012-08-10, controller commit 34b85e6); edit the finding in the lab, not here._

## Finding

- <a id="ddb-table-437"></a>**DDB-TABLE-437** `request-validation` · impact medium · unhandled (not handled in controller) · verified 2026-10-09
  **Empty-struct UpdateTable members: WarmThroughput {} and OnDemandThroughput {} -> HTTP 500 InternalFailure...**
  Idle PAY_PER_REQUEST and PROVISIONED tables, members sent alone (client-side validation disabled where
  botocore would refuse): WarmThroughput={} -> HTTP 500 InternalFailure on both tables (OnDemandThroughput={}
  likewise on both). StreamSpecification={} / {StreamViewType only} -> ValidationException 'Value null at
  streamSpecification.streamEnabled'; {StreamEnabled:true} without view type -> 'If stream is being enabled
  then UpdateViewType is required'; {StreamEnabled:false} with no stream -> 'Table has no stream to disable'.
  ProvisionedThroughput={} or with ONE member -> ValidationException naming the null member (no partial
  merge). GlobalSecondaryIndexUpdates [{}] -> 'One of ...Update, ...Create, ...Delete must be specified',
  [{Update:{}}]/[{Create:{}}]/[{Delete:{}}] -> 'Value null at ...indexName', [{Update:{IndexName}}] ->
  ResourceNotFoundException. ReplicaUpdates [] -> 'Member must have length greater than or equal to 1', [{}]
  -> 'There are no actions specified in the Replica Update Action'. TableClass='' / BillingMode='' -> enum
  ValidationException. SSESpecification={} on an AWS-owned-key table -> 'Table is already encrypted by
  default'; {Enabled:true} (no SSEType) -> 200 and the table re-encrypts with the AWS-managed key
  (SSEDescription.Status UPDATING ~22 s, SSE quota consumed); {SSEType:KMS} alone on the now-KMS table -> 200
  and another ~22 s re-encryption with no visible change; {Enabled:false, SSEType:KMS} -> 'SSEType can not be
  specified if Enabled is false'. AttributeDefinitions alone -> 'At least one of ...' (ignored member).
  - ACK: terminal_codes, custom_update, compare.nil_equals_zero_value · ops: UpdateTable · fields:
    WarmThroughput, OnDemandThroughput, StreamSpecification, ProvisionedThroughput,
    GlobalSecondaryIndexUpdates, ReplicaUpdates, SSESpecification, TableClass, BillingMode,
    AttributeDefinitions
  - repro: UpdateTable TableName=<idle table> WarmThroughput={} -> 500; UpdateTable
    SSESpecification={Enabled:true} -> 200 + 22 s re-encryption
  - measurements: sse_enabled_only_reencrypt_s=22.2
  - handling: not handled in the controller (as of commit 34b85e6)
  - related: [DDB-TABLE-178](../table-throughput-billing.md#ddb-table-178), [DDB-TABLE-161](../table-subresources.md#ddb-table-161), [DDB-TABLE-141](../table-streams-encryption-class.md#ddb-table-141), [DDB-TABLE-044](../table-streams-encryption-class.md#ddb-table-044), [DDB-TABLE-164](../table-indexes.md#ddb-table-164), [DDB-TABLE-015](../table-indexes.md#ddb-table-015),
    [DDB-TABLE-047](../table-throughput-billing.md#ddb-table-047), [DDB-TABLE-160](../table-indexes.md#ddb-table-160), [DDB-TABLE-449](../service.md#ddb-table-449), [DDB-TABLE-203](../table-replicas.md#ddb-table-203), [DDB-TABLE-223](../table-replicas.md#ddb-table-223), [DDB-TABLE-307](../table-replicas.md#ddb-table-307), [DDB-TABLE-225](../table-replicas.md#ddb-table-225),
    [DDB-TABLE-308](../table-replicas.md#ddb-table-308), [DDB-TABLE-294](../table-replicas.md#ddb-table-294) · evidence: table/creative/empty-struct-updates

## Notes

Extends [DDB-TABLE-178](../table-throughput-billing.md#ddb-table-178) (OnDemandThroughput {} -> 500) to WarmThroughput: a controller that materialises
spec.warmThroughput/onDemandThroughput as an empty struct (nil members) gets a 500 that is permanent for that
request and would be retried forever. All other nil-member shapes are ordinary ValidationExceptions, so the Go
SDK's omission of nil pointers is safe except for the two throughput structs. SSESpecification{Enabled:true}
without SSEType is NOT a no-op on a default-encrypted table: it is a real key migration that spends one of the
4 SSE changes per 24 h ([DDB-TABLE-142](../table-streams-encryption-class.md#ddb-table-142)). ProvisionedThroughput always needs both members (no server-side
merge).
