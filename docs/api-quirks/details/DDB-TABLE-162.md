<!-- generated: ack-api-quirks dynamodb 2026-10-09 model=2012-08-10 controller=34b85e6 -->
# DDB-TABLE-162: UpdateTable AttributeDefinitions is only used for GSI Create; unused, stale or type-conflicting entries are silently ignored elsewhere
_Full entry and notes of one finding; its summary entry is in [table-indexes.md](../table-indexes.md).
Generated from ack-api-quirks `services/dynamodb` (model 2012-08-10, controller commit 34b85e6); edit the
finding in the lab, not here._

## Finding

- <a id="ddb-table-162"></a>**DDB-TABLE-162** `request-validation` · impact high · handled · verified 2026-10-08
  **UpdateTable AttributeDefinitions is only used for GSI Create; unused, stale or type-conflicting entries are silently ignored elsewhere**
  GSI Create without AttributeDefinitions -> ValidationException 'AttributeDefinitions is not specified for
  index: gsi3'. AttributeDefinitions containing only the new key attribute is accepted (no need to resend
  existing ones). AttributeDefinitions with an extra unused attribute, with an attribute left over from a
  deleted index, or with the table key declared as N while it is S, sent together with a GSI Update, a GSI
  Delete, DeletionProtectionEnabled or a ProvisionedThroughput change, are all accepted (200) and
  DescribeTable AttributeDefinitions is unchanged afterwards. A GSI Create with a stale extra attribute in the
  list is also accepted. AttributeDefinitions as the only parameter -> the generic 'At least one of
  ProvisionedThroughput, BillingMode, ...' ValidationException.
  - ACK: custom_update, compare.is_ignored+delta_pre_compare · ops: UpdateTable · fields:
    AttributeDefinitions, GlobalSecondaryIndexUpdates.Create
  - repro: UpdateTable AttributeDefinitions=[{pk,N},{a,S},{b,S}] GlobalSecondaryIndexUpdates=[Update gsi1 PT
    3/3] on a table whose pk is S -> 200
  - handling: handled via `pkg/resource/table/hooks_global_secondary_indexes.go:175-178; pkg/resource/table/hooks_global_secondary_indexes.go:298-301`
  - related: [DDB-TABLE-433](../table-streams-encryption-class.md#ddb-table-433), [DDB-TABLE-156](../table-throughput-billing.md#ddb-table-156), [DDB-TABLE-358](../table-throughput-billing.md#ddb-table-358), [DDB-TABLE-056](../table-throughput-billing.md#ddb-table-056), [DDB-TABLE-174](../table-indexes.md#ddb-table-174), [DDB-TABLE-199](../table-replicas.md#ddb-table-199),
    [DDB-TABLE-224](../table-replicas.md#ddb-table-224), [DDB-TABLE-127](../table-indexes.md#ddb-table-127), [DDB-TABLE-151](../table-indexes.md#ddb-table-151), [DDB-TABLE-357](../table-indexes.md#ddb-table-357), [DDB-TABLE-359](../table-indexes.md#ddb-table-359) · evidence:
    table/mutation-matrix/gsi-update-granularity

## Notes

Confirms H-T-019 (must send the new attribute; only-new is enough). Refutes H-T-020 and H-T-128 for
UpdateTable (CreateTable enforces the exact match, UpdateTable does not) and refutes H-T-066's expectation of
a ValidationException: a changed key type passes through silently, so the controller itself must flag
KeySchema/AttributeType drift.
