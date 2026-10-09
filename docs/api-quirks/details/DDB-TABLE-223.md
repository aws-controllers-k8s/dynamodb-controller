<!-- generated: ack-api-quirks dynamodb 2026-10-09 model=2012-08-10 controller=34b85e6 -->
# DDB-TABLE-223: ReplicaUpdates error taxonomy: self/invalid/opt-in region, duplicate Create, no-op Update, mixed actions, Delete from the replica endpoint
_Full entry and notes of one finding; its summary entry is in [table-replicas.md](../table-replicas.md).
Generated from ack-api-quirks `services/dynamodb` (model 2012-08-10, controller commit 34b85e6); edit the
finding in the lab, not here._

## Finding

- <a id="ddb-table-223"></a>**DDB-TABLE-223** `error-code` · impact high · unhandled (not handled in controller) · verified 2026-10-09
  **ReplicaUpdates error taxonomy: self/invalid/opt-in region, duplicate Create, no-op Update, mixed actions, Delete from the replica endpoint**
  Table with ACTIVE replica us-east-1. create_already_replica -> ValidationException: Failed to create a the
  new replica of table with name: ‘ackq-c42a0c-rsv-none’ because one or more replicas already existed as
  tables. | self_region -> ValidationException: Cannot add or delete the local region through ReplicaUpdates.
  Use CreateTable, DeleteTable, or UpdateTable as required. | delete_self_region -> ValidationException:
  Cannot add or delete the local region through ReplicaUpdates. Use CreateTable, DeleteTable, or UpdateTable
  as required. | invalid_region -> ValidationException: Region us-fake-1 is not supported. The latest version
  of global tables are only supported in the following regions: [ap-south-2, ap-south-1, eu-south-1,
  eu-south-2, me-ce | uppercase_region -> ValidationException: Region EU-WEST-1 is not supported. The latest
  version of global tables are only supported in the following regions: [ap-south-2, ap-south-1, eu-south-1,
  eu-south-2, me-ce | optin_region_af_south_1 -> ValidationException: Failed to access the region:
  ‘af-south-1’. User is missing the permissions since the region is disabled. | empty_region ->
  AccessDeniedException: User: arn:aws:sts::<ACCOUNT>:assumed-role/<PRINCIPAL> is not authorized to perform:
  dynamodb:Scan on resource: arn:aws:dynamodb::<ACCOUNT>:table/ackq-c42a0c-r | update_region_only_no_change ->
  ValidationException: There are no actions specified in the Replica Update Action of the request. |
  empty_action -> ValidationException: There are no actions specified in the Replica Update Action of the
  request. | empty_replica_updates -> ParamValidationError: Parameter validation failed:
  Invalid length for parameter ReplicaUpdates, value: 0, valid min length: 1 | create_and_delete_same_element
  -> ValidationException: Update table operation with more than one type of replica actions not allowed. |
  update_non_replica -> ValidationException: Update global table operation failed because one or more replicas
  were not part of the global table. Please retry the request without these replicas: [eu-west-1]. |
  update_ppr_provisioned_override -> ValidationException: Neither ReadCapacityUnits nor WriteCapacityUnits can
  be specified when BillingMode is PAY_PER_REQUEST | update_same_table_class_STANDARD -> 200 OK |
  ppr_table_provisioned_override -> 200 OK | duplicate_create_same_region_twice -> 200 OK | delete_non_replica
  -> 200 OK | delete_base_region_from_replica_side_dryrun_invalid -> 200 OK | unknown_gsi_override ->
  ValidationException: Failed to create a the new replica of table with name: ‘ackq-c42a0c-rsv-none’ because
  one or more replicas already existed as tables. | create_from_replica_side_for_third_region_INVALID ->
  ValidationException: Region us-fake-1 is not supported. The latest version of global tables are only
  supported in the following regions: [ap-south-2, ap-south-1, eu-south-1, eu-south-2, me-ce
  - ACK: terminal_codes, custom_update · ops: UpdateTable · fields: ReplicaUpdates
  - repro: table with ACTIVE replica us-east-1 -> UpdateTable ReplicaUpdates with each malformed/duplicate
    action
  - handling: not handled in the controller (as of commit 34b85e6)
  - related: [DDB-TABLE-224](../table-replicas.md#ddb-table-224), [DDB-TABLE-265](../table-replicas.md#ddb-table-265), [DDB-TABLE-258](../table-global-tables.md#ddb-table-258), [DDB-TABLE-257](../table-global-tables.md#ddb-table-257), [DDB-TABLE-203](../table-replicas.md#ddb-table-203), [DDB-TABLE-307](../table-replicas.md#ddb-table-307),
    [DDB-TABLE-225](../table-replicas.md#ddb-table-225), [DDB-TABLE-308](../table-replicas.md#ddb-table-308), [DDB-TABLE-294](../table-replicas.md#ddb-table-294), [DDB-TABLE-437](../table-throughput-billing.md#ddb-table-437), [DDB-TABLE-250](../table-replicas.md#ddb-table-250), [DDB-TABLE-252](../table-replicas.md#ddb-table-252), [DDB-TABLE-251](../table-replicas.md#ddb-table-251),
    [DDB-TABLE-321](../table-replicas.md#ddb-table-321), [DDB-TABLE-229](../table-global-tables.md#ddb-table-229) · hypotheses: H-R-024, H-R-014, H-R-012 · evidence:
    table/error-taxonomy/replica-sync-validation

## Notes

CAVEAT: the sequence became entangled - 'ppr_table_provisioned_override' (Create eu-west-1 with
ProvisionedThroughputOverride on a PAY_PER_REQUEST table) was ACCEPTED and made eu-west-1 a real replica, so
'delete_non_replica' actually deleted that replica (200), 'duplicate_create_same_region_twice' ([Create
eu-west-1, Create eu-west-1]) was accepted and re-created it, 'delete_base_region_from_replica_side' (Delete
eu-west-1 issued from the us-east-1 endpoint) deleted it again (200), and 'unknown_gsi_override' hit the
'already existed' check for eu-west-1. Clean re-tests are in table/dependencies/replica-prerequisites. Stable
results: self region -> ValidationException 'Cannot add or delete the local region through ReplicaUpdates';
invalid/uppercase region -> ValidationException listing supported regions; opt-in region not enabled ->
ValidationException 'region is disabled'; empty RegionName -> AccessDeniedException (dynamodb:Scan on a
region-less ARN!); Update{RegionName only} and {} -> ValidationException 'There are no actions specified in
the Replica Update Action'; Create+Delete in one element -> 'more than one type of replica actions not
allowed'; Update{TableClassOverride:STANDARD} equal to current -> 200 and table UPDATING (not a rejected
no-op; refutes H-R-012 for this field); ProvisionedThroughputOverride via Update on PPR ->
ValidationException, but the same override inside Create is accepted (refutes the Create half of H-R-014).

Contradiction with [DDB-TABLE-307](../table-replicas.md#ddb-table-307), [DDB-TABLE-203](../table-replicas.md#ddb-table-203), [DDB-TABLE-225](../table-replicas.md#ddb-table-225): 223's behavior text records delete_non_replica
-> 200 and unknown_gsi_override -> 'already existed as tables'; 307 (clean re-test) and 203 show Delete of a
never-replica region -> ValidationException 'not part of the global table' and an unknown GSI override -> 200
silently accepted Resolution: 223's own CAVEAT explains it: eu-west-1 had become a real replica mid-sequence,
so those entries measured a real delete and a duplicate Create. 307/203 canonical for those two cases; 223
stays canonical for self/invalid/opt-in/empty region, mixed actions and Delete from the replica endpoint
