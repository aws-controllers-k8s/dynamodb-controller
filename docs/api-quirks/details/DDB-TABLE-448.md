<!-- generated: ack-api-quirks dynamodb 2026-10-09 model=2012-08-10 controller=34b85e6 -->
# DDB-TABLE-448: HTTP 500 catalogue: InternalFailure (EMPTY message) / InternalServerError ('Internal server error', KMS text) are deterministic shape bugs
_Full entry and notes of one finding; its summary entry is in [service.md](../service.md). Generated from
ack-api-quirks `services/dynamodb` (model 2012-08-10, controller commit 34b85e6); edit the finding in the lab,
not here._

## Finding

- <a id="ddb-table-448"></a>**DDB-TABLE-448** `error-code` · impact high · unhandled (not handled in controller) · verified 2026-10-09
  **HTTP 500 catalogue: InternalFailure (EMPTY message) / InternalServerError ('Internal server error', KMS text) are deterministic shape bugs**
  All HTTP 500 responses seen in 9562 error records are reproducible request-shape bugs, not transient faults:
  code 'InternalFailure' with an EMPTY message (UpdateTable WarmThroughput={} / OnDemandThroughput={} / GSI
  Update WarmThroughput on a ghost index; 1.5-1.8 s latency), code 'InternalServerError' with 'Internal server
  error' (ListGlobalTables RegionName=<bogus>), and 'InternalServerError' with 'KMS internal error:
  com.amazonaws.services.kms.model.AWSKMSException: EncryptionContext is supported only when creating a grant
  for a symmetric encryption KMS key. (Service: AWSKMS; Status Code: 400; Error Code: ValidationException;
  Request ID: <UUID>; Proxy: null)' (asymmetric CMK). A controller that retries 5xx forever never converges;
  the message is empty for the most common one, so only the (operation, request shape) identifies it.
  Validation order measured here: WarmThroughput={} on a MISSING table and combined with other members -> see
  live records (whether the 500 fires before ResourceNotFound / 'must be the only operation').
  - ACK: terminal_codes, requeue · ops: UpdateTable, CreateTable, ListGlobalTables · fields: WarmThroughput,
    OnDemandThroughput, KMSMasterKeyId
  - repro: UpdateTable TableName=<any> WarmThroughput={}; ListGlobalTables RegionName=bogus-region-1
  - handling: not handled in the controller (as of commit 34b85e6)
  - related: [DDB-TABLE-437](../table-throughput-billing.md#ddb-table-437), [DDB-TABLE-178](../table-throughput-billing.md#ddb-table-178), [DDB-TABLE-161](../table-subresources.md#ddb-table-161), [DDB-TABLE-022](../table-streams-encryption-class.md#ddb-table-022), [DDB-TABLE-456](../service.md#ddb-table-456), [DDB-TABLE-458](../table-indexes.md#ddb-table-458) ·
    evidence: table/creative/error-message-regions

## Notes

Classes: RETRY = TRANSIENT-RETRY, clears by itself (typical wait given); WAIT = TRANSIENT-WAIT-FOR-STATE,
clears when the named status changes; SPEC = PERMANENT-SPEC, user must change the spec / fix an external
dependency; QUOTA = PERMANENT-QUOTA, needs a quota increase or the stated 1h/24h/30d window. Messages mined
from 119 past evidence.jsonl files (9562 error records, 29 codes; mine_errors.py -> mined-catalogue.txt in
this probe dir) plus this probe's live runs in us-west-2 and us-east-1; <TBL>/<ARN>/<TS>/<N>/<REGION>/<IDX>
mark request-specific tokens. Machine-readable form: catalogue.py in the probe directory.

CATALOGUE (InternalFailure):
- [SPEC] <EMPTY MESSAGE> (ops: UpdateTable; HTTP 500 'InternalFailure': WarmThroughput={} /
OnDemandThroughput={} / GSI Update WarmThroughput on an unknown index; deterministic (11/11), ~1.5-1.8 s; fix
the request)
- [SPEC] Internal server error (ops: ListGlobalTables,UpdateTable; HTTP 500: ListGlobalTables
RegionName=<bogus>; deterministic)
- [SPEC] KMS internal error: com.amazonaws.services.kms.model.AWSKMSException: EncryptionContext is supported
only when creating a grant for a symmetric encryption KMS key. (Service: AWSKMS; Status Code: 400; Error Code:
ValidationException; Request ID: <UUID>; Proxy: null) (ops: CreateTable,UpdateTable; HTTP 500 for an
asymmetric CMK; permanent until the key is changed; carries a KMS request id)

LIVE this run (region/label: http code latency 'message'):
us-east-1/ise_warm_empty: 500 InternalFailure 80ms ''
us-west-2/ise_warm_empty: 500 InternalFailure 22ms ''
us-east-1/ise_odt_empty: 500 InternalFailure 74ms ''
us-west-2/ise_odt_empty: 500 InternalFailure 16ms ''
us-east-1/ise_warm_empty_missing_table: 400 ResourceNotFoundException 69ms 'Requested resource not found:
Table: ackq-e3283a-emr-e1-missing not found'
us-west-2/ise_warm_empty_missing_table: 400 ResourceNotFoundException 12ms 'Requested resource not found:
Table: ackq-e3283a-emr-w2-missing not found'
us-east-1/ise_warm_empty_plus_dp: 200 OK 72ms
us-west-2/ise_warm_empty_plus_dp: 200 OK 13ms
us-east-1/ise_warm_empty_plus_pt: 400 ValidationException 74ms 'One or more parameter values were invalid:
Neither ReadCapacityUnits nor WriteCapacityUnits can be specified when BillingMode is PAY_PER_REQUEST'
us-west-2/ise_warm_empty_plus_pt: 400 ValidationException 13ms 'One or more parameter values were invalid:
Neither ReadCapacityUnits nor WriteCapacityUnits can be specified when BillingMode is PAY_PER_REQUEST'
us-east-1/ise_warm_empty_bad_name: 400 ValidationException 64ms '1 validation error detected: Value 'ab' at
'tableName' failed to satisfy constraint: Member must have length greater than or equal to 3'
us-west-2/ise_warm_empty_bad_name: 400 ValidationException 6ms '1 validation error detected: Value 'ab' at
'tableName' failed to satisfy constraint: Member must have length greater than or equal to 3'
us-east-1/ise_lgt_bogus_region: 500 InternalServerError 65ms 'Internal server error'
us-west-2/ise_lgt_bogus_region: 500 InternalServerError 7ms 'Internal server error'
us-east-1/lgt_empty_region: 500 InternalServerError 199ms 'Internal server error'
us-west-2/lgt_empty_region: 500 InternalServerError 28ms 'Internal server error'
us-east-1/lgt_upper_region: 500 InternalServerError 64ms 'Internal server error'
us-west-2/lgt_upper_region: 500 InternalServerError 7ms 'Internal server error'
us-east-1/lgt_cn_region: 200 OK 80ms
us-west-2/lgt_cn_region: 200 OK 20ms
us-east-1/lgt_optin_region: 200 OK 79ms
us-west-2/lgt_optin_region: 200 OK 21ms
us-east-1/lgt_valid_region: 200 OK 82ms
us-west-2/lgt_valid_region: 200 OK 21ms

Validation order measured live: WarmThroughput={} on a MISSING table -> ResourceNotFoundException (existence
check first); WarmThroughput={} + ProvisionedThroughput on a PPR table -> the PT ValidationException;
WarmThroughput={} + DeletionProtectionEnabled -> 200 OK (the empty struct is silently dropped when any other
member is present - no 'must be the only operation'); TableName too short -> shape ValidationException.
ListGlobalTables: RegionName '' / 'US-WEST-2' / 'bogus-region-1' -> 500; 'cn-north-1' and 'af-south-1' (real
regions of other partitions / opt-in) -> 200 []. Latency of the 500s this run: 6-80 ms (vs 0.7-2.8 s in the
Oct-8 evidence).

Contradiction with [DDB-TABLE-458](../table-indexes.md#ddb-table-458): 448 (title + behavior) says every HTTP 500 seen is a deterministic,
permanent request-shape bug with exactly two codes / three messages (InternalFailure '', InternalServerError
'Internal server error' / KMS text); 458 observed InternalServerError 'Table is under system maintenance,
please try again later' for a table WarmThroughput change during GSI resource allocation - a state conflict
(the same request shape is valid on an idle table, 155) that clears when the index settles Resolution: keep
both; 448 stays the catalogue but must list 458's variant as a WAIT-class (retry-after-state-change) 500;
448's title corrected (title_fixes). Controller rule: a 5xx is permanent only for the {} / ghost-index shapes,
not for every 5xx
