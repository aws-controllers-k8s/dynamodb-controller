<!-- generated: ack-api-quirks dynamodb 2026-10-09 model=2012-08-10 controller=34b85e6 -->
# DDB-TABLE-116: GSI table: backups usable 1.4s after ACTIVE; insights on a CREATING GSI: RNF 'Index not found' ~20s, then 'IndexStatus must be ACTIVE'
_Full entry and notes of one finding; its summary entry is in
[table-subresources.md](../table-subresources.md). Generated from ack-api-quirks `services/dynamodb` (model
2012-08-10, controller commit 34b85e6); edit the finding in the lab, not here._

## Finding

- <a id="ddb-table-116"></a>**DDB-TABLE-116** `async-state-machine` · impact high · handled · verified 2026-10-08
  **GSI table: backups usable 1.4s after ACTIVE; insights on a CREATING GSI: RNF 'Index not found' ~20s, then 'IndexStatus must be ACTIVE'**
  GSI table CREATING (16.1s): DescribeContinuousBackups [('TableNotFoundException', 400, 'Table not found:
  ackq-34ad39-s2'), ('OK', 200, '')]; UpdateContinuousBackups [('TableNotFoundException', 400, 'Table not
  found: ackq-34ad39-s2'), ('ContinuousBackupsUnavailableException', 400, 'Backups are being enabled for the
  table: ackq-34ad39-s2. Please retry later')]; UpdateContributorInsights(table)
  [('ResourceNotFoundException', 400, 'Requested resource not found: Table: ackq-34ad39-s2 not found'),
  ('ValidationException', 400, 'Table or Index is not in a valid state to update Key Access Insights:
  TableStatus must be ACTIVE to enable ContributorIn')]; UpdateContributorInsights(index)
  [('ResourceNotFoundException', 400, 'Requested resource not found: Table: ackq-34ad39-s2 not found'),
  ('ValidationException', 400, 'Table or Index is not in a valid state to update Key Access Insights:
  TableStatus must be ACTIVE to enable ContributorIn')]; DescribeContributorInsights(index)
  [('ResourceNotFoundException', 400, 'Requested resource not found: Table: ackq-34ad39-s2 not found'), ('OK',
  200, '')]. First success after ACTIVE (s): {'DescribeContinuousBackups': None,
  'DescribeContributorInsights': None, 'ListContributorInsights': None, 'DescribeContributorInsights(index)':
  None, 'DescribeTimeToLive': 0.0, 'UpdateContributorInsights': 0.1, 'UpdateContributorInsights(index)': 0.1,
  'UpdateContinuousBackups': 1.4}; never succeeded: []. During UpdateTable(Create gsi2) backfill
  (TableStatus=UPDATING): {'DescribeTimeToLive': 'OK', 'UpdateTimeToLive': 'OK', 'DescribeContinuousBackups':
  'OK', 'UpdateContinuousBackups': 'OK', 'DescribeContributorInsights': 'OK', 'UpdateContributorInsights':
  'OK', 'ListContributorInsights': 'OK', 'DescribeContributorInsights(index)': "ResourceNotFoundException
  (HTTP 400) 'Requested resource not found: Index: gsi2 not found for table: ackq-34ad39-s2'",
  'UpdateContributorInsights(index)': "ResourceNotFoundException (HTTP 400) 'Requested resource not found:
  Index: gsi2 not found for table: ackq-34ad39-s2'"}; index ops series: [{'t_s': 1.7, 'table': 'UPDATING',
  'gsi2': ['CREATING'], 'UpdateCI(index)': {'ok': False, 'code': 'ResourceNotFoundException', 'http': 400,
  'message': 'Requested resource not found: Index: gsi2 not found for table: ackq-34ad39-s2'},
  'DescribeCI(index)': {'ok': False, 'code': 'ResourceNotFoundException', 'http': 400, 'message': 'Requested
  resource not found: Index: gsi2 not found for table: ackq-34ad39-s2'}, 'ListCI': {'ok': True, 'code': None,
  'http': 200, 'message': ''}}, {'t_s': 21.8, 'table': 'UPDATING', 'gsi2': ['CREATING'], 'UpdateCI(index)':
  {'ok': False, 'code': 'ValidationException', 'http': 400, 'message': 'Table or Index is not in a valid state
  to update Key Access Insights: IndexStatus must be ACTIVE to enable ContributorInsights.'},
  'DescribeCI(index)': {'ok': True, 'code': None, 'http': 200, 'message': ''}, 'ListCI': {'ok': True, 'code':
  None, 'http': 200, 'message': ''}}]. After gsi2 ACTIVE: {'UpdateContributorInsights(index gsi2 ACTIVE)':
  {'ok': True, 'code': None, 'http': 200, 'message': ''}, 'DescribeContributorInsights(index gsi2)': {'ok':
  True, 'code': None, 'http': 200, 'message': ''}, 'ttl_after_updating_window': {'TimeToLiveStatus':
  'ENABLED', 'AttributeName': 'ttl'}}. GSI add timeline: [{'value': ('ACTIVE', ('ACTIVE', 'CREATING')),
  'from_s': 0.01, 'to_s': 468.55, 'duration_s': 468.54}, {'value': ('ACTIVE', ('ACTIVE', 'ACTIVE')), 'from_s':
  468.55, 'to_s': None, 'duration_s': None}].
  - ACK: synced.when, requeue, terminal_codes · ops: UpdateContributorInsights, DescribeContributorInsights,
    UpdateContinuousBackups, UpdateTimeToLive, UpdateTable
  - repro: CreateTable with GSI; loop ops; UpdateTable Create gsi2; ops during backfill
  - measurements: gsi_table_create_to_active_s=16.1, cb_update_first_ok_after_active_s=1.4,
    insights_index_update_first_ok_after_active_s=0.1, gsi_add_table_updating_s=42,
    gsi_add_index_creating_s=468.5
  - handling: handled via `generator.yaml:104-109; pkg/resource/table/hooks.go:72-93; generator.yaml:46-50; pkg/resource/table/hooks_continuous_backup.go:27-94; generator.yaml:78-83; pkg/resource/table/hooks.go:882-960; templates/hooks/table/sdk_create_post_set_output.go.tpl:1-4; bcd26e1`
  - related: [DDB-TABLE-148](../table-indexes.md#ddb-table-148), [DDB-TABLE-138](../table-indexes.md#ddb-table-138), [DDB-TABLE-149](../table-indexes.md#ddb-table-149), [DDB-TABLE-163](../table-subresources.md#ddb-table-163), [DDB-TABLE-458](../table-indexes.md#ddb-table-458), [DDB-TABLE-165](../table-indexes.md#ddb-table-165),
    [DDB-TABLE-168](../table-indexes.md#ddb-table-168), [DDB-TABLE-169](../table-indexes.md#ddb-table-169), [DDB-TABLE-123](../table-indexes.md#ddb-table-123), [DDB-TABLE-134](../table-indexes.md#ddb-table-134), [DDB-TABLE-161](../table-subresources.md#ddb-table-161), [DDB-TABLE-380](../table-indexes.md#ddb-table-380), [DDB-TABLE-094](../table-subresources.md#ddb-table-094),
    [DDB-TABLE-137](../table-subresources.md#ddb-table-137), [DDB-TABLE-095](../table-subresources.md#ddb-table-095), [DDB-TABLE-344](../table-subresources.md#ddb-table-344), [DDB-TABLE-093](../table-subresources.md#ddb-table-093), [DDB-TABLE-112](../table-subresources.md#ddb-table-112) · evidence:
    table/state-machine/subresource-admissibility

## Notes

Hypotheses: H-S-025, H-S-015, H-S-103. Hypotheses: H-S-025 (partially confirmed: with the TABLE CREATING,
index-level Update/Describe return the same codes as table-level, i.e. ResourceNotFoundException 'Table: X not
found' then ValidationException 'TableStatus must be ACTIVE'; with the table ACTIVE/UPDATING and a NEW GSI in
IndexStatus=CREATING (UpdateTable Create), Update/Describe(IndexName=gsi2) -> ResourceNotFoundException
'Index: gsi2 not found for table: X' for the whole 7.8 min backfill, 200 once ACTIVE), H-S-015 (confirmed:
during the UPDATING window of the GSI add, UpdateTimeToLive(enable), UpdateContinuousBackups,
UpdateContributorInsights(table) and all Describes returned 200 - TTL became ENABLED on the UPDATING table),
H-S-103 (the GSI table's pre-availability lag after ACTIVE was 1.4 s vs 2.6 s for the plain table - not
longer). TableStatus went back to ACTIVE after ~42 s while gsi2 stayed CREATING for 468 s.

Contradiction with [DDB-TABLE-148](../table-indexes.md#ddb-table-148): 116's title/notes claim insights on the CREATING GSI are
ResourceNotFoundException 'for the whole 7.8 min backfill' and label the TableStatus=UPDATING window
'backfill'; 116's own series shows RNF 'Index: gsi2 not found' only at t=1.7 s and at t=21.8 s (still
UPDATING) DescribeContributorInsights(index) -> 200 and Update -> ValidationException 'IndexStatus must be
ACTIVE to enable ContributorInsights'; per 148/138 the UPDATING window is resource allocation
(Backfilling=false), the backfill runs with TableStatus=ACTIVE Resolution: keep 116 (data is sound,
measurements 42 s / 468 s agree with 148) with the corrected title; 148 is canonical for phase naming; an
insights reconciler must treat RNF-on-index as transient and then wait for IndexStatus=ACTIVE

Contradiction with [DDB-TABLE-148](../table-indexes.md#ddb-table-148), [DDB-TABLE-458](../table-indexes.md#ddb-table-458): 148 attributes the ~16.5-min (990 s) GSI backfill to a
PAY_PER_REQUEST table vs 507-537 s on PROVISIONED; 458 measured 510-539 s on four PPR tables and 1001-1003 s
on two PPR tables, 116 468 s on a PPR table - the duration is bimodal (~8.5 min or ~16.5 min) and not a
billing-mode effect Resolution: keep both; 148's title range (7-16 min) stands, but drop the billing-mode
attribution when summarising; readiness timeouts must allow >=17 min regardless of BillingMode
