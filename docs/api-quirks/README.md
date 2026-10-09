<!-- generated: ack-api-quirks dynamodb 2026-10-09 model=2012-08-10 controller=34b85e6 -->
# DynamoDB API quirks (dynamodb-controller)

Quirks are observed AWS API behaviors that matter when writing or reviewing this controller: state machines,
error codes, defaults, normalization, read gaps, tag semantics, quotas and the like. Each finding carries its
ACK implication and the controller's current handling status (handled / unhandled / suspected bug).

Provenance: generated from the ack-api-quirks lab (https://github.com/aws-controllers-k8s/ack-api-quirks, path
`services/dynamodb/`), where every finding's probe evidence lives. Re-render with `quirks render dynamodb
--controller-dir <this repo>`; the lab is the source of truth, so edit findings there, not here.

Reading budget: the Documents table gives each file's line count. Long notes and compacted entries live in
`details/<finding id>.md` (243 files, one finding each; full entry plus full notes), linked from the entries
as `full notes:` or `see:`; read them on demand.

Escape hatch: text inside preserved blocks (`preserved:start` ... `preserved:end` comments in the Overview,
Open questions and Supplementary notes sections) survives re-renders, and files without the generated marker
(for example under `supplementary/`) are never touched. Controller references in `handling` point at files and
symbols; where a line range is given it refers to controller commit 34b85e6 (see each document's marker), not
to the current tree.

## Load guide

| task | read |
| --- | --- |
| add a resource | service.md (inventory, scope verdicts) + every doc listing that resource in the table below (start with its catch-all: import.md, export.md, backup.md, table.md) |
| fix a reconcile bug | service.md `Handling gaps summary` (every suspected bug and partial handling, with its doc) first, then the entry and its doc's `Handling gaps (bugs to file)` |
| add a field / review a PR | the catch-all's `Field matrix` (C/U/R per leaf) + the doc(s) whose title or description covers the field or operation (see Documents) |
| e2e test work | each relevant doc's Overview (`Timing you should expect`, where present) then its `E2E timing` table (observed durations, n) |
| adoption / import of existing resources | the `Adoption and first-sync hazards` and `Delete semantics` sections of the resource docs |
| codegen / generator.yaml changes | service.md `Codegen notes` + the `Scope` and `Field behavior` sections |
| what the controller already handles | [service.md 'Ground truth catalog'](service.md#ground-truth) (controller hooks catalog, `GT-*` ids) |
| find the doc of a finding id | [index.md](index.md) (id -> document map); ids in the text are links already |
| full notes or full entry of one finding | `details/<finding id>.md` (linked from the entry) |

## How to read an entry

- Header line: `- <a id="<id lowercased>"></a>**<id>** <category> · impact <high|medium|low> · <handling
  label> · verified <date>[, re-verified][ · status: unverified|stale]` (the category is a code span). The
  anchor makes the entry addressable as `<doc>.md#<id lowercased>`; every finding id mentioned anywhere in
  these documents (overviews, `related:` lists, notes, tables) is rendered as a link to that anchor, in its
  own document or across documents. `index.md` is the plain id -> document map.
- Title line: the bold title; `(hypothesis refuted; behavior confirmed)` after it means the lab hypothesis
  behind the probe was refuted while the behavior described is confirmed (in this dataset `status: refuted` is
  about the hypothesis, never about the observation). `· status: unverified` in the header marks doc claims
  and untested items; they are also listed under `Open questions`.
- Handling labels (header) and the `- handling:` bullet: `handled` = `handled via <controller refs>`;
  `partially handled` = `partially handled via <refs>` (also listed under `Handling gaps`); `unhandled (not
  handled in controller)` = `not handled in the controller (as of commit <sha>)`; `SUSPECTED CONTROLLER BUG` =
  `suspected controller bug - see Handling gaps`; `n/a` = no handling line; `tracked in GitHub issue (not
  handled)` = the stored handling reference is an open issue, rendered `tracked in <url> (not handled)` with
  any code refs after it. Line ranges in refs refer to controller commit 34b85e6.
- Body bullets: `ACK:` implications · `ops:` · `fields:`; `repro:`; `measurements:` (k=v, nested dicts as
  k.sub=v); `handling:`; `related:` (links) · `hypotheses:` · `evidence:` (probe ids under
  `services/dynamodb/probes/`); `notes:` inline or an excerpt plus `full notes: details/<id>.md`. A probe's
  own truncation inside a stored behavior is shown as `[truncated in evidence]`; the renderer never cuts a
  high-impact behavior. Compacted medium entries (oversized docs only) are header + title + first sentence +
  `see: details/<id>.md`.
- `GT-*` ids are controller hooks catalog entries (behaviors the controller already handles, with the
  mechanism and file:line): [service.md 'Ground truth catalog'](service.md#ground-truth); they render as links
  followed by `(controller hooks catalog entry)`.
- Overview imperatives (must/never/always) are evidence-derived recommendations from the lab, not project
  decisions.

## Documents

| file | title | covers | resources | canonical findings | high | unhandled | suspect-bug | last verified | lines |
| --- | --- | --- | --- | --- | --- | --- | --- | --- | --- |
| [service.md](service.md) | DynamoDB: service facts and cross-cutting behaviors | Model facts, inventory, service-wide findings, handling gaps summary, ground truth catalog | all | 46 | 20 | 33 | 1 | 2026-10-09 | 1233 |
| [import.md](import.md) | ImportTable (scope investigation) | ImportTable/DescribeImport/ListImports behavior and the scope verdict for an Import resource. | Import | 20 | 8 | 18 | 0 | 2026-10-09 | 526 |
| [export.md](export.md) | ExportTableToPointInTime (scope investigation) | Export job lifecycle, idempotency and the scope verdict for an Export resource. | Export | 26 | 6 | 21 | 0 | 2026-10-09 | 565 |
| [backup.md](backup.md) | Backup resource | CreateBackup/DescribeBackup/DeleteBackup/ListBackups: lifecycle, identity, limits, scope verdict. | Backup | 28 | 13 | 14 | 0 | 2026-10-09 | 557 |
| [table-restore.md](table-restore.md) | Table restores (RestoreTableFromBackup, RestoreTableToPointInTime) | Restore behavior on Table: what is and is not restored, overrides, admissibility while restoring, errors and durations. | Table | 20 | 12 | 17 | 0 | 2026-10-09 | 508 |
| [table-global-tables.md](table-global-tables.md) | Global tables: multi-region consistency, witnesses, legacy 2017.11.29 API | MRSC/STRONG groups and witnesses, the legacy CreateGlobalTable/UpdateGlobalTable/GlobalTableSettings APIs and their scope verdicts. | Table, GlobalTable, GlobalTableSettings | 22 | 11 | 6 | 1 | 2026-10-09 | 521 |
| [table-replicas.md](table-replicas.md) | Table replicas (ReplicaUpdates, version 2019.11.21) | Replica create/update/delete rules, prerequisites, settings replication across regions, per-region views, autoscaling coupling, timings. | Table, TableReplicaAutoScaling | 51 | 36 | 36 | 3 | 2026-10-09 | 1430 |
| [table-streams-encryption-class.md](table-streams-encryption-class.md) | Table streams, encryption (SSE/KMS), table class, deletion protection | Round-trip, mutation rules, quotas, KMS key states and async phases for streams, encryption, table class and deletion protection. | Table | 68 | 42 | 42 | 2 | 2026-10-09 | 1550 |
| [table-policy-kinesis-autoscaling.md](table-policy-kinesis-autoscaling.md) | Table resource policy, Kinesis streaming destination and replica auto scaling | Put/Get/DeleteResourcePolicy semantics, Kinesis streaming destination lifecycle, and the Application Auto Scaling facade behind UpdateTableReplicaAutoScaling. | Table | 71 | 41 | 53 | 0 | 2026-10-09 | 1492 |
| [table-subresources.md](table-subresources.md) | Table sub-resources (TTL, PITR, Contributor Insights) | TTL, point-in-time recovery and Contributor Insights: API pairs managed outside CreateTable/UpdateTable/DescribeTable. | Table | 38 | 16 | 21 | 1 | 2026-10-09 | 916 |
| [table-indexes.md](table-indexes.md) | Table secondary indexes | GSI/LSI create/update/delete granularity, backfill state machine, validation, throughput interplay. | Table | 48 | 21 | 23 | 1 | 2026-10-09 | 1070 |
| [table-throughput-billing.md](table-throughput-billing.md) | Table billing mode and throughput (provisioned, on-demand, warm) | Billing-mode switches, provisioned/on-demand/warm throughput rules, decrease budgets, key schema immutability. | Table | 53 | 23 | 34 | 0 | 2026-10-09 | 1176 |
| [table.md](table.md) | Table (lifecycle, identity, errors, tags, limits) | State machine, identity and idempotency, error taxonomy, delete semantics, tags, account limits; the catch-all for Table findings not covered by the other Table documents. | Table | 24 | 4 | 15 | 0 | 2026-10-09 | 427 |

`unhandled` counts handling unhandled plus handled/partial entries whose only reference is an open GitHub
issue (rendered `tracked in <url> (not handled)`). [index.md](index.md) maps every finding id to its document.

Medium-impact entries are compacted (header, title, first sentence, link to `details/`) in:
table-streams-encryption-class.md, table-policy-kinesis-autoscaling.md. High-impact entries are always
rendered in full.

## Resources

| resource | scope verdict(s) | docs |
| --- | --- | --- |
| Backup | implement, implemented | [backup.md](backup.md) |
| Export | implement | [export.md](export.md) |
| GlobalTable | skip:deprecated, implemented | [table-global-tables.md](table-global-tables.md) |
| GlobalTableSettings | skip:deprecated | [table-global-tables.md](table-global-tables.md) |
| Import | field-on-parent | [import.md](import.md) |
| Table | field-on-parent, implement, implemented | [table-restore.md](table-restore.md), [table-global-tables.md](table-global-tables.md), [table-replicas.md](table-replicas.md), [table-streams-encryption-class.md](table-streams-encryption-class.md), [table-policy-kinesis-autoscaling.md](table-policy-kinesis-autoscaling.md), [table-subresources.md](table-subresources.md), [table-indexes.md](table-indexes.md), [table-throughput-billing.md](table-throughput-billing.md), [table.md](table.md) |
| TableReplicaAutoScaling | skip:no-crud | [table-replicas.md](table-replicas.md) |

## Totals

- findings: 548 (515 canonical, 33 duplicates) · suspect-bug: 9 · documents: 12 + service.md · details pages: 243
- model: 2012-08-10 (service/dynamodb v1.39.8) · controller commit: 34b85e6 · render date: see the marker on line 1 of each document

## Supplementary documents

None (files in this directory without the generated marker are listed here and never touched).

## AGENTS.md

The repository root `AGENTS.md` carries a short pointer block to this directory (between `ack-api-quirks:start` and
`ack-api-quirks:end` comments, replaced on every render); `CLAUDE.md` includes it via `@AGENTS.md`.
