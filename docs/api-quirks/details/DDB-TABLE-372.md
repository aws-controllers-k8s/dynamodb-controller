<!-- generated: ack-api-quirks dynamodb 2026-10-09 model=2012-08-10 controller=34b85e6 -->
# DDB-TABLE-372: Tag charset is the AWS tag regex: letters/digits/spaces and + - = . _ : / @ only; emoji, comma, %, quotes, &, *, ;, ?, |, \, <>, ~ rejected
_Full entry and notes of one finding; its summary entry is in [service.md](../service.md). Generated from
ack-api-quirks `services/dynamodb` (model 2012-08-10, controller commit 34b85e6); edit the finding in the lab,
not here._

## Finding

- <a id="ddb-table-372"></a>**DDB-TABLE-372** `request-validation` · impact medium · unhandled (not handled in controller) · verified 2026-10-09
  **Tag charset is the AWS tag regex: letters/digits/spaces and + - = . _ : / @ only; emoji, comma, %, quotes, &, *, ;, ?, |, \, <>, ~ rejected**
  TagResource accepted values/keys with Cyrillic ('ключ'), Latin-1 ('café'), CJK ('表'), NBSP and ideographic
  space, and the value 'a+b-c=d.e_f:g/h@i'. Rejected with ValidationException 'The Tag Value provided is
  invalid, Value: ...' (or 'The Tag Key provided is invalid'): emoji '🔑' (key and value), '100%', 'a,b',
  'it''s "q"', '(x)[y]{z}', 'a&b', 'a*b', 'a;b', 'a?b', 'a|b', 'a\b', '<a>', '~a'. Each rejection fails the
  whole TagResource call (all-or-nothing, [DDB-TABLE-107](../service.md#ddb-table-107)). Letters of any script are fine; punctuation other
  than + - = . _ : / @ is not.
  - ACK: terminal_codes, tags.custom-sync, docs-only · ops: TagResource, CreateTable · fields: Tags
  - repro: TagResource one tag at a time with each character class (22 cases), 2.2 s apart to respect the
    per-table tag write lock.
  - handling: not handled in the controller (as of commit 34b85e6)
  - related: [DDB-TABLE-106](../service.md#ddb-table-106), [DDB-TABLE-107](../service.md#ddb-table-107), [DDB-TABLE-110](../table.md#ddb-table-110), [DDB-TABLE-105](../service.md#ddb-table-105), [DDB-TABLE-108](../service.md#ddb-table-108), [DDB-TABLE-109](../service.md#ddb-table-109),
    [DDB-TABLE-368](../service.md#ddb-table-368) · evidence: table/creative/xs-kms-echo-tags, table/creative/xs-identity-tags-streams

## Notes

Extends [DDB-TABLE-106](../service.md#ddb-table-106) ('#' rejected, 'unicode accepted') with the full accept/reject list; comma-separated or
percent values copied from Kubernetes labels/annotations are a terminal ValidationException for the whole tag
set, and because CreateTable carries Tags inline ([DDB-TABLE-110](../table.md#ddb-table-110)) a single bad tag value also blocks table
creation.
