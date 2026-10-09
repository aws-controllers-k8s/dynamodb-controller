<!-- generated: ack-api-quirks dynamodb 2026-10-09 model=2012-08-10 controller=34b85e6 -->
# DDB-TABLE-108: 50-tag limit is a ValidationException (not LimitExceededException), checked against the post-merge set; no NextToken at 50 tags
_Full entry and notes of one finding; its summary entry is in [service.md](../service.md). Generated from
ack-api-quirks `services/dynamodb` (model 2012-08-10, controller commit 34b85e6); edit the finding in the lab,
not here._

## Finding

- <a id="ddb-table-108"></a>**DDB-TABLE-108** `quota-limit` · impact medium · unhandled (not handled in controller) · verified 2026-10-08
  **50-tag limit is a ValidationException (not LimitExceededException), checked against the post-merge set; no NextToken at 50 tags**
  With 50 tags present: TagResource {k51} -> ValidationException 'Number of Tags exceed the current limit for
  the provided ResourceArn'. {k01:new,k51} (post-merge 51) -> ValidationException 'Number of Tags exceed the
  current limit for the provided ResourceArn'. {k01:new} alone -> 200. All 50 existing keys with new values in
  one call -> 200 (k01 became v01b). 50 new distinct keys in one call -> ValidationException 'Number of Tags
  exceed the current limit for the provided ResourceArn'; 51 tags in one call -> ValidationException 'Number
  of Tags exceed the current limit for the provided ResourceArn'. TagResource of a 51st key immediately
  (<100ms) after UntagResource of another key -> LimitExceededException 'Subscriber limit exceeded: Table tags
  are being updated: ackq-202220-tgv'; after the untag became visible -> 200. ListTagsOfResource at 50 tags
  returned 50 tags with NextToken present=False. 49 tags in one TagResource call were visible after 1.53 s.
  - ACK: tags.custom-sync, terminal_codes · ops: TagResource, ListTagsOfResource, UntagResource · fields:
    Tags, NextToken
  - repro: TagResource with 49 tags; then the listed single-call variants
  - measurements: fifty_tags_visible_lag_s=1.53
  - handling: not handled in the controller (as of commit 34b85e6)
  - related: [DDB-TABLE-102](../service.md#ddb-table-102), [DDB-TABLE-103](../service.md#ddb-table-103), [DDB-TABLE-105](../service.md#ddb-table-105), [DDB-TABLE-173](../service.md#ddb-table-173), [DDB-TABLE-440](../table.md#ddb-table-440), [DDB-TABLE-441](../table.md#ddb-table-441),
    [DDB-TABLE-130](../service.md#ddb-table-130), [DDB-TABLE-104](../table.md#ddb-table-104), [DDB-TABLE-106](../service.md#ddb-table-106), [DDB-TABLE-107](../service.md#ddb-table-107), [DDB-TABLE-109](../service.md#ddb-table-109), [DDB-TABLE-110](../table.md#ddb-table-110), [DDB-TABLE-368](../service.md#ddb-table-368),
    [DDB-TABLE-372](../service.md#ddb-table-372), [DDB-TABLE-073](../table.md#ddb-table-073) · evidence: table/tags/validation-upsert-limits

## Notes

Refutes the LimitExceededException part of H-T-065/H-T-133/H-T-137: the count limit surfaces as
ValidationException 'Number of Tags exceed the current limit for the provided ResourceArn' (HTTP 400).
Confirms the post-merge check of H-T-137 (upserting existing keys at 50 tags is fine; any new key is not).
Pagination never kicks in at 50 tags. Because the limit is checked against the server's current (eventually
consistent) set, an UntagResource followed immediately by TagResource of a replacement key hits the per-table
lock first.
