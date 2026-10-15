<!-- generated: ack-api-quirks dynamodb 2026-10-09 model=2012-08-10 controller=34b85e6 -->
# DDB-TABLE-143: TTL cooldown is real and ~31 min: 2nd UpdateTimeToLive -> ValidationException even though Describe shows ENABLED; invisible in Describe
_Full entry and notes of one finding; its summary entry is in
[table-subresources.md](../table-subresources.md). Generated from ack-api-quirks `services/dynamodb` (model
2012-08-10, controller commit 34b85e6); edit the finding in the lab, not here._

## Finding

- <a id="ddb-table-143"></a>**DDB-TABLE-143** `async-state-machine` · impact high · SUSPECTED CONTROLLER BUG · verified 2026-10-09, re-verified
  **TTL cooldown is real and ~31 min: 2nd UpdateTimeToLive -> ValidationException even though Describe shows ENABLED; invisible in Describe**
  UpdateTimeToLive(Enabled=false, same attr) issued 8.1 s after a successful enable - DescribeTimeToLive
  already ENABLED (ENABLING was never observed at 1 s polling) - failed with ValidationException (HTTP 400)
  'Time to live has been modified multiple times within a fixed interval'. Retried every 60 s: still rejected
  at 1811 s, first 200 at 1872 s after the enable (~31 min, not the documented hour). The successful disable
  immediately started a new cooldown: UpdateTimeToLive(Enabled=true) right after it -> the same
  ValidationException. Failed attempts do not seem to extend the window. Nothing in DescribeTimeToLive (only
  TimeToLiveStatus/AttributeName) exposes the cooldown. Content errors are checked before the cooldown:
  'TimeToLive is already enabled' / 'already disabled' / 'active on a different AttributeName' are returned
  instead of the cooldown message.
  - ACK: requeue, terminal_codes · ops: UpdateTimeToLive, DescribeTimeToLive · fields: TimeToLiveSpecification
  - repro: UpdateTimeToLive(Enabled=true) -> poll DescribeTimeToLive until ENABLED ->
    UpdateTimeToLive(Enabled=false) -> retry every 60s
  - measurements: cooldown_until_success_s=1871.7, last_rejection_s=1811.6, first_rejection_s=8.1,
    retry_interval_s=60, enabling_state_observed=false
  - handling: suspected controller bug - see Handling gaps
  - related: [DDB-TABLE-144](../table-subresources.md#ddb-table-144), [DDB-TABLE-145](../table-subresources.md#ddb-table-145), [DDB-TABLE-146](../table-subresources.md#ddb-table-146), [DDB-TABLE-147](../table-subresources.md#ddb-table-147), [DDB-TABLE-114](../service.md#ddb-table-114), [DDB-TABLE-377](../service.md#ddb-table-377) ·
    evidence: table/mutation-matrix/ttl-updates, table/creative/reverify-set-a1

## Notes

Hypotheses: H-S-001, H-S-043, H-S-009. Hypotheses: H-S-001 confirmed (cooldown; but ~31 min not 60), H-S-043
refuted (settled status does not unlock), H-S-009 confirmed (ValidationException is not a declared
UpdateTimeToLive error shape).

Suspected controller bug confirmed by evidence: The cooldown is real: a 2nd UpdateTimeToLive 8 s after a
successful enable -> ValidationException 'Time to live has been modified multiple times within a fixed
interval', rejected for ~31 min (1872 s), each successful change starts a new window, and DescribeTimeToLive
exposes nothing about it. Since the controller treats ValidationException as terminal and only tolerates the
'already disabled' prefix, any TTL spec edit within ~31 min of the previous change (including the controller's
own post-create enable) parks the CR in ACK.Terminal until the spec changes again. [DDB-TABLE-147](../table-subresources.md#ddb-table-147) compounds it:
re-sending enable or changing the attribute name -> 'already enabled' / 'active on a different AttributeName'
(also ValidationException), and a rename requires disable -> cooldown -> enable, so the controller cannot
complete a rename without tolerating/requeueing on these messages.
