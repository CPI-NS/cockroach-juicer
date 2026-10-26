# Progress reporting

## Establish ownership before launch

The supervising agent remains accountable for the run, even when execution is
delegated. Give the operator the supervisor's message target, run/output path,
authorized scope, resource deadline and collection/cleanup allowance. Record the
operator ID, actual model, observer handle and event paths in the existing run
handoff. A separate guardian is optional; it does not replace the supervisor.

Attach a persistent observer to the event sources in [commands](commands.md).
Verify both coordinator progress and native control-batch cell events. Send one
initial message to the supervisor to confirm the delivery route before load.
Use the available agent-to-agent messaging tool for interim reports; a file
update or the operator's final completion notification alone is insufficient.

## Operator to supervisor

Send a concise message when provisioning/bootstrap/build passes, the first cell
starts, a batch ends, collection is verified, and resource release is audited.
Immediately report a failed exit, invalid cell, lost observer, collection problem,
or projected deadline/budget overrun. Report known facts before investigating
the cause; include the next safe action and whether a decision is required.

Use this compact message shape, with evidence under the run directory:

```text
run_id | timestamp UTC | phase | completed/total, failed, missing
change or blocker | evidence path | deadline remaining
action underway | decision needed (or none)
```

Keep the existing checkpoint current at phase changes and at least every five
minutes during a long phase. Include the last real event time and collection/
release status. Routine cell events stay in the observer stream; aggregate them
instead of waking a reasoning agent for every cell. If a requested decision is
unacknowledged after one minute, resend it once and mark it pending in the
checkpoint. Continue only work already authorized; preserve the independent
deadline and cleanup path.

## Supervisor to user

While actively supervising, check incoming messages and concise checkpoints at
least once per minute, using resumable tool calls or waits no longer than one
minute. A child reported as running does not prove experiment progress. After
five minutes without a fresh checkpoint, contact the operator and inspect the
observer/driver state; label progress unknown until verified. Assess a missing
cell event against the configured cell duration, not a fixed short timeout.

On a milestone or alert, inspect the cited summary/evidence, acknowledge the
operator and act within existing authorization. Tell the user promptly what
changed, its effect on the requested result, and the next action. Report a
blocker before its full diagnosis; do not wait for the user to ask. During
ongoing work, follow the session's progress-update cadence, keep updates brief
and distinguish measured progress from estimates. A checkpoint written to disk
is not a user notification.

Recheck the schedule after the first measured cells, reserving actual collection
and cleanup time. Resolve a time-risk alert before starting work that consumes
that reserve. On failure, apply [recovery assessment](recovery.md) and tell the
user whether a useful retry fits, the chosen action and its reason. Distinguish
an agent-selected estimate from a user limit. Scope, workload, budget and deadline
changes need their existing authorization; a progress alert does not grant it.

Keep supervision active through collection verification and resource release,
then report the requested outputs, missing results and cleanup outcome. If
handoff requires ending the active turn, use an available durable wakeup/monitor
and retain its handle; verify it can notify the supervisor. If the environment
has no such facility, disclose that limitation instead of promising background
notifications. The Skill itself does not schedule or wake an agent.
