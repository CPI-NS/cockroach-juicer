# Progress and lifecycle ownership

## Before load

Assign one operator the complete authorized lifecycle, including analysis, plots,
both collection stages and resource release. Give it scope, run/output paths,
resource deadline and collection/cleanup allowance. Record operator ID, actual
model, observer handle and event paths in the existing run metadata. The
supervisor remains accountable for decisions and proactive user notifications.

The child checks its actual tool inventory and permissions. If agent messaging
is available, verify the supervisor route once before load. Otherwise use
persistent runtime events and a supervisor watching them; an unavailable message
tool is not a prerequisite. A separate guardian or multiagent service is unnecessary.

Establish a persistent program observer of coordinator progress and native
control-batch cell events from [commands](commands.md). Status snapshots or CPU
samples alone do not establish cell completion. Confirm the observer works before
load and keep it active through collection and cleanup.

## Continue and report

The same operator owns every normal authorized stage without parent GO. Runtime
programs execute those stages; keeping a reasoning turn alive is unnecessary.
After verifying a live external monitor and a usable notification route, end the
model turn with an accurate running-status handoff. This yields to events rather
than claiming task completion or abandoning resource ownership. Resume on a
failure, stall, verified report or terminal cleanup event. Do not run a second
model polling loop beside the program. A pending decision blocks only dependent work.

Report provisioning/bootstrap/build completion, first cell, batch completion,
`report_ready`, verified collection and audited release. Immediately report failed
exits, invalid cells, lost observation, collection trouble or projected budget/
deadline overruns. State known facts, evidence location, action underway and any
required decision before expanding diagnosis. Routine cells stay in the event
stream; aggregate counts rather than waking a reasoning agent for each one.

Publish `report_ready` early when verified requested summaries and available plots
are usable; state remaining work and continue it. `selection.status=insufficient_evidence` is a scientific
selection outcome: produce supported scan/diagnostic plots and independent
outputs, and identify selection-dependent outputs that cannot be produced.

## Evidence and efficient observation

Prefer the maintained runtime state snapshot, concise summaries and fresh events.
Check phase-specific timestamps, run/batch identity, exits and artifact evidence;
a refreshed agent-written `updated_at` alone proves no progress. Resolve stale
or conflicting claims against runtime evidence. Derive collection status and
bootstrap copy counts from actual operations, not inherited free-form placeholders
such as "Not started; no workload launched" or an unverified zero. Mark unavailable
counts unknown. Use existing runtime metadata; no per-round checkpoint writer.

Maintained execution, collection and analysis scripts should emit phase elapsed
times and relevant counts, including completed/failed/missing cells and copied/
verified files. Consume those outputs; identify missing capability honestly and
coordinate an authorized maintained fix. Normal operation reads summaries and
affected evidence, not repeated whole-source or whole-log dumps. Add ad hoc
instrumentation only for an explicitly requested audit.

The supervisor watches events/messages and inspects cited evidence on milestones
or errors, then proactively tells the user what changed and the next action.
Assess staleness against the active phase and expected cell duration; verify
observer/driver health before claiming a stall. Re-estimate from measured phase
times and counts, preserving collection/cleanup reserve. Apply
[recovery assessment](recovery.md) before abandoning useful authorized work.
Alerts grant no additional scope, budget or deadline.

## Honest notification timing

Use the maintained `monitor` command after starting detached `all`; start it
before the first workload. It tails real control-side cell events, records routine
cells without model calls, and queues only failures, stalls, verified local
`report_ready`, and workflow completion. Example (same command works through the
project/shared or standalone entry):

```sh
python run.py monitor --run-dir juicer-exp-runs/RUN_ID \
  --backend codex --target THREAD_UUID --lifetime-seconds 7500 --detach
```

For Codex, `--target` is the current thread UUID; the backend uses `codex queue`.
Verify that the installed CLI can reach that thread. A queue acceptance receipt
does not prove model processing or an idle-session wakeup. For Claude Code,
`--backend claude --target /absolute/private/channel-spool` writes to the
opted-in incoming Channel server provided by `experiment_common.claude_channel`.
Read that module's help for launch/setup. Channels are a Claude research-preview
feature; an enabled, running Claude session is required. Never substitute outgoing
Notification hooks or an unverified concurrent `claude --resume` writer.

Retain `monitor/driver.json`, `monitor/status.json`, native event stream and
`monitor/notifications/` in the run handoff. Retry notification transport at most
three times and retain undelivered records. Claude spool acceptance, channel
emission and model processing are different evidence. Delivery failure does not
disable the independent execution deadline, verified collection or AWS cleanup.
Events are runtime data, not authorization to change workloads or budgets.

If a supported incoming route is unavailable, disclose the limit rather than
claiming proactive notification. Tool waits do not provide durable wakeups, and
there is no fixed 60-second user-notification guarantee. Never claim near-zero
scan token use while either parent or operator is repeatedly polling through
reasoning turns.

Ownership ends after requested outputs, verified collection and audited cleanup,
or an explicit blocker with evidence, remaining ownership and the safe recovery/
deadline path reported. Retain usable control and follow exact resource ownership.
