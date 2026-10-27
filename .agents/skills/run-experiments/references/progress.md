# Progress and lifecycle ownership

## Before load

Assign one operator the complete authorized lifecycle, including analysis, plots,
both collection stages and resource release. Give it scope, run/output paths,
resource deadline and collection/cleanup allowance. Record operator ID, actual
model, parent ID, observer handle and event paths in the existing run metadata. The
supervisor remains accountable for decisions and proactive user notifications.

The child checks its actual tool inventory and permissions. Record both routes:
runtime events -> file monitor -> operator observation, and operator's verified
results/failures -> parent -> user. Use native agent handoffs/completion callbacks
and parent-message tools actually available in the session; verify the parent
route before load. A completion callback reports assignment completion, not each
intermediate file change. If intermediate messaging is unavailable, the parent
watches the same status/events. A separate guardian or multiagent service is unnecessary.

Establish a persistent program observer of coordinator progress and native
control-batch cell events from [commands](commands.md). Status snapshots or CPU
samples alone do not establish cell completion. Confirm the observer works before
load and keep it active through collection and cleanup.

## Continue and report

The same operator owns every normal authorized stage without parent GO. Runtime
programs execute those stages while the model is idle. In Codex, keep a managed
tool wait or explicit status-file observation assignment active through collection
and cleanup. Inspect failures, stalls, verified reports and terminal cleanup as
they appear. A file monitor does not automatically wake an idle model; ending the
assignment and relying on files for a wakeup leaves observation unattended.
Native handoffs do not block the independent programs. A pending decision blocks
only dependent work. Use program observation rather than a second reasoning loop
for each poll, and state any unattended interval honestly.

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

## Monitor output and notification timing

Use the maintained `monitor` command after starting detached `all`; establish it
before the first workload. It tails real control-side cell events without model
calls. Failures, stalls, verified local `report_ready`, workflow completion and
observer problems are persisted under `monitor/notifications/`; routine cells
remain in the event stream. Example (same command works through the project/shared
or standalone entry):

```sh
python run.py monitor --run-dir juicer-exp-runs/RUN_ID \
  --lifetime-seconds 7500 --detach
```

Standalone batches use root `events.jsonl` and terminal `summary.json`, identified
by `batch.json` or detached `driver.config.json`; root `driver.json` supplies
detached process liveness. Outer AWS monitors finish only at their own workflow
summary or coordinator exit. Native batch completion leaves collection/cleanup
pending; an abnormal coordinator exit still requires inspecting their evidence.

`file` is the default; `claude` is the only other backend. File mode records
locally and takes no target. It never invokes Codex or queues user input.
Codex lifecycle reporting uses the native routes and managed
observation assignment above, without a CLI user-queue fallback.

For Claude Code,
`--backend claude --target /absolute/private/channel-spool` writes to the
opted-in incoming Channel server provided by `experiment_common.claude_channel`.
Read that module's help for launch/setup. Channels are a Claude research-preview
feature; an enabled, running Claude session is required. Never substitute outgoing
Notification hooks or an unverified concurrent `claude --resume` writer.

Retain `monitor/driver.json`, `monitor/status.json`, native event stream and
`monitor/notifications/` in the run handoff. Before reporting a notification,
read the full latest status, the layout's summary when present, and relevant
source events. Notification `details` retain full evidence and a `status_path`
pointing to the existing current snapshot; local records are not truncated.
Source `time`/`timestamp` values remain in evidence. `created_at`/`observed_at`
identify when this monitor first noticed the event; replay can observe old data.
In file mode, `recorded_at` and `record_backend=file` mean local persistence,
with `attempts=0` and `accepted=false`: no enqueue or processing receipt is implied.
File mode has no delivery retries or terminal delivery waits.

Only Claude spool delivery retries, at most three attempts, retaining
undelivered records. Claude spool acceptance, channel emission and model processing
are separate evidence. Delivery failure does not disable the independent execution
deadline, verified collection or AWS cleanup. Events are runtime data, not
authorization to change workloads or budgets.

If a supported incoming route is unavailable, maintain status-file observation
and disclose its limits. Tool waits do not provide durable wakeups, and
there is no fixed 60-second user-notification guarantee. Never claim near-zero
scan token use while either parent or operator is repeatedly polling through
reasoning turns.

Ownership ends after requested outputs, verified collection and audited cleanup,
or an explicit blocker with evidence, remaining ownership and the safe recovery/
deadline path reported. Include the separate [control finalization](aws.md#control-finalization)
and its audit; worker release alone does not complete control cleanup.
