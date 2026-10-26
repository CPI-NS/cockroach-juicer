# Stop, continuation, and per-run authorization

When the user says they are leaving or going to sleep before a run, record that instruction in this run's execution policy. It survives handoff, applies only to the authorized run, and does not carry into another experiment. In the normal attended mode, give a concise diagnosis and request a decision when the scope requires one.

Set runtime.unattended=true only for this run's away instruction and archive its scope in the run handoff. In away mode, diagnose blockers and make an authorized, straightforward framework, deployment, or collection fix when feasible. Test it in maintained source and record the new version. Database or transaction-client behavior changes require a decision. Ordinary errors fail clearly and clean up; automatic repair or a complex recovery framework is not promised.

The unattended field records execution policy; it does not launch a reasoning
agent or change a failed cell into an automatic engineering task. The observing
agent identifies a handoff from config errors, command exits, invalid results,
stalled events or missing collection, reports the concrete evidence and follows
the existing authorization. A pending unsupported system feature is not itself
a framework failure or permission to repair the system.

Use one attempt by default. An ordinary error stops new cells, saves the actual exit and evidence, and cleans owned processes in finally. When only parsing fails and raw evidence is complete, use the supported reparse operation rather than rerunning load. Identify failures from exits and status, not an absent plot alone.

Ctrl-C or the public stop command must stop dispatch, terminate owned children and descendants, retain evidence and report cleanup failures. Confirm the stopped/interrupted records and cleanup_errors=[]; keep unrelated processes and shared control running. A SIGKILL cannot run finally; the configured deadline remains the backstop.

Completed cells remain valid; interrupted cells run again from the beginning in a fresh batch with --previous. Supply all associated batches for continuation and analysis. Preserve the frozen config and successful results. Source/artifact changes require explicit compatibility review; never relabel them as an unchanged continuation.

A local UI or laptop disconnect must not stop the control driver. Losing experiment-node connectivity is a distinct failure, and a heartbeat alone does not prove successful measurement. If observation or collection fails, stop new cells, preserve evidence, and respect the independent deadline.
