# Stop, continuation, and per-run authorization

When the user says they are leaving or going to sleep before a run, record that instruction in this run's execution policy. It survives handoff, applies only to the authorized run, and does not carry into another experiment. In the normal attended mode, give a concise diagnosis and request a decision when the scope requires one.

Set runtime.unattended=true only for this run's away instruction and archive its scope in the run handoff. In away mode, diagnose blockers and make an authorized, straightforward framework, deployment, or collection fix when feasible. Test it in maintained source and record the new version. Database or transaction-client behavior changes require a decision. Ordinary errors fail clearly and clean up; automatic repair or a complex recovery framework is not promised.

The unattended field records execution policy; it does not launch a reasoning
agent or change a failed cell into an automatic engineering task. The observing
agent identifies a handoff from config errors, command exits, invalid results,
stalled events or missing collection, reports the concrete evidence and follows
the existing authorization. A pending unsupported system feature is not itself
a framework failure or permission to repair the system.

## Decide recovery before abandoning the task

An error stops new cells, saves the actual exit and evidence, and cleans owned
processes. The supervisor then assesses recovery before declaring the task over
or releasing reusable workers. A failed batch is not automatically a failed
research objective.

- Identify the failure from exits and evidence: transport/infrastructure, setup
  or configuration, tested-system process failure, incorrect results, or parsing
  only. An OOM is a process failure requiring investigation; it does not prove
  that a clean retry must fail or that the network failed. Reparse verified raw
  results for parsing-only failures, without launching load.
- Check the effective retry policy. The runner defaults to one attempt and only
  retries transport errors. Increasing max_attempts does not make every process
  failure retryable. This runtime setting does not itself prohibit the supervisor
  from starting a fresh, supported recovery batch within the authorized task.
- Consider one supervised retry of the failed cell with unchanged scientific
  configuration when owned processes are cleaned, hosts are healthy, and there
  is time and budget for setup, measurement, verified collection and cleanup.
  Use the maintained continuation entry; preserve failed attempts and completed
  cells. Report the choice and evidence to the user. Ordinary recovery within
  existing authorization does not need another permission request.
- Stop retrying when the same failure recurs, correctness is in doubt, cleanup
  is incomplete, or the remaining boundary cannot accommodate a useful retry.
  Explain the concrete reason. Workload, tested-system behavior, machine sizing,
  budget or deadline changes follow their existing authorization; do not silently
  change them to get a successful measurement.
- Distinguish insufficient time for the entire remaining matrix from insufficient
  time for one useful retry. Tell the user what remains possible. A user stop or
  an explicit no-more-load instruction takes precedence over retry assessment.

Use a fresh batch/output for recovery; keep the old batch failed and analyze only
compatible successful cells together. The [commands](commands.md) reference
identifies the supported continuation entry and named-entry limitations. Do not
copy a runner, rewrite frozen records or replay successful cells to hide failure.

## Budget collection realistically

Before provisioning, include setup, observed per-cell overhead, a recovery
allowance and collection/cleanup in the lifetime estimate. Identify which limits
come from the user and which are site defaults or agent choices; do not present
an agent-selected default as a user prohibition. Once created, keep the frozen
resource deadline unless an explicitly authorized change is implemented safely.

Re-estimate from measured progress. When collection dominates, inspect file
count, transfer rate, per-file connection overhead and duplicate full-tree
passes. Use existing supported bulk transfer or verified-file reuse when
available. If the maintained collector needs a fix, report that engineering
limitation; do not claim the fix is already present or invent a per-run collector.
An inefficient collector is not by itself proof that recovery is impossible.
Near an actual hard limit, prioritize data protection and cleanup and report
which work could not fit. Preserve both collection verification stages.

Ctrl-C or the public stop command must stop dispatch, terminate owned children and descendants, retain evidence and report cleanup failures. Confirm the stopped/interrupted records and cleanup_errors=[]; keep unrelated processes and shared control running. A SIGKILL cannot run finally; the configured deadline remains the backstop.

Completed cells remain valid; interrupted cells run again from the beginning in a fresh batch with --previous. Supply all associated batches for continuation and analysis. Preserve the frozen config and successful results. Source/artifact changes require explicit compatibility review; never relabel them as an unchanged continuation.

A local UI or laptop disconnect must not stop the control driver. Losing experiment-node connectivity is a distinct failure, and a heartbeat alone does not prove successful measurement. If observation or collection fails, stop new cells, preserve evidence, and respect the independent deadline.
