---
name: run-experiments
description: Run, stop, continue, collect, and analyze an already designed Juicer experiment through experiment-common or a system's standalone entry. Use for executing declared experiments; experimental design is separate.
---

# Run experiments

Use the maintained framework, with the system adapter installed at the locked version. Start from the requested experiment objective and variables. An experiment is configuration, not a new driver.

Run from the user's chosen launch directory in the locked Python environment.
`output_root` defaults to `juicer-exp-runs`; configuration `run_id` names this
execution (standalone runs may fall back to `experiment_id`). The CLI derives
the destination automatically. Use explicit `--output` only to override it.
Keep reports, collected files and plots under that run directory. No run-path
environment variable is required. File entries accept absolute script paths;
relative config/output paths resolve against the launch directory.

## Site selection

Every site file declares its `provider`: `aws` creates and releases EC2
workers; `cloudlab` reserves hosts from a fixed CloudLab inventory. There is no
default; pass `--site` when more than one site file exists. Before creating a cluster, read [AWS](references/aws.md)
or [CloudLab](references/cloudlab.md) for the selected site. Both sites use the
same named-cluster, bootstrap, run/all, observation, two-stage collection and
analysis commands. Worker lifetime, release/audit, cost caps, machine-size
limits, self-destruct and control finalization apply only to AWS.

For the manual workflow, create a named cluster, bootstrap each baseline environment
once, then run experiment configurations with `--cluster NAME --config FILE`.
Environment bootstrap is keyed by cluster, system and recipe hash, independently
of run_id. Reuse it until the declared packages or toolchains change. On AWS, manual runs retain workers until
explicit deletion or their frozen deadline. Before Skill-driven AWS creation, choose
and record a finite maximum release lifetime, including setup and collection,
and pass `--deadline-seconds`. Release workers when the authorized task ends.
Record control ownership, the final stop/terminate choice and estimated compute
and retained-storage cost before starting the task. After all authorized reuse
and both verified collection stages, explicitly finalize control from the
initiating host using [AWS control finalization](references/aws.md#control-finalization).
When the user chooses guided operation, provide the commands and inspect returned
output; the user performs AWS operations.

Read [commands](references/commands.md) for entries and installed resources.
A common or juicer-project checkout is not required by a standalone system.

## Normal path

1. Read the requested configuration and the chosen entry's `--help`. Use the system's standalone entry for a local or ad hoc SSH inventory, or the shared named-cluster entry (optionally through the project wrapper) for an AWS or CloudLab site. For AWS, the initiating host performs all AWS operations; a separate EC2 control builds, deploys, runs and collects through SSH without AWS credentials. Read [AWS](references/aws.md) before provisioning. For CloudLab, the remote control reaches the fixed workers by LAN name and no AWS operation exists; read [CloudLab](references/cloudlab.md). Check installed framework/adapter versions against their lock and build manifest.
   Cluster creation prepares machines and SSH access only. Use the selected
   baseline's explicit bootstrap command for environment setup; run never installs
   an environment implicitly. Read [commands](references/commands.md) for executable examples and [evaluation](references/evaluation.md) when selecting a current evaluation card.
2. Validate and generate the plan. Inspect its concise summary: cells/repeats, workload semantics, global load and process allocation, Juicer settings, measurement windows, artifacts, destination, and resource/time/cost boundaries. Retain the archived configuration stages and actual commands. Clarify missing essential intent; do not expand the research question.
3. Follow [source updates](references/source-updates.md) whenever changing code or
   preparing the next run. Commit and push authorized source changes after checks,
   then update control from the remote Git commit. Every new run fetches the
   selected branch and freezes a new checkout; active runs remain unchanged.
   For CC/CRDB comparisons, verify that both clean gRPC submodules pin the same reviewed shared Juicer revision. Build and deploy fixed artifacts before measurement, using control. Use available verified caches. Tool installation follows the user's source-disclosure and approval rules. Runtime never downloads or upgrades dependencies.
4. Assign one operator the full authorized lifecycle: preparation, execution, recovery, analysis, plotting, both collection stages and exact owned-resource cleanup. Follow [progress reporting](references/progress.md) before load. Prefer detached `all` plus the default file monitor; Claude can use its channel backend. Programs continue while the model is idle; files do not wake it. For Codex, keep a managed tool wait or status-file observation assignment through cleanup and use native agent handoffs and available parent-message tools for reports. Record operator and parent routes and the attended/away policy. The parent reports meaningful results and failures to the user. Supported separate commands remain one continuous assignment.
5. Read concise runtime summaries and fresh evidence first. Collect and verify both worker-to-control and control-to-durable copies. Analyze all compatible continuation batches together and produce every supported requested plot. Publish `report_ready` as soon as verified summaries/plots are usable, stating any pending collection or cleanup; continue through closure. On AWS, after all authorized reuse phases, release owned workers and complete [control finalization](references/aws.md#control-finalization); worker release alone does not finalize control. CloudLab hosts are left as they are. A `selection.status=insufficient_evidence` result limits selection-dependent followups, not available scan plots or other independent outputs. Retain failed attempts and provenance; successful cells are not repeated or selected for favorable results.
6. Report actual outcomes, invalid or missing cells, output paths, tested versions, and remaining limitations. Distinguish local functional smoke results from performance evidence. Report actual token counters when available, including cached-input accounting and a stated measurement boundary; do not estimate usage from wall time.

Use the highest available Opus or Sol model for experiment execution and experiment-log analysis when the project requires it. Record the actual model identifier. A running shell command itself is not an Agent reasoning turn.

## Exceptions

- If a configuration cannot express the requested experiment, identify the missing capability and hand off its concrete config error, exits or missing evidence. Implement a maintained framework/adapter fix only within the user's authorized scope. System behavior changes are separate work. Use a new batch with the new version; preserve frozen previous runs.
- On failure, assess a useful bounded retry before abandoning the task or releasing reusable workers, following [recovery](references/recovery.md). Diagnose exits, invalid results, stalled progress or incomplete collection from evidence. Runtime retries and an agent's recovery decision are separate; report the decision to the user.
- Use [stop and continuation](references/recovery.md) for interruption, errors, or unattended authorization. Read only the relevant logs, command exits, and affected cell records before expanding the investigation.
- Use [measurement](references/measurement.md) when interpreting rate, latency, retries, or continuation compatibility.
- Use [AWS](references/aws.md) for an AWS site, through either the shared entry or the project wrapper, and [CloudLab](references/cloudlab.md) for a CloudLab site.

Use the maintained framework entries. Repeated-key RMW requires matching repaired
Juicer-CC server/client/adapter artifacts for off/on and preserves repeated draws.
Native Mac execution requires platform validation. A checkout without a maintained
adapter is not made runnable by installing this Skill. The canonical Skill source
is `experiment-common/skills/run-experiments`; repository-local copies include all
references and are synchronized from that source.
