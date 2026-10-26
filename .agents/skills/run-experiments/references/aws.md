# AWS execution boundary

Run the shared AWS entry on the initiating host; the project wrapper delegates to this same entry. The initiator performs every AWS API operation: preflight, creation, queries, termination and final resource audit. It creates a separate EC2 control or reuses an explicitly selected control, prepares worker inventory and invokes the standalone system entry on control through SSH. The initiating host must not substitute for EC2 control in an AWS run.

Use `python -m experiment_common.infra --help` and the installed
`$EF_SHARE/docs/infra.md` (resolve `EF_SHARE` as in [commands](commands.md)) for the current commands and
declared bootstrap/artifact configuration. The project wrapper is optional.

Control builds and deploys fixed artifacts, starts and stops worker processes through SSH, and collects worker results. It performs no AWS operations and needs no AWS credentials or instance profile. Do not copy AWS credentials to control or investigate control IAM permissions as an experiment prerequisite. A cluster's lifecycle journal identifies its attempt and exact owned temporary resources. Shared control and unrelated clusters are protected.

On creation failure, reclaim this attempt's temporary resources and verify cleanup. If a retry is authorized, recreate the whole cluster with at most three total attempts; otherwise report the failure. A failure in one cluster is not permission to destroy another. Keep ordinary error handling and Ctrl-C cleanup straightforward.

Default to one cluster. Multiple independent clusters can run in parallel when requested/configured; there is no arbitrary hardcoded count ceiling. Actual account quotas, aggregate spending and per-run time boundaries still apply. Each cluster runs its own cells sequentially.

Build fixed versions on control, then distribute verified artifacts. Keep control-driven execution independent of the initiating laptop. Before load, verify the temporary workers' independent deadline cleanup. Do not install a self-destruct timer on shared control.

Reserve time and budget for collection and cleanup. Near the limit, start no new cells. Estimates are not instantaneous billing guarantees. Neither an unattended instruction nor an infrastructure retry expands the approved budget.

Collection is two-stage: workers to control, then the initiator retrieves control results to the configured durable destination. Verify file manifests, sizes and hashes at both stages; data remaining only on temporary instances is not durably saved. Collection failure gets bounded retries, stops new work and produces a clear report before the resource deadline. The initiator releases exact owned workers and audits the result; retain usable control. Never mark missing data as a successful run to satisfy cleanup.

## Named reusable clusters

Read the installed `docs/manual-aws.md`. A site file supplies one-time AWS and
control defaults. Manual creation/run/delete are separate operations. Workers
remain available after a named run until explicit delete or their fixed creation
deadline. For Skill-driven execution, explicitly choose and record the maximum
release lifetime and pass `--deadline-seconds`; release workers at task completion
and retain usable control. A new experiment never extends the existing deadline.
In guided mode the user runs AWS commands and the agent reviews returned output.

Keep source selection out of AWS site and cluster settings. Put `sources` with
`path` and exactly one `branch` or full `revision` in the experiment YAML. Each new run fetches declared repository sources, freezes their commit and uses
an isolated checkout. Existing sources without a repository URL are verified
without fetching. Active checkouts are never switched.

Cluster creation must not install Python packages, toolchains or system source.
Run the chosen baseline's explicit bootstrap command once per environment. It
uses existing file/hash/command setup primitives and no AWS API. Experiment
run preparation fetches and freezes sources separately, then uses the installed
environment to build, deploy and execute. Changing source alone reuses bootstrap.
See [source updates](source-updates.md) for commit/push and control synchronization.
