# AWS execution boundary

Run the shared AWS entry on the initiating host; the project wrapper delegates to this same entry. The initiator performs every AWS API operation: preflight, creation, queries, termination and final resource audit. It creates a separate EC2 control or reuses an explicitly selected control, prepares worker inventory and invokes the standalone system entry on control through SSH. The initiating host must not substitute for EC2 control in an AWS run.

Use `python -m experiment_common.infra --help` and the installed
`$EF_SHARE/docs/infra.md` (resolve `EF_SHARE` as in [commands](commands.md)) for the current commands and
declared bootstrap/artifact configuration. The project wrapper is optional.

Control builds and deploys fixed artifacts, starts and stops worker processes through SSH, and collects worker results. It performs no AWS operations and needs no AWS credentials or instance profile. Do not copy AWS credentials to control or investigate control IAM permissions as an experiment prerequisite. A cluster's lifecycle journal identifies its attempt and exact owned temporary resources. Shared control and unrelated clusters are protected.

On creation failure, reclaim this attempt's temporary resources and verify cleanup. If a retry is authorized, recreate the whole cluster with at most three total attempts; otherwise report the failure. A failure in one cluster is not permission to destroy another. Keep ordinary error handling and Ctrl-C cleanup straightforward.

Default to one cluster. Multiple independent clusters can run in parallel when requested/configured; there is no arbitrary hardcoded count ceiling. Actual account quotas, aggregate spending and per-run time boundaries still apply. Each cluster runs its own cells sequentially.

Build fixed versions on control, then distribute verified artifacts. Keep control-driven execution independent of the initiating laptop. Before load, verify the temporary workers' independent deadline cleanup. Do not install a self-destruct timer on shared control.

Reserve time and budget for collection and cleanup using the measured schedule
and [recovery assessment](recovery.md). Near the limit, start no new cells.
Estimates are not instantaneous billing guarantees. Neither an unattended
instruction nor an infrastructure retry expands the approved budget.

Collection is two-stage: workers to control, then the initiator retrieves control results to the configured durable destination. Maintained collection transfers tar bundles with per-file manifests, verifies them before publication and cleans temporary archives on normal interruption. Verify file manifests, sizes and hashes at both stages; data remaining only on temporary instances is not durably saved. Collection failure gets bounded retries, stops new work and produces a clear report before the resource deadline. The initiator releases exact owned workers and audits the result. Complete control finalization below after all authorized reuse phases. Never mark missing data as a successful run to satisfy cleanup.

## Control finalization

Before the task starts, record control selection, region/account, whether it is
newly created and task-owned exclusive or explicitly reused pre-existing, the
final action and its authorization, and estimated compute and retained-storage
cost in the run handoff. Newly created task-owned exclusive control defaults to
terminate; explicitly reused existing control is stopped when authorized. The
user's explicit stop authorization applies only to the selected reused control,
not to unrelated or shared controls. Stopped control retains billable EBS; record
volume type/size and the regional storage estimate, then report retained volumes.

Reuse control between authorized phases. Finalize only after ALL authorized reuse
phases and BOTH verified collections (worker -> control -> durable destination),
and only when no active experiment or other authorized job needs it. Do not stop
or terminate control needed by an active job, or touch unrelated control. Failed
collection is a blocker to normal finalization; report it and the remaining
resource ownership rather than claiming cleanup complete.

Framework worker release (`cluster delete`, `all --cleanup-policy release`, or
full-manifest cleanup) mechanically retains usable control. The Skill operator
must perform this separate step from the initiating host; no runtime helper or
control-side AWS credentials are needed.

Use the lifecycle journal and read-only preflight to verify account/region, the
exact instance ID, ownership/reuse authorization, active jobs and collection
receipts. Capture its attached EBS volume and ENI IDs and delete-on-termination
settings. Tags alone do not authorize cleanup. With `REGION` and `CONTROL_ID`
bound to that verified control, inspect it from the initiating host:

```sh
aws sts get-caller-identity
aws ec2 describe-instances --region "$REGION" --instance-ids "$CONTROL_ID"
```

For newly created task-owned exclusive control, after the gates above:

```sh
aws ec2 terminate-instances --region "$REGION" --instance-ids "$CONTROL_ID"
aws ec2 wait instance-terminated --region "$REGION" --instance-ids "$CONTROL_ID"
aws ec2 describe-instances --region "$REGION" --instance-ids "$CONTROL_ID"
```

For explicitly reused existing control with stop authorization:

```sh
aws ec2 stop-instances --region "$REGION" --instance-ids "$CONTROL_ID"
aws ec2 wait instance-stopped --region "$REGION" --instance-ids "$CONTROL_ID"
aws ec2 describe-instances --region "$REGION" --instance-ids "$CONTROL_ID"
```

Verify the described terminal state. After termination, use `describe-volumes`
and `describe-network-interfaces` on the captured exact IDs to verify applicable
owned EBS/ENI cleanup; resolve only proven task-owned leftovers within authorized
scope. After stop, verify retained EBS with `describe-volumes` and report its
continuing storage cost. A successful waiter or worker-release receipt alone is
not the complete audit. Save evidence in the run directory, not memory.

## Named reusable clusters

Read the installed `docs/manual-aws.md`. A site file supplies one-time AWS and
control defaults. Manual creation/run/delete are separate operations. Workers
remain available after a named run until explicit delete or their fixed creation
deadline. For Skill-driven execution, explicitly choose and record the maximum
release lifetime and pass `--deadline-seconds`; release workers at task completion
and finalize control as above. A new experiment never extends the existing deadline.
An individual cell failure triggers recovery assessment, not immediate cluster
deletion. Keep healthy workers available for an authorized useful retry within
their lifetime. Release on task completion, an explicit abandonment/stop decision,
or the resource boundary; report the reason and verify the actual release.
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
