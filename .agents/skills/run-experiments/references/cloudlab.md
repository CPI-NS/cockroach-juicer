# CloudLab execution boundary

Use this reference when the selected site file declares `provider: cloudlab`.
Read the installed `docs/manual-cloudlab.md` for commands and the site schema.
Shared phases (named cluster, experiment config, bootstrap, run/all, observation,
verified collection, analysis) follow the normal path in [SKILL.md](../SKILL.md); raw
outputs stay on control as described under Stop and collection.

## What differs from AWS

- The hosts are a fixed inventory that exists for the lifetime of the user's
  CloudLab experiment. Do not create, stop, release, reboot or audit machines.
  There is no AWS call, worker self-destruct timer, cost/instance/vCPU cap,
  ownership journal or control finalization. Do not apply [AWS](aws.md) steps.
- The AWS-era machine-size limit does not apply. Use the fixed hardware as it is:
  `cpus` are hardware threads, SMT stays on, no pinning, no CPU or memory caps.
- `budget.deadline_seconds` bounds each run's execution and collection.
  `expires_at` is the CloudLab experiment's expiration; create and each run
  refuse a deadline past it. If the user extends the experiment, update
  `expires_at` and recreate the named cluster. Never edit it to bypass the check.
- `cluster delete` and `all --cleanup-policy release` only end the local host
  reservation, and delete refuses while a bootstrap or run holds the cluster.
  Leave CloudLab hosts as they are at task completion.
- Full infrastructure manifests are AWS-only; use named clusters.

## Hosts, addresses and access

- The initiating host reaches control by its public name. Control reaches
  workers by experiment-LAN names with its own key and `known_hosts`; the
  initiating host never SSHes to workers.
- `cluster create` and each `run` perform a read-only check from control: SSH to
  every worker, resolution of every cluster name to one distinct `lan_cidr`
  address on control and every worker, and writable work directories. Report a
  failure; do not bypass host-key verification or substitute public names.
- A host belongs to at most one ready named cluster in the registry. Two named
  clusters may share one control and run concurrently.
- Write only to the task's own directories: control's `/workspace/<user>/` and
  workers' `/mnt/extra/<user>/`. Never write to group-shared NFS such as `/proj`
  or `/share`.
- Control fetches repository sources from GitHub with its own key; the
  initiator's agent is not forwarded. Bootstrap installs the cached
  `github.com` host keys; never accept a GitHub host key on first use.

## Environment and resources

- Bootstrap changes control state with sudo (Ubuntu packages, `/usr/local/bin`
  links). The one-time `/workspace/<user>` and `/mnt/extra/<user>` directories
  also need sudo. Run these only with the user's authorization and with download
  sources disclosed; prefer the verified cached runtime declared in the recipe.
- Hardware is recorded as `hardware`/`cpus` in scientific identity. Do not mix
  AWS and CloudLab batches as one continuation, and rebuild baselines on CloudLab
  hardware before comparing off/on results.
- Use worker paths under `/mnt/extra/<user>/` for artifact `deploy_to` and
  runtime libraries (for example Spanner's `LD_LIBRARY_PATH` directory).
  `runtime.workspace_root` is the same absolute path on control and workers;
  control has no `/mnt/extra`, so keep the default `/tmp/experiment-common`
  unless that path exists on every host.

## Stop and collection

Ctrl-C or `stop --cluster NAME --config FILE` stops dispatch, cleans owned process
groups on all hosts and collects evidence, as on AWS.

Raw worker outputs stay on control by default (`raw_data: control`). The two
verified stages are: worker -> control raw collection after every cell (SHA-256
per file), and control -> initiating host for results, analysis, configurations,
provenance, events, logs and each attempt's raw-file manifest with hashes. Verify
both stages, `raw-data.json` (control path, `missing` empty) and owned-process
cleanup before reporting completion. Pull raw outputs only when an analysis needs
them: `collect --cluster NAME --config FILE` verifies each pulled file against the
stage-1 hashes. A site with `raw_data: initiator` copies raw outputs after every
run as on AWS. A collection failure remains a failure; the hosts stay available
for an authorized retry.
