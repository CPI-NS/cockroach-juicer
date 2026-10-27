# Public commands

Use the locked Python environment containing experiment-common and the selected
adapter. File entries are `python run.py` in common, `python experiments/run.py`
in the project, and `python bench/run.py` in each maintained system. Absolute
script paths work from another directory.

## Named cluster workflow

For a new authorized create-and-run assignment, prefer the continuous entry:

```sh
python run.py all --cluster smoke --site aws-site.yaml \
  --instance-type c5.2xlarge --placement same-az --clients 3 --servers 3 \
  --template control-env.yaml --config experiment.yaml \
  --deadline-seconds 7200 --cleanup-policy release
```

Select machine size from workload requirements and the declared site. This
composes creation, bootstrap and the native experiment lifecycle. `release`
releases this workflow's workers and retains control; `retain` preserves workers
for manual reuse. Use the separate commands below for an existing cluster.
Verify `all --help` in the pinned environment before use. A reported milestone
does not require a new supervisor GO for the next authorized stage.

Prepare `aws-site.yaml` once from the supplied example. It supplies account,
network, AMI, machine estimates, placement presets. Source versions belong to the experiment configuration. Regular cluster creation needs only these five inputs:

```sh
python run.py cluster create --name smoke --instance-type c5.2xlarge \
  --placement same-az --clients 3 --servers 3
python run.py cluster status --name smoke
python run.py bootstrap --system juicer-cc --cluster smoke --config control-env.yaml
python run.py plan --cluster smoke --template control-env.yaml --config experiment.yaml
python run.py run --cluster smoke --template control-env.yaml --config experiment.yaml
python run.py status --cluster smoke --config experiment.yaml
python run.py stop --cluster smoke --config experiment.yaml
python run.py cluster delete --name smoke
```

Replace `run.py` with the chosen file entry. `run --detach` uses the maintained
background coordinator. The initiating host must remain available for collection.
Manual runs retain workers for reuse; explicit delete releases only owned workers
and retains control. Skill-driven creation also passes an explicitly chosen
`--deadline-seconds`; its maximum applies across all reuse and is not renewed by
another run. Read the installed `docs/manual-aws.md` and [AWS](aws.md).

## Configured outputs

```yaml
experiment_id: ww-study
run_id: ww-study-001
output_root: juicer-exp-runs
```

The default destination is `<launch-directory>/juicer-exp-runs/<run_id>/`.
`output_root` is optional; `run_id` falls back to `experiment_id` for standalone
runs. Change run_id for a new execution. These storage fields do not change
scientific continuation identity. Explicit `--output` takes precedence. State
commands use the same config; named-run state commands also use `--cluster`.

Named outputs contain `coordinator/` for progress and collected native batches,
`results/` for verified exports, and `run.json` for the cluster binding.
The cluster's inventory/ownership/deadline lives under
`juicer-exp-runs/clusters/<name>/`. Remote workspaces use configured control and
worker paths. Git ignores the run tree.

## Standalone local or existing SSH inventory

Omit `--cluster` and use the system's native inventory configuration:

```sh
python bench/run.py plan --config experiment.yaml
python bench/run.py run --config experiment.yaml
python bench/run.py status --config experiment.yaml
python bench/run.py stop --config experiment.yaml
```

For continuation, use a new run_id/output and explicit `--previous` batch paths;
analyze compatible batches together. Named-cluster `--previous` is not inferred:
the maintained named entry rejects `--previous`. Use the supported standalone
continuation entry and explicit batch paths; never guess local-to-control paths.

## Installed resources

The environment's data directory contains `share/experiment-common/` with
`examples/aws-site.yaml`, evaluation cards, capability coverage and operating
manuals. Resolve it with the selected interpreter's `sysconfig.get_path("data")`.
Use that environment's resources and pinned adapter, rather than assuming a
sibling source checkout. Shipped site/artifact placeholders are planning inputs,
not deployable settings.

Read only the requested evaluation card and capability document. Validate the
plan's parameter cells, roles, exact global load, windows and source/artifact
identity before load. Runtime does not download or upgrade dependencies.

Before execution, attach a persistent observer to `events.jsonl` (standalone) or
`coordinator/driver-events.jsonl` (named). Inspect cell_started/completed/failed/
interrupted events from the control batch as well as coordinator progress.
Status and heartbeat alone do not establish cell completion. Recollection uses
`collect`; parsing-only failure with verified raw files uses `analyze --reparse`
through the standalone analysis entry, without launching another workload.

## File monitor

```sh
python /absolute/path/to/experiment-common/run.py monitor \
  --run-dir /absolute/path/to/run --lifetime-seconds 7500 --detach
```

This source entry and its detached monitor use that checkout with the current
interpreter's existing dependencies; no installation or active environment update
is needed. The supported backends are `file` (default) and `claude`. Only Claude
delivery takes a target: `--backend claude --target /absolute/private/channel-spool`.

Read the existing `monitor/status.json`, event streams and full
`monitor/notifications/*.json` records. Files do not wake an idle model. Follow
[progress reporting](progress.md) for the managed observation assignment,
operator-to-parent route and parent-to-user reports through cleanup.
