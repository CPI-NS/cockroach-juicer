# Manual cluster and experiment workflow

Use the locked Python environment on the initiating host. All AWS operations run
there. EC2 control runs environment setup, Git preparation, builds, deployment,
workloads and collection through SSH without AWS credentials.

## Create machines

`aws-site.yaml` contains account, AMI, networking, SSH, machine types, placement
presets and resource limits. It contains no system source versions or environment
installation recipe. Resource creation prepares machines and SSH access only:

```sh
python bench/run.py cluster create --name demo --instance-type c5.2xlarge \
  --placement same-az --clients 3 --servers 3
python bench/run.py cluster status --name demo
```

There is one control plus the requested server and client hosts. Role IDs are
`server-1`, `server-2`, ... and `client-1`, `client-2`, ... . A worker host is not a
server process or shard. The worker deadline is fixed at creation and is not
extended by environment setup or subsequent runs. Explicit deletion or that
frozen deadline releases workers; control is retained.

## Bootstrap an environment once

`control-env.yaml` is separate from AWS and workload configuration. It declares
hash-verified cached files, setup command argv and the Python environment path.
The maintained setup recipe is `examples/control-setup.sh` in experiment-common.
It consumes the supplied cached runtime/toolchains and Python wheels. Declared
missing Ubuntu development packages are acquired only during explicit bootstrap
from the image's configured, signed Ubuntu APT repositories.

```sh
python bench/run.py bootstrap --cluster demo --config control-env.yaml
```

Each maintained baseline also has the same independent entry:

```sh
python bench/run.py bootstrap --cluster demo --config /absolute/path/control-env.yaml
```

Receipts live under `juicer-exp-runs/clusters/demo/environments/<system>/<recipe-hash>/`.
They depend on the cluster, system and environment recipe, not on run_id, source
versions, workload or concurrency. Repeating the same bootstrap reuses it. A new
package/toolchain recipe uses a new immutable Python environment; it does not
replace an environment used by an active experiment. Ordinary source changes do
not require another bootstrap. Standalone local bootstrap omits `--cluster` and
uses the launch directory's environment registry.

## Select source and workload

The environment is supplied through `--template`; the experiment file selects
source versions, topology, workload, load, Juicer settings and measurement:

```yaml
system: crdb
experiment_id: ww-check
run_id: ww-check-001
sources:
  - repository: git@github.com:CPI-NS/cockroach-juicer.git
    path: /home/ubuntu/src/cockroach-juicer
    branch: juicer-v26.2.3
# Supply the full topology/workload/artifact settings from a maintained example.
```

Use exactly one `branch` or full lowercase `revision` per source. A repository
source is cloned/fetched for each new run, with pinned submodules, into an
isolated detached worktree. Sources without a repository URL must already be
clean at their declared version. Preparation records actual commits and rebases
control-side source/build paths; active checkouts remain untouched.

Publish tested changes before the next experiment: commit and push the selected
branch, then let control fetch that commit. Local uncommitted/unpushed edits are
not copied by rsync. Do not silently use an old remote branch or bypass a disabled
push remote. GitHub private-repository access requires a working initiating-host
SSH agent; only the Git preparation SSH session forwards it. Trusted GitHub host
keys may be supplied as public bootstrap inputs. No private key/token is copied.

Artifact paths can contain `{run_id}` to isolate build/deployment outputs. Give
each new execution a new run_id; never overwrite an active binary path or relabel
a changed source version as an unchanged continuation.


For adapters requiring `artifacts.source_revision`, use `auto` with exactly one
declared source. Run preparation replaces it with that source's frozen commit;
branch labels are not used as measured-version provenance.

## Run and observe

```sh
python bench/run.py plan --cluster demo --template control-env.yaml --config crdb.yaml
python bench/run.py run --cluster demo --template control-env.yaml --config crdb.yaml
python bench/run.py status --cluster demo --config crdb.yaml
```

Run checks the existing environment receipt, fetches/freezes the selected source,
builds on control, deploys verified artifacts, then executes measurement cells
through the maintained system CLI. Run never implicitly installs an environment.
Named plan is offline and does not establish remote readiness. The optional
`run --detach` backgrounds the initiating coordinator; keep that host available
for observation, collection and resource ownership. Routine execution needs no
resident reasoning agent.

Results are collected workers -> control -> initiating host, with verification:

```text
juicer-exp-runs/
  clusters/demo/                    # machines, ownership, deadline
    environments/<system>/<hash>/   # reusable environment receipt and setup log
  ww-check-001/
    run.json                       # frozen environment/source/cluster binding
    coordinator/                   # progress and collected native batch
    results/                       # verified exports returned to this host
```

Stop a named run with Ctrl-C or the public command:

```sh
python bench/run.py stop --cluster demo --config crdb.yaml
```

Stop cleans owned experiment processes and collects available output. Manual
named runs retain workers for reuse, subject to the fixed deadline. To release:

```sh
python bench/run.py cluster delete --name demo
```

Deletion audits only owned workers and retains control. Skill-driven execution
must declare a finite release lifetime and release workers at task completion.
Native builds and full remote readiness require actual Ubuntu control validation;
configuration plans and local/fake-cloud tests alone do not establish acceptance.
