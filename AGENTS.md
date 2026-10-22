# Experiment workflow

Before planning, running or analyzing experiments, read [bench/README.md](bench/README.md) and its capability references. Use `python -m crdb_bench` for the supported SQL YCSBT scope; keep ordinary runs configuration-only, with explicit inventory, workload and pinned artifacts. Preserve historical scripts/data; maintain framework fixes in bench/common rather than copying a round runner.

Standalone execution requires experiment-common and this adapter, with no juicer-project checkout dependency. For AWS, read the installed `share/experiment-common/docs/infra.md`, or `docs/infra.md` in your experiment-common checkout. Use `python -m experiment_common.infra`: all AWS provisioning, queries, release and audit belong to the initiating host. EC2 control performs build/deploy/run/collection through SSH only, without AWS credentials or an instance profile.

Establish continuous progress observation and verify worker deadline guards before load. Use public status/stop/collection operations; verify worker → control → durable results before normal release. Cleanup targets only owned processes and workers; retain control and protect unrelated resources. Report unsupported capabilities explicitly before execution.
