# Source updates and reusable environments

Environment setup and source updates have different lifetimes.

- Run explicit `bootstrap` once per cluster, baseline and environment recipe. The
  recipe describes Python packages, toolchains and setup files, not workload,
  source commits or run_id. Reuse the environment for ordinary source changes.
- When this task changes maintained source, run the affected checks, commit only
  the task's changes and push to the configured writable project remote before
  updating control. Preserve other work. Never force-push or silently choose a
  disabled/upstream remote. If publication is unavailable, report that exact gap;
  a local commit does not make remote code current.
- For shared Juicer changes, commit the shared repository first, update the
  consumer submodule pins, then commit and push the consumers. Check that CC and
  CRDB use the same reviewed shared commit before a comparison.
- The experiment declares each repository URL and a branch or full commit.
  Every new run clones or fetches from that remote, freezes the chosen commit,
  initializes pinned submodules and uses its own detached checkout/build paths.
  This is the control-side equivalent of pulling the tested revision; never
  pull into an active run's checkout or substitute an old cached branch tip.
- Uncommitted or unpushed local edits are not synchronized by copying files.
  Do not mark them tested on control. Record actual source and submodule commits
  with the run and use a new run_id for a changed version.
- A common/adapter package or toolchain change creates a new immutable Python
  environment with new package/file hashes. Bootstrap that environment explicitly;
  leave the previous environment intact while an old run uses it.
- Authentication uses the initiating host's SSH agent only during source
  preparation. Do not copy private keys, GitHub tokens or AWS credentials to
  control. A user who performs AWS operations manually still runs those commands.
