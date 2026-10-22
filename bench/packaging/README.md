# Pinned standalone Python installation

The supported local release input is the common wheel named in
[common-pin.json](common-pin.json), placed under `bench/vendor/`. The generated
`uv.lock` pins its SHA-256. The manifest binds that artifact to a real Git commit,
tree and source-archive SHA-256. Version `0.1.0` alone does not identify the common
source revision.

Each common commit has its own vendor subdirectory. Do not replace a wheel at an
existing pinned path: uv may preserve that path's previous locked hash.

This replaces the sibling `../../common` development source in package metadata.
It does not require a juicer-project checkout or adjacent common source at
installation/runtime. Existing source-development recipes in bench documentation
are separate from this installed-package recipe.

No remote package or Git release is claimed. Wheel binaries are ignored by Git;
a fresh source checkout must receive the verified artifact separately. Until an
approved durable distribution channel exists, remote fresh-checkout installation
remains a release gap.

## Offline install with existing tools and artifacts

Supply an existing compatible Python 3.11+ interpreter, uv, the common wheel and
this adapter's prebuilt wheel. PyYAML 6.0.3 must already be in the uv cache for the
selected Python/platform. Offline mode fails if an input is unavailable.

Set these variables to your own absolute paths; `BENCH_ROOT` belongs to this
system checkout and `PACKAGE_RELEASE` contains the approved wheels:

```sh
export BENCH_ROOT=/path/to/system-checkout/bench
export PACKAGE_RELEASE=/path/to/verified-release
export RUNTIME_PYTHON=/path/to/existing/python
export TEST_PYTHON=/path/to/existing/python
"$TEST_PYTHON" - <<'PY'
import hashlib, json, os, pathlib, shutil
bench = pathlib.Path(os.environ["BENCH_ROOT"])
pin = json.loads((bench / "packaging/common-pin.json").read_text())
destination = bench / pin["wheel"]
destination.parent.mkdir(parents=True, exist_ok=True)
shutil.copyfile(pathlib.Path(os.environ["PACKAGE_RELEASE"]) / destination.name, destination)
assert hashlib.sha256(destination.read_bytes()).hexdigest() == pin["wheel_sha256"]
PY
uv sync --locked --offline --no-install-project --no-python-downloads \
  --python "$RUNTIME_PYTHON" --project "$BENCH_ROOT"
uv pip install --offline --no-index --no-deps \
  --python "$BENCH_ROOT/.venv/bin/python" \
  "$PACKAGE_RELEASE/crdb_bench-0.1.0-py3-none-any.whl"
uv pip check --python "$BENCH_ROOT/.venv/bin/python"
env -u PYTHONPATH -u PYTHONHOME "$BENCH_ROOT/.venv/bin/crdb-bench" validate \
  --config "$BENCH_ROOT/examples/local.json" --output /tmp/installed-adapter-validation
STANDALONE_BENCH="$BENCH_ROOT" "$TEST_PYTHON" "$BENCH_ROOT/packaging/test_standalone.py"
```

Verify the adapter wheel's SHA-256 against the release receipt before installing.
The public `validate` operation resolves configuration/allocation only; it starts
no native process. The tests run installed console commands from fresh unrelated
working directories, verify isolated imports and installed common source bytes,
and require uv to reject missing/changed common artifacts. They do not run any
workload or Skill evaluation. Without `STANDALONE_BENCH`, the opt-in tests skip.

This two-step installation deliberately consumes prebuilt adapter wheels.
Build-isolated `uv sync` also needs cached setuptools 75.1.0 and wheel 0.44.0
distributions. Their presence in an existing build interpreter does not populate
uv's wheel cache. No tool installation or download is part of this recipe.

## Reproduce the common artifact without changing its source

Use a clean Git archive of the manifest's full `git_commit`, and verify its tar
SHA-256 against `source_archive_sha256`. Extract it into a new directory. With
existing setuptools 75.1.0 and wheel 0.44.0, use:

```sh
SOURCE_DATE_EPOCH=<source_date_epoch-from-manifest> \
  "$BUILD_PYTHON" -m pip wheel --no-index --no-deps --no-build-isolation \
  --no-cache-dir --wheel-dir "$PACKAGE_RELEASE" /path/to/extracted-common-archive
```

Check the resulting wheel against `wheel_sha256`; do not update the manifest just
to accept a mismatch. Adapter wheels use the same build command on a clean system
bench source snapshot with its committed packaging overlay. Keep build commands,
tool versions, source revisions and artifact hashes in the release receipt.
Native binaries/submodules and platform dependencies have their own acceptance;
this Python installation does not attest to them.

