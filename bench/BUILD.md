# Native, offline build recipes

No recipe authorizes an installation or network download. It assumes existing native Python 3.11+, Go 1.26, the fork's Bazel 7.6 launcher/toolchain cache, and its C/C++ dependencies. Check existing tool versions first; if a toolchain or dependency is missing, stop and report it. Keep Linux amd64 and macOS arm64 artifacts and manifests separate.

## SQL client on Linux or Apple Silicon

From the checkout:

```sh
cd bench/sqlclient
GOPROXY=off GOSUMDB=off GOTOOLCHAIN=local go build -mod=readonly -o /absolute/artifacts/crdb-sql-client .
GOPROXY=off GOSUMDB=off GOTOOLCHAIN=local go vet ./...
```

Run this in a native Linux shell or a native arm64 macOS terminal. Do not set `GOOS`/`GOARCH` to cross-compile and then call the result native validation. `go.mod`/`go.sum` pin the cached pgx version and dependencies; a missing module fails offline. This small separate module avoids building CRDB just to change client scheduling.

## Juicer CRDB fork

Prefer the existing source-specific build manifest when reusing an artifact. For a new build, use this checkout and its corresponding Juicer gRPC Bazel source export, not an official stock CRDB binary. Example using only prepared caches:

```sh
bazel --nosystem_rc --nohome_rc build --nofetch \
  --repository_cache=/absolute/existing/bazel-repository-cache \
  --override_repository=org_golang_google_grpc=/absolute/prepared/juicer-grpc-bazel \
  //pkg/cmd/cockroach-short
```

This is a native build with the host's existing toolchain. On Apple Silicon, run it in an arm64 shell with the native Xcode command-line toolchain and prepared darwin_arm64 Bazel/Go dependencies. Current source declares that platform; **execution and build on a real Mac remain untested**. Linux smoke does not establish Mac compatibility. The native build recipe can fail if its cache is incomplete; `--nofetch` is intentional. The repository's `./dev build short` is the normal developer wrapper when dependency acquisition has separate authorization, but it must not silently acquire tools in this delivery's offline workflow.

Record source/submodule or override-tree commits/hashes, dependency locks, exact build argv, tool versions, OS/architecture, build log, executable SHA-256, and actual `cockroach version` in a source manifest. The expected output is under `bazel-bin/pkg/cmd/cockroach-short/cockroach-short_/cockroach-short`; verify the actual Bazel output rather than copying an old path after a failed build.

`artifacts.cockroach_build` can supply an explicit `{argv,cwd,host_id?,env?}` recipe for common's build operation. `artifacts.client_source` enables the offline Go client recipe. Native executable flags are passed as argv arrays. Deployment is a separate common operation; do not build per measurement cell.

## Existing Python environment and lock status

`bench/pyproject.toml` pins `experiment-common==0.1.0`, and the uv source override expects common beside the system checkout (`../../common` from `bench`). Initial source tests used existing Python 3.12.7/PyYAML 6.0.1; final isolated dependency tests use cached Python 3.14.8/PyYAML 6.0.3. This avoids installing anything:

```sh
export PYTHONPATH="$PWD/bench:/absolute/path/to/experiment-common"
/absolute/existing/python3.12 -m unittest discover -s bench/tests -v
```

The final `uv.lock` resolves common and PyYAML 6.0.3 offline. The explicit PyYAML dependency metadata in `pyproject.toml` is copied from the actual cached upstream wheel (`Requires-Python: >=3.8`, no `Requires-Dist`); it avoids a request for another platform's wheel metadata during universal resolution. It does not narrow the project's Python/platform support or invent a package hash.

The executed dependency-only isolated validation used existing uv and cached Python/PyYAML:

```sh
uv lock --offline --no-python-downloads --python /absolute/existing/python3.14 --project bench
uv sync --offline --frozen --no-python-downloads --python /absolute/existing/python3.14 \
  --project bench --no-install-project --no-install-package experiment-common
PYTHONPATH="$PWD/bench:/absolute/path/to/experiment-common" bench/.venv/bin/python \
  -m unittest discover -s bench/tests -v
```

This installs the cached external dependency into an isolated environment and uses local source packages. It is not a claim that a complete isolated build environment was downloaded or installed. Separately, the adapter wheel was built offline with the existing exact setuptools 75.1.0/wheel 0.44.0 toolchain, installed into the isolated environment, and its installed `crdb-bench validate` entry point passed with common supplied from its separate source checkout. Common's own wheel/build validation belongs to the common package.
