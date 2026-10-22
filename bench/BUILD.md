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

## Pinned Python package installation

Use [packaging/README.md](packaging/README.md) for the tested standalone
prebuilt-wheel installation. `pyproject.toml` and `uv.lock` resolve a common
wheel at a commit-specific `vendor/` path; `packaging/common-pin.json` records
the source and artifact digests. Supply that verified wheel and the adapter
wheel, plus existing Python/uv and the compatible PyYAML cache. No adjacent
common checkout is required.

For source-only development checks, use an existing compatible interpreter:

```sh
PYTHONPATH="$PWD/bench:/absolute/path/to/experiment-common" \
  /absolute/existing/python -m unittest discover -s bench/tests -v
```

Source-only tests and native builds have separate acceptance from isolated
installed-package checks. A fresh checkout still needs the verified release
artifacts; remote delivery and build-isolated source installation are not yet
accepted. An installed setuptools/wheel module is not evidence that uv has its
corresponding distribution cached. Offline operations fail on missing inputs.
