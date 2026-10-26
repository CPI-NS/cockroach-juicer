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

Both Go and Bazel read the pinned `juicer-grpc-go` submodule in this checkout.
`go.mod` replaces `google.golang.org/grpc` with `./juicer-grpc-go`, and
`DEPS.bzl` declares `org_golang_google_grpc` as a `local_repository` at that
same path. Initialize the pinned submodule from an already available local
repository before an offline build. The checkout is standalone: it needs no
`juicer-project` tree. BUILD metadata is committed inside the shared gRPC
repository alongside its sources; no source export or repository override is
part of the build.

Example using only prepared caches, from the CRDB checkout:

```sh
bazel --nosystem_rc --nohome_rc \
  --output_base=/absolute/existing/crdb-bazel-output-base \
  build --repository_disable_download \
  --repository_cache=/absolute/existing/bazel-repository-cache \
  --disk_cache= --remote_cache= --remote_executor= \
  //pkg/cmd/cockroach-short
```

Use the existing Bazel 7.6 executable directly if the launcher would download
it. Reuse an idle output base whose repositories and host toolchains are
already populated. Allow repository rules to materialize inputs from that
cache: `--nofetch` can retain a stale repository from the output base's
previous workspace. `--repository_disable_download` forbids new archive
downloads; keep `GOPROXY=off GOSUMDB=off GOTOOLCHAIN=local` for Go repository
tools as well. Do not share an output base with a concurrent build. Missing
cached inputs must fail without downloading.

When reusing an install base under `/tmp`, Linux sandboxing can hide Bazel's
own helper scripts. Use `--spawn_strategy=local` for that prepared native
environment, and `--test_strategy=standalone` for tests; this changes where
actions execute, not the source repository or test selection.

This is a native build with the host's existing toolchain. On Apple Silicon,
run it in an arm64 shell with the native Xcode command-line toolchain and
prepared darwin_arm64 Bazel/Go dependencies. Current source declares that
platform; **execution and build on a real Mac remain untested**. Linux smoke
does not establish Mac compatibility. The repository's `./dev build short`
is the normal developer wrapper when dependency acquisition has separate
authorization; it must not silently acquire tools in this offline workflow.

The expected output is under
`_bazel/bin/pkg/cmd/cockroach-short/cockroach-short_/cockroach-short` (the
workspace's configured symlink prefix). Verify the current Bazel output
after a successful build, and identify both the CRDB and submodule commits
when handing it off.

### Shared gRPC BUILD maintenance

When shared gRPC sources, packages or imports change, regenerate their BUILD
files in the submodule using an **existing** Gazelle binary. Use CRDB's
repository map so split genproto modules and other dependencies resolve to
the same repositories as the consumer. From the CRDB checkout:

```sh
GOPROXY=off GOSUMDB=off GOTOOLCHAIN=local /absolute/existing/gazelle \
  -repo_root="$PWD/juicer-grpc-go" -repo_config="$PWD/WORKSPACE" \
  -external=static -strict "$PWD/juicer-grpc-go"
```

Review and commit those BUILD files in the shared repository, then update
the consumer's submodule pin. Add `-mode=diff` to check without writing.
The root BUILD directives preserve existing target names and
`go_default_library` aliases, compile checked-in protobuf Go sources, and
exclude nested Go modules (including examples and optional integrations).
`juicer/spectest:spectest_test` consumes the same conformance fixtures as
`juicer:juicer_test`; keep their runfiles declarations when regenerating.
`DEPS.bzl`'s local repository and its Gazelle import mapping in `WORKSPACE`
are maintained declarations. Do not regenerate them with the dependency
mirror command, which assumes downloadable versioned Go modules; this local
submodule is not such a module.

### Offline integration checks

After the submodule pin and CRDB source bindings are ready, use the same
cached Bazel executable, output base and offline flags as above to compile
`//pkg/rpc:rpc_test` and run the shared tests:

```sh
bazel --nosystem_rc --nohome_rc \
  --output_base=/absolute/existing/crdb-bazel-output-base \
  test --repository_disable_download \
  --repository_cache=/absolute/existing/bazel-repository-cache \
  --disk_cache= --remote_cache= --remote_executor= \
  --test_output=errors \
  @org_golang_google_grpc//juicer:juicer_test \
  @org_golang_google_grpc//juicer/spectest:spectest_test
```

For a targeted RPC check, use `//pkg/rpc:rpc_test` with
`--test_filter='TestJuicer'`. A filtered run still compiles the complete RPC
test target. Separately build `//pkg/cmd/cockroach-short` for the production
binary. These are local build and test checks, not experiment execution.

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
