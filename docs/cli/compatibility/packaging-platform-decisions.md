# Standalone packaging and platform evidence

Validation date: 2026-10-15. Source baseline: `9b8661e58107eddb24cb8f15495cee4ac979219a` plus the CLI implementation working tree. This is a local pre-release snapshot, not a published package or immutable final-release mapping. The package still reports version `0.6.7`; release/version changes belong to the coordinating release step.

## Package and installation

Built the actual Hex source tar with:

```sh
PAGER=cat MIX_ENV=prod mix cmd --app ex_storage_service_cli mix hex.build --output /tmp/ess-cli-m9-package-20cbeo18/ex_storage_service_cli-0.6.7.tar
```

The tar contains 764 entries, with README, MIT LICENSE, correct `gsmlg-opt/ex_storage_service` repository metadata, C sources/compiler, pinned service/endpoint/paginator/waiter models and reference/parser notices. It excludes generated native binaries and server applications. Tar SHA256: `8f0bff5bf903463d6b357a45a2c2e42bfafd9102490143e58ff1cf12d708b4a4`.

Unpacked this tar into a fresh external consumer, with independent dependency/build/lock paths entirely under `/tmp/ess-cli-m9-package-20cbeo18`. No package source edits or path-dependency substitutions were used. `mix deps.get --only prod` resolved published Hex packages: http_fetch/http_core/http_runtime/ex_ssl/elixir_quic/elixir_quic_http3 0.18.1, gsmlg_toml 1.0.0, Jason 1.4.5, telemetry 1.4.2. Commands in that fresh package directory:

```sh
PAGER=cat MIX_ENV=prod MIX_BUILD_PATH=/tmp/ess-cli-m9-package-20cbeo18/consumer-build MIX_DEPS_PATH=/tmp/ess-cli-m9-package-20cbeo18/consumer-deps mix deps.get --only prod
PAGER=cat MIX_ENV=prod MIX_BUILD_PATH=/tmp/ess-cli-m9-package-20cbeo18/consumer-build MIX_DEPS_PATH=/tmp/ess-cli-m9-package-20cbeo18/consumer-deps mix compile --warnings-as-errors
PAGER=cat MIX_ENV=prod MIX_BUILD_PATH=/tmp/ess-cli-m9-package-20cbeo18/consumer-build MIX_DEPS_PATH=/tmp/ess-cli-m9-package-20cbeo18/consumer-deps mix escript.build
PAGER=cat MIX_HOME=/tmp/ess-cli-m9-package-20cbeo18/install-home mix escript.install /tmp/ess-cli-m9-package-20cbeo18/extracted/package/ess --force
```

All exited 0. The resulting escript has 1319 ZIP entries and SHA256 `537cc7b2cfc72c3cb8dd620355023d0210406a768a00473b3ea61ea8510b9a61`. Inspection verified the CLI's own `priv/native` library/helper plus metadata, pinned models, and notices. A real artifact check initially found own-app `priv` absent; `include_priv_for: [:ex_storage_service_cli]` fixes that packaging defect. Generated binaries stay under build-tree priv rather than contaminating the source package.

The application declares only kernel, stdlib, Elixir, logger, crypto, xmerl, http_fetch, Jason and gsmlg_toml. Starting it in the fresh consumer confirmed no storage core, cluster, S3 listener, web/Phoenix, Concord, or Ecto application starts. Dependencies are the published client transport closure only.

After installation, the extracted package source and consumer build directories were moved away. The installed escript ran from an empty unrelated directory with no native config or AWS credentials files. Five checks passed: version; modeled command help; input skeleton; a real unsigned local HTTP XML request producing exact Unicode JSON; and a real controlling-PTY request using the embedded pager/NIF/models. Temporary native extraction and spool directories were empty afterward. Evidence JSON and the local tar/escript are retained under the above temporary root.

This verifies installation from the freshly built local Hex source package, dependency resolution from Hex, and local escript installation. It does not verify installation of a newly published release from the public Hex registry; that belongs after publication.

## Platform contract and open gates

Actual environment: Linux x86_64, Elixir 1.18.5, OTP 28. Building/installing requires C11 compiler (`CC`, otherwise `cc`) and OTP headers. Runtime escripts require Erlang/OTP and matching OS/CPU/OTP-major native assets; they are not self-contained operating-system binaries. No runtime C compiler, repository checkout, or server application is required.

The NIF retains Darwin build flags and guarded source. Both normal Linux and non-Linux-guarded C (`cc -U__linux__ -Wall -Wextra -Werror`) compiled successfully on this Linux host. That guard check is not a macOS ABI or execution test. Darwin, ARM64/other CPU targets, and the minimum OTP/Elixir runtime matrix have not run. Interactive paging remains Linux-only. Do not mark the plan's supported Linux/macOS architecture gate complete from this evidence.

The updated `publish-cli.yml` uses Ubuntu with Elixir 1.19/OTP 28 and validates only the CLI and its dependency closure. It requires the requested version to be committed before dispatch; it does not make a hidden workflow version bump. It runs the source-package probe, publishes Hex source/docs, verifies the exact public Hex payload against the validated tar, and runs another extracted published consumer. It prepares the downloadable `ess-linux-x86_64-otp28` escript, SHA256SUMS, source/platform manifests, reference lock, coverage, and README. CLI-only dispatch uses `cli-v<version>`; a root release call attaches artifacts to its supplied version tag. The root `release.yml` also publishes the server image and core package, so it is not a CLI-only release path. No release workflow has been triggered for this implementation.

`scripts/cli_compatibility/package_probe.py` makes the standalone checks repeatable. In addition to required source/escript assets, it recomputes Hex v3 payload integrity and executes all byte values through native cat in the installed artifact. The release probe now also requires both pager assets and runs the installed Linux artifact through a controlling PTY, checking exact Unicode output, literal pager arguments, restored terminal settings and owned temporary cleanup. Injected server-app ZIP entries are rejected for unversioned and versioned application roots. Supplied public tars record the checkout used to run the probe separately from package provenance. `verify_published.py` rejects a public payload that differs from the validated source package, even if its untrusted CHECKSUM entry claims otherwise.

`cli-compatibility.yml` adds separate unit/wire/executable, 2 GiB resource, disposable ESS/RustFS, and platform package jobs. The proposed package matrix includes Linux Elixir 1.18.0/OTP 27, 1.18.5/OTP 28, 1.19/OTP 28 and macOS Elixir 1.19/OTP 28. Those new remote jobs have not yet run; their presence is not execution evidence. The three affected workflows pass actionlint locally. CLI compatibility uses explicit Bash/pipefail so tee cannot mask a resource-test failure. The server image push trigger skips CLI/documentation-only changes; build-workflow-only changes still have PR and explicit-dispatch coverage. Metadata-only Docker dependency resolution can load the CLI project without the not-yet-copied native compiler task, while actual compilation still requires that task. The provider job retains the four independent server failures and checks them through the exact pinned provider policy rather than treating their raw exit 1 as complete compatibility.

Release remains subject to final source checks, the complete scoped CLI acceptance suite, independent provider/AWS evidence, supported platform matrix, and the documented http_fetch 0.18 infinite-timeout upstream blocker (`gsmlg-dev/http_fetch#123`). A passing local package build does not close those gates.

## Assembled source package validation, 2026-10-15

The final assembled CLI source passed the repeatable package probe at
`/tmp/ess-cli-final-package-01a13b46`, using published dependencies and separate
build/dependency/source roots. Its version remains 0.6.7; this is an unpublished
implementation snapshot based on `9b8661e`, with `source_dirty: true`.

- Source tar SHA256: `d548b64e1a1da187ecd9de4ed6e24ac9f0fac2cb5e757005eeba4b475accf869`.
- Computed Hex payload checksum: `521b5b1dd2b0f577cec21b0f9974c8798d02561b6fb8324ba0fad3017264d687`.
- Escript SHA256: `f93f5a5bd1d3ca61f1377c9adf9a34a52982f5136e71c3033f91675889758d83`, with 1,338 ZIP entries.

All eleven probe contracts passed: source assets, published dependencies, strict
compile, escript assets/application exclusion, local installation, runtime after
source removal, model skeleton, real Unicode HTTP, all-byte raw stdout, computed
Hex integrity and installed Linux pager PTY. The JSON manifest and command log
retain exact commands and platform/runtime. No new public package or release is
implied; immutable committed-source and published-consumer checks remain separate.

## Own-license distribution correction

A later independent package audit found that the Hex source tar contained ESS's
MIT license but the downloadable escript omitted it. The CLI now embeds the
unchanged license as `priv/compatibility/LICENSE-ESS.txt`; the release workflow
also includes `LICENSE` in its downloadable assets and checksums. Package probes
require exact agreement between the source license and embedded notice and
reject missing, incorrect or duplicate escript notices. Eleven focused Python
package tests pass. Earlier tar/escript digests above predate this correction;
a fresh assembled-source probe is required before release.

## Stable integration package probe

After the 3,222-test integration run and the own-license correction, the fresh
probe passed all eleven contracts at
`/tmp/ess-cli-final-package-20261015-stable-a`. This supersedes the older local
artifact snapshots above for the current source. It is still version 0.6.7,
based on `9b8661e58107eddb24cb8f15495cee4ac979219a` with
`source_dirty: true`, on Linux x86_64 / Elixir 1.18.5 / OTP 28.

- Source tar SHA256: `65ba6b21ec87488f0c0970bed678e7f54736d317855730a5ed8f53c4ccf12d97`.
- Computed Hex payload checksum: `8b0bbd3c79b7a292f59b668df7dcd5fe50af675a61a5c8b5487dfb989a2efdce`.
- Escript SHA256: `424fd5915a24325b7d64920547aec9de151bb77acd72cd4436fa4fc9a572954f`, with 1,344 ZIP entries.

The probe checks exact ESS license agreement in the source and escript, model
and native assets, fresh published dependencies, strict compilation, local
installation, execution after source/build removal, actual Unicode and binary
HTTP bytes, payload integrity and controlling-PTY pager cleanup. The manifest
and command log retain the commands and digests. Python tooling passed 28 tests;
inventory regeneration/check and the three changed workflows' actionlint check
also passed. No next-version public release is implied by this result.
