# Standalone packaging and platform evidence

Validation dates: 2026-10-15 and 2026-10-16. The initial sections retain local snapshots based on `9b8661e58107eddb24cb8f15495cee4ac979219a` plus the implementation diff. Later sections record clean committed-source and remote package evidence. None is a published next-version package. The package still reports version `0.6.7`; release/version changes belong to the coordinating release step.

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

The initial NIF checks compiled both normal Linux and non-Linux-guarded C (`cc -U__linux__ -Wall -Wextra -Werror`) on this Linux host. That guard check alone is not a macOS ABI or execution test. The later remote matrix below supplies actual Darwin arm64 and Linux minimum-runtime package evidence. Interactive paging remains Linux-only; other platform/runtime combinations are unverified.

The updated `publish-cli.yml` uses Ubuntu with Elixir 1.19/OTP 28 and validates only the CLI and its dependency closure. It requires the requested version to be committed before dispatch; it does not make a hidden workflow version bump. It runs the source-package probe, publishes Hex source/docs, verifies the exact public Hex payload against the validated tar, and runs another extracted published consumer. It prepares the downloadable `ess-linux-x86_64-otp28` escript, SHA256SUMS, source/platform manifests, reference lock, coverage, and README. CLI-only dispatch uses `cli-v<version>`; a root release call attaches artifacts to its supplied version tag. The root `release.yml` also publishes the server image and core package, so it is not a CLI-only release path. No release workflow has been triggered for this implementation.

`scripts/cli_compatibility/package_probe.py` makes the standalone checks repeatable. In addition to required source/escript assets, it recomputes Hex v3 payload integrity and executes all byte values through native cat in the installed artifact. The release probe now also requires both pager assets and runs the installed Linux artifact through a controlling PTY, checking exact Unicode output, literal pager arguments, restored terminal settings and owned temporary cleanup. Injected server-app ZIP entries are rejected for unversioned and versioned application roots. Supplied public tars record the checkout used to run the probe separately from package provenance. `verify_published.py` rejects a public payload that differs from the validated source package, even if its untrusted CHECKSUM entry claims otherwise.

`cli-compatibility.yml` adds separate unit/wire/executable, 2 GiB resource, disposable ESS/RustFS, and platform package jobs. The actual package matrix results appear below. The three initially affected workflows pass actionlint locally. CLI compatibility uses explicit Bash/pipefail so tee cannot mask a resource-test failure. The server image push trigger skips CLI/documentation-only changes; build-workflow-only changes still have PR and explicit-dispatch coverage. Metadata-only Docker dependency resolution can load the CLI project without the not-yet-copied native compiler task, while actual compilation still requires that task. The provider job retains the four independent server failures and checks them through the exact pinned provider policy rather than treating their raw exit 1 as complete compatibility.

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

## Committed-source probe and first remote run

The clean committed implementation `b1df845ec3123a8a45122a13157d3b5631b12bee`
was pushed to `origin/main`. Its local extracted-package probe passed all eleven
contracts with `source_dirty: false` at `/tmp/ess-cli-package-b1df845`.
The source tar SHA256 is
`fae3cbea0276eed06a7eeeeddb9a43a7292bc17b3da2d6b5b9de09f13b956305`,
computed Hex payload checksum
`cfdec9463556490cdff45ffa16d631781ef18e54a640e14027d22ead71a82d62`,
and escript SHA256
`8e1aeb7c826fb189d38f9606068dbc093cf1ee0e23bf4a3601ad77c79526bd5e`.
This is still an unpublished 0.6.7 implementation artifact.

The first actual [remote CLI run](https://github.com/gsmlg-opt/ex_storage_service/actions/runs/38737634712)
failed before executing its runtime/provider/resource contracts. Fortified Ubuntu
rejected an ignored signal-handler write result; Darwin compilation did not expose
`O_NOFOLLOW`; Elixir 1.19 formatting differed in two source/test expressions.
These are CLI defects requiring in-scope repairs, not successful platform checks.
The accompanying umbrella CI compile/format failures have the same CLI causes.
The local successful probe above does not supersede those remote failures.

The ensuing CLI repair preserves the nonblocking signal pipe and real no-follow
flag: inspect write results, retry EINTR, coalesce EAGAIN/EWOULDBLOCK, and enable
Darwin declarations before headers. Fortified Linux and non-Linux guarded builds
pass; the actual Darwin result is recorded below. Two expressions now format identically
on Elixir 1.18.5 and 1.19.6. The affected 46-test suite and strict compile pass.
The b1df845 artifact above remains the exact earlier snapshot; subsequent
package/platform results are recorded below.

## Clean portability checkpoint packages

The local probe for clean source
`2380deac68b3181c6192c7047364390d6cd607c9` passed all eleven contracts at
`/tmp/ess-cli-package-pager-final`. Its tar SHA256 is
`aad74c878006c27b8d62710190a7a8a3bf5cd55e859279c9b83d96acb101627d`,
computed Hex payload checksum
`a46a615f695734448cd3cace92189913a9192b5a0a1e19e3c1c258f7ac67c843`,
and escript SHA256
`a7b26fff17a96e248ad61e50415ba0c3a12a0afac2ac11a8661e8e37e388c4fb`
(1,344 ZIP entries).

All four package jobs in [run 38739026057](https://github.com/gsmlg-opt/ex_storage_service/actions/runs/38739026057)
also passed with that exact clean source and version 0.6.7. Their common source
tar SHA256 is
`73ebea1d290b4ffebf6bc12cecf99c96f8a69d9e548fd8d193cd6d4439476d1e`.
Downloaded tar/escript bytes were independently hashed against each manifest.

| Runtime platform | Elixir / OTP | Escript SHA256 |
| --- | --- | --- |
| Linux x86_64 | 1.18.0 / 27 | `efda3ac07528712f8244ff23b7e7fcd61e7a7026eb4455797f99854c9b0b5bc6` |
| Linux x86_64 | 1.18.5 / 28 | `1f8ade53387121df9a9ac3e7e7e3d91751a867b042b309e2bb98ce3ea9a5bbce` |
| Linux x86_64 | 1.19.6 / 28 | `30787fe25ebf8343cc68c2c6e1cbd2994882d08153fbbacf4d4d6969a9180688` |
| Darwin arm64 | 1.19.6 / 28 | `18229d64d0af37fe1f78e080aa6681018328d4d9b1580862155a1f8a04396752` |

Linux ran eleven package contracts, including installed pager PTY cleanup;
Darwin ran ten, with no interactive pager claim. These are standalone
source-package/runtime probes, not complete per-platform unit suites. The
overall remote run failed its separate full test job; see
integration-validation.md. These artifacts are CI evidence, not public release
artifacts. The deadline blocker, complete-suite and publication gates remain
open, including http_fetch#129; no next-version release has been attempted.

The CI-environment correction checkpoint
`6816ee14c87ccc8de76a37a41811ec46a0c7aa9e` repeated all four package jobs in
[run 38740996062](https://github.com/gsmlg-opt/ex_storage_service/actions/runs/38740996062).
All passed with clean source and the same platform/check boundaries. All eight
downloaded tar/escript hashes match their manifests. The common source tar
SHA256 is `0062018bae267e39cf60f40bd544ada564f92ee97be116fc715e686ba640c593`;
the Linux x86_64 / Elixir 1.19.6 / OTP 28 escript SHA256 is
`7c83b3b07074047b6f717356566140f4a3e14705f4296374ef76ff1011ef5566`.
The four exact platform manifests and artifacts are retained at
`/tmp/ess-cli-6816ee1-packages`. These remain unpublished version 0.6.7 artifacts.
The separate complete CLI and umbrella Test jobs also passed at this source;
see integration-validation.md. The earlier checkpoint's failed test job remains
historical evidence, not the current complete-suite result. Deadline,
compatibility and publication gates remain open.

## macOS 26 package acceptance, 2026-10-16

Commit `6e3391625f4b9e539c2c8b87dcd43db493844bb0` replaces the macOS 14
package row with both macOS 26 architectures. GitHub's documented standard
labels are `macos-26` for arm64 and `macos-26-intel` for x86_64;
`macos-26-x86` is not listed in the [official runner table](https://github.com/actions/runner-images#available-images).
Both package jobs passed in [run 38867768925](https://github.com/gsmlg-opt/ex_storage_service/actions/runs/38867768925).

Downloaded manifests identify that exact clean source and Elixir 1.19.6/OTP 28.
Each macOS job passed twelve recorded commands and ten package/runtime checks,
including native loading, isolated installation, execution after source removal,
Unicode HTTP and raw binary stdout. The coordinator independently recomputed
the tar/escript SHA256 and inner Hex checksums. Both source tars have SHA256
`908b5468cf0e17d18914b8acb785b7bc5ef742fbc2b4f3c07798c13c4d157ee8`.

| Runner | Manifest architecture | Escript SHA256 |
| --- | --- | --- |
| `macos-26` | arm64 | `9156543844106f7fb5ecc52dc510baaea462e635cf751b51473994f0b240cc7a` |
| `macos-26-intel` | x86_64 | `1c9f4b518f41edda3ec4f466c6e7f0af3e91e1638fbd943e1d47da30aa8e6fa3` |

Evidence is retained under `/tmp/ess-cli-macos26-6e33916/`, including
`verified-packages.json`. Interactive paging remains Linux-only. These are
unpublished 0.6.7 package probes, not complete macOS unit suites or release
acceptance. Earlier dependency gates above are historical: selected HTTP
0.21.12 resolves #123/#129; current publication blockers remain #131/#132,
as detailed in [the HTTP evaluation](http-021-evaluation.md).

## Corrected implementation package evidence

All five package jobs passed at clean source
`582639746e65376ea7d1f5aaeb203bca34b5d79d` in
[run 38871534043](https://github.com/gsmlg-opt/ex_storage_service/actions/runs/38871534043).
The downloaded artifacts independently match their tar/escript hashes and inner
Hex checksums. Their common source tar SHA256 is
`6339e75ad1a5aa86ba6f105840a0e6a5ce889b1b9244b98cbadf31ec1168c067`,
also produced at the preceding 5ee5306 implementation checkpoint. Packaged
API/checksum sources match the corrected checkout. Native binary and C-source
digests, OS/CPU/OTP metadata, required model/license assets and absence of server
applications were checked, along with the coordinated published HTTP 0.21.12 apps.

| macOS 26 runner | Architecture | Escript SHA256 |
| --- | --- | --- |
| `macos-26` | arm64 | `caace7a3aa565ccd8eb28cc8f736c009108d8eb0c97c068806fb7a0cc4a5e5f1` |
| `macos-26-intel` | x86_64 | `8ee337c0be2dcc0a7952580e87d5284e22e1da5211ddfd943d1b621eac09488f` |

Each macOS artifact records twelve successful commands and ten package/runtime
checks; each Linux artifact records thirteen commands and eleven checks. The
verified manifests are `/tmp/ess-cli-ci-5826397/verified-packages.json`.
The complete standalone CLI run also passes, while the umbrella CLI retains its
single HTTP #132 cleanup failure. None of these unpublished 0.6.7 artifacts
closes the upstream, full compatibility or publication gates.

## 0.6.8 committed-source package and macOS 26 acceptance

All five jobs in [CLI run 38896782503](https://github.com/gsmlg-opt/ex_storage_service/actions/runs/38896782503)
built and executed version **0.6.8** from clean source
`eb3f3327519d98fbc21f0d3d06d48c4d52edc3d2`. Downloaded source tar, escript,
native assets and embedded model bytes were independently compared. Every tar
contains 789 source files matching that commit; the common tar SHA256 is
`a1392f6f2a50246ca050908f6e308683d4b7e7ff5fea3d2b8b0e46369be725d4`. Evidence is
`/tmp/ess-cli-ci-eb3f332/verified-packages.json`.

| Platform | Runtime | Escript SHA256 |
| --- | --- | --- |
| Darwin arm64 | 1.19.6 / OTP 28 | `c6c3e84c8062a6fdac3f12d90ea19109882625265575cf556c3919e2484ffca5` |
| Darwin x86_64 | 1.19.6 / OTP 28 | `6b4521ae4eb1636d736affd33928437f7099e6d7d4974f2d091bfec530f0fa79` |
| Linux x86_64 | 1.18.0 / OTP 27 | `960e1f2e146c5984ddb3f0a6fdd2365966c616666631242c291f3be10da9898b` |
| Linux x86_64 | 1.18.5 / OTP 28 | `f635c065f0915f2e282a4d465ef3ef4d2e6280abf06e37bdff8fd3c7085cbfbe` |
| Linux x86_64 | 1.19.6 / OTP 28 | `66637f6572208412a01ec63b18a47f174803707f381f6f6dd124fd0f2b30d132` |

Each installed artifact passes all ten credential-process tests with zero
failures, skips or exclusions, loading production modules only from its escript.
Both macOS manifests record `os: darwin` and all seven required checks: literal
argv, stderr suppression, bounded output, child-group isolation, and descendant
cleanup after timeout, owner death and overflow. The native metadata records
`aarch64` for the runner's `arm64` spelling; OS/CPU/OTP identity and asset hashes
match. Native browser/pager acceptance remains Linux-only. The downloaded
Linux Elixir 1.18.5/OTP 28 escript independently runs from `/tmp` and reports
`ess version 0.6.8`. These are pre-publication source artifacts; public-registry
verification remains a separate release step.

## Released 0.6.8 package and public runtime verification

The corrected source `515207e8c4063f689e499d999cf408e7f4c40ce1` passes all five
package jobs in [run 38899567013](https://github.com/gsmlg-opt/ex_storage_service/actions/runs/38899567013).
Both macOS jobs run actual macOS 26: `macos-26` is arm64 on 26.6.2/image
`macos-26-arm64` `20261013.0424.1`; `macos-26-intel` is x86_64 on 26.6.1/image
`macos-26` `20260824.0517.1`. Every installed artifact passes all ten
credential-process tests with no failures/skips/exclusions.

[CLI-only run 38900749007](https://github.com/gsmlg-opt/ex_storage_service/actions/runs/38900749007)
publishes version 0.6.8 and its source-tagged Linux x86_64/OTP 28 download.
Anonymous public assets/checksums, Hex package/docs, a fresh public consumer and
the actual downloaded escript pass independent verification. The publishing
tar contains 789 Git-identical files plus two lexer/parser outputs independently
reproduced from committed inputs; its exact source/inner checksums therefore
differ from CI's pre-compilation tar. [release-verification.md](release-verification.md)
records both mappings, all platform escript hashes and the actual public download
digest. Matching OS/CPU/OTP requirements and Linux-only interactive pager/browser
acceptance remain unchanged.
