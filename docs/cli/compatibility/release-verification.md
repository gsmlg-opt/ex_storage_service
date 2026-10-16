# CLI 0.6.8 release verification

Release acceptance is complete for the implemented M0–M9 scope and the explicit
compatibility boundaries below. This report follows the prepared source snapshot
shipped in the release assets; its historical pending checks do not describe the
final publication outcome.

## Immutable source and publishing boundary

- Release: [ESS CLI v0.6.8](https://github.com/gsmlg-opt/ex_storage_service/releases/tag/cli-v0.6.8).
- Public package: [ex_storage_service_cli 0.6.8](https://hex.pm/packages/ex_storage_service_cli/0.6.8).
- Source and remote release tag: `515207e8c4063f689e499d999cf408e7f4c40ce1`.
- Publishing: [CLI-only run 38900749007](https://github.com/gsmlg-opt/ex_storage_service/actions/runs/38900749007), with the complete source SHA supplied as `git_ref` and version `0.6.8`.
- Published at: 2026-10-16T12:38:32Z.
- No server/core publishing workflow was dispatched for this release. The approved coordinated HTTP dependency is the published `0.21.13`; no dependency patch or global timeout change is present.
- Baselines: AWS CLI `2.31.18`, mc `77f82e18b5401a65958f1619df6ebb994634bd88`; packaged reference-lock SHA256 `539328ea207f7129d9490948639139469e28d56120f836b095b9aef66d637ffd`.

## Corrected complete-source acceptance

[CLI run 38899567013](https://github.com/gsmlg-opt/ex_storage_service/actions/runs/38899567013)
passes all eight jobs. Its standalone unit/wire/executable suite records
**3,483 tests, zero failures, one resource exclusion**, in 452.6 seconds. All
**32 Python tooling tests**, inventory consistency, formatting and strict
compilation pass. Five package/platform jobs, separate 2 GiB resources and the
unchanged provider-limitation policy pass. The publishing workflow independently
repeats scoped validation and a fresh source-package install before publication.
Publishing validation passes **3,484 tests, zero failures**, in 437.3 seconds,
plus all 32 tooling tests. Source and registry-consumer probes each pass all
twelve package/runtime gates and ten installed credential-process tests.

[Umbrella run 38899566818](https://github.com/gsmlg-opt/ex_storage_service/actions/runs/38899566818)
passes all application suites: core **343/0 failures/1 exclusion**, cluster
**38/0/1**, CLI **3,484/0/0**, S3 **224/0/0**, web **21/0/0**. The root CLI
run includes the default resource case; its duration is 407.5 seconds. Separate
CI, E2E and cluster-integration workflows succeed at the same source.

The failed `8f2f863` run and the two cancelled publishing attempts remain in
[integration-validation.md](integration-validation.md). Test-only repairs at
`515207e` synchronize mutation with normal body-producer completion and isolate
an initial journal-port collision using a zero-wire guard. Production ownership,
source-change, cancellation, abort and no-completion assertions remain intact.
Passing corrected complete runs supersede the gate without removing that history.

## Package/platform proof

All five downloaded CI packages identify the exact clean source above. Each
contains **789 files byte-for-byte matching Git**. Tar/escript/native and
packaged model hashes match, and every recorded command exits successfully.
The common source tar SHA256 is
`a1392f6f2a50246ca050908f6e308683d4b7e7ff5fea3d2b8b0e46369be725d4`;
its inner Hex checksum is
`f3627894100612eab13e8eed0c083c216245f0653f367d6e44b1c62bb35ca5c8`.

| CI platform | Runtime | Escript SHA256 |
| --- | --- | --- |
| Linux x86_64 | Elixir 1.18.0 / OTP 27 | `54c8d7f653efd6cf3d7ec9d8a213f96d59a988326732bb90ac2731f9f595642b` |
| Linux x86_64 | Elixir 1.18.5 / OTP 28 | `75bc4adcc1766372cd44aae7d49208db6bd5e9418e24e39d60b242a84b916662` |
| Linux x86_64 | Elixir 1.19.6 / OTP 28 | `f3d4ade540d52dd061408d04f199d4f00a82c62cdbe345272fed3d5c414c45bb` |
| macOS 26 arm64 | Elixir 1.19.6 / OTP 28 | `bf20c6f90b10487b63be23e939de243d664e70ee905bacc9b879a9ee3f4db844` |
| macOS 26 x86_64 | Elixir 1.19.6 / OTP 28 | `87f4f66d07866a80bbb33d1cc5489e4182c76e53ad1e9b0040784dd2d81d5a1a` |

Every installed artifact runs all ten credential-process tests with no failures,
skips or exclusions and all seven required process/descendant-cleanup checks.
Both Darwin variants load their production modules from the installed escript.
The actual hosted runner logs confirm:

| Workflow label | Actual macOS | Runner image |
| --- | --- | --- |
| `macos-26` | 26.6.2, build `25G83` | `macos-26-arm64`, `20261013.0424.1` |
| `macos-26-intel` | 26.6.1, build `25G76` | `macos-26`, `20260824.0517.1` |

`macos-26-x86` is not the documented GitHub label. These are package/runtime
probes, not complete macOS unit suites. Interactive pager/browser acceptance
remains Linux-only. Source installation needs Elixir 1.18+, a C11 compiler and OTP headers;
built escripts require matching OS, CPU and OTP major.

## Resource and provider proof

The independent 2 GiB upload/download test passes in **151.7 seconds** under a
**1 MiB admission budget**. It samples 12,461 times: peak BEAM total memory
64,411,592 versus baseline 63,463,128 (**948,464-byte delta**); peak binary
1,967,808 versus baseline 1,225,264; maximum mailbox two and one admitted request.
Admission drains to zero active requests/bytes. A local disk stage can hold the
whole 2,147,483,648-byte object; the bounded-memory claim does not prohibit disk
staging. These measurements are test evidence, not universal memory/throughput
promises.

The unchanged independent provider policy reports **zero client regressions**,
with **ESS ten passes/three known server failures** and **RustFS thirteen
passes/one known server failure**. ESS retains literal-key inventory, Unicode
multi-delete and Content-Encoding metadata failures; RustFS rejects dot-segment
keys. Both independent executables retain those failures; raw gzip/deflate bytes
pass separately. Both owned buckets per provider are removed. Exact pinned
images and signatures remain in [provider-acceptance.md](provider-acceptance.md).
Four known server failures are not counted as provider passes.

## Independent public verification

| Public artifact | Verified value |
| --- | --- |
| Hex tar SHA256 | `644e0807893e95eb9eef29ecb7ff325273bdf36d0bc67aa30fc6e25c97d2d3ea` |
| Hex inner checksum | `d8aa7c02c8512b43e031beca3001c774bf3a4ad3fcf5be67c58a30126b732cb8` |
| GitHub Linux x86_64 / OTP 28 escript SHA256 | `622f92cabee0d2b4c2b0f52e94af42d783179b33cd530009a6d125e0e09ae41c` |

The public tar matches the publishing build manifest, including its inner
checksum. It has **791 files: 789 tracked files exactly matching Git and two
generated Erlang files**. The publisher validates/compiles before building its
source package, unlike the isolated CI package probes. This accounts for the
different CI/public source-tar hashes above. Both generated files were
independently regenerated from the committed `.xrl`/`.yrl` inputs with OTP 28
parsetools 2.7.1; bytes match after normalizing only the absolute OTP include
path in `-file` attributes.

| Generated public file | SHA256 |
| --- | --- |
| `lib/ex_storage_service_cli/query/src/ess_cli_jmes_lexer.erl` | `e6d081f1ad250a7f3d3ce1af212f8b0747595cc1bc5f29ce09debd3dda59aa27` |
| `lib/ex_storage_service_cli/query/src/ess_cli_jmes_parser.erl` | `7aeca9e689b7bc53f3732e321894e7f2c6a3a709a47c009d22d32b70c625e1c6` |

A separate local consumer anonymously downloads the public tar and passes all
thirteen recorded commands/twelve package gates on Elixir 1.18.5/OTP 28. It
compiles strictly, installs an escript, removes source/build/dependency paths
from their original locations, then verifies model output, real Unicode HTTP,
raw binary stdout, all ten credential-process tests and interactive pager PTY
ownership/restoration. The public GitHub escript independently runs from `/tmp`,
reports `ess version 0.6.8` and generates the modeled GetObject input skeleton.

The publishing build manifest records the exact clean release source. Its
archive-only published-consumer manifest records `source_sha: null`,
`source_dirty: true` and `probe_source_sha` equal to the release SHA; it is not
presented as a Git-clean build. Provenance is established by the public tar's
tracked-source comparison, generated-source reproduction and clean build
manifest. The independent local public consumer records a clean probe checkout.

The anonymous Hex release API returns version 0.6.8 with the exact public tar
checksum. [Versioned API documentation](https://hexdocs.pm/ex_storage_service_cli/0.6.8/api-reference.html)
returns HTTP 200 and title `API Reference — ex_storage_service_cli v0.6.8`; its
51,058-byte body SHA256 is
`5629553602e0b82b71a12c4723201003120106e019e2b6963619d1f1349e0d6f`.
The remote `cli-v0.6.8` tag directly resolves to the full release source SHA.

The checksum list verifies all nine payloads among ten anonymously downloaded
GitHub assets. Published README, LICENSE, models and reference/report assets
match the immutable source snapshot. The release retains its prepared acceptance
report; this subsequent report records the completed public verification.

## Compatibility limits retained at release

The pinned catalog contains all nine AWS high-level commands, 108 API operations,
670 shapes, seven paginators and four waiters. The requirement overlay preserves
`implemented-unverified` rows where exhaustive independent member/enum evidence
is absent. All 106 native command/global rows have tested mappings or explicit
scope differences; this release does not claim exact mc replacement.

Live AWS identity/account/hardware cases, including advanced STS/SSO, Outposts,
Object Lambda, MRAP and directory-bucket infrastructure, remain unrun. Their
wire/cryptographic fixtures are separately labeled and never counted as live
provider passes. See [milestone-acceptance-status.md](milestone-acceptance-status.md)
and the linked requirement/provider reports for the declared implementation and
evidence boundary.
