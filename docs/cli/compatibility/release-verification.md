# Unified 0.6.9 release verification

All six component version declarations are **0.6.9**, published together under
canonical [v0.6.9](https://github.com/gsmlg-opt/ex_storage_service/releases/tag/v0.6.9)
through the manually dispatched root `release.yml`. Both reusable Hex publishers
and finalization pass; no new CLI-prefixed tag is created.

## Canonical publication and checksums

- Workflow: [39048384577](https://github.com/gsmlg-opt/ex_storage_service/actions/runs/39048384577), all four jobs successful.
- Pre-bump source and compiled image source: `dfb3a314ae822b51944136ba6b923c3eb7ba443e`.
- Canonical tag and synchronized version commit: `5efddeac829cec1358a8c206a44b0ed401a27ba8`; its parent is the source above and its only changes are six version literals.
- GitHub release published at **2026-10-17T17:08:36Z**.
- Actual image build-start timestamp: **2026-10-17T16:35:31Z**, independently matched against compiled metadata and OCI labels.

| Published artifact | SHA256 |
| --- | --- |
| Core Hex tar, `ex_storage_service 0.6.9` | `58829dec7bcf678f77f763174c334861602505010df01451567df1a1e7c19c32` |
| CLI Hex tar, `ex_storage_service_cli 0.6.9` | `a978c889bbf3d182206c26e59cac824cf5f240b14f26dcdda6b8ac5aceb59f3e` |
| GitHub Linux x86_64 / OTP 28 escript | `0a839369113025f6c5a85acfb0968babc0e4b07124fa87f68cb13cdb7b204738` |
| `ghcr.io/gsmlg-dev/ess:0.6.9` and `latest` manifest | `3a6889306051d7b5626a5208a945e3cca2be292bc12f06448ec170c3eaf437dd` |

Fresh Actions candidate manifests supply the expected package hashes before
anonymous public downloads. All **21 GitHub assets** match their API sizes and
digests; `SHA256SUMS` covers all **20 payloads** exactly once. Its own SHA256 is
`3940c0e3ad301dec22fe9baceec10ec3dd21cf6f086f6f8f394731ea66eae355`. The canonical source archive SHA256 is
`c4f3ac72d69f498ea6577afc604fe706bce167c4da82693da51f0c665bc421b4`.

Both public Hex outer/API/inner checksums match the fresh candidates. Core has
**109 tracked files**, all matching canonical Git source, with no extras; its
inner checksum is `b94412312a5b3d07ee9491829b362368911c1aa888ad3eea2301d79139cbc519`.
CLI has **789 matching tracked files** plus only the two generated Erlang files
backed by the current `.xrl`/`.yrl` inventory. Their bytes are bound to the fresh
candidate tar hash; this audit does not claim to regenerate them.
CLI's inner checksum is `77f9aab84aee34c8872613ebbfc3d1c45b4c10d16ecd8c775bb49bbf3aeba06b`.

| Generated CLI source | SHA256 |
| --- | --- |
| `ess_cli_jmes_lexer.erl` | `e6d081f1ad250a7f3d3ce1af212f8b0747595cc1bc5f29ce09debd3dda59aa27` |
| `ess_cli_jmes_parser.erl` | `7aeca9e689b7bc53f3732e321894e7f2c6a3a709a47c009d22d32b70c625e1c6` |

## Tests, fresh installation and actual public runtime

The root workflow passes inventory consistency, formatting, strict compilation,
**47 compatibility-tooling tests**, **25 release-tooling tests** and complete
umbrella suites: core **343/zero failures/one exclusion**, cluster **38/zero/one**,
CLI **3,485/zero/zero** in 462.6 seconds, S3 **224/zero** and web **21/zero**.
The CLI publisher separately passes **3,485 tests, zero failures** in 351.8
seconds; core passes **343 tests, zero failures, one exclusion**. Fresh candidate
and public-package consumers pass for both Hex packages. CLI build and public
consumers each pass all twelve package/runtime gates and all eleven installed
credential tests, with eight named checks and zero failures/skips/exclusions.

The exact anonymously downloaded, checksum-verified escript runs from an
unrelated working directory on Linux x86_64, **Elixir 1.18.5 / OTP 28**, without
build or installation commands. It reports `ess version 0.6.9`, renders modeled
help/skeletons, passes all **eleven** installed credential-process regressions
and **eight** named checks including `prelaunch-owner-monitor`, and exchanges
an actual Unicode HTTP response. Raw stdout preserves **65,792 bytes**, SHA256
`120c518a83325c66464701a6ee080302f332bc768ea3f60473b209f1bfb091df`. Linux pager PTY ownership/restoration and native
extraction cleanup pass. The probe scripts and packaged CLI bind to canonical
source `5efddeac829cec1358a8c206a44b0ed401a27ba8`; image compilation separately binds to pre-bump source
`dfb3a314ae822b51944136ba6b923c3eb7ba443e`.

All five clean-source pre-bump package combinations at `dfb3a314ae822b51944136ba6b923c3eb7ba443e` independently
pass source/native/model/escript checks and eleven credential tests per package;
all **68 consumer commands** exit zero. This includes macOS **26.6.2 arm64** on
`macos-26` and **26.6.1 x86_64** on `macos-26-intel`, both Elixir 1.19.6/OTP 28,
plus the three declared Linux combinations. Those executables report **0.6.8**;
they are source/platform evidence rather than independently executed 0.6.9 Mac
binaries. [Integration validation](integration-validation.md) retains exact
runs, 2 GiB resource measurements and all earlier failures/unknown causes.

## Independent image, public indexes and documentation

Anonymous hashing verifies the manifest, config and all **five compressed image
layers**, then every filesystem DiffID. The imported untagged image config ID
matches. An owned Linux amd64 container runs with no network, host binds or
published ports; `/health`, `/health/ready` and admin `/login` all return HTTP 200.
RPC confirms four server apps at **0.6.9**. Compiled ref `v0.6.9`, source SHA,
actual build time and OCI labels match the fresh workflow inputs. Owned container
and anonymous-volume removal are verified. After finalization, anonymous `latest`
manifest hashing equals the exact versioned digest above.

Both Hex indexes identify **0.6.9** as latest stable, and GitHub's latest stable
release is canonical **v0.6.9**. Versioned API pages return HTTP 200 with exact
package/version titles. Core is 40,736 bytes, SHA256
`7eed9952ae0071290f0bebad770abebdecbb74efa229d84474eceaca51e1b1eb`; CLI is 51,058 bytes, SHA256
`2bfd7bee15dfd82576772b7f528de9ff8dd43d1d098446a716208561ae15df92`. HexDocs redirects to the exact package-specific hosts
`ex-storage-service.hexdocs.pm` and `ex-storage-service-cli.hexdocs.pm`, retaining
`/0.6.9/api-reference.html`. The first read-only index probe rejected this valid
canonical redirect; the corrected probe accepts only the exact original or
package-specific URL and still requires the exact versioned title. Both attempts
are retained.

## Evidence scope

References remain AWS CLI **2.31.18** and locked mc
`77f82e18b5401a65958f1619df6ebb994634bd88`. Live AWS account/hardware evidence,
exhaustive independent member/variant coverage, native mc differences and
Linux-only interactive pager/browser acceptance remain explicitly bounded.
The provider-policy pass retains **ESS ten passes/three known failures** and
**RustFS thirteen passes/one known failure**; those four failures remain provider
limitations. See [milestone acceptance](milestone-acceptance-status.md).

Prepared reports in immutable release assets retain their source snapshot;
this post-publication report records completed 0.6.9 verification. The following
0.6.8 sections are historical, including their then-current `latest`/index checks.
Historical `cli-v0.6.8` downloads remain available.

# Historical unified 0.6.8 release verification

All umbrella components and the standalone CLI share version **0.6.8** and the
canonical [v0.6.8 release](https://github.com/gsmlg-opt/ex_storage_service/releases/tag/v0.6.8).
Publication completed through the manually dispatched GitHub `release.yml`;
the core and CLI publishers are reusable children of that workflow.

The earlier CLI-only publication below is historical. Its `cli-v0.6.8` links
remain available; new releases do not create CLI-prefixed tags. Reports shipped
as release assets preserve their prepared source snapshots. This subsequent
report records the unified publication and its public verification.

## Canonical publication

- Workflow: [unified release 39012552369](https://github.com/gsmlg-opt/ex_storage_service/actions/runs/39012552369), all four jobs successful.
- Source before version synchronization: `fa3f31cc58f7d25fc5b46f346179aa191b3714b6`.
- Canonical tag and synchronized version commit: `d449e65d1076e21414dd37bcb7bcc9a47cc325ac`.
- GitHub release published at: **2026-10-17T08:06:42Z**.
- All six version declarations are `0.6.8`. The version commit changes only five version literals; the CLI was already `0.6.8` and its immutable package bytes remain unchanged.
- Image build-start timestamp: `2026-10-17T07:32:17Z`. Compiled/OCI build identity uses `v0.6.8` and the pre-bump source SHA above, as required by the build metadata contract.

| Published artifact | SHA256 |
| --- | --- |
| Core Hex tar, `ex_storage_service 0.6.8` | `917b5f588f6e4c5401c628ff7d75b6b5982abf287b4fd55086130dc3b74384dd` |
| CLI Hex tar, `ex_storage_service_cli 0.6.8` | `644e0807893e95eb9eef29ecb7ff325273bdf36d0bc67aa30fc6e25c97d2d3ea` |
| Canonical GitHub Linux x86_64 / OTP 28 escript | `81db90ddf0cca34694d55609c27ea1ea672bd5623ff4b5fdf283aab3f752e5ea` |
| `ghcr.io/gsmlg-dev/ess:0.6.8` and `latest` manifest | `4661cfee4db9731c160568dc4202d8d911e7d5dbc18a59e75b13ac174827f1db` |

Before publication the root workflow passes inventory consistency, formatting,
strict compilation, **46 compatibility-tooling tests and 21 release-tooling
tests**, and the complete umbrella suites: core **343/0 failures/1 exclusion**,
cluster **38/0/1**, CLI **3,484/0/0**, S3 **224/0/0**, and web **21/0/0**.
Fresh core and CLI source-package consumers pass before the atomic main/tag
push. The CLI registry preflight verifies exact equality with the existing
immutable package; core publication creates the missing version. Both reusable
publishers then pass their scoped tests and fresh public-package consumers.
The CLI source and public consumers each pass all twelve package/runtime gates
and all ten installed credential-process tests, with zero failures, skips or
exclusions. Only after both publishers succeed does the root workflow promote
`latest` and create the canonical GitHub release with aggregate checksums.

Independent anonymous Hex API checks return version `0.6.8` and the exact tar
checksums above for both packages. Their versioned API documentation returns
HTTP 200 with the correct package and version in each title. The core API page
is 40,736 bytes, SHA256
`659f871f9485102387d4612cb9e2e2c7857d2dff4dbeca2d2c41e9dead4e444c`;
the CLI page is 51,058 bytes, SHA256
`5629553602e0b82b71a12c4723201003120106e019e2b6963619d1f1349e0d6f`.

## Independent canonical artifact verification

All **21 public GitHub assets** were downloaded anonymously. Their API sizes
and digests match; `SHA256SUMS` covers all **20 payloads** exactly once and its
own SHA256 is
`d728692526cc58974d6aefe4f786f5f121ac4e9a5c31d698559cd661c0c5695c`.
The remote canonical tag, source-parent relationship, six version declarations,
release provenance, source/build manifests and registry reuse records agree.

The public core tar contains **109 tracked files exactly matching the canonical
GitHub source archive**, with no extras. Its inner Hex checksum is
`6f40cd71efcf4824e7e9f71822fce4b2b532a1d443bf56aa414afd0ea2bca7f1`.
The unchanged public CLI tar contains **789 matching tracked files and only the
two expected generated Erlang files**, whose exact hashes remain recorded in
the historical report below. Its inner checksum remains
`d8aa7c02c8512b43e031beca3001c774bf3a4ad3fcf5be67c58a30126b732cb8`.

The exact checksum-verified canonical escript runs from an unrelated working
directory on **Elixir 1.18.5 / OTP 28**, without build or installation commands.
It reports `ess version 0.6.8`, renders GetObject help and an input skeleton,
passes all ten installed credential-process tests and seven cleanup checks,
and handles an actual Unicode HTTP response. Raw stdout preserves all
**65,792 bytes**, SHA256
`120c518a83325c66464701a6ee080302f332bc768ea3f60473b209f1bfb091df`.
Interactive Linux pager PTY ownership/restoration passes, and native extraction
temporary files are removed.

Independent anonymous manifest-body hashing confirms that image tags `0.6.8`
and `latest` have the exact manifest digest above. The verified public image
runs in an isolated container with no network, published ports or host binds.
`/health`, `/health/ready` and the admin `/login` page all return HTTP 200.
RPC confirms all four server applications at `0.6.8`; runtime build metadata
and OCI labels agree on production version `0.6.8`, ref `v0.6.8`, pre-bump source
`fa3f31cc58f7d25fc5b46f346179aa191b3714b6` and actual build-start timestamp
`2026-10-17T07:32:17Z`. The owned container and anonymous volume are removed.

The initial local Docker pull timed out after 600 seconds; its exact cause is
unproven. Anonymous recovery rejected a largest-layer response truncated at
90 MiB, fetched the missing 163,624 bytes with a validated Content-Range, and
verified the complete manifest, config, all five compressed layer hashes and
all five uncompressed filesystem digests before import. Runtime execution is
bound to that public manifest through verified image/config ID
`sha256:3ec39c0a110658780bae2d5b8ccf709debf86e8914969ffbe75de07f52cf4498`.
A temporary probe initially omitted its generated environment variables and
correctly failed the production startup guard; correcting the probe's environment
handoff produced the passing checks above. Timeout, truncated-response and
failed-probe evidence is retained alongside the successful import/runtime audit.

## Historical unified-release interruption

On the earlier source
`33c58fe04fbdb8c860e8eb7cbf6142df8db6751c`,
[CLI compatibility 38926969934](https://github.com/gsmlg-opt/ex_storage_service/actions/runs/38926969934)
passes all eight jobs, including both macOS 26 architectures and the separate
2 GiB resource case.
[Umbrella tests 38926970061](https://github.com/gsmlg-opt/ex_storage_service/actions/runs/38926970061)
pass core **343/0 failures/1 exclusion**, cluster **38/0/1**, CLI **3,484/0/0**,
S3 **224/0/0**, and web **21/0/0**. The Linux journal-port reservation step
successfully reads back `49152-65535`; explicit socket binds remain unchanged.

[Server E2E 38926969954](https://github.com/gsmlg-opt/ex_storage_service/actions/runs/38926969954)
fails after batch deletion: the following HeadObject expects HTTP 404 and receives
400. Its server log records a Boto3 User-Agent fragment parsed as a request line,
supporting a keepalive framing problem. The batch-delete handler discards the
updated connection from `read_full_body/1`; exact packet-level causality remains
unverified. Server, E2E and dependency sources are unchanged from the previous
passing run. Restart/persistence checks were skipped, so passing unit tests do
not establish the complete server contract. Publication was stopped at that
point pending scope clarification; no S3 fix or local workaround was applied.

[Unified release 38927117399](https://github.com/gsmlg-opt/ex_storage_service/actions/runs/38927117399)
was cancelled during prepublication validation, before canonical tag, image,
registry or GitHub release publication steps. The earlier short-SHA dispatch
38926990600 failed at checkout without publication. The canonical tag was absent
when checked after cancellation. No immutable published CLI bytes were changed.

On current source `fa3f31cc58f7d25fc5b46f346179aa191b3714b6`,
[server E2E 38928241938](https://github.com/gsmlg-opt/ex_storage_service/actions/runs/38928241938)
passes the signed exercise, post-delete HeadObject HTTP 404, actual restart,
and checksum/listing persistence checks. Both server logs contain zero Bandit
HTTP errors. Core, S3, web, E2E, dependency-lock and E2E-workflow bytes are
identical across the failed and successful sources. This current passing
observation allowed the authorized unified release to resume; it does not
establish a fix or disprove the unchanged intermittent framing concern.
[CLI 38928236430](https://github.com/gsmlg-opt/ex_storage_service/actions/runs/38928236430),
[umbrella 38928236406](https://github.com/gsmlg-opt/ex_storage_service/actions/runs/38928236406),
[cluster 38928240500](https://github.com/gsmlg-opt/ex_storage_service/actions/runs/38928240500)
and [CI 38928236318](https://github.com/gsmlg-opt/ex_storage_service/actions/runs/38928236318)
also pass at this source.

## Historical CLI-only source and publishing boundary

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
