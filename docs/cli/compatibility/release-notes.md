# Unreleased CLI implementation

This change implements the standalone `ess` client described in the CLI design.
The compatibility references are AWS CLI 2.31.18 and
`minio/mc@77f82e18b5401a65958f1619df6ebb994634bd88`. It has not been released.
The package version remains 0.6.7 until the release gates pass.

## Changes

- Shared native, AWS `s3`, and modeled `s3api` frontends use typed parsing,
  credentials, endpoints, signing, requests, errors and exit statuses. The
  catalog includes all nine high-level commands, 108 operations, seven
  paginators and four waiters from the locked reference.
- Bounded raw-byte transfers add multipart journals and recovery, guarded
  synchronization and moves, checksums, atomic downloads, independent source
  and destination contexts, and owned cancellation on SIGINT/SIGTERM.
- AWS configuration, temporary credentials, credential processes, assume-role,
  web identity, container/IMDS and IAM Identity Center providers share refresh
  ownership. Special S3 endpoints, SigV4a, directory sessions and EventStream
  operations have scoped implementation and wire evidence.
- Structured and shorthand input, YAML, JMESPath, output formats, paging,
  prompting and Bash/Zsh/Fish completion are part of the standalone package.
  Native aliases, management, historical inspection, sharing, polling and
  transfer inspection use the same S3 machinery.
- Source-package and installed-consumer probes check model/native/license
  assets, raw bytes, exact artifact integrity, pager ownership and absence of
  storage-server runtime applications. Separate CI jobs cover unit/wire tests,
  resources, providers and package/runtime combinations.
- Native `--conn-read-deadline` / `--conn-write-deadline` configure independent
  socket inactivity (ten-minute defaults; zero disables each). AWS positive
  `--cli-read-timeout` now measures socket read inactivity, and zero disables it.
  Connection setup has its own deadline; neither frontend imposes an overall
  request timeout. Published HTTP 0.21.12 supplies the required per-request
  network-await policy.
- AWS stdin `--expected-size` is a multipart sizing hint. Shorter or longer
  input retains its actual byte length; native `pipe` keeps its exact-size
  assertion. Pinned AWS executable captures cover both behaviors.
- Text pagination restores absent result-key placeholders; YAML-stream retains
  service-page fields and emits no synthetic resume-token page. Independent
  AWS captures cover full output, queries, limits and actual token resume.
- Omitted retention/legal-hold payloads remain zero-byte entities with correct
  checksums, distinct from empty XML structures. The expanded optional-member
  matrix covers 52 captured AWS cases across 27 operations.

## Migration and support

Valid legacy invocations and structured output retain compatibility adapters.
Unknown options now fail explicitly. Native commands follow the documented ESS
contract; they are not advertised as an exact replacement for every mc command,
option or output. Consult the [guide](../guide.md) and the
[native difference inventory](mc-native-gap-checklist.md).

Source installation needs Elixir 1.18+, Erlang/OTP, a C11 compiler and OTP
headers. Built escripts require matching OS, CPU and OTP-major native assets.
Remote package/runtime probes pass on Linux x86_64 with Elixir 1.18.0/OTP 27,
1.18.5/OTP 28 and 1.19.6/OTP 28, and macOS 26 arm64/x86_64 with Elixir
1.19.6/OTP 28, using `macos-26` and `macos-26-intel` respectively.
Other platform/runtime combinations remain unverified.
Interactive paging is currently Linux-only.

## Release gates

The [acceptance status](milestone-acceptance-status.md) records incomplete
requirements and the exact boundaries of the existing evidence. Inventory
coverage is distinct from independently verified behavior and live-provider
support.

The user-approved dependency update selects coordinated HTTP 0.21.12, extends
core's supported ranges, and requires `~> 0.21.12` for the standalone CLI.
HTTP 0.21.12 resolves the earlier infinity/socket prerequisites and
[#129](https://github.com/gsmlg-dev/http_fetch/issues/129)'s network-await
policy. Actual native and AWS-zero adapter downloads cross a 65-second network
gap with exact bytes and unchanged global settings. The initial isolated CLI
suite passed **3,244 tests, zero failures and one resource exclusion**.
The affected core/cluster/S3/web suites pass, and a fresh Linux x86_64
Elixir 1.18.5/OTP 28 source-package/install/escript probe passes with exact raw
bytes, native assets and standalone runtime checks. These local results are
distinct from the historical remote matrix below.

A distinct upstream acknowledgement deadline still fails during slow consumer
pacing: a 512-byte download at 8 B/s delivers all bytes at 63 seconds, then
returns a transport timeout. This release blocker is
[#131](https://github.com/gsmlg-dev/http_fetch/issues/131); the repository includes
a real-adapter diagnostic in `scripts/cli_compatibility/http_consumer_ack_reproducer.exs`.
No VM-wide timeout change, early-ACK drain or dependency patch is used.

Remote full-suite checks of pushed source `600c8755` exposed a second blocker,
[#132](https://github.com/gsmlg-dev/http_fetch/issues/132): a successful
fixed-length response can kill the upload enumerable producer before its
resource finalizer finishes. The CLI job failed two tests; the umbrella CLI
suite failed its source-cleanup test. Its other app suites passed. The separate
stalled-write fixture failure was reproduced under CPU contention and corrected
by selecting SHA256, retaining the real socket deadline and cleanup assertions;
12 scoped deadline tests pass. The upstream source-cleanup assertion remains
unchanged and its failure independently reproduced; a larger assertion timeout
cannot fix a killed producer.
No file-descriptor leak or payload corruption is established by the missing
cleanup notification alone. At that source, four-platform package, 2 GiB resource
and provider-policy jobs passed, as did CI/E2E/cluster workflows; they do not
close either blocker or turn the failed full suites into acceptance.
See [HTTP evaluation](http-021-evaluation.md) and
[integration validation](integration-validation.md) for the current evidence.

The later output/checksum fixes pass **3,271 local CLI tests, zero failures and
one resource exclusion**, with complete formatting and strict compilation.
All five package jobs pass at clean source `5826397`; downloaded manifests,
native assets, source tar and escript bytes were independently verified.
These artifacts still report 0.6.7 and remain unpublished. A separate evidence
description correction at `5826397` preserves all test assertions and statuses.
Its standalone CLI run passes 3,270 tests with zero failures and one resource
exclusion, plus all eight jobs. The umbrella CLI again fails only the unchanged
#132 cleanup assertion: 3,271 tests, one failure. Other app suites and separate
CI/E2E/cluster workflows pass. These results do not close either HTTP blocker.

Historical clean source 6816ee1 was pushed; its complete remote CLI
suite passed 3,222 tests with zero failures and one resource exclusion. Formatting,
strict compilation, four package jobs, 2 GiB resource acceptance and provider
limitation-policy checks also pass. The umbrella job passed 3,223 CLI tests and
its other app suites. The previously recorded completion/runtime-test failures
are resolved. Deadline integration, remaining independent compatibility evidence,
exact native differences, immutable release mapping and publication verification
remain explicit release gates.

The [provider report](provider-acceptance.md) preserves independent ESS 0.6.7
and RustFS 1.0.1 failures. Live AWS account-specific infrastructure has not been
validated. No public package, release tag or downloadable next-version artifact
is implied by a successful local build.
