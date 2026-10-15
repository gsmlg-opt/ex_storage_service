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

## Migration and support

Valid legacy invocations and structured output retain compatibility adapters.
Unknown options now fail explicitly. Native commands follow the documented ESS
contract; they are not advertised as an exact replacement for every mc command,
option or output. Consult the [guide](../guide.md) and the
[native difference inventory](mc-native-gap-checklist.md).

Source installation needs Elixir 1.18+, Erlang/OTP, a C11 compiler and OTP
headers. Built escripts require matching OS, CPU and OTP-major native assets.
Linux x86_64 / Elixir 1.18.5 / OTP 28 has local runtime evidence; other platform
and minimum-runtime jobs must be inspected before expanding that claim.
Interactive paging is currently Linux-only.

## Release gates

The [acceptance status](milestone-acceptance-status.md) records incomplete
requirements and the exact boundaries of the existing evidence. Inventory
coverage is distinct from independently verified behavior and live-provider
support.

The selected http_fetch 0.18 line lacks the required infinite-timeout fix
tracked in [#123](https://github.com/gsmlg-dev/http_fetch/issues/123) and the
socket-inactivity fix from
[#126](https://github.com/gsmlg-dev/http_fetch/issues/126). Published HTTP
0.21.11 contains both; its coordinated adoption requires the pending dependency
scope decision and affected-application validation. These contracts remain
blocked rather than approximated. Further actual wire checking found a separate
response-stream timer in 0.21.11; the missing request-scoped policy is tracked
as blocker [#129](https://github.com/gsmlg-dev/http_fetch/issues/129). No local
workaround or candidate dependency change has been applied. The current CLI implementation passed
3,222 tests with zero failures and one separate resource exclusion; formatting
and strict compilation also pass. Exact native differences, remote acceptance,
immutable source/artifact mapping and publication verification remain explicit
release gates.

The [provider report](provider-acceptance.md) preserves independent ESS 0.6.7
and RustFS 1.0.1 failures. Live AWS account-specific infrastructure has not been
validated. No public package, release tag or downloadable next-version artifact
is implied by a successful local build.
