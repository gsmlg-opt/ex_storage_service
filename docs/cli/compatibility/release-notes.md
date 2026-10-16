# ESS CLI 0.6.8

Standalone `ess` implementation against AWS CLI 2.31.18 and
`minio/mc@77f82e18b5401a65958f1619df6ebb994634bd88`. The release uses the CLI-only publishing workflow and the validation boundary below.

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
  request timeout. Published HTTP 0.21.13 supplies separate per-request
  network-await and consumer-acknowledgement policies while retaining bounded
  sink delivery, cancellation and explicit finite total budgets.
- AWS stdin `--expected-size` is a multipart sizing hint. Shorter or longer
  input retains its actual byte length; native `pipe` keeps its exact-size
  assertion. Pinned AWS executable captures cover both behaviors.
- Text pagination restores absent result-key placeholders; YAML-stream retains
  service-page fields and emits no synthetic resume-token page. Independent
  AWS captures cover full output, queries, limits and actual token resume.
- Omitted retention/legal-hold payloads remain zero-byte entities with correct
  checksums, distinct from empty XML structures. The expanded optional-member
  matrix covers 57 captured AWS cases across 32 operations, including object
  attributes, inventory encryption and intelligent-tiering filters.
- Version-specific native undo verifies the provider's returned version identity
  before copying. Confirmed dry-run recovery prepares copy-source encoding
  through the same customization path as live requests.
- RenameObject creates one UUID token for an omitted ClientToken and reuses it
  across retries. Explicit tokens remain unchanged. Restored Glacier/Deep Archive
  objects are eligible for high-level transfers without forcing an archive read.
- Native conditional `put --if-not-exists` protects single writes and multipart
  completion, including resume. Recursive tag selection adds the pinned mc
  `--exclude-folders` full-key rule. Polling watch adds events/prefix/suffix and
  recursion selectors while retaining complete reconciliation snapshots.
- Native `anonymous set private` aliases the existing guarded `none` policy
  replacement. Provider acceptance of the empty-policy representation remains
  separately documented.
- Native listing adds incomplete multipart inventories, exact storage-class
  filtering and bounded summaries with explicit completeness. Incomplete-upload
  byte totals remain unknown; historical combinations fail explicitly.
- Native put adds explicit part sizing, single-PUT selection and supported
  checksum selection, retaining conditional creation and response verification.
  Explicit parallel parts remain bounded by global admission and journal-owner
  checkpoints; workers and requests drain before owned-upload abort.

- Native diff reports destination-only entries and consistently compares source
  times in either transfer direction. Native rm adds versions/fake aliases and
  bounded age predicates with complete preflight and conditional deletion fences.

- Native SQL adds CSV/JSON serialization, compression, bounded recursive selection
  and raw Records stdout, preserving legacy atomic outfile publication.

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

## Compatibility and validation boundary

The client catalogs all nine high-level commands, 108 S3 operations, seven
paginators and four waiters from the pinned baseline. Generated inventories
separately track implementation, independent wire evidence and provider
acceptance; they do not claim exhaustive independent verification of every
member/enum variant. Native commands retain the documented ESS grammar and
[explicit mc differences](native-compatibility-dispositions.md).

Published HTTP 0.21.13 resolves the former request-idle, slow-consumer ACK and
upload-finalization blockers without dependency patches or VM-wide timeout
changes. The current graph requires `~> 0.21.13` for the CLI; core's approved
`~> 0.21.12` range already admits it.

Raw s3api SSE-C keys use the pinned base64/MD5 customizations, with explicit-MD5
pairs preserved. Empty bucket-location responses decode to JSON null. STS role
and web-identity credentials use the packaged partition/endpoint rules and
regional signing; configured service endpoints remain independent of data
`--endpoint-url`. Credential-process package probes require literal argv,
bounded output and descendant cleanup using the installed escript modules on
Linux and both macOS architectures.

The final local standalone suite passes 3,483 tests with zero failures and one
resource exclusion; formatting, strict compilation and all 32 tooling tests pass.
Corrected committed-source CI passes 3,483 standalone and 3,484 umbrella CLI tests with
zero failures. All five package jobs, measured 2 GiB resources and the independent
ESS/RustFS limitation policy pass. Downloaded source/escript/native/model hashes
match the tested commit, and both macOS architectures pass all ten installed
credential-process checks. CLI-only publishing passes another 3,484 tests with
zero failures, publishes Hex/GitHub version 0.6.8 from
`515207e8c4063f689e499d999cf408e7f4c40ce1`, and verifies a fresh published consumer.
Independent anonymous public-asset, registry, documentation, source and installed
runtime verification pass. The public tar adds two source-reproduced lexer/parser
outputs to the 789 Git-matching files, so its checksums are recorded separately
from CI's source tar. Exact results and immutable source mapping are in
[release verification](release-verification.md),
[integration validation](integration-validation.md),
[packaging evidence](packaging-platform-decisions.md) and
[acceptance status](milestone-acceptance-status.md). Historical failed runs and
HTTP 0.21.12 blockers are retained in those reports with their version scope.

ESS and RustFS have explicitly recorded server limitations. Live AWS advanced
identity, Outposts, Object Lambda, MRAP and directory-bucket infrastructure was
not available; independent wire/cryptographic fixtures are labeled separately.
No live-service success is inferred from a generated catalog or fixture pass.
