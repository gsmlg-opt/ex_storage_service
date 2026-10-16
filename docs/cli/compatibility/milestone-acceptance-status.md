# Milestone acceptance status

M0–M9 implementation and the declared pre-release validation matrix are complete
within the documented compatibility boundaries. The remaining gate is CLI-only
publication and independent verification of the public package/artifacts. This
report separates implementation, wire fixtures and live-provider evidence; it
does not advertise exact mc replacement or exhaustive independent verification
of every AWS member/variant.

Version **0.6.8** is committed and pushed at
`eb3f3327519d98fbc21f0d3d06d48c4d52edc3d2`. Implementation began at
`9b8661e58107eddb24cb8f15495cee4ac979219a`. References remain AWS CLI **2.31.18**
and mc `77f82e18b5401a65958f1619df6ebb994634bd88`; exact digests are in the
reference lock. The release report commit follows the tested implementation
without changing packaged CLI source.

[CLI run 38896782503](https://github.com/gsmlg-opt/ex_storage_service/actions/runs/38896782503)
passes all eight jobs: 3,482 standalone tests/zero failures/one resource exclusion,
32 tooling tests, five isolated package/runtime combinations, separate measured
2 GiB acceptance and the unchanged ESS/RustFS limitation policy. The local
complete run passes 3,483 tests/zero failures/one resource exclusion.
[Umbrella run 38896782363](https://github.com/gsmlg-opt/ex_storage_service/actions/runs/38896782363)
passes 3,483 CLI tests with zero failures; all other application suites pass.
Formatting and strict compilation CI also pass.

All five downloaded source packages contain 789 files matching Git; source,
escript, native and model hashes match their manifests. Both macOS 26 arm64 and
x86_64 installed artifacts pass all ten credential-process tests with no skips
or exclusions, including descendant cleanup. Historical HTTP 0.21.12 blockers
and failed runs remain version-scoped in
[integration-validation.md](integration-validation.md). Published HTTP **0.21.13**
passes the former #129/#131/#132 contracts, including bounded slow delivery,
request policy isolation and unchanged source-finalization regressions.

Live AWS account/hardware cases remain unrun. ESS ten passing contracts/three
server failures and RustFS thirteen passing contracts/one server failure retain
exact independent controls and owned cleanup; a policy pass is not a provider
success for those four failures. Interactive pager/browser acceptance remains
Linux-only. These limits and the native differences below remain part of the
release's stated contract.

Checked tasks in the plan describe implemented behavior, not milestone or
per-requirement verification. The [evidence overlay](implementation-evidence.json)
keeps source/test links and separate wire/provider evidence. A shared model or
matching flag spelling does not prove every effect. M8 permits a tested mapping
or an explicit scope/difference record; all 106 native command/global rows have
[dispositions](native-compatibility-dispositions.md). Exact absent spellings
remain visible in [mc-native-gap-checklist.md](mc-native-gap-checklist.md).
Those native choices do not remove any pinned AWS namespace requirement.

| Milestone | Implementation and scoped evidence | Acceptance boundary |
| --- | --- | --- |
| M0 | Locked complete mc tree; nine AWS high-level commands; 108 API operations, 670 shapes, seven paginators and four waiters; deterministic offline inventories, legacy snapshots, transport and parser/crypto decisions. | Inventory and source/model scope are fixed. Final source/artifact identity and current-source acceptance belong to M9. |
| M1 | Shared Registry/Parser/Runner, immutable targets/contexts, atomic private config/aliases, legacy output adapters, offline help and generated Bash/Zsh/Fish completion. Command-specific option rescanning preserves distinct `versions` meanings. | Golden and scoped parser/config tests do not imply every target/config combination. Repeat installed-artifact checks for the release bytes. See [completion-decisions.md](completion-decisions.md) and the requirement overlay. |
| M2 | Typed raw REST-XML client, encoding/signing, structured/embedded errors, credentials, pagination, TLS and fresh regional signing; published HTTP 0.21.13 socket/stream/ACK controls and owned cancellation. | Real loopback CA/trust/mTLS, finite/zero deadlines and slow-consumer tests are scoped client evidence. Current integrated/remote results, HTTP/2/HTTP/3 and live endpoint variants remain separate. See [transport-cancellation-decisions.md](transport-cancellation-decisions.md) and the HTTP report. |
| M3 | Bounded planner/reducer/coordinator, replayable sources, verified atomic sinks, multipart/copy, journals and exclusive resume, retries, separate endpoint contexts, fail-closed moves and signal cleanup. Explicit native put part sizing/checksum/parallel controls have scoped wire tests. | Prior measured 2 GiB and actual filesystem/signal acceptance is retained in [resource-filesystem-acceptance.md](resource-filesystem-acceptance.md). Current package/resource runs and representative live-provider interruption/resume cases remain separate; controlled-parent-directory and platform limits still apply. |
| M4 | All nine AWS high-level handlers and native raw/transfer conveniences; complete pinned argument grammar and modeled effects; separate sync/mirror/diff policies, bounded inventories and guarded deletion. The [high-level matrix](differential-highlevel.md) has 42 actual AWS cases/67 exchanges across all nine commands, including SSE-C, sync directions and restored archives. | Representative executable cases do not establish every flag/direction/error/progress variant. Preserve explicit warning/MIME/safety differences and AWS stdin sizing-hint versus native pipe exact-size semantics. Full pinned AWS evidence remains an M9 gate. |
| M5 | Complete modeled catalog/help/validation, explicit SDK customizations, JSON/YAML/shorthand/file/binary inputs, skeletons, JMESPath/presenters, CLI pagination, all waiters and raw outfile handlers. Actual operation fixtures cover all 108 operations; optional-member captures cover 57 cases/32 operations, plus output/pagination and [SSE-C/metadata/location customizations](differential-sdk-customizations.md). | Generic binding and bounded captures are not exhaustive member/output/error/query verification. Prompt and Linux pager have documented interface/platform differences. See [differential-operation-matrix.md](differential-operation-matrix.md), [differential-optional-members.md](differential-optional-members.md) and [differential-output-pagination.md](differential-output-pagination.md). |
| M6 | Complete catalog-backed object/bucket management families, complete native documents and selected convenience edits, version/delete-marker recovery, capability records and known-ESS safety exclusions. Governance, lifecycle, notifications, replication, anonymous and tag conveniences retain scoped effects and mutation fences. | Ten AWS governance captures and fourteen locked mc observations are bounded fixtures, not live enforcement. Independent complex configuration/provider/account round trips remain separately labeled. See [management-convenience-decisions.md](management-convenience-decisions.md), [native-governance-decisions.md](native-governance-decisions.md) and [differential-governance-members.md](differential-governance-members.md). |
| M7 | Credential providers/refresh/SSO, vendored S3/STS endpoint rules and partitions, directory sessions, SigV4a/streaming signatures, EventStream Select/Object Lambda, all five checksum algorithms and restore restrictions have scoped implementations. Darwin credential-process execution uses verified OTP Unix group setup; installed-artifact checks pass on all five declared runtime combinations. | Both macOS 26 architectures pass all ten installed process-group/descendant-cleanup tests, zero failures/skips/exclusions. Live STS/SSO/IMDS/desktop, directory/session/encryption and account/hardware-specific AWS cases remain unrun. Browser fallback, credential endpoint policies and other explicit differences remain; see [credential-decisions.md](credential-decisions.md), [sigv4a-decisions.md](sigv4a-decisions.md) and [special-protocol-decisions.md](special-protocol-decisions.md). |
| M8 | Bounded current/historical inspection/find/tree/du, exact `stat --no-list`, raw reads, rewind/age selection, diff, sharing/POST/private history, governance-bypass deletion, actual Select, explicit polling watch/mirror-watch, generic diagnostics versus ESS health, doctor/completion and transfer recovery. Invocation-scoped globals and bounded endpoint pacing retain their scoped tests. | All 106 native rows have explicit mappings/differences; this satisfies the implementation/disposition task without claiming mc drop-in parity. Prefix-selected encryption, omitted deletion/find conveniences, native output/defaults and MinIO extensions remain explicitly distinct. Selected-version recovery and named polling do not implement history-range undo or notification replay. See [native-compatibility-dispositions.md](native-compatibility-dispositions.md) and the linked per-feature reports. |
| M9 | Current standalone/umbrella, five-platform fresh package/install, measured 2 GiB resource, provider policy, generated inventories and reports pass; downloaded bytes match the committed source. | CLI-only publication and independent public-package/download verification remain open. Live AWS and exhaustive per-member evidence remain explicitly bounded above. |

## Actionable open acceptance and release gates

- [x] Complete committed-source umbrella acceptance; the standalone frozen-source CLI run passes 3,483 tests with zero failures and one resource exclusion. Earlier failures remain in integration-validation.md; isolated retries are not substituted for this complete run.
- [x] Complete current-source formatting, strict compilation, inventory/evidence tooling, pinned executable differential suites and the separate resource/provider-policy jobs. Scoped passes do not count as a passing final graph, and generic model traversal does not verify every member or variant.
- [x] Run fresh standalone package consumers and all five declared runtime combinations. Each macOS manifest must record `installed-credential-process`, `credential_process.os = darwin`, all seven named checks and at least ten tests with zero failures/skips/exclusions for both `macos-26` arm64 and `macos-26-intel` x86_64. Historical package jobs do not validate the current guard or modules.
- [x] Reconcile the final requirement overlay, native dispositions, command/migration/configuration guide, compatibility report and release notes with the actual final implementation and reference results. Keep absent native convenience effects visible, every AWS requirement assigned, and fixture/source-only/live-provider evidence distinct.
- [ ] Build and inspect the immutable next-version source package/escript; record exact version/source SHA, digests, reference lock and notices; commit/push/release through the configured CLI process, then independently verify the remote ref, workflow, public Hex consumer and downloadable artifacts. Version 0.6.8 is prepared; no tag or publication is counted by this report.

## Retained version-scoped evidence

- At `6816ee14c87ccc8de76a37a41811ec46a0c7aa9e`, CLI run 38740996062 passed 3,222 tests and umbrella run 38740995957 passed 3,223 CLI tests. Historical failed snapshots and later dependency-specific results remain in integration-validation.md.
- At `582639746e65376ea7d1f5aaeb203bca34b5d79d`, [CLI run 38871534043](https://github.com/gsmlg-opt/ex_storage_service/actions/runs/38871534043) passed all eight jobs, including 3,270 tests/zero failures/one resource exclusion. [Umbrella run 38871533923](https://github.com/gsmlg-opt/ex_storage_service/actions/runs/38871533923) reproduced the former HTTP #132 source-finalization failure. Passing standalone acceptance did not erase that failure; HTTP 0.21.13 now has separate scoped finalization evidence.
- The five verified prior-graph package combinations were Linux x86_64 on Elixir 1.18.0/OTP 27, 1.18.5/OTP 28 and 1.19.6/OTP 28, plus macOS 26 arm64/x86_64 on Elixir 1.19.6/OTP 28. [packaging-platform-decisions.md](packaging-platform-decisions.md) retains downloaded manifest/digest/native-asset proof. Interactive paging and measured resource acceptance remain Linux-only; those package probes were not full platform unit suites.
- [provider-acceptance.md](provider-acceptance.md) retains independent ESS 0.6.7/RustFS 1.0.1 controls and owned cleanup: ten ESS passing contracts/three server failures, thirteen RustFS passing contracts/one server failure. Exact pinned images and failure signatures remain provider-policy limits, not passing provider contracts or missing client implementations. Live AWS account/hardware evidence remains unrun.

## Reproducing this inventory audit

Run from the repository root:

```sh
PAGER=cat python3 scripts/cli_compatibility/generate.py
PAGER=cat python3 scripts/cli_compatibility/generate.py --check
PAGER=cat python3 -m unittest discover -s scripts/cli_compatibility -p 'test_*.py'
```

The native spelling checklist compares Registry command/global definitions with locked mc source definitions. This reconciliation reads source and scoped reports; it runs no BEAM or provider process. Matching spellings remain insufficient to establish matching behavior, and an explicit disposition does not change independent wire/provider evidence.
