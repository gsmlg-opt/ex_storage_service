# ESS CLI Implementation Plan

- Status: implementation in progress; acceptance and release remain open. See [milestone-acceptance-status.md](compatibility/milestone-acceptance-status.md) for scoped evidence and blockers.
- Date: 2026-10-14.
- Target: `apps/ex_storage_service_cli` and its standalone `ess` executable.
- Source baseline inspected: `42a9fe2b41faa82cf10c3f07840b890be18b876d`; CLI version at inspection: `0.6.7`.
- Architecture and compatibility contract: [design.md](design.md).

## 1. Goal and execution boundary

Deliver a standalone Elixir S3 client with mc-style native commands, complete pinned AWS CLI `s3` functionality, and complete pinned `s3api` functionality. Preserve valid legacy ESS CLI invocations while replacing the existing handcrafted command/transport paths with shared, tested foundations.

The original design-only change consisted of `docs/cli/design.md` and this file. The user subsequently authorized implementing M0–M9, committing/pushing, and releasing the next version. The original source baseline above remains historical; the implementation audit baseline is `9b8661e58107eddb24cb8f15495cee4ac979219a` plus the implementation diff. Checklist items are not closed by a generic handler or parser registration; acceptance reports distinguish implementation, wire and provider results.

The complete target is all milestones M0-M9. An early useful release is not permission to rename partial CRUD coverage as full compatibility. Separate client implementation status, wire-contract evidence, and provider integration evidence throughout.

### 1.1 Scope for subsequent implementation

Primary scope:

- `apps/ex_storage_service_cli/lib/**`
- `apps/ex_storage_service_cli/test/**`
- `apps/ex_storage_service_cli/priv/**` for required versioned model data/fixtures, if not compiled into modules
- `apps/ex_storage_service_cli/mix.exs`, `README.md`, and package metadata
- `docs/cli/**`

Shared files may change only for a demonstrated CLI requirement: the root lockfile, CLI-relevant documentation/environment descriptions, CLI tests/workflows, and existing release orchestration. Read root `AGENTS.md` first. Do not modify core storage, cluster, S3 router, or Phoenix/UI behavior merely to make the client tests pass. Record genuine server deficiencies as separate work with reproductions.

No Ecto/database, no dependency on ESS core/Concord/Phoenix, no runtime `aws`/`mc`/Python wrapper, and no unrelated transport migration. Continue using `http_fetch`. Do not add a dependency without documenting its necessity and following repository approval rules.

## 2. Delivery sequence and parallel work

| Milestone | Deliverable | Depends on |
| --- | --- | --- |
| M0 | Immutable references, complete inventories, baseline tests, dependency spikes | None |
| M1 | Command registry, targets, context, secure config, legacy adapter | M0 |
| M2 | S3 protocol core, static/session credentials, endpoint/signing/streaming adapter | M1 |
| M3 | Bounded transfers, multipart, retry/cancellation, durable resume | M2 |
| M4 | AWS high-level command behavior and native sync/mirror policies | M3 |
| M5 | Model-backed `s3api`, CLI inputs/outputs/query, paginators/waiters | M2; model inventory from M0 |
| M6 | Full bucket/object management operation families | M5; shared protocol from M2 |
| M7 | Advanced AWS credentials, endpoints, streaming/special operations | M3 and M5 |
| M8 | Remaining portable mc commands and explicit provider extensions | M4 and M6; M7 for relevant AWS cases |
| M9 | Full compatibility evidence, packaging, migration docs, release closure | M0-M8 |

After M2, transfer work (M3-M4) and modeled API work (M5-M6) can proceed in parallel. Once their shared interfaces are stable, credentials/special-endpoint work and provider fixtures can also run independently. Keep one owner for shared request/response types, registry, transport, and coverage schema; parallel agents must not independently redesign those contracts.

Work in the current checkout/branch unless the user explicitly requests branches or worktrees. Requested worktrees belong in `.trees/<branch>`. Commit coherent increments, integrate through normal branch protections, and never force-push over unrelated work. A milestone may have several implementation PRs but has one explicit acceptance report.

## 3. Milestones

## M0 — Freeze references and establish evidence

### Tasks

- [x] Read current CLI sources/tests, package metadata, root instructions, and publishing workflows; update the inspected source SHA rather than assuming this document's baseline is still HEAD.
- [x] Capture the behavior of all valid existing commands and their JSON output, option positions, configuration/profile selection, and return codes. Record current failures separately from behavior intentionally preserved.
- [x] Pin mc to `77f82e18b5401a65958f1619df6ebb994634bd88`, or explicitly review a replacement; capture nested command help, flags, defaults, and binary/source digest.
- [x] Select one real AWS CLI v2 release; capture its executable digest, source revision, bundled S3 service/SDK-extra models, customizations, endpoint/partition data, paginators, and waiters. Do not select a version solely from inconsistent rolling documentation headers.
- [x] Create the `docs/cli/compatibility/` lock and inventory files specified in the design. Enumerate every high-level command/flag and every `s3api` operation/member; no manually selected CRUD subset.
- [x] Classify all mc commands as portable S3, client-side utility, provider-specific extension, or explicitly outside scope; record every planned difference, including the existing `ess version` meaning.
- [x] Create an acceptance matrix with requirement ID, reference, milestone owner, code location, test IDs, client status, wire evidence, and provider evidence.
- [x] Spike the actual locked `http_fetch` APIs for streamed file/range uploads, streamed downloads, cancellation, Content-Length, raw Content-Encoding bytes, deadlines, trailers, and connection cleanup. Document gaps with failing wire tests; do not invent library options.
- [x] Evaluate the smallest adequate approach for JMESPath, YAML, bounded REST-XML/EventStream decoding, SigV4a, and all checksum algorithms in the reference. Record dependency/license/runtime decisions; do not hide unsupported algorithms behind ignored flags.
- [x] Record exact provider transfer limits in bytes and supported bucket/addressing/auth variants. Do not reuse a historical object-size ceiling.
- [x] Set up subprocess CLI tests, a controllable local wire server, temporary config/state roots, and credential-redaction assertions.

### Acceptance

A deterministic inventory regeneration produces no unexplained diff. Every reference command/operation/member has an assigned status and milestone. Existing CLI tests have actual recorded results; unavailable test environments are named, not counted as passing. The transport spike proves the bounded-stream prerequisites or opens specific dependency blockers before transfers are implemented.

### Deliverables

Reference lock, initial generated coverage report, test harness, compatibility snapshots, and a short dependency/transport decision record. This phase may publish documentation/test changes but must not claim new command compatibility.

## M1 — CLI foundation and backward-compatible target resolution

### Tasks

- [x] Introduce `CLI.Registry`, `CLI.Parser`, `CLI.Runner`, immutable `Target`/`Context`, `Error`, and `ExitStatus` under `ExStorageServiceCli`.
- [x] Implement the native, `s3`, and `s3api` frontend selection without duplicating execution logic. Reject unknown/invalid/conflicting options; support nested help, global option positions, repeated ordered filters, and `--`.
- [x] Keep `ess version` and `ess --version` for CLI version; reserve explicit `version enable|suspend|info` subcommands for bucket versioning.
- [x] Preserve native `--endpoint`, `--expires`, current valid targets, and existing JSON contracts through adapters. Add AWS spellings only in the intended compatibility surface.
- [x] Parse explicit local paths, alias paths, `ess://`, `s3://`, ARN targets, stdin/stdout, trailing slashes, and platform paths into typed targets. Do not normalize S3 keys as filesystem paths.
- [x] Implement `alias set/list/remove/import/export` and legacy `configure` on the common config layer. Aliases hold provider/profile references where possible.
- [x] Implement atomic config/state writes, locking, private permissions, escaping, corruption reporting, explicit migration backups, secret-redacted output, and preservation of unrelated configuration.
- [x] Define namespace-specific resolution rules: AWS config/provider behavior is not implicitly overridden by ESS defaults; native cross-alias targets retain independent contexts.
- [x] Centralize stdout/stderr ownership and final exit handling. Command handlers return typed values; only the top-level executable may halt the VM.
- [x] Add help/completion generation from the registry; help must not start networking or resolve credentials.

### Acceptance tests

Golden tests cover every existing valid command form. Unknown flags fail nonzero rather than disappear. Alias/local collisions require explicit targets. Round-trip configuration preserves quoted/backslash/newline values and unrelated profiles; a malformed file is not overwritten. A fake credential process and metadata endpoint observe zero calls during help. The built escript starts without any ESS server applications.

## M2 — Correct S3 protocol and transport boundary

### Tasks

- [ ] Establish typed request/response structures, an initial operation catalog, REST-XML serializers, structured errors, and one `Transport.HTTPFetch` adapter.
- [ ] Replace default object-body `read_all` behavior with owned streams; keep bounded materialization only for control responses and explicit small compatibility paths.
- [ ] Preserve manual redirects and `decode_body: false` on object traffic; propagate known Content-Length without recompression or payload rewriting.
- [ ] Implement independently tested S3 key/query/header encoding, canonical request construction, duplicate headers/query values where modeled, binary payload hashes, ports/IPv6, and correct copy-source encoding.
- [ ] Add complete static and temporary credential records; include session tokens in signed requests and presigned requests; never mix credential fields from different providers.
- [ ] Implement basic AWS/custom endpoint resolution and path/virtual-hosted addressing. Do not silently choose path-style for endpoints/bucket types that forbid it.
- [ ] Implement standard credential-missing errors and explicit unsigned mode; remove implicit anonymous fallback from the new execution path.
- [ ] Handle regional bucket creation requirements, response-header types, empty responses, HEAD failures, request IDs, XML namespaces, and bounded parser failure behavior.
- [ ] Detect embedded errors in successful HTTP responses for operations that require it. CopyObject must not succeed merely because the status is 200.
- [ ] Implement shared pagination primitives with missing/repeated-token detection; retain multi-token pagination support for later version/multipart operations.
- [ ] Add TLS verification/custom CA/native mTLS configuration and explicit timeout/cancellation ownership. Region correction constructs and signs a fresh validated request, never forwards old authentication to arbitrary redirects.

### Acceptance tests

Independent known signature vectors and a reference verifier validate signing; calling the same signer to verify itself is insufficient. Wire cases cover literal `%`, `+`, `?`, `#`, spaces, Unicode, repeated slashes, dot segments, tokens, metadata headers, and session tokens. Existing gzip/deflate/binary/HEAD tests continue to pass. Cross-origin redirects receive no credentials. Malformed XML, external entities, oversized control responses, and HTTP-200 S3 errors produce typed nonzero failures.

## M3 — Transfer engine, multipart, cancellation, resume

### Tasks

- [ ] Implement pure `Transfer.Planner`/`Transfer.Reducer`, an invocation supervisor, bounded coordinators/tasks, and count-plus-byte-budget scheduling.
- [ ] Implement streamed local upload/download, raw stdout/stdin, conditional/ranged downloads, temporary-file verification, and atomic destination publication.
- [ ] Add incremental payload/checksum calculation and replayable source descriptors. Detect source changes between hash, upload, retry, and resume.
- [ ] Implement multipart create/upload/list/complete/abort and multipart server-side copy. Derive part sizing from exact endpoint limits and source size; account for every modeled checksum algorithm/trait, with missing special implementations assigned to M7 rather than accepted silently.
- [ ] Implement bounded/spooled unknown-size input with expected-size support and explicit restart-resume limitations.
- [ ] Implement versioned local journals, exclusive resume ownership, atomic checkpoints, ListParts reconciliation, and explicit transfer list/resume/abort commands.
- [ ] Implement retry classification, jitter/backoff, per-attempt/overall deadlines, credential refresh hooks, and non-replayable/indeterminate outcomes.
- [ ] Implement same-service copy versus cross-service streaming bridge selection with separate source/destination contexts.
- [ ] Implement move as copy/verify/delete-selected-source. Reject same-resource moves and preserve a source whose identity changed or cannot be safely deleted.
- [ ] Implement SIGINT/SIGTERM cleanup, default multipart abort, explicit keep-incomplete policy, and inspectable failed-abort/uncertain-completion state.
- [ ] Secure local destination mapping against traversal, symlinks, case/normalization conflicts, and file-versus-directory conflicts. Test actual filesystem behavior rather than only string normalization.

### Acceptance tests

Upload and download objects much larger than the configured memory budget while sampling BEAM memory, process mailboxes, temporary disk, and active request count. Check actual streaming through a throttled peer, not only a lazy-looking enumerable. A test-generated multi-GiB object must not become a whole binary; a small object and a large object should stay within the same configured memory envelope apart from measured fixed overhead.

Exercise every multipart boundary using local fixtures, plus representative real-provider uploads. Interrupt every state; verify sockets/files/tasks are released and only owned uploads are aborted. Resume after restart; reject changed files, keys, endpoint contexts, and part options. An ignored Range response must not append a full object to a partial download. A partial download must leave an existing destination untouched. Copy errors, checksum mismatch, and copy-success/delete-failure must never be reported as a successful move.

## M4 — High-level AWS workflows and safe synchronization

### Tasks

- [ ] Implement all nine pinned `ess s3` commands (including `website`, identified during M0) through shared operations/transfers, plus native `get`, `put`, `cat`, `head`, and `pipe` conveniences.
- [ ] Add the complete pinned high-level flag inventory: recursive/filter/dry-run, MIME/metadata/content headers, ACL/grants, SSE variants, storage class, checksums, requester pays, Glacier controls, source region, copy properties, progress, overwrite/case-conflict behavior, and other captured flags.
- [ ] Map parameters to actual operation fields/effects; flags requiring M6/M7 capabilities remain explicitly tracked as incomplete until those dependencies pass, never silently accepted.
- [ ] Implement distinct AWS-sync and mc-mirror policy modules. Preserve direction-specific timestamp/size decisions, ordered include/exclude rules, trailing slashes, metadata/copy-property rules, and symlink behavior.
- [ ] Build lazy inventories and bounded external manifests/sorts when provider ordering is not guaranteed. Handle unreadable local subtrees as errors, not empty directories.
- [ ] Implement copy/update/skip/conflict/delete plans, dry-run output, same-target/overlap checks, and deterministic summaries.
- [ ] Add the guarded delete phase: complete successful inventory, successful required transfers, protected exclusions, candidate revalidation, and fail-closed behavior after errors.
- [ ] Implement recursive removal, batched deletion with per-key error parsing, selected-version and delete-marker behavior, and explicit all-version operations without overloading bucket-force semantics.
- [ ] Implement high-level presign behavior plus native share policy planning, respecting endpoint/signature/session expiration constraints.

### Acceptance tests

Run differential executable tests against the pinned AWS CLI and mc in separate isolated prefixes. Cover local-to-S3, S3-to-local, same-service S3-to-S3, and native cross-endpoint transfers. Include ordered filters, empty directories, symlink loops, Unicode/case conflicts, metadata-only changes, timestamp precision, large page counts, dry-run, and partial failures.

Inject a listing failure after earlier pages, a repeated token, an unreadable source directory, and a transfer failure: assert that the destination-delete phase performs zero deletions. Excluded destination entries remain intact. Concurrent source/destination changes are skipped or reported according to the declared safety policy, not erased. This milestone delivers high-level workflows but does not independently establish full AWS parity before M5-M7.

## M5 — Model-backed s3api and AWS CLI machinery

### Tasks

- [ ] Implement deterministic generation/loading of the pinned complete operation/shape catalog and corresponding notices. Include runtime-required artifacts in the published package.
- [ ] Generate validation, protocol bindings, command help, and completion metadata. Keep operation-specific customizations explicit rather than hidden inside generated code.
- [ ] Implement input structures, ordered repeated values, booleans, timestamps, exact numeric values, blobs, headers/query/body locations, and absent-versus-empty behavior.
- [ ] Implement AWS shorthand, `--cli-input-json`, `--cli-input-yaml`, file references, `fileb://`, binary-format selection, and skeleton generation/validation.
- [ ] Implement reference-compatible JSON/text/table/YAML/streaming/off and error output formats as applicable, plus JMESPath evaluation and output-specific pagination/query behavior.
- [ ] Implement CLI pagination tokens separately from service continuation tokens, aggregate/truncate rules, `--max-items`, `--page-size`, and `--no-paginate` semantics.
- [ ] Implement every pinned S3 waiter and its acceptors, attempts, delay, failure status, and cancellation.
- [ ] Implement custom command argument forms such as GetObject's output filename; preserve raw payload delivery separately from result metadata.
- [ ] Add shared AWS configuration flags, retry/timeouts, color/pager/prompt controls and command-specific option applicability. Do not assume every generic global flag changes every high-level command.
- [ ] Generate an explicit missing-handler report for special protocol/auth traits and advanced operation behavior assigned to M7.

### Acceptance tests

Every pinned operation is discoverable, parses its modeled parameters, validates required members, and has request/response fixture coverage appropriate to its implementation status. Each paginator and waiter has success, failure, exhaustion, cancellation, and continuation tests. Cross-check token and query behavior with the reference binary. Package tests work without downloading models. No stub or unconditional unsupported handler is counted as implemented; special-handler gaps remain release blockers.

## M6 — Complete object and bucket management families

### Tasks

- [ ] Complete all standard modeled operation families: object attributes/version access, ACLs, policies/status, public-access block, ownership controls, CORS, tagging, and requester-pays settings.
- [ ] Implement bucket versioning, ListObjectVersions/delete markers, explicit version deletion/recovery, and native version/undo behavior.
- [ ] Implement lifecycle and S3 replication documents, notification configuration, bucket/default encryption, Object Lock, legal hold, retention, and all associated modeled fields.
- [ ] Implement website, logging, acceleration, inventory, metrics, analytics, intelligent-tiering, and S3 metadata configuration operations.
- [ ] Cover currently modeled ABAC, annotations, object-encryption updates, deprecated operations, and any inventory entries not named in this prose; assign special endpoint/auth cases to M7 without dropping them.
- [ ] Add native convenience adapters (`tag`, `anonymous`, `policy`, `acl`, `cors`, `ilm`, `replicate`, `event`, `encrypt`, `retention`, `legalhold`) over the same S3 operations.
- [ ] Preserve full caller-provided documents and unknown response fields where safe; prevent destructive read/modify/write that drops fields a convenience presenter does not understand.
- [ ] Add provider/version/bucket-type capability records, read-only diagnostics, and known-ESS safety exclusions for unsupported subresources.
- [ ] Preserve actual AccessDenied, NotFound, unsupported, and invalid-response distinctions; do not infer universal capabilities from one principal's permissions.

### Acceptance tests

Round-trip representative complex configuration documents through an independent compatible endpoint. Exercise rejected ACLs, lock restrictions, explicit governance bypass, delete markers, and partial multi-delete failures. Verify unsupported ESS configuration requests are not sent into known unsafe fallback routes. Cloud-only operations have independent wire-contract fixtures plus separately labeled live evidence. The operation matrix has no unassigned family or hidden generic NotImplemented implementation.

## M7 — Advanced AWS identity, endpoint, and streaming support

### Tasks

- [ ] Complete AWS provider resolution including explicit-profile behavior, environment/shared config, credential process, STS assume-role/MFA/web identity, container credentials, IMDSv2, and IAM Identity Center/SSO lifecycle; cover any additional providers in the pinned reference.
- [ ] Implement credential refresh before expiry with single-flight ownership, bounded waiters, session-token propagation, and safe credential-cache persistence where necessary.
- [ ] Finish endpoint/partition/auth rule evaluation for access points, ARN region rules, FIPS/dualstack/accelerate, Outposts, Object Lambda, MRAP/SigV4a, and directory buckets.
- [ ] Implement operation-specific directory-bucket CreateSession usage, session refresh, list behavior, RenameObject and conditional operations; keep operations such as CopyObject on their correct authentication path.
- [ ] Implement SelectObjectContent EventStream framing, checksums, event/error handling, terminal events, and streaming output; implement WriteGetObjectResponse's distinct endpoint/stream semantics.
- [ ] Finish all modeled checksum algorithms, whole-object versus composite verification, trailer handling, and streaming-signature cases identified in M0/M5.
- [ ] Verify restore/Glacier and account/region/bucket-type restrictions without claiming that unsupported providers gain those server features.
- [ ] Finish remaining high-level flags and modeled member/auth variants whose acceptance depended on advanced capabilities.

### Acceptance tests

A transfer that outlives temporary credentials continues using refreshed credentials without a refresh stampede or leaked secrets. Run negative tests for malformed credential-process output, role cycles, expired sessions, disallowed metadata redirects, wrong-region ARNs, and wrong auth for directory-bucket operations.

Use independent cryptographic vectors for SigV4a and wire fixtures for every special endpoint/auth variant. EventStream fragmentation/truncation/checksum errors must fail without leaking resources or reporting a completed stream. Run AWS integration tests for available general-purpose, session, encryption, and directory-bucket cases. Hardware/account-dependent cases such as Outposts must report exactly what was or was not exercised; fixture coverage is not live infrastructure evidence.

## M8 — Remaining mc ergonomics and provider boundaries

### Tasks

- [ ] Complete `ls`, `tree`, `du`, `find`, `stat`, `diff`, `share`, and remaining portable mc flag/subcommand mappings against the locked inventory.
- [ ] Implement bounded tree/find/summary output, version-aware inspection, raw head/cat behavior, and explicit result limiting without pretending a partial result is exhaustive.
- [ ] Implement presigned upload/download workflows and POST policy generation where included by the native command contract.
- [ ] Implement `watch` and mirror-watch adapters only with explicit provider/source semantics: native notifications, configured event-source integration, or clearly labeled polling. Notification configuration alone is not an event consumer.
- [ ] Reconcile after notification reconnect/overflow and document duplicate/out-of-order events and missed-change limitations. No exactly-once or permanent replay guarantee without a supporting source protocol.
- [ ] Keep `ping`, `ready`, `info`, and `admin` separated into generic connection diagnostics and genuine ESS/provider extension APIs. Preserve legacy ESS info behavior.
- [ ] Make MinIO-only inventory entries visibly out of the portable contract rather than ignored command names; do not fabricate ESS counterparts for MinIO licenses, IDPs, quotas, or batch-job APIs.
- [ ] Complete native doctor/capability explanations, completion scripts, transfer-journal inspection, and migration guidance.

### Acceptance tests

Every mc inventory row has a working tested mapping or an explicit scope/difference record. Portable commands work against more than the ESS server. Unsupported extensions fail predictably without mutating unrelated S3 resources. Watch reconnects and polling modes report their true source and guarantee. Legacy command snapshots still pass or have an explicitly approved migration record.

## M9 — Compatibility closure and release

### Tasks

- [ ] Close every implementation gap in the pinned `s3`/`s3api` inventories, including parameters, enums, customizations, endpoints, credential modes, paginators, waiters, outputs, and error semantics.
- [ ] Run the complete CLI test matrix and differential reference suites; publish generated coverage with exact command/model/binary versions and provider versions.
- [ ] Validate fresh standalone Hex installation, escript building/running, required generated assets/notices, and absence of server-umbrella runtime dependencies.
- [ ] Correct CLI package metadata links to the actual repository where necessary; verify LICENSE, README, version reporting, executable naming, and artifact contents.
- [ ] Test supported Linux/macOS architectures and declared OTP/Elixir minimums. State the Erlang runtime requirement for escripts; add alternative self-contained packaging only with its own tested support contract.
- [ ] Add real memory/backpressure/cancellation benchmarks and documented resource defaults. Report measured results, not extrapolated throughput promises.
- [ ] Publish command reference, legacy migration guide, AWS configuration guide, mc difference table, provider limitations, safety/restore procedures, and examples using non-secret credentials references.
- [ ] Prepare changelog, checksums, model/reference lock, compatibility report, and immutable source/artifact mapping.
- [ ] Commit, push, and release through the existing configured CLI publishing process after gates pass. Inspect root orchestration before triggering it so a CLI-only release does not accidentally publish unrelated server packages.
- [ ] Verify the remote ref, workflow result, published package, and downloadable artifact. Report blockers truthfully; never call an attempted or queued publish a completed release.

### Final acceptance

All in-scope client features are implemented; no stubs or silently ignored flags remain. Wire and executable behavior is verified against the pinned reference, subject only to explicitly recorded compatibility deviations. Provider-ineligible cases retain correct client implementation and clear evidence labels. A full-compatibility claim must always state the exact baseline and any deviations; unavailable live evidence cannot be represented as a passing provider suite.

## 4. Cross-cutting acceptance matrix

These cases are mandatory across milestones, not optional polish at the end.

| Requirement | Minimum evidence |
| --- | --- |
| Standalone runtime | Fresh consumer installs/runs CLI without server applications or repository-relative files |
| No silent flag loss | All inventory flags have parser, effect, and result tests; unknown flags fail |
| Key fidelity | Spaces, `%`, `+`, `?`, `#`, Unicode, repeated slashes, trailing slashes, dot segments, and encoded version/copy sources round-trip |
| Independent signing | External/reference vectors; tokens and signed fields verified without relying on the same implementation as both oracle and subject |
| Raw bytes | Stored gzip/deflate/binary data unchanged through upload, download, copy, and bridge |
| Bounded memory | Large generated object and large synthetic inventory stay within configured buffers plus measured overhead; request counts and mailboxes remain bounded |
| Pagination | More than one page for objects, versions, buckets where applicable, uploads, and parts; repeated/missing/multi-token continuation failures |
| Safe publication | Truncation, checksum mismatch, disk-full, cancellation, unsafe path, and Range mismatch leave original local destination intact |
| Multipart recovery | Crash before/after part acknowledgment, completion ambiguity, abort failure, duplicate resumer, and source mismatch |
| Move safety | No source deletion on copy failure or changed source; copy-complete/delete-failed outcome reported accurately |
| Safe synchronization | No deletions after incomplete enumeration/failed transfers; exclusions protected; explicit overlap/concurrency policy |
| Service error semantics | HTTP-200 embedded errors and batch per-key failures produce nonzero/partial-failure results |
| Identity handling | Temporary credentials, refresh, explicit-profile selection, role/provider failures, and redaction |
| Filesystem mapping | Traversal, symlink, case/normalization, Windows name/drive constraints on supported platforms, and file/directory collisions |
| Process lifecycle | SIGINT/SIGTERM, broken stdout pipe, timeout, and cancelled queries release owned resources |
| Output contracts | Raw stdout never polluted; structured formats/query/return codes match the chosen frontend |
| Provider independence | ESS, generic S3-compatible provider, and independent AWS/reference wire evidence |
| Reproducible packaging | Pinned models present in package; escript/runtime requirements, version, digest, source SHA, and notices verified |

## 5. Test execution and resource policy

Run tests with explicit working directories rather than embedding directory changes in shell commands. Follow the root instruction to scope tests to modified applications. From the repository root, `mix test apps/ex_storage_service_cli/test` is the normal scoped suite; an isolated package/consumer harness separately proves the CLI does not require the umbrella at runtime. Run formatting/compilation checks only for the implementation paths and required dependency closure.

Keep separate CI jobs for:

1. Pure/unit/raw-wire and actual-executable tests, always available without external credentials.
2. Local ESS and generic compatible-provider interoperability; retain existing relevant RustFS/mc compatibility coverage rather than replacing it with self-tests.
3. Opt-in AWS tests with dedicated short-lived resources and credentials.
4. Packaging/platform and fault/resource-bound tests.

Cloud tests require an explicit test-account/bucket allowlist and resource prefix, a cost envelope, and cleanup manifests. Never use production aliases for destructive tests. Governance/legal-hold cases must have reversible fixtures where possible; compliance-lock tests require separately provisioned disposable resources and must not create indefinite retained objects incidentally. No watch notification or bucket policy should expose user data during testing.

A test report records the exact command, working directory, source SHA, environment/provider versions, passed/failed/skipped cases, cleanup outcome, and known limitations. Record server defects independently; do not weaken authentication or change expected checksums to obtain green tests.

## 6. Milestone completion and release rules

Each milestone is complete only after code, tests, documentation, compatibility ledger, and release notes agree. Do not check off tasks because files or command names exist.

For each implementation delivery:

- Run affected tests and the relevant acceptance cases, then inspect the actual diff for scope and secrets.
- Commit and push automatically through the repository's normal policy; do not force-push, discard unrelated changes, or bypass branch protections.
- Publish a release for a completed releasable implementation increment using the existing CLI workflow and the appropriate version. Smaller working milestones may be prereleases with an explicit partial-coverage report.
- Keep published artifacts traceable to the source commit/version; verify publishing rather than assuming workflow dispatch succeeded.
- When environment, credentials, tests, permissions, or dependency defects block release, report the concrete blocker and completed work. Do not fabricate a release, downgrade test expectations, or publish known-broken binaries merely to satisfy a workflow preference.

Documentation-only changes do not require an executable version bump or a binary/Hex release. Never label M1-M4 as full AWS S3 compatibility while M5-M7 remain incomplete.

## 7. Coding-agent execution contract

When subsequently asked to execute this plan, begin with M0 and preserve the design's compatibility boundaries. Use the actual current repository and dependency APIs, not assumptions from this planning snapshot. Parallelize independent work after shared interfaces have tests, and serialize changes to shared contracts and release metadata.

For each task, first add a reproducer or acceptance test, implement the smallest coherent change, update its compatibility row, run scoped checks, and commit/push. Proceed in dependency order until the requested implementation scope is complete. Do not stop at CRUD, add placeholders for advanced operations, or claim unsupported provider functionality as implemented client coverage.

The final implementation handoff must include source commits, completed/incomplete requirement IDs, exact test evidence, compatibility baseline, provider caveats, and verified release/package/artifact identifiers. Feature code, tests, package assets, and documentation must tell the same story.
