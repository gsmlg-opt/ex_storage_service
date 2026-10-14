# ESS CLI Design

- Status: proposed architecture; this document does not claim the features are implemented.
- Date: 2026-10-14.
- Package: `apps/ex_storage_service_cli`.
- Executable: `ess`; Elixir namespace: `ExStorageServiceCli`.
- Repository baseline inspected: `42a9fe2b41faa82cf10c3f07840b890be18b876d`.
- Delivery plan: [implement-plan.md](implement-plan.md).

## 1. Product decision

Build a standalone, Elixir-native S3 client with **MinIO mc-style ergonomics and complete AWS CLI S3 functionality**. Keep one implementation of credentials, signing, S3 operations, pagination, transfers, synchronization, and error handling underneath distinct command frontends.

The three command surfaces are:

| Surface | Contract | Purpose |
| --- | --- | --- |
| `ess <command>` | mc-inspired commands plus preserved ESS legacy invocations | Everyday storage management, aliases, inspection, cross-endpoint transfers, mirroring |
| `ess s3 <command>` | AWS CLI v2 high-level S3 compatibility | Migrate `aws s3` workflows by replacing the executable, without translating each command |
| `ess s3api <operation>` | AWS CLI v2 low-level S3 compatibility | Access every operation and modeled parameter in the pinned S3 reference, including advanced and provider-specific operations |

Do not wrap or shell out to `mc`, `aws`, Python, boto3, or a running ESS server to perform storage operations. Reference binaries may be used in development and interoperability tests, not as runtime dependencies. An explicitly configured credential process is a credential provider, not a storage-command implementation.

### 1.1 Definition of complete

The final target includes all eight high-level AWS S3 commands (`ls`, `cp`, `mv`, `rm`, `mb`, `rb`, `sync`, `presign`), their applicable options, and the complete pinned `s3api` command/operation/shape inventory. It also includes the supporting credential providers, endpoint rules, pagination, waiters, input syntax, output/query behavior, transfer semantics, and return codes required to use those commands.

Implementing a few CRUD commands, accepting arbitrary flags, or returning `not implemented` from registered commands is not parity. Generating operation names is not sufficient: request serialization, authentication, response decoding, failure behavior, and tests must also exist.

Measure these dimensions independently:

1. Command and parameter coverage.
2. Wire-protocol and behavioral coverage.
3. Provider integration evidence.

An operation can be implemented in the client but unsupported by an ESS or MinIO endpoint. Conversely, an ESS feature does not imply that the CLI exposes it. Skipped cloud tests are not passing tests; a service-retired or account-ineligible API must retain its correct request/response contract and have its unavailable integration evidence explicitly recorded.

### 1.2 Boundaries

The AWS compatibility target is the `s3` and `s3api` namespaces, not the entire AWS CLI. Separate services such as `s3control`, `s3tables`, `s3vectors`, IAM administration, and general KMS administration are extension tracks, not silently included in this promise. S3 data-plane access through access points, Outposts, Object Lambda, or directory buckets remains in scope whenever it is part of the pinned S3 namespace. S3 metadata-table configuration operations exposed through `s3api` also remain in scope even though a separately managed service may back them.

MinIO server administration, identity-provider management, licensing, quota management, and server-side batch-job APIs are not portable S3 APIs. Record them explicitly in the mc inventory; do not advertise universal `mc` drop-in compatibility. ESS-specific administration belongs behind `ess admin` and a provider capability boundary. A polling implementation must never pretend to provide a server-native event stream.

This work does not expand the ESS server API, introduce a database, create a new umbrella application, or change the web/admin UI. Missing server operations become separately tracked server work.

## 2. Evidence and current implementation

This is a source inspection, not a statement that the current test suite was executed. At the inspected revision the package version is `0.6.7`.

| Existing area | Observed implementation | Design consequence |
| --- | --- | --- |
| Entrypoint | A flat map dispatches `configure`, `mb`, `rb`, `ls`, `tree`, `cp`, `rm`, `mv`, `presign`, `info`, and `version`; several parsers discard invalid options | Introduce a typed command registry, nested commands, strict parsing, and centralized exit handling |
| Transport | `S3Client` uses `http_fetch`, path-style addressing, manual redirects, and raw object response bytes; it then calls `HTTP.Response.read_all` | Preserve raw-byte and redirect protections, but expose bounded response streams |
| Copy | Uploads call `File.read!`; recursive listing accumulates pages with `acc ++ contents`; recursive downloads directly join object-derived paths to local paths | Replace whole-object buffering and accumulated inventories; explicitly secure local destination mapping |
| Credentials | TOML profiles contain endpoint, access key, secret, and region; the inspected signer/context have no session-token path | Add coherent credential records, refreshable providers, AWS profiles, and session-token signing |
| Configuration | Parse/read failures can become an empty profile; writes rebuild TOML and write directly | Fail explicitly on corrupt configuration; add atomic, permission-restricted, lossless updates |
| S3 operations | A small handcrafted CRUD/list/copy/HEAD client; `CopyObject` accepts status 200 without validating its result XML | Add model-backed operations and operation-specific success/error validation |
| Tests | Existing tests cover binary uploads, encoded keys, gzip/deflate preservation, chunked responses, HEAD behavior, errors, and redirects | Retain these regressions and add independent signing, actual executable, fault, and provider tests |
| Packaging | CLI has no core application dependency; dependencies include `http_fetch`, TOML, and JSON tooling; an escript/Hex publishing workflow exists | Keep independently installable and test it outside the umbrella |

Source paths:

- [CLI entrypoint](../../apps/ex_storage_service_cli/lib/ex_storage_service_cli.ex)
- [S3 client](../../apps/ex_storage_service_cli/lib/ex_storage_service_cli/s3_client.ex)
- [Copy command](../../apps/ex_storage_service_cli/lib/ex_storage_service_cli/commands/cp.ex)
- [Configuration](../../apps/ex_storage_service_cli/lib/ex_storage_service_cli/config.ex)
- [Signer](../../apps/ex_storage_service_cli/lib/ex_storage_service_cli/sigv4.ex)
- [Transport regression tests](../../apps/ex_storage_service_cli/test/ex_storage_service_cli/s3_client_test.exs)
- [Package definition](../../apps/ex_storage_service_cli/mix.exs)
- [CLI publishing workflow](../../.github/workflows/publish-cli.yml)

The inspected [ESS S3 router](../../apps/ex_storage_service_s3/lib/ex_storage_service_s3/router.ex) exposes core object/bucket operations, multipart paths, versioning, lifecycle, notification configuration, and ESS health endpoints. This is not evidence of full AWS parity. Some bucket PUT subresources fall through to bucket creation. For a known ESS version, unsupported configuration mutations must be blocked by an accurate capability profile rather than optimistically dispatched through that fallback.

## 3. Reference management and compatibility ledger

The mc source reference for this design is `minio/mc` commit `77f82e18b5401a65958f1619df6ebb994634bd88`. Its command-line behavior is a reference, not code to copy into the MIT CLI package. Preserve applicable notices for any intentionally imported model data; do not change the package license incidentally.

AWS documentation consulted on 2026-10-14 displayed different version headers on different pages. Treat those pages as discovery material, not as a coherent immutable release. Phase M0 must select one actual AWS CLI v2 release and record the matching source commit, bundled service models, customizations, and executable digest. Never combine the newest documentation page with an unrelated botocore model and call that a pinned baseline.

The implementation will maintain these artifacts under `docs/cli/compatibility/`:

- `reference-lock.json`: ESS source baseline, mc revision/binary digest, AWS CLI release/revision/binary digest, corresponding model revisions/digests, capture date, platform, and model-data notices.
- `mc-commands.json`: command/subcommand/flag inventory, defaults, target grammar, provider requirements, mappings, intentional differences, and test IDs.
- `aws-s3-commands.json`: every high-level command, applicable flag, default, input/output rule, and test ID.
- `aws-s3api-operations.json`: every operation, input/output member, trait, paginator, waiter, endpoint/auth variant, customization, and test ID.
- `coverage.md`: generated counts and independently reported implementation, wire-test, and provider-test results.

Each item has one status: `planned`, `implemented-unverified`, `verified`, `provider-unavailable`, or `out-of-scope-separate-service`. The last status cannot be used to remove an operation actually present in the pinned `s3`/`s3api` namespaces. `provider-unavailable` does not excuse missing client implementation. Every item requires an owner milestone and evidence or a linked blocker.

Refresh references through an explicit reviewed change, with a generated delta of new commands, members, defaults, and enum values. Do not fetch models during ordinary CLI execution or builds.

## 4. Command and target contracts

### 4.1 Native command families

| Commands | Intended coverage | Boundary |
| --- | --- | --- |
| `alias set/list/remove/import/export`, `configure` | Named endpoints, profile references, safe migration, optional mc config import | Never silently import or export secrets |
| `ls`, `tree`, `du`, `find`, `stat` | Buckets, prefixes, object metadata, versions, filters, size summaries | Lazy inventory; operations that need sorting may spill to bounded temporary storage |
| `cp`, `mv`, `get`, `put`, `cat`, `head`, `pipe` | Local/object transfers, stdout/stdin, server-side and cross-endpoint copies | All use the shared transfer engine |
| `mirror`, `diff`, `sync` | Synchronization, comparison, deletion planning, optional watch mode | `mirror` follows mc policy; `sync` follows AWS policy; they are not simple aliases |
| `mb`, `rb`, `rm` | Bucket creation/deletion, object deletion, recursive and version-aware deletion | Bulk-delete results and version/delete-marker semantics are explicit |
| `share download/upload`, `presign` | Presigned GET/PUT and presigned POST policy where applicable | Honor signing and credential-expiry limits; `ess s3 presign` remains AWS GET semantics |
| `version` subcommands, `undo` | Bucket versioning, selected-version recovery, delete-marker recovery | No promise of undo after irreversible deletion |
| `tag`, `anonymous`, `policy`, `acl`, `cors` | Tagging, anonymous bucket-policy workflows, access and CORS configuration | Do not replace an existing policy without a deliberate update |
| `ilm`, `replicate`, `event`, `encrypt` | Lifecycle, S3 replication configuration, notification configuration, bucket encryption | Full modeled documents preserved; provider availability reported |
| `retention`, `legalhold` | Object Lock configuration, retention, legal hold, version-specific actions | Governance bypass requires an explicit option; compliance retention is never bypassed |
| `sql` | S3 Select request and EventStream processing | Server/account support varies; no local SQL substitution advertised as S3 Select |
| `watch` | Native notification stream or a separately named polling/event-source adapter | Report adapter, ordering, duplication, replay, and delivery guarantees |
| `ping`, `ready`, `info`, `admin` | Generic connectivity diagnostics versus ESS health/admin operations | Never send ESS health paths to AWS as an S3 capability probe |
| `od`, `quota`, `batch`, `idp`, `license`, `support`, `update` | Inventory all upstream commands | Benchmarking is optional; MinIO-specific management and binary self-update are outside portable S3 parity |
| `help`, `completion`, `doctor`, `transfer list/resume/abort` | Discoverability, diagnosis, and explicit local transfer-journal management | ESS additions, not claimed AWS commands |

M0 expands grouped entries to the pinned mc subcommand/flag inventory. Reuse portable S3 operations where possible; isolate genuinely provider-specific behavior. Global native options include endpoint/profile selection, region, TLS options, structured output, quiet/progress controls, debug redaction, transfer bounds, and dry-run where meaningful.

### 4.2 Preserve existing invocations

Keep valid existing `configure`, `mb`, `rb`, `ls`, `tree`, `cp`, `rm`, `mv`, `presign`, and `info` invocations working. Preserve existing structured-output shapes through a legacy presenter until an explicitly announced output-contract migration. Improving an invalid-option failure is permitted but must be documented.

Resolve the important naming collision deliberately:

- `ess version` with no subcommand continues to print the CLI version.
- `ess --version` also prints the CLI version.
- `ess version enable|suspend|info <target>` manages bucket versioning.
- `ess info` remains a compatibility entrypoint for ESS health; `stat` is object/bucket inspection.

Existing `--endpoint` is retained; the AWS spelling is `--endpoint-url`. Existing `--expires` remains available for native `presign`; the AWS adapter uses `--expires-in`. Do not change existing meanings merely because a similarly named mc command differs.

### 4.3 Target grammar

Normalize arguments into immutable targets before making a request:

| Form | Meaning |
| --- | --- |
| `./path`, `../path`, absolute path, `file://...` | Explicit local path |
| `<alias>/<bucket>/<key>` | Native mc-style target when the first segment is a configured alias |
| `ess://<alias>/<bucket>/<key>` | Explicit, unambiguous native alias target |
| `s3://<bucket>/<key>` | S3 target using the selected execution context |
| `s3://<access-point-arn>/<key>` | AWS access-point grammar, parsed with the ARN rules rather than ordinary slash splitting |
| `-` | Raw stdin/stdout only for commands that explicitly support it |

Bare legacy bucket/prefix arguments remain valid only in the legacy commands that already accept them. A native alias/local-path collision is an error; require an explicit form rather than guessing. Recognize `--` as the end of options. Preserve Windows drive/UNC paths as local paths when supported.

S3 keys are not filesystem paths. Preserve repeated slashes, trailing slashes, literal percent signs, plus signs, Unicode, and dot segments according to the wire protocol; encode once at the serialization boundary. Keep version IDs and request options separate from key bytes. Apply command-specific basename/trailing-slash rules, not a universal `Path.join` rule.

Each remote source and destination has its own endpoint, region, credentials, TLS settings, and capabilities. A cross-alias transfer must never reuse destination credentials on the source endpoint.

## 5. AWS functionality coverage

The following families organize implementation work; the pinned model remains the exhaustive inventory.

| Family | Required behavior |
| --- | --- |
| High-level workflows | All `s3` commands; all applicable recursive/filter/dry-run/progress/MIME/metadata/ACL/SSE/storage-class/checksum/requester-pays/Glacier/source-region/copy-property/no-overwrite/case-conflict flags in the selected release |
| Object data | GET, HEAD, PUT, COPY, DELETE, multi-delete, ranged and conditional requests, version selection, object attributes, metadata, annotations when modeled, restoration, and legacy modeled operations |
| Multipart | Create, upload part, upload-part-copy, list uploads, list parts, complete, abort; native resume/cleanup built on these primitives |
| Bucket basics | Create/delete/head/list buckets, ListObjects v1/v2, location, versioning, versions and delete markers, directory-bucket enumeration |
| Access and ownership | Bucket/object ACLs, bucket policy/status, public-access block, ownership controls, CORS, requester-pays configuration, modeled ABAC operations |
| Governance | Object Lock, retention, legal hold and all modeled fields, lifecycle, encryption, replication, notification configuration |
| Storage administration | Website, logging, acceleration, inventory, analytics, metrics, intelligent-tiering configuration, S3 metadata configuration/table operations |
| AWS-special data paths | Directory buckets and CreateSession, RenameObject, access-point/Outposts/Object Lambda endpoints, MRAP/SigV4a where required, WriteGetObjectResponse, SelectObjectContent EventStream |
| New and legacy modeled operations | Include operations such as UpdateObjectEncryption and object annotations when present; retain deprecated operations as modeled requests rather than silently dropping them |
| CLI machinery | Shorthand and structured inputs, binary/file inputs, skeletons, query/output formats, paginators, waiters, return codes, profile and endpoint behavior |

### 5.1 Model-backed `s3api`

Vendor a reviewed immutable snapshot of the selected release's S3 service definition, SDK extras/customizations, paginators, waiters, endpoint rules/tests, and relevant partition data. Generate Elixir data/modules at development time. Ordinary builds remain offline with respect to upstream schemas.

The generic engine handles operation lookup, typed input validation, REST-XML encoding/decoding, headers, query/subresource parameters, timestamps, enums, maps/lists, and pagination. Keep absent values distinct from explicit false, zero, empty values, and null where permitted. Do not create atoms from user input or untrusted XML/JSON names.

Explicit custom handlers cover streaming bodies, EventStream, checksum traits/trailers, special endpoint/auth rules, embedded-error responses, and AWS CLI customizations such as output-file arguments. A generic HTTP-method dispatcher cannot replace those handlers.

Required CLI inputs and outputs include `--cli-input-json`, `--cli-input-yaml`, file references, shorthand structures, `file://`, `fileb://`, `--cli-binary-format`, skeleton generation, `--query` with JMESPath semantics, and every output/error format applicable in the pinned release. Avoid lossy numeric conversion. Add small dependencies only after documenting the need and verifying compatibility; do not invent incomplete YAML/JMESPath substitutes and call them compatible.

Pagination distinguishes service continuation tokens from CLI `--starting-token` values. Implement the pinned CLI's `--page-size`, `--max-items`, `--no-paginate`, aggregation, token, and output/query interaction semantics. Never treat a repeated/missing continuation token on a truncated result as successful completion. Waiters use the pinned acceptors, delay, attempts, failure rules, and cancellation behavior.

### 5.2 Authentication and endpoint support

Use an immutable credential record containing access key, secret, optional session token, optional expiry, and provenance. Never assemble a key pair from different providers.

Native aliases reference either explicit credentials or a credential provider/profile. Existing ESS TOML profiles remain supported. For native non-secret settings the precedence is explicit command options, explicitly selected alias/profile settings, documented ESS environment overrides for otherwise unset settings, and defaults. Credentials follow the selected provider, not field-by-field fallback. A missing named profile or malformed config is an error, not permission to use localhost anonymously.

AWS namespaces use the pinned AWS resolution behavior, including the distinction between an explicitly supplied `--profile` and environment-selected profiles. They read AWS shared config/credentials and relevant environment settings without modifying those files automatically. An ESS alias provider can explicitly bridge to an AWS profile; do not merge the two stores implicitly or let an ESS default endpoint override normal AWS resolution.

Required provider work includes environment/static session credentials, shared profiles, `credential_process`, STS assume-role/MFA and web identity, container credentials, IMDSv2, and IAM Identity Center/SSO login/session refresh needed by supported profiles. Include additional credential mechanisms found in the selected AWS baseline in M0. Refresh is single-flight per credential context, with expiry skew and bounded waiters. Help/completion must not trigger credential processes or metadata-service calls. Explicit anonymous mode bypasses credential loading; missing credentials never silently become anonymous requests.

Credential processes use an explicitly configured executable/arguments, bounded output, timeout, exit-status checks, and the documented credential response format. Never evaluate arbitrary shell snippets discovered from object data. Restrict metadata endpoints and protect tokens from redirects/proxies. Temporary credentials, MFA values, SSE-C keys, Authorization headers, and presigned query strings must not enter logs, crash reports, or journals.

Endpoint resolution supports custom endpoints, path-style and virtual-hosted addressing, regions/partitions, access-point ARNs, FIPS/dualstack/accelerate where applicable, directory-bucket zonal endpoints, and operation-specific signing names/regions. Implement SigV4a only through independently tested cryptographic primitives and reference vectors. Directory-bucket authentication is operation-specific; do not apply CreateSession credentials blindly to CopyObject.

Signed requests always use manual HTTP redirects. A validated AWS region/endpoint correction may construct a fresh request and sign it again, subject to replayability and a bounded correction count; it must not forward the old Authorization header to an arbitrary Location URL. TLS verifies peers by default, supports custom CAs and native mTLS configuration, and never falls back to disabled verification. An explicit insecure option emits a warning without contaminating stdout data.

## 6. Internal architecture

Use a functional core with a small supervised effect layer:

`argv -> parser/registry -> resolved targets/context -> command intent -> plan -> effect interpreter -> structured result/events -> presenter -> exit status`

| Component | Responsibility |
| --- | --- |
| `CLI.Parser`, `CLI.Registry`, `CLI.Runner` | Pure parsing/validation, frontend selection, command metadata, top-level lifecycle |
| `Target`, `Context`, `Config`, `Credentials.*` | Immutable endpoint/target configuration; explicit I/O and credential providers |
| `S3.OperationCatalog`, `S3.Request`, `S3.Response` | Modeled operation definitions and typed protocol values |
| `S3.Encoder`, `S3.Decoder`, `S3.Endpoint`, `S3.Auth` | Serialization, bounded parsing, endpoint rules, signing |
| `Transport.HTTPFetch` | The only storage HTTP adapter; streaming, cancellation, deadlines, raw bytes |
| `Inventory`, `Paginator`, `Filter` | Lazy enumeration, pagination safeguards, ordered filters |
| `Transfer.Planner`, `Transfer.Reducer` | Pure transfer decisions and state transitions that emit effects |
| `Transfer.Coordinator`, `Transfer.Journal` | Supervised bounded work, resource ownership, durable local resume state |
| `Sync.Planner`, `Sync.Executor` | Policy-specific comparisons and guarded delete phase |
| `Presenter`, `Error`, `ExitStatus` | Data output, terminal output, typed failures, deterministic exit aggregation |
| `Provider.*` | Versioned capability profiles and genuine extension APIs |

The existing `S3Client`, `SigV4`, command modules, and output modules may initially delegate to the new components. Do not duplicate signing or transfer loops inside each frontend. Audit any documented library API before changing it; the main compatibility commitment is the command-line contract.

Start only the CLI's dependencies and a small invocation supervisor containing the HTTP resources, a `Task.Supervisor`, the credential refresh owner when needed, and transfer coordinators. Do not start Phoenix, Bandit, Concord, storage engines, or the server umbrella to run `ess`.

Use bounded tasks for requests/transfers, not a process per listed object or per chunk. Keep scheduling state in the coordinator and transition decisions in pure reducers. Only the executable boundary calls `System.halt`; workers return results and release resources. A cancelled invocation stops scheduling, cancels active requests, closes streams/files, records cleanup outcomes, and only then returns its exit status within a bounded shutdown deadline.

## 7. Streaming transfer engine

### 7.1 Resource invariants

Memory consumption must be a function of configured buffers, concurrency, and bounded control data, not object size or bucket cardinality. Use both request-count and byte-budget limits. Do not multiply object concurrency by part concurrency without a global cap.

Initial native tuning targets, to be measured rather than claimed as established performance: four active objects, eight active requests globally, four parts per object subject to that global cap, 256 KiB I/O chunks, and a 64 MiB shared in-flight byte budget. AWS mode preserves the selected AWS configuration defaults where user-visible, while retaining a documented resource-safety envelope. A part is a streamed range, not necessarily a part-sized binary in memory.

Support producer backpressure all the way from the network sink to disk/stdin. Retain `decode_body: false` for storage data, explicit known Content-Length, and manual redirects. Never decompress, recompress, normalize content encoding, or mutate bytes to make transport work. Small XML/control responses may be materialized under a configurable explicit limit; object bodies may not.

### 7.2 Sources, sinks, and verification

File sources are reopenable bounded ranges with a captured identity/fingerprint. Hashing is incremental. When payload signing requires a precomputed hash, use a replayable bounded prepass or verified streaming-signature mode; never read the object into one binary. Detect source mutation between planning, hashing, and retries.

Downloads stream to an exclusive temporary file in the destination filesystem. Verify response length, range/content identity, and requested checksum before atomic publication. A failed or truncated download must not replace an existing destination. On stdout, bytes already written cannot be recalled: return failure on a later checksum/transport error and never append a JSON summary to the object stream.

Resume downloads only when remote identity and range semantics can be validated. Validate `206` and Content-Range; a provider returning `200` to a resume request must not cause append corruption. Use version IDs where available and conditional reads otherwise.

Object-to-local mapping rejects path traversal, absolute-path escapes, unsafe symlink traversal, file/directory collisions, and filesystem name collisions. Detect case-insensitive and Unicode-normalization conflicts. A string-prefix or `Path.expand` check alone is not a complete filesystem safety boundary; test the actual publication procedure and document platform guarantees. Do not silently rename remote keys.

### 7.3 Multipart and resume

Multipart configuration is provider-specific. The AWS limits page inspected for this design lists 10,000 parts, 5 MiB through 5 GiB per part except the final-part minimum, and a maximum object size of 48.8 TiB. Do not retain a historical 5 TB object ceiling or assume every compatible server has AWS limits. Record exact byte limits and their source in the capability data during M0.

For known object size, choose part size from the provider limits and maximum part count before uploading. For unknown-length stdin, accept an expected-size hint and use a documented part-sizing/spooling strategy; fail before exceeding limits and never silently truncate. Retrying non-replayable input requires retained bounded/spooled data. Do not claim restart-resume for stdin unless the required source bytes were explicitly persisted.

The reducer state progression is `planned -> initiating -> transferring -> completing -> verifying -> completed`, with explicit retry, cancellation, abort, failed, and indeterminate outcomes. Completion may return an embedded S3 error in HTTP 200; parsing a valid success result is mandatory.

A local versioned journal records endpoint identity, credential-provider reference, bucket/key, source fingerprint, upload ID, part numbers/ranges/checksums/ETags, transfer options, completion identity, and cleanup state. It contains no secrets or presigned URLs. Use atomic files plus an append-only event/checkpoint format under the ESS state directory; no database is introduced. Lock against simultaneous resumers. On resume, reconcile with ListParts and remote state, and reject changed sources/options/endpoints.

Journal writes must make acknowledged progress recoverable before releasing required replay data. On cancellation, abort by default; `--keep-incomplete` or an explicit resume policy retains the upload. Aborted versus abort-failed versus retained must be visible. Cleanup may only touch uploads owned by the journal or explicitly selected by the operator.

An interrupted CompleteMultipartUpload can have an unknown outcome. Verify the recorded destination identity where possible; do not blindly start a second upload or report success from an unrelated existing object. Keep unresolved outcomes inspectable.

### 7.4 Copy, move, retries

Use server-side CopyObject or multipart UploadPartCopy only when the endpoint/auth topology permits it. Encode the copy source and version correctly; preserve metadata, tags, annotations, encryption options, and copy directives according to the selected policy. Validate the complete success/error response, including HTTP-200 errors.

Different S3 services require a bounded download-to-upload bridge with distinct contexts; do not send a source URL to the destination as though S3 CopyObject were a universal remote-copy API. Make cross-endpoint transfer cost and the selected plan visible.

A move is copy, verify, then delete the selected source identity. Do not delete after an HTTP status alone, after a partial recursive copy, or after a changed-source conflict. A nonversioned source cannot provide an unconditional atomic move guarantee across services. If safe conditional deletion is unavailable, preserve the source and report the conflict/limitation rather than deleting a possibly newer object.

Classify retries by transport state, operation semantics, provider error, replayability, and remaining deadline. Apply exponential backoff with jitter and bounded attempts for eligible failures. Authentication errors, malformed input, and authorization failures are not ordinary transient retries. Re-sign retries with current credentials/time. PUT/COPY into versioned buckets and deletion that creates delete markers can have additional effects on replay; do not assume every PUT/DELETE is harmless to retry. Report uncertain outcomes explicitly.

## 8. Synchronization and destructive operations

`mirror` and AWS `sync` share mechanics but use separate comparison/filter/default policies derived from their pinned references. Test all three transfer directions and exact trailing-slash behavior. AWS filter rules are ordered; later matching include/exclude rules take precedence. Preserve timestamp/size, `--size-only`, `--exact-timestamps`, copy-properties, metadata-only changes, symlink, and Glacier behavior as defined by the selected frontend, not by a convenient common approximation.

The planner emits copy/update/skip/conflict/delete intents with reason and observed source/destination identities. Dry-run performs no remote mutation and creates no multipart upload. Same-target and overlapping source/destination cases are rejected or explicitly handled before destructive planning.

Never hold entire bucket listings in memory. Use paginated cursors and bounded local traversal. Where ordering is guaranteed, use a merge comparison; otherwise use a bounded external sort/partitioned manifest on disk. Directory buckets and other providers must not be assumed to return lexicographically ordered inventories.

Deletion is a separate phase after successful, complete enumeration and successful required transfers. The plan carries an explicit enumeration-complete marker. On listing errors, missing/repeated continuation tokens, unreadable local subtrees, cancelled scans, or transfer failures, suppress deletion. Excluded items remain protected. Revalidate deletion candidates against observable current source/destination state; a non-snapshot S3 listing cannot justify a snapshot-consistency claim. Prefer skipping newly changed items over deleting concurrent writes.

Native recursive deletion uses explicit confirmation/force controls, dry-run, and optional deletion-count thresholds. AWS namespaces preserve the reference's explicit destructive flags and noninteractive semantics rather than unexpectedly adding prompts; documented fail-closed safety deviations are recorded in the compatibility ledger. Never treat an alias root as an implicitly authorized delete-all target.

Versioned deletion distinguishes current-object delete markers, a selected version, all versions, multipart uploads, and bucket removal. `rb --force` must not be interpreted as permission to erase all historical versions unless that is an explicit command contract. Batch DeleteObjects parses per-key failures even when the HTTP request succeeds; retry only eligible failed entries. Object Lock and legal holds are enforced by the service and never bypassed implicitly.

## 9. Output, errors, security, and configuration

### 9.1 Script contracts

Stdout carries requested data only. Progress, warnings, prompts, and diagnostics go to stderr. Disable color/progress when inappropriate for a non-TTY. Escape terminal control sequences in displayable keys without altering the underlying key. Raw `cat`, `pipe`, and stdout download modes have exclusive ownership of their binary stream.

Retain legacy JSON shapes for legacy invocations through an adapter. New native `--output ndjson` uses a versioned per-item event schema with operation, target, status, bytes, error, and request ID, plus a documented final summary. Avoid unbounded arrays for streaming commands. AWS `s3api` renders the service result structure without an ESS envelope and follows the reference's query/pagination/output rules. Do not silently truncate results to satisfy a memory limit; offer streaming output or explicit limits, and fail clearly when an inherently materialized query exceeds the configured budget.

Use typed errors internally: category, operation, provider code/message, HTTP status, request IDs, retryable flag, attempt, target, and safe details. Distinguish client implementation gaps from provider unavailability, access denial, not found, and transport failure. A 403 is not proof an operation is unsupported. Avoid dumping arbitrary upstream bodies into terminal/log output.

AWS compatibility return codes follow the pinned reference, including success 0, failed S3 transfers 1, skipped-transfer conditions 2, interruption 130, syntax 252, configuration/credentials 253, service errors 254, and other failures 255. Preserve waiter-specific behavior and any command-specific exceptions. Native commands use the same category vocabulary with documented aggregation; `--quiet` never converts failure to success. Map SIGTERM separately and test that both signals cause cleanup.

### 9.2 Local configuration and secrets

Keep `~/.config/ess/config.toml` compatible and add documented XDG/config-directory overrides. Store aliases separately from credential material where practical; allow environment/profile/credential-process references. An optional OS credential-store integration must not become a hidden requirement on headless Linux.

Create private configuration/state directories and permission-restricted files; use atomic replacement with locking and symlink checks. Preserve unrelated sections, comments where supported, and TOML escaping. Back up once before an explicit schema migration. Invalid or unwritable configuration is an error, never an instruction to reset profiles.

Alias listing and config export redact secrets by default. Importing mc configuration is an explicit read-only source operation with a preview and conflict policy. Existing command-line secret flags are retained for compatibility but safer input/provider mechanisms are documented; never repeat supplied secrets in errors.

XML parsing must be bounded, reject external entities/DTDs, avoid unbounded nesting/entity expansion and dynamic-atom growth, and retain namespaces and unknown response fields according to the protocol contract. Sanitize terminal output and error bodies. Journals, temporary files, and credential refresh processes have bounded lifetimes and reliable cleanup paths.

### 9.3 Capabilities

Capability records distinguish `supported`, `unsupported`, and `unknown`, scoped by endpoint/provider version, operation, bucket type, and where relevant principal/region. Unknown is not unsupported. Probes are read-only, bounded, opt-in when they may be costly, and never create/delete resources to discover support.

For known ESS versions, explicitly deny operations whose router handling is known unsafe or absent. For generic endpoints, send correctly modeled operations and preserve authentic provider errors; do not hide the operation merely because another provider lacks it. Cache capability observations with invalidation and provenance. Always provide a diagnostic explanation of the operation, provider limitation, and independent CLI implementation status.

## 10. Validation and release contracts

Test the actual built `ess` executable, not only functions called inside the umbrella. Required layers are pure/unit tests, raw-wire fixtures, deterministic fault injection, CLI process tests, ESS/RustFS/MinIO interoperability where applicable, and dedicated AWS integration evidence. AWS is the reference for AWS-specific behavior; an ESS server and CLI sharing the same bug cannot validate each other.

Maintain regression coverage for existing raw-byte/redirect behavior. Add independent signature vectors, unusual-key round trips, pagination over multiple pages, streaming memory bounds, source mutation, HTTP-200 embedded errors, partial batch deletion, incomplete multipart operations, cancellation, resume conflicts, temporary-credential refresh, and filesystem escapes/collisions.

Release artifacts must include the escript, platform/runtime requirements, checksums, compatibility report, and changelog. Test the packaged Hex dependency in a fresh consumer outside the umbrella. Include required generated model data/notices in the package rather than assuming repository-relative files exist. Do not promise a self-contained native binary when shipping an escript that requires an Erlang runtime.

Use existing CLI publishing/release workflows and align release versions with immutable source commits. Implementation milestones should be tested, committed, pushed, and released through the configured process, with actual outcomes reported. This design-only change does not publish a new executable or imply completion of a milestone.

## 11. Rejected shortcuts

- A separate handwritten transfer implementation per command or frontend.
- Shelling out to mc/AWS CLI as the implementation.
- Starting the ESS server or importing its core/Concord metadata to operate the client.
- Advertising full S3 support based on a command-name list or generated stubs.
- Copying MinIO-specific admin operations into the portable S3 contract.
- Whole-file/whole-bucket buffering, blind redirect following, or implicit anonymous fallback.
- Assuming ETag is universally an MD5 checksum or that an HTTP 200 guarantees S3 success.
- Treating `mirror --remove` as a safe incremental delete loop during incomplete enumeration.
- Guessing that current provider limits, credential mechanisms, or checksum enums still match an older implementation.

## 12. References

References below are primary sources consulted for the design. Rolling documentation is not the release lock described in section 3.

- [MinIO mc pinned source](https://github.com/minio/mc/tree/77f82e18b5401a65958f1619df6ebb994634bd88)
- [AWS CLI high-level S3](https://docs.aws.amazon.com/cli/latest/reference/s3/)
- [AWS CLI S3 API inventory](https://docs.aws.amazon.com/cli/latest/reference/s3api/)
- [AWS CLI cp options](https://docs.aws.amazon.com/cli/latest/reference/s3/cp.html)
- [AWS CLI sync options](https://docs.aws.amazon.com/cli/latest/reference/s3/sync.html)
- [AWS CLI presign](https://docs.aws.amazon.com/cli/latest/reference/s3/presign.html)
- [AWS CLI return codes](https://docs.aws.amazon.com/cli/latest/userguide/cli-usage-returncodes.html)
- [AWS authentication](https://docs.aws.amazon.com/cli/latest/userguide/cli-chap-authentication.html)
- [AWS configuration and credential files](https://docs.aws.amazon.com/cli/latest/userguide/cli-configure-files.html)
- [AWS multipart limits](https://docs.aws.amazon.com/AmazonS3/latest/userguide/qfacts.html)
- [CopyObject, including embedded errors and endpoint/auth exceptions](https://docs.aws.amazon.com/AmazonS3/latest/API/API_CopyObject.html)
- [CompleteMultipartUpload, including embedded errors](https://docs.aws.amazon.com/AmazonS3/latest/API/API_CompleteMultipartUpload.html)
