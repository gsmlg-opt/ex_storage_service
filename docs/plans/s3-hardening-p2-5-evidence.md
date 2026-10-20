# P2.5 tagging, CORS, and location evidence

Goal: ESS-S3-HARDENING-001. Branch `main`; source baseline
`3477bc540c3732dba3d47a7e08779783f9be25fc`. All changes are uncommitted.
This document covers the authorization/configuration worker only. It is not a
claim that Phase 2 or the overall goal has passed.

## Implementation boundary

Implemented shared tag validation, transaction-fenced bucket tag/CORS storage,
bounded SAX control XML, bucket/object/version Tagging handlers, CORS config and
preflight/actual-response handling, and GetBucketLocation. Added hosted
`ESS_S3_REGION` configuration with default `us-east-1`; existing accepted signing
regions and gateway upstream regions remain independent.

Root owns operation/router/IAM integration, shared error mappings, common region
headers, version-tag metadata transactions, and listing/version work. Integrity
worker owns tags on PUT/COPY/multipart, tag-count response headers, and associated
transfer regressions. Gateway object tag CRUD is explicitly rejected before
external requests; no unsupported operation silently changes local bytes.

## Completed checks

- Parent reports production strict compilation passed after the initial P2.5
  modules were added. Parent owns the combined compilation evidence.
- `elixir --erl '+S 2:2'` with OTP `:xmerl_sax_parser.stream/2` confirmed the
  actual S3 namespace SAX events have empty attribute lists and return an empty
  binary remainder. The parser retains Unicode codepoints correctly.
- Fresh VM pure tests loaded current source modules and existing test dependency
  paths, without Mix or application startup: **7 tests, 0 failures**.
  Evidence: `docs/reports/s3-hardening/p2-5-pure-control.log`.
  Test files: `storage/tags_test.exs`, `storage/bucket_cors_test.exs`, and
  `handlers/control_xml_test.exs`.
- These cases cover Unicode and encoded tag headers, duplicate/reserved/invalid
  tags and count limits, same-rule CORS matching, wildcards, invalid rules,
  normalized CORS serialization, namespaces, entity rejection, attributes,
  ambiguous structures, multiple roots, trailing text and XML depth limits.

## Integrated application/HTTP verification

The coordinator handed this worker the runtime. Initial integrated validation
passed core 7/0 and failed S3 2/20: an assertion incorrectly assumed a single
Vary header field (compression correctly uses an additional field), and common
HEAD responses omitted the configured region header. The test now parses all
Vary fields and the coordinator added the common authoritative region header.

Repair/validation round 1 passed: **core 7/0 and S3 20/0**, process exit 0.
Evidence: `docs/reports/s3-hardening/p2-5-integrated-r1.log`; initial failures
remain in `p2-5-integrated.log`. Mix formatted all owned source/tests and additive
config fields. The run included:

- Core `storage/tag_cors_config_test.exs`: persisted state, injected transaction
  timeout, invalid-update preservation, stale generation/deletion/recreation,
  missing bucket, and removal of configuration during bucket deletion.
- S3 `tagging_http_test.exs`: real signed bucket/current/historical tag CRUD,
  isolated IAM actions, 10/50 tag limits, malformed/oversized controls, missing
  identities, delete-marker handling, retained body/version identity and gateway
  501 with a zero-request upstream spy.
- S3 `cors_location_http_test.exs`: real signed configuration CRUD and grants,
  missing/denied/allowed preflight, actual unsigned 403 with CORS headers,
  signed content, exposed headers, Vary, region defaults/changes, and signing
  compatibility.

These tests passed against the integrated real HTTP route. A24 also requires
PUT/COPY/multipart integration and cluster proof outside this worker's source
ownership. New independent `e2e/s3_hardening_tag_cors.py` covers those contracts
using normal Boto3 checksums against disposable loopback endpoints; Python AST
validation passed, but the probe has not yet executed. A24 is therefore partial.
A25's scoped HTTP requirements pass; whole-goal acceptance remains coordinator
owned. A strict compilation warning was reported to the coordinator in another
worker's `handlers/object/cloud_copy.ex:20` (unreachable error clause), so this
worker does not claim a clean combined warnings-as-errors compile.

## Process and repair state

No worker-owned application or service is running. No new dependency, commit,
push, release, deployment or production operation occurred. P2.5 implementation
used one successful repair/validation round after the first integrated run; the
first pure validation passed. Runtime ownership has been returned to root. Prior Phase 1
repair counts and evidence remain in `s3-hardening-auth-evidence.md`.
