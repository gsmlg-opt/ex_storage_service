# Move fences and source encryption

## Selected GET responses

High-level downloads and raw stdout now validate the returned ETag and selected
version before consuming bytes whenever preflight captured an identity. HEAD
success alone cannot establish that GET honored its selection. A missing or
different returned identity fails before atomic local publication, before any
stdout bytes, and before move-source deletion. Byte-length and checksum checks
remain independent requirements.

The same boundary applies to a cross-endpoint bridge's full-object staging GET.
Sending IfMatch and VersionId is insufficient if a provider ignores those
members. `RemoteSource.stage` requests explicit response-identity verification
before publishing its temporary source or allowing a destination upload. Ranged
bridge requests already validate their selected identity and Content-Range.

## Recursive moves

Recursive native and AWS moves first copy every selected object without deleting
sources. Successful copies produce a second private, bounded disk manifest with
the captured source and copied destination identities. It contains no credentials
or encryption keys. The existing external merge implementation bounds records,
batch memory and open files; the implementation never collects the complete
inventory in memory.

After every required copy succeeds, a full read-only pass verifies both sides.
The deletion pass then rechecks both sides immediately before each removal. Local
sources retain their original file identity; remote sources retain the exact
version or an ETag fenced by the verified conditional-delete contract. Destination
checks use the copied object's identity and destination encryption context.
Missing provider identity, an unreadable or changed candidate, failed copy,
skipped archive, callback error, or incomplete manifest prevents the deletion
phase. A skipped required source makes the move incomplete even when archive
warnings were suppressed. Dry-run retains sources and creates no upload.

This is not an atomic multi-object move. Copies can remain after a later copy
failure. Once deletion starts, a later conflict or service failure can leave some
sources removed and others preserved; the command fails. Every removed source
must already have a verified copy. The controlled-parent-directory requirement
of `Transfer.Sink` and portable local filesystem race boundaries still apply.

### Deliberate AWS safety deviation

The locked AWS CLI 2.31.18 uses per-transfer `DeleteSource*Subscriber` callbacks:
`awscli/customizations/s3/subscribers.py:102-154`, installed by
`s3handler.py:367-374`, `421-427` and `459-463`. It deletes each source as its
individual transfer succeeds, regardless of a later object's failure.

The retained independent executable observation is
`test/fixtures/s3/recursive-move-aws-capture.json`. With concurrency one, upload
`a` succeeds and upload `b` receives AccessDenied. When `b` reaches the fixture,
`a` has already been deleted; AWS exits 1 and leaves only `b`. ESS deliberately
preserves both sources in that failure case, as required by design.md's recursive
move safety contract. This observation does not establish all recursive AWS or
mc argument, output or scheduling behavior.

Recapture with the hash-locked Linux executable:

```sh
python3 apps/ex_storage_service_cli/test/fixtures/s3/capture_recursive_move_aws.py \
  --aws /tmp/ess-cli-reference-m0/aws-release/aws/dist/aws \
  --output /tmp/ess-recursive-move-aws-recapture.json
```

## Source SSE-C separation

Accepted `--sse-c-copy-source` and `--sse-c-copy-source-key` now take precedence
over destination SSE-C fields in source HEAD/GET preflight. Server-side
CopyObject and every UploadPartCopy translate those source fields into the
modeled `CopySourceSSECustomerAlgorithm`, `CopySourceSSECustomerKey` and
`CopySourceSSECustomerKeyMD5` members. Destination encryption stays on the
ordinary SSECustomer members. No source key is applied to multipart initiation.
Existing request diagnostics omit headers, and durable journals retain input
digests instead of key material.

## Verification

The initial eight move regressions failed against the prior implementation. The
two source-encryption effect tests also failed: copy-source headers were missing,
and the first source HEAD used the destination key. An additional nonversioned
GET regression reproduced publication before IfMatch response verification.

The final scoped run passed **89 tests, 0 failures** on Linux / Elixir 1.18.5 /
OTP 28. It covers move safety, source SSE-C headers and diagnostic/journal
redaction, the existing copy and high-level suites, pipe, native routes, parser,
version selection, debug and native policy guards. Strict compilation, scoped
format checks and `git diff --check` passed. The actual pinned AWS recapture
matches the retained observation. These loopback checks do not prove live
provider SSE-C enforcement or full platform and frontend parity.

A follow-up focused integration repair passed **13 tests, 0 failures**, covering
the raw-byte executable test, server copy, and new full-object RemoteSource
staging regressions. The new staging suite first produced two failures against
the prior code: a mismatched GET was published as a temporary source and uploaded
across endpoints. Full staging now enables explicit response validation. Four
missing/mismatched ETag/version cases fail before publication; a matching response
retains all raw bytes. Existing raw-download and gzip-bridge fixtures now include
their selected response ETag, without changing their byte assertions. Copy's
journal-using tests run synchronously to avoid deterministic local mutex ports
colliding with unrelated concurrent wire fixtures; production locking and its
collision assertions are unchanged. Strict compilation, scoped formatting and
diff checks also passed for this repair.
