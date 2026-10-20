# Daily-use verification — 2026-10-20

The user explicitly replaced the current completion gate with normal daily-use
testing, excluding scale, extreme cases and high concurrency. The current
deliverable is complete: **14/14 daily groups passed**, process exit **0**, in
**3.461 seconds** on this already-built development checkout. This is not a
fresh-CI timing claim or full-matrix certification.

```bash
PAGER=cat bash e2e/run_hardening_daily.sh /tmp/ess-s3-hardening-python/bin/python 20261020-normal-r3
```

Use a new evidence label when repeating; the runner refuses to overwrite an
existing report directory. Install the existing pinned dependencies from
`e2e/hardening-requirements.txt` if the chosen virtualenv is new. The runner
strictly compiles production applications, then starts only the core/S3 fixture.

- Branch: `main`, starting/ending HEAD `3477bc540c3732dba3d47a7e08779783f9be25fc`.
- Uncommitted source fingerprint: `f854dedc7146316f830beffea13dcf42c6a9d62ad2525003190de60ed30012ee`.
- Runtime: Elixir 1.18.5, OTP 28, ERTS 16.4.0.5; Python 3.13.15.
- Independent client: Boto3/botocore 1.43.114. Request/response checksum policies
  both remain `when_supported`; SigV4 and anonymous-read denial are checked.
- Locked dependencies: Concord/VSR/ExTurso 3.1.3; http_fetch 0.21.13.
- Scope: one standalone loopback server, one sequential client, small fixtures;
  a normal two-part upload uses a 5 MiB first part plus a 16-byte final part.

The groups verify strict production compilation, health/readiness, bucket
CRUD/location/tags, object PUT/HEAD/GET/ranges/SHA256, default SDK CRC32 and real
gzip bytes, listing/pagination/delimiters, copy and replace metadata/tags,
presigned GET/PUT plus authentication, CORS persistence/preflight/authenticated
read, multipart/list/complete/abort, UploadPartCopy, batch and single delete,
versions/historical read and copy/delete markers, and removal of created data.
See [daily.json](daily.json) for every group and its actual result;
[compile.log](compile.log) and [server.log](server.log) retain execution logs.

The listener is supervised under the existing S3 supervisor and binds only
127.0.0.1. Fresh roots and a generated master key isolate test data from any
existing instance. Seed credentials are private and removed before business
checks. The owned server exited, its listener closed and its temporary root was
removed. An independent post-run check confirmed all three attempt PIDs, ports
and roots were gone; see `verification.json`.

The first two attempts remain failed receipts in adjacent directories. `r1`
failed before business checks because the fixture listener was linked to the
short-lived script process. Attaching it to the S3 supervisor fixed startup.
`r2` passed the earlier groups and failed an overly literal CORS fixture
comparison: this implementation returns an empty ID and canonical lowercase
header names. The normal fixture now supplies an explicit ID and lowercase
`etag`, retaining the full configuration assertion. No production behavior,
authentication or checksum check was changed to pass either attempt.

The hardening workflow now defaults to the daily profile for push/PR/manual
runs; no weekly heavy schedule remains. Original extended scripts and assertions
are retained behind explicit manual `extended: true`. YAML parsing, shell/Python
syntax, the new Elixir fixture's format and whitespace checks passed. Hosted
GitHub Actions execution was not dispatched or claimed.

Historical full acceptance remains 35/38 on its original fingerprint
`60ada0fa…`. A28/A30/A38 remain unpassed there. No scale, resource sampling,
high-concurrency, fault/crash, cluster, gateway, restore, release or multi-GiB
profile was executed for this daily delivery. Historical results are not reused
as current-source passes. The existing architecture, quorum/mode guards and
unrelated dirty changes are preserved. No commit, push, publish, deployment or
production-data operation occurred.
