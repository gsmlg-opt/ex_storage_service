# v0.6.10 release verification

[v0.6.10](https://github.com/gsmlg-opt/ex_storage_service/releases/tag/v0.6.10)
was published on **2026-10-20 at 06:36:33 UTC** as the latest stable release.
All four jobs in the canonical
[release workflow](https://github.com/gsmlg-opt/ex_storage_service/actions/runs/39376926215)
passed. Both public Hex packages, GitHub downloads and the server image were
independently verified after publication. No production deployment or
production-data operation was performed.

## Release identity and published artifacts

- Validated source: `4077715a93d9a3c58893110d7e8522e80810587a`.
- Canonical tag/version commit: `9fcf1988b15d83461a99f65be4521e6f5b1bb63a`.
  Its parent is the source above; an independent comparison confirms its only
  changes are the six version literals, all set to `0.6.10`.
- Image build-start time: `2026-10-20T06:02:49Z`, matched against compiled
  metadata and OCI labels. This is the actual build time, not a commit date.
- This checkout fast-forwarded to the workflow version commit while preserving
  local verification reports.

| Public artifact | SHA256 |
| --- | --- |
| Core Hex tar, `ex_storage_service 0.6.10` | `09ac9e772b3de97da4ccfc3be1b47d671811034b43b163ede71ff98012e97320` |
| CLI Hex tar, `ex_storage_service_cli 0.6.10` | `101f2f3541ecfa6af2e0e387def41dfa1e6c5cae7fa2aa49c71c02460ef1b2e3` |
| GitHub Linux x86_64 / OTP 28 escript | `28d3bc8439fa6823c51da933581e32944fd34e746f32d7b96212f1dc47415446` |
| GHCR `0.6.10` and `latest` manifest | `c5a4596ca71fd13ed8d6912f203694b074bd9c769adde60ce0b3ab2a1ead9c09` |

All **21 GitHub assets** were downloaded anonymously. API sizes/digests match;
`SHA256SUMS` covers all **20 payloads** exactly once. The actual downloaded
escript is 5,744,512 bytes and matches the clean candidate manifest. Anonymous
registry checks confirm `ghcr.io/gsmlg-dev/ess:0.6.10` and `latest` resolve to the
same verified image digest.

## Independent public package and runtime verification

The Core tar contains **133 files**, all byte-for-byte identical to the canonical
Git source, with no missing or extra paths. Its Hex inner checksum is
`992fa316d78f61c945a8835bf04e3ff19ff2ea56295e803d252f2125434dc6a8`;
outer SHA256 matches the Hex API and producer manifest. A fresh isolated public
consumer passes dependency resolution, strict compilation and execution. It
loads version `0.6.10` and `Storage.Engine` without the web application.

The CLI public tar's inner checksum is
`1eaba75b0910826e793c4930d62798f37e87616706788cef42e0e3626bd20a90`.
Both package checksums match the producer candidate. A fresh independent install
passes all **12 daily package/runtime gates**, including source/build/deps-hidden
execution, model help/skeleton, small real HTTP/Unicode, binary stdout, one
successful credential helper and normal pager PTY. The public tar was rebuilt
with Elixir 1.18.5/OTP 28; its rebuild hash is recorded separately and is not
substituted for the GitHub executable.

The exact downloaded GitHub executable runs from an unrelated `/tmp` directory
on Linux x86_64, Elixir 1.18.5/OTP 28, with no build or installation. All **seven
normal runtime groups** pass: embedded assets/application boundary, version,
model help/skeleton, one daily credential helper, Unicode HTTP, binary HTTP and
normal pager. The helper verifies literal argv, typed credentials, discarded
stderr and redacted records; no extended helper cases execute.

The exact public image passes liveness, readiness, all four server app versions,
compiled source/ref/build time, OCI labels and ordinary read-only operator RPCs.
The owned container uses network `none`, no host binds or published ports, and
is removed. HTTP fixture threads stop and native/pager temporary roots are
empty after the CLI probes.

Both Hex package indexes identify `0.6.10` as latest and latest stable. Both
release APIs and both versioned documentation index/API-reference pages return
HTTP 200 with matching versions/titles: **8/8 public surfaces** pass.

The umbrella retains coordinated HTTP lock **0.21.13**. Independent fresh Core
and CLI consumers resolve **0.21.14** within their published constraints and
pass their checks. Concord, Viewstamped Replication and ExTurso resolve together
to **3.1.3**. No dependency source, authentication, checksum or quorum guard was
weakened for publication.

## Daily verification and its limits

Release validation passes strict compilation, formatting, **40 daily application
tests** (core 4, CLI 11, S3 4, web 21) and the sequential standalone S3 profile,
**14/14 groups**. The workflow daily run takes 75.414 seconds including strict
production compilation. Its start/end commit is the canonical version commit;
source fingerprint is
`605ff8c0fff866e7b6971b2883b6c692de6a4c2d567f232ba2b9e91104e1deed`.
Server stop, listener close and disposable-root removal are all confirmed.
Real SigV4 authentication and default SDK request/response checksums remain
active; client concurrency is one and the largest multipart part is 5 MiB.

The user explicitly selected daily-use testing on 2026-10-20. Scale, stress,
high concurrency, extreme/fault/crash and multi-GiB profiles were not run for
this release. Retained historical acceptance is **35/38** at its recorded
source; **A28/A30/A38 remain unpassed**. It is not current-source certification
or a benchmark/capacity claim. Original failures and extended profiles remain
available; none are marked passed by the daily result.

Existing buckets still require the quiesced, backup-verified
[listing index conversion](../operations/listing-index.md). Existing metadata
retains command version `0`; do not switch automatically. Fresh empty restore
targets require version `1` on every voter through recovery/restarts/activation.
Verify backup checksums and restore on the same OTP major before upgrading.
See the [release notes](../releases/s3-hardening.md) for the recovery boundary.

## Preparation and retained evidence

The user authorized main-branch commits, push and the next GitHub release.
Baseline was `3477bc540c3732dba3d47a7e08779783f9be25fc`, matching origin/main.
Only this checkout was registered; `.trees/` contained no worktrees to merge or
remove. Logical source commits were `ff9c8ec` (CLI fixes), `e2060c3`
(Core/S3/cluster), `2ab9709` (daily/extended release verification) and `4077715`
(documentation), followed by the workflow-owned version commit.

Local preflight passed whole-project formatting, YAML checks, 13 inventory/
release-selection checks, 16 narrow packaging checks, 40 application tests,
a fresh 12-gate CLI install and a 14/14 daily S3 run. The local CLI preflight
reports pre-bump `0.6.9` and is explicitly not public `0.6.10` evidence. The
local daily source fingerprint was
`00cae04ae1887fed5d9c00a3849039b068ab35dfe6cfc79a3ca8e312ca3ef411`.
The workflow then rebuilt and checked the committed `0.6.10` artifacts.

- [Independent public artifacts and runtime receipt](s3-release-0.6.10-public.json).
- [Canonical release daily S3 receipt](s3-release-0.6.10-daily.json).
- [Pre-bump local CLI preflight](s3-release-0.6.10-cli-preflight.json).
- [Local daily preflight](s3-hardening/daily-20261020-release-preflight-r1/daily.json).
- [Historical full acceptance](s3-hardening-verification.md).

Raw local hardening evidence (3,682 files, about 640 MB, including samples and
disposable Concord files) remains local and ignored. Compact reviewed receipts
were added explicitly; no raw directory was staged wholesale.

Status: release and all daily-scope publication verification complete. Final
post-publication records are maintained separately from the immutable release
tag.
