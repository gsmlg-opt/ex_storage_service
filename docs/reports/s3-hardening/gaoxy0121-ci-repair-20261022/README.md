# Gaoxy 0.12.1 CI repair evidence — 2026-10-22

The first hosted full audit [39768692842](https://github.com/gsmlg-opt/ex_storage_service/actions/runs/39768692842) completed **failed** at candidate `0f347902a2e5c0c649bcec4c14d904c5281a3bd6` (source fingerprint `47c8e99c7999ac49c92a0224001e91d18a16c0b09a2a3932ca4a2656a5a94ec5`). Every job completed; failed, blocked, and unrun scenarios remain unpassed. The local checks below cover uncommitted CI harness repairs, not a new immutable candidate or full acceptance. No publication is established by this evidence. Historical C7 receipts and ledger remain unchanged.

## Hosted outcomes

| Lane | Result and observed boundary |
| --- | --- |
| Daily, gateway, crashes, faults, release | Jobs succeeded. Release lane validates artifacts/packages; it does not mean a release was published. |
| Correctness | Format and strict compile passed. Core **606/0** (1 excluded), cluster transport **43/0** (1 excluded), CLI **3491/0**, S3 **379/0**, web **24/5**. All five failures were web authentication assertions under inherited CI administrator configuration. |
| Standalone | Earlier phase1, versions/lists, tags/CORS, multipart/races and multipart restart passed; clients profile failed because child processes did not receive the supplied `--mc` path. Missing default executable `/tmp/ess-s3-hardening-tools/mc`; subsequent coverage not certified. |
| Cluster | Tagged three-voter test command passed; wrapper later failed at the same missing `mc` path. |
| Index | Missing `rg` blocked source-inspection gates before A28/A29 workloads started. |
| Restore | Standalone A36 passed. Cluster source `data-a` failed S3 listener startup with `:eaddrinuse`. The original conflicting port owner and mechanism remain unknown. |
| Resilience | First resilience command passed. Replication-backlog service exited during startup; original artifact omitted the private service log needed for diagnosis. |
| Resources | Standalone service exited at startup before groups completed; original cause unknown. Cluster temporary sampling failed the unchanged 100 ms limit. Tagged large-stream command passed. |
| Aggregate | Failed; complete full-acceptance evidence unavailable. |

[hosted-run.json](hosted-run.json) retains live-queried job IDs/results and extracted correctness counts/failure names. [provenance.json](provenance.json) records hashes of actual input receipts/logs. Existing detailed triage lives in sibling `gaoxy0121-ci-39768692842-*-triage/` directories.

## Local repair checks

The coordinator ran these commands from the repository root, each with exit status 0. [local-verification.log](local-verification.log) preserves their complete compact outputs: **57 Python tests passed**, **24 web tests passed**, and strict test compilation completed without warnings. No checks were rerun merely to curate this directory.

```sh
PAGER=cat PYTHONDONTWRITEBYTECODE=1 PYTHONPATH=e2e python3 -m unittest \
  e2e.test_hardening_acceptance e2e.test_hardening_runtime_logs \
  e2e.test_resource_cadence e2e.test_restore_startup_diagnostics \
  scripts.release.test_daily_workflows

PAGER=cat ERL_FLAGS='+S 4:4 +SDcpu 2 +SDio 2' MIX_ENV=test \
  ESS_ADMIN_USER=ci-override ESS_ADMIN_PASSWORD_HASH='<workflow value omitted>' \
  ESS_DATA_ROOT=/tmp/ess-gaoxy0121-web-ci-env-20261022 \
  mix test apps/ex_storage_service_web/test --max-cases 1 --no-color

PAGER=cat ERL_FLAGS='+S 4:4 +SDcpu 2 +SDio 2' MIX_ENV=test \
  mix compile --force --warnings-as-errors
```

The password-hash placeholder deliberately omits the inherited value; the executed command used the existing workflow value. The coordinator reports an initial web invocation stopped before any tests because the build cache held a compile-time source SHA from `52e4b57` instead of `0f34790`. Strict recompilation fixed the stale cache; the identical web invocation then passed. This directory contains the successful retry output and recompile log, not the initial failure log. No validation-disabling flag was used.

Repairs cover `--mc` propagation, installation of the mandatory ripgrep prerequisite, isolation of web fixture administrator credentials, sanitized service-log export even after cleanup failures, and restore-startup socket-owner diagnostics. These local checks do not prove that all original hosted startup failures are fixed.

## Restore and listener diagnostics

[local-restore-a37.json](local-restore-a37.json) selects actual receipts from `../gaoxy0121-local-restore-diag-20261022-cluster/`: three restored voters verified, quarantine restart verified, activation completed, original roots inaccessible, object/configuration/identity/multipart checks passed, fencing retained, and six invalid backup/key cases rejected. Recovery took **16.097 s**. This is local cluster A37 coverage, not a replacement for the failed hosted run.

[listener-reuse.json](listener-reuse.json) selects receipts from `/tmp/ess-gaoxy-reuse-l4h3y6pp`: 20 immediate same-VM stop/restart cycles per S3/internal profile (40 total), plus three full-VM cycles per profile (six total, including SIGTERM and SIGKILL), passed using production listener settings with a trivial Plug. The cross-VM receipt records reaping and stopped process groups. The effective listening sockets already had `reuseaddr=true`; an explicit backend override was rejected as unsupported. No production option was changed. These probes did **not** reproduce or explain the hosted `:eaddrinuse` failure.

## Resource failure remains valid

[resource-cadence.json](resource-cadence.json) retains the failed 64 MiB, concurrency-4 strict-quorum PUT group, successful byte/hash checks and two ready replicas per upload, original resource envelopes, per-node sampling results, and four selected `data-a` observations. Its only group failure is `data-a:cadence:temporary:interval_over100ms`: **138.940762 ms** conservative coverage gap, with adjacent fresh temporary scans taking **61.109401 ms** and **63.141805 ms**. Other nodes' temporary scans remained below 1 ms in that group. Temporary bytes stayed **268435456** during the selected scans; observer process CPU barely advanced.

The conservative formula remains `end(current) - start(previous)`, not completion-to-completion spacing. The receipts do not distinguish a blocked filesystem metadata syscall from observer descheduling or another wait. CAS publication performs file/directory sync and rename operations, but their involvement in this gap is **not established**. No sample was dropped or cached; no interval, threshold, deadline, or resource envelope was relaxed. Remaining unexecuted groups stay unpassed.

## Sanitization boundary

Only selected JSON fields, compact already-inspected local outputs, source hashes, and this explanation are retained. Raw hosted logs, CI secret-mask lines, private cookies/controller state, request JSON, credentials, and authentication headers are excluded. Restore temporary roots and cross-VM diagnostic PIDs were omitted. Original full receipts are referenced, not duplicated. Sanitization checks are recorded in `sanitization.json`.
