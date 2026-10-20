# v0.6.10 release execution

The user explicitly authorized merging `.trees/` worktrees into `main`, logical
commits, push, and the next GitHub release on 2026-10-20. Their daily-use testing
scope remains in force: no scale, fault/crash or high-concurrency acceptance.

Baseline: `main@3477bc540c3732dba3d47a7e08779783f9be25fc`, matching `origin/main`.
`git worktree list --porcelain` lists only this checkout; `.trees/` has no
registered worktrees to merge or remove. Live latest release is `v0.6.9`.
The next patch release is `v0.6.10`, through root `release.yml` only.

Current implementation and operational data remain preserved. Raw local
hardening evidence (3,682 files, about 640 MB, including samples and disposable
Concord files) stays local and is ignored for publication; compact reviewed
receipts and reports are staged explicitly. Historical full acceptance remains
35/38 on its original source; A28/A30/A38 are unpassed, not a current daily gate.

Release preparation adds explicit daily validation to source/package workflows.
The full CLI credential-process suite remains the default extended package
probe; publication explicitly selects `--verification-profile daily`, retaining
fresh isolated install, embedded assets/licenses, actual installed execution,
normal helper credentials/redaction, real HTTP, binary stdout and normal pager.
Old large/concurrency/topology jobs remain available by manual selection.

Local preparation passed:

- Whole-project formatting and workflow YAML syntax checks.
- 13 inventory/release-selection Python checks; 16 narrow package-tooling checks.
- `PAGER=cat bash e2e/run_daily_unit_tests.sh all`: 40 tests, 0 failures
  (core 4, CLI 11, S3 4, web 21), `--max-cases 1`.
- Actual fresh CLI source tar, isolated dependencies, installation and source-hidden
  runtime: `/tmp/ess-v0610-cli-daily-r1/manifest.json`, daily profile. It reports
  pre-bump version 0.6.9 and dirty source, not published 0.6.10. All 12 package
  gates passed, including exactly one successful installed credential helper,
  real HTTP/binary output and normal pager PTY. The root workflow will rebuild
  and verify the committed 0.6.10 candidates and public bytes independently.

- Current-source daily HTTP: `bash e2e/run_hardening_daily.sh /tmp/ess-s3-hardening-python/bin/python 20261020-release-preflight-r1`, 14/14 passed in 3.917 seconds, strict prod compile exit0, normal SDK signing/checksum policies retained. Source fingerprint `00cae04ae1887fed5d9c00a3849039b068ab35dfe6cfc79a3ca8e312ca3ef411`; identical after the logical source commits. Owned server/listener stopped and disposable root removed.

Local commits prepared: `ff9c8ec` CLI cleanup/diagnostics; `e2060c3` core/S3/cluster implementation; `2ab9709` daily and extended testing/release gates. Documentation is the next logical commit. All `.trees/` worktree checks are complete (none to merge).

Status: local release preparation verified; ready to push and dispatch root release workflow.
Publication, public assets/packages/image and production use are not yet verified.
