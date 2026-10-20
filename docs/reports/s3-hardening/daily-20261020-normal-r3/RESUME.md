# Current ESS-S3-HARDENING-001 handoff

Current user scope: daily-use tests only; exclude scale, extreme-case and
high-concurrency testing. This scope is complete: `daily.json` records 14/14
groups passed, exit 0, source `f854dedc…`, Concord/VSR/ExTurso 3.1.3 and HTTP
0.21.13. Start/end HEAD remains `3477bc540c3732dba3d47a7e08779783f9be25fc`.

Read `SUMMARY.md`, `daily.json` and `verification.json` in this directory, then
`docs/plans/s3-hardening-results.json:current_execution_scope` for the current
result. Preserve all existing uncommitted work. No runtime remains from these
attempts; each owned server, listener and disposable root was independently
confirmed absent. No commit/push/publish/deploy/production operation occurred.

The default hardening CI entry point is `e2e/run_hardening_daily.sh`. To repeat,
use the literal `/tmp/ess-s3-hardening-python/bin/python` and a **new** label;
do not resolve its virtualenv symlink. Preserve MIX_HOME/HEX_HOME. The wrapper
sets PAGER and the usual bounded ERL_FLAGS. No dependency source patch is used.

The full historical matrix is not complete (35/38, A28/A30/A38 unpassed). Its
original source identities and failures remain unchanged. Do not restart the
old scale/resource/concurrency/fault/restore/release workloads or the full
umbrella test suite under this current user scope. Extended CI execution
requires an explicit manual selection and has not been run here.
