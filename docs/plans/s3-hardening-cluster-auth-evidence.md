# Phase 1 cluster authorization and bucket-race evidence

Goal ESS-S3-HARDENING-001; baseline main
3477bc540c3732dba3d47a7e08779783f9be25fc. New verification scope only:
`e2e/cluster_iam_hardening.py` and
`e2e/scripts/cluster_hardening_control.exs`. Existing harness and its strict
RF=2/W=2, three voters, private transport and public-write guards are retained.

Status: in_progress. Python AST and imports pass with the existing
`/tmp/ess-s3-hardening-python/bin/python` (Boto3 1.43.114). Controller formatted
and executed. Tagged three-voter integration passed (1 test, 0 failures), and
production `mix compile --warnings-as-errors` passed. Real cluster IAM checks
passed as described below. Full A13/A14 acceptance remains blocked by executed
failures; no whole acceptance scenario is inferred from source inspection.

The Python script independently signs requests with Botocore and checks both
data endpoints immediately after each acknowledged IAM mutation, without an
eventual-consistency sleep or retry. It covers source-copy denial and absent
destination, explicit deny, missing/malformed attached policy, suspension,
reactivation, key deletion, and presigned-request revocation. Its controller
mutates only a new disposable principal and private 0600 state. Controller logs
and credentials never enter result JSON; failed runs retain private diagnostics
under a printed `/tmp/ess-cluster-iam-*` directory.

It also runs eight synchronized PUT/bucket-delete races across the endpoints,
checks that recreating a delete-winning bucket exposes no old object, and races
multipart completion against bucket deletion. The coordinator's separate
`e2e/s3_hardening_phase1.py` covers 32 create-only PUT writers and 32 conditional
multipart completions. These are independent required checks.

Run after a fresh prod compile, disposable harness `start`/`wait-ready`, and
`seed_cluster_e2e.exs`, with the existing harness cookie and seeded credentials:

```sh
PAGER=cat /tmp/ess-s3-hardening-python/bin/python e2e/cluster_iam_hardening.py
```

Required environment: `E2E_S3_ENDPOINTS` contains both data endpoint URLs,
`E2E_ACCESS_KEY_ID`/`E2E_SECRET_ACCESS_KEY` contain disposable full-access
credentials, and `ESS_CLUSTER_E2E_COOKIE` matches the harness. The optional
`E2E_CONTROL_TARGET` defaults to `ess_data_a@127.0.0.1`. The controller runs
`MIX_ENV=prod mix run --no-start --no-compile`; it never starts another ESS
instance, modifies dependencies, or changes quorum configuration.

## Executed results

Commands ran with disposable root `/tmp/ess-s3-hardening-cluster`, shared test
cookie, default harness test master key, and `ERL_FLAGS='+S 4:4 +SDcpu 2 +SDio 2'`.
The existing strict RF=2/W=2 and all guards remained unchanged. The production
controller receives the same explicit `ESS_MASTER_KEY` as the harness. The first
controller attempt exposed this required runtime environment even with
`--no-start`; the subprocess environment was corrected before the successful
IAM execution (one automation repair round).

- `cluster-three-voter.log`: tagged integration 1 test, 0 failures.
- `cluster-prod-compile.log`: strict production compilation exit 0.
- `cluster-iam-hardening.log`, first JSON record: all six IAM groups passed on
  both data endpoints, including immediate policy changes, missing/malformed
  attached deny, suspension, key/presign revocation and denied copy. The full
  script subsequently exited 1 in its separate bucket-race lane.
- `cluster-phase1-independent.log`: synchronized 32 create-only PUTs produced
  one 200, thirteen 412 and eighteen 500 responses. Node logs
  `cluster-failed32-data-a.log` and `cluster-failed32-data-b.log` establish
  `RuntimeError: reached the maximum number of tasks for this task supervisor`
  from `WriteCoordinator.transfer_remote/5`. Per-request `async_stream`
  concurrency does not admit requests against the shared supervisor's global
  child limit. Coordinator owns the scoped fix; tests were not relaxed.
- The bucket-race lane passed its strict PUT/delete status check but then got
  HTTP 400 from ListObjectsV2 while verifying the recreated bucket. This remains
  an unresolved executed failure; remaining race iterations and multipart race
  were not executed and are not passed. Read-only diagnosis of
  `cluster-iam-data-b.log` identifies a Bandit request-line parse error:
  `"ening-race-295d728113dd484a?list-type=2&encoding-type=url HTTP/1.1\\r\\n"`.
  Exactly nine leading bytes (`GET /hard`) were consumed, matching the rejected
  PUT's nine-byte `candidate` body. The losing PUT returned before reading its
  body; the independent client uses Expect/continue and sends no body after the
  early failure. Reusing that connection consumed the next request as the old
  body. This is early-rejection connection handling, not listing dispatch.
  Coordinator was notified that the bounded failure path must close an
  unconsumed body-bearing connection; no Shared/handler edits were made here.

Every retained shell session traps shutdown of its three owned nodes. Detached
startup from a completed tool command was terminated by the executor, so actual
checks used one retained shell session for start/wait/seed/Python/stop. All own
commands are complete, all harness nodes stopped (confirmed by epmd), and seeded
credential files were removed. Failed roots/logs remain disposable evidence;
original saturation state is archived at
`/tmp/ess-s3-hardening-cluster-failed32` without its credentials file.

The script leaves disposable buckets for diagnosis; the harness owner shuts
down only its owned processes and retains their logs. The existing harness's
`start` deletes its root: use a newly allocated `/tmp` root and never point it
at existing application/production storage.
