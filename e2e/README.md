# End-to-End Tests

## Closeout gate — 2026-10-21

ESS-S3-HARDENING-CLOSEOUT-002 restores full A01–A38 acceptance as the publication
gate. Isolated scale/resource/concurrency/fault/cluster/restore runs are authorized;
run heavy work serially on a shared runner. Daily receipts from v0.6.10 remain
valid for their original narrower scope and are not full acceptance.

Automatic PR/push correctness runs complete core, S3, transport and web suites,
with retained daily CLI contracts. Local and CI use the same command:

```bash
PAGER=cat python3 e2e/run_correctness_tests.py --output docs/reports/s3-hardening/unique-correctness
```

The receipt records the complete discovered file inventory, per-app counts,
failures, skips and exclusions. Missing/shrunken suites fail. Full acceptance uses
the same runner with `--extended` for the entire CLI suite; existing cluster and
large-stream tags remain in dedicated heavy lanes. Publication requires a
successful extended workflow for the exact candidate through `acceptance_run_id`.

## Retained daily-use profile

The lightweight daily profile is a sequential standalone Boto3 exercise of
normal bucket/object CRUD, listing/COPY, attributes/tags, versions,
CORS/location, multipart and presigned operations. It uses disposable roots,
authenticated real HTTP, small payloads and default SDK checksums:

```bash
PAGER=cat bash e2e/run_hardening_daily.sh /absolute/venv/bin/python unique-run
```

Install the pinned clients from `e2e/hardening-requirements.txt` into the private
virtual environment first. Pass its absolute Python path and a unique run label.
The runner writes `docs/reports/s3-hardening/daily-unique-run/daily.json` and
retained logs, and shuts down its own server. No new production setting is added.

A daily pass does not promote unexecuted original A01–A38 clauses. The historical
35/38 result retains its source and scope, with A28/A30/A38 unpassed.

## Earlier feature profiles — 2026-10-19

The 2026-10-19 execution priority is normal business flow first, followed by
stress, failure recovery, scale and backup/restore hardening. Run the existing
profiles with an explicit `features` scope after strict production compilation:

```bash
PAGER=cat MIX_ENV=prod mix compile --warnings-as-errors
PAGER=cat bash e2e/run_hardening_phase2_standalone.sh /absolute/venv/bin/python unique-standalone features
PAGER=cat bash e2e/run_hardening_phase1_cluster.sh /absolute/venv/bin/python unique-cluster features
PAGER=cat /absolute/venv/bin/python e2e/gateway_hardening.py \
  --root /tmp/ess-s3-hardening-unique-gateway --mc /absolute/verified/mc --scope features
```

Hosted feature scope runs versions/listings, tags/CORS/location, multipart and
selected AWS CLI/Boto3/mc checks. The healthy cluster uses both data endpoints
and retains RF2/W2 plus ordinary policy, suspension and revocation checks. It
does not remove replicas, corrupt metadata, run concurrency races or restart
nodes. Gateway feature scope verifies authenticated CRUD, listing/COPY,
attributes/checksums, raw gzip/ranges/freshness, mode guards and retained mc
compatibility, without injected upstream failures or restart reconciliation.

Omitting the scope keeps the original full hardening behavior. Feature results
are recorded separately and do not mark unexecuted A01–A38 assertions passed.
Known Concord issues remain follow-up work; they do not block unrelated normal
feature delivery. See [feature progress](../docs/plans/s3-feature-status.md).

## Original extended S3 hardening acceptance

Install the exact client versions in `e2e/hardening-requirements.txt` into a
private virtual environment. The full local entry point is:

```bash
/absolute/venv/bin/python e2e/hardening_acceptance.py run --profile all \
  --python /absolute/venv/bin/python --mc /absolute/checksum-verified/mc \
  --label unique-run --output docs/reports/s3-hardening/unique-run
```

The independent lanes are `correctness`, `standalone`, `gateway`, `cluster`,
`index`, `resources`, `faults`, `crashes`, `resilience`, `restore`, and `release`.
`--profile NAME` runs only one
lane; it does not certify the whole goal. Each command has a deadline, a retained
log, source revision/content fingerprint, and a non-success result on failure.
The fingerprint includes `apps/`, `config/`, `e2e/`, `scripts/`, `.github/`,
Mix/npm manifests and locks, Dockerfile, .dockerignore, .npmrc, .formatter.exs and
`docs/cli/compatibility/*.json` generator/test inputs. Other documentation and
reports are explicitly excluded. A report-only descendant of a frozen
candidate may cite that candidate; any executable-input diff or fingerprint
mismatch rejects reuse. Reports do not have to contain their own future commit.
The resource lane includes the retained tagged two-GiB authenticated transport
test in addition to every local/gateway and cluster manifest case. Independent
resource topologies and index migration still run after a workload failure;
the lane remains blocked and a failed prerequisite compile stops its workloads.
The index scale entry point refuses the reproduced Concord 3.1.4 blockers
[#101](https://github.com/gsmlg-dev/concord/issues/101) and
[#102](https://github.com/gsmlg-dev/concord/issues/102) before expensive fixtures.
Use the smaller `index_setup_probe.exs` and `index_upstream_probe.exs` diagnostics
to validate a released upstream repair before admitting another full scale run.
`ESS_INDEX_PROGRESS` selects bounded JSONL diagnostic progress;
`ESS_INDEX_SOURCE_SHA256` and `ESS_INDEX_SOURCE_HEAD` record its source identity.
Roots, containers, processes and credentials belong to disposable harnesses.
The gateway lane requires Docker and the verified `mc` binary; isolated restore
requires working bubblewrap user/PID/mount namespaces. Prerequisite failure is
blocked evidence, never a successful skip. Production data/endpoints are outside
these runners. The release lane builds and executes only local artifacts.
The faults lane also requires bubblewrap: its real ENOSPC test mounts only the
disposable CAS/staging roots on a private8MiB tmpfs. It never fills a host disk.
The crashes lane stops only its own disposable BEAM at injected storage commit
boundaries, then runs a separate three-voter case that stops the origin after a
real authenticated RF/W peer acknowledgement. The latter verifies uncertain
operation isolation, retained-root rejoin and strict RF=2/W=2 internal recovery.
Neither case certifies external cross-cluster disaster-recovery crash behavior.
The separate resilience lane runs a small fresh three-voter workload with
maintenance, a data-node outage/recovery, and actual-primary election/rejoin.
It complements the existing cluster lane and retains independent lifecycle logs.

The aggregate verifier additionally requires every A01–A38 row in
`docs/plans/s3-hardening-results.json` to be passed with a complete assertion
audit, command, exit status, mode, toolchain, starting/ending SHA and retained
evidence. Partial logs do not automatically promote a scenario. Recheck retained
records with:

```bash
python3 e2e/hardening_acceptance.py verify \
  --output docs/reports/s3-hardening/unique-run
```

`.github/workflows/s3-hardening.yml` now defaults to the daily-use profile on
relevant pushes, PRs and manual runs. Extended acceptance requires an explicit
manual selection; no scheduled full-suite execution is part of the daily gate.
Extended lanes retain their original assertions and separate evidence. The
workflow has read-only repository permissions and publishes no package or image.

Bucket index readiness is a separate bounded operator check:

```bash
/app/bin/ess rpc 'IO.inspect(ExStorageService.Operations.ListingIndex.status("bucket"))'
/app/bin/ess rpc 'IO.inspect(ExStorageService.Operations.ListingIndex.readiness("bucket"))'
```

Each checks the bucket and its active index descriptor with two exact reads.
It does not scan every bucket on `/health/ready`. Node liveness, metadata/data
readiness and a particular bucket's index migration readiness are distinct.
Listing duration/page counters use finite `kind`/`result` labels.
`listing_index_checks_total{state=...}` counts operator observations rather than
claiming a global bucket inventory; no bucket names, object keys or tokens are
metric labels.

This directory contains black-box S3 compatibility checks for ExStorageService.
The tests use Boto3 against the running HTTP API, with path-style addressing and
AWS Signature V4 enabled, so they exercise the same client surface used by S3
tools. Cloud-cache checks use RustFS as an S3-compatible upstream and `mc` as
the client.

## Local Run

```bash
export MIX_ENV=prod
export ESS_S3_AUTH_ENABLED=true
export ESS_S3_PORT=9000
export ESS_ADMIN_PORT=4900
export ESS_DATA_ROOT=/tmp/ex_storage_service/e2e-data
export ESS_MASTER_KEY=test-master-key-for-local-e2e
export ESS_ADMIN_PASSWORD_HASH=$(printf '%s' 'local-e2e-admin' | sha256sum | cut -d ' ' -f 1)
export SECRET_KEY_BASE=$(mix phx.gen.secret)

mix deps.get --only prod
bun install --cwd apps/ex_storage_service_web
mix assets.deploy
mix compile
E2E_GITHUB_ENV=/tmp/ex_storage_service/e2e.env mix run --no-start e2e/scripts/seed_e2e.exs
set -a
. /tmp/ex_storage_service/e2e.env
set +a

mix phx.server
```

In a second shell:

```bash
python3 -m venv /tmp/ex_storage_service/e2e-venv
. /tmp/ex_storage_service/e2e-venv/bin/activate
pip install -r e2e/requirements.txt
python e2e/s3_compat.py --phase exercise --state-file /tmp/ex_storage_service/e2e-state.json
```

Restart the app with the same `ESS_DATA_ROOT`, then run:

```bash
python e2e/s3_compat.py --phase verify-persistence --state-file /tmp/ex_storage_service/e2e-state.json
```

The first multipart part in this production-mode exercise is at least 5 MiB,
matching the S3 minimum for every non-final part.

## Cloud-cache run with RustFS

The cloud-cache harness checks uploads, downloads, copies, moves, and directory
operations through ESS and verifies the results directly on RustFS. Install
Docker and the MinIO Client (`mc`), then prepare the ESS environment, assets,
and signed-S3 credentials using **Local Run**, stopping before `mix phx.server`.
Configure the test upstream in the same shell:

```bash
export RUSTFS_ENDPOINT=http://localhost:9100
export RUSTFS_ACCESS_KEY=rustfsadmin
export RUSTFS_SECRET_KEY=rustfsadmin
export RUSTFS_BUCKET=upstream-e2e
```

These values are also the defaults for the seed script and cloud-cache harness.
The seed uses the generic `:s3_compat` cloud provider.

Start RustFS 1.0.1 with an isolated container volume and wait for its health
endpoint:

```bash
docker run -d --name rustfs-server \
  -p 127.0.0.1:9100:9000 \
  -e RUSTFS_ACCESS_KEY \
  -e RUSTFS_SECRET_KEY \
  -e RUSTFS_CONSOLE_ENABLE=false \
  rustfs/rustfs:1.0.1

rustfs_ready=false
for attempt in $(seq 1 30); do
  echo "Waiting for RustFS readiness (${attempt}/30)"
  if curl -fsS --max-time 2 "${RUSTFS_ENDPOINT}/health" > /dev/null 2>&1; then
    rustfs_ready=true
    break
  fi
  sleep 2
done
if [ "$rustfs_ready" != true ]; then
  docker logs rustfs-server
  echo "RustFS readiness timed out" >&2
  exit 1
fi
```

After RustFS is ready, create the upstream bucket and seed cloud-cache settings
before starting ESS:

```bash
mc alias set rustfs-upstream "$RUSTFS_ENDPOINT" "$RUSTFS_ACCESS_KEY" "$RUSTFS_SECRET_KEY" --api S3v4
mc mb --ignore-existing "rustfs-upstream/${RUSTFS_BUCKET}"
PAGER=cat mix run --no-start e2e/scripts/seed_cloud_cache.exs
PAGER=cat mix phx.server
```

In a second shell, export the same `RUSTFS_*` settings and load the signed-S3
credentials generated by **Local Run**:

```bash
set -a
. /tmp/ex_storage_service/e2e.env
set +a
bash e2e/cloud_cache_e2e.sh
```

The local ESS bucket defaults to `cloud-e2e`. To change it, set
`ESS_CLOUD_E2E_BUCKET` for seeding and the same value in `LOCAL_BUCKET` for the
shell harness. The Boto3 standalone compatibility harness continues to target
ESS; RustFS supplies the cloud-cache upstream.

When finished, remove the test container and its volume:

```bash
docker rm -fv rustfs-server
```

## Three-node active-active run

The cluster harness starts three distributed Erlang OS processes with isolated
data, legacy Ra, Concord, blob, and temporary roots:

- metadata-only voter `meta`;
- S3/data node `data-a` on `http://127.0.0.1:9001`;
- S3/data node `data-b` on `http://127.0.0.1:9002`.

It uses strict `RF=2/W=2`. Losing one data node preserves reads from the
survivor, but readiness and new writes must return HTTP 503 until both data
nodes are healthy again. The metadata voter is first in the ordered VSR member
list so the failure phase can confirm and stop the actual primary.

Prepare the source tree and Python environment from the repository root:

```bash
export MIX_ENV=prod
export ESS_CLUSTER_E2E_COOKIE=ess_cluster_e2e_cookie
export ESS_CLUSTER_E2E_MASTER_KEY=cluster-e2e-master-key
export E2E_S3_ENDPOINTS=http://127.0.0.1:9001,http://127.0.0.1:9002
export E2E_STATE_FILE=/tmp/ex_storage_service/cluster-e2e-state.json
export AWS_DEFAULT_REGION=us-east-1

PAGER=cat mix deps.get --only prod
PAGER=cat mix compile --warnings-as-errors
python3 -m venv /tmp/ex_storage_service/cluster-e2e-venv
. /tmp/ex_storage_service/cluster-e2e-venv/bin/activate
pip install -r e2e/requirements.txt
chmod +x e2e/cluster_harness.sh
```

Start the first empty cluster, wait for metadata and data-plane readiness, and
seed a signed-S3 credential through the running Concord quorum:

```bash
e2e/cluster_harness.sh start
trap 'e2e/cluster_harness.sh stop-all' EXIT
e2e/cluster_harness.sh wait-ready

ESS_S3_AUTH_ENABLED=true \
ESS_PUBLIC_S3_ENABLED=false \
ESS_WEB_ENABLED=false \
ESS_MASTER_KEY="$ESS_CLUSTER_E2E_MASTER_KEY" \
E2E_GITHUB_ENV=/tmp/ex_storage_service/cluster-e2e.env \
elixir --name ess_controller@127.0.0.1 \
  --cookie "$ESS_CLUSTER_E2E_COOKIE" \
  -S mix run --no-start e2e/scripts/seed_cluster_e2e.exs

set -a
. /tmp/ex_storage_service/cluster-e2e.env
set +a
```

Run the active-active, failure, recovery, and persistence phases in order:

```bash
python3 e2e/cluster_e2e.py --phase exercise --state-file "$E2E_STATE_FILE"

e2e/cluster_harness.sh stop-data-b
python3 e2e/cluster_e2e.py --phase data-node-failure --state-file "$E2E_STATE_FILE"

e2e/cluster_harness.sh restart-data-b
e2e/cluster_harness.sh wait-ready
python3 e2e/cluster_e2e.py --phase data-node-recovered --state-file "$E2E_STATE_FILE"

ESS_S3_AUTH_ENABLED=true \
ESS_PUBLIC_S3_ENABLED=false \
ESS_WEB_ENABLED=false \
ESS_MASTER_KEY="$ESS_CLUSTER_E2E_MASTER_KEY" \
elixir --name ess_primary_check@127.0.0.1 \
  --cookie "$ESS_CLUSTER_E2E_COOKIE" \
  -S mix run --no-start e2e/scripts/assert_cluster_primary.exs

primary_state="${ESS_CLUSTER_E2E_ROOT:-/tmp/ex_storage_service/cluster-e2e}/primary-loss.json"
primary="$(python3 -c 'import json,sys; p=json.load(open(sys.argv[1]))["before"]["primary"]; assert p in ("meta","data-a","data-b"); print(p)' "$primary_state")"
e2e/cluster_harness.sh stop-voter "$primary"
ESS_PUBLIC_S3_ENABLED=false ESS_WEB_ENABLED=false ESS_MASTER_KEY="$ESS_CLUSTER_E2E_MASTER_KEY" \
  elixir --name ess_election_check@127.0.0.1 --cookie "$ESS_CLUSTER_E2E_COOKIE" \
  -S mix run --no-start e2e/scripts/assert_cluster_primary.exs elected
python3 e2e/cluster_e2e.py --phase metadata-leader-failure --state-file "$E2E_STATE_FILE" \
  --primary-state-file "$primary_state"

e2e/cluster_harness.sh restart-voter "$primary"
e2e/cluster_harness.sh wait-ready
ESS_S3_AUTH_ENABLED=true \
ESS_PUBLIC_S3_ENABLED=false \
ESS_WEB_ENABLED=false \
ESS_MASTER_KEY="$ESS_CLUSTER_E2E_MASTER_KEY" \
elixir --name ess_rejoin_check@127.0.0.1 \
  --cookie "$ESS_CLUSTER_E2E_COOKIE" \
  -S mix run --no-start e2e/scripts/assert_cluster_rejoined.exs

python3 e2e/cluster_e2e.py --phase verify-persistence --state-file "$E2E_STATE_FILE"
```

The harness removes its root only on `start`; every restart uses the same
node-local state with `ESS_CLUSTER_BOOTSTRAP=false`. Per-node lifecycle logs
append under `/tmp/ex_storage_service/cluster-e2e/logs/`.

The primary-loss sequence records the actual converged consensus primary rather
than assuming the metadata-only voter still leads after unrelated restarts. It
requires two surviving voters to agree on a new primary, a higher view, unchanged
configuration and nondecreasing applied commit, with the stopped voter unreachable.
A metadata-only primary outage retains both data replicas and strict writes;
a data-primary outage retains surviving reads but requires HTTP 503 for strict
W=2 writes. Restarting the same voter uses retained roots and bootstrap=false;
all three voters must converge before both data endpoints prove persistence and
restored strict writes. `primary-loss.json` retains pre-loss, election and all-voter
rejoin evidence. The historical Python phase name `metadata-leader-failure` now
performs this role-aware actual-primary check.

Resource acceptance uses a detached BEAM Port owner and a stdlib Python observer.
The runner pins `E2E_RESOURCE_PYTHON` to its absolute interpreter path; this is
acceptance-only configuration. The helper inherits the BEAM PID/mount namespaces,
including the private 8 MiB ENOSPC tmpfs, and reads RSS for the explicit
`System.pid()` target. Its own RSS and CPU usage are reported separately.

Python owns the anchored 25 ms schedule. One generation/sequence request is in
flight; BEAM returns fresh memory/queue values with their original native probe
windows. Python performs fresh filesystem probes and synchronous JSONL export.
Collection windows use only Python clocks. Never compare BEAM and Python
monotonic origins. Slow IPC, scanning, and export remain failures of the unchanged
100 ms per-metric validator; missed slots are skipped without catch-up samples.
Protocol frames are capped at 4096 bytes before decoding, each JSONL row at
16384 bytes, the sample file at 1 GiB, and a helper at 432000 rows/three hours.
There is no exporter thread or mailbox. Owner EOF stops the helper; harness
cleanup additionally verifies the recorded OS PID/start-time identity stopped
and retains `observer-stop-verification.json` (including an observed zombie
state when the external reaper has not yet removed an exited process).

Focused observer checks run without starting ESS applications:

```bash
PAGER=cat /absolute/pinned/python -B -m unittest discover -s e2e -p 'test_resource_sampler.py' -v
PAGER=cat /absolute/pinned/python -B -m unittest discover -s e2e -p 'test_resource_observer.py' -v
```
