# End-to-End Tests

This directory contains black-box S3 compatibility checks for ExStorageService.
The tests use Boto3 against the running HTTP API, with path-style addressing and
AWS Signature V4 enabled, so they exercise the same client surface used by S3
tools that normally talk to MinIO.

## Local Run

```bash
export MIX_ENV=prod
export ESS_S3_AUTH_ENABLED=true
export ESS_S3_PORT=9000
export ESS_ADMIN_PORT=4900
export ESS_DATA_ROOT=/tmp/ex_storage_service/e2e-data
export ESS_MASTER_KEY=test-master-key-for-local-e2e
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

e2e/cluster_harness.sh stop-meta
e2e/cluster_harness.sh wait-ready
python3 e2e/cluster_e2e.py --phase metadata-leader-failure --state-file "$E2E_STATE_FILE"

e2e/cluster_harness.sh restart-meta
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
