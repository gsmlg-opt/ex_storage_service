#!/usr/bin/env bash
# Small fresh-metadata A32-A35 profile; run after strict production compilation.
set -euo pipefail
export PAGER=cat
export ERL_FLAGS="${ERL_FLAGS:-+S 4:4 +SDcpu 2 +SDio 2}"
python="${1:?usage: run_hardening_resilience.sh /path/to/pinned/python run-label}"
label="${2:?a unique evidence label is required}"
[[ "$label" =~ ^[a-zA-Z0-9_-]+$ ]] || exit 64
if epmd -names | rg -q 'name ess_(meta|data_a|data_b) '; then
  echo 'Disposable cluster node names are already in use; refusing to interfere.' >&2
  exit 1
fi
export ESS_CLUSTER_E2E_ROOT
ESS_CLUSTER_E2E_ROOT="$(mktemp -d /tmp/ess-s3-hardening-cluster-XXXXXXXX)"
logs=docs/reports/s3-hardening
mkdir -p "$logs"
cleanup() {
  status=$?
  bash e2e/cluster_harness.sh stop-all || status=1
  for id in meta data-a data-b; do
    if [[ -f "$ESS_CLUSTER_E2E_ROOT/logs/$id.log" ]]; then
      cp "$ESS_CLUSTER_E2E_ROOT/logs/$id.log" "$logs/$label-$id.log"
    fi
  done
  if [[ -f "$ESS_CLUSTER_E2E_ROOT/resilience-result.json" ]]; then
    cp "$ESS_CLUSTER_E2E_ROOT/resilience-result.json" "$logs/$label-result.json"
  fi
  echo "Disposable resilience roots retained: $ESS_CLUSTER_E2E_ROOT; exit $status"
  return "$status"
}
trap cleanup EXIT
trap 'exit 130' INT
trap 'exit 143' TERM
bash e2e/cluster_harness.sh start
bash e2e/cluster_harness.sh wait-ready
export E2E_GITHUB_ENV="$ESS_CLUSTER_E2E_ROOT/credentials.env"
export ESS_MASTER_KEY=cluster-e2e-master-key
export ESS_WEB_ENABLED=false
export ESS_PUBLIC_S3_ENABLED=false
MIX_ENV=prod elixir --name "ess_resilience_seed_$$@127.0.0.1" \
  --cookie ess_cluster_e2e_cookie -S mix run --no-start --no-compile \
  e2e/scripts/seed_cluster_e2e.exs
set -a
source "$E2E_GITHUB_ENV"
set +a
export E2E_S3_ENDPOINTS=http://127.0.0.1:9001,http://127.0.0.1:9002
state="$ESS_CLUSTER_E2E_ROOT/resilience-state.json"
"$python" e2e/s3_hardening_resilience.py seed --state "$state"
"$python" e2e/s3_hardening_resilience.py read-repair --state "$state"
"$python" e2e/s3_hardening_resilience.py maintenance --state "$state"
bash e2e/cluster_harness.sh crash-data-b
"$python" e2e/s3_hardening_resilience.py outage --state "$state"
bash e2e/cluster_harness.sh restart-data-b
bash e2e/cluster_harness.sh wait-ready
"$python" e2e/s3_hardening_resilience.py recovered --state "$state"
"$python" e2e/s3_hardening_resilience.py record-leader --state "$state"
primary="$("$python" -c 'import json,sys; p=json.load(open(sys.argv[1]))["before_loss"]["primary"]; assert p in ("meta","data-a","data-b"); print(p)' "$state")"
bash e2e/cluster_harness.sh crash-voter "$primary"
"$python" e2e/s3_hardening_resilience.py elected --state "$state"
bash e2e/cluster_harness.sh restart-voter "$primary"
bash e2e/cluster_harness.sh wait-ready
"$python" e2e/s3_hardening_resilience.py verify --state "$state"
