#!/usr/bin/env bash
# Real internal transport ack crash; separate roots from the resilience lane.
set -euo pipefail
export PAGER=cat
export ERL_FLAGS="${ERL_FLAGS:-+S 4:4 +SDcpu 2 +SDio 2}"
python="${1:?absolute pinned Python required}"
label="${2:?unique evidence label required}"
[[ "$python" == /* && -x "$python" && "$label" =~ ^[A-Za-z0-9_-]+$ ]] || exit 64
if epmd -names | rg -q 'name ess_(meta|data_a|data_b) '; then
  echo 'Disposable cluster names are in use; refusing to interfere.' >&2
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
  echo "Disposable replication crash roots retained: $ESS_CLUSTER_E2E_ROOT; exit $status"
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
MIX_ENV=prod elixir --name "ess_replication_seed_$$@127.0.0.1" \
  --cookie ess_cluster_e2e_cookie -S mix run --no-start --no-compile e2e/scripts/seed_cluster_e2e.exs
set -a
source "$E2E_GITHUB_ENV"
set +a
"$python" e2e/s3_hardening_replication_crash.py --evidence "$logs/$label"
