#!/usr/bin/env bash
# Fresh private RF2/W2 three-voter fixture; never shares another lane's roots.
set -euo pipefail
export PAGER=cat
export ERL_FLAGS="${ERL_FLAGS:-+S 4:4 +SDcpu 2 +SDio 2}"
python="${1:?absolute pinned Python required}"
label="${2:?unique evidence label required}"
[[ "$python" == /* && -x "$python" && "$label" =~ ^[A-Za-z0-9_-]+$ ]] || exit 64
export E2E_RESOURCE_PYTHON="$python"
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
  cleanup_failed=0
  trap - EXIT
  bash e2e/cluster_harness.sh stop-all || cleanup_failed=1
  for id in meta data-a data-b; do
    if [[ -d "$ESS_CLUSTER_E2E_ROOT/resource-samples/$id" ]]; then
      "$python" -B e2e/scripts/resource_observer.py verify-stopped \
        --root "$ESS_CLUSTER_E2E_ROOT/resource-samples/$id" || cleanup_failed=1
    fi
    if [[ -f "$ESS_CLUSTER_E2E_ROOT/logs/$id.log" ]]; then
      cp "$ESS_CLUSTER_E2E_ROOT/logs/$id.log" "$logs/$label-$id.log" || cleanup_failed=1
    fi
  done
  if [[ "$status" -eq 0 && "$cleanup_failed" -ne 0 ]]; then
    status=1
  fi
  echo "Disposable cluster resource roots retained: $ESS_CLUSTER_E2E_ROOT; exit $status"
  exit "$status"
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
MIX_ENV=prod elixir --name "ess_resource_seed_$$@127.0.0.1" \
  --cookie ess_cluster_e2e_cookie -S mix run --no-start --no-compile e2e/scripts/seed_cluster_e2e.exs
set -a
source "$E2E_GITHUB_ENV"
set +a
"$python" e2e/s3_hardening_cluster_resources.py --evidence "$logs/$label"
