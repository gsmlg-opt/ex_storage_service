#!/usr/bin/env bash
# A29 actual interrupted rebuild/rejoin; independent of the A28 scale command.
set -euo pipefail
export PAGER=cat
export ERL_FLAGS="${ERL_FLAGS:-+S 4:4 +SDcpu 2 +SDio 2}"
python="${1:?absolute pinned Python required}"
label="${2:?unique evidence label required}"
[[ "$python" = /* && -x "$python" && "$label" =~ ^[A-Za-z0-9_-]+$ ]] || exit 64
# This is a real native backfill lane, not a workaround for its known blocker.
protocol_source=deps/viewstamped_replication/lib/viewstamped_replication/protocol.ex
protocol_status=0
rg -q 'Enum.each\(1\.\.previous.commit_number' "$protocol_source" || protocol_status=$?
case "$protocol_status" in
  0)
    echo 'BLOCKED A29: gsmlg-dev/concord#94; released coordinated fix required before native backfill/rejoin.' >&2
    exit 1
    ;;
  1) ;;
  *)
    echo 'Cannot inspect released VSR protocol source; refusing unverified index workload.' >&2
    exit 1
    ;;
esac
if epmd -names | rg -q 'name ess_(meta|data_a|data_b) '; then
  echo 'Disposable cluster node names are already in use; refusing to interfere.' >&2
  exit 1
fi
logs="docs/reports/s3-hardening/$label"
[[ ! -e "$logs" ]] || exit 1
mkdir -p "$logs"
export ESS_CLUSTER_E2E_ROOT
ESS_CLUSTER_E2E_ROOT="$(mktemp -d /tmp/ess-s3-hardening-cluster-XXXXXXXX)"
chmod 700 "$ESS_CLUSTER_E2E_ROOT"
cleanup() {
  status=$?
  bash e2e/cluster_harness.sh stop-all || status=1
  for id in meta data-a data-b; do
    if [[ -f "$ESS_CLUSTER_E2E_ROOT/logs/$id.log" ]]; then
      cp "$ESS_CLUSTER_E2E_ROOT/logs/$id.log" "$logs/$id.log"
    fi
  done
  rm -f "$ESS_CLUSTER_E2E_ROOT/credentials.env"
  echo "Disposable index roots retained: $ESS_CLUSTER_E2E_ROOT; exit $status"
  return "$status"
}
trap cleanup EXIT
trap 'exit 130' INT
trap 'exit 143' TERM
bash e2e/cluster_harness.sh start
chmod 700 "$ESS_CLUSTER_E2E_ROOT"
bash e2e/cluster_harness.sh wait-ready
export E2E_GITHUB_ENV="$ESS_CLUSTER_E2E_ROOT/credentials.env"
export ESS_MASTER_KEY=cluster-e2e-master-key
export ESS_WEB_ENABLED=false ESS_PUBLIC_S3_ENABLED=false
MIX_ENV=prod elixir --name "ess_index_seed_$$@127.0.0.1" \
  --cookie ess_cluster_e2e_cookie -S mix run --no-start --no-compile \
  e2e/scripts/seed_cluster_e2e.exs
set -a
source "$E2E_GITHUB_ENV"
set +a
export E2E_S3_ENDPOINTS=http://127.0.0.1:9001,http://127.0.0.1:9002
state="$ESS_CLUSTER_E2E_ROOT/index-state.json"
"$python" e2e/s3_hardening_index_cluster.py seed --state "$state" --evidence "$logs/results.json"
"$python" e2e/s3_hardening_index_cluster.py prepare --state "$state" --evidence "$logs/results.json"
bash e2e/cluster_harness.sh wait-ready
"$python" e2e/s3_hardening_index_cluster.py gated --state "$state" --evidence "$logs/results.json"
printf 'coordinator interruption: ' >>"$logs/restart-lifecycle.log"
cat "$ESS_CLUSTER_E2E_ROOT/pids/meta" >>"$logs/restart-lifecycle.log"
bash e2e/cluster_harness.sh crash-meta
bash e2e/cluster_harness.sh wait-ready
"$python" e2e/s3_hardening_index_cluster.py gated --state "$state" --evidence "$logs/results.json"
bash e2e/cluster_harness.sh restart-meta
bash e2e/cluster_harness.sh wait-ready
printf 'retained-root bootstrap=false restart: ' >>"$logs/restart-lifecycle.log"
cat "$ESS_CLUSTER_E2E_ROOT/pids/meta" >>"$logs/restart-lifecycle.log"
"$python" e2e/s3_hardening_index_cluster.py rejoin --state "$state" --evidence "$logs/results.json"
"$python" e2e/s3_hardening_index_cluster.py finish --state "$state" --evidence "$logs/results.json"
