#!/usr/bin/env bash
# Run from the repository root after a strict production compile.
set -euo pipefail
export PAGER=cat
export ERL_FLAGS="${ERL_FLAGS:-+S 4:4 +SDcpu 2 +SDio 2}"
python="${1:?usage: run_hardening_phase1_cluster.sh /path/to/pinned/python run-label}"
label="${2:?a unique log label is required}"
scope="${3:-all}"
[[ "$label" =~ ^[a-zA-Z0-9_-]+$ ]] || exit 64
[[ "$scope" == all || "$scope" == features ]] || exit 64
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
  # Preserve the original failure and retain logs even if teardown itself fails.
  bash e2e/cluster_harness.sh stop-all || status=1
  for id in meta data-a data-b; do
    if [[ -f "$ESS_CLUSTER_E2E_ROOT/logs/$id.log" ]]; then
      cp "$ESS_CLUSTER_E2E_ROOT/logs/$id.log" "$logs/$label-$id.log"
    fi
  done
  if [[ -f "$ESS_CLUSTER_E2E_ROOT/primary-loss.json" ]]; then
    cp "$ESS_CLUSTER_E2E_ROOT/primary-loss.json" "$logs/$label-primary-loss.json"
  fi
  echo "Disposable roots retained: $ESS_CLUSTER_E2E_ROOT; exit $status"
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
MIX_ENV=prod elixir --name "ess_hardening_seed_$$@127.0.0.1" \
  --cookie ess_cluster_e2e_cookie -S mix run --no-start --no-compile \
  e2e/scripts/seed_cluster_e2e.exs
set -a
source "$E2E_GITHUB_ENV"
set +a
export E2E_S3_ENDPOINTS=http://127.0.0.1:9001,http://127.0.0.1:9002
# Collect independent profiles after failures without waiving their exit status.
# Setup and dependent restart/failure sequences remain subject to set -e.
profile_status=0
run_profile() {
  local profile="$1"
  shift
  if "$python" "$profile" "$@"; then
    echo "PROFILE $profile passed"
  else
    failure=$?
    echo "PROFILE $profile failed exit $failure" >&2
    profile_status=1
  fi
}
if [[ "$scope" == all ]]; then
  run_profile e2e/s3_hardening_phase1.py
fi
run_profile e2e/cluster_iam_hardening.py --scope "$scope"

if [[ "$scope" == features || "${ESS_HARDENING_PHASE2:-false}" == true ]]; then
  run_profile e2e/s3_hardening_versions_lists.py
  run_profile e2e/s3_hardening_tag_cors.py
  run_profile e2e/s3_hardening_multipart.py --scope "$scope"
  if [[ "$scope" == all ]]; then
    run_profile e2e/s3_hardening_multipart_races.py
  fi
  run_profile e2e/s3_hardening_clients.py
  if [[ "$scope" == features ]]; then
    exit "$profile_status"
  fi
  restart_state="$ESS_CLUSTER_E2E_ROOT/multipart-restart-state.json"
  "$python" e2e/s3_hardening_multipart_restart.py prepare --state "$restart_state"
  cp "$restart_state" "$logs/$label-multipart-restart-state.json"
  previous_pid="$(<"$ESS_CLUSTER_E2E_ROOT/pids/data-b")"
  [[ "$previous_pid" =~ ^[0-9]+$ ]] && kill -0 "$previous_pid"
  cp "$ESS_CLUSTER_E2E_ROOT/logs/data-b.log" "$logs/$label-data-b-before-multipart-restart.log"
  # Existing restart preserves roots and sets bootstrap=false; never call start here.
  bash e2e/cluster_harness.sh restart-data-b
  current_pid="$(<"$ESS_CLUSTER_E2E_ROOT/pids/data-b")"
  [[ "$current_pid" =~ ^[0-9]+$ && "$current_pid" != "$previous_pid" ]]
  kill -0 "$current_pid"
  [[ -f "$restart_state" && -d "$ESS_CLUSTER_E2E_ROOT/data-b/metadata" ]]
  bash e2e/cluster_harness.sh wait-ready
  curl -fsS http://127.0.0.1:9001/health/ready >/dev/null
  curl -fsS http://127.0.0.1:9002/health/ready >/dev/null
  printf 'Retained-root data-b restart: previous BEAM PID %s; current BEAM PID %s; both data endpoints ready\n' \
    "$previous_pid" "$current_pid" > "$logs/$label-multipart-restart-lifecycle.log"
  "$python" e2e/s3_hardening_multipart_restart.py verify --state "$restart_state"
fi

if [[ "${ESS_HARDENING_FAILURES:-false}" == true ]]; then
  state="$ESS_CLUSTER_E2E_ROOT/active-active-state.json"
  "$python" e2e/cluster_e2e.py --phase exercise --state-file "$state"
  bash e2e/cluster_harness.sh stop-data-b
  "$python" e2e/cluster_e2e.py --phase data-node-failure --state-file "$state"
  bash e2e/cluster_harness.sh restart-data-b
  bash e2e/cluster_harness.sh wait-ready
  "$python" e2e/cluster_e2e.py --phase data-node-recovered --state-file "$state"
  # Record the actual converged primary; restarts may legitimately advance views.
  MIX_ENV=prod elixir --name "ess_primary_check_$$@127.0.0.1" \
    --cookie ess_cluster_e2e_cookie -S mix run --no-start --no-compile \
    e2e/scripts/assert_cluster_primary.exs
  primary="$("$python" -c 'import json,sys; p=json.load(open(sys.argv[1]))["before"]["primary"]; assert p in ("meta","data-a","data-b"); print(p)' "$ESS_CLUSTER_E2E_ROOT/primary-loss.json")"
  bash e2e/cluster_harness.sh stop-voter "$primary"
  MIX_ENV=prod elixir --name "ess_election_check_$$@127.0.0.1" \
    --cookie ess_cluster_e2e_cookie -S mix run --no-start --no-compile \
    e2e/scripts/assert_cluster_primary.exs elected
  "$python" e2e/cluster_e2e.py --phase metadata-leader-failure --state-file "$state" \
    --primary-state-file "$ESS_CLUSTER_E2E_ROOT/primary-loss.json"
  bash e2e/cluster_harness.sh restart-voter "$primary"
  bash e2e/cluster_harness.sh wait-ready
  MIX_ENV=prod elixir --name "ess_rejoin_check_$$@127.0.0.1" \
    --cookie ess_cluster_e2e_cookie -S mix run --no-start --no-compile \
    e2e/scripts/assert_cluster_rejoined.exs
  "$python" e2e/cluster_e2e.py --phase verify-persistence --state-file "$state"
  cp "$state" "$logs/$label-active-active-state.json"
fi

exit "$profile_status"
