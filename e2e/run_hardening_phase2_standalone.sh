#!/usr/bin/env bash
# Strictly disposable current-production-build acceptance; run from repo root.
set -euo pipefail
export PAGER=cat
export ERL_FLAGS="${ERL_FLAGS:-+S 4:4 +SDcpu 2 +SDio 2}"
python="${1:?pinned python required}"
label="${2:?unique evidence label required}"
scope="${3:-all}"
[[ "$label" =~ ^[a-zA-Z0-9_-]+$ ]] || exit 64
[[ "$scope" == all || "$scope" == features ]] || exit 64
export ESS_DATA_ROOT
ESS_DATA_ROOT="$(mktemp -d /tmp/ess-s3-hardening-standalone-XXXXXXXX)"
export ESS_MODE=standalone ESS_AUTO_START=true ESS_WEB_ENABLED=false ESS_S3_AUTH_ENABLED=true
export ESS_BLOB_ROOT="$ESS_DATA_ROOT/cas" ESS_TMP_ROOT="$ESS_DATA_ROOT/tmp"
export ESS_METADATA_ROOT="$ESS_DATA_ROOT/metadata" ESS_RA_ROOT="$ESS_DATA_ROOT/ra"
export ESS_MASTER_KEY=hardening-disposable-standalone-key
export E2E_GITHUB_ENV="$ESS_DATA_ROOT/credentials.env"
export ESS_S3_PORT
ESS_S3_PORT="$("$python" -c 'import socket; s=socket.socket(); s.bind(("127.0.0.1",0)); print(s.getsockname()[1]); s.close()')"
export E2E_S3_ENDPOINTS="http://127.0.0.1:$ESS_S3_PORT"
logs=docs/reports/s3-hardening
mkdir -p "$logs"
pid=""
cleanup() {
  status=$?
  if [[ -n "$pid" ]]; then
    kill -TERM "$pid" 2>/dev/null || true
    wait "$pid" 2>/dev/null || true
  fi
  rm -f "$E2E_GITHUB_ENV"
  echo "Disposable standalone root $ESS_DATA_ROOT; exit $status"
}
trap cleanup EXIT
start_server() {
  server_log="$1"
  shift
  MIX_ENV=prod mix run --no-compile --no-halt "$@" > "$server_log" 2>&1 &
  pid=$!
  for attempt in $(seq 1 100); do
    if [[ -s "$E2E_GITHUB_ENV" ]] && curl -fsS "$E2E_S3_ENDPOINTS/health" >/dev/null 2>&1; then break; fi
    kill -0 "$pid" || exit 1
    sleep 0.2
  done
  [[ -s "$E2E_GITHUB_ENV" ]] || exit 1
  curl -fsS "$E2E_S3_ENDPOINTS/health" >/dev/null
}
start_server "$logs/$label-server.log" e2e/scripts/seed_hardening.exs
set -a
source "$E2E_GITHUB_ENV"
set +a
# Retain every independent profile even when an earlier mandatory profile fails.
# The runner still returns failure; no failing assertion is waived.
profile_status=0
profiles=(phase1 versions_lists tag_cors multipart multipart_races clients)
if [[ "$scope" == features ]]; then
  profiles=(versions_lists tag_cors multipart clients)
fi
for profile in "${profiles[@]}"; do
  extra=()
  if [[ "$profile" == multipart ]]; then
    extra=(--scope "$scope")
  fi
  if "$python" "e2e/s3_hardening_$profile.py" "${extra[@]}"; then
    echo "PROFILE $profile passed"
  else
    failure=$?
    echo "PROFILE $profile failed exit $failure" >&2
    profile_status=1
  fi
done
if [[ "$scope" == features ]]; then
  exit "$profile_status"
fi
restart_state="$ESS_DATA_ROOT/multipart-restart-state.json"
"$python" e2e/s3_hardening_multipart_restart.py prepare --state "$restart_state"
previous_pid="$pid"
kill -KILL "$pid"
wait "$pid" 2>/dev/null || true
pid=""
# Same roots, master key, credentials and port; do not create a new IAM identity.
start_server "$logs/$label-restarted-server.log"
[[ "$pid" != "$previous_pid" ]] || exit 1
printf 'Stopped BEAM PID %s with SIGKILL; restarted BEAM PID %s with retained roots\n' \
  "$previous_pid" "$pid" > "$logs/$label-restart-lifecycle.log"
"$python" e2e/s3_hardening_multipart_restart.py verify --state "$restart_state"
cp "$restart_state" "$logs/$label-multipart-restart-state.json"

exit "$profile_status"
