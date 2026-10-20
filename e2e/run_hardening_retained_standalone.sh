#!/usr/bin/env bash
# Preserve the original signed SDK/admin/persistence lane in an isolated runtime.
set -euo pipefail
umask 077
export PAGER=cat
export ERL_FLAGS="${ERL_FLAGS:-+S 4:4 +SDcpu 2 +SDio 2}"
python="${1:?absolute pinned python required}"
label="${2:?unique evidence label required}"
[[ "$python" == /* && "$label" =~ ^[A-Za-z0-9_-]+$ ]] || exit 64
command -v setsid >/dev/null
logs="docs/reports/s3-hardening/$label"
[[ ! -e "$logs" ]] || exit 1
mkdir -p "$logs"
export ESS_DATA_ROOT
ESS_DATA_ROOT="$(mktemp -d /tmp/ess-hardening-retained-XXXXXXXX)"
export MIX_ENV=prod ESS_MODE=standalone ESS_AUTO_START=true ESS_WEB_ENABLED=true
export ESS_S3_AUTH_ENABLED=true ESS_PUBLIC_S3_ENABLED=true
export ESS_BLOB_ROOT="$ESS_DATA_ROOT/cas" ESS_TMP_ROOT="$ESS_DATA_ROOT/tmp"
export ESS_METADATA_ROOT="$ESS_DATA_ROOT/metadata" ESS_RA_ROOT="$ESS_DATA_ROOT/ra"
export ESS_MASTER_KEY=hardening-disposable-retained-key
export ESS_ADMIN_USER=admin
export ESS_ADMIN_PASSWORD_HASH=58183f72d80da07a4a90f521f3720f80990e002245a0f15cd9e5321bcd34125d
export SECRET_KEY_BASE=disposable-retained-standalone-secret-key-base-at-least-sixty-four-bytes-123456789
export E2E_GITHUB_ENV="$ESS_DATA_ROOT/credentials.env"
export E2E_STATE_FILE="$ESS_DATA_ROOT/persistence-state.json"
read -r ESS_S3_PORT ESS_ADMIN_PORT < <("$python" - <<'PY'
import socket
with socket.socket() as first, socket.socket() as second:
    first.bind(("127.0.0.1", 0))
    second.bind(("127.0.0.1", 0))
    print(first.getsockname()[1], second.getsockname()[1])
PY
)
export ESS_S3_PORT ESS_ADMIN_PORT
export E2E_S3_ENDPOINT="http://127.0.0.1:$ESS_S3_PORT"
export E2E_ADMIN_ENDPOINT="http://127.0.0.1:$ESS_ADMIN_PORT"
pid=""
stop_owned() {
  if [[ -n "$pid" ]]; then
    kill -TERM -- "-$pid" 2>/dev/null || true
    for attempt in $(seq 1 100); do
      kill -0 "$pid" 2>/dev/null || break
      sleep 0.1
    done
    kill -KILL -- "-$pid" 2>/dev/null || true
    wait "$pid" 2>/dev/null || true
    pid=""
  fi
}
cleanup() {
  status=$?
  trap - EXIT
  stop_owned
  rm -f "$E2E_GITHUB_ENV"
  echo "Retained standalone root $ESS_DATA_ROOT; private runtime logs retained; exit $status"
  exit "$status"
}
trap cleanup EXIT
trap 'exit 130' INT
trap 'exit 143' TERM

# Existing CI builds production assets before exercising the unchanged admin UI.
# Seed output can include GitHub secret-mask commands, so keep it private.
mix assets.deploy > "$ESS_DATA_ROOT/assets.log" 2>&1
mix run --no-start --no-compile e2e/scripts/seed_e2e.exs > "$ESS_DATA_ROOT/seed.log" 2>&1
[[ -s "$E2E_GITHUB_ENV" ]] || exit 1
while IFS='=' read -r name value; do
  case "$name" in
    E2E_ACCESS_KEY_ID|E2E_SECRET_ACCESS_KEY|E2E_READONLY_ACCESS_KEY_ID|E2E_READONLY_SECRET_ACCESS_KEY)
      export "$name=$value" ;;
    *) exit 65 ;;
  esac
done < "$E2E_GITHUB_ENV"

start_server() {
  setsid mix run --no-compile --no-halt > "$ESS_DATA_ROOT/$1.log" 2>&1 &
  pid=$!
  for attempt in $(seq 1 150); do
    kill -0 "$pid" || return 1
    if curl -fsS "$E2E_S3_ENDPOINT/health" >/dev/null 2>&1 &&
       curl -fsS "$E2E_ADMIN_ENDPOINT/" >/dev/null 2>&1; then
      return 0
    fi
    sleep 0.2
  done
  return 1
}

start_server server
"$python" e2e/s3_compat.py --phase exercise --state-file "$E2E_STATE_FILE"
previous_pid="$pid"
kill -KILL -- "-$pid"
wait "$pid" 2>/dev/null || true
pid=""
start_server restarted-server
[[ "$pid" != "$previous_pid" ]] || exit 1
"$python" e2e/s3_compat.py --phase verify-persistence --state-file "$E2E_STATE_FILE"
printf 'SIGKILL of owned BEAM PID %s; restart PID %s; same isolated roots and credentials\n' \
  "$previous_pid" "$pid" > "$logs/restart-lifecycle.log"
"$python" - "$logs/results.json" <<'PY'
import importlib.metadata
import json
import os
from pathlib import Path
import subprocess
import sys
state = json.loads(Path(os.environ["E2E_STATE_FILE"]).read_text())
result = {"profile": "retained-standalone-sdk-admin-persistence", "complete": True,
          "checks": ["unchanged-s3-compat-exercise", "admin-and-metrics", "retained-root-sigkill-restart",
                     "unchanged-s3-compat-persistence-and-cleanup"],
          "boto3": importlib.metadata.version("boto3"),
          "botocore": importlib.metadata.version("botocore"),
          "source_sha": subprocess.check_output(["git", "rev-parse", "HEAD"], text=True).strip(),
          "persistence_sha256": state["sha256"]}
Path(sys.argv[1]).write_text(json.dumps(result, indent=2) + "\n")
print(json.dumps(result, sort_keys=True))
PY
