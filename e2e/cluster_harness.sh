#!/usr/bin/env bash
set -euo pipefail

root="${ESS_CLUSTER_E2E_ROOT:-/tmp/ex_storage_service/cluster-e2e}"
cookie="${ESS_CLUSTER_E2E_COOKIE:-ess_cluster_e2e_cookie}"
members="meta=ess_meta@127.0.0.1,data-a=ess_data_a@127.0.0.1,data-b=ess_data_b@127.0.0.1"
internal_secret="${ESS_CLUSTER_E2E_INTERNAL_SECRET:-cluster-e2e-internal-secret-at-least-32-bytes}"
master_key="${ESS_CLUSTER_E2E_MASTER_KEY:-cluster-e2e-master-key}"

mkdir -p "$root/logs" "$root/pids"

node_name() {
  case "$1" in
    meta) echo "ess_meta@127.0.0.1" ;;
    data-a) echo "ess_data_a@127.0.0.1" ;;
    data-b) echo "ess_data_b@127.0.0.1" ;;
    *) return 1 ;;
  esac
}

node_role() {
  if [[ "$1" == "meta" ]]; then echo "metadata"; else echo "data"; fi
}

s3_port() {
  case "$1" in
    data-a) echo "9001" ;;
    data-b) echo "9002" ;;
    meta) echo "9003" ;;
  esac
}

internal_port() {
  case "$1" in
    data-a) echo "9101" ;;
    data-b) echo "9102" ;;
    meta) echo "9103" ;;
  esac
}

start_node() {
  local id="$1"
  local bootstrap="$2"
  local role
  local public_s3
  local cluster_data_plane
  local repair_enabled
  role="$(node_role "$id")"

  if [[ "$role" == "data" ]]; then
    public_s3=true
    cluster_data_plane=true
    repair_enabled=true
  else
    public_s3=false
    cluster_data_plane=false
    repair_enabled=false
  fi

  mkdir -p "$root/$id/blob" "$root/$id/tmp" "$root/$id/metadata" "$root/$id/ra"
  printf '\n===== start %s bootstrap=%s at %s =====\n' \
    "$id" "$bootstrap" "$(date -u +%Y-%m-%dT%H:%M:%SZ)" >>"$root/logs/$id.log"

  env \
    MIX_ENV=prod \
    ESS_MODE=cluster \
    ESS_AUTO_START=true \
    ESS_INSTANCE="$id" \
    ESS_NODE_ROLE="$role" \
    ESS_NODE_ID="$id" \
    ESS_NODE_GENERATION=1 \
    ESS_CLUSTER_NAME=ess-cluster-e2e \
    ESS_CLUSTER_TOPOLOGY=static \
    ESS_CLUSTER_MEMBERS="$members" \
    ESS_CLUSTER_BOOTSTRAP="$bootstrap" \
    ESS_DATA_ROOT="$root/$id" \
    ESS_BLOB_ROOT="$root/$id/blob" \
    ESS_TMP_ROOT="$root/$id/tmp" \
    ESS_RA_ROOT="$root/$id/ra" \
    ESS_METADATA_ROOT="$root/$id/metadata" \
    ESS_WEB_ENABLED=false \
    ESS_PUBLIC_S3_ENABLED="$public_s3" \
    ESS_CLUSTER_DATA_PLANE_ENABLED="$cluster_data_plane" \
    ESS_S3_AUTH_ENABLED=true \
    ESS_S3_PORT="$(s3_port "$id")" \
    ESS_INTERNAL_BIND=127.0.0.1 \
    ESS_INTERNAL_PORT="$(internal_port "$id")" \
    ESS_INTERNAL_ADVERTISED_URL="http://127.0.0.1:$(internal_port "$id")" \
    ESS_INTERNAL_SECRET="$internal_secret" \
    ESS_REPLICATION_FACTOR=2 \
    ESS_WRITE_QUORUM=2 \
    ESS_REPAIR_ENABLED="$repair_enabled" \
    ESS_SCRUB_ENABLED=false \
    ESS_MASTER_KEY="$master_key" \
    elixir --name "$(node_name "$id")" --cookie "$cookie" -S mix phx.server \
    >>"$root/logs/$id.log" 2>&1 &

  echo "$!" >"$root/pids/$id"
}

stop_node() {
  local id="$1"
  local pid_file="$root/pids/$id"

  if [[ -f "$pid_file" ]]; then
    local pid
    pid="$(<"$pid_file")"
    kill "$pid" 2>/dev/null || true
    for _attempt in $(seq 1 30); do
      if ! kill -0 "$pid" 2>/dev/null; then break; fi
      sleep 0.2
    done
    kill -9 "$pid" 2>/dev/null || true
    rm -f "$pid_file"
  fi
}

wait_http() {
  local url="$1"
  local expected="${2:-200}"

  for _attempt in $(seq 1 90); do
    local status
    status="$(curl -sS -o /dev/null -w '%{http_code}' "$url" || true)"
    if [[ "$status" == "$expected" ]]; then return 0; fi
    sleep 1
  done

  echo "timed out waiting for $url (expected HTTP $expected)" >&2
  return 1
}

case "${1:-}" in
  start)
    rm -rf "$root"
    mkdir -p "$root/logs" "$root/pids"
    start_node meta true
    start_node data-a true
    start_node data-b true
    wait_http http://127.0.0.1:9001/health
    wait_http http://127.0.0.1:9002/health
    ;;
  stop-all)
    stop_node data-b
    stop_node data-a
    stop_node meta
    ;;
  stop-data-b)
    stop_node data-b
    ;;
  restart-data-b)
    stop_node data-b
    start_node data-b false
    wait_http http://127.0.0.1:9002/health
    ;;
  stop-meta)
    stop_node meta
    ;;
  restart-meta)
    stop_node meta
    start_node meta false
    ;;
  wait-ready)
    wait_http http://127.0.0.1:9001/health/ready
    wait_http http://127.0.0.1:9002/health/ready
    ;;
  *)
    echo "usage: $0 {start|stop-all|stop-data-b|restart-data-b|stop-meta|restart-meta|wait-ready}" >&2
    exit 64
    ;;
esac
