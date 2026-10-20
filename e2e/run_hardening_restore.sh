#!/usr/bin/env bash
# A36/A37 are real isolated restores, never a mock or a successful skip.
set -euo pipefail
export PAGER=cat
python="${1:?usage: run_hardening_restore.sh /absolute/pinned/python evidence-label [standalone|cluster|all]}"
label="${2:?unique evidence label required}"
profile="${3:-all}"
[[ "$python" == /* && "$label" =~ ^[A-Za-z0-9_-]+$ ]] || exit 64
case "$profile" in standalone|cluster) modes=("$profile");; all) modes=(standalone cluster);; *) exit 64;; esac
for executable in bwrap elixir mix; do command -v "$executable" >/dev/null; done
owned_pid=""
cleanup() {
  status=$?
  if [[ -n "$owned_pid" ]]; then
    kill -TERM "$owned_pid" 2>/dev/null || true
    wait "$owned_pid" 2>/dev/null || true
  fi
  exit "$status"
}
trap cleanup EXIT
trap 'exit 130' INT
trap 'exit 143' TERM
# A namespace prerequisite failure must be reported as a failed/blocked lane.
bwrap --unshare-user --unshare-pid --die-with-parent --ro-bind / / --proc /proc --dev /dev -- true
"$python" -c 'import boto3, botocore; print("restore clients: boto3=" + boto3.__version__ + " botocore=" + botocore.__version__)'
for mode in "${modes[@]}"; do
  root="$(mktemp -d /tmp/ess-hardening-restore-XXXXXXXX)"
  evidence="$PWD/docs/reports/s3-hardening/$label-$mode"
  [[ ! -e "$evidence" ]] || { echo 'Evidence label already exists; choose a fresh label.' >&2; exit 1; }
  mkdir -m 0700 -p "$evidence"
  "$python" e2e/s3_hardening_restore.py source --mode "$mode" --root "$root" --evidence "$evidence" --python "$python" &
  owned_pid=$!
  wait "$owned_pid"
  owned_pid=""
  printf 'Disposable restore root retained: %s\n' "$root"
done
