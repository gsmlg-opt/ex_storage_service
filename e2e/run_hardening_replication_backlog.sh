#!/usr/bin/env bash
set -euo pipefail
export PAGER=cat
export ERL_FLAGS="${ERL_FLAGS:-+S 4:4 +SDcpu 2 +SDio 2}"
python_path=${1:?absolute pinned Python path required}
label=${2:?unique evidence label required}
[[ "$python_path" = /* && -x "$python_path" ]]
[[ "$label" =~ ^[a-zA-Z0-9_-]+$ ]]
exec "$python_path" e2e/s3_hardening_replication_backlog.py \
  --root "/tmp/ess-hardening-backlog-$label" \
  --evidence "docs/reports/s3-hardening/$label"
