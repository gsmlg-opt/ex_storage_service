#!/usr/bin/env bash
set -euo pipefail
export PAGER=cat
python_path=${1:?absolute pinned Python path required}
label=${2:?unique evidence label required}
[[ "$python_path" = /* && -x "$python_path" ]]
[[ "$label" =~ ^[a-zA-Z0-9_-]+$ ]]
exec "$python_path" e2e/s3_hardening_faults.py \
  --root "/tmp/ess-hardening-fault-$label" \
  --evidence "docs/reports/s3-hardening/$label"
