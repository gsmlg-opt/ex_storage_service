#!/usr/bin/env bash
set -euo pipefail
export PAGER=cat
export ERL_FLAGS="${ERL_FLAGS:-+S 4:4}"
python="${1:?pinned python required}"
label="${2:?unique evidence label required}"
[[ "$python" == /* && "$label" =~ ^[A-Za-z0-9_-]+$ ]] || exit 64
"$python" e2e/s3_hardening_crashes.py --root "/tmp/ess-hardening-crash-$label" --evidence "docs/reports/s3-hardening/$label"
