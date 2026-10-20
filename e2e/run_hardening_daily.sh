#!/usr/bin/env bash
# Small, sequential business checks against a newly created local ESS instance.
set -euo pipefail
export PAGER=cat
export ERL_FLAGS="${ERL_FLAGS:-+S 4:4 +SDcpu 2 +SDio 2}"
python="${1:?absolute pinned Python executable required}"
label="${2:?unique evidence label required}"
[[ "$python" == /* && -x "$python" ]] || exit 64
[[ "$label" =~ ^[a-zA-Z0-9_-]+$ ]] || exit 64
# Do not resolve the executable symlink: it belongs to the selected virtualenv.
exec "$python" e2e/s3_hardening_daily.py --label "$label"
