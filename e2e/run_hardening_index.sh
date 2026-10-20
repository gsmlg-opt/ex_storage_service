#!/usr/bin/env bash
# Actual Concord workload. Pure tree fixtures cannot satisfy A28/A29.
set -euo pipefail
export PAGER=cat
export ERL_FLAGS="${ERL_FLAGS:-+S 4:4}"
python="${1:?pinned python required}"
label="${2:?unique evidence label required}"
[[ "$python" == /* && "$label" =~ ^[A-Za-z0-9_-]+$ ]] || exit 64
# The documented blocker must be fixed in a released dependency before this
# mandatory expensive lane resumes. Never patch or disable protocol assertions.
protocol_source=deps/viewstamped_replication/lib/viewstamped_replication/protocol.ex
protocol_status=0
rg -q 'Enum.each\(1\.\.previous.commit_number' "$protocol_source" || protocol_status=$?
case "$protocol_status" in
  0)
    echo 'BLOCKED A28/A29: gsmlg-dev/concord#94; released VSR must remove historical full-prefix transition work.' >&2
    exit 1
    ;;
  1) ;;
  *)
    echo 'Cannot inspect released VSR protocol source; refusing unverified index workload.' >&2
    exit 1
    ;;
esac
export ESS_DATA_ROOT
ESS_DATA_ROOT="$(mktemp -d /tmp/ess-hardening-index-XXXXXXXX)"
export ESS_BLOB_ROOT="$ESS_DATA_ROOT/blob" ESS_TMP_ROOT="$ESS_DATA_ROOT/tmp"
export ESS_METADATA_ROOT="$ESS_DATA_ROOT/metadata" ESS_RA_ROOT="$ESS_DATA_ROOT/ra"
export ESS_MODE=standalone ESS_AUTO_START=true ESS_WEB_ENABLED=false ESS_PUBLIC_S3_ENABLED=false
export ESS_REPAIR_ENABLED=false ESS_SCRUB_ENABLED=false
export ESS_MASTER_KEY=disposable-index-acceptance-key
export ESS_INDEX_EVIDENCE="docs/reports/s3-hardening/$label-index.json"
[[ ! -e "$ESS_INDEX_EVIDENCE" ]] || exit 1
MIX_ENV=prod mix run --no-compile --no-start e2e/scripts/index_resource.exs
