#!/usr/bin/env bash
# Build and execute only local artifacts. No registry login, push or publication.
set -euo pipefail
export PAGER=cat
python="${1:?absolute python required}"
label="${2:?unique evidence label required}"
[[ "$python" == /* && "$label" =~ ^[A-Za-z0-9_-]+$ ]] || exit 64
command -v docker >/dev/null
logs="docs/reports/s3-hardening/$label"
[[ ! -e "$logs" ]] || exit 1
mkdir -p "$logs"
export MIX_ENV=prod
export ESS_MASTER_KEY=hardening-disposable-release-only
export SECRET_KEY_BASE
SECRET_KEY_BASE="$(openssl rand -hex 64)"
export ESS_ADMIN_PASSWORD_HASH
ESS_ADMIN_PASSWORD_HASH="$(openssl rand -hex 32)"
export ESS_WEB_ENABLED=false ESS_PUBLIC_S3_ENABLED=false ESS_AUTO_START=false
export ESS_DATA_ROOT
ESS_DATA_ROOT="$(mktemp -d /tmp/ess-hardening-release-XXXXXXXX)"
version="$("$python" -c 'import re; print(re.search(r"version: \"([^\"]+)\"", open("mix.exs").read())[1])')"
source_sha="$(git rev-parse HEAD)"
build_time="$(date -u +%Y-%m-%dT%H:%M:%SZ)"
export ESS_BUILD_GIT_REF="v$version" ESS_BUILD_GIT_SHA="$source_sha" ESS_BUILD_TIME="$build_time"
# Recompile cached development artifacts with this invocation's build identity
# before the umbrella assets alias starts a child Mix application.
mix compile --force --warnings-as-errors
mix compile.protocols --force
mix do --app ex_storage_service_web cmd mix compile.protocols --force
mix assets.deploy
mix compile --warnings-as-errors
mix release ess --overwrite
image="ess-hardening-local:$label"
docker build --tag "$image" --build-arg "ESS_BUILD_GIT_REF=v$version" \
  --build-arg "ESS_BUILD_GIT_SHA=$source_sha" --build-arg "ESS_BUILD_TIME=$build_time" .
"$python" scripts/release/release.py smoke --image "$image" --version "$version" \
  --source "$source_sha" --output "$logs/container-smoke.json"
docker image inspect "$image" --format '{{.Id}}' > "$logs/image-id.txt"
