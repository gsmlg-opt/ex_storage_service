#!/usr/bin/env bash
# Small application contracts; full suites stay in explicit extended CI lanes.
set -euo pipefail
export PAGER=cat
export MIX_ENV=test
export ERL_FLAGS="${ERL_FLAGS:-+S 4:4 +SDcpu 2 +SDio 2}"
scope="${1:-all}"
core=(
  apps/ex_storage_service/test/ex_storage_service/storage/tags_test.exs
  apps/ex_storage_service/test/ex_storage_service/storage/bucket_cors_test.exs
)
cli=(
  apps/ex_storage_service_cli/test/ex_storage_service_cli/config_test.exs
  apps/ex_storage_service_cli/test/ex_storage_service_cli/cli/target_test.exs
  apps/ex_storage_service_cli/test/ex_storage_service_cli/output_test.exs
)
s3=(apps/ex_storage_service_s3/test/ex_storage_service_s3/health_test.exs)
web=(
  apps/ex_storage_service_web/test/ex_storage_service_web/application_test.exs
  apps/ex_storage_service_web/test/ex_storage_service_web/layouts_test.exs
  apps/ex_storage_service_web/test/ex_storage_service_web/session_controller_test.exs
)
case "$scope" in
  all) tests=("${core[@]}" "${cli[@]}" "${s3[@]}" "${web[@]}") ;;
  core) tests=("${core[@]}") ;;
  cli) tests=("${cli[@]}") ;;
  *) echo "Expected all, core or cli" >&2; exit 64 ;;
esac
exec mix test --max-cases 1 "${tests[@]}"
