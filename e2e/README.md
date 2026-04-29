# End-to-End Tests

This directory contains black-box S3 compatibility checks for ExStorageService.
The tests use Boto3 against the running HTTP API, with path-style addressing and
AWS Signature V4 enabled, so they exercise the same client surface used by S3
tools that normally talk to MinIO.

## Local Run

```bash
export MIX_ENV=prod
export ESS_S3_AUTH_ENABLED=true
export ESS_S3_PORT=9000
export ESS_ADMIN_PORT=4000
export ESS_DATA_ROOT=/tmp/ex_storage_service/e2e-data
export ESS_MASTER_KEY=test-master-key-for-local-e2e
export SECRET_KEY_BASE=$(mix phx.gen.secret)

mix deps.get --only prod
cd apps/ex_storage_service_web && bun install && cd ../..
mix assets.deploy
mix compile
E2E_GITHUB_ENV=/tmp/ex_storage_service/e2e.env mix run --no-start e2e/scripts/seed_e2e.exs
set -a
. /tmp/ex_storage_service/e2e.env
set +a

mix phx.server
```

In a second shell:

```bash
python3 -m venv /tmp/ex_storage_service/e2e-venv
. /tmp/ex_storage_service/e2e-venv/bin/activate
pip install -r e2e/requirements.txt
python e2e/s3_compat.py --phase exercise --state-file /tmp/ex_storage_service/e2e-state.json
```

Restart the app with the same `ESS_DATA_ROOT`, then run:

```bash
python e2e/s3_compat.py --phase verify-persistence --state-file /tmp/ex_storage_service/e2e-state.json
```
