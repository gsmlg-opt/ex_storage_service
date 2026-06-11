# ExStorageService

[![GitHub release](https://img.shields.io/github/v/release/gsmlg-opt/ex_storage_service)](https://github.com/gsmlg-opt/ex_storage_service/releases)
[![Docker image](https://img.shields.io/badge/docker-ghcr.io%2Fgsmlg--dev%2Fess-blue)](https://github.com/orgs/gsmlg-dev/packages/container/package/ess)
[![ex_storage_service_cli on Hex.pm](https://img.shields.io/hexpm/v/ex_storage_service_cli.svg)](https://hex.pm/packages/ex_storage_service_cli)

ExStorageService is an S3-compatible object storage server built with Elixir/OTP.
It runs a Plug/Bandit S3 API, a Phoenix LiveView admin portal, and an optional
`ess` command-line client from one umbrella repository. Metadata is stored in
Concord/Raft KV, and object bytes are stored directly on a content-addressed
disk backend; the service does not use Ecto or an external database.

## Features

- **S3-compatible API** - Path-style bucket and object operations with AWS Signature V4 authentication
- **IAM** - Users, access keys, and AWS-style policy evaluation with allow/deny statements, action wildcards, and ARN resource matching
- **Multipart uploads** - Create, upload parts, complete, abort, list parts, and garbage collect abandoned uploads
- **Bucket versioning** - Enable or suspend object versioning per bucket
- **Object lifecycle** - Configure expiration rules for objects
- **Replication** - Asynchronous cross-node replication with retry and dead-letter handling
- **Cloud cache** - Per-bucket upstream cache support for AWS S3, Cloudflare R2, MinIO, and S3-compatible providers
- **Webhook notifications** - S3-style bucket event notifications for object changes
- **Presigned URLs** - Time-limited object access with policy checks at generation time
- **Admin portal** - Phoenix LiveView UI for buckets, users, policies, settings, cloud cache, and audit logs
- **Observability** - Prometheus-style metrics, Phoenix LiveDashboard in development, telemetry, and audit logging
- **Content-addressable storage** - SHA-256-addressed object files with deduplication and zero-copy reads

## Quick Start

```bash
# Install dependencies and build assets
mix setup

# Start the S3 API on :9000 and admin portal on :4900
mix phx.server
```

Open the admin portal at <http://localhost:4900>.

Default development admin credentials:

```text
admin / admin
```

By default, `ESS_S3_AUTH_ENABLED=false`, so local S3 requests do not require
real credentials. Development startup also seeds a fixed full-access key for
auth-enabled testing:

```text
Access Key: AKIA-DEV-ACCESS-KEY
Secret Key: DEV-SECRET-ACCESS-KEY-DO-NOT-USE
```

Health and metrics endpoints:

```bash
curl http://localhost:9000/health
curl http://localhost:4900/metrics
```

## Docker Deployment

ExStorageService is distributed as a lightweight Docker image at
`ghcr.io/gsmlg-dev/ess:latest`.

### Running with Docker

To start a quick instance with S3 auth disabled and a persistent data volume:

```bash
docker run -d \
  --name ess \
  --restart unless-stopped \
  -p 9000:9000 \
  -p 4900:4900 \
  -v ess-data:/data \
  -e ESS_DATA_ROOT=/data \
  ghcr.io/gsmlg-dev/ess:latest
```

### Running with Docker Compose

Create a `docker-compose.yml` file:

```yaml
services:
  ess:
    image: ghcr.io/gsmlg-dev/ess:latest
    restart: unless-stopped
    ports:
      - "9000:9000"   # S3 API
      - "4900:4900"   # Admin portal
    volumes:
      - ess-data:/data
    environment:
      ESS_DATA_ROOT: /data
      ESS_S3_PORT: "9000"
      ESS_ADMIN_PORT: "4900"
      ESS_S3_AUTH_ENABLED: "true"
      ESS_ADMIN_USER: admin
      ESS_ADMIN_PASSWORD_HASH: "${ESS_ADMIN_PASSWORD_HASH}"
      ESS_MASTER_KEY: "${ESS_MASTER_KEY}"
      SECRET_KEY_BASE: "${SECRET_KEY_BASE}"
      PHX_HOST: "${PHX_HOST:-localhost}"

volumes:
  ess-data:
```

Create a `.env` file in the same directory:

```bash
# Generate with: mix phx.gen.secret 32
ESS_MASTER_KEY=<your-master-key>
# Generate with: mix phx.gen.secret
SECRET_KEY_BASE=<your-secret-key-base>
# Generate with: echo -n "your-password" | sha256sum | awk '{print $1}'
ESS_ADMIN_PASSWORD_HASH=<your-password-sha256-hash>
PHX_HOST=localhost
```

Then start the service:

```bash
docker compose up -d
```

For production-oriented deployment notes, see [docs/deploy.md](docs/deploy.md).

## Clients

### AWS CLI

```bash
aws configure set aws_access_key_id dummy
aws configure set aws_secret_access_key dummy
aws configure set default.region us-east-1

aws --endpoint-url http://localhost:9000 s3 ls
aws --endpoint-url http://localhost:9000 s3 mb s3://my-bucket
aws --endpoint-url http://localhost:9000 s3 cp file.txt s3://my-bucket/file.txt
aws --endpoint-url http://localhost:9000 s3 cp s3://my-bucket/file.txt downloaded.txt
aws --endpoint-url http://localhost:9000 s3 rm s3://my-bucket/file.txt
```

### MinIO Client

[MinIO Client (`mc`)](https://min.io/docs/minio/linux/reference/minio-mc.html)
works well for local object storage workflows.

```bash
mc alias set ess http://localhost:9000 dummy dummy

mc ls ess
mc mb ess/my-bucket
mc cp file.txt ess/my-bucket/
mc cp ess/my-bucket/file.txt .
mc rm ess/my-bucket/file.txt
mc mirror ./local-dir ess/my-bucket/backup
```

When S3 auth is enabled, create an IAM user and access key in the admin portal,
then use those credentials instead of `dummy`.

### ExStorageService CLI

This repository also contains `apps/ex_storage_service_cli`, which builds the
`ess` escript client and is published as
[`ex_storage_service_cli`](https://hex.pm/packages/ex_storage_service_cli) on Hex.pm.

```bash
mix do --app ex_storage_service_cli escript.build
./apps/ex_storage_service_cli/ess configure
./apps/ex_storage_service_cli/ess mb my-bucket
./apps/ex_storage_service_cli/ess cp ./file.txt s3://my-bucket/file.txt
./apps/ex_storage_service_cli/ess ls my-bucket
```

Published releases can be installed with:

```bash
mix escript.install hex ex_storage_service_cli
```

CLI commands include:

| Command | Purpose |
|---|---|
| `ess configure` | Store endpoint and credentials in `~/.config/ess/config.toml` |
| `ess mb <bucket>` | Create a bucket |
| `ess rb <bucket>` | Delete a bucket |
| `ess ls [bucket[/prefix]]` | List buckets or objects |
| `ess cp <src> <dst>` | Upload, download, or copy objects |
| `ess rm s3://bucket/key` | Delete an object |
| `ess mv <src> <dst>` | Move an object by copy and delete |
| `ess presign s3://bucket/key` | Generate a presigned object URL |
| `ess info` | Show server health information |
| `ess version` | Print the CLI version |

## Architecture

The umbrella contains four apps:

| App | Purpose |
|---|---|
| `ex_storage_service` | Core storage engine, Concord metadata, IAM, replication, lifecycle, cloud cache, notifications, and background processes |
| `ex_storage_service_s3` | S3-compatible Plug.Router served by Bandit on port 9000 |
| `ex_storage_service_web` | Phoenix LiveView admin portal served on port 4900 |
| `ex_storage_service_cli` | Standalone `ess` command-line client packaged as an escript |

Runtime services:

| Server | Default port | Stack | Purpose |
|---|---:|---|---|
| S3 API | 9000 | Plug.Router + Bandit | S3-compatible object operations |
| Admin portal | 4900 | Phoenix + LiveView + Bandit | Web dashboard and management |

Repository layout:

| Path | Contents |
|---|---|
| `apps/ex_storage_service/` | Core domain logic, Concord metadata, IAM, storage, replication, lifecycle, notifications, metrics |
| `apps/ex_storage_service_s3/` | Plug.Router S3 API, SigV4 auth, authorization plugs, XML responses, multipart handlers |
| `apps/ex_storage_service_web/` | Phoenix LiveView admin portal, DuskMoon UI components, Volt asset pipeline |
| `apps/ex_storage_service_cli/` | Standalone `ess` escript client |
| `e2e/` | Python and shell integration checks for S3 compatibility and cloud cache |
| `docs/` | Deployment notes and product requirements |

### Metadata and Storage

- **Metadata**: [Concord](https://hex.pm/packages/concord), a Ra/Raft-backed distributed key-value store. No Ecto database, migrations, or Repo are used.
- **Objects**: Content-addressable files on disk under `ESS_DATA_ROOT`, addressed by SHA-256 and deduplicated across object keys.
- **Secrets**: IAM and cloud-cache secrets are encrypted with `ESS_MASTER_KEY`.

### S3 API Operations

| Category | Operations |
|---|---|
| Buckets | ListBuckets, CreateBucket, DeleteBucket, HeadBucket |
| Objects | ListObjectsV2, GetObject, HeadObject, PutObject, CopyObject, DeleteObject, DeleteObjects |
| Multipart | CreateMultipartUpload, UploadPart, CompleteMultipartUpload, AbortMultipartUpload, ListParts |
| Config | Versioning, Lifecycle, Notification |
| Access | SigV4 headers, presigned URLs, IAM authorization |

### Admin Portal Pages

- **Dashboard** - System overview and health
- **Buckets** - Create, delete, browse objects, generate presigned URLs
- **Bucket files** - Object browser with upload/download/delete workflows
- **Bucket settings** - Versioning, lifecycle, notifications, replication, and cloud cache
- **Users** - Create, suspend, activate, delete IAM users and manage access keys
- **Policies** - Create and manage IAM policies
- **Audit log** - Search administrative and S3 events
- **Metrics** - Prometheus text endpoint at `/metrics`

## Configuration

| Variable | Default | Description |
|---|---|---|
| `ESS_DATA_ROOT` | `/tmp/ex_storage_service/data` | Storage, Ra, and Concord data root |
| `ESS_S3_PORT` | `9000` | S3 API port |
| `ESS_ADMIN_PORT` | `4900` | Admin portal port |
| `ESS_S3_AUTH_ENABLED` | `false` | Require SigV4 authentication and IAM authorization for S3 requests |
| `ESS_ADMIN_USER` | `admin` | Root admin username |
| `ESS_ADMIN_PASSWORD_HASH` | SHA256 of `admin` | Root admin password hash |
| `ESS_MASTER_KEY` | fixed dev/test key | AES-256 secret encryption key; required in production |
| `SECRET_KEY_BASE` | none | Phoenix session signing key; required in production |
| `PHX_HOST` | `localhost` | Production URL host for the admin portal |

Production startup refuses insecure defaults:

- `ESS_S3_AUTH_ENABLED` must be true.
- `ESS_ADMIN_PASSWORD_HASH` must not be the default `admin` hash.
- `ESS_MASTER_KEY` and `SECRET_KEY_BASE` must be set.

Generate production secrets with:

```bash
mix phx.gen.secret 32   # ESS_MASTER_KEY
mix phx.gen.secret      # SECRET_KEY_BASE
```

## Development

### Requirements

- Elixir `~> 1.18` or newer
- Erlang/OTP 28 for CI parity
- No Node.js, npm CLI, Bun, or standalone Tailwind CLI is required for normal setup

### Commands

```bash
mix setup                             # Install deps, npm_ex packages, DuskMoon bundle, and assets
mix phx.server                        # Start S3 API and admin portal with dev watchers
mix test                              # Run all tests
mix test apps/ex_storage_service/test     # Run core tests only
mix test apps/ex_storage_service_s3/test  # Run S3 API tests only
mix test apps/ex_storage_service_web/test # Run web tests only
mix test apps/ex_storage_service_cli/test # Run CLI tests only
mix test path/to/test.exs             # Run one test file
mix test path/to/test.exs:42          # Run one test
mix format                            # Format code
mix format --check-formatted          # Check formatting
mix compile --warnings-as-errors      # Compile with CI warning strictness
mix volt.build --tailwind             # Build frontend assets
mix do --app ex_storage_service_cli escript.build # Build the local ess CLI
```

### Asset Pipeline

The admin UI uses Volt and DuskMoon:

- `volt` builds JavaScript and Tailwind CSS from Elixir tooling.
- `npm_ex` manages npm packages such as `@duskmoon-dev/core`; no npm CLI is needed.
- `phoenix_duskmoon` provides the Phoenix LiveView UI components.
- Assets live in `apps/ex_storage_service_web/assets/`.
- Static output is written to `apps/ex_storage_service_web/priv/static/assets/`.

### End-to-End Checks

The `e2e/` directory contains S3 compatibility checks that run against a local
server. See [e2e/README.md](e2e/README.md) for the exact environment setup and
commands.

For Docker deployment details beyond the quick examples above, see
[docs/deploy.md](docs/deploy.md).

## Coding Agents & Automation

This project includes [AGENTS.md](AGENTS.md), which documents repo-specific
instructions for terminal-based AI coding agents.

## End-to-End Testing

Black-box S3 compatibility checks live in [e2e/](e2e/). See
[e2e/README.md](e2e/README.md) for the local signed-S3 exercise and persistence
verification workflow.

## CI/CD

GitHub Actions workflows include:

- **CI** (`ci.yml`) - compile with warnings as errors and check formatting
- **Test** (`test.yml`) - run the test suite
- **Build** (`build.yml`) - build the release image
- **Release** (`release.yml`) - manually dispatch a versioned GHCR image, GitHub release, and CLI publish
- **E2E Test** (`e2e-test.yml`) - run S3 and admin integration checks
- **Cloud Cache E2E** (`cloud-cache-e2e.yml`) - validate cloud cache against MinIO
- **Publish CLI** (`publish-cli.yml`) - publish the `ex_storage_service_cli` package

## License

Copyright (c) 2025 GSMLG
