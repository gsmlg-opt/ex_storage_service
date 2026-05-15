# ExStorageService

An S3-compatible object storage server built with Elixir and Phoenix.

## Features

- **S3-Compatible API** — Path-style bucket and object operations with AWS Signature V4 authentication
- **IAM** — Users, access keys, and policy-based authorization with ARN resource matching
- **Multipart Uploads** — Full lifecycle support with automatic garbage collection
- **Bucket Versioning** — Enable/suspend versioning per bucket
- **Object Lifecycle** — Configurable expiration rules
- **Replication** — Async cross-node replication with event-driven sync and anti-entropy
- **Webhook Notifications** — Event-driven notifications for object operations
- **Presigned URLs** — Time-limited access URLs with policy enforcement at generation time
- **Admin Dashboard** — Phoenix LiveView UI for managing buckets, users, policies, and audit logs
- **Observability** — Prometheus metrics endpoint, OpenTelemetry tracing, audit logging
- **Content-Addressable Storage** — SHA-256 deduplication with zero-copy reads

## Quick Start

```bash
# Install dependencies and build assets
mix setup

# Start the server (S3 API on :9000, Admin UI on :4900)
mix phx.server
```

Default admin credentials: `admin` / `admin`

### Using with AWS CLI

```bash
# Configure AWS CLI for local use
aws configure set aws_access_key_id <your-access-key>
aws configure set aws_secret_access_key <your-secret-key>

# List buckets
aws --endpoint-url http://localhost:9000 s3 ls

# Create a bucket
aws --endpoint-url http://localhost:9000 s3 mb s3://my-bucket

# Upload a file
aws --endpoint-url http://localhost:9000 s3 cp file.txt s3://my-bucket/file.txt

# Download a file
aws --endpoint-url http://localhost:9000 s3 cp s3://my-bucket/file.txt downloaded.txt
```

### Use with mc

[MinIO Client (`mc`)](https://min.io/docs/minio/linux/reference/minio-mc.html) provides a modern alternative to UNIX commands like ls, cat, cp, mirror, diff, etc. for object storage.

**Getting Access Keys:**
- **Development Mode:** When running in dev mode (`mix phx.server`), the service automatically seeds a fixed test access key with full permissions:
  - Access Key: `AKIA-DEV-ACCESS-KEY`
  - Secret Key: `DEV-SECRET-ACCESS-KEY-DO-NOT-USE`
- **When Auth is Disabled:** If `ESS_S3_AUTH_ENABLED` is `false` (the default), you can use any dummy strings for the credentials (e.g., `dummy` and `dummy`).
- **When Auth is Enabled:** Log into the Admin Dashboard (`http://localhost:4900`) using the default credentials (`admin` / `admin`). Navigate to the **Users** tab, select a user (or create one), and generate a new Access Key to get your `<your-access-key>` and `<your-secret-key>`.

```bash
# Add an alias for your local ExStorageService instance
# Syntax: mc alias set <ALIAS> <YOUR-S3-ENDPOINT> [YOUR-ACCESS-KEY] [YOUR-SECRET-KEY]
mc alias set ess http://localhost:9000 <your-access-key> <your-secret-key>

# List buckets
mc ls ess

# Create a bucket
mc mb ess/my-bucket

# Upload a file
mc cp file.txt ess/my-bucket/

# Download a file
mc cp ess/my-bucket/file.txt .

# Remove a file
mc rm ess/my-bucket/file.txt

# Mirror a directory
mc mirror ./my-local-dir ess/my-bucket/backup
```

## Architecture

The application runs two HTTP servers under one OTP supervision tree:

| Server | Port | Stack | Purpose |
|---|---|---|---|
| S3 API | 9000 | Plug.Router + Bandit | S3-compatible object operations |
| Admin Portal | 4900 | Phoenix + LiveView | Web dashboard and management |

### Storage

- **Metadata**: [Concord](https://hex.pm/packages/concord) (Raft-based distributed KV store built on Ra) — no external database required
- **Objects**: Content-addressable files on disk using SHA-256 hashing, enabling automatic deduplication

### S3 API Operations

| Category | Operations |
|---|---|
| Buckets | ListBuckets, CreateBucket, DeleteBucket, HeadBucket |
| Objects | ListObjectsV2, GetObject, HeadObject, PutObject, CopyObject, DeleteObject, DeleteObjects |
| Multipart | CreateMultipartUpload, UploadPart, CompleteMultipartUpload, AbortMultipartUpload, ListParts |
| Config | Versioning (GET/PUT), Lifecycle (GET/PUT/DELETE), Notification (GET/PUT/DELETE) |

### Admin Portal Pages

- **Dashboard** — System overview and health
- **Buckets** — Create, delete, browse objects, manage versioning/lifecycle/notifications, generate presigned URLs
- **Users** — Create, suspend, activate, delete IAM users with access keys
- **Policies** — Create and manage AWS-style IAM policies (allow/deny, action wildcards, ARN matching)
- **Audit Log** — Searchable log of administrative and S3 operations

## Configuration

| Variable | Default | Description |
|---|---|---|
| `ESS_DATA_ROOT` | `/tmp/ex_storage_service/data` | Storage root directory |
| `ESS_S3_PORT` | `9000` | S3 API port |
| `ESS_ADMIN_PORT` | `4900` | Admin portal port |
| `ESS_S3_AUTH_ENABLED` | `false` | Require SigV4 authentication and IAM authorization for S3 requests |
| `ESS_ADMIN_USER` | `admin` | Root admin username |
| `ESS_ADMIN_PASSWORD_HASH` | SHA256("admin") | Admin password hash |
| `ESS_MASTER_KEY` | auto-generated (dev/test) | AES-256 encryption key (**required in prod**) |
| `SECRET_KEY_BASE` | — | Phoenix session key (**required in prod**) |
| `MIX_BUN_PATH` | — | Override bun binary (for devenv/Nix) |
| `MIX_TAILWIND_PATH` | — | Override tailwind binary (for devenv/Nix) |

## Development

### Requirements

- Elixir ~> 1.19
- OTP 28
- Bun (for JS bundling) — downloaded automatically by `mix setup`

### Commands

```bash
mix setup                    # Install all deps + build assets
mix phx.server               # Start dev server with watchers
mix test                     # Run all tests
mix test path/to/test.exs    # Run a single file
mix test path/to/test.exs:42 # Run a specific test
mix format                   # Format code
```

### Asset Pipeline

Assets use Bun (JS bundler) and Tailwind CSS v4 with the DuskMoon UI component system:

- CSS: `assets/css/app.css` — imports Tailwind + DuskMoon themes/components
- JS: `assets/js/app.js` — Phoenix LiveView + DuskMoon hooks
- Components: `phoenix_duskmoon` library (not standard Phoenix core_components)

## CI/CD

GitHub Actions workflows:

- **CI** (`ci.yml`) — Compile with `--warnings-as-errors` and format check on every push/PR
- **Test** (`test.yml`) — Full test suite on push/PR to main
- **Release** (`release.yml`) — Manual dispatch: build Docker image (GHCR), create git tag
- **E2E Test** (`e2e-test.yml`) — Manual dispatch: start server in prod mode, run S3 + admin integration checks

## License

Copyright (c) 2025 GSMLG
