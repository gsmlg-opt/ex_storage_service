# ExStorageService

An S3-compatible object storage server built with Elixir and Phoenix.

## Features

- **S3-Compatible API** — Path-style bucket and object operations with AWS Signature V4 authentication
- **IAM** — Users, access keys, and policy-based authorization with ARN resource matching
- **Multipart Uploads** — Full lifecycle support with automatic garbage collection
- **Bucket Versioning** — Enable/suspend versioning per bucket
- **Object Lifecycle** — Configurable expiration rules
- **Replication** — Cross-node data replication with anti-entropy sync
- **Webhook Notifications** — Event-driven notifications for object operations
- **Presigned URLs** — Time-limited access URLs
- **Admin Dashboard** — Phoenix LiveView UI for managing buckets, users, policies, and audit logs
- **Observability** — Prometheus metrics, OpenTelemetry tracing, audit logging

## Quick Start

```bash
# Install dependencies and build assets
mix setup

# Start the server (S3 API on :9000, Admin UI on :4000)
mix phx.server
```

## Architecture

The application runs two HTTP servers:

| Server | Port | Stack | Purpose |
|---|---|---|---|
| S3 API | 9000 | Plug.Router + Bandit | S3-compatible object operations |
| Admin Portal | 4000 | Phoenix + LiveView | Web dashboard and management |

Metadata is stored via [Concord](https://hex.pm/packages/concord) (Raft-based distributed KV), and objects use content-addressable storage on disk (SHA-256).

## Configuration

Key environment variables:

| Variable | Default | Description |
|---|---|---|
| `ESS_DATA_ROOT` | `/tmp/ex_storage_service/data` | Storage root directory |
| `ESS_S3_PORT` | `9000` | S3 API port |
| `ESS_ADMIN_PORT` | `4000` | Admin portal port |
| `ESS_ADMIN_USER` | `admin` | Root admin username |
| `ESS_ADMIN_PASSWORD_HASH` | SHA256("admin") | Admin password hash |
| `ESS_MASTER_KEY` | auto-generated | AES-256 encryption key (required in prod) |
| `SECRET_KEY_BASE` | — | Phoenix session key (required in prod) |

## Testing

```bash
mix test                          # Run all tests
mix test path/to/test.exs         # Run a single file
mix test path/to/test.exs:42      # Run a specific test
```

## CI/CD

GitHub Actions workflows are provided:

- **CI** — Compile checks and format verification on push/PR to main
- **Test** — Full test suite on push/PR to main
- **Release** — Manual dispatch: build Docker image, create git tag and GitHub release
- **E2E Test** — Manual dispatch: start the server and run integration checks

## License

Copyright (c) 2025 GSMLG
