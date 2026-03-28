# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Project Overview

ExStorageService is an S3-compatible object storage server built with Elixir/Phoenix. It runs two HTTP servers: an S3 API (Plug.Router on Bandit, default port 9000) and a Phoenix admin portal (LiveView, default port 4000).

## Common Commands

```bash
mix setup              # Install deps + build assets
mix test               # Run all tests
mix test test/ex_storage_service/s3/s3_api_test.exs       # Run a single test file
mix test test/path_test.exs:42                             # Run a specific test by line
mix format             # Format code
mix format --check-formatted  # Check formatting (CI)
mix compile --warnings-as-errors  # Compile with strict warnings (CI)
mix phx.server         # Start both S3 API and admin portal
```

## Key Design Decisions

- **No Ecto/database.** All metadata lives in Concord (Raft KV). There are no migrations, no Repo, no schemas.
- **S3 router is not Phoenix.** The S3 API (`lib/ex_storage_service/s3/router.ex`) is a standalone `Plug.Router`, not a Phoenix router. Don't use Phoenix helpers there.
- **Assets use Bun + Tailwind v4.** The admin UI uses `phoenix_duskmoon` (GitHub dep: `duskmoon-dev/phoenix-duskmoon-ui` tag `v9.0.0-rc.3`), not standard Phoenix components.

## Architecture

### Dual HTTP Server Design

The app runs two independent HTTP servers under the same supervision tree:

- **S3 API** (`lib/ex_storage_service/s3/router.ex`): A `Plug.Router` served by Bandit on port 9000. Implements path-style S3 operations (`/{bucket}/{key}`). Not a Phoenix router.
- **Admin Portal** (`lib/ex_storage_service_web/router.ex`): Phoenix with LiveView on port 4000. Dashboard, bucket/user/policy management, audit log.

### Metadata via Concord (Raft Consensus)

All metadata is stored in Concord (a distributed KV store built on Ra/Raft), not a traditional database. The `Metadata` module (`lib/ex_storage_service/metadata.ex`) wraps Concord with a namespace schema:
- `"bucket:{name}"` — bucket metadata
- `"obj:{bucket}:{key}"` — object metadata
- `"access_key:{id}"` — IAM access keys
- `"mpu:{bucket}:{upload_id}"` — multipart uploads
- `"audit:{timestamp}:{id}"` — audit entries

The application startup (`application.ex`) handles Ra system initialization and waits for Concord cluster readiness with retries.

### Content-Addressable Storage

`Storage.Engine` writes objects to disk using SHA-256 content addressing. Files are organized as `{data_root}/{bucket}/objects/{hash_prefix}/{hash_rest}`. PUT operations compute SHA-256 + MD5 in a single streaming pass.

### S3 Request Pipeline

Requests flow through: `assign_request_id` → `check_presigned_auth` → `SigV4` (identity) → `RateLimiter` → `Authorize` (IAM policy) → route match → `Handlers`/`MultipartHandlers` → `Storage.Engine` (disk) + `Metadata` (Concord) → XML response.

### IAM & Auth

- **SigV4** (`s3/auth/sig_v4.ex`): AWS Signature V4 verification. Access key secrets are AES-256-CTR encrypted at rest using `ESS_MASTER_KEY`.
- **Authorize** (`s3/plugs/authorize.ex`): Maps HTTP method + path to S3 actions (e.g., `s3:GetObject`), evaluates IAM policies. Root admin bypasses checks.
- **Policy** (`iam/policy.ex`): AWS-style policy engine with allow/deny statements, action wildcards, and ARN resource matching.

### Background Processes

The supervision tree includes several GenServers:
- `Storage.MultipartGC` — cleans abandoned multipart uploads (24h max age)
- `Storage.ContentGC` — removes unreferenced content files (30min interval)
- `Storage.Lifecycle` — evaluates object expiration rules
- `Replication.JobQueue` + `Replication.Sync` — cross-node replication with dead-letter queue

## Environment Variables

| Variable | Default | Description |
|---|---|---|
| `ESS_DATA_ROOT` | `/tmp/ex_storage_service/data` | Storage root directory |
| `ESS_S3_PORT` | `9000` | S3 API port |
| `ESS_ADMIN_PORT` | `4000` | Admin portal port |
| `ESS_ADMIN_USER` | `admin` | Root admin username |
| `ESS_ADMIN_PASSWORD_HASH` | SHA256("admin") | Admin password hash |
| `ESS_MASTER_KEY` | auto-generated (dev/test) | AES-256 encryption key (required in prod) |
| `SECRET_KEY_BASE` | — | Phoenix session key (required in prod) |

## UI Library

This project uses the DuskMoon UI system:

- **`phoenix_duskmoon`** — Phoenix LiveView UI component library (primary web UI)
- **`@duskmoon-dev/core`** — Core Tailwind CSS plugin and utilities
- **`@duskmoon-dev/css-art`** — CSS art utilities
- **`@duskmoon-dev/elements`** — Base web components
- **`@duskmoon-dev/art-elements`** — Art/decorative web components

Do NOT use DaisyUI or other CSS component libraries. Do NOT use `core_components.ex` — use `phoenix_duskmoon` components instead.
Use `@duskmoon-dev/core/plugin` as the Tailwind CSS plugin.

### Reporting issues or feature requests

If you encounter missing features, bugs, or need functionality not yet available in any DuskMoon package, open a GitHub issue in the appropriate repository with the label `internal request`:

- **`phoenix_duskmoon`** — https://github.com/gsmlg-dev/phoenix_duskmoon/issues
- **`@duskmoon-dev/core`** — https://github.com/gsmlg-dev/duskmoon-dev/issues
- **`@duskmoon-dev/css-art`** — https://github.com/gsmlg-dev/duskmoon-dev/issues
- **`@duskmoon-dev/elements`** — https://github.com/gsmlg-dev/duskmoon-dev/issues
- **`@duskmoon-dev/art-elements`** — https://github.com/gsmlg-dev/duskmoon-dev/issues

## Testing Notes

- Tests use S3 port `9001` and admin port `4002` (see `config/test.exs`)
- Rate limiting is disabled in test env
- `test/test_helper.exs` cleans Ra/Concord data directories before each run to avoid stale state
- Tests cover: S3 API operations, SigV4 auth, IAM policies, XML parsing, multipart uploads, replication
