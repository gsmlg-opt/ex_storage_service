# AGENTS.md

## Project Overview

ExStorageService is an S3-compatible object storage server built with Elixir/Phoenix, structured as an **umbrella project** with three apps:

- **`ex_storage_service`** (core) — Storage engine, metadata (Concord/Raft KV), IAM, replication, background processes. No Phoenix dependency.
- **`ex_storage_service_s3`** (S3 API) — Plug.Router served by Bandit on port 9000. S3-compatible REST API with SigV4 auth.
- **`ex_storage_service_web`** (admin portal) — Phoenix LiveView on port 4000. Dashboard, bucket/user/policy management, audit log.

## Common Commands

```bash
mix setup                          # Install deps + build assets
mix test                           # Run all tests (all apps)
mix test --app ex_storage_service  # Run core app tests only
mix test --app ex_storage_service_s3  # Run S3 app tests only
mix test --app ex_storage_service_web # Run web app tests only
mix format                         # Format code
mix format --check-formatted       # Check formatting (CI)
mix compile --warnings-as-errors   # Compile with strict warnings (CI)
mix phx.server                     # Start all apps (S3 API + admin portal)
cd apps/ex_storage_service_web && bun install  # Install JS deps
```

## Key Design Decisions

- **No Ecto/database.** All metadata lives in Concord (Raft KV). There are no migrations, no Repo, no schemas.
- **S3 router is not Phoenix.** The S3 API (`apps/ex_storage_service_s3/lib/ex_storage_service_s3/router.ex`) is a standalone `Plug.Router`, not a Phoenix router. Don't use Phoenix helpers there.
- **S3 modules use `ExStorageServiceS3.*` naming** (not `ExStorageService.S3.*`).
- **Assets use Bun + Tailwind v4.** The admin UI uses `phoenix_duskmoon` (GitHub dep), not standard Phoenix components. Assets live in `apps/ex_storage_service_web/assets/`.
- **Elixir ~> 1.19, OTP 28.** CI uses these versions. The built-in `JSON` module is available (Elixir 1.18+).

## Architecture

### Umbrella Structure

```
apps/
├── ex_storage_service/        # Core domain (storage, metadata, IAM, replication)
├── ex_storage_service_s3/     # S3 API (Plug.Router + Bandit)
└── ex_storage_service_web/    # Admin portal (Phoenix LiveView)
```

### Supervision Trees (3 separate apps)

**Core (`ExStorageService.Application`):** Ra/Concord init → Storage.Engine → PubSub → MultipartGC → ContentGC → Replication.JobQueue → Replication.Sync → NotificationTaskSupervisor → Storage.Lifecycle

**S3 (`ExStorageServiceS3.Application`):** Bandit (S3 Router on port 9000)

**Web (`ExStorageServiceWeb.Application`):** Phoenix.Endpoint (port 4000)

OTP starts apps in dependency order: core → S3 → web.

### Metadata via Concord (Raft KV)

`Metadata` module (`apps/ex_storage_service/lib/ex_storage_service/metadata.ex`) wraps Concord with namespace prefixes:
- `"bucket:{name}"` — bucket metadata
- `"obj:{bucket}:{key}"` — object metadata
- `"user:{user_id}"` / `"access_key:{id}"` / `"policy:{id}"` / `"user_policies:{user_id}"` — IAM
- `"mpu:{bucket}:{upload_id}"` — multipart uploads
- `"audit:{timestamp}:{id}"` — audit entries

Prefix queries are O(N) full table scan — acceptable for < 50K keys.

### Content-Addressable Storage

`Storage.Engine` writes objects to disk using SHA-256 content addressing. Layout: `{data_root}/{bucket}/objects/{hash_prefix}/{hash_rest}`. PUT operations compute SHA-256 + MD5 in a single streaming pass. Reads use zero-copy sendfile.

### S3 Request Pipeline

`assign_request_id` → `check_presigned_auth` → `SigV4` (identity) → `RateLimiter` → `Authorize` (IAM policy) → route match → `Handlers`/`MultipartHandlers` → `Storage.Engine` (disk) + `Metadata` (Concord) → XML response.

### IAM & Auth

- **SigV4** (`ExStorageServiceS3.Auth.SigV4`): AWS Signature V4 verification. Access key secrets are AES-256-CTR encrypted at rest using `ESS_MASTER_KEY`. Health endpoint and presigned requests bypass SigV4.
- **Authorize** (`ExStorageServiceS3.Plugs.Authorize`): Maps HTTP method + path to S3 actions (e.g., `s3:GetObject`), evaluates IAM policies. Root admin and presigned requests bypass authorization.
- **Policy** (`ExStorageService.IAM.Policy`): AWS-style policy engine — default deny → explicit allow → explicit deny wins. Supports action wildcards and ARN resource matching.

### Background Processes

- `Storage.MultipartGC` — cleans abandoned multipart uploads (24h max age, 1h check interval)
- `Storage.ContentGC` — removes unreferenced content files (30min interval)
- `Storage.Lifecycle` — evaluates object expiration rules
- `Replication.JobQueue` + `Replication.Sync` — async cross-node replication with dead-letter queue

### Admin LiveView Pages

Routes require admin session (`RequireAdmin` plug):
- `/dashboard` — DashboardLive
- `/buckets` — BucketLive.Index (list, create, delete)
- `/buckets/:name` — BucketLive.Show (objects, presigned URL generation with policy check)
- `/users` — UserLive.Index (list, create, suspend, delete with audit)
- `/users/:id` — UserLive.Show
- `/policies` — PolicyLive.Index / `/policies/:id` — PolicyLive.Show
- `/audit` — AuditLive.Index

## Environment Variables

| Variable | Default | Description |
|---|---|---|
| `ESS_DATA_ROOT` | `/tmp/ex_storage_service/data` | Storage root directory |
| `ESS_S3_PORT` | `9000` | S3 API port |
| `ESS_ADMIN_PORT` | `4000` | Admin portal port |
| `ESS_S3_AUTH_ENABLED` | `false` | Require SigV4 authentication and IAM authorization for S3 requests |
| `ESS_ADMIN_USER` | `admin` | Root admin username |
| `ESS_ADMIN_PASSWORD_HASH` | SHA256("admin") | Admin password hash |
| `ESS_MASTER_KEY` | auto-generated (dev/test) | AES-256 encryption key (required in prod) |
| `SECRET_KEY_BASE` | — | Phoenix session key (required in prod) |
| `MIX_BUN_PATH` | — | Override bun binary path (for devenv) |
| `MIX_TAILWIND_PATH` | — | Override tailwind binary path (for devenv) |

## Configuration

- Core config: `config :ex_storage_service, ...`
- Web endpoint config: `config :ex_storage_service_web, ExStorageServiceWeb.Endpoint, ...`
- S3 port is under core config: `config :ex_storage_service, s3_port: ...`
- Asset build tools (bun, tailwind) use profile `:ex_storage_service_web`

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
- Core app's `test_helper.exs` cleans Ra/Concord data directories before each run to avoid stale state
- Tests cover: S3 API operations, SigV4 auth, IAM policies, XML parsing, multipart uploads, replication
- Run per-app tests: `mix test --app ex_storage_service_s3` for S3, `mix test --app ex_storage_service` for core/IAM
