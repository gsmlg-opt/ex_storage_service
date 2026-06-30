# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Project Overview

ExStorageService is an S3-compatible object storage server built with Elixir/Phoenix, structured as an **umbrella project** with four apps:

- **`ex_storage_service`** (core) — Storage engine, metadata (Concord/Raft KV), IAM, replication, lifecycle, cloud cache, webhook notifications, background processes. No Phoenix dependency.
- **`ex_storage_service_s3`** (S3 API) — Plug.Router served by Bandit on port 9000. S3-compatible REST API with SigV4 auth.
- **`ex_storage_service_web`** (admin portal) — Phoenix LiveView on port 4900. Dashboard, bucket/user/policy management, audit log, `/metrics`.
- **`ex_storage_service_cli`** (client) — Standalone `ess` escript, published to Hex as `ex_storage_service_cli`. Not part of the server release; depends only on `req`/`jason`/`gsmlg_toml`, not the other umbrella apps.

The `ess` release includes only the three server apps. The CLI's version in `apps/ex_storage_service_cli/mix.exs` is kept in sync with the umbrella version in root `mix.exs`.

## Common Commands

```bash
mix setup                          # Install deps + Duskmoon npm packages + DuskMoon bundle + build assets
mix test                           # Run all tests (all apps)
mix test --app ex_storage_service  # Run core app tests only
mix test --app ex_storage_service_s3  # Run S3 app tests only
mix test --app ex_storage_service_web # Run web app tests only
mix test path/to/test.exs          # Run one test file
mix test path/to/test.exs:42       # Run one test
mix format                         # Format code
mix format --check-formatted       # Check formatting (CI)
mix compile --warnings-as-errors   # Compile with strict warnings (CI)
mix phx.server                     # Start all apps (S3 API :9000 + admin portal :4900)
mix duskmoon_bundler.build         # Build frontend assets manually
```

Build the CLI escript: `mix escript.build` inside `apps/ex_storage_service_cli` (produces `./ess`).

## Key Design Decisions

- **No Ecto/database.** All metadata lives in Concord (Raft KV). There are no migrations, no Repo, no schemas.
- **S3 router is not Phoenix.** The S3 API (`apps/ex_storage_service_s3/lib/ex_storage_service_s3/router.ex`) is a standalone `Plug.Router`, not a Phoenix router. Don't use Phoenix helpers there.
- **S3 modules use `ExStorageServiceS3.*` naming** (not `ExStorageService.S3.*`).
- **Assets use Duskmoon Bundler, not Bun/Tailwind CLI.** The `duskmoon_bundler` hex dep builds JS + Tailwind v4 from Elixir tooling; `duskmoon_npm` provides `mix npm.install` - no Node, npm CLI, or Bun binary is required. Duskmoon Bundler config lives in `config/config.exs`; the dev watcher is `Mix.Tasks.DuskmoonBundler.Dev`. Assets live in `apps/ex_storage_service_web/assets/`, output goes to `apps/ex_storage_service_web/priv/static/assets/`.
- **Elixir >= 1.18.0, OTP 28.** CI uses these versions. The built-in `JSON` module is available (Elixir 1.18+).
- **S3 auth is off by default in dev** (`ESS_S3_AUTH_ENABLED=false`), so local S3 requests accept dummy credentials. Dev startup seeds a fixed full-access key: `AKIA-DEV-ACCESS-KEY` / `DEV-SECRET-ACCESS-KEY-DO-NOT-USE`.

## Architecture

### Umbrella Structure

```
apps/
├── ex_storage_service/        # Core domain (storage, metadata, IAM, replication, cloud cache, notifications)
├── ex_storage_service_s3/     # S3 API (Plug.Router + Bandit)
├── ex_storage_service_web/    # Admin portal (Phoenix LiveView)
└── ex_storage_service_cli/    # `ess` escript client (Hex package, standalone)
```

### Supervision Trees (3 server apps)

**Core (`ExStorageService.Application`):** Ra/Concord init + recovery → Storage.Engine → PubSub → MultipartGC → ContentGC → Replication.JobQueue → Replication.Sync → NotificationTaskSupervisor → Storage.Lifecycle. In dev it also seeds the fixed dev access key. It deliberately shuts down Concord's libcluster gossip discovery (single-node deployment; see WORKAROUND comment referencing concord#11).

**S3 (`ExStorageServiceS3.Application`):** Bandit (S3 Router on port 9000)

**Web (`ExStorageServiceWeb.Application`):** Phoenix.Endpoint (port 4900)

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

- **SigV4** (`ExStorageServiceS3.Auth.SigV4`): AWS Signature V4 verification. Access key secrets are AES-256-CTR encrypted at rest using `ESS_MASTER_KEY`. Health endpoint and presigned requests bypass SigV4. The whole layer is skipped when `ESS_S3_AUTH_ENABLED=false`.
- **Authorize** (`ExStorageServiceS3.Plugs.Authorize`): Maps HTTP method + path to S3 actions (e.g., `s3:GetObject`), evaluates IAM policies. Root admin and presigned requests bypass authorization.
- **Policy** (`ExStorageService.IAM.Policy`): AWS-style policy engine — default deny → explicit allow → explicit deny wins. Supports action wildcards and ARN resource matching.

### Background Processes

- `Storage.MultipartGC` — cleans abandoned multipart uploads (24h max age, 1h check interval)
- `Storage.ContentGC` — removes unreferenced content files (30min interval)
- `Storage.Lifecycle` — evaluates object expiration rules
- `Replication.JobQueue` + `Replication.Sync` — async cross-node replication with dead-letter queue
- `NotificationTaskSupervisor` — fires webhook bucket event notifications

### Other Core Subsystems

- `CloudCache` (`cloud_cache/`) — per-bucket upstream cache for AWS S3 / R2 / MinIO and other S3-compatible providers; secrets encrypted with `ESS_MASTER_KEY`
- `Storage.Versioning` — per-bucket object versioning (enable/suspend)
- `Metrics` / `Telemetry` — Prometheus-style metrics exposed at `/metrics` on the admin portal

### Admin LiveView Pages

Routes require admin session (`RequireAdmin` plug):
- `/dashboard` — DashboardLive
- `/buckets` — BucketLive.Index (list, create, delete)
- `/buckets/:name` — BucketLive.Show (objects, presigned URL generation with policy check)
- `/buckets/:name/files` — BucketLive.Files (object browser: upload/download/delete)
- `/buckets/:name/settings` — BucketLive.Settings (versioning, lifecycle, notifications, replication, cloud cache)
- `/users` — UserLive.Index / `/users/:id` — UserLive.Show
- `/policies` — PolicyLive.Index / `/policies/:id` — PolicyLive.Show
- `/audit` — AuditLive.Index

`/metrics` (Prometheus text) does not require an admin session.

## Environment Variables

| Variable | Default | Description |
|---|---|---|
| `ESS_DATA_ROOT` | `/tmp/ex_storage_service/data` (`…/test_data` in test) | Storage, Ra, and Concord data root |
| `ESS_S3_PORT` | `9000` (test: `9001`) | S3 API port |
| `ESS_ADMIN_PORT` | `4900` (test: `4002`) | Admin portal port |
| `ESS_S3_AUTH_ENABLED` | `false` | Require SigV4 auth + IAM authorization for S3 requests |
| `ESS_ADMIN_USER` | `admin` | Root admin username |
| `ESS_ADMIN_PASSWORD_HASH` | SHA256("admin") | Admin password hash |
| `ESS_MASTER_KEY` | fixed dev/test key | AES-256 encryption key (required in prod) |
| `SECRET_KEY_BASE` | — | Phoenix session key (required in prod) |
| `PHX_HOST` | `localhost` | Production URL host for the admin portal |

Production startup (`config/runtime.exs`) refuses insecure defaults: `ESS_S3_AUTH_ENABLED` must be true, `ESS_ADMIN_PASSWORD_HASH` must not be the default, and `ESS_MASTER_KEY` / `SECRET_KEY_BASE` must be set.

## Configuration

- Core config: `config :ex_storage_service, ...` — includes `s3_port`, `admin_port`, `data_root`, GC intervals, size limits (see `config/runtime.exs`)
- Web endpoint config: `config :ex_storage_service_web, ExStorageServiceWeb.Endpoint, ...`
- Duskmoon Bundler asset pipeline config: `config :duskmoon_bundler, ...` in `config/config.exs` (entry, outdir, tailwind sources include `deps/phoenix_duskmoon`)

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

- Tests use S3 port `9001`, admin port `4002`, and an isolated data root `/tmp/ex_storage_service/test_data` (see `config/test.exs` and `config/runtime.exs`)
- Rate limiting is disabled in test env
- Core app's `test_helper.exs` cleans Ra/Concord data directories before each run to avoid stale state
- Tests cover: S3 API operations, SigV4 auth, IAM policies, XML parsing, multipart uploads, replication, CLI commands
- `e2e/` holds out-of-process integration checks (`s3_compat.py`, `cloud_cache_e2e.sh`) run by the E2E GitHub Actions workflows — not part of `mix test`
