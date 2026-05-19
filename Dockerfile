# Stage 1: Build Elixir release (Elixir + Bun + Tailwind all-in-one)
FROM ghcr.io/gsmlg-dev/phoenix:1.8.3-alpine AS builder

# Install build tools
RUN apk add --no-cache build-base git

WORKDIR /app

ENV MIX_ENV=prod

# Install hex + rebar
RUN mix local.hex --force && mix local.rebar --force

# Fetch Elixir deps
COPY mix.exs mix.lock ./
COPY apps/ex_storage_service/mix.exs ./apps/ex_storage_service/mix.exs
COPY apps/ex_storage_service_s3/mix.exs ./apps/ex_storage_service_s3/mix.exs
COPY apps/ex_storage_service_web/mix.exs ./apps/ex_storage_service_web/mix.exs

RUN mix deps.get --only prod

# Install JS deps (copy all workspace package.json files for bun workspaces)
COPY package.json bunfig.toml bun.lock* ./
COPY apps/ex_storage_service_web/package.json ./apps/ex_storage_service_web/package.json
RUN bun install

# Copy source and config
COPY config ./config
COPY apps ./apps

# Build assets (tailwind + bun bundler)
# Dummy env vars satisfy runtime.exs prod guards during the build step only.
# Real values must be provided at container runtime by the operator.
RUN ESS_MASTER_KEY=$(openssl rand -base64 32) \
    SECRET_KEY_BASE=$(openssl rand -base64 64) \
    ESS_S3_AUTH_ENABLED=true \
    ESS_ADMIN_PASSWORD_HASH=0000000000000000000000000000000000000000000000000000000000000000 \
    mix assets.deploy

# Build the Elixir release
RUN ESS_MASTER_KEY=$(openssl rand -base64 32) \
    SECRET_KEY_BASE=$(openssl rand -base64 64) \
    ESS_S3_AUTH_ENABLED=true \
    ESS_ADMIN_PASSWORD_HASH=0000000000000000000000000000000000000000000000000000000000000000 \
    mix release ess

# Stage 2: Lean runtime image
# IMPORTANT: must use the same base image as the builder to ensure
# ERTS and NIF (e.g. crypto) ABI compatibility.
FROM ghcr.io/gsmlg-dev/phoenix:1.8.3-alpine AS runtime

RUN apk add --no-cache libstdc++ openssl ncurses-libs

WORKDIR /app

RUN adduser -D -h /app appuser
USER appuser

COPY --from=builder --chown=appuser:appuser /app/_build/prod/rel/ess ./

# S3 API port
EXPOSE 9000
# Admin portal port
EXPOSE 4900

ENV ESS_DATA_ROOT=/data
VOLUME ["/data"]

HEALTHCHECK --interval=30s --timeout=5s --start-period=15s --retries=3 \
  CMD wget -qO- http://127.0.0.1:9000/health || exit 1

ENTRYPOINT ["/app/bin/ess"]
CMD ["start"]
