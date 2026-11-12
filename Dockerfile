# Stage 1: Build Elixir release
FROM rust:1.91.1-slim-trixie AS rust_toolchain

FROM elixir:1.19.5-otp-28-slim AS beam_toolchain

# Published Bundler GNU NIFs require Ubuntu 26.04 / glibc 2.43.
# Reuse the official BEAM toolchain on the supported native-library baseline.
FROM ubuntu:26.04 AS build_toolchain
COPY --from=beam_toolchain /usr/local /usr/local

# Install native build tools, git, curl, openssl, node, npm
RUN apt-get update && apt-get install -y ca-certificates libncurses6 build-essential cmake git curl openssl && \
    curl -fsSL https://deb.nodesource.com/setup_20.x | bash - && \
    apt-get install -y nodejs && \
    apt-get clean && rm -rf /var/lib/apt/lists/*

# Concord's ex_turso NIF requires Rust 1.91 through Rustler 0.38.
COPY --from=rust_toolchain /usr/local/cargo /usr/local/cargo
COPY --from=rust_toolchain /usr/local/rustup /usr/local/rustup
ENV CARGO_HOME=/usr/local/cargo \
    RUSTUP_HOME=/usr/local/rustup \
    LANG=C.UTF-8 \
    PATH=/usr/local/cargo/bin:$PATH

WORKDIR /app

ENV MIX_ENV=prod

# Install hex + rebar
RUN mix local.hex --force && mix local.rebar --force

FROM build_toolchain AS builder

# Fetch Elixir deps
COPY mix.exs mix.lock ./
COPY apps/ex_storage_service/mix.exs ./apps/ex_storage_service/mix.exs
COPY apps/ex_storage_service_cluster/mix.exs ./apps/ex_storage_service_cluster/mix.exs
COPY apps/ex_storage_service_cli/mix.exs ./apps/ex_storage_service_cli/mix.exs
COPY apps/ex_storage_service_s3/mix.exs ./apps/ex_storage_service_s3/mix.exs
COPY apps/ex_storage_service_web/mix.exs ./apps/ex_storage_service_web/mix.exs

RUN mix deps.get --only prod

# Install JS deps via npm workspaces
COPY package.json package-lock.json .npmrc ./
COPY apps/ex_storage_service_web/package.json ./apps/ex_storage_service_web/package.json
RUN npm ci

# Copy source and config
COPY config ./config
COPY apps ./apps

# Embed source identity and the release build timestamp in the admin UI.
ARG ESS_BUILD_GIT_REF
ARG ESS_BUILD_GIT_SHA
ARG ESS_BUILD_TIME
ENV ESS_BUILD_GIT_REF=$ESS_BUILD_GIT_REF \
    ESS_BUILD_GIT_SHA=$ESS_BUILD_GIT_SHA \
    ESS_BUILD_TIME=$ESS_BUILD_TIME

# Build assets (duskmoon.bundle + duskmoon_bundler.build)
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
# IMPORTANT: must use a base OS with a glibc version compatible with the
# builder image's bundled ERTS.
FROM ubuntu:26.04 AS runtime

RUN apt-get update && apt-get install -y openssl libncurses6 wget && rm -rf /var/lib/apt/lists/*

ENV LANG=C.UTF-8

WORKDIR /app

# Keep appuser at UID/GID 1000 so existing data volumes stay writable.
# ubuntu images ship an `ubuntu` user at 1000; remove it before creating
# appuser, and fail the build if the identity ever drifts again.
RUN if id -u ubuntu >/dev/null 2>&1; then userdel --remove ubuntu; fi && \
    if getent group ubuntu >/dev/null; then groupdel ubuntu; fi && \
    groupadd --gid 1000 appuser && \
    useradd --uid 1000 --gid 1000 -d /app appuser && \
    test "$(id -u appuser):$(id -g appuser)" = "1000:1000" && \
    mkdir -p /data /var/lib/ess && \
    chown -R appuser:appuser /data /var/lib/ess
USER appuser

COPY --from=builder --chown=appuser:appuser /app/_build/prod/rel/ess ./

# S3 API port
EXPOSE 9000
# Private cluster data-plane port (publish only on a trusted network)
EXPOSE 9100
# Admin portal port
EXPOSE 4900

ENV ESS_DATA_ROOT=/data
VOLUME ["/data"]

HEALTHCHECK --interval=30s --timeout=5s --start-period=15s --retries=3 \
  CMD wget -qO- http://127.0.0.1:9000/health || exit 1

ENTRYPOINT ["/app/bin/ess"]
CMD ["start"]
