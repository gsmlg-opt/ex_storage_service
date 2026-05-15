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

# Install JS deps
COPY package.json bunfig.toml bun.lock* ./
RUN bun install

# Copy source and config
COPY config ./config
COPY apps ./apps

# Build assets (tailwind + bun bundler)
RUN mix assets.deploy

# Build the Elixir release
RUN mix release ess

# Stage 2: Lean runtime image
FROM alpine:3.21 AS runtime

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

CMD ["/app/bin/ess", "start"]
