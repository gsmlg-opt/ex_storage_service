# Stage 1: Build assets (Bun + Tailwind)
FROM oven/bun:1 AS assets-builder

WORKDIR /app

COPY package.json bunfig.toml bun.lock* ./
RUN bun install --frozen-lockfile

COPY apps/ex_storage_service_web/assets ./apps/ex_storage_service_web/assets

# Stage 2: Build Elixir release
FROM hexpm/elixir:1.19.2-erlang-28.0.1-debian-bookworm-20250520-slim AS builder

# Install build tools
RUN apt-get update -y && \
    apt-get install -y build-essential git curl && \
    apt-get clean && rm -f /var/lib/apt/lists/*_*

# Install Bun for asset compilation
RUN curl -fsSL https://bun.sh/install | bash
ENV PATH="/root/.bun/bin:${PATH}"

WORKDIR /app

# Set build env
ENV MIX_ENV=prod

# Install Elixir deps
RUN mix local.hex --force && mix local.rebar --force

COPY mix.exs mix.lock ./
COPY apps/ex_storage_service/mix.exs ./apps/ex_storage_service/mix.exs
COPY apps/ex_storage_service_s3/mix.exs ./apps/ex_storage_service_s3/mix.exs
COPY apps/ex_storage_service_web/mix.exs ./apps/ex_storage_service_web/mix.exs

RUN mix deps.get --only prod

# Copy source
COPY config ./config
COPY apps ./apps

# Copy pre-built node_modules from assets stage
COPY --from=assets-builder /app/node_modules ./node_modules
COPY package.json bunfig.toml bun.lock* ./

# Build assets
RUN mix assets.deploy

# Build the release
RUN mix release ess

# Stage 3: Runtime image
FROM debian:bookworm-slim AS runtime

RUN apt-get update -y && \
    apt-get install -y libstdc++6 openssl libncurses5 locales ca-certificates && \
    apt-get clean && rm -f /var/lib/apt/lists/*_* && \
    sed -i '/en_US.UTF-8/s/^# //g' /etc/locale.gen && locale-gen

ENV LANG=en_US.UTF-8
ENV LANGUAGE=en_US:en
ENV LC_ALL=en_US.UTF-8

WORKDIR /app

RUN useradd --create-home appuser
USER appuser

COPY --from=builder --chown=appuser:appuser /app/_build/prod/rel/ess ./

# S3 API port
EXPOSE 9000
# Admin portal port
EXPOSE 4900

ENV ESS_DATA_ROOT=/data
VOLUME ["/data"]

CMD ["/app/bin/ess", "start"]
