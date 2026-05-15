# Deployment Guide

ExStorageService is distributed as a Docker image:

```
ghcr.io/gsmlg-dev/ess:latest
```

## Prerequisites

- Docker 24+ (or Docker Compose v2)
- A persistent volume for object data
- A strong random key for `ESS_MASTER_KEY` (generate with `openssl rand -base64 32`)
- A strong random key for `SECRET_KEY_BASE` (generate with `openssl rand -base64 64`)

---

## Docker Run

### Minimal (auth disabled, data in named volume)

```bash
docker run -d \
  --name ess \
  --restart unless-stopped \
  -p 9000:9000 \
  -p 4900:4900 \
  -v ess-data:/data \
  ghcr.io/gsmlg-dev/ess:latest
```

### Production (auth enabled, custom admin password)

Generate secrets first:

```bash
ESS_MASTER_KEY=$(openssl rand -base64 32)
SECRET_KEY_BASE=$(openssl rand -base64 64)
# SHA-256 hash your desired admin password:
ESS_ADMIN_PASSWORD_HASH=$(echo -n "your-password" | sha256sum | awk '{print $1}')
```

Then run:

```bash
docker run -d \
  --name ess \
  --restart unless-stopped \
  -p 9000:9000 \
  -p 4900:4900 \
  -v ess-data:/data \
  -e ESS_DATA_ROOT=/data \
  -e ESS_S3_PORT=9000 \
  -e ESS_ADMIN_PORT=4900 \
  -e ESS_S3_AUTH_ENABLED=true \
  -e ESS_ADMIN_USER=admin \
  -e ESS_ADMIN_PASSWORD_HASH="$ESS_ADMIN_PASSWORD_HASH" \
  -e ESS_MASTER_KEY="$ESS_MASTER_KEY" \
  -e SECRET_KEY_BASE="$SECRET_KEY_BASE" \
  -e PHX_HOST=your.domain.com \
  ghcr.io/gsmlg-dev/ess:latest
```

---

## Docker Compose

Create a `compose.yml` (or `docker-compose.yml`):

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

Store secrets in a `.env` file (do **not** commit this file):

```bash
# .env
ESS_MASTER_KEY=<output of: openssl rand -base64 32>
SECRET_KEY_BASE=<output of: openssl rand -base64 64>
ESS_ADMIN_PASSWORD_HASH=<output of: echo -n "your-password" | sha256sum | awk '{print $1}'>
PHX_HOST=your.domain.com
```

Start the service:

```bash
docker compose up -d
```

Check logs:

```bash
docker compose logs -f ess
```

---

## Environment Variables

| Variable | Default | Description |
|---|---|---|
| `ESS_DATA_ROOT` | `/data` | Directory where object data and metadata are stored |
| `ESS_S3_PORT` | `9000` | S3 API listen port |
| `ESS_ADMIN_PORT` | `4900` | Admin portal listen port |
| `ESS_S3_AUTH_ENABLED` | `false` | Set to `true` to require SigV4 auth on S3 requests |
| `ESS_ADMIN_USER` | `admin` | Root admin username for the web portal |
| `ESS_ADMIN_PASSWORD_HASH` | SHA-256 of `"admin"` | Hex-encoded SHA-256 of the admin password |
| `ESS_MASTER_KEY` | — | **Required in prod.** AES-256 key (base64-encoded) for encrypting access key secrets |
| `SECRET_KEY_BASE` | — | **Required in prod.** Phoenix session signing key |
| `PHX_HOST` | `localhost` | Public hostname for URL generation in the admin portal |

---

## Accessing the Services

| Service | URL |
|---|---|
| S3 API | `http://<host>:9000` |
| Admin Dashboard | `http://<host>:4900` |

Default admin credentials (change in production): `admin` / `admin`

---

## Using mc with a deployed instance

```bash
mc alias set ess http://<host>:9000 <access-key> <secret-key>
mc ls ess
mc mb ess/my-bucket
mc cp myfile.txt ess/my-bucket/
```

Access keys are created through the Admin Dashboard under **Users → Access Keys**.
