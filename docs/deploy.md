# Deployment Guide

ExStorageService is distributed as a Docker image:

```
ghcr.io/gsmlg-dev/ess:latest
```

## Prerequisites

- Docker 24+ (or Docker Compose v2)
- A persistent volume for object data
- For a fresh volume, create the private `ess/server.env` file with Bash and OpenSSL using the [first-time Docker configuration](../README.md#first-time-docker-configuration).
- For an existing volume, restore its original `ESS_MASTER_KEY`, admin password hash and session key from your backup.

---

## Docker Run

### Standalone S3 and admin portal

```bash
docker run -d \
  --name ess \
  --restart unless-stopped \
  -p 127.0.0.1:9000:9000 \
  -p 127.0.0.1:4900:4900 \
  -v ess-data:/data \
  -e ESS_DATA_ROOT=/data \
  --env-file "${XDG_CONFIG_HOME:-$HOME/.config}/ess/server.env" \
  ghcr.io/gsmlg-dev/ess:latest
```

Wait for <http://localhost:9000/health/ready>, then sign in to the admin portal
at <http://localhost:4900> with the password chosen during setup. For an
external HTTPS reverse proxy, set `PHX_HOST` to the public hostname.

---

## Docker Compose

Create a `compose.yml` (or `docker-compose.yml`):

```yaml
services:
  ess:
    image: ghcr.io/gsmlg-dev/ess:latest
    restart: unless-stopped
    ports:
      - "127.0.0.1:9000:9000"   # S3 API
      - "127.0.0.1:4900:4900"   # Admin portal
    volumes:
      - ess-data:/data
    environment:
      ESS_DATA_ROOT: /data
      ESS_S3_PORT: "9000"
      ESS_ADMIN_PORT: "4900"
      ESS_S3_AUTH_ENABLED: "true"
      ESS_ADMIN_USER: admin
      ESS_ADMIN_PASSWORD_HASH: "${ESS_ADMIN_PASSWORD_HASH:?required}"
      ESS_MASTER_KEY: "${ESS_MASTER_KEY:?required}"
      SECRET_KEY_BASE: "${SECRET_KEY_BASE:?required}"
      PHX_HOST: "${PHX_HOST:-localhost}"

volumes:
  ess-data:
```

Start the service with the same private configuration file:

```bash
docker compose --env-file "${XDG_CONFIG_HOME:-$HOME/.config}/ess/server.env" up -d
```

Check logs:

```bash
docker compose --env-file "${XDG_CONFIG_HOME:-$HOME/.config}/ess/server.env" logs -f ess
```

---

## Three-node active-active cluster

The supported local cluster layout uses two S3/data nodes and one
metadata-only Concord voter with separate state volumes and strict RF=2/W=2.
Start with public writes disabled, complete metadata migration and replica
preflight, then enable the two data-node listeners.

Use the checked-in
[three-node Compose example](../deploy/cluster/README.md) for a clean topology.
For an existing standalone dataset, follow the
[standalone-to-cluster upgrade runbook](operations/cluster-upgrade.md);
preserve `ESS_MASTER_KEY`, verify both metadata and blob backups, and observe
the documented post-v2 rollback boundary.

---

## Environment Variables

| Variable | Default | Description |
|---|---|---|
| `ESS_DATA_ROOT` | `/data` | Directory where object data and metadata are stored |
| `ESS_S3_PORT` | `9000` | S3 API listen port |
| `ESS_ADMIN_PORT` | `4900` | Admin portal listen port |
| `ESS_S3_AUTH_ENABLED` | `false` in development | Set to `true` for a production S3 listener |
| `ESS_ADMIN_USER` | `admin` | Root admin username for the web portal |
| `ESS_ADMIN_PASSWORD_HASH` | Required for the production portal | Lowercase hex SHA-256 of the chosen admin password |
| `ESS_MASTER_KEY` | — | **Required in prod.** AES-256 key (base64-encoded) for encrypting access key secrets |
| `SECRET_KEY_BASE` | — | **Required in prod.** Phoenix session signing key |
| `PHX_HOST` | `localhost` | Public hostname for URL generation in the admin portal |

The complete standalone, embedding, and cluster configuration contract is in
the root [README](../README.md#configuration).

---

## Accessing the Services

| Service | URL |
|---|---|
| S3 API | `http://<host>:9000` |
| Admin Dashboard | `http://<host>:4900` |

The Docker examples bind both services to loopback. Sign in as `admin` with the
password chosen during setup and create an IAM access key for signed S3 requests.

---

## Using mc with a deployed instance

```bash
mc alias set ess http://<host>:9000 <access-key> <secret-key>
mc ls ess
mc mb ess/my-bucket
mc cp myfile.txt ess/my-bucket/
```

Access keys are created through the Admin Dashboard under **Users → Access Keys**.
