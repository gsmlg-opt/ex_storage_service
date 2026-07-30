# Three-node local cluster

This example starts the supported fixed three-voter topology:

- `data-a`: optional public S3 on host port `9000`, private transport at
  `http://data-a:9100` only on the Docker network;
- `data-b`: optional public S3 on host port `9001`, private transport at
  `http://data-b:9100` only on the Docker network;
- `metadata-c`: metadata-only Concord voter with no S3, admin, CAS, or
  maintenance workers.

Each voter has a separate persistent Docker volume. The two data nodes use
strict `RF=2/W=2`; degraded writes are disabled. This is a single-datacenter
development example, not a production network-security template.

## First start

Run commands from the repository root:

```sh
cp deploy/cluster/.env.example deploy/cluster/.env
${EDITOR:-vi} deploy/cluster/.env
test "$(awk -F= '$1 == "ESS_CLUSTER_BOOTSTRAP" {print $2}' deploy/cluster/.env)" = true
test "$(awk -F= '$1 == "ESS_PUBLIC_S3_ENABLED" {print $2}' deploy/cluster/.env)" = false
docker compose --env-file deploy/cluster/.env \
  -f deploy/cluster/compose.yml up --build -d
docker compose --env-file deploy/cluster/.env \
  -f deploy/cluster/compose.yml ps
```

All three state volumes must be empty when
`ESS_CLUSTER_BOOTSTRAP=true`. Wait for `data-a`, `data-b`, and `metadata-c` to
be healthy. Then change `ESS_CLUSTER_BOOTSTRAP=false` in
`deploy/cluster/.env` and immediately bake that setting into all containers:

```sh
docker compose --env-file deploy/cluster/.env \
  -f deploy/cluster/compose.yml up -d --force-recreate
```

Every subsequent recreate, restart, and upgrade must use false; never
bootstrap a voter that already has durable VSR state. Do not use
`docker compose restart` while containers still carry the first-start value.

The initial data-node health checks use release RPC and verify metadata quorum,
the local storage engine, and both authenticated RF=2 data replicas. They do
not depend on public S3, which remains disabled through restore and durability
preflight. The metadata-only health check verifies Concord quorum through
release RPC.

For a new empty cluster, enable the repair/scrub planners while leaving public
writes closed:

```sh
sed -i 's/^ESS_REPAIR_ENABLED=false$/ESS_REPAIR_ENABLED=true/' deploy/cluster/.env
sed -i 's/^ESS_SCRUB_ENABLED=false$/ESS_SCRUB_ENABLED=true/' deploy/cluster/.env
docker compose --env-file deploy/cluster/.env \
  -f deploy/cluster/compose.yml up -d --force-recreate data-a data-b
```

Run the complete preflight in
[`docs/operations/cluster-upgrade.md`](../../docs/operations/cluster-upgrade.md).
Only after it passes, enable the two writer guards and recreate A/B:

```sh
sed -i 's/^ESS_CLUSTER_DATA_PLANE_ENABLED=false$/ESS_CLUSTER_DATA_PLANE_ENABLED=true/' \
  deploy/cluster/.env
sed -i 's/^ESS_PUBLIC_S3_ENABLED=false$/ESS_PUBLIC_S3_ENABLED=true/' \
  deploy/cluster/.env
docker compose --env-file deploy/cluster/.env \
  -f deploy/cluster/compose.yml up -d --force-recreate data-a data-b
curl --fail http://127.0.0.1:9000/health/ready
curl --fail http://127.0.0.1:9001/health/ready
```

The enabled data-node endpoints are:

```text
http://127.0.0.1:9000  # data-a
http://127.0.0.1:9001  # data-b
```

Keep `ESS_MASTER_KEY` unchanged for the lifetime of the metadata and every
backup. All voters need the same value to decrypt IAM and cloud-cache secrets.
Store it outside Git and back it up through the deployment secret manager; a
metadata restore without the matching key is unusable.

## Optional load balancer

Start HAProxy after both data nodes are healthy:

```sh
docker compose --env-file deploy/cluster/.env \
  -f deploy/cluster/compose.yml --profile load-balancer up -d
```

The round-robin endpoint is `http://127.0.0.1:9002`. HAProxy removes a node from
rotation unless `GET /health/ready` returns 200. The compose file does not
publish private transport ports on the host; peers reach them only through the
`ess-cluster` Docker network.

## Status and logs

```sh
docker compose --env-file deploy/cluster/.env \
  -f deploy/cluster/compose.yml logs data-a data-b metadata-c
docker compose --env-file deploy/cluster/.env \
  -f deploy/cluster/compose.yml exec data-a \
  /app/bin/ess rpc 'ExStorageService.Cluster.Readiness.check()'
```

`GET /health` is liveness-only. Use `/health/ready` before admitting public
traffic: it requires metadata quorum and enough healthy, eligible data nodes to
satisfy `W`.

## Stop, restart, and reset

Preserve data:

```sh
docker compose --env-file deploy/cluster/.env \
  -f deploy/cluster/compose.yml down
docker compose --env-file deploy/cluster/.env \
  -f deploy/cluster/compose.yml up -d
```

The second command requires `ESS_CLUSTER_BOOTSTRAP=false`.

Removing volumes destroys all metadata and blobs and is therefore an explicit
reset only:

```sh
docker compose --env-file deploy/cluster/.env \
  -f deploy/cluster/compose.yml down --volumes
```

Do not use the reset command for upgrades. Follow
[`docs/operations/cluster-upgrade.md`](../../docs/operations/cluster-upgrade.md)
for backup, migration, preflight, activation, and rollback boundaries.
