# Standalone-to-cluster upgrade

This runbook moves a quiesced Concord 3 standalone installation to the fixed
three-voter, two-data-node topology. It uses a verified logical Concord backup
to move KV state between the standalone VSR group and the new cluster VSR
group. Copying a Concord state directory between those configurations is not
supported.

Concord 2.x Ra state is a separate compatibility boundary. Do not point
Concord 3 at a Concord 2 state directory. Export through a binary that supports
the source format before following this runbook.

The current core requires Concord `~> 3.1.3` with matching VSR and ExTurso
packages. New native backups declare `:sha256_deterministic_etf_v1`; ESS verifies
that encoding before using snapshot entries. Verify recovery on the same OTP
major before a runtime-major upgrade. Legacy backups keep their original
checksum semantics, including rejection of mismatches. Recreate affected legacy
backups on their originating VM after upgrading; never bypass verification or
relabel an old checksum as the new scheme.

Empty recovery targets must set `ESS_CONCORD_COMMAND_VERSION=1` before their
first Concord start, and retain it through every quarantine and activation
restart on all voters. Version 1 rebuilds the revision records used by ESS;
the frozen version 0 restore updates only the legacy value representation.
`Backup.restore/3` rejects version 0 before importing or changing its marker.
For embedded consumers, set `command_version: 1` in `config :concord, :vsr`
before starting the dependency instead. Do not change existing installations
from the compatibility default 0 merely to adopt the new package. Their
[upstream migration procedure](https://github.com/gsmlg-dev/concord/blob/v3.1.2/docs/v1/configuration.md)
requires quiesced writers, treatment of legacy indexes, explicit
`:reconcile_legacy_state` and confirmation of the resulting current-state
status. This runbook restores into empty targets; it performs no in-place
command-version migration.
The supplied cluster Compose file forwards this setting to every voter; set it
in the recovery environment file before starting those empty targets.

All commands run from the repository root. Keep the original backup until the
rollback window is closed.

## Preconditions and secret custody

- Plan one maintenance window in a single datacenter.
- Provision data nodes A and B plus metadata-only voter C with isolated,
  node-local metadata state. A and B also need independent blob roots.
- Use the same ordered `ESS_CLUSTER_MEMBERS`, `ESS_CLUSTER_NAME`,
  `RELEASE_COOKIE`, `ESS_INTERNAL_SECRET`, and `ESS_MASTER_KEY` on all voters.
- Preserve the existing `ESS_MASTER_KEY` in the deployment secret manager. It
  is required to decrypt IAM and cloud-cache secrets after restore; do not
  generate a replacement key.
- Keep `ESS_CLUSTER_DATA_PLANE_ENABLED=false`,
  `ESS_PUBLIC_S3_ENABLED=false`, `ESS_REPAIR_ENABLED=false`, and
  `ESS_SCRUB_ENABLED=false` through metadata restore and node registration.
- Keep the load balancer out of rotation until the final gate passes.

Prepare a private backup directory. The local compose example mounts this path
read-only at `/var/lib/ess-backups` on every voter:

```sh
install -d -m 0700 deploy/cluster/backups
export ESS_UPGRADE_BACKUP_ROOT="$PWD/deploy/cluster/backups"
```

## 1. Quiesce and back up standalone

Block new S3 writers, drain in-flight requests, and stop maintenance/outbound
workers as well as listeners. A disabled public writer or cluster data plane
does not disable every GC/lifecycle/replication worker. The running release can
quiesce its local instance while retaining Concord for a logical export:

```sh
/path/to/ess/bin/ess rpc \
  'with {:ok, context} <- ExStorageService.Context.default(), do: ExStorageService.Operations.Backup.quiesce(context)'
```

Quiesce every participating node and any explicit listing-index build before
copying artifacts. Do not resume writes, maintenance, or operator backfill until
the backup is fully verified. A backup is a quiesced boundary, not an online
snapshot assembled from unrelated live copies.

Release RPC:

```sh
/path/to/ess/bin/ess rpc \
  'Concord.Backup.create(path: "/absolute/writable/backup", timeout: 120_000)'
/path/to/ess/bin/ess rpc \
  'Concord.Backup.verify("/absolute/writable/backup/CONCORD_FILE.backup")'
```

Source equivalent: first stop the original BEAM; never open the same metadata
root from a second concurrent process. These short-lived export processes keep
the local instance and public/admin listeners disabled:

```sh
PAGER=cat ESS_AUTO_START=false ESS_PUBLIC_S3_ENABLED=false ESS_WEB_ENABLED=false MIX_ENV=prod mix run -e \
  'IO.inspect(Concord.Backup.create(path: System.fetch_env!("ESS_UPGRADE_BACKUP_ROOT"), timeout: 120_000))'
export PRE_V2_BACKUP="$(
  find "$ESS_UPGRADE_BACKUP_ROOT" -maxdepth 1 -type f -name 'concord_backup_*.backup' \
    -printf '%T@ %p\n' | sort -n | tail -1 | cut -d' ' -f2-
)"
PAGER=cat ESS_AUTO_START=false ESS_PUBLIC_S3_ENABLED=false ESS_WEB_ENABLED=false MIX_ENV=prod mix run -e \
  'case Concord.Backup.verify(System.fetch_env!("PRE_V2_BACKUP")) do {:ok, :valid} = ok -> IO.inspect(ok); other -> raise "invalid backup: #{inspect(other)}" end'
```

After `verify/1` returns `{:ok, :valid}`, stop the standalone process. Take raw
rollback archives of both planes and record checksums plus a non-secret
fingerprint of the master key:

```sh
test -n "$ESS_METADATA_ROOT"
test -n "$ESS_BLOB_ROOT"
tar --acls --xattrs -C "$ESS_METADATA_ROOT" -cpf \
  "$ESS_UPGRADE_BACKUP_ROOT/pre-v2-metadata-root.tar" .
tar --acls --xattrs -C "$ESS_BLOB_ROOT" -cpf \
  "$ESS_UPGRADE_BACKUP_ROOT/pre-v2-blob-root.tar" .
sha256sum "$PRE_V2_BACKUP" \
  "$ESS_UPGRADE_BACKUP_ROOT/pre-v2-metadata-root.tar" \
  "$ESS_UPGRADE_BACKUP_ROOT/pre-v2-blob-root.tar" \
  > "$ESS_UPGRADE_BACKUP_ROOT/pre-v2.sha256"
printf '%s' "$ESS_MASTER_KEY" | sha256sum \
  > "$ESS_UPGRADE_BACKUP_ROOT/master-key.sha256"
```

Store the actual `ESS_MASTER_KEY` only in the secret manager, separately from
these archives.

### Independent same-mode recovery bundles

For recovery of an existing standalone installation or the supported fixed
three-voter/two-data-node cluster, use `ExStorageService.Operations.Backup`.
The upgrade's standalone-to-cluster transformation below remains a separate
procedure; the recovery wrapper deliberately rejects mode/topology changes.

After quiescence, `Backup.create(context, "/private/backups/backup-id")`
publishes a private bundle only after snapshot, artifact and reference
verification. Cluster creation requires `sources: contexts` for every configured
voter; the coordinator must have read access to the declared quiesced node-local
blob/tmp roots. Metadata nodes contribute topology identity, not blob files.
The bundle includes immutable packs and sidecars, multipart data, IAM/config,
job state, intents and journal-referenced staging bytes. It excludes reconstructible
gateway cache and upstream-only bodies. It does not copy VSR state between groups.

```sh
/path/to/ess/bin/ess rpc \
  'with {:ok, context} <- ExStorageService.Context.default(), do: ExStorageService.Operations.Backup.create(context, "/private/backups/backup-id")'
/path/to/ess/bin/ess rpc \
  'ExStorageService.Operations.Backup.verify("/private/backups/backup-id")'
```

Keep the original master key separately provisioned. The private manifest binds
its fingerprint; wrong or absent keys fail verification before import. Checksums
detect incomplete/corrupt artifacts; they do not authenticate a maliciously
replaced bundle. Never print decrypted credentials into verification reports.

Prepare completely empty target roots offline with the target configuration and
a strictly higher generation for each retained logical node ID. For cluster,
use new Erlang node names, isolated roots/network and the same supported ordered
voter identities. `prepare/3` writes a persistent recovery marker before boot:

```sh
/path/to/ess/bin/ess eval \
  'with {:ok, config} <- ExStorageService.InstanceConfig.from_application_env(), do: ExStorageService.Operations.Backup.prepare(ExStorageService.Context.new(config), "/private/backups/backup-id")'
```

Boot with `ESS_AUTO_START=false`, `ESS_PUBLIC_S3_ENABLED=false`,
`ESS_WEB_ENABLED=false`, and `ESS_CLUSTER_DATA_PLANE_ENABLED=false`.
The recovery marker rejects an unsafe normal startup and suppresses the private
blob listener. Bootstrap clean cluster voters once, then restart all with
bootstrap=false before import. Call `Backup.restore(context, bundle)` once on
data A, then `Backup.install(context, bundle)` on each other voter. Restore
fences imported running jobs, re-registers each installed voter, verifies copied
bytes before recording higher-generation locations, and retains unknown intents.
Never lower RF=2/W=2 to complete recovery.

Call `Backup.verify_restore(context, bundle)` on every target. The original roots
must be inaccessible during verification. `Backup.activate(context, bundle,
workers: complete_worker_map)` is a separate explicit gate that repeats
verification. The complete map includes multipart_gc, content_gc, cas_gc, packer,
lifecycle, cross_cluster_replication, repair and scrub. All-false is a supported
initial policy: recovered S3 can be enabled while historical maintenance and
outbound work stays disabled. Startup rejects any worker policy that differs
from the explicitly activated map. Unresolved intents/journals/GC locks prevent
activation of workers; do not silently discard them to bypass that guard.
Publication GC guards remain ordinary byte-protection records and do not block
activation. When a pending/unknown intent has been explicitly reconciled through
the supported intent transaction API, pass the same reviewed
`intent_resolutions: %{"operation-id" => :committed}` (or `:aborted`) to
`verify_restore` and `activate`. Verification still compares its hash, size,
owner/generation, protection deadline, and every other immutable field; only the
declared terminal state and a nondecreasing update timestamp may differ.

Retain backup ID, consistency point, recovery duration, isolation proof and
sanitized verification output. The disposable A36/A37 runner is
`bash e2e/run_hardening_restore.sh /absolute/pinned/python unique-label all`;
namespace/prerequisite failures are failures, never passing skips.

## 2. Migrate the quiesced standalone KV to v2

Run the new binary against the original standalone Concord 3 state and blob
root with listeners closed. Use the future data-A identity so checksum-verified
local location records remain valid after the blob archive is installed on A:

```sh
export ESS_MODE=standalone
export ESS_NODE_ROLE=data
export ESS_NODE_ID=data-a
export ESS_NODE_GENERATION=1
export ESS_PUBLIC_S3_ENABLED=false
export ESS_WEB_ENABLED=false
export ESS_METADATA_SCHEMA=v1
PAGER=cat MIX_ENV=prod mix ess.metadata.schema_status --replication-factor 2
PAGER=cat MIX_ENV=prod mix ess.metadata.migrate_v2 --replication-factor 2
export ESS_METADATA_SCHEMA=v2
PAGER=cat MIX_ENV=prod mix ess.metadata.schema_status --replication-factor 2
PAGER=cat MIX_ENV=prod mix ess.metadata.migrate_v2 --replication-factor 2
```

The final schema status must report `migration_complete: true`,
`migration_required: false`, `replication_ready: true`, zero v1-only objects,
zero under-target descriptors, and zero invalid records. Retained v1 counts
remain visible for compatibility but do not keep the migration open. The
second migration pass must complete without creating another version,
promoting another descriptor, or reporting missing local blobs. Create and
verify the v2 logical export:

```sh
PAGER=cat MIX_ENV=prod mix run -e \
  'IO.inspect(Concord.Backup.create(path: System.fetch_env!("ESS_UPGRADE_BACKUP_ROOT"), timeout: 120_000))'
export V2_BACKUP="$(
  find "$ESS_UPGRADE_BACKUP_ROOT" -maxdepth 1 -type f -name 'concord_backup_*.backup' \
    -printf '%T@ %p\n' | sort -n | tail -1 | cut -d' ' -f2-
)"
PAGER=cat MIX_ENV=prod mix run -e \
  'case Concord.Backup.verify(System.fetch_env!("V2_BACKUP")) do {:ok, :valid} = ok -> IO.inspect(ok); other -> raise "invalid backup: #{inspect(other)}" end'
sha256sum "$V2_BACKUP" >> "$ESS_UPGRADE_BACKUP_ROOT/pre-v2.sha256"
```

Do not resume standalone writes after this export.

## 3. Bootstrap empty voters and restore the v2 export

Copy `deploy/cluster/.env.example` to `deploy/cluster/.env`, install the same
secrets on every voter, and leave all four activation flags false. Restore the
blob archive and copy the verified logical backup into the new, otherwise
empty data-A volume. The one-off container runs as root so the host backup
directory may remain private; the running release later reads only the
appuser-owned `0400` copy:

For these empty recovery targets, explicitly put
`ESS_CONCORD_COMMAND_VERSION=1` in the edited `.env` before the first boot.
Keep it at1 on every voter through all restarts and activation. Existing
version0 installations retain their compatibility setting until an explicit
quiesced migration; do not change the Compose default for existing data.

```sh
cp deploy/cluster/.env.example deploy/cluster/.env
${EDITOR:-vi} deploy/cluster/.env
test "$(awk -F= '$1 == "ESS_CONCORD_COMMAND_VERSION" {print $2}' deploy/cluster/.env)" = 1
export V2_BACKUP_BASENAME="$(basename "$V2_BACKUP")"
docker compose --env-file deploy/cluster/.env \
  -f deploy/cluster/compose.yml run --rm --no-deps --user root \
  -e V2_BACKUP_BASENAME="$V2_BACKUP_BASENAME" \
  --entrypoint /bin/sh data-a -c \
  'rm -rf /var/lib/ess/cas &&
   mkdir -p /var/lib/ess/cas &&
   tar -C /var/lib/ess/cas -xpf /var/lib/ess-backups/pre-v2-blob-root.tar &&
   install -d -o appuser -g appuser -m 0700 /var/lib/ess/import &&
   install -o appuser -g appuser -m 0400 \
     "/var/lib/ess-backups/$V2_BACKUP_BASENAME" \
     /var/lib/ess/import/v2.backup &&
   chown -R appuser:appuser /var/lib/ess/cas'
```

Start all three completely empty VSR roots once with bootstrap true:

```sh
test "$(awk -F= '$1 == "ESS_CLUSTER_BOOTSTRAP" {print $2}' deploy/cluster/.env)" = true
docker compose --env-file deploy/cluster/.env \
  -f deploy/cluster/compose.yml up --build -d data-a data-b metadata-c
docker compose --env-file deploy/cluster/.env \
  -f deploy/cluster/compose.yml ps
```

Immediately set bootstrap false and recreate all voters. Never bootstrap a
non-empty VSR root:

```sh
sed -i 's/^ESS_CLUSTER_BOOTSTRAP=true$/ESS_CLUSTER_BOOTSTRAP=false/' \
  deploy/cluster/.env
docker compose --env-file deploy/cluster/.env \
  -f deploy/cluster/compose.yml up -d --force-recreate data-a data-b metadata-c
```

Verify the backup inside the release container, then perform the destructive,
replicated restore exactly once:

```sh
docker compose --env-file deploy/cluster/.env \
  -f deploy/cluster/compose.yml exec -T data-a /app/bin/ess rpc \
  'case Concord.Backup.verify("/var/lib/ess/import/v2.backup") do {:ok, :valid} = ok -> ok; other -> exit({:invalid_backup, other}) end'
docker compose --env-file deploy/cluster/.env \
  -f deploy/cluster/compose.yml exec -T data-a /app/bin/ess rpc \
  'case Concord.Backup.restore("/var/lib/ess/import/v2.backup", force: true, verify: true, timeout: 120_000) do :ok -> :ok; other -> exit({:restore_failed, other}) end'
```

`restore/2` overwrites the complete cluster KV snapshot. Startup registration
records created while the empty group formed are intentionally overwritten.
Re-register all three configured nodes after the restore and before repair:

```sh
for service in data-a data-b metadata-c; do
  docker compose --env-file deploy/cluster/.env \
    -f deploy/cluster/compose.yml exec -T "$service" /app/bin/ess rpc \
    'with {:ok, context} <- ExStorageService.Context.default(), do: ExStorageService.Operations.Cluster.bootstrap(context)'
done
```

## 4. Run every owned repair shard

Enable maintenance while the public writer guards remain false:

```sh
sed -i 's/^ESS_REPAIR_ENABLED=false$/ESS_REPAIR_ENABLED=true/' deploy/cluster/.env
sed -i 's/^ESS_SCRUB_ENABLED=false$/ESS_SCRUB_ENABLED=true/' deploy/cluster/.env
docker compose --env-file deploy/cluster/.env \
  -f deploy/cluster/compose.yml up -d --force-recreate data-a data-b
```

Run the following release RPC once on `data-a` and once on `data-b`. It obtains
the live membership, selects only shards owned by that node, and follows every
page for all 256 SHA-256 prefix shards:

```sh
for service in data-a data-b; do
  docker compose --env-file deploy/cluster/.env \
    -f deploy/cluster/compose.yml exec -T "$service" /app/bin/ess rpc '
      with {:ok, context} <- ExStorageService.Context.default(),
           {:ok, members} <- ExStorageService.Cluster.Membership.members(context.config) do
        walk = fn walk, shard, cursor ->
          case ExStorageService.Operations.Repair.run_page(context, shard, cursor, 100) do
            {:ok, %{next_cursor: nil}} -> :ok
            {:ok, %{next_cursor: next}} -> walk.(walk, shard, next)
            other -> throw({:repair_page_failed, shard, cursor, other})
          end
        end

        context.config.node_id
        |> ExStorageService.Cluster.Repair.Planner.owned_shards(members)
        |> Enum.each(fn shard -> :ok = walk.(walk, shard, nil) end)
      else
        other -> exit({:repair_setup_failed, other})
      end
    '
done
```

Wait for durable jobs and scrub work to settle. Repeat the command if topology
changed during the scan.

## 5. Mechanical durability preflight

First audit every blob-descriptor page and fail on any unavailable or
under-replicated record:

```sh
docker compose --env-file deploy/cluster/.env \
  -f deploy/cluster/compose.yml exec -T data-a /app/bin/ess rpc '
    with {:ok, context} <- ExStorageService.Context.default() do
      walk = fn walk, cursor, totals ->
        case ExStorageService.Operations.Blob.audit_page(context, cursor, 100) do
          {:ok, page} ->
            totals = %{
              blobs: totals.blobs + page.blobs,
              under_replicated: totals.under_replicated + page.under_replicated,
              unavailable: totals.unavailable + page.unavailable
            }

            if page.next_cursor,
              do: walk.(walk, page.next_cursor, totals),
              else: totals

          other ->
            throw({:blob_audit_failed, cursor, other})
        end
      end

      case walk.(walk, nil, %{blobs: 0, under_replicated: 0, unavailable: 0}) do
        %{under_replicated: 0, unavailable: 0} = totals -> totals
        other -> exit({:durability_preflight_failed, other})
      end
    else
      other -> exit({:context_failed, other})
    end
  '
```

Then require the topology-fenced status snapshot to be complete, RF/W to be
2/2, both data nodes eligible, no unavailable/under-replicated blobs, and no
pending, running, or failed maintenance job:

```sh
docker compose --env-file deploy/cluster/.env \
  -f deploy/cluster/compose.yml exec -T data-a /app/bin/ess rpc '
    with {:ok, context} <- ExStorageService.Context.default(),
         {:ok,
          %{
            metadata: %{status: :normal},
            membership_status: :ok,
            cluster: %{
              status: :ok,
              complete: true,
              desired_replicas: 2,
              required_write_quorum: 2,
              eligible_nodes: 2,
              under_replicated_blobs: 0,
              unavailable_blobs: 0,
              repair_backlog: %{
                pending: 0,
                running: 0,
                failed: 0,
                under_replicated: 0
              }
            }
          } = status} <- ExStorageService.Operations.Cluster.status(context) do
      status
    else
      other -> exit({:cluster_preflight_failed, other})
    end
  '
```

Do not enable writers unless both commands exit zero.

## 6. Enable active-active writes

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

Verify signed S3 reads, versioned writes, multipart completion, and persistence
through both direct endpoints. Only then start the optional load balancer:

```sh
docker compose --env-file deploy/cluster/.env \
  -f deploy/cluster/compose.yml --profile load-balancer up -d
```

## Operator command equivalents

The container image contains the release, not Mix. Use the expression in the
right column with `/app/bin/ess rpc 'EXPRESSION'`; source deployments may use
the Mix command in the left column.

| Source command | Release RPC expression |
|---|---|
| `mix ess.metadata.schema_status --replication-factor 2` | `ExStorageService.Metadata.Schema.status(replication_factor: 2)` |
| `mix ess.metadata.migrate_v2 --replication-factor 2` | `ExStorageService.Metadata.Migration.migrate_v2(replication_factor: 2)` |
| `mix ess.cluster.bootstrap` | `with {:ok, c} <- ExStorageService.Context.default(), do: ExStorageService.Operations.Cluster.bootstrap(c)` |
| `mix ess.cluster.status` | `with {:ok, c} <- ExStorageService.Context.default(), do: ExStorageService.Operations.Cluster.status(c)` |
| `mix ess.blob.locate HASH` | `ExStorageService.Operations.Blob.locate("HASH")` |
| `mix ess.blob.audit --cursor CURSOR --limit 100` | `with {:ok, c} <- ExStorageService.Context.default(), do: ExStorageService.Operations.Blob.audit_page(c, "CURSOR", 100)` |
| `mix ess.repair.plan --shard HEX --cursor CURSOR --limit 100` | `with {:ok, c} <- ExStorageService.Context.default(), do: ExStorageService.Operations.Repair.plan_page(c, "HEX", "CURSOR", 100)` |
| `mix ess.repair.run --shard HEX --cursor CURSOR --limit 100` | `with {:ok, c} <- ExStorageService.Context.default(), do: ExStorageService.Operations.Repair.run_page(c, "HEX", "CURSOR", 100)` |
| `mix ess.node.drain NODE_ID` | `with {:ok, c} <- ExStorageService.Context.default(), do: ExStorageService.Operations.Node.drain(c, "NODE_ID")` |
| `mix ess.node.status NODE_ID --cursor CURSOR --limit 100` | `with {:ok, c} <- ExStorageService.Context.default(), do: ExStorageService.Operations.Node.status(c, "NODE_ID", "CURSOR", 100)` |

Use `nil` instead of `"CURSOR"` for the first page. These operations are
bounded; continue until `next_cursor` is nil.

## Exact logical restore commands

`Concord.Backup.restore/2` is destructive: it overwrites the complete
replicated KV snapshot. Quiesce writers and verify the backup first.

Use empty targets already started with `ESS_CONCORD_COMMAND_VERSION=1` on
every voter, and retain1 through recovery and activation restarts. These raw
calls bypass the ESS wrapper's command-format guard. Setting an environment
variable in an RPC shell does not reconfigure a running release.

Release RPC:

```sh
/path/to/ess/bin/ess rpc \
  'case Concord.Backup.verify("/absolute/backup.backup") do {:ok, :valid} -> Concord.Backup.restore("/absolute/backup.backup", force: true, verify: true, timeout: 120_000); other -> exit({:invalid_backup, other}) end'
```

Source equivalent:

```sh
export RESTORE_BACKUP=/absolute/backup.backup
PAGER=cat MIX_ENV=prod mix run -e \
  'case Concord.Backup.verify(System.fetch_env!("RESTORE_BACKUP")) do {:ok, :valid} -> IO.inspect(Concord.Backup.restore(System.fetch_env!("RESTORE_BACKUP"), force: true, verify: true, timeout: 120_000)); other -> raise "invalid backup: #{inspect(other)}" end'
```

After restoring a cluster snapshot, explicitly re-register each configured
node as shown above.

## Rolling binary upgrade

Back up first. Upgrade one voter at a time with
`ESS_CLUSTER_BOOTSTRAP=false`, waiting for it to rejoin and for cluster
readiness before proceeding. Upgrade metadata C, then one data node, then the
other. Strict `RF=2/W=2` means writes may be unavailable while one data node is
stopped; do not lower W. Re-run the mechanical preflight before returning each
data node to the load balancer.

## Rollback boundary and destructive raw restore

Before the first v2-only write, rollback can use the old compatible binary with
the untouched pre-migration metadata and blob archives.

After any v2-only write, an old binary cannot read the new record set. Rollback
then requires either a binary that understands v2/current Concord 3 state, or
stopping all writers and restoring the complete pre-migration metadata and
blob snapshot as one unit. The latter loses every write accepted after the
backup.

With every storage process stopped, require an explicit confirmation and
restore both raw roots:

```sh
test "${ESS_CONFIRM_DESTRUCTIVE_RESTORE:-}" = RESTORE_PRE_V2
test -n "$ESS_METADATA_ROOT"
test -n "$ESS_BLOB_ROOT"
rm -rf -- "$ESS_METADATA_ROOT" "$ESS_BLOB_ROOT"
install -d -m 0700 "$ESS_METADATA_ROOT" "$ESS_BLOB_ROOT"
tar --acls --xattrs -C "$ESS_METADATA_ROOT" -xpf \
  "$ESS_UPGRADE_BACKUP_ROOT/pre-v2-metadata-root.tar"
tar --acls --xattrs -C "$ESS_BLOB_ROOT" -xpf \
  "$ESS_UPGRADE_BACKUP_ROOT/pre-v2-blob-root.tar"
sha256sum -c "$ESS_UPGRADE_BACKUP_ROOT/pre-v2.sha256"
```

Restore the original `ESS_MASTER_KEY` from the secret manager before starting
the compatible binary. Never restore only one plane, mix node-local metadata
roots from different snapshots, bootstrap over non-empty VSR state, or reuse a
node generation after replacing its storage.
