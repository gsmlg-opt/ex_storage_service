# ExStorageService core

This umbrella app owns object metadata, local content-addressed storage,
cluster placement, durability, repair, scrub, drain, and safe cleanup. It uses
Concord/VSR for metadata and does not use Ecto or an external database.

## Embedding

Add the versioned core package without the S3 or admin applications:

```elixir
def deps do
  [
    {:ex_storage_service, "~> 0.6"}
  ]
end
```

Disable the default local instance when the host owns its supervision tree:

```elixir
config :ex_storage_service,
  data_root: "/var/lib/singularity/ess",
  blob_root: "/var/lib/singularity/ess/cas",
  tmp_root: "/var/lib/singularity/ess/cas/tmp",
  ra_root: "/var/lib/singularity/ess/ra",
  metadata_root: "/var/lib/singularity/ess/concord",
  instance_config: [auto_start: false, web_enabled: false]

config :concord,
  data_dir: "/var/lib/singularity/ess/concord",
  vsr: [
    group_id: :ex_storage_service_metadata,
    replica_id: node(),
    members: [%{id: node(), endpoint: node()}],
    storage: :file,
    bootstrap: false
  ]
```

Concord 3 requires the explicit ordered `:members` list even for a standalone
singleton. The umbrella's `config/runtime.exs` derives this same VSR shape
automatically; an embedding host must configure Concord itself.

Then supervise an instance with stable roots. Staging and ready roots must be
on the same filesystem so publication can use an atomic rename.

```elixir
instance_options = [
  instance: :singularity,
  auto_start: false,
  web_enabled: false,
  workers: %{
    multipart_gc: false,
    content_gc: false,
    cas_gc: false,
    packer: false,
    lifecycle: false,
    cross_cluster_replication: false,
    repair: false,
    scrub: false
  }
]

children = [{ExStorageService, instance_options}]
```

For direct content-addressed storage, derive options from the validated
instance context and use `ExStorageService.BlobStore.LocalCAS`:

```elixir
{:ok, context} = ExStorageService.context(instance_options)
blob_options = ExStorageService.Context.blob_store_options(context)

{:ok, staged} = ExStorageService.BlobStore.LocalCAS.stage(data, blob_options)
# staged.hash is the lowercase SHA-256 digest; staged.size is the byte count.

{:ok, ready} = ExStorageService.BlobStore.LocalCAS.commit(staged, blob_options)
# Commit is put-if-absent: identical content reuses the verified ready blob.

{:ok, %{size: size}} =
  ExStorageService.BlobStore.LocalCAS.stat(ready.hash, blob_options)

{:ok, {:file, path, offset, length}} =
  ExStorageService.BlobStore.LocalCAS.open(ready.hash, {10, 20}, blob_options)

:ok = ExStorageService.BlobStore.LocalCAS.verify(ready.hash, blob_options)
```

Call `LocalCAS.discard/2` to abort a staged write. `LocalCAS.commit/2` is the
finalization/pinning boundary: it syncs the staged file and atomically publishes
the immutable SHA-256 path. A successful commit may be retried safely and never
overwrites different content. `LocalCAS.open/3` returns a bounded file source;
consumers should stream it rather than load object-sized data into memory.

Ready blobs live below `objects/sha256/<first-two-hex>/<remaining-hex>` under
the configured blob root. Staging paths are private implementation details.
Preserve the entire data, blob, temporary, and Concord metadata roots together
during backup and upgrades. Package updates preserve ready-blob paths; any
future incompatible on-disk or metadata migration will require an explicit
release-note and migration boundary rather than an automatic destructive
startup migration. Concord/VSR remains shared infrastructure, so only one
metadata instance is supported per BEAM.

## Operator tasks

Run source-tree tasks from the repository root:

```sh
PAGER=cat MIX_ENV=prod mix ess.metadata.schema_status --replication-factor 2
PAGER=cat MIX_ENV=prod mix ess.metadata.migrate_v2 --replication-factor 2
PAGER=cat MIX_ENV=prod mix ess.cluster.bootstrap
PAGER=cat MIX_ENV=prod mix ess.cluster.status
PAGER=cat MIX_ENV=prod mix ess.blob.locate SHA256
PAGER=cat MIX_ENV=prod mix ess.blob.audit --limit 100
PAGER=cat MIX_ENV=prod mix ess.repair.plan --shard 00 --limit 100
PAGER=cat MIX_ENV=prod mix ess.repair.run --shard 00 --limit 100
PAGER=cat MIX_ENV=prod mix ess.node.drain NODE_ID
PAGER=cat MIX_ENV=prod mix ess.node.status NODE_ID --limit 100
```

Inventory and plan commands are read-only. Migration must run with object
writers quiesced after a verified metadata and blob backup. Repair and audit
commands operate on bounded pages; process every reported cursor and owned
shard before treating the result as a cluster-wide preflight.

Release images do not contain Mix or source files. Use the release RPC
equivalents and exact backup, migration, activation, and rollback sequence in
[the cluster upgrade runbook](../../docs/operations/cluster-upgrade.md).
