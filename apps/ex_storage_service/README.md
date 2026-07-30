# ExStorageService core

This umbrella app owns object metadata, local content-addressed storage,
cluster placement, durability, repair, scrub, drain, and safe cleanup. It uses
Concord/VSR for metadata and does not use Ecto or an external database.

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
