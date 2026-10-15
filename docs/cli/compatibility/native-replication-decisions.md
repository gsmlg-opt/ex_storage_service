# Portable replication convenience commands

The native `replicate` adapter uses the shared S3 `GetBucketReplication`,
`PutBucketReplication`, and `DeleteBucketReplication` operations. Its rules and
configuration JSON use the pinned AWS CLI 2.31.18 model. Local effect and XML
encoding tests provide implementation evidence; no live replication result,
provider administration, or byte-for-byte mc output parity is claimed.

The grammar was checked against mc commit
`77f82e18b5401a65958f1619df6ebb994634bd88`, specifically `replicate-add.go`,
`replicate-update.go`, `replicate-list.go`, `replicate-remove.go`,
`replicate-import.go`, `replicate-export.go`, and the S3 client operation mapping.
Only behavior and command contracts were inspected; no MinIO implementation is
copied into the adapter.

## Commands and selection

- `replicate add TARGET --remote-bucket BUCKET_ARN --priority N --replicate MODES`
  appends one rule. `--id`, `--tags`, `--storage-class`, and `--disable` match the
  bounded pinned convenience surface. An absent ID is generated. A target key is
  the literal source prefix. Initial configuration requires `--role IAM_ROLE_ARN`,
  an ESS addition; subsequent additions preserve the shared role. Changing that
  role requires a deliberate full configuration import.
- `replicate update TARGET --id ID` changes only explicitly selected fields.
  `edit` is an alias. `--state enable|disable`, priority, destination, tags, storage
  class, replication modes, and a nonempty target prefix are supported. Tags can
  be cleared with `--tags ''`. A bucket target preserves the existing prefix;
  clearing a prefix requires a full document edit. Nested destination encryption,
  ownership, metrics, RTC, source encryption criteria, and unrelated rules remain
  intact.
- `replicate list TARGET [--status enabled|disabled]` returns complete modeled
  configuration with an optional case-insensitive rule-status filter. `ls` is an
  alias. `replicate export TARGET` returns the complete reusable configuration
  document, without mc's per-rule status envelope or remote-admin target details.
- `replicate import TARGET` reads a complete JSON configuration from stdin within
  2 MiB. The input is validated against the shared model without dropping unknown
  fields. Empty rules, more than 1,000 rules, duplicate IDs, and duplicate
  priorities fail. Full import/export preserves every modeled field, including
  historical `ExistingObjectReplication` records.
- `replicate remove TARGET --id ID` removes exactly the selected rule; removing
  its final rule deletes the configuration. `rm` is an alias. Clearing all rules
  requires `--all --force` without an ID. This replaces the earlier ESS
  unqualified full-document `replicate remove TARGET` behavior with explicit
  selection. Missing and ambiguous selections fail without publishing changes.

Write actions support `--dry-run`: prerequisite reads and validation still run,
but the result contains the planned operation/input and performs no PUT/DELETE.
Full removal with `--all --force` can plan DELETE without reading a document.
Versioning, permissions, and provider support are not changed implicitly.
Provider errors remain provider errors; only explicit configuration-not-found
responses permit an initial addition.

## Portable and provider boundaries

Pinned mc addition configures a MinIO Admin remote target before its S3 PUT;
update, list, and selected removal also use MinIO target administration. The ESS
portable adapter does not perform those operations. `--remote-bucket` accepts an
S3 bucket ARN in the AWS, China, or GovCloud partitions. Credential-bearing URLs
and destination aliases fail with a redacted `provider_extension` error.
`--arn` also fails with this typed error; it is not reinterpreted as an IAM role.
MinIO `--bandwidth`, `--sync`, `--proxy`, `--disable-proxy`,
`--healthcheck-seconds`, and `--path` flags are excluded/rejected. MinIO reset,
resync, replication metrics/status, and remote credential updates remain outside
this portable command contract.

The mc `--replicate` default includes nonportable behaviors. Addition therefore
requires an explicit mode string: `delete-marker`, `metadata-sync` (also accepting
the pinned `replica-metadata-sync` spelling), their combination, or an empty string
to disable both. These map to modeled `DeleteMarkerReplication` and
`SourceSelectionCriteria.ReplicaModifications` status fields. Existing source
encryption criteria remain intact. Tag-filtered rules cannot enable delete-marker
replication. Permanent-delete replication is a MinIO extension. Although the AWS
model retains `ExistingObjectReplication`, its documentation says it is no longer
supported; new `existing-objects` convenience requests are rejected rather than
promising historical object replication. Full documents retain that modeled field
for explicit compatibility use.

Convenience storage classes match the pinned `STANDARD` and `REDUCED_REDUNDANCY`
surface; full import supports the complete model. Tag strings are literal
`key=value` pairs separated by `&`, with distinct keys, S3 string bounds, and at
most ten tags. Percent decoding is not applied. Rules retain their original order
and convenience priorities must be nonnegative 32-bit integers and unambiguous.

S3 replication PUT has no compare-and-swap revision precondition. Convenience
edits fetch, preserve, and replace a complete configuration, so operators must
serialize concurrent configuration writers. The tests prove local preservation,
selection, planned/actual operation inputs, and XML output; they do not claim
concurrency-safe merges, live AWS acceptance, or delivery of replicated objects.

## Evidence

`apps/ex_storage_service_cli/test/ex_storage_service_cli/cli/native_replication_test.exs`
covers command entries, new/selected rules, complete nested-field preservation,
literal prefixes, tag clearing, status filtering, reusable export, bounded import,
explicit deletion, dry runs, absence versus service failure, rejected provider
extensions, and modeled XML output. Run it from the standalone CLI application:

```sh
PAGER=cat mix test test/ex_storage_service_cli/cli/native_replication_test.exs
```

Parser/Runner registration and executable acceptance require the coordinating
integration checks; these module tests alone do not establish executable parity.
