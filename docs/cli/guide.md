# ESS CLI usage and migration

This guide describes the current implementation. The pinned AWS reference is
CLI 2.31.18; the native reference is mc commit
`77f82e18b5401a65958f1619df6ebb994634bd88`. Release readiness and unverified
contracts are recorded in [milestone acceptance](compatibility/milestone-acceptance-status.md).
The presence of a command does not establish full reference or provider parity.

## Existing ESS installations

Preserve native configuration and transfer state before upgrading. Existing
`[default]` and `[profiles.<name>]` TOML tables remain readable. Config lookup
uses `ESS_CONFIG_DIR`, then `XDG_CONFIG_HOME/ess`, then `~/.config/ess`.
State uses `ESS_STATE_DIR`, then `XDG_STATE_HOME/ess`, then `~/.local/state/ess`.
Configuration updates use private atomic files and retain unrelated settings.

Existing valid configure/bucket/list/tree/copy/move/remove/presign/info commands
retain their structured output contracts. Unknown options now fail explicitly.
`ess version` still prints the client version; bucket versioning uses
`ess version enable|suspend|info TARGET`. `ess info` remains ESS health inspection;
use `ess stat` for object metadata. Native `--endpoint` and `--expires` retain
their meanings; AWS commands use `--endpoint-url` and `--expires-in`.

Native aliases are explicit configuration. The client does not automatically
import mc configuration or modify AWS files. Export aliases with secrets
redacted; inspect an import with `--dry-run` before applying it. A redacted static
credential export cannot restore a secret. Prefer references to existing profiles.

## AWS configuration

The `s3` and `s3api` namespaces resolve AWS shared configuration independently
of ESS defaults. Select a profile explicitly when a workflow requires one:

```sh
ess s3 ls s3://example-bucket/ --profile archive
ess s3api head-object --bucket example-bucket --key report.txt --profile archive
ess s3api put-bucket-cors --generate-cli-skeleton input
ess s3api list-objects-v2 --bucket example-bucket --query 'Contents[].Key' --output json
```

Use environment or profile credential providers instead of placing secrets in
shell arguments. Access key, secret and session token come from one provider.
Supported provider machinery includes shared/environment credentials, credential
processes, role/web identity, IAM Identity Center, container credentials and IMDS.
Live account-specific evidence is separate from controlled provider fixtures.
`--no-sign-request` explicitly bypasses credential loading.

All nine pinned high-level commands and 108 modeled operations are discoverable
through offline help. Model files ship in the package; execution does not download
schemas. Structured JSON/YAML/shorthand inputs, file references, queries, CLI
pagination and waiters share the same modeled operations. Use command-specific
help for applicability. AWS aliases are S3 URIs; native aliases are not substituted
inside AWS namespaces.

See the [package README](../../apps/ex_storage_service_cli/README.md) for the
environment table, retry/checksum settings, TLS, pager, prompt and installation
requirements. The coordinated HTTP 0.21.13 update supplies true infinite
timeouts, independent socket inactivity and request-scoped response-stream policies.
AWS read/connect settings are independent; zero disables the selected timeout.
Native `--conn-read-deadline` and `--conn-write-deadline` default to `10m`, with
zero disabling that direction. Positive Go durations must resolve exactly to
whole milliseconds within `4294967295ms`; no truncation or finite replacement is
used. Bounded slow delivery uses a separate request-scoped acknowledgement policy
without changing global timers. Successful fixed-length upload responses confirm
source finalization; cancellation can interrupt a finalizer. See the
[dependency evaluation](compatibility/http-021-evaluation.md).

AWS stdin `--expected-size` guides multipart sizing without asserting the input
length. Smaller or larger hints upload the actual bytes. Native pipe preserves
its exact-size check; both paths retain private bounded spooling and cleanup.

Restored Glacier and Deep Archive sources can be downloaded, copied or moved
when HEAD reports `ongoing-request="false"`. Un-restored archives are skipped
with the documented warning/status unless explicitly forced. RenameObject
generates an omitted idempotency token once per invocation and retains it on
retry; explicit tokens are preserved.

## Native workflows and differences from mc

| Workflow | ESS contract |
| --- | --- |
| Named endpoints | `alias set/list/remove/import/export`; use explicit provider/profile references |
| Transfers | `cp/mv/get/put` share bounded transfers; `cat/head/pipe` preserve raw bytes |
| Inspection | `ls/tree/du/find/stat` use bounded projections and explicit `Partial` output |
| History | Single explicit version selection; inspection and `cp --rewind TIME` use complete version inventories |
| Synchronization | AWS sync and native mirror have distinct comparison/overwrite/remove policies |
| Management | Portable full modeled documents plus documented lifecycle, replication, policy, notification and tag conveniences |
| Sharing | Signed GET/PUT and exact/prefix POST policies; bounded private history and offline redacted listing |
| Watch | Explicitly selected polling with documented missed-change and ordering limits |
| Diagnostics | Generic S3 probes are separate from ESS health; MinIO administration APIs are outside portable scope |

Native spellings, argument counts, output layouts and defaults deliberately differ
from mc in several places. The [native checklist](compatibility/mc-native-gap-checklist.md)
and linked decision reports identify accepted forms and remaining gaps. Unsupported
options fail; similarly named commands are not universal mc replacements.
Date-only rewind input is UTC. `du --rewind` counts all older data versions;
copy/raw reads select one eligible version. Delete markers hide historical data
from copy snapshots and do not count as stored objects in size totals.

`ls --versions --prefix VALUE --max-keys N` applies the literal prefix and page
size to version requests. The default delimiter is `/`; use `--recursive` for a
flat view. Historical `--rewind` views remain flat and reject an explicit
delimiter. `pipe TARGET --dryrun` performs no stdin, spool or upload I/O;
copy-source, filtering and listing options are rejected for pipe.

`ls --incomplete` lists multipart upload identities without reading object data
or aborting uploads. `--storage-class VALUE` applies an exact case-sensitive
filter, retaining rows with no class. `--summarize` reports the retained row
count and bytes with an explicit completeness marker; incomplete-upload bytes
are unknown (`null`). Rewind rejects these combinations. See the
[listing contract](compatibility/native-ls-options-decisions.md).

Native mirror uses `--overwrite` and `--remove`; AWS sync's `--size-only`,
`--exact-timestamps` and `--delete` are rejected for mirror. Diff also rejects
`--delete`. Use `s3 sync` for the AWS size/timestamp/delete policy.

Native `cp`, `mv`, and `mirror` accept `--older-than` and `--newer-than`
with bounded durations such as `1d6h` or absolute UTC/RFC3339 timestamps.
Relative older includes the cutoff and relative newer excludes it; absolute
older excludes the cutoff and absolute newer includes it, matching the retained
mc observation. Mirror also accepts repeated, exact `--exclude-storageclass`
values. Age, class, and name exclusions protect matching destinations; a
destination-only removal must also satisfy the selected age and class predicates.
Missing timestamps fail closed. See the
[selection contract](compatibility/native-transfer-selection-decisions.md)
for units, reference evidence, and the conservative mirror deletion difference.

`mb --with-lock --with-versioning TARGET` sends Object Lock during creation
and separately enables versioning. `--ignore-existing` ignores only the two
documented bucket-exists service codes; a later versioning failure reports
completed creation without deleting the bucket. Deprecated `ilm add|edit|ls|rm|export|import`
forms reuse lifecycle-rule handlers. `encrypt clear|info` reuse encryption
remove/get.

`legalhold set|clear|info TARGET` supports selected versions and bounded recursive
or historical inventories. `retention set governance 30d TARGET` and
`retention set compliance 1y TARGET` use UTC calendar arithmetic; `--default`
edits an already-enabled bucket Object Lock configuration. Governance bypass
must be explicit. Full-document set forms retain their existing syntax and
reject convenience selectors. Selected mutations require confirmed immutable
version identities before the first write. See the
[governance contract](compatibility/native-governance-decisions.md).

`tag get|list|set|remove` supports `--recursive`, `--versions` and `--rewind`.
Complete inventories precede selected tag operations; bulk/historical writes
require confirmed immutable versions. `--limit` bounds result rows while writes
still apply to the complete selection. See the
[tag selection contract](compatibility/native-tag-selection-decisions.md).
Recursive `tag set --exclude-folders` follows the pinned mc full-key rule:
exclude keys whose first `/` occurs after byte zero. This includes nested
objects, not only folder markers; `/leading/key` remains eligible.

`diff SOURCE TARGET` reports names missing on either side, size changes and
newer source timestamps without reading object contents or writing targets.
See the [comparison contract](compatibility/native-diff-decisions.md) for the
retained native output and exact differences. Native `rm` adds `--versions`,
`--fake`, `--older-than` and `--newer-than`; age-selected removal validates
timestamps and identities before deleting. See the
[removal selection contract](compatibility/native-rm-selection-decisions.md).

`put LOCAL_OR_STDIN TARGET --if-not-exists` conditions the single PUT or multipart
completion with `If-None-Match: *`. A 412 preserves the existing object; resume
cannot drop the recorded condition. See the
[conditional-put contract](compatibility/native-conditional-put-decisions.md).
Native put also accepts explicit SI/IEC `--part-size`, `--disable-multipart` and
case-insensitive `--checksum` values CRC32, CRC32C, CRC64N/CRC64NVME, SHA1 or
SHA256. Provider limits are validated before upload. See the
[put controls](compatibility/native-put-options-decisions.md) for exact defaults,
unsupported variants and framing differences.
`--parallel N` selects 1 through 64 multipart workers under the existing shared
request/byte budgets. Omitted concurrency retains the serial legacy default;
`--disable-multipart` conflicts with parallel values above one.

Native `sql TARGET --query EXPR` streams raw S3 Select Records. CSV/JSON
serialization flags, compression and explicit output headers have modeled
effects; `--recursive` selects from a complete bounded inventory and `--limit`
reports truncation. The legacy `sql TARGET EXPR OUTFILE` syntax retains atomic
file publication. See the [SQL contract](compatibility/native-sql-decisions.md)
for inference, raw-output and provider boundaries.

Native `rm --bypass TARGET` explicitly requests governance-retention bypass on
single or batch deletion. Omitted/false bypass sends no bypass header; service
permissions, compliance retention and legal holds remain enforced. Version
selection, conditional-current deletion and complete-inventory safety still
apply. See the [removal contract](compatibility/native-rm-bypass-decisions.md).

`stat --no-list TARGET` inspects an exact object or bucket without prefix
discovery. Slash-suffixed object markers stay literal; a missing object keeps its
404. Selected versions require a confirming HEAD response. Recursive/all-version
inspection conflicts with no-list. See the
[stat contract](compatibility/native-stat-options-decisions.md).

Raw `s3api` SSE-C customer keys use the pinned base64/MD5 customization when no
MD5 is supplied. Explicit MD5 means the caller supplies both wire values.
Upload/copy metadata must be ASCII. The
[independent SDK captures](compatibility/differential-sdk-customizations.md)
record these hooks and empty bucket-location null outputs.

`watch TARGET --adapter polling --events put,delete --prefix NAME --suffix EXT`
filters emitted create/modify/remove changes by target-relative name. Recursion
defaults to true; `--no-recursive` selects immediate names. Every polling snapshot
still completes before filtering, preserving reconciliation. The source and
defaults differ from mc native notifications; see the
[watch contract](compatibility/watch-polling-decisions.md).

`anonymous set private TARGET` aliases `none`. Both use the existing guarded
full-policy replacement; existing policy requires explicit `--replace`.
See the [anonymous-policy contract](compatibility/native-anonymous-decisions.md)
for the empty-policy provider boundary.

Repeated native `--resolve HOST[:PORT]=literal-IP` pins socket addresses while
preserving the request URL, signed Host, TLS SNI and certificate identity.
Port-specific pins precede host-only pins; failed pins do not fall back to DNS.

`share download --expire 1h --version-id VERSION TARGET` signs a selected GET.
`share upload --recursive TARGET/PREFIX/` grants a nonempty POST key prefix.
Recursive downloads enumerate the complete inventory and enforce a batch cap.
`share list download|upload` reads unexpired private history without credentials
or networking; signed capabilities require explicit `--reveal`. History shares
config-directory selection, uses private atomic files, and is bounded to 1000
active entries and 1 MiB. Local history deletion cannot revoke a remote grant.
See the [sharing contract](compatibility/native-share-decisions.md).

## Safe transfers, recovery and cleanup

Inspect sync/mirror plans with `--dryrun` before authorizing removals. Exclusions
protect destination objects; incomplete listings or failed required transfers
prevent the deletion phase. A move deletes only a verified source version or a
provider-supported conditional source. Unverified unversioned deletion fails closed
and reports copy completion separately.

Downloads publish only after length/checksum/identity checks. A failed transfer
retains the previous local destination. Keep the destination directory under the
invoking user's control; the filesystem contract does not claim protection against
hostile concurrent parent-directory renames. Keys remain literal S3 identifiers,
while unsafe local mappings fail explicitly.

Use `ess transfer list`, `resume` and `abort` to inspect durable multipart state.
Keep journals until completion is confirmed; failed aborts and indeterminate
completion retain inspectable state. Do not reuse a journal after a source or
provider identity change. SIGINT/SIGTERM stop owned work and clean staging; an
upstream abort failure can still require manual cleanup. Config and journal
backup is separate from backing up stored objects.

## Provider and artifact evidence

The [provider report](compatibility/provider-acceptance.md) records independent
ESS 0.6.7 and RustFS 1.0.1 controls, exact image/client hashes, cleanup and four
server limitations. It describes its captured client snapshot. AWS-special
account/bucket infrastructure remains unrun where explicitly stated.

The [packaging report](compatibility/packaging-platform-decisions.md) distinguishes
local source installation, platform CI and publication. Escript runtime requires
matching Erlang/OTP and native platform assets. A release must verify the public
Hex payload, a fresh consumer and downloadable checksums against its immutable
source; a successful local build alone is insufficient.

The [native compatibility dispositions](compatibility/native-compatibility-dispositions.md)
record preserved ESS argument/output contracts and omitted mc conveniences,
including per-prefix encryption maps, archive traversal and history-range undo.
Use the exact command help and guide when migrating; matching command names do
not imply matching mc grammar.
