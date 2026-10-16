# Portable management conveniences

Implementation baseline: `main` at `9b8661e58107eddb24cb8f15495cee4ac979219a` plus the current CLI diff. The command grammar is grounded in pinned mc `77f82e18b5401a65958f1619df6ebb994634bd88` help/source (`cmd/ilm-rule-{add,edit,list,import,export}.go`, `cmd/ilm/{options,parse,table}.go`, `cmd/event-{add,list,remove}.go`, `cmd/client-s3.go`, `cmd/tag-set.go`) and the packaged AWS CLI 2.31.18 S3 model. Implementation is independent Elixir code; no MinIO implementation is incorporated.

`CLI.Management.commands/0` supplies the real Registry grammar. The Runner recognizes the three-part lifecycle path and selects JSON by default for management export. The existing complete-document `ilm get|set|remove`, `event get|set|remove`, and tag JSON forms remain available.

## Lifecycle rules

```sh
ess ilm rule add team/bucket --prefix docs/ --tags 'team=ops' --expire-days 90
ess ilm rule add team/bucket --transition-days 30 --transition-tier STANDARD_IA
ess ilm rule edit team/bucket --id selected --disable --expire-days 120
ess ilm rule list team/bucket --expiry
ess ilm rule remove team/bucket --id selected
ess ilm rule remove team/bucket --all --force --dry-run
ess ilm rule export team/bucket > lifecycle.json
ess ilm rule import team/bucket < lifecycle.json
```

Add supports explicit prefix, literal ampersand-separated tag predicates, integer size predicates with byte/SI/IEC units, expiration days or expired delete markers, current/noncurrent transitions, and noncurrent expiration/retained-version count. Current and noncurrent transition tier values must name portable AWS storage classes (`STANDARD_IA`, `ONEZONE_IA`, `INTELLIGENT_TIERING`, `GLACIER`, `DEEP_ARCHIVE`, `GLACIER_IR`); arbitrary MinIO remote-tier names fail explicitly. Size parsing accepts integer bytes and B/KB/MB/GB/TB/KiB/MiB/GiB/TiB, including an intervening space, within signed 64-bit bytes. Fractional/case-variant unit syntax is rejected rather than silently approximated.

Add obtains the complete current modeled configuration, treats only `NoSuchLifecycleConfiguration` as empty, appends one randomly identified enabled rule, and respects the 1000-rule ceiling. AccessDenied and other failures propagate. Edit requires an ID matching exactly one existing rule, modifies only requested fields, and retains untouched rules, filter predicates, actions and transition defaults. A transition edit refuses to choose among multiple existing transitions. Use the complete-document API for such changes. Status enable/disable are mutually exclusive. At least one action is required; day/count, size ordering, transition pair and expiration conflicts are validated before mutation. Retained noncurrent counts currently require a positive count and an explicit noncurrent expiration day option; the pinned zero default and isolated-count edit semantics are not claimed equivalent.

List returns modeled Rules or an expiration/transition projection, preserving rule identity/status/filter. `--expiry` and `--transition` are mutually exclusive as in the reference. Native structured output is not the mc table/status-envelope schema. Export returns a reusable complete JSON object; import reads at most 8 MiB plus one byte and rejects malformed, oversized or empty Rules replacements. The response's `TransitionDefaultMinimumObjectSize` is retained across export/import and read/modify/write as the actual request header instead of being inserted into the XML body.

Selected remove preserves other rules; removing the last rule uses `DeleteBucketLifecycle`. Whole configuration removal requires `--all --force` and cannot be combined with an ID. Existing `ilm remove TARGET` remains the explicit full-document delete API. All new mutations support `--dry-run`, which validates the complete request and reports its intended operation/input without issuing the write. Read/modify/write dry runs still read the existing configuration.

Missing lifecycle conveniences remain explicit: deprecated `ilm add|edit|ls|rm|export|import` aliases; restore; hidden date/legacy spellings; noncurrent transition retained-count shorthand; MinIO all-version expiration extensions; object-key target prefix inference; complete mc output/error/default parity. Complete modeled lifecycle documents remain available for portable fields not exposed by these conveniences.

## Notifications

```sh
ess event add team/bucket arn:aws:sqs:us-east-1:123456789012:photos --event put,delete --prefix photos/ --suffix .jpg
ess event list team/bucket
ess event list team/bucket arn:aws:sqs:us-east-1:123456789012:photos
ess event remove team/bucket arn:aws:sqs:us-east-1:123456789012:photos --event put,delete --prefix photos/ --suffix .jpg
```

Add uses actual SNS/SQS/Lambda destination fields, portable ARN partitions, modeled events and prefix/suffix filters. It appends to the complete notification configuration and preserves unrelated queues, topics, functions and EventBridge settings. List optionally filters destination entries by ARN while retaining other configuration fields. Targeted removal selects the ARN, with exact event-set/filter selectors when supplied, and preserves everything else. No match fails without writing. `--force` conflicts with a selected ARN; omit the ARN for full removal. The pre-existing `event remove TARGET` remains the full-document removal API, also accepting explicit `--force`. A prefix/suffix/event selector without an ARN fails rather than clearing the whole document.

The event option is required for add: the mc default `put,delete,get` includes the nonportable `s3:ObjectAccessed:*`. Accepted shorthand is `put` → `s3:ObjectCreated:*`, `delete` → `s3:ObjectRemoved:*`, and `replica` → `s3:Replication:*`. The mixed `ilm` expansion and `get`/`scanner` require MinIO events and are rejected. Other modeled AWS event variants can use `event set` with a complete notification document. No notification delivery, subscription or server feature is fabricated.

`--ignore-existing` currently skips an exact duplicate of destination/events/filter (ignoring its generated ID). It does not suppress arbitrary overlap or permission failures from the provider, unlike broader MinIO error-text handling. Short `-p` is not implemented; use the long flag. Omitted event selectors on ARN removal select that ARN's entries, a deliberate difference from mc's nonportable default event set. Output uses native modeled documents, not mc's flattened notification/status envelope.

## Tags

```sh
ess tag list team/bucket
ess tag list team/bucket/key --version-id selected
ess tag set team/bucket/key 'team+name=a%2Bb%3Dc&empty='
ess tag set team/bucket/key '{"TagSet":[{"Key":"team","Value":"ops"}]}'
```

List delegates to actual bucket/object tagging operations, retaining explicit object version selection. `--version-id` on a bucket fails instead of being ignored. Key-value set follows URL query escaping (`+` means space, `%2B` means literal plus, and values may contain encoded equals). Distinct nonempty keys, valid UTF-8/percent escapes, key/value character lengths and bucket/object tag-count limits are checked. Existing complete JSON/file documents remain accepted. Recursive/rewind/all-version selections and `--vid` now use the bounded identity-checked engine; see [native-tag-selection-decisions.md](native-tag-selection-decisions.md), including the pinned full-key `--exclude-folders` rule.

## Safety and validation boundary

New lifecycle/notification read/modify/write paths validate the complete modeled request before calling the transport. Unknown fields in a returned configuration fail validation instead of being dropped by these adapters. The existing wire decoder's modeled output boundary still applies. S3 configuration PUT has no portable conditional revision precondition: these operations do not claim concurrency-safe merging. Quiesce other configuration writers when using conveniences or complete replacements; a concurrent remote change may otherwise be overwritten by the service's normal last-writer behavior.

No live provider configuration mutation or cross-platform acceptance is inferred from the local fixtures. No new dependency/environment variable is introduced. Unimplemented MinIO extensions retain their compatibility inventory classifications.

Scoped validation from `apps/ex_storage_service_cli`:

```sh
PAGER=cat mix test test/ex_storage_service_cli/cli/management_test.exs test/ex_storage_service_cli/cli/management_convenience_test.exs test/ex_storage_service_cli/cli/parser_test.exs test/ex_storage_service_cli/cli/completion_test.exs
PAGER=cat mix compile --warnings-as-errors
PAGER=cat mix format --check-formatted lib/ex_storage_service_cli/cli/management.ex test/ex_storage_service_cli/cli/management_convenience_test.exs
```

Result: **45 tests, 0 failures**; strict compilation and scoped formatting passed on Linux x86_64 / Elixir 1.18.5 / OTP 28. The 20 new convenience tests exercise real Registry parsing and expected operation/input effects, full-document preservation, dry runs, selected and whole removal, permission/missing distinctions, unknown fields, conflicting flags/IDs, transition ambiguity, tag escaping/counts, all destination ARN fields, and bounded stdin. Request effects are checked against literal expected inputs and the actual REST-XML encoder. This is scoped implementation evidence, not a pinned mc executable or live configuration-provider parity claim.
