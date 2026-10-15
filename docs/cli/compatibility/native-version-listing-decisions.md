# Native version listing selection

`ess ls --versions TARGET` sends modeled `ListObjectVersions` requests rather
than using a transfer inventory whose directory normalization would change the
requested prefix. `--prefix` overrides the target key literally, including an
empty prefix. `--max-keys` selects `MaxKeys` on every page (1 through 1000).
Pagination preserves both the key and version-ID markers.

The default delimiter is `/`, matching existing native `ls` folder views.
`--delimiter` selects another literal delimiter, including an empty string.
`--recursive` omits the delimiter and takes precedence over an explicit
delimiter, matching the legacy command. This deliberately changes the default
of the newly introduced version-listing path, which previously ignored these
options and traversed the complete prefix. Use `--recursive` for that flat view.
Basic legacy `ls` invocations keep their existing implementation and output.

Structured version entries retain the native `key`, `version_id`,
`delete_marker`, `name`, `object_name`, `modified`, `etag` and `size` fields.
Returned common prefixes are explicit `{"prefix":"...","common_prefix":true}`
entries, rather than silently discarded directories. Objects, individual
versions, delete markers and common prefixes each count toward `Returned`.
`--limit` defaults to 1000 and accepts 0 through 10000; an additional row proves
`Partial: true`. A bounded result makes no exhaustive matched-total claim and
does not scan later pages after its partial status is known. Service/pagination
errors encountered before that boundary fail the command.

Native NDJSON emits bounded version/prefix items followed by the listing bound
and the invocation summary. Other structured formats return the native
`Entries`/`Returned`/`Partial` envelope. Plain output retains version labels and
delete-marker annotations, and prints directory rows as S3 paths. These formats
are native contracts rather than mc display parity.

Historical `ls --rewind` retains the existing complete, flat snapshot-selection
contract, including explicit `--recursive` traversal. It applies literal prefix
overrides and page sizes before selecting historical immutable versions. An
explicit `--delimiter` combined with `--rewind` now fails before metadata I/O;
historical directory projection is unsupported. Other historical inspection
commands retain their existing contracts.

## Evidence

`cli/native_version_listing_test.exs` covers literal/overridden prefixes, default,
custom and empty delimiters, recursive precedence, per-page `MaxKeys`, paired
pagination markers, folders, delete markers, limits, protocol/service failures,
plain and NDJSON output, historical version selection and pre-I/O rejection.
A real Runner/loopback HTTP case checks the actual two-page request query and
the modeled XML response output. From `apps/ex_storage_service_cli`:

```sh
PAGER=cat mix test test/ex_storage_service_cli/cli/native_version_listing_test.exs test/ex_storage_service_cli/cli/native_routes_test.exs test/ex_storage_service_cli/cli/native_events_test.exs test/ex_storage_service_cli/cli/native_inspection_test.exs test/ex_storage_service_cli/transfer/version_selection_test.exs --seed 628644
PAGER=cat mix compile --warnings-as-errors
```

The scoped suites passed **62 tests, 0 failures**, including 13 new version
listing tests. Strict compilation, scoped formatting and `git diff --check`
passed. The first wire fixture omitted its generic S3 capability declaration;
it correctly failed with `provider_unsupported` before requests. After declaring
`--service s3`, its real request assertions passed. No provider guard changed.

An initial umbrella-root test command could not start tests because the
unrelated web application's compiled build-info SHA differed from the checkout.
The successful validation used the isolated CLI working directory; web source,
build metadata and caches were not changed.
