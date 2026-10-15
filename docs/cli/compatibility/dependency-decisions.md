# CLI dependency and transport decisions

Evaluated on 2026-10-15 at source baseline `9b8661e`. No dependency was added,
installed into the project, replaced, or overridden by the initial spike. This
record now distinguishes that historical spike from the implemented client and
its dedicated acceptance reports.

## Locked storage transport

Keep the actual locked **http_fetch 0.18.1**, MIT, and coordinated
`http_core`/`http_runtime` 0.18.1 dependencies. The CLI package requires `~> 0.18.1`;
core retains its independent 0.17/0.18 compatibility constraint. No override or
unrelated UI/toolchain migration has been applied.

`transport_spike_test.exs` runs eight independent raw HTTP/1 wire tests, all
passing. It calls the public library, never the existing S3 client's signer or
body-buffering helper:

| Prerequisite | Actual API and observed evidence |
| --- | --- |
| File/range source | `HTTP.Stream.from_enumerable/2` accepts a bounded `Stream.resource` file-range reader; `duplex: :half` uploads exactly the selected bytes and closes the source file |
| Length/raw bytes | Explicit `Content-Length` is retained, no chunk framing is substituted, and gzip object bytes/header reach the peer unchanged |
| Response streaming | `stream_response: true`, `decode_body: false`, `redirect: :manual`; even a small known-length gzip response becomes a stream and writes exact encoded bytes |
| Download backpressure | `{:read_chunk, reader, :ack}` / `{:stream_chunk_ack, ref}`; a second independently sent chunk is withheld until acknowledgment |
| Upload backpressure | A generated 1 GiB source stalls against a peer that does not read; fewer than 256 64 KiB chunks are produced before cancellation rather than enumerating the entire source |
| Cancellation/cleanup | `HTTP.AbortController` through `signal:` aborts an upload with `{:error,:aborted}`; after response headers it emits `{:stream_error,stream,:aborted}` and closes peer socket and stream |
| Overall deadline | `timeout:` remains active after response headers; a stalled body emits `:request_timeout`, closes peer socket, and terminates the stream |
| Trailers | HTTP/1 upload `HTTP.Stream.finish/2` plus a declared `Trailer` header emits exact chunked wire trailers; response trailers arrive after body acknowledgment and before stream end |

The initial spike established finite-deadline HTTP/1 streaming. Later acceptance
found **infinite request/connect deadlines broken in 0.18.1**. The defect is fixed
in upstream 0.21.9, but the umbrella's coordinated HTTP 0.18 graph cannot select
that version without changing unrelated DuskMoon/core requirements. The release
blocker is [gsmlg-dev/http_fetch#123](https://github.com/gsmlg-dev/http_fetch/issues/123),
requesting a supported backport/release strategy. `Transport.HTTPFetch` returns
`upstream_timeout_blocked` for the pinned AWS zero-timeout settings instead of
substituting finite timeouts. Other implementation work can proceed. On 2026-10-15 the upstream maintainer confirmed a validated surgical 0.18.1 backport is preserved locally, but no compatible 0.18.x artifact is published; a maintenance-line release strategy remains unresolved. A local upstream patch is not a selectable published package or permission to override this repository's dependency graph.

The client now has owned cancellation, raw-byte streaming, atomic verified
publication, replay-aware retry, source-change detection, checksums and AWS
chunk/trailer signing. See `transport-cancellation-decisions.md`,
`upload-checksum-decisions.md`, and `retry-region-decisions.md`. Actual custom-CA
and mutual-TLS wire tests pass. HTTP/2, HTTP/3 and live AWS endpoint variants are
not established by the HTTP/1 spike. Request trailers are unsupported by the
inspected HTTP/2/3 dependency API and must not be silently dropped.

The virtual 1 GiB source in the initial spike was cancelled after backpressure.
A later, distinct throttled **2 GiB upload and download** completed while sampling
memory, mailboxes, temporary disk and coordinator counts; exact measurements and
commands are in `resource-filesystem-acceptance.md`. Neither result is a provider
throughput claim.

Consumers must use acknowledged stream reads and stop/abort owned streams on
every early exit. `HTTP.Response.read_all/1` and `clone/1` materialize complete
stream bodies. `write_to/2` may leave a partial output; it cannot alone provide
verified atomic destination publication.

## Native connection inactivity deadlines

Pinned mc's hidden `--conn-read-deadline` and `--conn-write-deadline` reset
independent timers before each socket read/write, defaulting to ten minutes.
These are different from HTTP.fetch's whole-request and connection-establishment
deadlines. HTTP 0.18.1 exposes no complete independent read-idle/write-idle API;
`socket_opts[:send_timeout]` covers only a partial HTTP/1 write surface.
[gsmlg-dev/http_fetch#126](https://github.com/gsmlg-dev/http_fetch/issues/126)
was closed by main-branch PR #127 on 2026-10-15. The fix is not available in
the selected 0.18 dependency series, so this native flag task remains blocked
until a compatible release can be selected and tested. The client does not
register flags that silently substitute whole-request limits.
Application entity-byte rate limiting remains an independent client policy.

A later live check on 2026-10-15 confirmed Hex `http_fetch` 0.21.11 and the
matching upstream GitHub release include PR #127. The published 0.18 releases
remain 0.18.0 and 0.18.1. Thus the fix is published in a newer dependency family,
but no compatible package can satisfy this checkout's coordinated constraints.
This does not authorize an override or an unrelated umbrella dependency migration.

## Parser, query, cryptography, and checksum choices

| Requirement | Evaluated approach | Decision and remaining acceptance |
| --- | --- | --- |
| JMESPath | Reviewed CLI-private MIT port of `jmes` 0.4.1 with built-in JSON and private leex/yecc grammars | Removed untrusted atom lookup and corrected semantics. The licensed package corpus plus the external official corpus (892 semantic cases) pass. `query-decisions.md` records source, budgets, licenses and remaining frontend evidence boundaries. No new Hex dependency. |
| YAML input/output | CLI-private BSD-2-Clause `yamerl` 0.10.0 parser and MIT `ymlr` 5.1.6 emitter | Standard bounded JSON-compatible data only; fixed constructors, no atom/fun/custom-tag extensions, isolated workers, duplicate-key/alias/depth controls. Actual AWS input/output vectors and safety cases pass; see `yaml-decisions.md`. No new Hex dependency. |
| REST-XML | Bounded binary-name `S3.XML` parser and modeled encoder/decoder | Rejects DTD/external entities, excessive depth/size/nodes and malformed documents. Names stay binaries and modeled namespaces/empty/embedded errors have dedicated fixtures. This is not an assertion that every modeled member has independent wire coverage. |
| AWS EventStream | Native incremental framing and OTP CRC32 | Prelude/message CRCs, header types, bounds, truncation and Select event/error handling have independent fixtures. See `special-protocol-decisions.md`; no local substitute for server-side Select. |
| SigV4a | OTP HMAC/SHA256, P-256 ECDSA and AWS key derivation | Independent AWS/reference key/signature verification, region-set, presign and streaming fixtures pass. See `sigv4a-decisions.md` and `upload-checksum-decisions.md`. Live MRAP/Outposts acceptance remains unrun. |
| SHA1/SHA256/CRC32 | OTP incremental hash/CRC primitives | Independent digests, multipart/composite/full-object rules, upload response and atomic download validation are implemented. MD5 remains for modeled Content-MD5 requirements; it is not one of the five pinned flexible-checksum algorithm enum values. |
| CRC32C/CRC64NVME | Fixed-polynomial bounded incremental implementation and GF(2) CRC combination | Independent `awscrt` known-answer/composite vectors, chunked updates and multipart combination pass. All five pinned algorithms have explicit behavior; incompatible SHA FULL_OBJECT and NVME COMPOSITE combinations fail before transmission. See checksum/download/upload tests and their decision records. |

The original candidate review rejected unchanged `jmes` because missing-key
lookup called `String.to_atom/1`. It rejected Hex `yaml` 0.2.0 because the inspected
package had no claimed encoder and its source link returned 404. Those candidates
were not added as external dependencies. The selected private ports retain their
original licenses/notices in both source and binary distributions.

Parser/query/crypto machinery is implemented and tested within the stated
budgets; complete command/member/provider parity is tracked separately in the
compatibility ledger. The external dependency blockers are infinite HTTP deadlines and independent
read/write inactivity deadlines; parser and crypto primitives are available.

Candidate metadata was read from `https://hex.pm/api/packages/<name>` and its
release endpoints. Published sources were inspected in memory from
`https://repo.hex.pm/tarballs/jmes-0.4.1.tar` and `yaml-0.2.0.tar`, without adding
them to the project. Package release checksums:

- jmes 0.4.1: `a16f10e87af9a1dfe829a072437d7dd89d46fc4f6c59aff936ef9ec8306f2db1`
- yamerl 0.10.0: `346adb2963f1051dc837a2364e4acf6eb7d80097c0f53cbdc3046ec8ec4b4e6e`
- yaml_elixir 2.12.2: `e7c1b10122f973e6558462d51c39026ba0e14afbc6745318e990ea82cfe9e159`
- yaml 0.2.0: `a515bdb1dd4f00a971022339425bbaea788a562af8fab805c0fec1855aa78655`
