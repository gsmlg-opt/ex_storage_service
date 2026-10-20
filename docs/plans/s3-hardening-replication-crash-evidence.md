# ESS-S3-HARDENING-001 A32 replication boundary checkpoint

Current/main baseline `3477bc540c3732dba3d47a7e08779783f9be25fc` with shared
uncommitted implementation. No production changes are needed for this fixture.

The existing local crash lane passed eight whole-BEAM failure boundaries in
`phase3-crashes-r5/results.json`; those results do not prove the internal RF/W
transport crash boundary. A32 still requires the replication assertion below.

New source-only execution command after strict production compilation:

```bash
bash e2e/run_hardening_replication_crash.sh /absolute/pinned/python phase3-replication-crash-r1
```

**Runtime status: r1 exited0; three replication crash/rejoin/resume groups passed.** The
coordinator owns runtime. The aggregate `crashes` profile now requires both the
local crash runner and this separate runner. Total profiles remain eleven.

The runner creates a fresh guarded three-voter fixture using the existing
cluster harness. It refuses occupied fixture node names. The two data nodes
retain RF=2/W=2, node generation1, no degraded writes and existing guards.
The new test-only controller validates exact disposable roots, node identities,
roles/configuration and fixed fixture names before mutating anything.

1. Public signed Boto3 PUT acknowledges an explicit version. The runner retains
   its exact bytes/version and verifies them throughout the fault/recovery.
2. A per-call transport wrapper on data-b delegates to the actual authenticated
   HTTP `put_blob` to data-a. After a real durable acknowledgement with matching
   hash/size/generation, it syncs a witness and halts the entire origin BEAM137.
   No fake transport success, quorum reduction or metadata publication is used.
3. On the survivor, the acknowledged version remains exact. The interrupted
   object's public GET/ListObjectVersions remain absent, its operation intent
   is pending, no location metadata claims quorum, and the peer's actual CAS
   file passes hash verification.
4. Existing retained-root restart uses bootstrap=false. The fixture requires a
   changed origin PID and real VSR normal/commit-applied/view/config convergence
   across all three voters. Both endpoints retain absence plus acknowledged
   data; the origin still lacks a ready blob for the pending operation.
5. The same internal operation ID/body resumes through normal ObjectService.
   Both endpoints must expose exactly one version, exact body/attributes/hash,
   a committed intent and two ready generation1 locations. Actual CAS files
   on both nodes must verify. This is bounded internal recovery and does not
   promise exactly-once semantics for arbitrary independent client retries.

Cleanup stops only this owned fixture, retains separate per-node lifecycle logs,
public results and private RPC/credentials in the disposable root. No publish,
deploy, external replication target or real bucket is used. Cross-cluster
eventual disaster-recovery fault coverage is outside this particular case.

## A31 cold-range and cache-eviction audit

The real A31 connection-fault/ENOSPC runner does not itself evict a cache entry
or make a cold Range request. Its six connection/restart groups must not be
used alone to claim those assertions. They are independently implemented in
`GatewayReadHTTPTest`:

- `cold ranges, warm cache and out-of-band replacement expose one current
  identity` checks206 exact bytes/Content-Range/current attributes and omission
  of a full-object checksum on the partial response.
- `cache eviction cannot invalidate an already-open HTTP response` receives
  and acknowledges the first bounded HTTP chunk, clears the cache, then hashes
  the remaining response to prove exact full bytes through the open descriptor.

The coordinator's `phase3-gateway-error-headers-green.log` has16 tests/0 failures
for the selected error framing, GatewayReadHTTP, range/read and late-stream
files; it was not run with trace, so the log alone does not enumerate names.
`LocalStoreConcurrencyTest` also checks an opened descriptor through eviction,
but `phase3-independent-contracts-r2.log` contains a failing combined core suite
and must not be presented as a wholly passing core acceptance run.
Its exact lower-level test name is `cache contention returns immediately and
eviction preserves an opened reader`; the HTTP cache-eviction case above is
the independent public response contract. `LateStreamHTTPTest` has the exact
test `failure after headers truncates the transfer without appending an error
document`. Existing16/0 and other non-trace summaries do not by themselves
identify every included test name; the fresh scoped trace below is still needed
for a direct name-to-execution map.

Before auditing the full A31 row, retain a fresh explicit scoped trace command:

```bash
mix test apps/ex_storage_service_s3/test/ex_storage_service_s3/gateway_read_http_test.exs apps/ex_storage_service_s3/test/ex_storage_service_s3/late_stream_http_test.exs apps/ex_storage_service/test/ex_storage_service/cloud_cache/local_store_concurrency_test.exs --trace
```

This audit is source/evidence mapping; no new Mix or server command was run by
the worker. The coordinator owns scenario statuses and the main results ledger.

Coordinator A31r7 results were inspected without changing scenario status:
`phase3-a31-r7/results.json` records eight passing runtime groups, real ENOSPC
after seven complete1MiB filler chunks,311 queue samples with maximum/total1,
and zero staging files in every post-fault snapshot. This does not replace the
separate cold-range/open-reader trace evidence.

Before runtime, rejoin RPC handling was hardened from source review: only
transport `:nodedown`/`:timeout` while polling `rejoined` becomes an unavailable
status with `ready: false`. Genuine remote eval/assertion failures still fail.
Results must match a map plus list binding, so `{:badrpc, reason}` cannot be
misread as a successful Code.eval_string tuple. Convergence checks unavailable
nodes before accessing commit/view fields. This is fixture readiness handling,
not a relaxed cluster convergence assertion.
