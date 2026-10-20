# ESS-S3-HARDENING-001: P3.5/P3.6 source handoff

Baseline `3477bc540c3732dba3d47a7e08779783f9be25fc`; current checkout remains
uncommitted. This document records source/static verification only. It does not
claim A38 or any other compound scenario passed.

Owned source additions: `Operations.ListingIndex` and two exact-read/readiness
contracts; listing page timing in `Metadata.ListIndex`; fixed-cardinality listing
metrics and label-redaction contract; `e2e/hardening_acceptance.py`, five aggregate
failure contracts, local release/container lane, and `s3-hardening.yml`. Existing
standalone runner now includes the independent Phase 1 client; cluster runner can
run existing active-active data loss/election/rejoin/persistence phases after its
normal hardening coverage. The parent owns runtime, resource and scale runners.

The aggregate distinguishes lane execution from audited matrix rows. All eleven
lanes and all 38 audited scenario records are mandatory for aggregate success.
Missing logs, nonzero exits, pending/blocked rows, missing assertion audits, stale
revisions and changed source contents fail verification. A SHA256 fingerprint of
tracked/untracked source also fences an uncommitted checkout. It never updates
scenario statuses based solely on a command's exit status.

Actually executed static checks:

- `python3 -m unittest e2e.test_hardening_acceptance`: initially4 tests, 0 failures;
  later fifth test requires both fault/crash lanes (see A31 source evidence).
- `bash -n` on changed standalone/cluster/release/restore wrappers: exit 0.
- Python AST parse of aggregate and restore runner: exit 0.
- Workflow YAML parsed using `Bun.YAML.parse`: exit 0. PyYAML was unavailable;
  no extra dependency was installed.
- `git diff --check`: exit 0 at the source checkpoint.

Elixir formatting/compilation and the new two operator/one metrics/two recovery
policy contracts are assigned to the parent runtime window and remain unverified
here. All full CI lanes, local release/container smoke and aggregate completion
verification remain unexecuted at this checkpoint. No own processes are running.
The current matrix ledger deliberately retains its partial statuses.

No dependencies, network services, public authentication exclusions or quorum
requirements were changed. All shared application/config/index implementation
remains parent-owned except the agreed `ListIndex.page/4` telemetry wrapper.

## Cache concurrency follow-up

The parent assigned `cloud_cache/local_store.ex` plus cache tests after identifying
raw cache-index read/modify/write lost updates under concurrency. The source now
uses an immediate, node-local per-bucket lock for canonical path mutations and a
bounded eight-attempt metadata CAS. The commit compares bucket revision and exact
listing-root revision, pins bucket generation across retries, and accounts for
eviction in the cache-index transaction. It does not put cache residency into the
object listing index. Reader descriptors are opened before serving; Unix unlink
does not invalidate them. Lock contention returns explicit `:cache_busy` without
waiting through network transfer/response delivery.

Five deterministic contracts cover four concurrent fills, competing metadata CAS,
cutover race, generation replacement, immediate contention and pinned-reader
eviction. Parent reports these five passed in
`docs/reports/s3-hardening/phase3-independent-contracts-r2.log`. The same broader
run had 17 failures from older WriteThrough/DeleteThrough fixtures without ready
index roots. Those fixtures now seed the exact original-generation descriptor;
the original injected errors/assertions remain. Their rerun is parent-owned and
not claimed passed here.

Independent restore remains blocked by
[gsmlg-dev/concord#95](https://github.com/gsmlg-dev/concord/issues/95): byte-identical
native backups can fail inner checksum verification in a fresh BEAM with another
atom initialization history. The parent filed the Bug/internal-request blocker;
the checksum callsite retains its TODO. A36/A37 and full aggregate completion are
not passed. Local/server CI source and successful narrow contracts do not replace
this missing recovery acceptance. No local dependency patch or checksum bypass
was introduced.
