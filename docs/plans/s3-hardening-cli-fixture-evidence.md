# CLI journal socket fixture integration

Goal `ESS-S3-HARDENING-001`; repository `main` at committed baseline
`3477bc540c3732dba3d47a7e08779783f9be25fc`, coordinated uncommitted work.
This worker owns only the three test fixtures described here. Production Journal
ownership, kernel locking, safe busy responses and all deadlines remain intact.

`docs/reports/s3-hardening/phase2-umbrella-current.log` records CLI3485/4:
JournalTest's callback-exception test never reached its callback; CopyTest returned
raw initial `journal_busy` with `mutex_port:55363,socket_reason:eaddrinuse`;
NativePutParallelTest returned the same initial failure on57225 and58387 before
expected part messages. The verified-TLS tests passed in this run.

The runner reports `/proc/sys/net/ipv4/ip_local_port_range` as32768–60999.
Journal deliberately hashes root identity and transfer ID into49152–65535.
Those ranges overlap: unrelated listeners, client sockets and TCP TIME_WAIT can
correctly prevent a bind even when ExUnit modules are synchronous. This is an
environment/test-fixture collision, not permission to steal a lock, remove
ownership, change host networking or alter production port selection.

The coordinator assigned and approved these bounded fixture repairs:

- `transfer/journal_test.exs`: before the test, select a fresh private root and
  record whose initial create/claim/release succeeds. Retry only exact raw
  `journal_busy` with two diagnostic keys and `socket_reason:eaddrinuse`, at most
  four attempts. Existing tests reuse that selected ID; the indexed-page test
  creates its remaining two records. Deliberate occupied-port, live-owner,
  equivalent-path, abrupt claimant/owner death and cross-VM tests retain their
  original assertions. Setup retries are not applied to the tested operation.
- `cli/native_put_parallel_test.exs`: a local-file invocation's initial journal
  is acquired before HTTP. Retry only the same raw initial socket collision on a
  fresh private root, at most four attempts. Preserve attempted journals and
  record the selected root for existing assertions. Never retry an error that
  includes acquired transfer identity/cleanup or any subsequent part failure.
- `transfer/copy_test.exs`: Copy.remote first captures and validates its remote
  source over HEAD, then acquires the journal. It would be false to call its
  setup retry pre-HTTP. On the exact initial collision, assert every completed
  wire request is HEAD, close/stop that owned fixture, and retry with a fresh
  fixture/root, at most four attempts. A GET or any mutating request prevents
  retry. Successful wire/checksum/SSE-C/no-source-GET assertions remain unchanged.

Each suite adds a deterministic occupied-initial-index regression proving setup
advances without stealing/closing that listener. Native verifies one initiation;
Copy verifies the final successful fixture has six HEADs,two POSTs,two PUTs and
no source GET. Retried setup only consumed verified read-only HEADs.

Source edits and `git diff --check` are complete. Formatting, compilation and
runtime validation are coordinator-owned and pending at this checkpoint; no
passing result is inferred from source. No worker-owned Mix process or server
was started. All worker shell commands finished. Initial fixture implementation
has not yet consumed a repair-and-validation round.
