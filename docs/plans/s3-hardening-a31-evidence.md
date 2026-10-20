# ESS-S3-HARDENING-001 A31 fault source checkpoint

Baseline/current HEAD `3477bc540c3732dba3d47a7e08779783f9be25fc`, shared dirty
`main` checkout. No production data, credentials or processes are inputs.

New runtime lane (requires production compilation by the coordinating worker):

```bash
bash e2e/run_hardening_faults.sh /absolute/pinned/python phase3-a31-r1
```

Runtime status at this checkpoint: **unexecuted**, not acceptance passed.
The coordinator owns Mix compilation and all server runtime windows.

Required assertions implemented in the lane:

- Real SigV4 local/gateway PUT declares8MiB, sends64KiB, confirms server staging
  began, then closes the owner socket. The old local body remains byte-identical;
  gateway partial bytes never produce upstream PUT or a cache/object entry.
- Proxy closes a successful upstream GET after64KiB while retaining its real
  full Content-Length. The client must fail, cache must remain unindexed, and
  temporary files must disappear within20s.
- Local/gateway downstream reader closes after64KiB; subsequent full reads
  retain SHA256 identity and no staging/process growth remains after20s.
- Owned BEAM is killed/restarted after faults using retained roots. Local old
  body and subsequent gateway body remain exact and staging is empty.
- A separate owned BEAM uses a private8MiB tmpfs for CAS plus staging on the
  same filesystem; metadata/evidence remain outside it. Fill is bounded16MiB
  maximum and must return real ENOSPC. Failed local/gateway PUT cannot replace
  the prior local object or reach upstream. Removing the filler permits a new
  successful write/read without restarting.
-25ms telemetry must retain queue maximum<=128 and total<=2048 throughout the
  main connection-fault workload. Post-fault process count is baseline+32 or
  lower; every temporary-file count must settle to zero within20s. These are
  test assertions, not claims of mathematical resource bounds.

Existing focused HTTP coverage complements this lane:
`GatewayReadHTTPTest` cold-range identity and open-response cache eviction;
`LocalStoreConcurrencyTest` descriptor remains readable after eviction unlink;
`LateStreamHTTPTest` late failures terminate responses. These require actual
coordinator execution and must be audited together before marking A31 passed.
Cluster replica/repair/replication cancellation is not covered by this lane.

Resource runner SIGTERM now stops its owned BEAM and releases its fault proxy
before raising, so executor joins wake from pending sockets; normal finally
retains result/samples and removes the owned container. No production policy
or checksum/authentication behavior changed.

Actually verified without Mix/server execution:

- `bwrap --ro-bind / / --size8388608 --tmpfs /tmp` with a bounded Python writer:
  private tmpfs returned ENOSPC after8 complete1MiB chunks. Host paths untouched.
- Python AST and shell syntax checks passed at the source checkpoint.
- `python3 -m unittest e2e.test_hardening_acceptance e2e.test_gateway_fault_proxy`:
  six tests, zero failures. The proxy contract proves an actual Content-Length
  mismatch raises `IncompleteRead`, and clearing the fault restores exact bytes.
- `bash -n` on fault/crash wrappers; Python AST; workflow YAML via `Bun.YAML.parse`;
  `git diff --check`: all exit zero.

The aggregate now independently requires `faults`, `crashes` and `resilience`
lane records, making eleven required lanes. Relevant CI pushes/PRs include all
three; scheduled/manual full verification requires all eleven plus38 audited
matrix rows. The crash lane
does not establish the replication portion of A32.

Controller follow-up: both the inherited gateway `Runtime.control` and A31
`FaultRuntime.fault_control` replace subprocess exit/timeout exceptions with a
generic diagnostic using `from None`; argv contains the private BEAM cookie and
must not appear in public tracebacks. Child output remains in private control
logs. `python3 -m unittest e2e.test_hardening_acceptance
e2e.test_gateway_fault_proxy e2e.test_hardening_control_redaction`: seven tests
passed, including four failure/timeout redaction subcases against the actual
methods. Python/shell/workflow syntax and `git diff --check` also passed.
No Mix/server runtime was executed for this follow-up.

## First runtime fault and reproduced response-header defect

Coordinator run `phase3-a31-r1` passed both signed upload-disconnect checks,
then failed on the truncated upstream GET: Botocore raised
`AttributeError: 'StreamingChecksumBody' object has no attribute 'strip'`.
This was not accepted as a successful error response.

Source tracing found gateway reads attach upstream HEAD object headers before
fetching the body; a download failure reused them on XML errors. Botocore's
checksum handler saw the object's checksum on a503 XML response, wrapped its
error body, and its XML error parser failed. Object length/encoding/metadata
also had no valid meaning for that error body.

The coordinator's first reproducer run failed on an incorrect500 expectation;
the documented existing mapping is503 ServiceUnavailable. Corrected tests then
gave meaningful RED evidence in `phase3-gateway-error-headers-red-r2.log`:
11 tests,2 failures, specifically stale Content-Length in `ErrorFramingTest`
and stale x-amz-checksum-crc32 in real `GatewayReadHTTPTest` truncation.

Targeted source repair in `Shared.error_response` removes only object payload
headers (framing, encoding/range/disposition/language, caching, ETag, checksum,
user metadata) before sending XML. Version/delete-marker, Last-Modified and
request diagnostics remain. Valid object checksums and default SDK verification
are unchanged. A31 now requires both signed raw503/XML/framing assertions and a
normally parsed default SDK503/ServiceUnavailable; no broad transport/SDK error
is swallowed. Scoped GREEN and full A31r2 remain coordinator-owned and pending
at this source checkpoint. Seven Python contracts/diff checks pass.

Coordinator A31r2 passed all six connection-fault/restart checks, including both
raw and default SDK parsing of the truncated-response503. The ENOSPC subservice
then failed before readiness because the Elixir shell launcher could not write
`/dev/null` through the read-only root mount. No ENOSPC acceptance was claimed.
`FullDiskRuntime` now supplies private `--dev /dev --proc /proc` mounts while
retaining the read-only host root, only its disposable writable root, and exact
8MiB CAS/staging tmpfs. A no-BEAM prerequisite probe confirmed writable private
`/dev/null` and real ENOSPC after eight1MiB chunks. Python AST/diff checks pass;
the complete S3 ENOSPC rerun remains pending with the coordinator.

## Actual ENOSPC cleanup defect

Coordinator A31r6 again passed all six connection/restart checks and reached
real isolated ENOSPC. It failed after the20s cleanup deadline with two leaked
zero-byte stages, process count572, queue maximum/total0. The private disk-full
BEAM log traced `ErlangError :enospc` from `IO.binwrite` through
`LocalCAS.write_reader_chunk`; the raised filesystem error bypassed the reader
error-state/close/remove path.

Added source-only RED contracts in `local_cas_io_failure_test.exs`: actual
`FileSystem.write` on a closed raw descriptor must return a file-error tuple;
reader staging using that boundary must retain the consumed reader state,
attempt descriptor closure and remove staging without blob publication. This
portable contract requires no `/dev/full` platform device. The coordinator owns
RED execution before the narrow write-boundary repair; no production repair
or passing S3 ENOSPC result is claimed at this checkpoint.

Coordinator RED confirmed in `phase3-local-cas-io-red.log`: both new contracts
failed at `IO.binwrite` with `:einval`. The targeted production repair changes
only `LocalCAS.FileSystem.write/2` to `:file.write/2`, returning filesystem error
tuples through the existing state-threaded staging and cleanup path. No broad
exception catcher was added. Scoped GREEN, production compile and the complete
S3 ENOSPC rerun remain coordinator-owned and pending.
