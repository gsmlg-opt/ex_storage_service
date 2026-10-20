# A32 retained-root publication boundary verification

This lane runs an owned standalone BEAM against disposable roots. It invokes
existing per-call LocalCAS/ObjectService fault boundaries, writes a sanitized
boundary witness, and calls `System.halt(137)` inside that process. It waits for
that exact exit, starts a fresh BEAM with the same roots/master key, and verifies
an earlier acknowledged explicit version and all bytes after every restart.

Boundaries are stage, after-stage, file sync, rename, directory sync, after blob
publication, metadata commit and response delivery. Prepublication objects must
remain absent to both ObjectService and a signed independent S3 GET; operation
intents must be absent before staging finishes and pending after the intent opens.
Response-loss must retain exactly one public version, committed intent and the
expected full content SHA256. This exercises service/domain commit boundaries;
response-loss at actual S3 framing is separately retained in Phase2 multipart and
gateway acceptance. Replication failure belongs to the cluster lane, so this
standalone lane alone cannot pass the entire compound A32 row.

Run after strict production compilation:

```bash
bash e2e/run_hardening_crashes.sh /absolute/pinned/python UNIQUE_LABEL
```

Sanitized results are under `docs/reports/s3-hardening/UNIQUE_LABEL/`; private
credentials, master key and RPC inputs remain in the exclusive temporary root.
SIGTERM stops the owned process; no production data or services are inputs.
Initial runs found test-harness errors (RPC tuple match order, inspect truncation,
domain absence atom and operation-id option placement). They are failed runs,
not production defects or acceptance passes. The current lane asserts the exact
intent identity rather than treating an absent unrelated intent as evidence.
