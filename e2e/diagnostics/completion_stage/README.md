# Conditional multipart completion stage diagnostic

This manual diagnostic wraps only the second `synchronized` call in the original
`e2e/s3_hardening_phase1.py`: the 32 conditional multipart completions. It delegates
the same operations and admission object to the original barrier. Requests,
authentication, SDK retries, checksums, assertions and subsequent profiles remain
the original harness's responsibility. Other Python invocations use `os.execv`
with their original arguments. Run only against the disposable local three-voter
fixture; the controller connects to its two fixed data-node names and cookie.

Set the existing private instrumentation variables before passing the executable
`python-launcher` as the original harness's Python argument:

- `ESS_DIAGNOSTIC_PINNED_PYTHON`: absolute executable Python in a real virtualenv.
- `ESS_DIAGNOSTIC_EVIDENCE`: absolute fresh diagnostic output directory, created
  by the caller. Source files are always loaded relative to this directory of
  scripts, never from the output directory.

Each node has an isolated OTP trace session with exact local MFA patterns and
arity-only call events. The six stages cover multipart preparation, existing-blob
commit, operation-intent open, replica assurance, object commit and Concord
transaction. A seventh pattern projects only the storage-error reason. Output
contains fixed stage names, numeric process aliases, monotonic timing, classified
returns and the boolean transaction compare result. Reasons retain only
allowlisted atoms and tuples at most two levels deep; all other values become
`other`. Arguments, connection structs, object identifiers, headers, credentials
and raw return payloads are never serialized.
The tracer redacts GenServer status reports and suppresses its own process's
logging, including separate OTP crash reports that could contain queued terms.
Application logging remains enabled.

Tracing stops after 20 seconds or 4,096 consumed trace events per node, or an
explicit stop. It destroys only its own session, drains trace delivery, syncs and
closes the JSONL, then writes a private `*.jsonl.stopped.json` receipt. Start/stop
RPC acknowledgements are separate receipts. All output files use mode `0600`.
The workflow's streaming log filter forwards seed mask commands to the Actions
command processor, excludes them from retained bytes and redacts later matching
values. An explicit Bash pipeline preserves the original harness failure status.
The post-cleanup receipt checks reserved EPMD registrations only; it is not an
independent final OS-process identity receipt. Full server lifecycle logs stay
outside this diagnostic artifact.
Automatic stopping, truncated stages, unmatched returns and failed setup must be
retained when interpreting evidence. Diagnostic success cannot certify acceptance
or authorize publication; a non-reproduction does not replace the original failed
hosted receipt.

The isolated privacy/return/cleanup fixture defines fake modules in a fresh Elixir
VM and does not start Mix, ESS, networking or an acceptance workload:

```sh
PAGER=cat elixir --erl '-logger handle_otp_reports true handle_sasl_reports true level debug' \
  e2e/diagnostics/completion_stage/test_trace.exs
PAGER=cat python3 -B e2e/diagnostics/completion_stage/test_output.py
```

It checks seven sanitized events, the default local preparation wrapper, preserved
`compare_succeeded: false`, bounded reason projection, private file modes, stopped
tracer identity and survival of an independently owned trace session. A queued
private sentinel appears in a control crash with process-level protection removed;
the protected crash excludes it while parent logging remains visible. SASL/debug
settings apply only to this isolated fixture VM. Its unique
temporary output is removed only after the diagnostic tracer has stopped.
