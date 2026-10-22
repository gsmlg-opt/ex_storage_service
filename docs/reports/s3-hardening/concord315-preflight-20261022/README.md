# Concord 3.1.5 preflight diagnostics

These receipts support admitting the coordinated Concord/VSR/ExTurso 3.1.5 cohort to the unchanged A28 workload. They do not certify A28 or full hardening acceptance.

The published packages were reviewed against the fixes for gsmlg-dev/concord#101 and #102. The preflight pins both Hex checksum layers and five reviewed source hashes. Unknown, mixed, modified, and missing dependencies remain refused.

The retained minimal probe ran public ready-singleton reads and map-form transactions at 1,000, 2,000, and 4,000 unrelated keys. Median read reductions stayed at 114, with no historical protocol validation or full materialization in the measured read path. Transaction branch/idempotency results, Core effects, and compatibility views were checked without full materialization; total transaction work still grew with retained protocol history.

The unchanged production `e2e/scripts/index_setup_probe.exs` ran real ObjectCommit operations at 100, 500, and 1,000 target objects plus unrelated objects, retaining invariants and first/delimiter/absent pagination checks. Minimal and production probes completed in 35.10 s and 20.53 s within the original 300 s deadline. All owned process groups stopped, and the recorded source fence was unchanged.

`run_probes.py` is the exact executed diagnostic wrapper and records its original checkout and temporary output paths. `runtime_probe.exs` is the corrected minimal probe. An earlier instrumentation-only attempt used OTP call_time with argument filters; OTP ignores those filters. The retained successful probe uses argument-aware native call events instead. No dependency source was patched.

The receipts identify the pre-HTTP/2-configuration diagnostic source at HEAD 52e4b57047f67d466b2bed47bd7261d1b9fbb5fe with dirty-source fingerprint 42317cc8771d0be6748d7df91c6e86ac1a5092706218da3a28e4ea3ae7549191. Final-candidate publication still requires the exact committed source's complete extended acceptance workflow, including the original 10k/100k index lane.
