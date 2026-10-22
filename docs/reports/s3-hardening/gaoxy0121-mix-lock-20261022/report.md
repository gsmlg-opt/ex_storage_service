# Bounded Mix lock reproduction, 2026-10-22

Source candidate: `bd0e266c3172c42f916603ac8d7c22dcab1e31aa`. Investigation only: no project source, workflow, dependency or lock-policy mutation.

## Result and limit

The unmodified installed Elixir **1.19.6 / OTP 28 (erts 16.4.0.5)** runtime self-deadlocked on the **second** public `Mix.Sync.Lock.with_lock` acquisition. No compilation, project load, or build workload was run. The SDK path appears in `proof.json`; its BEAM abstract code was inspected and matches the old `try_lock`, `probe` and `switch_file_create!` functions in the retained official v1.19.6 source.

The reproducer asserted distinct host/private network namespace IDs before changing only the private namespace's ephemeral port range to 32768–32769, enabled its loopback, then launched one worker with a fresh task-specific TMPDIR. This intentionally forces immediate port reuse. It is not a reproduction of the complete hosted runner environment.

`proof.json` proves all required identities simultaneously: worker PID 1335470; lock_0 contains port 32769 and PID 1335470; lock_0/port_32769 share inode 12194117 with link count 2; the worker owns listener inode 3359754 plus both established connection endpoints 3359757 and 3359758. The callback prints `TAKEN 1335470 SELF 1335470`. The bounded observer terminated its owned worker and confirmed it stopped. Namespace teardown removes private network settings; no host sysctl or existing cache was changed.

The first bounded probe used eight ports and completed 100 acquisitions without triggering; its observer then raced the exited worker's /proc entry and raised FileNotFoundError after recording exit0/stopped. The observer was repaired to inspect /proc only for a live worker, and the second probe forced the initial port reuse with two ports. This is one instrumentation repair round, not evidence of a project regression.

## Mechanism and historical evidence

In old Mix, the first acquisition hard-links port_P to lock_0. Unlock writes port0 but leaves both names. Reusing P truncates/writes the same inode, replacing lock_0 with the new worker's own PID/port. Its probe then connects to itself and waits indefinitely for its own listener to close.

Hosted Test run 39780739722 at source e754ad83af47934666c79c663eb6cac2077828c0 logged `Waiting for lock on the build directory (held by process 3427)` just after core compilation and before cluster compilation, then runner cleanup terminated orphan3427 (beam.smp). Its steps were sequential; cluster has no custom compiler and the CLI native compiler comes later. There were no historical socket, decoded-lock or process-stack receipts. Therefore this report proves a matching toolchain failure mechanism and a credible explanation, **not a conclusive historical root cause**. Cached _build/deps do not contain Mix's /tmp lock listeners.

## Official upstream status

- Issue https://github.com/elixir-lang/elixir/issues/15761 was opened for empty lockfile MatchError, a distinct symptom sharing the stale hardlink mechanism.
- Later comment https://github.com/elixir-lang/elixir/issues/15761#issuecomment-5316152984 separately describes/proves the self-connect deadlock; its exact text is retained.
- PR https://github.com/elixir-lang/elixir/pull/15765, merged2026-08-18 as b703302cd1fc91dd1b2df51659daf9b8d6053776, fixes stale port files, performs takeover cleanup on the first acquisition, adds own-port rejection, and changes the lock namespace to v2.
- Current tag list returned v1.19.6 as latest1.19 and v1.20.4 as latest stable. Direct source checks of v1.19.6, v1.20.3 and v1.20.4 found neither own-port rejection nor v2 namespace. GitHub ancestry comparisons also show the merge commit is not an ancestor of those tags. Main contains the fix. Do not recommend a released fixed toolchain from these checks.

## Bounded mitigation check and operating conditions

Official v1.19.6 `Mix.Sync.Lock.with_lock` docs (lines92–94) document that falsy MIX_OS_CONCURRENCY_LOCK ignores this lock; `lock_disabled?/0` (line137) accepts exactly `0` or `false`. Official `Mix.Project.with_build_lock/2` (project.ex lines1000–1016) delegates to that API.

`mitigation.exs` called **Mix.Project.with_build_lock/2 twice** on a fresh private build path with MIX_OS_CONCURRENCY_LOCK=0, using the exact SDK in a fresh private network namespace and TMPDIR. Both callbacks completed; both observed **zero Mix lock files and zero TCP listeners**; process exit0 (`mitigation-result.json`). No project build was executed. This proves the documented opt-out bypasses the defective path, not that arbitrary parallel builds are safe.

A narrowly scoped opt-out is appropriate only for isolated hosted jobs with exclusive checkout/build directories and strictly sequential Mix invocations, including any descendant commands. It removes cross-process exclusion for both build and dependency locks; it must not apply where multiple OS processes may access the same build/deps concurrently. It does not alter ESS native journal locks, application locking or production behavior. Those boundaries must remain intact. No mitigation was implemented by this investigator.

Alternative: a verified official fixed toolchain when released. A new private TMPDIR per sequential Mix invocation does not prevent multiple acquisitions within one Mix process and therefore is not a demonstrated remedy. Relaxing port reservation merely lowers collision probability and is not a correctness fix. No reservation change is recommended from this reproduction.

## Commands and shutdown

Runtime version: `/tmp/ess-elixir-1.19-otp28-format/bin/elixir --version`.
Reproducer: outer `timeout25s`; `unshare --user --map-root-user --net python3 repro.py`; child deadline12s, terminate/wait3s with owned process-group kill fallback. The outer coordinator supplies ESS_PROBE_HOST_NET_NS from the actual host /proc/self/ns/net. TMPDIR must be fresh when rerunning.
Mitigation: `timeout15s unshare --user --map-root-user --net ...`, child Python subprocess timeout10s, environment MIX_OS_CONCURRENCY_LOCK=0 and fresh TMPDIR. No source or dependency patching and no project Mix command.

All investigator commands and owned workers finished. Worker1335470 is stopped. Sol repair rounds:1 instrumentation repair (the first /proc race); no production repair. No Astra repair counter. No acceptance/release claim.
