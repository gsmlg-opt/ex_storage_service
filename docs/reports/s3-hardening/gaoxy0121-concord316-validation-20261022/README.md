# Gaoxy 0.12.1 / Concord 3.1.6 validation

Full acceptance failed. Minor release v0.7.0 was not dispatched or published.
The changes use Gaoxy for Phoenix, public S3 and private cluster HTTP, and update
Hex/npm dependencies. `mix hex.outdated` and the requested
`npx npm-check-updates@latest -w` both completed successfully with no further
updates. The locked Gaoxy/HTTP/Concord families are 0.12.1 / 0.21.16 / 3.1.6.

## Tested identity and original artifacts

[Full run 39795784957, attempt 1](https://github.com/gsmlg-opt/ex_storage_service/actions/runs/39795784957)
tested clean `main@6a356c3c90bee76ee6338774efa848b30390b215`, fingerprint
`129879f910d5c9c22d6504574b150f85c3e5209f5e619fad7a08ea0712debe09`.
It completed with conclusion `failure`. [receipts.json](receipts.json) retains
allowlisted command exits, original receipt/log hashes, artifact IDs/digests and
the reviewed aggregate guard outcome. Raw logs remain private because some
contain generated credential masking directives.

The 12 downloadable artifacts cover daily and all 11 extended lanes. The shell
aggregate guard saw `LANE_RESULT=failure` and exited 1 before invoking Python
verification. There is no aggregate or candidate publication-verification
artifact. Resilience's command receipts, all steps and complete logs passed;
its final job/check-run API still reported `in_progress` at the recorded audit,
despite the completed failed parent run. This inconsistency is retained.

## Results and remaining failures

| Check | Observed result |
| --- | --- |
| Daily authenticated HTTP | 14 groups passed; default SDK checksums and raw gzip bytes retained; cleanup confirmed |
| Extended application correctness | 4,543 executed, zero failures/skips: core 606, transport 43, CLI 3,491, S3 379, web 24 |
| Tagged cases | Excluded three-voter and actual 2 GiB stream tests passed in separate lanes |
| Python harness | Reported 158 tests, zero failures, one skip; runtime output did not name the skipped test |
| Passing lane receipts | correctness, standalone, gateway, faults, crashes, resilience, restore, release-artifact |
| Cluster | Child exits `[0,0,1]`; multipart race included six HTTP 503s; later concurrent version PUTs returned 503 after retained-root restart |
| Index | Child exits `[0,1,0]`; A28 failed, independent A29 migration/rejoin passed |
| Resources | Child exits `[0,1,1,0]`; both resource matrices incomplete; separate actual 2 GiB stream passed |

The failed `cluster_e2e.py` command did not reach its later data-node
failure/recovery or primary-loss/election/rejoin/persistence stages. Successful
independent profiles do not substitute for those unexecuted stages.

A28's 10k primary build, listing-before, unrelated growth and listing-after completed in
49.917274 / 17.417997 / 77.309097 / 21.685537 seconds. The primary 100k build
completed in 726.005336 seconds; inventory/listing-before took 3.565434 /
23.287840 seconds. During unrelated 100k growth, the last recorded progress was
52,900/100,000 at 645.157268 seconds. A later operation returned `{:error,
:timeout}` from the ObjectCommit setup path; its exact failed ordinal is not
identified. A28 exited 1 after 1,577.418 seconds, without a complete result file
or post-growth 100k measurements. A29 independently passed in 18.52 seconds.

Standalone resources completed 25 clean groups, then failed group 26/56
(`gateway-slow-put`, 64 MiB, concurrency 4). Cluster completed six clean groups,
then failed group 7/20 (`authenticated-peer-get`, 64 MiB, concurrency 4) on the
metadata node's coverage. All executed operation assertions and numeric limits
passed, and stages ended at zero. Thirty standalone and thirteen cluster groups
remain unexecuted. A standalone temporary-tree probe took 205.897168 ms, with a
229.956091 ms conservative gap. The cluster metadata queue scan took
87.377654 ms, with 138.543913 / 109.797989 ms conservative gaps. These are distinct
observations. No matching syscall/scheduler trace identifies their causes, and
no sampler or 100 ms requirement was changed. Independent recomputation checked
47 native measurement records and all four complete exports/observer deaths.

## Reproduced upstream blocker

[Concord #105](https://github.com/gsmlg-dev/concord/issues/105), type `Bug`, label
`internal request`, severity `blocker`, includes both exact executed standalone
scripts, published package/source hashes and the fresh-consumer recipe. Legal
durable recovery and actual emitted event/token flows preserve exhaustive public
state/effect parity and final invariants. Native observations and separate
call-count brackets exclude recovery, parity and final validation work.

| Pure runtime diagnostic, 1k to 10k retained history | Observed work |
| --- | --- |
| Three voters, five writes / 50 normal events | Recursive applied-index calls 80,275 to 800,275; five replies and 15 applies; all nodes end at N+5 |
| Singleton, five current emitted heartbeat tokens | Recursive applied-index calls 5,005 to 50,005; no writes/applies/replies; history and client table unchanged |

Each measured event invokes the exhaustive fallback and rebuilds the runtime
history/index. The first canonicalization converts indexed history; additional
canonical calls may receive a list. The singleton timer advertises the default
500 ms delay; the probe delivers logical ticks and measures no real timer rate.
Parity runs in the same process, so later GC can affect diagnostic wall/reduction
measurements. These observations establish history-dependent work. They do not
prove the cause of CI's HTTP 503s, A28's timeout, the separate local 10-second
cluster read timeout, or resource coverage gaps.

All local diagnostic commands finished, owned children/groups were reaped and
cluster listeners closed. No dependency patch or local workaround was applied.
The required callsite TODO is the only subsequent Elixir source change and has
no runtime behavior change; these results remain bound to the tested identity.
The historical C7 machine ledger was not promoted. Resume publication work only
after adopting an upstream fix and passing complete current-source acceptance.
