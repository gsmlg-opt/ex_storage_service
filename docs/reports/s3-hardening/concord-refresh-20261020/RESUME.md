# Current resume authority — daily-use scope, 2026-10-20

The user explicitly reduced the current gate to normal daily-use testing and
excluded scale, extreme-case and high-concurrency tests. The revised daily gate
passed all 14 groups on source `f854dedc…`, with complete disposable cleanup.
Resume from [the daily handoff](../daily-20261020-normal-r3/RESUME.md).
Do not resume the historical A28/A30 workloads under the current scope.

The checkpoint below remains historical evidence. Its 35/38 result, failed
checks and unexecuted cases are preserved; the new daily result does not pass
the original complete matrix.

# Historical Concord3.1.3 terminal checkpoint

Concord/VSR/ExTurso3.1.3 installed/locked; HTTP0.21.13 unchanged. Main start/end
3477bc540c3732dba3d47a7e08779783f9be25fc, preserved uncommitted implementation
60ada0faafb210257f799f275f6709be0830dd4168b27e093ca2572eb6eac781.
No active application runtime/build or worker. Both sessions48081/11177 are reaped.
Latest integrated result: CONCORD_UPDATE.md and final-checkpoint-r3.json.

35/38accepted: A01–A27,A29,A31–A37. Phases1/2 complete; Phase3notaccepted.
Current correctness4496/0,24Python and40additionalresource regressions, complete
normal standalone/gateway/three-voter business, faults/crashes/resilience,
independent standalone/cluster restore, actual2GiB, independentA29 and production
assets/release/container/defaultprod gates passed. See their current lane receipts,
row-promotion-r1/review.json and business/recovery/tagged-index summaries.
Final actual aggregate-checkpoint-r3.json exited1: A28/A30/A38in_progress,
completeindexreceipt absent and resources incomplete. No lane/row pass synthesized.

A28: accepted a28-current-handoff-r1/README.md and a28-architecture-decision-r1 remain.
Original normal-invariant10k/100k timed out10800.086s without complete measurements.
Current3.1.3prefix2958puts/170.843s reached no10kmeasurement. Increasing latency
is measured; initiating timeout cause unknown.3.1.3runtime unchanged from3.1.2;
latest currency-r3 at18:47UTC found no newer coordinated release;99/100open/needed,
not proven timeout causes. Static targeted ESS audit found no small retained-state
traversal candidate. Re-entry requires evidenced released/ESS improvement, then all
original measurements; no unchanged3h retry, rawKV/fakefixture/fabricatedbackup,
new batching feature solely to finishfixtures, dependency patch or increaseddeadline.

A30: canonical11044reapedexit1; productioncompile0; local39/56passed, group40failed
up to150.506ms metricgap,16unexecuted; cluster15/20passed, group16failed temporary
gap108.734ms,4unexecuted. All88recordednumericbounds passed; missing coverage and
readbacks remain unpassed. Exact replay and canonicalcleanup285identities/137groups
passed. See a30-final-audit-r1/REVIEW.md and canonical-resources-summary-r1.json.

Mode-copy failure48081was beforegroup1; immutable r2startup reports retain EACCES,
noobserver/tracerexecution and failedcleanupverdict. Corrected/bootstrap0700r3
session11177reapedexit1:32groupspassed, group33local-upload-part-copy1GiB/c1 failed
four100mscadencechecks,23unexecuted. All33numericenvelopes and exactrawreplay passed;
6readbackgroups15objects45assertions. Native-memory-end to Python-RSS-start window
91.572960ms is unclassified. Target40notentered;61542events/5006109Bdiscarded,
0retained. Trace cause remainsunknown; no automaticretarget/retry is justified.
Originalcleanup and independentverifier remainfalse because target40attachment
records absent. Separate actualidentityproof confirms observer/tracer, all11tracked
identities/6groups,1528tasks,5listenerinodes and exactfixture gone,0readerrors.
Offline analyzer UNEXECUTED: unchanged cleanup/target40trace prerequisites fail.
See a30-local-r3-terminal-summary.json and r3 TERMINAL_REVIEW/INSTRUMENTATION_CLEANUP.
Inherited instrumentation Solrepairs2; documentationrepair1. No more runnerrepair
or runtimepreparation requested; allcommandscomplete. New facts are in reports only.

Next unblock conditions: tested upstream/native commit improvement or evidenced
small ESSfix for A28; reliable original100ms observation on a controlled runner or
decisive causal capture for A30; then complete original56+20resource profiles,
100kmeasurements and finalaggregate. Do not repeat unchanged commands to chase green.
A31+restore/release passes remain accepted and need no unchanged reruns.

Use literal /tmp/ess-s3-hardening-python/bin/python. Preserve MIX_HOME/HEX_HOME and
ERL_FLAGS='+S 4:4 +SDcpu 2 +SDio 2'. Preserve everydirtychange, auth/checksums/RF2W2,
original sizes/concurrencies/limits/deadlines/100mscoverage. Commandversiondefault0
for existingdatabases; emptyrestoretargets explicit1 on everyvoter through recovery,
allrestarts andactivation. No branch/commit/push/publish/deploy/productionoperations.
