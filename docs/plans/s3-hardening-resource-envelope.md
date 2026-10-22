# Predeclared resource acceptance envelope

This is an acceptance specification, not measured results. Record runner CPU/RAM,
filesystem/free space, OTP/Elixir/client versions, configured concurrency, baseline,
and actual measurements before claiming A28/A30. Mandatory sizes are67108864 and
1073741824 bytes, each at concurrency1 and4; run every supported path below.

Closeout instrumentation retains separate scheduled, probe-start, collection and
per-metric monotonic windows. Each row also records its BEAM request/reply window
and the preceding persisted row's completed JSON encoding/output-write windows,
byte count and thread CPU time. The first preceding export is null; the final
export has no following row. Collection of a row may precede the prior export's
completion. These timings attribute observation gaps without interpolating
missing samples or changing the100ms coverage criterion.

C2 proved that synchronous output stalls delayed fresh observations: the failed
local/gateway and cluster receipts remain immutable. The repaired observer uses
one writer thread and a fixed128-row FIFO, at most16KiB per encoded row (2MiB
queued payload plus one in flight), predeclared before the fresh acceptance run.
Capacity is checked before requesting another observation; no sample is silently
discarded. A transient blocked export must preserve actual fresh observations;
a sustained stall must fail bounded admission. EOF must drain within5seconds,
and strict cleanup requires a generation-bound successful terminal manifest
with admitted==persisted, contiguous sequences and bounded output. This observer
buffer does not change the service memory envelopes or permit absent samples.

Case receipts compare exact `(path, size, concurrency)` identities with the
current local/gateway and cluster manifests, rejecting missing, duplicate,
unexpected and failed groups. A passing numeric envelope for a partial manifest
does not establish A30. Record inherited SDK checksum settings with runner/client
versions; default checksum behavior remains enabled.

## Streaming model

The hosted decoded request reader pulls at most1MiB per read. LocalCAS writes each
chunk before pulling another. Source file delivery reads256KiB. Outbound HTTP
splits upload chunks into64KiB acknowledged units. Gateway GET stages a validated
raw upstream response on disk and acknowledges only after bounded sink delivery;
it opens the staged/cache descriptor before response/eviction. Gateway PUT stages
and validates before any upstream publication. CAS files, disk stages, and cache
files are separate from BEAM binary memory. Concurrent replicas use the existing
bounded admission path and never lower RF/W.

A warm process must satisfy these fixed, object-size-independent peak increases
above its immediately preceding idle baseline (c is active client concurrency):

| Measurement | Maximum increase |
|---|---:|
| BEAM binary memory |64MiB +32MiB*c |
| BEAM total memory |128MiB +48MiB*c |
| Process RSS |256MiB +64MiB*c |
| Any relevant worker/HTTP process mailbox |128 messages |
| Aggregate relevant worker/HTTP queued messages |2048 messages |

These allowances cover multiple request/response/replica chunks, checksum state,
HTTP/parser state, process heaps, and ordinary warmed runtime/allocator overhead.
They do not allow buffering a1GiB object. The same limits apply to both object
sizes. Report each baseline and delta; distinguish BEAM binary/total memory from
RSS and OS page cache. Sample at100ms or faster and retain samples plus peak and
post-idle values. A bound violation is a failure requiring diagnosis, not an
invitation to scale the threshold with object size.

Temporary disk is permitted to scale with active bytes: at most two owned staged
copies per active object plus64MiB overhead. CAS/cache retention and disposable
upstream data must be measured separately from active temporary files. Verify
post-cancellation and post-idle cleanup, preserving published CAS/cache data and
unknown-outcome durable intents. A completed upload's staging file cannot remain
permanently leased. Document configured cache capacity and eviction behavior.

Run local PUT/GET/COPY, file-backed multipart upload/completion, full-range
UploadPartCopy/completion, gateway-source COPY to hosted storage, gateway
PUT/cache-miss/cache-hit, and throttled upload/download
peers; verify actual streaming hashes without loading a complete object into the
client. Cluster acceptance covers remote replica/repair transfer and strict RF/W.
Repeat with client disconnect/cancellation and upstream interruption. Record queue
and memory behavior during backpressure, not only at completion.

## Ordered listing model

Mandatory10k/100k metadata fixtures must use the same transactional index updates
as normal commits and include multiple buckets, versions/deletes, unrelated keys,
and a large common-prefix group. Test page sizes0,1,10,100,1000. The steady-state
metadata/index visited-node count must be bounded by index depth plus page visits,
including seeking past common prefixes. No global snapshot/full-bucket sort is
permitted in the measured request path. Record actual reads/allocations, index
bytes, and at least30 measured p50/p95 samples after warmup. Demonstrate that
increasing unrelated bucket size does not increase visited records. Startup and
resumable backfill are measured separately and cannot be hidden in each request.

Current source uses the transactional radix index for public object/version/upload
listing, with exact root revision barriers and guarded offline backfill. Released
Concord/VSR/ExTurso 3.1.5 passed the preflights for the earlier 3.1.4 #101/#102
blockers. Mandatory 10k/100k execution with the unchanged read/byte/heap gates
remains incomplete; the interrupted run does not establish a scale pass or
assertion failure. Retained-history write CPU work is tracked as needed in
`gsmlg-dev/concord#104`. The independent cluster backfill/interruption/rejoin/cutover
fixture and smaller diagnostics do not certify the scale datasets. See the
[3.1.5 preflights](../reports/s3-hardening/concord315-preflight-20261022/README.md)
and [write CPU evidence](../reports/s3-hardening/concord315-write-cpu-20261022/README.md).

Before the upgraded scale run, the fixture must validate encoded keys<=160B,
radix height<=16, serialized nodes<=2KiB and hydrated records<=4KiB; flat heap
limits are8KiB/node and16KiB/hydrated record. For page size P>0, reject reads
above33*(P+1)+8, returned bytes above78,080*(P+1)+34,816, serialized output
above8,192*(P+2), or output flat heap above20,480*(P+2). Empty pages have separate
fixed bounds. These gates apply identically before and after unrelated growth.

The fixed128MiB request-heap ceiling exceeds a99,147,776B fixture-derived budget:
page/container20,520,960B, two seek paths262,144B, input32,768B and fixed1MiB,
multiplied by3 for live/transient/GC copying, plus32MiB runtime reserve. Record
GC/sample-observed heap capacity with an independent VM heap guard; do not call
it exact instantaneous process memory or cumulative allocation. Argument-free
isolated trace sessions inspect the owned request and actual local replica;
every query boundary must be an exact get_record, match wrapper read counts and
contain no forbidden scan/materialization API. A512-message collector threshold
is a stop condition. The original30 samples, six query kinds, page sizes and
10k/100k ObjectCommit fixtures remain required. Detailed derivation and native
source audit: docs/reports/s3-hardening/index-acceptance-bounds-20261019-r1/README.md.

Steady listing samples run in the disposable fixture with only
`Concord.Telemetry.Poller` acknowledged suspended. Its independent periodic
status query reaches the same native replica and performs a forbidden stats
query; the real overlap/control diagnostic is retained under
`docs/reports/s3-hardening/a28-forbidden-boundary-20261019-r1/`. The replica and
engine remain active, all native guards still apply, and `after` requires the
poller to resume on success and failure. No messages or timers are removed.
These measurements exclude that independent background statistics workload;
they do not claim its global work is bounded or removed from production.
