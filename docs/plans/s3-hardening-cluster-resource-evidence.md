# Large cluster transfer evidence

P3.2 requires bounded remote replica, repair and asynchronous replication paths.
A30's local/gateway matrix alone does not close that requirement. Existing real
transport contracts use 4 MiB incremental uploads and 16 MiB cancellation;
resilience repair explicitly limits its fixture to 4096 bytes. The replication
crash lane proves a real durable acknowledgement with a roughly 233 KiB object.
Those checks retain their correctness purpose and do not prove large-transfer
resource bounds.

Run from the repository root, after the coordinating agent has compiled the
current production source and ensured that other cluster lanes have stopped:

```sh
bash e2e/run_hardening_cluster_resources.sh /absolute/pinned/python phase3-cluster-resource-r1
```

The wrapper refuses occupied fixture node names, creates a fresh private root,
starts the existing authenticated three-voter fixture, and stops its owned nodes
on exit. No deployment or production roots are used. Expected evidence is
`docs/reports/s3-hardening/<label>/results.json`, per-node raw samples under
`<label>/samples`, and the three retained lifecycle logs. The private temporary
root holds controller logs and credentials; do not publish its contents.

The lane has 20 mandatory groups: each of the following runs at 64 MiB and 1 GiB,
with concurrency 1 and 4:

| Group | Actual transfer and proof |
| --- | --- |
| strict-quorum-replica-put | Signed public PUT stages and replicates under RF=2/W=2; both node CAS files and ready generation-1 locations must match the source hash and size. |
| authenticated-peer-get | Production internal HTTP adapter opens a peer source; a bounded reducer checks received bytes and SHA-256 through a 32 MiB/s download proxy. |
| fenced-repair-pull | After both copies are verified, remove only the generated fixture CAS file on data-a and mark that location unavailable. Production Repair.Handler reads data-b over authenticated paced HTTP, stages locally and updates locations under a live durable job fence. |
| fenced-repair-push | Remove the verified fixture file on data-b; production Repair.Handler on data-a streams its local source over authenticated paced HTTP PUT to data-b. Both CAS files and locations must verify afterwards. |
| cross-cluster-worker-paced-sink | Durable cross-cluster job calls the production Handler and Worker with no request/source callbacks. An independent bearer-authenticated sink consumes at 32 MiB/s in at most 64 KiB chunks and independently verifies length/SHA-256. HEAD always misses, so idempotency cannot skip the measured upload. |

Every concurrent payload has a distinct generated SHA-256. Clients use files and
bounded reads; payload generation never makes an object-sized binary. One fixed
set of object keys per size/concurrency keeps the metadata population small and
does not turn this lane into a listing-scale test.

For deterministic transfer measurement, only the private fixture's ordinary
outbox dispatchers are suspended while test-controlled durable jobs are claimed,
executed by the unchanged production handlers, and completed. Claims retain
owner data-a, generation 1, a live lease, and the fencing token. The real transport
wrapper changes only the peer URL to the bounded transparent pacing proxy;
authentication, descriptor checks, topology checks, and RF/W remain enabled.
This measures handlers and transport; dispatcher lease renewal, backlog, retry
and stale-worker acceptance remain the separate A35 resilience requirements.

All three nodes reuse `resource_sampler.exs` at 25 ms. Each group retains warm
baseline, active peak, post-idle state, binary/total BEAM memory, RSS, mailbox
maximum/sum, and temporary bytes. Limits are exactly the predeclared
`docs/plans/s3-hardening-resource-envelope.md` values, applied per node. Retained
CAS file counts/bytes and host page-cache counters are reported separately.
Each pre-group, post-group and final staging file count must be zero, including
zero-byte files; post-idle sampled temporary bytes must also be zero. A missing active
sample, wrong byte count/hash, failed job/fence, missing ready replica, or any
envelope excess fails the lane.

Implementation source checks only: Python compilation and shell syntax checking
passed. The coordinating agent must parse/format the Elixir controller and run
the full real fixture. **No runtime acceptance is claimed by this document.**
In particular, a predicted default HTTP deadline interaction with 1 GiB paced
transfers must be reproduced and fixed if observed; do not change the workload's
size, pace or declared limits to hide it.
