# Durable external replication backlog

External S3 disaster recovery jobs are separate from in-cluster RF/W durability.
Inspect their paginated observed backlog using the release runtime:

```sh
/app/bin/ess rpc 'IO.inspect(ExStorageService.CrossClusterReplication.Status.snapshot())'
```

The default scan reads at most ten pages of 100 durable jobs. The report counts
pending, running and failed external PUT/DELETE jobs, excluding completed jobs
and maintenance jobs. `oldest_age_ms` measures the age since creation of the
oldest unfinished external job, including failed jobs; zero means none was
observed. This is an operational observation during a bounded traversal, not an
atomic point-in-time inventory under concurrent mutation.

The page count and returned records are bounded. Concord 3.1.2 native `KV.list`
seeks maintained ordered prefix/range indexes instead of materializing or sorting
unrelated metadata. Expired records and historical tombstones inside the selected
range can still increase work before the page is filled. Legacy `prefix_scan`,
`get_all` and the compatibility state-machine query adapter retain inventory-scale
costs; this upgrade does not prove all metadata writes or scale acceptance bounded.
The diagnostic runs only when explicitly invoked, never on a scrape or as an added
background scan. Native reads share a timeout budget across primary discovery,
quorum-confirmed reads and redirects; a failed RPC is not a fresh observation.

`complete: false` means the scan hit its page bound; its counts and age cover
only the observed portion. A `next_cursor` is retained for diagnosis. Errors
remain errors. Do not interpret a partial scan or unavailable metadata as an
empty backlog. For an explicit larger bounded inspection, pass `page_size`
(1–1000) and `max_pages` (1–100); the defaults remain small.

Each inspection emits fixed-cardinality Prometheus gauges:

- `replication_backlog{state="pending|running|failed"}`
- `replication_oldest_job_age_seconds`
- `replication_backlog_scan_complete`

Counts and age update only after a complete scan. An incomplete/failed scan sets
the completeness gauge to zero while leaving the last complete observations
unchanged. Refresh through the operator call before interpreting these gauges;
they are not a VM-wide background poll or a synchronous scan on each scrape.
No raw object keys, job IDs, endpoints or credentials appear in metric labels.

Pending jobs obey their persisted exponential backoff. A worker must own the
current node/generation/fencing token and unexpired lease to renew or complete
work. Restarting a process does not reset attempts, terminal failure or leases.
Inspect failed jobs before any explicit recovery action; never rewrite job
records to make a health check pass.

Inspect one known failed job without exposing its payload or stored exception:

```sh
/app/bin/ess rpc 'case ExStorageService.Metadata.JobStore.get("KNOWN_JOB_ID", timeout: 1000) do
  {:ok, job} -> IO.inspect(Map.take(job, [:job_id, :kind, :state, :attempts, :max_attempts, :fencing_token, :lease_until_ms, :next_attempt_at_ms]))
  {:error, :not_found} -> IO.puts("job not found")
  {:error, _} -> raise "job lookup unavailable"
end'
```

Replace the literal ID with the job ID retained by the operator. This makes one
exact metadata read and never claims, retries, resets or deletes a job. A
`failed` state requires investigation; do not print the full job record because
its payload and `last_error` can contain object identities or internal details.
Absence is distinct from an unavailable read. Concord 3.1.2 returns structured
availability errors within the native read budget; a failed RPC is not an empty
result. The explicit paginated backlog inspection retains the selected-range
candidate caveat above; no automatic inventory is introduced.

The disposable acceptance lane uses actual public mutations, the ordinary
durable dispatcher, an independently implemented local bearer-compatible peer,
peer outage, and whole-BEAM restart with retained roots:

```sh
bash e2e/run_hardening_replication_backlog.sh /path/to/pinned/python unique-run-label
```

It requires the precompiled production checkout, writes only fresh
`/tmp/ess-hardening-backlog-*` roots, and retains sanitized results plus private
service/control logs. It does not accept production endpoints, increase the
default job attempt budget, or add HTTP client retries. Run this independently
from the heavier bounded-byte external-worker resource profile.
