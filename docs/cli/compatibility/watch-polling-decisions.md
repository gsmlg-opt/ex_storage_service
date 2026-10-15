# Explicit polling watch

`watch` requires `--adapter polling`. It compares complete local-directory or
remote bucket/prefix metadata inventories. It does not configure bucket
notifications, consume a notification queue, or claim event delivery guarantees.
Local traversal does not follow symlinks. Remote scans complete every listing page
before comparing a snapshot. Private disk manifests use bounded external sorting;
the comparison holds at most two inventory records and streams events individually.
The snapshot temporary directory must be outside a local watched directory so the
watcher cannot inventory its own files. Set `--tmpdir` to an external directory
when watching the normal system temporary root. Invalid placement fails explicitly.

The initial inventory is a silent baseline. `--interval` is an integer number of
seconds from 1 to 3600 (default 1), measured after the previous scan/reconciliation
finishes. `--count` limits completed scans after that baseline; 0 means unlimited.
An unchanged scan counts. Idle waits check invocation cancellation at intervals of
at most 50 milliseconds. Inventory/network work retains its own cancellation
contract; this is not a latency guarantee for an active provider request.

Each event contains `Adapter: polling`, `Event: create|remove|modify`, `Name`,
`Before`, and `After`. Names are ordered within one comparison. A modification
means the visible inventory metadata changed. Polling can miss intermediate
changes, changes reverted between scans, and content changes that retain identical
listed metadata/local file identity. There is no replay cursor, cross-scan ordering
guarantee, deduplication promise, or exactly-once delivery. Restarting establishes
a new baseline. A failing output callback may have consumed part of a scan.

The optional `on_reconcile` callback runs after the initial baseline is captured
and after a complete successful changed scan. The baseline precedes the first
mirror transfer so a source change during that transfer remains visible on the
next scan. Unchanged scans do not repeat reconciliation. A failed inventory,
comparison, event output, or reconciliation stops the watcher; it cannot trigger a
later mirror deletion. The caller must preserve the mirror engine's independent
complete-inventory and fail-closed deletion checks.

The previous and next snapshots remain owned until comparison and reconciliation
succeed. Only then does the next snapshot replace the previous one. Both snapshots
are removed on failure, and the remaining snapshot is removed on finite completion
or cancellation. No entire event or inventory list is collected in memory.

`CLI.Watch.run(invocation, target, context, opts)` returns `:ok` or a typed CLI
error. Injectable callbacks are `inventory.(target, context, opts)` returning a
Manifest/error, `sleep.(milliseconds)`, `emit.(event)`, and `on_reconcile.()`.
Effect callbacks return `:ok`, `{:ok, result}`, or `{:error, Error}`. Raised/thrown
callback failures become sanitized typed errors. The coordinating runner owns
presentation and native `mirror --watch` routing.
