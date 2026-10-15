# Request cancellation ownership

Every transport request has a private AbortController and a lifetime monitor.
An optional caller `signal` is borrowed: the request never changes its active
request PID, aborts it, or stops it. Normal completion, early close, sink failure,
and coordinator cancellation preserve a reusable, unmodified caller controller.
An explicitly aborted caller signal cancels all requests borrowing it.

The dependency exposes no subscription API. The monitor reads only the borrowed
controller's cancellation bit every 25 milliseconds with a bounded 25 millisecond
read timeout. A dead/unresponsive signal fails closed. The coordinator tracks
only the request's private controller. Cancellation applies while waiting for
headers and during acknowledged body consumption; it does not require body data
to arrive to detect the caller's cancellation.

The monitor owns the HTTP Promise before network execution begins, so request
owner death cannot strand a Promise between allocation and registration. Once
headers arrive it retains the raw stream until explicit close or consumption.
Consumption transfers lifetime ownership to the consuming process. Adoption
first checks the request's private controller; an already cancelled response
returns the typed aborted error instead of monitoring an already closed stream
and losing the known interruption as `:noproc`. A deterministic regression covers
cancellation between response delivery and reader adoption. Owner or
consumer death aborts the request, stops an outstanding Promise, drains or kills
the owned stream within one second, stops the private controller, and releases
the admission lease. Cleanup is idempotent and does not depend on a process
dictionary or a caller running an `after` block. The monitor is request owned and
does not require a separately running invocation supervisor.

Tests use real TCP peers stalled before headers and partway through a response,
plus shared borrowed controllers, independent close, consumer exceptions, owner
and consumer death, and lease/controller/monitor shutdown checks.
