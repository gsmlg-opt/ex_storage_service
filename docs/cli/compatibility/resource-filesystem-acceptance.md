# Transfer resource and filesystem acceptance

The CLI transfer acceptance harness exercises the actual HTTP/1 adapter against
a throttled local socket peer. The peer retains at most one 64 KiB body chunk;
it never buffers an object. A sparse source file supplies generated zero bytes,
and the download creates a real file through exclusive staging and publication.
Both transfer directions complete. This is local wire evidence, not provider
integration or HTTP/2/HTTP/3 evidence.

Run from `apps/ex_storage_service_cli`:

```sh
PAGER=cat mix test test/ex_storage_service_cli/transfer/resource_acceptance_test.exs
ESS_RESOURCE_LARGE_BYTES=2147483648 PAGER=cat mix test test/ex_storage_service_cli/transfer/resource_acceptance_test.exs
```

The default large object is 64 MiB. The explicit acceptance run uses 2 GiB;
both are compared with a 1 MiB object after a warmup. Admission is configured
for one active request and a 1 MiB byte budget. The test samples total BEAM
memory, binary memory, process counts, every process mailbox length, and staging
disk usage. Coordinator high-water marks independently record active request
and reservation maxima. Assertions permit a fixed 64 MiB VM overhead above
the byte budget, rather than claiming that the admission budget limits total
BEAM memory. A five-minute request timeout accommodates deliberate peer
throttling. The ordinary 60-second request timeout correctly expires before a
2 GiB throttled request completes.

The initial 64 MiB validation passed: total VM memory increased by 1,208,240
bytes and binary memory by 678,032 bytes. The maximum mailbox length was one;
active requests peaked at one, reserved bytes at 131,072. Staging stayed within
the object length and was removed after publication. Exact run measurements
are emitted as `ESS_RESOURCE_EVIDENCE` JSON by the test.

The 2 GiB upload and download completed in 143.3 seconds with 12,163 samples.
Total VM memory increased by 1,692,464 bytes, binary memory by 800,576 bytes,
and processes from 150 to a peak of 159. The maximum mailbox length was two;
active requests peaked at one and reserved bytes at 131,072. The staging file
reached 2,147,483,648 bytes and was removed after publication; final active
requests and reserved bytes were zero. This run used `mix test --no-compile`
against the previously validated transfer modules while independent CLI source
work was in progress. The final integrated strict compile remains a separate
release requirement.

`filesystem_acceptance_test.exs` uses real Linux filesystem entries to cover
distinct case/Unicode names, a file blocking a child directory, destination and
parent symlink replacement, and replacement of an owned staging path with a
foreign symlink. Publication validates the captured staging inode as well as
parents and destination. Abort never removes a foreign replacement. A broken
source symlink emits one typed error and terminates enumeration normally,
closing its native directory resources through `Stream.resource` cleanup.
The filesystem, inventory-policy and engine suites pass 20 tests together.

`executable_signal_test.exs` builds the actual escript and inspects packaged
native assets, model data and legal notices. Real SIGINT and SIGTERM during a
stalled download return 130 and 143, preserve the previous destination and
remove owned staging. Interrupting after the first multipart checkpoint closes
the active socket, aborts only the captured upload ID, records journal failure
and abort cleanup, and releases the journal's kernel ownership lock. Together
with preserved native JSON golden cases, these executable suites pass 13 tests.

Linux x86_64 / OTP 28 is the measured platform. These tests do not establish
Darwin or Windows behavior, live cloud behavior, or directory-capability safety
against hostile concurrent parent renames. The sink's documented invoking-user
control requirement still applies.

## Remote committed-source resource run

The resource job in [CLI run 38739026057](https://github.com/gsmlg-opt/ex_storage_service/actions/runs/38739026057)
passed on clean source `2380deac68b3181c6192c7047364390d6cd607c9`:
one test, zero failures, in 151.3 seconds. The 2,147,483,648-byte upload and
download used the same 1 MiB admission budget with 11,970 large-run samples.
Total VM memory peaked at 63,375,896 bytes from a 62,646,520-byte baseline;
binary memory peaked at 1,964,344 bytes from 1,237,824. Maximum mailbox length
was two, active requests one and reserved bytes 131,072. Disk staging peaked
at the object length; final request reservations and staging were released.
The downloaded log is `/tmp/ess-cli-2380dea-resource/cli-resource.log`.
This does not expand the filesystem/platform or live-provider boundaries above.

The follow-up [run 38740996062](https://github.com/gsmlg-opt/ex_storage_service/actions/runs/38740996062)
repeated the same 2 GiB contract from clean source
`6816ee14c87ccc8de76a37a41811ec46a0c7aa9e`: one test, zero failures, 152.5
seconds and 12,029 large-run samples. Total/binary memory peaked at
64,080,152/2,038,464 bytes from baselines of 63,220,632/1,250,304. Mailbox,
request and reservation maxima remained two, one and 131,072 respectively;
final reservations and staging were released. The downloaded evidence is
retained at `/tmp/ess-cli-6816ee1-resource`.
