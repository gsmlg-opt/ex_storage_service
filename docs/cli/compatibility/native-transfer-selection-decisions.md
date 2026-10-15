# Native transfer age and storage-class selection

Native `cp`, `mv`, and `mirror` accept `--older-than` and `--newer-than`.
Only native `mirror` accepts repeated `--exclude-storageclass` values. Separate
and inline repeated flags preserve argument order; matching is exact and
case-sensitive, without glob expansion. An omitted remote class means STANDARD;
local files have no storage class. AWS command grammar and semantics are unchanged.

## Pinned mc reference

The reference is mc commit `77f82e18b5401a65958f1619df6ebb994634bd88`, with
executable SHA256
`a62c4c85f9efbe5d07e03e2ff9923625fbf659a9f272a077a0cd846279f1ccc2`.
`cmd/utils.go:172-209` implements predicates as skip decisions:
relative older includes equality and relative newer excludes equality.
Supported bounded duration units are `ns`, `us`, `µs`, `μs`, `ms`, `s`, `m`,
`h`, `d`, `w`, and `y`; days are 24 hours, weeks seven days, and years 365 days.
Signed, fractional, concatenated durations and bare zero are accepted, with the
signed positive 64-bit nanosecond limit. Uppercase units and calendar months
are rejected. The existing Find parser supplies the shared bounded parser.

The actual locked executable shows a different equality boundary for absolute
timestamps: older excludes equality, newer includes equality. It samples
`time.Since(objectTime)` before parsing and sampling `time.Since(cutoffTime)`.
The retained `test/fixtures/s3/native-transfer-age-mc-capture.json` records six
successful local copies/skips at the absolute cutoff and one second either side.
ESS represents these observed boundaries explicitly using one invocation clock,
avoiding per-object timing races. Tests also cover dotted dates, dotted minute
and second date-times, RFC3339, and UTC date-time strings.

Recapture with the locked executable:

```sh
python3 apps/ex_storage_service_cli/test/fixtures/s3/capture_native_transfer_age_mc.py \
  --mc /tmp/ess-cli-reference-m0/mc \
  --output /tmp/ess-native-transfer-age-mc-recapture.json
```

## Conservative mirror deletion boundary

Pinned mc applies age and storage-class predicates to source content only:
`cmd/mirror-main.go:831-838` and `cmd/mirror-url.go:200-209`. Destination-only
deletions are emitted independently at `cmd/mirror-url.go:248-258`.

ESS deliberately uses the conservative contract accepted for this implementation:
source exclusions protect matching destinations, and destination-only deletion
candidates must themselves satisfy age predicates and avoid excluded storage
classes. Existing name exclusions remain protective. Inventories are retained in
full; the bounded disk planner marks exclusions instead of dropping source records
and accidentally turning their destinations into deletion candidates.

Invalid time filters fail before source inspection, requests, or temporary state.
Missing modification times fail closed when age selection is active, before
planned copies or removals. Stdio has no portable modification timestamp and fails
before reading bytes. Single-object dry runs inspect timestamps without copying;
selected remote objects retain the captured identity for subsequent verification.
Recursive moves retain their complete-copy, verification, then deletion fences.

## Validation

Initial TDD run: nine tests failed against the prior code. The first implementation
passed eight; repeated flags exposed Parser's last-value aggregation. A surgical
aggregation fix preserves all storage classes. The final focused suite passed
**78 tests, 0 failures**, covering twelve new selection tests plus the existing
high-level, bounded-planner, move-safety, parser, and Find suites. Strict compilation
and scoped formatting checks passed. The retained executable capture was generated
successfully using the locked mc binary. These local loopback and local-file checks
do not establish live-provider or complete mc command parity.
