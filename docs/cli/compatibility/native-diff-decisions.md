# Native diff comparisons

`ess diff SOURCE TARGET` completes both bounded inventories, then reports names
present on either side, size changes, and equal-size objects whose source time
is newer. The comparison direction remains source to target for local, remote
and mixed targets. Neither object contents nor local files are changed. Missing
timestamps suppress the time comparison, matching the pinned ordinary fallback.
Name exclusions protect entries on either side.

The locked mc executable independently supplies six retained local cases in
`test/fixtures/differential/native-diff-mc.json`: source-only, target-only,
different size, newer source, older source, and equal-size/equal-time objects
with different bytes. Every case has an ESS comparison test. Separate wire
regressions cover remote-to-local time direction and absent remote timestamps.
Before the fix, all four initial regressions failed: target-only names were
skipped, and AWS download sync policy reversed the time comparison.

ESS retains its `different: NAME` output and existing `--size-only` and
`--exact-timestamps` extensions. It does not claim mc's reason-specific output,
active-active metadata comparisons, NFC key matching or file/directory type
reporting. These are explicit compatibility differences; the six captures do
not establish full native mc output or provider acceptance. The pinned source
is `cmd/difference.go:86–108,264–284,303–370` at
`77f82e18b5401a65958f1619df6ebb994634bd88`.

Recapture with the checksum-verified locked binary:

```sh
PAGER=cat python3 apps/ex_storage_service_cli/test/fixtures/differential/capture_native_diff.py \
  --mc /tmp/ess-cli-reference-m0/mc --output /tmp/ess-native-diff-mc.json
```
