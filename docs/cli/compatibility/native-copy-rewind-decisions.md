# Native historical copy

`ess cp --rewind TIME SOURCE DESTINATION` selects one remote object version or
a recursive prefix snapshot strictly before TIME. Selection shares the bounded
version manifest and cutoff rules used by native inspection. A latest eligible
delete marker hides older data; a recreation at or after the cutoff does not
restore it. The pinned mc reference establishes this strict cutoff; see
native-inspection-decisions.md and its retained executable capture.

All version pages and external sorting finish before a recursive copy creates
destination files or sends mutations. Late listing failures fail the operation.
Excluded single objects make no inventory or object requests. Dry-run resolves
and confirms selected historical identities without reading bodies or copying.
Temporary manifests are removed on success and failure.

HEAD must confirm the selected version, length, ETag and modification instant.
Downloads carry that VersionId and If-Match and confirm the response identity
before publication. Server-side copy and bridges retain the selected version
through source recapture and copy-source encoding. A changed recaptured identity
fails before a destination mutation. Version IDs remain opaque encoded values.
These checks also protect mutable `null` versions from changed historical
metadata; no source deletion is part of historical copy.

Rewind requires an S3 source and conflicts with `--version-id`. Explicit IDs
still select one object. The duration/date parser is shared with inspection;
date-only input means UTC, whereas mc uses the host timezone. No new AWS `s3`
option or native move-rewind contract is introduced.

The focused run of native_cp_rewind_test.exs, version_selection_test.exs,
high_level_test.exs, copy_test.exs and executable_highlevel_matrix_test.exs
passed 69 tests on Linux x86_64 / Elixir 1.18.5 / OTP 28. These are real loopback
HTTP, planner and executable regression checks. Historical copy itself has no
new live-provider or exact mc-output parity claim.
