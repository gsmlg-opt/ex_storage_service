# Native lifecycle aliases and absolute dates

This extends the portable `ilm rule` document conveniences described in
`management-convenience-decisions.md`. Full configuration import/export and the
existing ESS day/tier flags remain available. The generic modeled S3 API remains
the complete-document interface for multiple transitions and fields without a
convenience flag.

## Pinned grammar and executable evidence

Pinned mc commit: `77f82e18b5401a65958f1619df6ebb994634bd88`. Relevant sources:
[`ilm-rule-add.go`](https://github.com/minio/mc/blob/77f82e18b5401a65958f1619df6ebb994634bd88/cmd/ilm-rule-add.go),
[`ilm/options.go`](https://github.com/minio/mc/blob/77f82e18b5401a65958f1619df6ebb994634bd88/cmd/ilm/options.go),
[`ilm/parse.go`](https://github.com/minio/mc/blob/77f82e18b5401a65958f1619df6ebb994634bd88/cmd/ilm/parse.go),
[`ilm-rule-list.go`](https://github.com/minio/mc/blob/77f82e18b5401a65958f1619df6ebb994634bd88/cmd/ilm-rule-list.go),
and [`ilm-remove.go`](https://github.com/minio/mc/blob/77f82e18b5401a65958f1619df6ebb994634bd88/cmd/ilm-remove.go).
Hidden reference flags are supported only where their effects have portable S3
equivalents; hidden does not mean unimplemented.

Actual pinned mc execution against an isolated loopback peer captured GET/PUT
lifecycle XML for these cases:

| Flags/effect | Captured reference behavior |
| --- | --- |
| `--expiry-date 2032-02-29 --transition-date 2031-01-01 --storage-class glacier` | Expiration/transition dates at `T00:00:00Z`, storage class `GLACIER`. |
| Legacy noncurrent days/count/tier flags | `NoncurrentDays`, `NewerNoncurrentVersions`, `StorageClass` in their corresponding expiration/transition elements. |
| `--expired-object-delete-marker` | `Expiration/ExpiredObjectDeleteMarker=true`. |
| Date edit of an existing day rule | Removed day fields, retained the selected rule ID, filter, status, and transition storage class. |
| `--storage-class GLACIER --transition-tier DEEP_ARCHIVE` | mc silently chose `DEEP_ARCHIVE`; ESS rejects conflicting aliases. |
| Invalid `2031-02-29` | mc read existing configuration, then failed with no PUT; ESS validates before service I/O. |
| `--expire-days 90 --expire-all-object-versions` | Non-AWS `ExpiredObjectAllVersions=true` XML extension; ESS rejects this option. |
| `--transition-tier MINIOTIER-1` | XML storage class `MINIOTIER-1`; this is a MinIO configured-tier reference, not an AWS storage class. ESS rejects it. |

These probes verify request effects and parsing, not cloud enforcement of
lifecycle scheduling. Their owned HTTP servers were stopped after execution.

## Accepted aliases and fields

| Flags | Portable field |
| --- | --- |
| `--expire-days`, `--expiry-days` | `Expiration.Days` |
| `--expiry-date` | `Expiration.Date` |
| `--expire-delete-marker`, `--expired-object-delete-marker` | `Expiration.ExpiredObjectDeleteMarker` |
| `--transition-days`, `--transition-date` | Selected `Transitions` element's `Days` or `Date` |
| `--transition-tier`, `--tier`, `--storage-class` | Selected `Transitions` element's `StorageClass` |
| `--noncurrent-expire-days`, `--noncurrentversion-expiration-days` | `NoncurrentVersionExpiration.NoncurrentDays` |
| `--noncurrent-expire-newer`, `--newer-noncurrentversions-expiration` | `NoncurrentVersionExpiration.NewerNoncurrentVersions` |
| `--noncurrent-transition-days`, `--noncurrentversion-transition-days` | Selected `NoncurrentVersionTransitions` element's `NoncurrentDays` |
| `--noncurrent-transition-newer`, `--newer-noncurrentversions-transition` | Selected `NoncurrentVersionTransitions` element's `NewerNoncurrentVersions` |
| `--noncurrent-transition-tier`, `--noncurrentversion-tier`, `--noncurrentversion-transition-storage-class` | Selected `NoncurrentVersionTransitions` element's `StorageClass` |

`ilm rule ls` and `ilm rule rm` are aliases for `list` and `remove`, including
their existing selection and explicit-force safeguards.

Calendar dates must be exactly `YYYY-MM-DD`; they become UTC-midnight ISO 8601
strings in operation input and XML. Invalid leap dates, timestamps with times or
offsets, year zero, and the zero-time sentinel `0001-01-01` are rejected. Past
calendar dates are not rejected based on the local clock. Expiration accepts one
of days, date, or delete-marker action per invocation. Transition days and date
are mutually exclusive. Date edits remove days; day edits remove dates. Switching
expiration to/from delete-marker mode removes the conflicting alternatives.

Selected edits preserve other rules, filters, statuses, multipart-abort settings,
other actions, and untouched nested transition fields. A tier-only edit can retain
an existing absolute transition date. A retained noncurrent-version-count edit
can retain existing days and storage class; a new action requires its timing and
storage class where applicable. Retained counts are 1..100. A document containing
multiple transitions requires full-document editing: no element is selected
implicitly, including for a date or count-only update. Unknown modeled-document
fields continue to fail validation rather than being silently discarded.

Tier aliases normalize case and accept only the established modeled AWS storage
classes. They do not create, inspect, or reference MinIO remote tier definitions.
Conflicting aliases fail instead of applying mc's precedence. The shared
`storage-class` parser key is retained for other native commands, then normalized
only in the lifecycle command path. Same-key parser aliases must agree exactly;
the separately keyed `storage-class` and `transition-tier` values are compared
after case normalization.

MinIO all-version expiration, remote tier management, and arbitrary custom tier
names remain outside portable S3 support. In particular, all-version expiration
is never translated to current expiration, noncurrent expiration, or a recursive
delete. This slice does not change restore behavior or add provider features.

## Validation

`LifecycleAliasesTest` adds 14 scoped tests, including the alias cross product,
encoded XML effects, conflicting values, invalid dates/counts, selected edits,
multiple-transition refusal, provider boundaries, remove/list aliases, and
no-PUT dry-run. Together with the existing Management suites: **45 tests, zero
failures**. Strict CLI compilation, scoped formatting, and diff checks passed.
