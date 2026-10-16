# Native removal selection

Native `rm` accepts `--versions` as the `--all-versions` alias and `--fake` as
the `--dry-run` alias. `--older-than` and `--newer-than` use the same bounded
duration/absolute-time parser and invocation clock as native transfer selection.
AWS grammar and defaults retain their separate contracts.

Recursive removal completes its disk-backed inventory and validates every
selected timestamp before the first destructive batch. Name exclusions remain
protective. Before deleting current objects, HEAD revalidates the inventory
timestamp and existing conditional-deletion identity. Selected immutable
versions retain their exact version identifiers. A single age-selected object
also requires a confirmed selected version or verified conditional-current-delete
capability. Missing/changed timestamps, invalid predicates and unavailable
conditional deletion fail closed. Dry runs read the necessary identities and
apply selection without deleting.

The thirteen new regressions cover aliases, boundaries, single/recursive and
version/delete-marker selection, changed/missing identity, no-I/O validation,
exclusions and late malformed timestamps. The private scoped acceptance passes
37 tests, including bounded removal and transfer selection. Full integrated
acceptance is recorded separately.

This does not implement mc's stdin/multiple targets, incomplete-upload removal,
rewind, non-current/purge or force/dangerous contracts. Explicit governance bypass
is implemented separately in native-rm-bypass-decisions.md. Output retains ESS
delete/dry-run labels. Conditional deletion is a deliberate preservation boundary;
the matching flag spellings do not establish complete mc parity. Pinned mc time
observations are retained in native-transfer-selection-decisions.md.
