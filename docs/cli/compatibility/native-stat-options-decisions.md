# Native stat exact-object inspection

`stat --no-list TARGET` disables prefix discovery. An object target receives a
literal HeadObject, including keys ending in `/`; a 404 remains the original
service error. A bucket target receives only HeadBucket, preserving the expected
owner constraint. Object-only encryption fields are not sent to HeadBucket.
Explicit `--no-list=false` retains ESS's existing missing-object prefix fallback.

An explicit object VersionId, including a slash marker, must be confirmed by the
HEAD response. A missing or different returned version fails. Recursive and
all-version inspection conflict with no-list. Bucket no-list also rejects rewind
before provider access; bucket HEAD cannot represent a historical object
snapshot. Object rewind retains the existing version-inventory selection and
identity checks; no-list does not eliminate that explicit historical lookup.

Ten scoped regressions cover parsing, literal paths, 404 behavior, bucket owner,
selected-version confirmation, conflict validation and the disabled fallback.
Native stat retains bounded ESS structured output rather than mc's verbose
display. The selected mc reference is
`77f82e18b5401a65958f1619df6ebb994634bd88`; matching a flag does not establish
complete mc output/default or live-provider acceptance.

The added rm `--versions` alias also exposed a shared parser-key collision with
inspection `--versions`. Parser first identifies the command without side
effects, then scans against that command's option table. This preserves globals
before/after the command, per-command keys, conflict detection and SQL's separate
query expression. Regression coverage includes ls/stat/du/find/rm and SQL/query.
