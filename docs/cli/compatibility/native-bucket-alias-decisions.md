# Native bucket flags and deprecated management aliases

Baseline: mc `77f82e18b5401a65958f1619df6ebb994634bd88`; ESS `9b8661e`
plus the implementation diff. This is scoped native behavior, not full mc parity.

Reviewed the actual pinned `cmd/mb-main.go`, `cmd/client-s3.go`,
`cmd/ilm-deprecated-cmds.go`, `cmd/encrypt-clear.go` and `cmd/encrypt-info.go`,
alongside their locked help captures. The bucket handler calls MakeBucket with
the lock option, ignores only `BucketAlreadyOwnedByYou`/`BucketAlreadyExists`
when requested, and separately enables versioning after a successful or ignored
create. Deprecated lifecycle commands call the same lifecycle-rule handlers;
encryption clear/info call delete/get. No MinIO implementation was copied.

Native `mb` now accepts `--ignore-existing`, `--with-lock` and
`--with-versioning`. Creation preserves the selected region and sends the modeled
Object Lock creation header. Object Lock implies versioning for a newly created
bucket; `--with-versioning` additionally performs PutBucketVersioning. Only the
two exact creation-conflict codes above can be ignored. Authentication, other
conflicts and subsequent versioning errors remain failures. An ignored existing
bucket is not retrofitted with Object Lock. Results distinguish new/existing
buckets and lock application during this creation; a later versioning failure
retains the service error and records whether creation completed. There is no
rollback deletion after a partial success. Declared ESS capability exclusions
continue to reject unsupported Object Lock requests before transport.

`ilm add|edit|ls|rm|export|import` reuse the existing corresponding `ilm rule`
specifications and handlers. Selected edits preserve other rules; whole removal
still requires `--all --force`; import retains bounded JSON stdin. `encrypt clear`
and `encrypt info` reuse remove/get, including dry-run and provider failures.
ESS full-document/structured output and existing parameter differences remain;
registering these aliases does not establish mc display/default parity. Native
bucket short aliases and multi-target/directory creation are outside this slice.
AWS `s3 mb` does not accept the new native flags and keeps its existing request
and output path. Native bucket creation without these flags also retains its
existing path.

`NativeBucketAliasesTest` exercises real loopback HTTP creation/lock/region and
versioning requests, narrow ignore behavior, partial failure with no rollback,
the runner route without an output selector, unchanged unflagged/AWS creation,
and declared ESS rejection. Lifecycle/encryption tests assert selected effects,
preservation, import and destructive/dry-run guards. These are independent wire
and handler fixtures, not live provider acceptance or a pinned executable
differential. The scoped five-file suite passed 56 tests with zero failures;
strict compilation and scoped formatting also passed. The initial ten new tests
failed before implementation. First integration corrected fixture assumptions
about the existing `?versioning=` spelling and rejection of unknown modeled
request members; neither protocol/schema behavior was weakened.

The native `--resolve` grammar entry was added at the coordinating agent's
request; its effect integration and acceptance belong to that agent and are not
claimed by this slice.
