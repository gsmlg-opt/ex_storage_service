# Native rm governance bypass

Native `rm --bypass` explicitly requests `BypassGovernanceRetention: true`
through the shared modeled S3 input. The existing `DeleteObject` and
`DeleteObjects` paths select the field and sign the resulting
`x-amz-bypass-governance-retention: true` header. Omitting the option or using
`--bypass=false` omits the modeled field and header. The option belongs only to
native `rm`; AWS high-level `s3 rm` does not acquire a new option.

This request does not grant permission or bypass compliance retention. The
provider still enforces Object Lock and `s3:BypassGovernanceRetention`
authorization. Exact selected version IDs and delete-marker IDs are retained.
Current recursive deletion retains complete inventory and conditional identity
preflight; age-selected single deletion retains its ETag or immutable-version
fence. Dry-run, conflicting-version rejection and per-object batch failure
reporting remain active. Inspection/HEAD/list requests never receive the bypass
header.

Scoped wire tests cover signed single current/selected-version deletion,
age-selected current/versioned deletion, current/version/delete-marker batches,
false/default omission, parser boundaries, unsafe-current and mismatched-version
refusal, dry-run and HTTP-200 per-object AccessDenied. These are ESS client
request/effect tests, not live Object Lock authorization or full mc parity.
The parent records final integration acceptance separately.
