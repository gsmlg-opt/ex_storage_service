# Native anonymous policy commands

Scope: `anonymous get-json`, `set-json`, `list`, and `links` over portable S3
bucket-policy APIs. This does not implement MinIO administrative APIs or an IAM
authorization engine. Existing `anonymous get` and `set` remain separate commands.

`anonymous set private TARGET` aliases the existing `none` mode, matching pinned
`cmd/anonymous-main.go:198-259`. Both use ESS's guarded complete empty policy:
an existing policy requires `--replace`, denied reads propagate, and dry-run
performs no write. This differs from mc's selected-statement `SetPolicy` update
and deletion of an empty result. Local tests prove no public permissions are
introduced and preserve those safeguards; real-provider acceptance of the empty
policy representation remains unverified. The 51-test related suite passes
after two red alias regressions.

## Pinned reference and executable observations

Reference: mc commit `77f82e18b5401a65958f1619df6ebb994634bd88`,
[`cmd/anonymous-main.go`](https://github.com/minio/mc/blob/77f82e18b5401a65958f1619df6ebb994634bd88/cmd/anonymous-main.go)
and [`cmd/client-s3.go`](https://github.com/minio/mc/blob/77f82e18b5401a65958f1619df6ebb994634bd88/cmd/client-s3.go).
The actual pinned executable was exercised against an isolated loopback S3 peer;
these are observed contracts, not inferred flag-name matches:

| Invocation | Captured behavior |
| --- | --- |
| `mc anonymous get-json fixture/bucket` | GET `?policy=`, complete policy JSON on stdout. |
| `mc --json anonymous get-json fixture/bucket` | Envelope with `operation`, `status`, `bucket`, `permission`, and complete `anonymous` policy. |
| `mc anonymous set-json FILE fixture/bucket` | PUT `?policy=`, exact file bytes including final newline; no preceding policy GET. |
| `mc --json anonymous list fixture/bucket` | One `resource`/`allow` row for each recognized rule. |
| `mc --json anonymous --recursive links fixture/bucket/p/` | Policy GET, then ListObjectsV2 with prefix `p/`; unpresigned object URL in `status`/`url` JSON row. |

The probe policy granting only `s3:GetObject` to `bucket/p/*` produced mc
`allow:"none"` and no links. Adding unconditional bucket-level
`s3:GetBucketLocation` and `s3:ListBucket` produced `readonly` and the object URL.
Adding an object-grant condition `aws:SourceVpc` changed mc's classification to
`none`. Adding an explicit public `Deny s3:GetObject` alongside the unconditional
allow still produced mc `readonly` and an object URL. ESS deliberately refuses
links for that ambiguous authorization document.

## Portable behavior and explicit differences

* `get-json TARGET` returns the complete parsed policy object, including custom
  conditions and unrecognized fields. Its output contract is ESS JSON, without
  the mc status envelope. Prefix targets still export the complete bucket policy.
  Provider errors, including `NoSuchBucketPolicy`, propagate.
* `set-json FILE TARGET` reads a literal local regular filepath, not `file://`,
  JSON text, or stdin. It accepts nonempty files up to 120 KiB, matching mc's
  source limit, validates a complete JSON policy containing object statements,
  and sends the original bytes. No unrelated statement or unknown field is
  discarded. Unlike mc, an existing policy requires `--replace`; absence is
  recognized only by `NoSuchBucketPolicy`. The target must be bucket-only since
  the effect replaces the entire bucket policy. `--dry-run` validates and can
  inspect the existing policy, but never performs PUT.
* `list TARGET` reports `Rules` with `resource`/`allow` summaries for unconditional
  public `s3:GetObject` and `s3:PutObject` grants. It combines grants for the same
  resource and supports related prefix or exact object targets. It uses
  `readonly`, `writeonly`, and `readwrite` to describe these object grants, without
  requiring the bucket-level actions used by mc's classifier. Unconditional
  bucket `ListBucket`/`GetBucketLocation` grants are accepted but produce no object
  rule themselves. Output order is deterministic.
* Supported principals are literal `"*"`, `{"AWS":"*"}`, and
  `{"AWS":["*"]}`. Supported resources are literal general-purpose
  `arn:aws:s3:::BUCKET` resources and exact object paths or one trailing `*`.
  Conditions, deny statements, negated predicates, other principals/actions,
  other ARN partitions, policy variables, `?`, and complex wildcard patterns are
  reported by one-based `UnclassifiedStatements` indices. They are never silently
  treated as anonymous permission. This is intentionally conservative.
* `links TARGET [--recursive] [--limit N]` requires a fully classified policy.
  It lists only keys covered by unconditional object read grants. The default
  limit is 1000, with 0..10000 accepted. Listing uses the target's literal key
  prefix and a `/` delimiter unless recursive. Covered common prefixes retain
  directory URLs in nonrecursive output. Exact object grants never expand into
  broader prefix grants.
* Link inventory completes all pages in a private disk-backed manifest before
  output. A later page failure is fatal, including when the display limit is
  already reached. Provider keys outside the requested prefix are rejected.
  The manifest is removed on success or failure. Results include `Returned`,
  `MatchedObjects`, and `Partial`; the latter means the display limit omitted
  matched entries. This differs from mc's partial output on some listing errors.
* URLs use the shared operation endpoint resolver and its configured path or
  virtual addressing, base path, and percent encoding. They contain no signatures
  or credentials; no object content request is needed to construct them.

`list` and `links` always include `PolicyOnly:true` and
`PublicAccessVerified:false`. Bucket policy alone does not prove public access:
block-public-access configuration, ACLs, organization restrictions, endpoint or
network policy, and current provider authorization remain outside this command's
assessment. No real cloud public-access claim was validated by the loopback
reference peer.

## Validation

`NativeAnonymousTest` covers complete custom-policy export, literal-file byte
preservation, replacement protection, no-PUT dry-run, service error propagation,
file and JSON limits, conservative classification, prefix selection, paginated
links, URL escaping, directory URLs, bounded materialization, later-page errors,
and manifest cleanup. The scoped suite passed 13 tests; strict CLI compilation
passed. Reference probes terminated their owned HTTP peers after execution.
