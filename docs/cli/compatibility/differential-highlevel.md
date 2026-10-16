# Pinned high-level executable matrix

The independent oracle is AWS CLI 2.31.18, executable SHA256
`87630ec5232080b737502f4098f4c9adbf820c01d23c63b62affcad50059ad0a`.
`capture_highlevel.py` runs the executable against owned loopback peers with
isolated configuration and fixture credentials. Explicit and environment S3
endpoints both point to those peers: the pinned source client drops the explicit
endpoint when `--source-region` is present. Inherited proxies are cleared.

The committed `highlevel-wire.json` has 42 cases and 67 recorded exchanges,
SHA256 `30f94eebef01b1d71912bd7538d1704154eb830229b79c8d8756ffdc091643ac`.
A second complete capture was byte-identical. It covers all nine commands:

- `ls`: bucket filters and human-readable recursive summaries, including byte
  singular/plural and the rounding boundary before the next unit.
- `mb`, `rb`: regional bucket creation and empty/force-empty removal.
- `rm`, `website`, `presign`: escaped keys, requester pays, website documents,
  and unsigned presigning without an HTTP request.
- `cp`: metadata/content headers, grants, ACL, encryption/storage class,
  checksums, binary download, and six single-copy property/directive variants.
- `mv`: same-endpoint server copy with a different source region.
- `sync`: an actual filtered local upload, excluding a second local file.
- Archive selection: restored and un-restored Glacier downloads, copies and
  moves; warning suppression, forced transfer and restored Deep Archive download.
  Un-restored skips issue no GET, copy or delete. Confirmed restored sources
  proceed using the HEAD `Restore` state.
- SSE-C: upload, download and server copy with distinct binary `fileb://` keys,
  source/destination customer-key and MD5 headers, including safety probes.
- Sync comparison: equal-size older/newer timestamps in all three directions,
  exact timestamps and size-only, protected exclusions, deletion and dry-run.
  Download cases also compare final local bytes, mtimes and deletion state.

`ExecutableHighlevelMatrixTest` builds a real escript and compares exit status,
stdout/stderr, raw entity bytes, XML semantics and recorded request parameters.
The combined matrix, parser, high-level unit and copy suite passed **55 tests,
zero failures**. Subsequent integration still requires the final complete CLI
suite. Source-only audits and this representative matrix do not prove every
flag, transfer direction, error, progress mode, or provider behavior.

The archive expansion passed 40 related tests after four restored-source red
regressions. The subsequent SSE-C/sync matrix passes **43 tests, zero failures**
(seed 628644); it required no production correction. Both expanded AWS captures
were independently repeated byte-identically, retaining the original 18 cases.
Archive warning text remains an explicit difference: replay compares the warning
outcome and skip status, while successful output and mutation sequences retain
their exact comparisons.

## Repairs demonstrated by the oracle

AWS high-level duplicate scalar options now use the last occurrence, retaining
native conflict rejection. Human sizes match the pinned byte labels and rounded
unit thresholds. Grant values preserve their literal principal syntax and the
last occurrence of a permission, including quoted principal IDs. Invalid header
control bytes remain rejected.

Same-service copy identity no longer includes region; changing source region on
the same endpoint/credential context therefore retains server-side copy. Single
copy now implements `--copy-props none` and `metadata-directive`, inherits missing
metadata fields when explicit properties require replacement, and gives explicit
`--metadata-directive` precedence over automatic copy-property subscribers.
Multipart explicit directives also disable automatic metadata/tag inheritance.

## Deliberate request differences

ESS verifies a copied destination and revalidates the selected source before
deletion. The matrix identifies and removes only those HEAD probes before
comparing the reference operation sequence; unexpected mutations still fail.
Filtered sync similarly has an extra destination-identity HEAD before upload.
Conditional source-copy/deletion headers remain enabled as safety checks.

Copy-source comparison permits the S3-defined optional leading slash before the
bucket, preserving all remaining escaped key/version bytes. Listing comparison
permits an explicit `max-keys=1000` service-default bound. Entity lengths must
match actual bytes. Payload/checksum signature fields are covered by separate
independent signature and checksum tests; this matrix does not equate differing
default checksum policies. It is controlled HTTP evidence, not live AWS account
or cloud-provider acceptance.

Reproduce the oracle from the repository root with the pinned binary:

```sh
PAGER=cat python3 apps/ex_storage_service_cli/test/fixtures/differential/capture_highlevel.py --aws /tmp/ess-cli-reference-m0/aws-release/aws/dist/aws --output /tmp/ess-highlevel-wire.json
```
