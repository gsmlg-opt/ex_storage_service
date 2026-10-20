# C5 evidence publication copies

Original restore logs remain unchanged and private. Do not stage the five original logs listed in `manifest.json` under `sanitized_mappings`; publish their mapped `*.sanitized.log` copies instead. The manifest records original and sanitized SHA256 values and byte lengths. Original hashes preserve provenance; sanitized hashes identify derivative public content.

Only credential material is replaced with `[REDACTED_CREDENTIAL]`: fixture access keys/secrets, encrypted-secret record values and exact repeats elsewhere in those logs. Test outcomes, failure details, timestamps, counts and every other byte are preserved. No failed result is promoted.

The review covers all `closeout002-c5*` files/directories and `closeout-002/candidate-c5` as enumerated in the manifest. Each raw `samples.jsonl` is excluded in favor of its existing gzip archive, which was scanned while decompressed. Private restore credential fixtures were read solely for an exact-value search; their contents are never copied into publication evidence.

Use `publishable-paths.json` as an explicit payload allowlist and `exclude-original-paths.json` as the staging exclusion list. These lists do not stage anything. Also include this README, the manifest, both lists, and the sanitized copies when publishing the review itself. Future or changed files require a new review.

## Sanitized mappings

- `docs/reports/s3-hardening/closeout002-c5-restore-cluster/recovered-data-a-active.log` → `docs/reports/s3-hardening/closeout-002/candidate-c5/evidence-publication/sanitized/closeout002-c5-restore-cluster/recovered-data-a-active.sanitized.log`
- `docs/reports/s3-hardening/closeout002-c5-restore-cluster/recovered-data-b-active.log` → `docs/reports/s3-hardening/closeout-002/candidate-c5/evidence-publication/sanitized/closeout002-c5-restore-cluster/recovered-data-b-active.sanitized.log`
- `docs/reports/s3-hardening/closeout002-c5-restore-cluster/source-data-a-source.log` → `docs/reports/s3-hardening/closeout-002/candidate-c5/evidence-publication/sanitized/closeout002-c5-restore-cluster/source-data-a-source.sanitized.log`
- `docs/reports/s3-hardening/closeout002-c5-restore-standalone/recovered-data-a-active.log` → `docs/reports/s3-hardening/closeout-002/candidate-c5/evidence-publication/sanitized/closeout002-c5-restore-standalone/recovered-data-a-active.sanitized.log`
- `docs/reports/s3-hardening/closeout002-c5-restore-standalone/source-data-a-source.log` → `docs/reports/s3-hardening/closeout-002/candidate-c5/evidence-publication/sanitized/closeout002-c5-restore-standalone/source-data-a-source.sanitized.log`
