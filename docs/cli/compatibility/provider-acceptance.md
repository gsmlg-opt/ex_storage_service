# Disposable provider acceptance

The actual standalone ESS escript was exercised against published ESS 0.6.7 and RustFS 1.0.1 on 2026-10-15. Independent AWS CLI 2.31.18 verified uploaded/downloaded bytes, listings, and configuration. This is bounded provider evidence, not proof that either server implements every modeled S3 operation.

## Executables and providers

- ESS CLI escript SHA256: `832080b5f9fe283360005f4faa7d8fd0966f076e5b216ed6b5080e3f69b2eba0`. This private test snapshot contains the CLI implementation, including raw stdout handling, at uncommitted work based on `9b8661e58107eddb24cb8f15495cee4ac979219a`.
- Independent AWS CLI: 2.31.18, executable SHA256 `87630ec5232080b737502f4098f4c9adbf820c01d23c63b62affcad50059ad0a` (matches the reference lock).
- ESS image: `ghcr.io/gsmlg-dev/ess:0.6.7`, immutable digest `ghcr.io/gsmlg-dev/ess@sha256:59c6b85d2bc60a18660b1304c62b41479138c55aebb4078682f6b307fc85fb02`. Container image ID `607bda66e90360b02cd79d40a57ec867ccfad6d0b45b44d43c40e0fee0593e6a`.
- RustFS image: `rustfs/rustfs:1.0.1`, immutable digest `docker.io/rustfs/rustfs@sha256:1803faef57627e2d9c2e7d89d655d712ddded5389040054987163043fecb6a3c`. Container image ID `f6b22807f10db2aee80b8a9ed1e36bd12c2fba25ac2d98dc3b0332a23f1a5c4d`.

Both containers exposed port 9000 only through dynamically allocated `127.0.0.1` host ports. They used anonymous disposable volumes, with no host bind mounts. ESS authentication stayed enabled; a generated IAM user/policy/key was provisioned by release RPC. Credential files had private permissions and were removed without printing their values. The probe isolates AWS/native configuration paths without repurposing HOME.

## Results

| Contract | ESS 0.6.7 | RustFS 1.0.1 |
| --- | --- | --- |
| Authenticated buckets and native environment-credential alias | Pass | Pass |
| Binary/gzip/deflate unusual-key upload; independent AWS, native cat, and high-level download bytes | Pass | Pass |
| Native copy; unverified unversioned move preserves source and verifies destination | Pass | Pass |
| Native versioned move; exact immutable source version deleted, destination bytes independently verified | Not claimed | Pass |
| 9 MiB multipart transfer with independent bytes | Pass | Pass; no incomplete upload remains |
| Five objects through two-item pages | Pass | Pass |
| Sync upload/delete/exclusion with independent listing and bytes | Pass | Pass |
| Enable/read/suspend bucket versioning; lifecycle put/read/delete | Pass | Pass |
| Literal unusual-key inventory identity | Server failure: plus becomes space | Pass |
| Standard Content-Encoding metadata | Server failure: omitted | Pass |
| Unicode bulk delete | Server failure: MalformedXML | Not the ESS-specific control |
| Literal `dot//./../key` upload | Not claimed | Server rejects with InvalidArgument |

Raw compressed entities remain byte-exact even when ESS omits the Content-Encoding header. A separate AWS-only PUT/HEAD/GET control confirms ESS's header loss. Native unversioned move intentionally fails closed because conditional source deletion is not verified; the successful RustFS versioned move is recorded separately.

The first provider run also exposed a real client defect: BEAM stdout converted bytes 128–255 to UTF-8 during native cat. The repaired executable passes all raw binary/gzip/deflate roundtrips against both providers. The final recorded provider failures are independently reproduced server boundaries.

The final run recorded 10 passing contracts and three provider failures for ESS, and 13 passing contracts and one provider failure for RustFS. Its exit status was 1, preserving those failures rather than claiming complete provider compatibility.

## Server issues and cleanup

- [ESS #20](https://github.com/gsmlg-opt/ex_storage_service/issues/20): ASCII DeleteObjects succeeds; valid UTF-8 key `delete-雪` fails with MalformedXML using both clients.
- [ESS #21](https://github.com/gsmlg-opt/ex_storage_service/issues/21): gzip/deflate object bytes survive but standard Content-Encoding is missing.
- [ESS #22](https://github.com/gsmlg-opt/ex_storage_service/issues/22): independent AWS uploads `plus+a`, but ListObjectsV2 returns `plus a`.
- RustFS's literal dot-segment key produces InvalidArgument with both the current CLI and pinned AWS. Keys without dot segments, including percent/plus/space/question/hash/Unicode, pass the raw-byte tests.

No production client workaround rewrites these requests or returned keys. After capturing failures, fixture cleanup may delete only known original keys and use individual deletes for owned fixtures. The probe marks those fixture-only operations with upstream issue numbers. RustFS version cleanup removes the explicitly listed versions/delete markers. Container/volume teardown removes the generated IAM state.

All final-run buckets and the two retained earlier-run ESS buckets were removed. After report capture, both owned containers and their anonymous volumes were removed with identity checks; subsequent container and volume inspection confirmed absence. No acceptance process remains running.

## Reproduction and report semantics

Run from the repository root with the owned disposable containers already listening:

```sh
PAGER=cat python3 scripts/cli_compatibility/provider_probe.py \
  --escript /path/to/private/ess \
  --aws-executable /path/to/pinned/aws \
  --ess-endpoint http://127.0.0.1:38959 \
  --rustfs-endpoint http://127.0.0.1:42623 \
  --ess-container ess-cli-m9-server-01a13 \
  --rustfs-container ess-cli-m9-rustfs-01a13 \
  --output-dir /tmp/ess-cli-provider-acceptance --keep-containers
```

The harness returns 1 for any recorded failure or failed fixture cleanup, including independently confirmed server limitations. It does not turn those into passing tests. Without an independent AWS executable, reports say `independent_aws: false`; same-artifact verification cannot establish independent compatibility.

The redacted [provider report](../../../apps/ex_storage_service_cli/test/fixtures/providers/provider-acceptance.json) preserves pass labels, literal failures, both executable statuses/errors, exact listed keys, header metadata, cleanup, artifact hashes, and immutable image identifiers. A CI policy may compare the exact pinned image, full required passing contracts, and explicitly documented failure signatures. Unexpected results, client/reference disagreement, omitted independent controls, or cleanup failures must fail that policy; accepting any nonzero status wholesale is insufficient.

## Remote committed-source checkpoint

The provider job in [CLI run 38739026057](https://github.com/gsmlg-opt/ex_storage_service/actions/runs/38739026057)
repeated the independent probes from clean source
`2380deac68b3181c6192c7047364390d6cd607c9`. The tested escript SHA256 was
`fdb48d7add6bf4dda1ec8c435451ef5347e249e473b9e6d4454692f445ddbd6b`.
The downloaded report records `independent_aws: true`, the same pinned provider
image digests, **ESS: ten passes and three server failures**, and **RustFS:
thirteen passes and one server failure**. The exact failures remain those
described above. Owned bucket cleanup completed and the workflow removed the
owned containers/volumes. Evidence is retained at
`/tmp/ess-cli-2380dea-providers/cli-providers`.

The green provider job means that the full required passing contracts and exact
known-limitation policy matched. It does not turn the four server failures into
passes or establish complete provider compatibility. Live AWS account-specific
acceptance remains unrun.
