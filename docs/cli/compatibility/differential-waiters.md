# Independent AWS waiter wire matrix

The pinned AWS CLI 2.31.18 executable produced 14 loopback captures across all four modeled S3 waiters. Every waiter has immediate success, one real five-second retry followed by success, and a terminal service error. BucketExists also exercises its modeled 301 and 403 success acceptors. Requests, response sequences, stdout, stderr and exit statuses come from the actual reference executable.

| Waiter | Immediate success | Retry sequence | Terminal service error |
| --- | --- | --- | --- |
| BucketExists | 200, 301, 403 | 404 then 200 | 401 |
| BucketNotExists | 404 | 200 then 404 | 403 |
| ObjectExists | 200 | 404 then 200 | 403 |
| ObjectNotExists | 404 | 200 then 404 | 403 |

Successful waiters produce no stdout/stderr and exit 0. Terminal errors exit 255 and include the waiter name, status-derived code, and service message; for example, `Waiter ObjectExists failed: An error occurred (403): Forbidden`. Synthetic HEAD responses have empty bodies, so these errors use HTTP status semantics rather than XML error payloads.

AWS's S3 region redirect layer consumes and retries the first BucketExists 301 response before the waiter sees the second response. The fixture includes `x-amz-bucket-region: us-east-1` and records both HEAD requests. Omitting that header makes the pinned reference recursively probe HeadBucket for a region; such a malformed redirect fixture is not promoted as a waiter oracle. Other immediate cases issue one HEAD request, and the four retry-then-success cases issue two.

The escript replay compares literal method/path, query pairs, entity bytes, captured headers, exact request count/sequence, output/status, and stderr with surrounding whitespace normalized. Object paths include literal percent sequences, plus signs, spaces and Unicode. Tests retain the model's actual five-second delay; they do not inject a shortened waiter model.

The integrated waiter/region/executable run passes all waiter cases after preserving service error details and allowing one freshly signed same-region correction. A separate repaired baseline/anonymous integration run passes 22 tests; the policy JSON regression exercises public executable dispatch.

## Evidence limits

These are custom loopback endpoints with static fixture credentials. No live AWS authorization, real bucket/object lifecycle transition, endpoint discovery, or S3 region change is established. The twenty-attempt exhaustion path and interruption timing remain separately tested with controlled unit callbacks; this executable matrix does not establish those paths. Captured success/terminal outcomes and request counts do not measure exact delay scheduling.

## Reproduction

AWS executable SHA256: `87630ec5232080b737502f4098f4c9adbf820c01d23c63b62affcad50059ad0a`.
Fixture SHA256: `a038af887ef810e3feb6e6d213940ad6facba438fd171f7d6ab180a3b7c26a5b`.

```sh
PAGER=cat python3 apps/ex_storage_service_cli/test/fixtures/differential/capture_waiters.py \
  --aws /path/to/pinned/aws --output /tmp/waiter-wire.json
```

The generator checks the executable digest, isolates AWS/ESS/MC configuration, disables metadata discovery and network proxy settings, and bounds each subprocess/socket wait. HOME is preserved. Optional `--ess` adds historical diagnostics and must be omitted from promoted oracle data.

Replay from the standalone CLI app:

```sh
PAGER=cat mix test test/ex_storage_service_cli/executable_waiter_matrix_test.exs
```

The inventory audit pins fixture, executable and waiter-model hashes and requires all four waiters plus success/retry/terminal scenarios.
