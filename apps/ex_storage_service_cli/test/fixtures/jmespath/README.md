# JMESPath compliance fixtures

The 15 JSON fixture files are unmodified from the published **jmes 0.4.1**
package, which includes these compliance fixtures under its MIT license.
`LICENSE` retains that complete license. `manifest.json` pins every file's SHA256
and the Hex package checksum. This redistributable snapshot contains **837**
semantic/error cases. Runtime modules retain the same complete license in
`lib/ex_storage_service_cli/query/NOTICE.txt`.

The latest official `jmespath/jmespath.test` corpus was independently downloaded
and evaluated from `/tmp`, pinned to revision
`53abcc37901891cf4308fcd910eab287416c4609`. It contains **892 semantic/error
cases** and **16 benchmark-only cases**. All 892 semantic/error cases passed.
Benchmarks are excluded because they provide no expected result; no performance
claim derives from them. The official repository has no explicit LICENSE file,
so those newer fixtures are not redistributed here.

To repeat that independent check, obtain that exact upstream revision and run
from `apps/ex_storage_service_cli`:

```sh
PAGER=cat mix run --no-start test/fixtures/jmespath/validate_external.exs /path/to/jmespath.test/tests
```

The external corpus is an independent oracle. It imports no ESS implementation
to generate its expected results. Passing either corpus establishes tested
JMESPath behavior within the configured budgets, not complete AWS CLI output,
pagination or API parity.
