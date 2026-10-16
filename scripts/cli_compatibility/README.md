# Locked CLI references

Normal inventory generation is offline and uses only the vendored reference
assets. No ESS command, compilation, or package build calls the capture tool.

```sh
python3 scripts/cli_compatibility/generate.py
python3 scripts/cli_compatibility/generate.py --check
python3 -m unittest discover -s scripts/cli_compatibility -p 'test_*.py'
```

An explicit reference recapture requires Python 3.12+, Go, a Linux x86_64 host
that can execute the pinned official AWS binary, and network access for the
checksum-locked reference archives and mc's locked Go build dependencies:

```sh
python3 scripts/cli_compatibility/capture.py --cache /tmp/ess-cli-reference-m0
python3 scripts/cli_compatibility/generate.py
```

The mc probe is original capture glue compiled into a temporary checkout of
the pinned source. It reads registered command/flag definitions, including
hidden commands and framework-injected help/version flags. The executable's
captured help independently checks visible flag coverage. The six positional `anonymous` actions are separately
extracted from the pinned dispatcher and inventoried; `links` appears only
in examples rather than the usage synopsis. No MinIO implementation source
is copied into ESS. Captured home paths are normalized to `${HOME}`. Three
nested admin help paths return exit 1; their stdout, stderr, and status are
retained instead of omitting those commands.

AWS help is the pinned executable's generated roff, intercepted with a
pass-through `groff` executable. This avoids host renderer differences and
preserves the exact synopsis and help input. Reference documentation warnings
are emitted by the pinned AWS binary. AWS source customizations are reference
data from the matching release, with the complete Apache-2.0 license and
applicable Botocore notice from the checksum-locked distribution; ESS does not execute
them. Bundled model and source model SHA-256 values were compared on capture.

Record validated implementation evidence in
`docs/cli/compatibility/implementation-evidence.json`, keyed by stable requirement
ID. Generation preserves those explicit status, code location, test ID, and
evidence overrides while retaining independent reference/wire/provider fields.
Unknown IDs and unsupported verification claims are rejected. A verified client
requires code locations, test IDs, and a recorded client result; this does not
automatically mark wire or provider evidence as passing. Provider-unavailable
also requires a recorded provider blocker and verified client evidence.

The reference lock records archive, executable, and every asset digest. A new
reference requires reviewing the source revision, archives, notices, generated
command/member/default/enum delta, and compatibility implications. Capturing
an inventory proves discovery, never implemented compatibility. Requirement
IDs and milestone assignments remain stable until a reviewed reference change.

## Standalone packages and published verification

Build the actual source tar, independently resolve its published dependencies,
install its escript, move source/build/dependency directories away, and run the
installed artifact from an unrelated directory:

```sh
PAGER=cat python3 scripts/cli_compatibility/package_probe.py --output-dir /tmp/ess-package-check
```

The output directory must be empty. `manifest.json` records version, source
snapshot, platform/runtime, package and escript SHA256 values, and the verified
Hex payload checksum. `commands.json` records actual commands and outcomes.
The probe checks model/notices/native assets, rejects server applications in
the escript, and exercises real Unicode XML plus every byte value on stdout.
`--package /path/to/package.tar` verifies a supplied tar; `--package-url` permits
only the CLI's public `repo.hex.pm` tarballs. Supplied packages record the probe
checkout separately; they do not inherit its source SHA as package provenance.

After publication, `verify_published.py --version VERSION --manifest MANIFEST
--output OUTPUT.tar` downloads the exact public version, independently computes
its Hex checksum, and requires equality with the validated package. Run a fresh
`package_probe.py --package OUTPUT.tar` afterward. `--candidate-package` also
checks the local candidate against the manifest before any registry request.
Release preflight uses `--allow-missing --status-file STATUS.json`: only HTTP 404
permits publication, while an existing package must have the exact same verified
inner checksum to be reused. Other HTTP/network errors, corrupt packages and
checksum differences fail. `--package-name ex_storage_service` applies the same
verification to the core package.

## Independent provider checks

`provider_probe.py` requires explicitly named, disposable `ess-cli-*`
containers bound only to loopback, without host bind mounts. Both containers
are checked before IAM seed or object writes; operations and teardown use
admitted immutable container IDs. See
[provider acceptance](../../docs/cli/compatibility/provider-acceptance.md) for
the complete command and independently reproduced server limitations. Pass
`--runtime docker` when the containers use Docker; the default is Podman.

The raw probe exits 1 for the four retained provider failures. CI then runs
`provider_policy.py --report REPORT --escript ESCRIPT`. This checks the exact
immutable images, pinned independent AWS executable, CLI hash, full required
contract set, precise known failures, independent controls, and fixture cleanup
against `provider-expectations.json`. Its successful outcome is explicitly
`known-provider-limitations`, with the raw failures retained in artifacts.
Missing evidence, client/reference disagreement, and changed failures require
review and fail CI. A successful policy check does not mean full provider parity.

`cli-compatibility.yml` keeps unit/wire/executable, 2 GiB resource, disposable
provider, and standalone platform/package jobs separate. `publish-cli.yml` is
callable only by the root release workflow. It requires a committed CLI version,
validates the package before publishing or exact reuse, verifies public bytes
and a fresh consumer, and uploads an escript, checksums, manifests and reference
reports as an Actions artifact. The root `release.yml` publishes all components
under the shared `v<version>` tag and attaches the CLI assets to that release.

Linux Actions jobs that exercise journal transfers reserve `49152-65535` from
automatic ephemeral allocation before tests. The runner helper preserves existing
`ip_local_reserved_ports` entries and verifies the resulting reservation. Explicit
journal binds and occupied-port regression tests still use real sockets. This
reduces interference from automatic socket allocation; it does not prevent
existing sockets, explicit binds or journal hash collisions.
