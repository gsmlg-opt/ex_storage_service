# Gaoxy 0.12.1 migration validation

The candidate replaces Bandit on Phoenix, public S3, and private cluster listeners. S3 and Phoenix preserve cleartext HTTP/2 prior knowledge alongside HTTP/1. Private cluster transport remains HTTP/1 with optional TLS. HEAD representation lengths, inactivity-only storage transfer limits, decoded integrity bounds, raw object bytes, and bounded transfer windows are retained.

Local pre-commit validation used Gaoxy 0.12.1, HTTP 0.21.16, Concord/VSR/ExTurso 3.1.5, Phoenix 1.8.16, and LiveView 1.2.13:

- Complete fast server suites plus retained daily CLI contracts: 1,063 executed, zero failures. The two existing cluster/large_stream exclusions retain separate heavy lanes.
- Additional tagged 2 GiB cluster upload: one test, zero failures; sender process peak 8,968 bytes and receiver peak 55,296 bytes, each below 64 MiB.
- Additional CLI transport and runtime suites: 50 tests, zero failures, including cancellation, pacing, acknowledgement policy, TLS, and standalone server isolation.
- Real private TLS 1.3/ALPN HTTP/1.1 probe: authenticated PUT/HEAD/GET, exact raw bytes, HEAD length 32,768 with no body, unsigned/replayed HEAD rejection. Listener and supervisor stopped; independent connection returned ECONNREFUSED afterward.
- Authenticated Boto3 daily harness: all 14 groups passed with default SDK checksums, gzip raw bytes, multipart, ranges, copy, and versioning.
- Format checks, strict dev/test/prod compilation, and production asset build passed. `mix hex.outdated` reported every direct dependency current. Root and web `npx npm-check-updates@latest -w` checks also reported direct npm dependencies current.

During S3 HTTP/2 test development, a 9 MiB HTTP/1 Req upload intermittently timed out at its existing deadline in a small combined test run. No test or production timeout was increased. Two fresh exact combined runs passed, twelve fresh mixed-protocol listeners accepted exact 9 MiB HTTP/1 uploads through a bounded drain, and the final complete correctness and daily runs passed. The initial failure remains unexplained; representative failure and later probe logs are retained rather than discarded.

Gaoxy #12 and #13 are closed and their storage contracts have real regressions here. Nonblocking gsmlg-dev/gaoxy#15 remains open for logging when an upload peer closes before exchange creation; incomplete uploads remain unpublished and the listener continues serving.

These local receipts do not certify the 38-clause hardening closeout or authorize publication by themselves. The exact committed source must pass the unchanged extended acceptance workflow before the v0.7.0 release workflow can publish.

The archived Hex table removes trailing display padding; version/status values are unchanged.
