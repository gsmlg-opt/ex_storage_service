# Credential implementation evidence and remaining gaps

The credential implementation uses the pinned AWS CLI 2.31.18 `botocore/credentials.py`, `tokens.py`, and `session.py` provider order. Scoped credential, login/logout command, and native-resource suites provide local implementation evidence, not full AWS parity or a live AWS authentication claim.

Implemented providers are coherent environment/static/session records, shared credentials/config, bounded literal-argv `credential_process`, STS AssumeRole/source_profile/credential_source, web identity, container credentials, IMDSv2, IAM Identity Center cached legacy/session profiles with OIDC refresh, default PKCE login and explicit device login, Original EC2 credential files, and Boto config files. Explicit profiles disable environment credentials; environment-selected profiles retain their pinned distinction. Missing selected profiles and incomplete pairs fail explicitly. Signed requests do not borrow another provider's token. Unsigned contexts read nonsecret shared-profile configuration but never open the credentials file or invoke a credential provider. Profile S3, output, binary format, retry, timeout, checksum, FIPS/dualstack and CA settings retain their documented precedence.

Wire fixtures verify STS source signing and explicit region, unsigned web-identity exchange, container authorization, IMDS token/role/credential sequencing, response limits, expiry rejection, redirect non-forwarding, SSO role credentials, OIDC refresh, and pending-to-success device login. Credential-process tests verify literal arguments, bounded stdout/deadline, stderr discard, and timeout/owner-death cleanup of owned Linux process groups. Token cache publication is atomic and uses mode 0600. Refresh owners use pinned 900-second advisory and 600-second mandatory windows, serialize concurrent loads, permit still-valid advisory reuse, bound and cancel waiters, avoid expired fallback, redact diagnostic status, monitor invocation ownership, and stop abandoned refresh workers. The 60-second refresh-attempt throttle applies to SSO token records; base credentials retain the pinned unthrottled behavior. IMDS conditionally falls back to v1 after eligible token failures, honors environment/profile v1-disable policy, and never downgrades after malformed token responses, token HTTP 400, redirects, or authenticated GET 401. Native selected-profile SSO login/logout commands avoid storage credential resolution; logout removes only the selected token cache and rejects symlinks.

Actual loopback IMDS peers stall the token response body after HTTP 200. The enabled policy closes the timed-out token request before two tokenless IMDSv1 credential GETs; v1-disabled policy closes that request without any credential GET. Killing the invocation while its token request is stalled closes the wire and never starts an IMDSv1 downgrade. These cases live in `credentials/metadata_test.exs` and match the pinned `botocore/utils.py` read-timeout fallback.

The shared PKCE/device notification follows pinned `awscli/customizations/sso/utils.py` `OpenBrowserHandler`: preserve manual URL/code display, open the complete verification URL, and treat launcher exceptions as nonfatal. By default Linux invokes `xdg-open` with the HTTPS URL as one literal argument through `spawn_executable`, without a shell wrapper. Both child output streams are discarded, output and invocation time are bounded, and timeout or invocation death kills only the launcher-owned process group. `--no-browser` suppresses the default launcher and injected `open_browser` callback. The dedicated `credentials/browser_test.exs` uses actual helper processes to prove argv preservation, output suppression, failure fallback, timeout, and owner-death cleanup; `credentials/sso_test.exs` proves device-flow notifier integration. No real desktop or live IAM Identity Center session was exercised.

Remaining compatibility work:

- `credential_process` is currently Linux-only; other platforms return `credential_process_platform_unavailable` rather than run an unvalidated cleanup strategy.
- Production IMDS endpoint overrides currently allow only standard metadata addresses; loopback is an explicit test override.
- PKCE defaults to a bounded loopback callback with state validation, S256, registration scopes, token exchange, and atomic cache publication. Device login uses `use_device_code: true`. Default browser launch is validated only on Linux; unsupported platforms, missing launchers, and launch failures retain manual URL/code display. Python `webbrowser` custom `BROWSER` command selection is not implemented; the launcher unsets `BROWSER` and uses the desktop default through `xdg-open`. Caller-injected browser callbacks remain caller-owned. Top-level login/logout executable acceptance remains an integration check beyond the scoped command-module tests.
- MFA accepts an explicit six-digit `mfa_code`, a caller-owned `mfa_prompt`, or a real terminal prompt backed by native echo suppression. A real PTY-to-STS probe proves that the code is transmitted without terminal echo and restores termios. Noninteractive input fails with `mfa_required`. The prompt path depends on the matching packaged native runtime.
- Endpoint partition variants for STS/SSO currently cover ordinary AWS, China, and GovCloud. Full pinned partition/endpoint-rule coverage belongs to the endpoint milestone.
- Credential HTTP calls verify TLS peers and accept the resolved CA bundle. Actual TLS/custom-CA and live STS/SSO/metadata-service acceptance have not been exercised against real providers.

The user-approved coordinated HTTP update is integrated in the working copy:
core admits 0.21.12 alongside its existing alternatives, CLI requires 0.21.12,
and the lock selects the published coordinated family without overrides.
AWS `--cli-read-timeout` now controls socket read inactivity independently of
total request duration: positive seconds reset with network progress, zero
disables the limit. Connect timeout is independent and zero selects infinity.
Real finite-read trickle/stall and unchanged-default 65-second AWS-zero/native
network-gap fixtures pass. The combined CLI suite passed 3,244 tests with
zero failures and one resource exclusion (seed 628644), and all four
affected-app suites passed with zero failures. Logs are
`/tmp/ess-cli-http02112-coordinated-20261016/{cli-full,affected-apps}.log`.
These local tests do not establish live credential-provider acceptance; fresh
package acceptance and remote checks remain separate gates.

Historically, the selected 0.18.1 graph rejected infinity before I/O and depended
on [#123](https://github.com/gsmlg-dev/http_fetch/issues/123); 0.21.11 also had
[#129](https://github.com/gsmlg-dev/http_fetch/issues/129)'s separate response
stream idle cap. Those version-specific limits are resolved by the selected
0.21.12 API plus its explicit request-scoped stream option. They are not current
blockers of this graph. The remaining release blocker is
[#131](https://github.com/gsmlg-dev/http_fetch/issues/131): fixed consumer ACK
budgets can expire during valid bounded slow-rate delivery. No global timer
mutation or dependency patch is used. See
[http-021-evaluation.md](http-021-evaluation.md) for exact wire evidence and the
scope of that distinct limit.
