# AWS stdin expected-size behavior

AWS `s3 cp - s3://bucket/key --expected-size N` treats N as a sizing hint.
It does not assert the exact input length. ESS now applies this contract only in
the AWS frontend. Native pipe retains its existing exact-length assertion and
mismatch cleanup, covered by native_pipe_test.exs and inventory_policy_test.exs.

`CLI.HighLevel` validates the AWS hint before reading stdin. The bounded private
spool captures actual bytes and remains the replayable upload source. The hint
selects multipart at the pinned eight-MiB threshold and starts part sizing at
eight MiB, doubling within provider part/count limits as needed. Explicit internal
part sizing remains bounded by provider limits. The shared engine then plans the
actual source length, increases the part size further if required, enforces the
actual object limit, and sends exact Content-Length. A hint never pads/truncates
input or permits actual bytes beyond the provider limit.

Independent observations use the actual locked AWS CLI 2.31.18 Linux x86_64
executable, SHA256
`87630ec5232080b737502f4098f4c9adbf820c01d23c63b62affcad50059ad0a`.
A raw loopback peer receives exact `abc` with Content-Length 3 and exit 0 for hints 2
and 4. A 100 GiB hint with 9 MiB of ASCII `a` initiates multipart and sends one 9 MiB
part, proving sizing is affected rather than simply discarding the hint. The
frozen capture, payload hashes and invocation settings live in
`test/fixtures/aws_expected_size/reference.json`; its README describes the fixture.
ESS additionally performs its existing final HEAD verification after completion.

Initial validation in the isolated CLI package on Elixir 1.18.5/OTP 28, coordinated HTTP 0.18.1:
aws_expected_size_test.exs, native_pipe_test.exs, inventory_policy_test.exs and
high_level_test.exs passed 41 tests with zero failures. Strict compilation and scoped
formatting pass. The new regression first failed both expected-size mismatch and
invalid-before-read cases. The subsequent approved HTTP 0.21.12 integration
passed the full CLI suite (3,244 tests, zero failures, one resource exclusion)
and fresh standalone Linux package checks; see integration-validation.md.
Live-provider behavior and #131 slow-consumer completion remain separate
acceptance surfaces. No new
dependency, public interface or spool resource policy is introduced.
