# Native address pinning

Native `--resolve HOST[:PORT]=literal-IP` uses the locked http_fetch 0.18.1
`connect_address` API. The request URL, Host header, canonical request, TLS SNI
and certificate hostname remain the original authority. Only the socket dial
address changes. There is no DNS fallback after a failed pin, URL rewriting,
global resolver mutation or redirect replay.

Entries are invocation-local and may repeat. The last entry for an authority
wins. An explicit port takes priority over a host-only entry; a host-only entry
applies to all that host's ports. IPv4 and IPv6 literals are accepted, including
bracketed IPv6 authorities. Ports must be 1–65535. DNS names in the address
position, wildcards, credentials, paths, malformed addresses and scoped IPv6
addresses fail before dispatch without echoing values. Scoped addresses are
rejected because the transport's IP tuple cannot retain an interface scope;
OTP's literal parser otherwise discards that suffix. AWS namespaces do not
accept this native option.

The pin is selected separately for each actual request authority, including
fresh regional requests and independent copy contexts. Both modeled and legacy
request paths use it. Manual redirects continue to produce an error without
forwarding the signature or address pin to the destination. Transport protocol,
TLS verification, cancellation and deadline ownership remain with http_fetch.

## Evidence

The pinned mc source (`cmd/globals.go:185–208` and `cmd/client-s3.go:96–151`)
stores a last-wins map and replaces the dial address while retaining TLS SNI.
The actual locked executable independently passed two loopback cases: exact
authority and repeated authority with a last pin replacing an unreachable IP.
Both preserve `pin.invalid:PORT` in the listing and HEAD requests. The fixture
is `test/fixtures/differential/native-resolve-wire.json`, SHA256
`6df6e17cdecd92f773c83d05900b841cf9d4987350efc5aa1c8fe9f021064e65`;
its capture script verifies the executable digest first.

The capture initially returned an object-not-found result because the test peer
answered mc's preliminary listing with a location document. Giving the peer a
real listing result fixed the fixture; no production behavior was changed.
The first ESS integration run caught missing repeated-option collection in
the parser. The next caught OTP silently dropping an IPv6 scope suffix; both
were corrected with the strict assertions retained.

The final focused resolve/TLS/native-global/legacy-client run passed 32 tests.
It includes real sockets, original signing Host, no DNS fallback, manual
redirect isolation, original TLS SNI, successful custom-CA verification and
rejection of a wrong original certificate identity. The fixed mc source looks
up the complete dial authority; ESS additionally makes the documented host-only
form useful across ports. No MC_RESOLVE environment alias or full mc
grammar/default/output parity is claimed. Final integrated and installed
artifact evidence are separate gates.
