# Independent SigV4a streaming verifier fixture

`sigv4a-streaming.json` contains public AWS example credentials, public-key
coordinates, and string-to-sign fragments copied from aws-c-auth
`tests/test_chunked_signing.c` at revision
`dd505b55fd46222834f35c6e54165d8cbebbfaaa`. The source URL and SHA256 are in the
fixture. Each named value is decoded directly from its C string literal.

The AWS test constructs ECDSA strings to sign from its fixed prefix, the previous
unpadded signature, and its fixed suffix. Our tests do the same with the recorded
fragments and verify against the recorded P-256 public key through OTP crypto.
Signatures are randomized, so no generated signature is stored as an expected
golden. A changed string must fail verification. The source's signing implementation
`source/aws_signing.c` defines a 72-byte maximum DER signature, hex encoding to
144 characters, `*` padding, and trimming that padding before chaining.

The pinned AWS CLI 2.31.18 botocore `crt/auth.py`,
`CrtSigV4AsymAuth.add_auth`, selects `STREAMING-UNSIGNED-PAYLOAD-TRAILER` for a
streaming checksum payload. Signed ECDSA chunk/trailer support is independently
covered here for CRT's explicit streaming algorithm contract.
