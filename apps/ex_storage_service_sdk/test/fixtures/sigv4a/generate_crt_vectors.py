"""Development-only oracle: awscrt==0.27.6 and cryptography; never used at runtime."""
import hashlib
import json
from datetime import datetime, timezone
from pathlib import Path
from urllib.parse import parse_qsl, quote, urlsplit

from awscrt import auth, http
from cryptography.hazmat.primitives import hashes
from cryptography.hazmat.primitives.asymmetric import ec

ACCESS = "AKIDEXAMPLE"
SECRET = "wJalrXUtnFEMI/K7MDENG+bPxRfiCYEXAMPLEKEY"
PUBLIC = "04b6618f6a65740a99e650b33b6b4b5bd0d43b176d721a3edfea7e7d2d56d936b1865ed22a7eadc9c5cb9d2cbaca1b3699139fedc5043dc6661864218330c8e518"
DATE = datetime(2024, 1, 15, 12, tzinfo=timezone.utc)
PATH = "/a%25%2B%3F%23%20/%E4%B8%AD%E6%96%87//./../end?dup=z&dup=a%20%2B&empty="
CASES = [
    ("wildcard-key", "GET", PATH, "*", None, "UNSIGNED-PAYLOAD"),
    ("regions-token", "GET", PATH, "us-east-1,us-west-*", "token +/=", "UNSIGNED-PAYLOAD"),
    ("streaming-digest", "PUT", "/binary%00%FF", "*", "temporary-token", hashlib.sha256(b"\x00\xff").hexdigest()),
    ("duplicate-query", "GET", "/?Param1=value2&Param1=Value1", "us-east-1", None, hashlib.sha256(b"").hexdigest()),
]


def encoded_query(pairs):
    # parse_qsl's form-decoding is safe here: fixtures deliberately escape all '+' bytes.
    return "&".join(f"{quote(k, safe='-._~')}={quote(v, safe='-._~')}" for k, v in sorted(pairs, key=lambda pair: (quote(pair[0], safe='-._~'), quote(pair[1], safe='-._~'))))


vectors = []
for name, method, path, region, token, payload in CASES:
    provider = auth.AwsCredentialsProvider.new_static(ACCESS, SECRET, token)
    original_headers = [("Host", "example.amazonaws.com"), ("x-amz-meta-name", "  value   with space  ")]
    for presign in [False, True]:
        request = http.HttpRequest(method=method, path=path, headers=http.HttpHeaders(original_headers))
        config = auth.AwsSigningConfig(
            algorithm=auth.AwsSigningAlgorithm.V4_ASYMMETRIC,
            signature_type=auth.AwsSignatureType.HTTP_REQUEST_QUERY_PARAMS if presign else auth.AwsSignatureType.HTTP_REQUEST_HEADERS,
            credentials_provider=provider, region=region, service="s3", date=DATE,
            use_double_uri_encode=False, should_normalize_uri_path=False,
            signed_body_value=payload,
            signed_body_header_type=auth.AwsSignedBodyHeaderType.NONE if presign else auth.AwsSignedBodyHeaderType.X_AMZ_CONTENT_SHA_256,
            expiration_in_seconds=3600 if presign else None,
        )
        signed = auth.aws_sign_request(request, config).result()
        headers = list(signed.headers)
        if presign:
            query = dict(parse_qsl(urlsplit(signed.path).query, keep_blank_values=True))
            signature = query["X-Amz-Signature"]
            names = query["X-Amz-SignedHeaders"].split(";")
        else:
            authorization = dict(headers)["Authorization"]
            signature = authorization.split("Signature=", 1)[1]
            names = authorization.split("SignedHeaders=", 1)[1].split(",", 1)[0].split(";")
        canonical_headers = {}
        for key, value in headers:
            if key.lower() in names:
                canonical_headers.setdefault(key.lower(), []).append(" ".join(value.split()))
        header_block = "".join(f"{key}:{','.join(canonical_headers[key])}\n" for key in names)
        uri = urlsplit(signed.path)
        pairs = [(key, value) for key, value in parse_qsl(uri.query, keep_blank_values=True) if key != "X-Amz-Signature"]
        canonical = "\n".join([method, uri.path, encoded_query(pairs), header_block, ";".join(names), payload])
        string_to_sign = "AWS4-ECDSA-P256-SHA256\n20240115T120000Z\n20240115/s3/aws4_request\n" + hashlib.sha256(canonical.encode()).hexdigest()
        ec.EllipticCurvePublicKey.from_encoded_point(ec.SECP256R1(), bytes.fromhex(PUBLIC)).verify(bytes.fromhex(signature), string_to_sign.encode(), ec.ECDSA(hashes.SHA256()))
        vectors.append(dict(name=name, presign=presign, method=method, url="https://example.amazonaws.com" + path,
                            region_set=region, token=token, payload_hash=payload, headers=original_headers,
                            canonical_request=canonical, string_to_sign=string_to_sign, signature=signature, public_key=PUBLIC))
Path(__file__).with_name("crt-vectors.json").write_text(json.dumps(dict(source="awscrt 0.27.6", credentials=dict(access_key_id=ACCESS, secret_access_key=SECRET), vectors=vectors), indent=2) + "\n")
print(f"Verified {len(vectors)} CRT S3 signatures independently with cryptography")
