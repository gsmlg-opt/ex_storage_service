#!/usr/bin/env python3
"""Generate vectors with pinned AWS CLI 2.31.18's unmodified botocore signer.

PYTHONPATH must point to that archive's awscli directory and its dependencies.
No ExStorageService implementation is imported. Input preparation uses Python's
RFC3986 quote; duplicate query values bypass botocore's dict-based presign
preparation, then use its actual S3 canonical request and signing implementation.
"""
import base64
import hashlib
import json
from pathlib import Path
from urllib.parse import quote, urlencode

import botocore
import botocore.auth
from botocore.awsrequest import AWSRequest
from botocore.credentials import Credentials

AUTH_SOURCE_SHA256 = '298d690158ff374e13b73d764f42bdd410183686771e8d46a60b972980db467d'
source = Path(botocore.auth.__file__)
if hashlib.sha256(source.read_bytes()).hexdigest() != AUTH_SOURCE_SHA256:
    raise SystemExit('Expected unmodified botocore/auth.py from AWS CLI 2.31.18')

ACCESS = 'AKIDEXAMPLE'
SECRET = 'wJalrXUtnFEMI/K7MDENG+bPxRfiCYEXAMPLEKEY'
DATE = '20240115T120000Z'
TOKEN = 'session+/= token'
BODY = bytes([0, 255, 128, 10, 13])


def vector(name, base, key, query, headers, presign=False, token=None):
    path = '/' + quote(key, safe='/-_.~')
    params = list(query)
    if presign:
        params += [('X-Amz-Algorithm', 'AWS4-HMAC-SHA256'),
                   ('X-Amz-Credential', ACCESS + '/20240115/us-east-1/s3/aws4_request'),
                   ('X-Amz-Date', DATE), ('X-Amz-Expires', '3600'),
                   ('X-Amz-SignedHeaders', 'host')]
        if token:
            params.append(('X-Amz-Security-Token', token))
    encoded_query = urlencode(params, quote_via=quote, safe='-_.~')
    url = base + path + ('?' + encoded_query if encoded_query else '')
    request = AWSRequest(method='GET' if presign else 'PUT', url=url, data=BODY)
    for key_, value in headers:
        request.headers.add_header(key_, value)
    request.context['timestamp'] = DATE
    cls = botocore.auth.S3SigV4QueryAuth if presign else botocore.auth.S3SigV4Auth
    signer = cls(Credentials(ACCESS, SECRET, token), 's3', 'us-east-1')
    if not presign:
        signer._modify_request_before_signing(request)
    canonical = signer.canonical_request(request)
    signature = signer.signature(signer.string_to_sign(request, canonical), request)
    if presign:
        expected = base + path + '?' + signer.canonical_query_string(request) + '&X-Amz-Signature=' + signature
    else:
        signer._inject_signature_to_request(request, signature)
        expected = request.headers['Authorization']
    input_query = urlencode(query, quote_via=quote, safe='-_.~')
    input_url = base + path + ('?' + input_query if input_query else '')
    return dict(name=name, method=request.method, base=base, key=key, url=input_url,
                query=query, headers=headers, body_base64=base64.b64encode(BODY).decode(),
                session_token=token, presign=presign, canonical_request=canonical,
                expected=expected)


cases = [
    vector('binary_and_encoded_key', 'https://s3.example.com', 'bucket/a%+?# space/中文//./../end',
           [('same', 'z'), ('same', 'a +/%?'), ('empty', ''), ('versionId', 'é')],
           [('X-Amz-Meta-Title', '  alpha\t beta   '), ('x-amz-meta-title', 'gamma  delta')]),
    vector('ipv6_port_session', 'http://[2001:db8::1]:9000', 'bucket//./../key',
           [('uploads', ''), ('same', '2'), ('same', '1')], [], token=TOKEN),
    vector('default_port', 'https://example.com:443', 'bucket/key', [], []),
    vector('presign_duplicates_session', 'http://[2001:db8::1]:9000', 'bucket/a%+?# space/中文//./../end',
           [('same', 'z'), ('same', 'a +/%?'), ('response-content-disposition', 'attachment; filename="a b"')],
           [], presign=True, token=TOKEN),
    vector('presign_root', 'https://example.com', '', [('empty', '')], [], presign=True),
]
document = dict(reference={'aws_cli': '2.31.18', 'botocore': botocore.__version__,
                          'auth_source_sha256': hashlib.sha256(source.read_bytes()).hexdigest()},
                credentials={'access_key_id': ACCESS, 'secret_access_key': SECRET},
                now='2024-01-15T12:00:00Z', vectors=cases)
Path(__file__).with_name('vectors.json').write_text(json.dumps(document, ensure_ascii=False, indent=2) + '\n')
