#!/usr/bin/env python3
"""Pinned AWS CLI 2.31.18 parser/skeleton oracle, with no ESS imports."""
import datetime
import hashlib
import json
from pathlib import Path
from types import SimpleNamespace
import awscli.argprocess
import awscli.shorthand
from botocore.model import ServiceModel
from botocore import xform_name
from botocore.utils import ArgumentGenerator

SOURCE_REVISION = 'f4f66e65e70f23f1bf232ecb4a6d8696f4dda3bc'
EXPECTED_SOURCES = {
    'argprocess': 'bc7b7408eaff27cc1fb7796e312eed874429a0ad344f9574d2fb4a24393409a2',
    'shorthand': '076f6214d1065875bb8a931e634a8a3d1b18889c4bc28628acc4a003567b3970',
}
sources = {name: hashlib.sha256(Path(module.__file__).read_bytes()).hexdigest()
           for name, module in [('argprocess', awscli.argprocess), ('shorthand', awscli.shorthand)]}
if sources != EXPECTED_SOURCES:
    raise SystemExit('Expected unmodified AWS CLI 2.31.18 parser sources')
repo = Path(__file__).resolve().parents[5]
model_root = repo / 'apps/ex_storage_service_cli/priv/compatibility/aws'
data = json.loads((model_root / 'service-2.json').read_text())
extras = json.loads((model_root / 'service-2.sdk-extras.json').read_text())['merge']
def merge(left, right):
    for key, value in right.items():
        if isinstance(value, dict) and isinstance(left.get(key), dict):
            merge(left[key], value)
        else:
            left[key] = value
merge(data, extras)
model = ServiceModel(data, service_name='s3')
cases = [
    ('PutObject', 'Metadata', r'a=x\,y,b="hello world"'),
    ('DeleteObjects', 'Delete', 'Objects=[{Key=one,VersionId=v1},{Key=two}],Quiet=false'),
    ('PutBucketTagging', 'Tagging', 'TagSet=[{Key=first,Value=""},{Key=second,Value="a,b"}]'),
    ('PutBucketLifecycleConfiguration', 'LifecycleConfiguration', 'Rules=[{ID=r,Status=Enabled,Filter={Prefix=docs/},Expiration={Days=30}}]'),
    ('PutBucketCors', 'CORSConfiguration', 'CORSRules=[{AllowedMethods=[GET,PUT],AllowedOrigins=[https://example.com],MaxAgeSeconds=300}]'),
    ('RestoreObject', 'RestoreRequest', 'Days=5,GlacierJobParameters={Tier=Standard}'),
]
vectors = []
parser = awscli.argprocess.ParamShorthandParser()
for operation, member, value in cases:
    shape = model.operation_model(operation).input_shape.members[member]
    argument = SimpleNamespace(argument_model=shape, cli_name=member, py_name=member, name=member)
    expected = parser(argument, value, 'process-cli-arg.s3.' + operation)
    vectors.append(dict(operation=operation, member=member, value=value, expected=expected))
skeletons = []
for operation in ['PutObject', 'DeleteObjects', 'GetObject', 'PutBucketLifecycleConfiguration']:
    op = model.operation_model(operation)
    for kind, shape in [('input', op.input_shape), ('output', op.output_shape)]:
        expected = ArgumentGenerator(use_member_names=kind == 'output').generate_skeleton(shape) if shape else {}
        skeletons.append(dict(operation=operation, kind=kind, expected=expected))
syntax = []
for value in ['a=b,c=d', 'a=b,c,d=e', 'a=[x,y]', 'a=[x,]', 'a={b=x,c=[y,z]}', 'a="x,y"', r'a=x\,y']:
    syntax.append(dict(value=value, expected=awscli.shorthand.ShorthandParser().parse(value)))
member_flags = {name: {xform_name(member, '-'): member for member in (model.operation_model(name).input_shape.members if model.operation_model(name).input_shape else {})}
                for name in model.operation_names}
doc = dict(reference={'aws_cli': '2.31.18', 'source_revision': SOURCE_REVISION, 'source_sha256': sources},
           shorthand=vectors, syntax=syntax, member_flags=member_flags, skeletons=skeletons)
Path(__file__).with_name('vectors.json').write_text(json.dumps(doc, indent=2, default=lambda obj: obj.isoformat() if isinstance(obj, datetime.datetime) else obj) + '\n')
