#!/usr/bin/env python3
"""Independent pinned AWS CLI safe YAML input/output oracle. No ESS imports."""
import base64
import datetime
import hashlib
import io
import json
from pathlib import Path
from types import SimpleNamespace
import ruamel.yaml
import awscli.customizations.cliinput as cliinput
import awscli.formatter as formatter

EXPECTED_SOURCES = {
    'cliinput': '38f7f26608d60f321a180477d33e38b06ec691e92bbe4a0e512b6fe6c24ed361',
    'formatter': 'b7754fca42770a1449a22a2ba1744a27aac1fc18101c21766bb95376c67135c2',
}
SOURCES = {name: hashlib.sha256(Path(module.__file__).read_bytes()).hexdigest()
           for name, module in [('cliinput', cliinput), ('formatter', formatter)]}
if SOURCES != EXPECTED_SOURCES:
    raise SystemExit('Expected unmodified AWS CLI 2.31.18 YAML input/output sources')

if ruamel.yaml.__version__ != '0.17.21':
    raise SystemExit('Expected ruamel.yaml 0.17.21, pinned by AWS CLI 2.31.18')

def normalized(value):
    if isinstance(value, datetime.datetime):
        if value.tzinfo is None:
            value = value.replace(tzinfo=datetime.timezone.utc)
        value = value.astimezone(datetime.timezone.utc)
        return {'__yaml_tag__': 'timestamp', 'value': value.isoformat().replace('+00:00', 'Z')}
    if isinstance(value, datetime.date):
        return {'__yaml_tag__': 'date', 'value': value.isoformat()}
    if isinstance(value, bytes):
        return {'__yaml_tag__': 'binary', 'value': base64.b64encode(value).decode()}
    if isinstance(value, dict):
        return {key: normalized(item) for key, item in value.items()}
    if isinstance(value, list):
        return [normalized(item) for item in value]
    return value

documents = [
    '', 'null', 'false', '42', 'hello',
    'Bucket: sample\nKey: path/a\nMetadata: {first: one, second: two}\n',
    'Delete:\n  Objects:\n  - Key: one\n  - Key: two\n  Quiet: false\n',
    'value: [1, 2, false, null, "", {x: y}]\n',
    'value: "escaped\\nline\\twith\\u263a"\n',
    "value: 'single ''quoted'' scalar'\n",
    'value: |\n  first\n  second\n',
    'value: |-\n  first\n  second\n',
    'value: |+\n  first\n\n',
    'value: >\n  first\n  second\n',
    'value: >-\n  first\n\n  third\n',
    'base: &base {x: 1, y: 2}\nvalue: {<<: *base, y: 3}\n',
    'base: &base\n  x: 1\n  y: 2\nvalue:\n  <<: *base\n  y: 3\n',
    'first: &first {x: 1, y: 2}\nsecond: &second {x: 3, z: 4}\nvalue: {<<: [*first, *second]}\n',
    'value: {"<<": literal}\n',
    'value: !!str 123\n',
    'value: !!int "42"\n',
    'value: !!float "1.5"\n',
    'value: !!bool "true"\n',
    'value: !!null null\n',
    'value: !!binary SGVsbG8=\n',
    'value: 2024-01-02\n',
    'value: 2024-01-02T03:04:05Z\n',
    'value: 2024-01-02 03:04:05.125 +02:30\n',
    'value: !!timestamp 2024-01-02\n',
    'value: "2024-01-02"\n',
    'value: yes\n', 'value: on\n',
    'value: 1_000\n', 'value: 0b1010\n', 'value: 0o12\n', 'value: 0xFF\n',
    'value: 1_000.25\n', 'value: 1e3\n', 'value: -.25\n',
    'value: 9007199254740993\n',
    '%YAML 1.1\n---\nvalue: yes\n',
    '%YAML 1.1\n---\nvalue: 012\n',
    '%YAML 1.1\n---\nvalue: 1:20\n',
    'key: one\nkey: two\n',
    'value: [unterminated\n',
    'value: !unsupported data\n',
    'value: *missing\n',
    '---\na: 1\n---\nb: 2\n',
]
vectors = []
for index, text in enumerate(documents):
    try:
        value = cliinput.CliInputYAMLArgument._load_parameters(SimpleNamespace(name='cli-input-yaml'), text)
        vectors.append({'name': f'input-{index}', 'text': text, 'value': normalized(value)})
    except Exception:
        vectors.append({'name': f'input-{index}', 'text': text, 'error': True})

outputs = [
    None, False, 42, 1.5, 'hello', '☃', [], {},
    {'a': None, 'b': [], 'c': {}, 'd': False},
    {'Users': [{'UserName': 'alice', 'CreateDate': '2024-01-02T03:04:05Z', 'Path': '/'}]},
    {'a': [1, None, {'x': False}], 'b': '2024-01-01'},
    {'x': '1_000', 'y': 'true', 'z': 'a: b'},
    {'value': 'multiline\ntext\n'},
    {'value': 'first\n second'},
    {'value': 'first\n\nthird'},
    {'value': "a'b\nc"},
    {'value': "a'b", 'other': '\\path'},
    {'value': '\x00\x85\u2028'},
    [[1, 2], [3, 4]],
]
output_vectors = []
for index, value in enumerate(outputs):
    stream = io.StringIO()
    formatter.YAMLDumper().dump(value, stream)
    output_vectors.append({'name': f'output-{index}', 'value': value, 'text': stream.getvalue()})

result = {
    'aws_cli_version': '2.31.18',
    'aws_cli_source_revision': 'f4f66e65e70f23f1bf232ecb4a6d8696f4dda3bc',
    'ruamel_yaml_version': ruamel.yaml.__version__,
    'source_sha256': {name: hashlib.sha256(Path(module.__file__).read_bytes()).hexdigest()
                      for name, module in [('cliinput', cliinput), ('formatter', formatter)]},
    'input': vectors,
    'output': output_vectors,
}
Path(__file__).with_name('vectors.json').write_text(json.dumps(result, ensure_ascii=False, indent=2) + '\n')
