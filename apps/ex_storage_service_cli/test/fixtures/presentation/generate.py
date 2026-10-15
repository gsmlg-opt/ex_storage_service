#!/usr/bin/env python3
"""Capture actual pinned AWS formatter results, without importing ESS."""
import copy
import hashlib
import io
import json
import unicodedata
from pathlib import Path
from types import SimpleNamespace
import jmespath
import ruamel.yaml
import awscli.formatter as formatter
import awscli.text as text
import awscli.table as table

sources = {name: hashlib.sha256(Path(module.__file__).read_bytes()).hexdigest()
           for name, module in [('formatter', formatter), ('text', text), ('table', table)]}
assert sources == {
    'formatter': 'b7754fca42770a1449a22a2ba1744a27aac1fc18101c21766bb95376c67135c2',
    'text': '6a901025a95957e2b6a2ca14aebcf68b2698b77f05b8b5c1de987093864f5b8f',
    'table': 'da39ecccb9ee7858ff934b3fcd1fa9b65097ba6b65d08ff9e509e6d3b1658eb7',
}, 'Expected original AWS CLI 2.31.18 sources'
assert ruamel.yaml.__version__ == '0.17.21', 'Expected AWS pinned ruamel.yaml version'

def sorted_maps(value):
    if isinstance(value, dict):
        return {key: sorted_maps(value[key]) for key in sorted(value)}
    if isinstance(value, list):
        return [sorted_maps(item) for item in value]
    return value

values = [
    {}, [], None, False, True, 0, 42, 1.5, '', 'hello', '☃',
    [1, 2, None, False], [[1, 2], [3, 4]], [1, [2, 3], 4, []],
    {'Names': ['alice', 'bob']}, {'A': None, 'B': True, 'C': '☃'},
    {'Items': [{'A': 1, 'B': 'first'}, {'B': 'second', 'C': False}]},
    {'Items': [{'A': 1, 'Child': {'Name': 'one'}}, {'B': 2, 'Child': {'Name': 'two'}}]},
    {'Items': [{'A': 1, 'Child': []}, {'A': 2}]},
    {'A': [], 'B': {}, 'C': 'value'},
    {'Contents': [{'Key': 'first', 'Size': 10}, {'Key': 'second', 'Size': 20}], 'IsTruncated': False},
    {'ResponseMetadata': {'RequestId': 'secret-id'}, 'Value': 7},
    {'Description': 'long ' * 20, 'Name': 'sample', 'Number': 4},
    {'Columns': [{'Key': 'é·中文😀', 'Name': 'e\u0301'}]},
    {'Values': [1e-5, 1e-4, 1e15, 1e16, 1e20, -0.0]},
    {'Items': [{'A': 1, 'B': [2, 3]}, {'A': 2, 'B': [4, 5]}]},
]
vectors = []
for index, value in enumerate(values):
    original = sorted_maps(value)
    for output in ['json', 'text', 'table', 'yaml', 'yaml-stream']:
        value = original
        query = None
        if not isinstance(value, (dict, list)):
            value, query = {'Root': value}, 'Root'
        args = SimpleNamespace(query=jmespath.compile(query) if query else None, color='off')
        instance = formatter.get_formatter(output, args)
        if output == 'table':
            instance.table._terminal_width = 80
        stream = io.StringIO()
        vector = {'name': f'{output}-{index}', 'output': output, 'data': value, 'query': query, 'color': 'off', 'terminal_width': 80}
        try:
            instance('ListObjectsV2', copy.deepcopy(value), stream)
            vector['text'] = stream.getvalue()
        except Exception as error:
            vector['error'] = type(error).__name__
        vectors.append(vector)

for output in ['json', 'text', 'table', 'yaml', 'yaml-stream']:
    value = {'Contents': [{'Key': 'a', 'Size': 4}, {'Key': 'b', 'Size': 8}], 'ResponseMetadata': {'RequestId': 'x'}}
    query = 'Contents[?Size > `4`].Key'
    args = SimpleNamespace(query=jmespath.compile(query), color='off')
    instance = formatter.get_formatter(output, args)
    stream = io.StringIO()
    instance('ListObjectsV2', copy.deepcopy(value), stream)
    vectors.append({'name': f'{output}-query', 'output': output, 'data': value, 'query': query, 'text': stream.getvalue()})

for color in ['on', 'off', 'auto']:
    args = SimpleNamespace(query=None, color=color)
    instance = formatter.TableFormatter(args)
    stream = io.StringIO()
    value = {'Items': [{'Name': 'first', 'Child': {'Name': 'one'}}]}
    instance('ListObjectsV2', copy.deepcopy(value), stream)
    vectors.append({'name': f'table-color-{color}', 'output': 'table', 'color': color, 'data': value, 'text': stream.getvalue()})

Path(__file__).with_name('vectors.json').write_text(json.dumps({
    'aws_cli_version': '2.31.18',
    'aws_cli_source_revision': 'f4f66e65e70f23f1bf232ecb4a6d8696f4dda3bc',
    'source_sha256': sources, 'unicode_version': unicodedata.unidata_version, 'vectors': vectors,
}, ensure_ascii=False, indent=2) + '\n')
