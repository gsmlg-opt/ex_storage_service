#!/usr/bin/env python3
"""Actual pinned AWS high-level command parser, with its sync registrations."""
import hashlib
import json
from pathlib import Path
from types import SimpleNamespace
import awscli.argparser as argparser
import awscli.customizations.s3.subcommands as subcommands
from awscli.customizations.s3.syncstrategy.register import register_sync_strategies
from botocore.session import Session
from awscli.argprocess import unpack_cli_arg

sources = {name: hashlib.sha256(Path(module.__file__).read_bytes()).hexdigest() for name, module in [('argparser', argparser), ('subcommands', subcommands)]}
assert sources == {
    'argparser': 'a1ad2e4b658150c214bd9125a91071728ef10bd1c9ccbc3516d417ba17fdb588',
    'subcommands': '20d87a3470d7f9ebd3958a6722a39036679dfcd56984479d0f86746528672bb1',
}, 'Expected original AWS CLI 2.31.18 parser and high-level definitions'

root = Path(__file__).resolve().parents[5]
inventory = json.loads((root / 'docs/cli/compatibility/aws-s3-commands.json').read_text())
classes = {
    'cp': subcommands.CpCommand, 'mv': subcommands.MvCommand,
    'sync': subcommands.SyncCommand, 'rm': subcommands.RmCommand,
    'ls': subcommands.ListCommand, 'mb': subcommands.MbCommand,
    'rb': subcommands.RbCommand, 'presign': subcommands.PresignCommand,
    'website': subcommands.WebsiteCommand,
}
vectors = []
for row in inventory['commands']:
    command = row['command']
    session = Session()
    register_sync_strategies({}, session)
    instance = classes[command](session)
    instance.lineage = [SimpleNamespace(name='s3'), instance]
    table = instance._build_arg_table()
    parser = argparser.ArgTableArgParser(table)
    paths = ['./source', 's3://bucket/key'] if command in ['cp', 'mv', 'sync'] else ['s3://bucket/key']
    for argument in row['arguments']:
        if argument['positional']:
            continue
        definition = argument['definition']
        flag = definition['name']
        action = definition.get('action')
        suffix = []
        if action not in ['store_true', 'store_false']:
            if definition.get('nargs') == '+':
                suffix = ['read=id=first', 'full=uri=second']
            elif definition.get('choices'):
                suffix = [definition['choices'][0]]
            elif definition.get('cli_type_name') == 'integer':
                suffix = ['7']
            elif flag == 'metadata':
                suffix = ['first=one,second=two']
            else:
                suffix = ['sample-value']
        argv = paths + ['--' + flag] + suffix
        parsed, remaining = parser.parse_known_args(argv)
        assert remaining == [], (command, flag, remaining)
        destination = definition.get('dest', flag.replace('-', '_'))
        if destination == 'dir_op':
            destination = 'recursive'
        elif destination == 'filters':
            destination = flag
        value = vars(parsed)[definition.get('dest', flag.replace('-', '_'))]
        if destination in ['include', 'exclude']:
            value = value[0][1]
        # AWS deliberately leaves typed integers as strings in argparse, then
        # applies this actual scalar coercer before the command handler. ESS's
        # pure parser combines those two phases into one typed invocation.
        if definition.get('cli_type_name') == 'integer':
            value = unpack_cli_arg(table[flag], value)
        vectors.append({'command': command, 'flag': flag, 'argv': argv, 'key': destination, 'value': value})
        if definition.get('nargs') == '?':
            parsed, remaining = parser.parse_known_args(paths + ['--' + flag])
            vectors.append({'command': command, 'flag': flag + '-default', 'argv': paths + ['--' + flag], 'key': destination, 'value': vars(parsed)[destination]})
        if definition.get('choices'):
            try:
                parser.parse_known_args(paths + ['--' + flag, 'invalid-private-value'])
                raise AssertionError('Invalid choice unexpectedly accepted')
            except argparser.ArgParseException:
                vectors.append({'command': command, 'flag': flag + '-invalid', 'argv': paths + ['--' + flag, 'invalid-private-value'], 'error': True})

Path(__file__).with_name('vectors.json').write_text(json.dumps({
    'aws_cli_version': '2.31.18',
    'source_sha256': sources,
    'vectors': vectors,
}, indent=2) + '\n')
