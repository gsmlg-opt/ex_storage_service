#!/usr/bin/env python3
"""Generate exhaustive compatibility ledgers offline from locked reference data."""
import argparse
import ast
from collections import Counter
import hashlib
import json
from pathlib import Path
import re

ROOT = Path(__file__).resolve().parents[2]
ASSETS = ROOT / 'apps/ex_storage_service_cli/priv/compatibility'
DOCS = ROOT / 'docs/cli/compatibility'


def load(path):
    return json.loads(path.read_text())


def kebab(name):
    return re.sub(r'([a-z0-9])([A-Z])', r'\1-\2', re.sub(r'([A-Z]+)([A-Z][a-z])', r'\1-\2', name)).lower()


def row(identifier, milestone, reference, status='planned', **fields):
    return dict(requirement_id=identifier, status=status, owner_milestone=milestone,
                reference=reference, code_locations=[], test_ids=[],
                evidence={'inventory': 'locked reference; generated offline', 'wire': 'not-run', 'provider': 'not-run'}, **fields)


def traits(value):
    return {k: v for k, v in value.items() if k not in {'documentation', 'members', 'member', 'key', 'value', 'enum'}}


def mc_classify(path):
    top = path[0]
    if top in {'admin', 'batch', 'idp', 'license', 'quota', 'support'} or path[:2] == ['ilm', 'tier']:
        return 'provider-specific-extension', 'out-of-scope-separate-service', 'MinIO administration protocol; outside portable S3 parity'
    if top == 'replicate' and len(path) > 1 and path[1] in {'status', 'resync', 'backlog'}:
        return 'provider-specific-extension', 'out-of-scope-separate-service', 'MinIO replication administration protocol'
    if top == 'od':
        return 'explicitly-outside-scope', 'out-of-scope-separate-service', 'Optional benchmarking is outside required portable S3 parity'
    if top == 'update':
        return 'explicitly-outside-scope', 'out-of-scope-separate-service', 'MinIO binary self-update does not apply to the ESS escript'
    if top in {'alias', 'diff', 'find', 'mirror', 'tree', 'du', 'share', 'ping'}:
        return 'client-side-utility', 'planned', 'Local client policy over portable APIs; no MinIO implementation copied'
    if top in {'ready', 'watch'}:
        return 'provider-specific-extension', 'planned', 'Explicit provider adapter required; never emulate a native event stream with unnamed polling'
    return 'portable-s3', 'planned', 'Portable S3 operations; individual provider flags remain separately classified'


def mc_inventory():
    tree = load(ASSETS / 'mc/command-tree.json')
    registered_count = len(tree['commands'])
    for variant in load(ASSETS / 'mc/action-variants.json'):
        parent = next(item for item in tree['commands'] if item['path'] == variant['parent'])
        tree['commands'].append(dict(parent, **variant))
    results = {tuple(item['path']): item for item in load(ASSETS / 'mc/help-results.json')}
    commands = []
    def flag(item, identifier, milestone, reference, parent_status='planned'):
        definition = item['definition']
        names = [s.strip() for s in definition['Name'].split(',')]
        default = definition.get('Value')
        if default is None:
            default = True if item['type'] == 'cli.BoolTFlag' else False if item['type'] == 'cli.BoolFlag' else None
        classification = 'client-side-utility' if identifier.startswith('MC:global:') else 'portable-s3'
        provider_flags = {'zip', 'purge', 'tier', 'transition-tier', 'noncurrentversion-tier', 'noncurrent-transition-tier', 'expire-all-object-versions', 'bandwidth', 'sync', 'healthcheck-seconds', 'disable-proxy', 'proxy'}
        if names[0] in provider_flags or parent_status == 'out-of-scope-separate-service':
            classification = 'provider-specific-extension'
        return row(identifier, milestone, reference, parent_status, names=names, classification=classification, provider_requirement='Explicit provider capability required' if classification == 'provider-specific-extension' else 'Client utility or portable API; see command boundary', type=item['type'], default=default, default_origin='pinned flag definition; boolean intrinsic default where Value is absent', environment=definition.get('EnvVar'), hidden=definition.get('Hidden', False), definition=definition, definition_origin=item.get('origin', 'registered flag'), help=item['help'])
    for item in tree['commands']:
        path = item['path']
        classification, status, boundary = mc_classify(path)
        milestone = 'M1' if path[0] == 'alias' else 'M4' if path[0] in {'cp', 'mv', 'rm', 'mb', 'rb', 'mirror', 'diff', 'get', 'put', 'cat', 'head', 'pipe'} else 'M8'
        reference = 'apps/ex_storage_service_cli/priv/compatibility/mc/help/' + '__'.join(path) + '.txt'
        identifier = 'MC:' + ':'.join(path)
        differences = []
        if path == ['version']:
            differences.append('ess version without a subcommand prints the CLI version; bucket versioning uses explicit enable|suspend|info subcommands')
        if path == ['stat']:
            differences.append('ess info remains legacy ESS health; object inspection uses stat')
        if path == ['policy']:
            differences.append('Pinned mc policy is hidden and only prints a migration notice to anonymous; ESS policy workflows are a separately planned API')
        argument_variants = [row(identifier + ':permission:' + value, milestone, reference, value=value, help_documented=value != 'none') for value in tree['anonymous_permissions']] if path == ['anonymous', 'set'] else []
        commands.append(row(identifier, milestone, reference, status, path=path, aliases=item['aliases'] or [], hidden=item['hidden'], has_action=item['has_action'], usage=item['usage'], args_usage=item['args_usage'], description=item['description'], classification=classification, provider_requirement=boundary, mapping=('ess ' + ' '.join(path)) if status == 'planned' else None, intentional_differences=differences, dispatch=item.get('dispatch', 'registered command'), source_variant=item.get('source_sha256'), argument_variants=argument_variants, target_grammar='Pinned usage/description in this row and help reference; native ESS resolution follows design.md section 4.3', help_capture=results[tuple(path)], flags=[flag(f, identifier + ':flag:' + f['definition']['Name'].split(',')[0], milestone, reference, status) for f in item['flags']]))
    return {'schema_version': 1, 'reference': 'minio/mc@77f82e18b5401a65958f1619df6ebb994634bd88', 'registered_command_count': registered_count, 'positional_action_variant_count': len(tree['commands']) - registered_count, 'commands': commands, 'global_flags': [flag(f, 'MC:global:' + f['definition']['Name'].split(',')[0], 'M1', 'apps/ex_storage_service_cli/priv/compatibility/mc/help/root.txt') for f in tree['global_flags']]}


def static_value(node, env):
    if isinstance(node, ast.Constant):
        return node.value
    if isinstance(node, ast.Name):
        return env.get(node.id, {'source_symbol': node.id})
    if isinstance(node, (ast.List, ast.Tuple)):
        return [static_value(n, env) for n in node.elts]
    if isinstance(node, ast.Dict):
        return {static_value(k, env): static_value(v, env) for k, v in zip(node.keys, node.values)}
    if isinstance(node, ast.BinOp) and isinstance(node.op, ast.Add):
        return static_value(node.left, env) + static_value(node.right, env)
    return {'source_expression': ast.unparse(node)}


def source_tables(path):
    env = {}
    classes = {}
    for node in ast.parse(path.read_text()).body:
        if isinstance(node, ast.Assign):
            for target in node.targets:
                if isinstance(target, ast.Name):
                    env[target.id] = static_value(node.value, env)
        elif isinstance(node, ast.ClassDef):
            local = dict(env)
            for child in node.body:
                if isinstance(child, ast.Assign):
                    for target in child.targets:
                        if isinstance(target, ast.Name):
                            local[target.id] = static_value(child.value, local)
            if 'ARG_TABLE' in local:
                classes[local['NAME']] = local
    return env, classes


def help_flags(path):
    source = path.read_text().replace('\\-', '-')
    synopsis = source.split('.SH SYNOPSIS\n', 1)[1].split('.SH OPTIONS\n', 1)[0]
    return set(re.findall(r'--([a-z][a-z0-9-]*)', synopsis))


def generic_cli_arguments(identifier, reference):
    result = []
    for filename, class_name in [('cliinput.py', 'CliInputJSONArgument'), ('cliinput.py', 'CliInputYAMLArgument'), ('generatecliskeleton.py', 'GenerateCliSkeletonArgument')]:
        source = ASSETS / 'aws/customizations' / filename
        node = next(node for node in ast.parse(source.read_text()).body if isinstance(node, ast.ClassDef) and node.name == class_name)
        assignment = next(item for item in node.body if isinstance(item, ast.Assign) and any(isinstance(target, ast.Name) and target.id == 'ARG_DATA' for target in item.targets))
        definition = static_value(assignment.value, {})
        name = definition['name']
        result.append(row(identifier + ':cli-flag:' + name, 'M5', str(source.relative_to(ROOT)), name=name, definition=definition, default=None, default_origin='Absent flag is None; flag without value uses const when defined', enum_values=[row(identifier + ':cli-flag:' + name + ':enum:' + value, 'M5', reference, value=value) for value in definition.get('choices', [])], applicability='Excluded when outfile positional argument is present'))
    return result


def aws_s3_inventory():
    _, tables = source_tables(ASSETS / 'aws/customizations/s3/subcommands.py')
    commands = []
    globals_raw = load(ASSETS / 'aws/cli.json')['options']
    global_flags = []
    for name, definition in sorted(globals_raw.items()):
        global_flags.append(row('AWS:global:' + name, 'M5', 'apps/ex_storage_service_cli/priv/compatibility/aws/cli.json', name=name, definition=definition, default=definition.get('default'), default_origin='cli.json when provided; AWS config/environment otherwise'))
    global_names = set(globals_raw)
    for name, table in sorted(tables.items()):
        arguments = list(table['ARG_TABLE'])
        if name == 'sync':
            for filename, symbol in [('sizeonly.py', 'SIZE_ONLY'), ('exacttimestamps.py', 'EXACT_TIMESTAMPS'), ('delete.py', 'DELETE')]:
                env, _ = source_tables(ASSETS / 'aws/customizations/s3/syncstrategy' / filename)
                arguments.append(env[symbol])
        reference = 'apps/ex_storage_service_cli/priv/compatibility/aws/help/s3__' + name + '.roff'
        binary_flags = help_flags(ROOT / reference)
        source_flags = {arg['name'] for arg in arguments if not arg.get('positional_arg')}
        unexpected = binary_flags - source_flags - global_names
        if unexpected:
            raise ValueError('Uninventoried binary flags for ' + name + ': ' + ', '.join(sorted(unexpected)))
        flags = []
        for argument in arguments:
            default = argument.get('default')
            if 'default' not in argument and argument.get('action') == 'store_true':
                default = False
            elif 'default' not in argument and argument.get('action') == 'store_false':
                default = True
            identifier = 'AWS:s3:' + name + ':arg:' + argument['name']
            flags.append(row(identifier, 'M4', reference, definition=argument, default=default, default_origin='matching source ARG_TABLE; argparse boolean default when omitted', positional=argument.get('positional_arg', False), enum_values=[row(identifier + ':enum:' + value, 'M7' if argument['name'] == 'checksum-algorithm' else 'M4', reference, value=value) for value in argument.get('choices', [])]))
        commands.append(row('AWS:s3:' + name, 'M4', reference, command=name, arguments=flags, global_flags=sorted(binary_flags & global_names), source_flags_not_in_help=sorted(source_flags - binary_flags), description=table.get('DESCRIPTION'), input_rule=table.get('USAGE'), output_rule='Pinned executable help and matching customizations; differential output testing required'))
    if len(commands) != 9:
        raise ValueError('Expected complete pinned nine-command S3 namespace')
    return {'schema_version': 1, 'reference': 'aws-cli/2.31.18', 'commands': commands, 'global_flags': global_flags, 'namespace_difference': 'Pinned release includes website: nine high-level commands, rather than the historical eight-command assumption.'}


def merge(base, extra):
    for key, value in extra.items():
        if isinstance(value, dict) and isinstance(base.get(key), dict):
            merge(base[key], value)
        else:
            base[key] = value


def op_milestone(name):
    if name in {'CreateSession', 'RenameObject', 'SelectObjectContent', 'WriteGetObjectResponse', 'GetObjectTorrent'}:
        return 'M7'
    if name in {'GetObject', 'PutObject', 'CopyObject', 'CreateMultipartUpload', 'UploadPart', 'UploadPartCopy', 'CompleteMultipartUpload', 'AbortMultipartUpload', 'ListParts', 'ListMultipartUploads'}:
        return 'M3'
    return 'M6'


def aws_api_inventory():
    model = load(ASSETS / 'aws/service-2.json')
    merge(model, load(ASSETS / 'aws/service-2.sdk-extras.json')['merge'])
    shapes = model['shapes']
    paginators = load(ASSETS / 'aws/paginators-1.json')
    merge(paginators, load(ASSETS / 'aws/paginators-1.sdk-extras.json')['merge'])
    paginators = paginators['pagination']
    waiters = load(ASSETS / 'aws/waiters-2.json')['waiters']
    operations = []
    def members(shape_name, parent, seen, milestone, reference):
        shape = shapes[shape_name]
        if shape_name in seen:
            return [row(parent + ':recursive-shape', milestone, reference, shape=shape_name, recursive=True)]
        records = []
        children = list(shape.get('members', {}).items())
        children += [('[]', shape['member'])] if 'member' in shape else []
        children += [('[key]', shape['key']), ('[value]', shape['value'])] if 'key' in shape else []
        for name, member in children:
            target = shapes[member['shape']]
            identifier = parent + '.' + name
            owner = 'M7' if 'Checksum' in name or target.get('eventstream') or target.get('event') else milestone
            records.append(row(identifier, owner, reference, member=name, shape=member['shape'], type=target['type'], required=name in shape.get('required', []), binding_traits=traits(member), shape_traits=traits(target), enum_values=[row(identifier + ':enum:' + value, owner, reference, value=value) for value in target.get('enum', [])], children=members(member['shape'], identifier, seen | {shape_name}, owner, reference)))
        return records
    for name, definition in sorted(model['operations'].items()):
        identifier = 'AWS:s3api:' + kebab(name)
        reference = 'apps/ex_storage_service_cli/priv/compatibility/aws/help/s3api__' + kebab(name) + '.roff'
        milestone = op_milestone(name)
        input_shape = definition.get('input', {}).get('shape')
        output_shape = definition.get('output', {}).get('shape')
        flags = sorted(help_flags(ROOT / reference))
        cli_arguments = [row(identifier + ':cli-flag:' + flag, 'M5', reference, name=flag, default=None, default_origin='Absent modeled values stay absent; global/configuration defaults are defined in aws-s3-commands.json and matching customization source') for flag in flags]
        output_payload = shapes.get(output_shape, {}).get('payload')
        payload_member = shapes.get(output_shape, {}).get('members', {}).get(output_payload, {})
        payload_shape = shapes.get(payload_member.get('shape'), {})
        customizations = []
        if payload_member.get('streaming') or payload_shape.get('streaming') or name == 'SelectObjectContent':
            customizations = ['apps/ex_storage_service_cli/priv/compatibility/aws/customizations/s3events.py' if name == 'SelectObjectContent' else 'apps/ex_storage_service_cli/priv/compatibility/aws/customizations/streamingoutputarg.py']
            cli_arguments.append(row(identifier + ':positional:outfile', 'M5', reference, name='outfile', positional=True, required=True, response_payload=output_payload, default=None))
        else:
            existing = {item['name'] for item in cli_arguments}
            cli_arguments.extend(item for item in generic_cli_arguments(identifier, reference) if item['name'] not in existing)
        operations.append(row(identifier, milestone, reference, operation=name, command=kebab(name), protocol_traits=traits(definition), input_shape=input_shape, output_shape=output_shape, input_members=members(input_shape, identifier + ':input', set(), milestone, reference) if input_shape else [], output_members=members(output_shape, identifier + ':output', set(), milestone, reference) if output_shape else [], paginator=row(identifier + ':paginator', 'M5', 'apps/ex_storage_service_cli/priv/compatibility/aws/paginators-1.json', definition=paginators[name]) if name in paginators else None, cli_arguments=cli_arguments, customization_references=customizations, special_handler_required=milestone == 'M7'))
    rules = load(ASSETS / 'aws/endpoint-rule-set-1.json')
    schemes = {}
    def scan(value):
        if isinstance(value, dict):
            for scheme in value.get('authSchemes', []):
                schemes[json.dumps(scheme, sort_keys=True)] = scheme
            for child in value.values():
                scan(child)
        elif isinstance(value, list):
            for child in value:
                scan(child)
    scan(rules)
    return {'schema_version': 1, 'reference': 'aws-cli/2.31.18; bundled S3 2006-03-01; sdk-extras merged', 'operations': operations, 'shapes': {name: row('AWS:shape:' + name, 'M5', 'apps/ex_storage_service_cli/priv/compatibility/aws/service-2.json', definition=shape) for name, shape in sorted(shapes.items())}, 'waiters': [row('AWS:s3api:wait:' + kebab(name), 'M5', 'apps/ex_storage_service_cli/priv/compatibility/aws/waiters-2.json', command=kebab(name), definition=definition, cli_arguments=generic_cli_arguments('AWS:s3api:wait:' + kebab(name), 'apps/ex_storage_service_cli/priv/compatibility/aws/help/s3api__wait__' + kebab(name) + '.roff')) for name, definition in sorted(waiters.items())], 'endpoint_parameters': [row('AWS:endpoint:' + name, 'M7', 'apps/ex_storage_service_cli/priv/compatibility/aws/endpoint-rule-set-1.json', name=name, definition=definition) for name, definition in sorted(rules['parameters'].items())], 'endpoint_auth_variants': [row('AWS:auth:' + str(index), 'M7' if scheme['name'] != 'sigv4' else 'M2', 'apps/ex_storage_service_cli/priv/compatibility/aws/endpoint-rule-set-1.json', definition=scheme) for index, (_, scheme) in enumerate(sorted(schemes.items()))]}


def all_rows(value):
    if isinstance(value, dict):
        if 'requirement_id' in value:
            yield value
        for child in value.values():
            yield from all_rows(child)
    elif isinstance(value, list):
        for child in value:
            yield from all_rows(child)


def apply_evidence(inventories, document):
    if document.get('schema_version') != 1 or not isinstance(document.get('requirements'), dict):
        raise ValueError('Invalid implementation evidence schema')
    records = {item['requirement_id']: item for item in all_rows(inventories)}
    fields = {'status', 'code_locations', 'test_ids', 'evidence'}
    statuses = {'planned', 'implemented-unverified', 'verified', 'provider-unavailable', 'out-of-scope-separate-service'}
    for identifier, changes in sorted(document['requirements'].items()):
        if identifier not in records:
            raise ValueError('Unknown evidence requirement: ' + identifier)
        if not isinstance(changes, dict) or set(changes) - fields:
            raise ValueError('Invalid evidence fields: ' + identifier)
        record = records[identifier]
        status = changes.get('status', record['status'])
        if status not in statuses:
            raise ValueError('Unknown evidence status: ' + identifier)
        if status == 'out-of-scope-separate-service' and record['status'] != status:
            raise ValueError('Evidence cannot expand out-of-scope classification: ' + identifier)
        for field in ['code_locations', 'test_ids']:
            if field in changes and (not isinstance(changes[field], list) or any(not isinstance(value, str) or not value.strip() for value in changes[field])):
                raise ValueError('Invalid ' + field + ': ' + identifier)
        if 'evidence' in changes and not isinstance(changes['evidence'], dict):
            raise ValueError('Invalid evidence: ' + identifier)
        updated = dict(record, **changes)
        updated['evidence'] = dict(record['evidence'], **changes.get('evidence', {}))
        if status in {'implemented-unverified', 'verified', 'provider-unavailable'} and not updated['code_locations']:
            raise ValueError('Implemented status requires code locations: ' + identifier)
        if status in {'verified', 'provider-unavailable'}:
            client = updated['evidence'].get('client')
            if not updated['test_ids'] or not isinstance(client, str) or not client.strip() or client == 'not-run':
                raise ValueError('Verified client status requires tests and client evidence: ' + identifier)
        if status == 'provider-unavailable' and updated['evidence'].get('provider') in [None, '', 'not-run']:
            raise ValueError('Provider unavailable requires a provider blocker: ' + identifier)
        record.update(updated)


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--check', action='store_true', help='Reject drift instead of writing')
    args = parser.parse_args()
    lock = load(DOCS / 'reference-lock.json')
    for name, expected in lock['artifact_sha256'].items():
        actual = hashlib.file_digest((ROOT / name).open('rb'), 'sha256').hexdigest()
        if actual != expected:
            raise SystemExit('Locked reference artifact changed: ' + name)
    inventories = {'mc-commands.json': mc_inventory(), 'aws-s3-commands.json': aws_s3_inventory(), 'aws-s3api-operations.json': aws_api_inventory()}
    apply_evidence(inventories, load(DOCS / 'implementation-evidence.json'))
    outputs = {name: json.dumps(value, indent=2, sort_keys=True, ensure_ascii=False) + '\n' for name, value in inventories.items()}
    lines = ['# CLI compatibility coverage', '', 'Generated by `python3 scripts/cli_compatibility/generate.py`; verify with `--check`.', '', 'This is an exhaustive reference ledger, not a compatibility claim. Client implementation, wire contract, and provider integration evidence remain independently tracked. No generated stub or inventory row counts as implemented.', '', '| Reference | Command/operation count | Requirement rows | Status counts |', '| --- | ---: | ---: | --- |']
    for name, inventory in inventories.items():
        rows = list(all_rows(inventory))
        ids = [r['requirement_id'] for r in rows]
        if len(ids) != len(set(ids)):
            raise SystemExit('Duplicate requirement IDs in ' + name)
        statuses = Counter(r['status'] for r in rows)
        count = len(inventory.get('commands', inventory.get('operations', [])))
        lines.append('| ' + name + ' | ' + str(count) + ' | ' + str(len(rows)) + ' | ' + ', '.join(k + ': ' + str(v) for k, v in sorted(statuses.items())) + ' |')
    api = inventories['aws-s3api-operations.json']
    evidence_rows = list(all_rows(inventories))
    evidence_counts = {kind: sum(record['evidence'].get(kind) not in [None, '', 'not-run'] for record in evidence_rows) for kind in ['client', 'wire', 'provider']}
    lines += ['', '- S3 API shapes: ' + str(len(api['shapes'])) + '; paginators: ' + str(sum(op['paginator'] is not None for op in api['operations'])) + '; waiters: ' + str(len(api['waiters'])) + '.', '- All input/output structure members, list/map variants, enum variants, modeled traits, and endpoint/auth parameters are included. SDK-extra overlays are merged.', '- AWS high-level namespace includes `website`: this pinned version contains nine commands.', '- Explicit evidence annotations: ' + ', '.join(kind + ': ' + str(count) for kind, count in evidence_counts.items()) + ' requirement rows. An annotation describes its stated scope; it is not automatically a passing result.', '- Implementation annotations come from `implementation-evidence.json`. Shared model/grammar support is `implemented-unverified` until the full row contract has independent evidence; no operation inherits verification from a wrapper or member.', '- See `milestone-acceptance-status.md` for acceptance gaps and `mc-native-gap-checklist.md` for exact native grammar/flag differences. Provider evidence remains separate from client implementation.', '- Existing baseline and scoped validation results are recorded in the corresponding decision reports; historical baseline reports are not final integration results.', '- Some nested mc admin help paths return nonzero; the exact exit codes and stderr are retained in `priv/compatibility/mc/help-results.json` and help captures.', '- `out-of-scope-separate-service` applies to identified MinIO administration, optional benchmarking, and binary self-update surfaces. No pinned AWS S3 operation uses this status.', '', 'Every requirement row has an owner milestone and locked-reference evidence. Empty code/test lists mean implementation evidence is pending; they never mean verified. Reference capture artifacts and deterministic generation establish inventory completeness only.', '']
    outputs['coverage.md'] = '\n'.join(lines)
    for name, content in outputs.items():
        path = DOCS / name
        if args.check:
            if not path.exists() or path.read_text() != content:
                raise SystemExit('Generated inventory drift: ' + str(path))
        else:
            path.write_text(content)
    print('Reference hashes and exhaustive inventories verified' if args.check else 'Generated exhaustive compatibility inventories')


if __name__ == '__main__':
    main()
