"""Independent checks for the frozen discovery boundary, not CLI parity."""
import json
from pathlib import Path
import re
import subprocess
import sys
import unittest
import generate

ROOT = Path(__file__).resolve().parents[2]
DOCS = ROOT / 'docs/cli/compatibility'
ASSETS = ROOT / 'apps/ex_storage_service_cli/priv/compatibility'


class InventoryTest(unittest.TestCase):
    def read(self, path):
        return json.loads(path.read_text())

    def test_inventory_is_deterministic_and_all_locked_assets_match(self):
        subprocess.run([sys.executable, str(ROOT / 'scripts/cli_compatibility/generate.py'), '--check'], check=True, capture_output=True)

    def test_no_registered_or_positional_mc_commands_are_omitted(self):
        original = self.read(ASSETS / 'mc/command-tree.json')['commands']
        variants = self.read(ASSETS / 'mc/action-variants.json')
        inventory = self.read(DOCS / 'mc-commands.json')['commands']
        self.assertEqual(301, len(original))
        self.assertEqual(6, len(variants))
        self.assertEqual({tuple(item['path']) for item in original + variants}, {tuple(item['path']) for item in inventory})
        self.assertIn(('anonymous', 'links'), {tuple(item['path']) for item in inventory})
        anonymous_set = next(item for item in inventory if item['path'] == ['anonymous', 'set'])
        self.assertEqual({'none', 'private', 'public', 'download', 'upload'}, {item['value'] for item in anonymous_set['argument_variants']})
        for command in original:
            captured = next(item for item in inventory if item['path'] == command['path'])
            self.assertEqual([item['definition'] for item in command['flags']], [item['definition'] for item in captured['flags']])

    def test_all_high_level_commands_include_website_and_sync_customizations(self):
        inventory = self.read(DOCS / 'aws-s3-commands.json')['commands']
        self.assertEqual({'ls', 'cp', 'mv', 'rm', 'sync', 'mb', 'rb', 'presign', 'website'}, {item['command'] for item in inventory})
        sync = next(item for item in inventory if item['command'] == 'sync')
        self.assertTrue({'size-only', 'exact-timestamps', 'delete'} <= {item['definition']['name'] for item in sync['arguments']})
        for command in inventory:
            self.assertEqual([], command['source_flags_not_in_help'])

    def test_mc_executable_help_flags_have_inventory_rows(self):
        inventory = self.read(DOCS / 'mc-commands.json')
        def captured_names(path):
            names = set()
            for line in path.read_text().splitlines():
                match = re.match(r'^\s+(--?[\w-]+(?: value)?(?:, --?[\w-]+(?: value)?)*)(?=\s{2,})', line)
                if match:
                    names.update(re.findall(r'--?([\w-]+)', match[1]))
            return names
        def inventory_names(flags):
            return {name for flag in flags for name in flag['names']}
        global_names = inventory_names(inventory['global_flags'])
        visible_global_names = inventory_names([flag for flag in inventory['global_flags'] if not flag['hidden']])
        self.assertEqual(visible_global_names, captured_names(ASSETS / 'mc/help/root.txt'))
        self.assertTrue({'help', 'h', 'version', 'v'} <= global_names)
        for command in inventory['commands']:
            actual = captured_names(ROOT / command['reference'])
            declared = inventory_names(command['flags'])
            with self.subTest(path=command['path']):
                self.assertFalse(actual - declared - global_names)
                if 'help' in actual:
                    help_flag = next(item for item in command['flags'] if 'help' in item['names'])
                    self.assertEqual(['help', 'h'], help_flag['names'])
                    self.assertFalse(help_flag['default'])
                    self.assertEqual('framework implicit', help_flag['definition_origin'])

    def test_complete_apache_license_and_botocore_notice_are_preserved(self):
        license = (ASSETS / 'aws/LICENSE-APACHE-2.0.txt').read_text()
        self.assertIn('1. Definitions.', license)
        self.assertIn('9. Accepting Warranty or Additional Liability.', license)
        self.assertIn('APPENDIX: How to apply the Apache License to your work.', license)
        self.assertTrue(license.rstrip().endswith('limitations under the License.'))
        notice = (ASSETS / 'aws/NOTICE-BOTOCORE.txt').read_text()
        self.assertIn('Copyright 2012-2017 Amazon.com, Inc. or its affiliates.', notice)
        self.assertIn('Requests License', notice)

    def test_evidence_overlay_preserves_independent_results_and_rejects_false_verification(self):
        def inventory():
            return {'commands': [generate.row('MC:cp', 'M4', 'locked-reference')]}
        updates = {'schema_version': 1, 'requirements': {'MC:cp': {
            'status': 'verified', 'code_locations': ['transfer.ex'], 'test_ids': ['copy fault fixture'],
            'evidence': {'client': 'scoped fixtures passed; source SHA and command recorded', 'wire': 'reference fixture passed'}
        }}}
        data = inventory()
        generate.apply_evidence(data, updates)
        first = json.dumps(data, sort_keys=True)
        generate.apply_evidence(data, updates)
        self.assertEqual(first, json.dumps(data, sort_keys=True))
        self.assertEqual('verified', data['commands'][0]['status'])
        self.assertEqual('not-run', data['commands'][0]['evidence']['provider'])
        self.assertEqual('locked reference; generated offline', data['commands'][0]['evidence']['inventory'])
        cases = [
            {'MC:missing': {}},
            {'MC:cp': {'status': 'verified'}},
            {'MC:cp': {'status': 'provider-unavailable'}},
            {'MC:cp': {'status': 'silently-supported'}},
            {'MC:cp': {'status': 'out-of-scope-separate-service'}},
            {'MC:cp': {'reference': 'unlocked-reference'}},
        ]
        for requirements in cases:
            with self.subTest(requirements=requirements), self.assertRaises(ValueError):
                generate.apply_evidence(inventory(), {'schema_version': 1, 'requirements': requirements})

    def test_all_service_operations_and_nested_members_are_present(self):
        model = self.read(ASSETS / 'aws/service-2.json')
        inventory = self.read(DOCS / 'aws-s3api-operations.json')
        self.assertEqual(108, len(model['operations']))
        self.assertEqual(set(model['operations']), {item['operation'] for item in inventory['operations']})
        self.assertEqual(670, len(inventory['shapes']))
        def check_shape(shape_name, records, visited):
            if shape_name in visited:
                self.assertEqual(1, len(records))
                self.assertTrue(records[0]['recursive'])
                return
            shape = model['shapes'][shape_name]
            members = dict(shape.get('members', {}))
            if 'member' in shape:
                members['[]'] = shape['member']
            if 'key' in shape:
                members.update({'[key]': shape['key'], '[value]': shape['value']})
            self.assertEqual(set(members), {item['member'] for item in records})
            for item in records:
                child = model['shapes'][members[item['member']]['shape']]
                self.assertEqual(set(child.get('enum', [])), {entry['value'] for entry in item['enum_values']})
                check_shape(item['shape'], item['children'], visited | {shape_name})
        evidence = self.read(DOCS / 'implementation-evidence.json')['requirements']
        for operation in inventory['operations']:
            expected = evidence.get(operation['requirement_id'], {}).get('status', 'planned')
            self.assertEqual(expected, operation['status'])
            for direction in ['input', 'output']:
                if operation[direction + '_shape']:
                    check_shape(operation[direction + '_shape'], operation[direction + '_members'], set())

    def test_model_linkage_does_not_claim_full_operation_or_member_verification(self):
        inventory = self.read(DOCS / 'aws-s3api-operations.json')
        for operation in inventory['operations']:
            if 'not this entire operation' in operation['evidence'].get('client', ''):
                self.assertEqual('implemented-unverified', operation['status'])
            for direction in ['input', 'output']:
                for member in generate.all_rows(operation[direction + '_members']):
                    if not member['test_ids']:
                        self.assertNotIn(member['status'], ['verified', 'provider-unavailable'])
                    if 'not independent evidence for this member' in member['evidence'].get('client', ''):
                        self.assertEqual('implemented-unverified', member['status'])
                        self.assertEqual([], member['test_ids'])
                        self.assertEqual('not-run', member['evidence']['wire'])
                        self.assertEqual('not-run', member['evidence']['provider'])

    def test_implementation_evidence_links_existing_code_and_test_files(self):
        evidence = self.read(DOCS / 'implementation-evidence.json')['requirements']
        for identifier, record in evidence.items():
            for path in record.get('code_locations', []) + record.get('test_ids', []):
                with self.subTest(identifier=identifier, path=path):
                    self.assertTrue((ROOT / path.split('::', 1)[0]).is_file())

    def test_stream_and_advanced_operations_cannot_disappear(self):
        inventory = self.read(DOCS / 'aws-s3api-operations.json')
        operations = {item['operation']: item for item in inventory['operations']}
        for name in ['GetObject', 'GetObjectTorrent', 'SelectObjectContent']:
            self.assertIn('outfile', {item['name'] for item in operations[name]['cli_arguments']})
            self.assertNotIn('cli-input-json', {item['name'] for item in operations[name]['cli_arguments']})
        self.assertTrue({'cli-input-json', 'cli-input-yaml', 'generate-cli-skeleton'} <= {item['name'] for item in operations['PutBucketPolicy']['cli_arguments']})
        self.assertTrue((ASSETS / 'aws/skeletons/put-bucket-policy.json').is_file())
        for name in ['CreateSession', 'RenameObject', 'SelectObjectContent', 'WriteGetObjectResponse']:
            self.assertTrue(operations[name]['special_handler_required'])
        self.assertEqual({'sigv4', 'sigv4a', 'sigv4-s3express'}, {item['definition']['name'] for item in inventory['endpoint_auth_variants']})
        self.assertEqual(4, len(inventory['waiters']))
        self.assertEqual(7, sum(item['paginator'] is not None for item in inventory['operations']))


if __name__ == '__main__':
    unittest.main()
