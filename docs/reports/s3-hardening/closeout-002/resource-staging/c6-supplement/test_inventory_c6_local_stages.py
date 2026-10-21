#!/usr/bin/env python3
"""Synthetic-only tests: never execute against retained C6 fixtures."""
import ast
import hashlib
import os
from pathlib import Path
import tempfile
import types
import unittest
from unittest import mock

UTILITY = Path('/fast/build/ess-closeout-c6-acceptance/inventory_c6_local_stages.py')
module = types.ModuleType('inventory_c6_local_stages_under_test')
exec(compile(UTILITY.read_text(), str(UTILITY), 'exec'), module.__dict__)


class InventoryTests(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory(prefix='ess-inventory-synthetic-')
        self.addCleanup(self.temp.cleanup)
        self.root = Path(self.temp.name)

    def scan(self, choose=lambda name: True, recursive=True):
        fd = module.anchored_directory(self.root)
        try:
            return module.inventory(fd, choose, recursive)
        finally:
            os.close(fd)

    def test_zero_nonzero_and_hidden_tmp_metadata(self):
        (self.root / 'zero').touch()
        (self.root / '.hidden').write_bytes(b'abc')
        (self.root / 'nested').mkdir()
        (self.root / 'nested' / '.zero').touch()
        result = self.scan()
        self.assertEqual((result['files'], result['bytes'], result['zero_byte_files']), (3, 3, 2))
        self.assertEqual([x['relative_path'] for x in result['files_metadata']],
                         ['.hidden', 'nested/.zero', 'zero'])

    def test_cache_tmp_suffix_excludes_published_objects(self):
        (self.root / 'prefix').mkdir()
        (self.root / 'prefix' / 'published').write_bytes(b'published')
        (self.root / 'prefix' / '.hidden.tmp').touch()
        (self.root / 'prefix' / 'object.random.tmp').write_bytes(b'xx')
        result = self.scan(lambda name: name.endswith('.tmp'))
        self.assertEqual((result['files'], result['bytes']), (2, 2))

    def test_inline_prefix_is_direct_only(self):
        (self.root / '.cache-stage-zero').touch()
        (self.root / 'published').write_bytes(b'ignore')
        (self.root / 'nested').mkdir()
        (self.root / 'nested' / '.cache-stage-out-of-scope').touch()
        result = self.scan(lambda name: name.startswith('.cache-stage-'), False)
        self.assertEqual((result['files'], result['bytes']), (1, 0))

    def test_symlink_in_recursive_scope_refused(self):
        (self.root / 'link').symlink_to('/etc/passwd')
        with self.assertRaisesRegex(RuntimeError, '^inventory_symlink_refused$'):
            self.scan()

    def test_selected_inline_symlink_refused(self):
        (self.root / '.cache-stage-link').symlink_to('/etc/passwd')
        with self.assertRaisesRegex(RuntimeError, '^inventory_symlink_refused$'):
            self.scan(lambda name: name.startswith('.cache-stage-'), False)

    def test_ancestor_symlink_refused(self):
        (self.root / 'real').mkdir()
        (self.root / 'link').symlink_to(self.root / 'real', target_is_directory=True)
        with self.assertRaises(OSError):
            module.anchored_directory(self.root / 'link')

    def test_missing_directory_is_explicit(self):
        fd = module.anchored_directory(self.root)
        try:
            with self.assertRaises(FileNotFoundError):
                module.child_directory(fd, ('missing',))
        finally:
            os.close(fd)

    def test_entry_cap_refused(self):
        for name in ['a', 'b', 'c']:
            (self.root / name).touch()
        with mock.patch.object(module, 'MAX_ENTRIES', 2):
            with self.assertRaisesRegex(RuntimeError, '^inventory_entry_bound$'):
                self.scan()

    def test_depth_cap_refused(self):
        (self.root / 'a' / 'b').mkdir(parents=True)
        with mock.patch.object(module, 'MAX_DEPTH', 1):
            with self.assertRaisesRegex(RuntimeError, '^inventory_depth_bound$'):
                self.scan()

    def test_nonregular_entry_refused(self):
        os.mkfifo(self.root / 'pipe')
        with self.assertRaisesRegex(RuntimeError, '^inventory_nonregular_entry_refused$'):
            self.scan()

    def test_regular_fixture_files_never_opened(self):
        (self.root / 'nested').mkdir()
        (self.root / 'nested' / 'secret').write_bytes(b'not read')
        real_open = os.open
        calls = []
        def directory_only(path, flags, *args, **kwargs):
            self.assertTrue(flags & os.O_DIRECTORY)
            self.assertTrue(flags & os.O_NOFOLLOW)
            calls.append(str(path))
            return real_open(path, flags, *args, **kwargs)
        with mock.patch.object(module.os, 'open', side_effect=directory_only):
            result = self.scan()
        self.assertEqual(result['bytes'], 8)
        self.assertNotIn('secret', calls)

    def test_directory_identity_change_refused(self):
        (self.root / 'child').mkdir()
        real_fstat = os.fstat
        count = 0
        def changed_child(fd):
            nonlocal count
            info = real_fstat(fd)
            count += 1
            if count == 2:
                return types.SimpleNamespace(st_dev=info.st_dev, st_ino=info.st_ino + 1)
            return info
        with mock.patch.object(module.os, 'fstat', side_effect=changed_child):
            with self.assertRaisesRegex(RuntimeError, '^inventory_directory_identity_changed$'):
                self.scan()

    def test_directory_mutation_refused(self):
        real_fstat = os.fstat
        count = 0
        def changed_after(fd):
            nonlocal count
            info = real_fstat(fd)
            count += 1
            if count == 2:
                return types.SimpleNamespace(st_dev=info.st_dev, st_ino=info.st_ino,
                    st_mtime_ns=info.st_mtime_ns + 1, st_ctime_ns=info.st_ctime_ns)
            return info
        with mock.patch.object(module.os, 'fstat', side_effect=changed_after):
            with self.assertRaisesRegex(RuntimeError, '^inventory_directory_changed_during_observation$'):
                self.scan()

    def test_git_source_uses_candidate_not_changed_worktree(self):
        relative = 'e2e/source.py'
        (self.root / 'e2e').mkdir()
        (self.root / relative).write_bytes(b'changed worktree')
        pinned = b'immutable candidate source'
        with mock.patch.object(module, 'REPO', self.root), \
             mock.patch.object(module, 'PINS', {relative: hashlib.sha256(pinned).hexdigest()}), \
             mock.patch.object(module, 'SOURCE_FROM_GIT', {relative}), \
             mock.patch.object(module.subprocess, 'check_output', return_value=pinned) as git:
            self.assertEqual(module.read_pinned(relative), pinned)
            git.assert_called_once_with(['git', 'show', f'{module.CANDIDATE}:{relative}'],
                cwd=self.root, stderr=module.subprocess.DEVNULL)
            with self.assertRaisesRegex(RuntimeError, '^source_is_not_a_receipt$'):
                module.read_receipt(relative)

    def test_wrong_pin_refused(self):
        relative = 'e2e/source.py'
        with mock.patch.object(module, 'PINS', {relative: '0' * 64}), \
             mock.patch.object(module, 'SOURCE_FROM_GIT', {relative}), \
             mock.patch.object(module.subprocess, 'check_output', return_value=b'wrong source'):
            with self.assertRaisesRegex(RuntimeError, '^provenance_hash_changed$'):
                module.read_pinned(relative)

    def execute_synthetic(self, change_inode=False):
        info = self.root.stat()
        cleanup = {'status': 'passed', 'exit_status': 0,
            'verifier_pid_namespace': os.readlink('/proc/self/ns/pid'),
            'process_identity_checks': [
                {'source': module.VIRTUAL + '/observer-synthetic.json', 'role': role,
                 'expected': {'pid': pid, 'start_ticks': 1}}
                for role, pid in [('beam', 900000001), ('observer', 900000002)]],
            'disposable_root_inventory': [{'virtual_root': module.VIRTUAL,
                'physical_root': str(self.root), 'exists': True, 'symlink': False,
                'device': info.st_dev, 'inode': info.st_ino + int(change_inode)}]}
        lane = {'status': 'passed', 'commands': [{'exit_status': 0}],
                'starting_sha': module.CANDIDATE, 'ending_sha': module.CANDIDATE,
                'source_sha256': module.FINGERPRINT}
        def receipt(path):
            if path.endswith('/resources.json'): return lane
            if path.endswith('/terminal-cleanup.json'): return cleanup
            return {'exit_status': 1, 'ended_at': 'synthetic-terminal'}
        with mock.patch.object(module, 'PHYSICAL', self.root), \
             mock.patch.object(module, 'PINS', {}), \
             mock.patch.object(module, 'read_receipt', side_effect=receipt), \
             mock.patch.object(module, 'pid_identity', return_value=None):
            return module.execute()

    def test_execute_only_synthetic_missing_scopes(self):
        result = self.execute_synthetic()
        self.assertEqual(result['terminal_stage_files'], 0)
        self.assertEqual(len(result['inventories']), 3)
        self.assertTrue(all(x['state'] == 'absent_at_observation' for x in result['inventories']))

    def test_retained_root_identity_change_refused(self):
        with self.assertRaisesRegex(RuntimeError, '^retained_root_identity_changed$'):
            self.execute_synthetic(change_inode=True)

    def test_execute_detects_zero_byte_hidden_stage(self):
        (self.root / 'data' / 'tmp').mkdir(parents=True)
        (self.root / 'data' / 'tmp' / '.hidden-zero').touch()
        with self.assertRaisesRegex(RuntimeError, '^terminal_stage_files_present$'):
            self.execute_synthetic()

    def test_ast_and_no_default_execution(self):
        ast.parse(UTILITY.read_text())
        with mock.patch.object(module, 'execute', side_effect=AssertionError('must not execute')), \
             mock.patch('sys.argv', [str(UTILITY)]), mock.patch('builtins.print'):
            self.assertEqual(module.main(), 1)


if __name__ == '__main__':
    unittest.main(verbosity=2)
