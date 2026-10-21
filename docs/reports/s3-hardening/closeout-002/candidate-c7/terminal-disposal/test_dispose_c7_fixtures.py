"""Disposable synthetic fixtures only; never inspect or modify real C7 roots."""
import copy
import hashlib
import importlib.util
import io
import json
import os
from pathlib import Path
import stat
import tarfile
import tempfile
import unittest
from unittest import mock

spec = importlib.util.spec_from_file_location('disposal', Path(__file__).with_name('dispose_c7_fixtures.py'))
module = importlib.util.module_from_spec(spec)
spec.loader.exec_module(module)


class DisposalTest(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory(prefix='synthetic-c7-disposal-')
        self.addCleanup(self.temp.cleanup)
        self.runner = Path(self.temp.name) / 'runner'
        self.tmp = self.runner / 'tmp'
        self.tmp.mkdir(parents=True)
        self.archive = self.runner / 'private-archive' / 'metadata.tar'
        self.receipt = self.archive.parent / 'terminal-disposal.json'
        self.rows = []
        for name in sorted(module.ROOT_NAMES):
            root = self.tmp / name
            root.mkdir()
            (root / 'state.json').write_bytes(b'{"synthetic":true}\n')
            (root / 'payload.blob').write_bytes(b'not metadata')
            info = root.stat()
            self.rows.append(dict(physical_root=str(root), device=info.st_dev,
                                  inode=info.st_ino, exists=True, symlink=False))
        self.cleanup = dict(disposable_root_inventory=self.rows)
        for name, value in (('RUNNER', self.runner), ('TMP', self.tmp)):
            patch = mock.patch.object(module, name, value)
            patch.start()
            self.addCleanup(patch.stop)
        self.mounts = mock.patch.object(module, 'mount_points', return_value=[]).start()
        self.addCleanup(mock.patch.stopall)
        self.probes = mock.patch.object(module, 'fresh_probes', return_value={'status': 'passed'}).start()
        self.inputs = mock.patch.object(module, 'load_inputs', return_value=self.cleanup).start()

    def run_disposal(self, cleanup=None):
        return module.dispose(cleanup or self.cleanup, self.archive, self.receipt, execute=True)

    def assert_failed_before_delete(self, result):
        self.assertEqual(result['status'], 'failed')
        self.assertFalse(result['acceptance'])
        self.assertEqual(result['removed_roots'], [])
        self.assertEqual(result['attempted_roots'], [])

    def test_explicit_execute_required_before_outputs_or_probes(self):
        with self.assertRaisesRegex(ValueError, 'explicit --execute'):
            module.dispose(self.cleanup, self.archive, self.receipt)
        self.assertFalse(self.archive.parent.exists())
        self.probes.assert_not_called()

    def test_success_archives_only_metadata_verifies_private_modes_and_exact_roots(self):
        foreign = self.runner / 'unrelated.json'
        foreign.write_bytes(b'outside sentinel')
        root = Path(self.rows[0]['physical_root'])
        (root / 'link.json').symlink_to(foreign)
        (root / 'nested').mkdir()
        (root / 'nested' / 'runtime.log').write_bytes(b'private runtime metadata\n')
        result = self.run_disposal()
        self.assertEqual(result['status'], 'disposed')
        self.assertEqual(len(result['removed_roots']), 14)
        self.assertFalse(result['acceptance'])
        self.assertEqual(result['archive']['files'], 15)
        self.assertEqual(result['archive']['skipped_symlinks'], 1)
        self.assertTrue(foreign.is_file())
        self.assertTrue(all(not Path(r['physical_root']).exists() for r in self.rows))
        self.assertEqual(stat.S_IMODE(self.archive.parent.stat().st_mode), 0o700)
        self.assertEqual(stat.S_IMODE(self.archive.stat().st_mode), 0o600)
        self.assertEqual(stat.S_IMODE(self.receipt.stat().st_mode), 0o600)
        self.assertEqual(result['archive']['sha256'], hashlib.sha256(self.archive.read_bytes()).hexdigest())
        with tarfile.open(self.archive) as archive:
            self.assertTrue(all(m.isreg() and m.mode == 0o600 for m in archive))
            self.assertNotIn(root.name + '/link.json', archive.getnames())
            self.assertFalse(any(name.endswith('.blob') for name in archive.getnames()))
        self.assertEqual(json.loads(self.receipt.read_text()), result)

    def test_wrong_inode_fails_all_roots_before_delete(self):
        self.rows[-1]['inode'] += 1
        self.assert_failed_before_delete(self.run_disposal())
        self.assertTrue(all(Path(r['physical_root']).is_dir() for r in self.rows))
        self.probes.assert_not_called()

    def test_wrong_root_and_old_candidate_root_are_refused(self):
        for path in (self.tmp / 'unrelated', self.runner / 'ess-hardening-crash-closeout002-c7-crashes',
                     self.tmp / 'ess-hardening-crash-closeout002-c6-crashes'):
            data = copy.deepcopy(self.cleanup)
            data['disposable_root_inventory'][0]['physical_root'] = str(path)
            with module.directory(self.tmp) as fd:
                with self.assertRaises(ValueError):
                    module.validate_roots(data, fd, [])

    def test_symlink_root_and_symlink_parent_are_refused(self):
        root = Path(self.rows[0]['physical_root'])
        saved = self.runner / 'saved'
        root.rename(saved)
        root.symlink_to(saved, target_is_directory=True)
        self.assert_failed_before_delete(self.run_disposal())
        self.assertTrue((saved / 'state.json').is_file())
        with self.assertRaises(OSError):
            with module.directory(root):
                self.fail('directory symlink followed')

    def test_mount_at_root_or_nested_mount_blocks_before_delete(self):
        for mount in (Path(self.rows[0]['physical_root']), Path(self.rows[-1]['physical_root']) / 'nested'):
            with module.directory(self.tmp) as fd:
                with self.assertRaisesRegex(ValueError, 'mount'):
                    module.validate_roots(self.cleanup, fd, [mount])
        self.mounts.return_value = [Path(self.rows[0]['physical_root']) / 'newmount']
        self.assert_failed_before_delete(self.run_disposal())

    def test_failed_fresh_probe_preserves_every_root_and_records_failure(self):
        self.probes.side_effect = ValueError('known process still present')
        result = self.run_disposal()
        self.assert_failed_before_delete(result)
        self.assertIn('known process', result['failures'][0])
        self.assertEqual(result['archive']['status'], 'unverified')
        self.assertTrue(self.receipt.is_file())

    def test_archive_cap_failure_never_deletes(self):
        with mock.patch.object(module, 'MAX_FILE', 4):
            result = self.run_disposal()
        self.assert_failed_before_delete(result)
        self.assertIn('cap', result['failures'][0])

    def test_archive_verification_failure_never_deletes(self):
        with mock.patch.object(module, 'verify_archive', side_effect=ValueError('hash mismatch')):
            result = self.run_disposal()
        self.assert_failed_before_delete(result)

    def test_archive_hash_and_size_corruption_are_rejected(self):
        raw = io.BytesIO()
        with tarfile.open(fileobj=raw, mode='w') as archive:
            header = tarfile.TarInfo('fixture/state.json')
            header.size = 4
            archive.addfile(header, io.BytesIO(b'good'))
        for record in ({'name': 'fixture/state.json', 'bytes': 3, 'sha256': hashlib.sha256(b'good').hexdigest()},
                       {'name': 'fixture/state.json', 'bytes': 4, 'sha256': hashlib.sha256(b'evil').hexdigest()}):
            with self.assertRaisesRegex(ValueError, 'mismatch'):
                module.verify_archive(raw, [record])

    def test_changed_frozen_input_after_archive_blocks_all_deletes(self):
        self.inputs.return_value = {'different': True}
        result = self.run_disposal()
        self.assert_failed_before_delete(result)
        self.assertEqual(result['archive']['status'], 'verified')

    def test_mount_appearing_after_archive_blocks_all_deletes(self):
        self.mounts.side_effect = [[], [Path(self.rows[-1]['physical_root']) / 'mount']]
        self.assert_failed_before_delete(self.run_disposal())

    def test_no_overwrite_and_insecure_parent_refused(self):
        self.archive.parent.mkdir(mode=0o700)
        self.archive.write_text('immutable sentinel')
        result = self.run_disposal()
        self.assert_failed_before_delete(result)
        self.assertEqual(self.archive.read_text(), 'immutable sentinel')
        self.archive.unlink()
        self.archive.parent.chmod(0o755)
        self.assert_failed_before_delete(self.run_disposal())

    def test_partial_deletion_failure_records_actual_outcomes_without_retry(self):
        original = module.shutil.rmtree
        calls = []
        def remove(name, *, dir_fd):
            calls.append(name)
            if len(calls) == 2:
                raise OSError('synthetic deletion failure')
            return original(name, dir_fd=dir_fd)
        remove.avoids_symlink_attacks = True
        with mock.patch.object(module.shutil, 'rmtree', remove):
            result = self.run_disposal()
        self.assertEqual(result['status'], 'failed')
        self.assertEqual(len(result['removed_roots']), 1)
        self.assertEqual(len(result['attempted_roots']), 2)
        self.assertEqual(len(calls), 2)
        self.assertFalse(Path(self.rows[0]['physical_root']).exists())
        self.assertTrue(Path(self.rows[1]['physical_root']).is_dir())
        self.assertEqual(json.loads(self.receipt.read_text())['removed_roots'], result['removed_roots'])


class FreshProbeTest(unittest.TestCase):
    def test_reused_pid_is_allowed_but_original_identity_or_zombie_blocks(self):
        cleanup = dict(verifier_pid_namespace='pid:[synthetic]',
            process_identity_checks=[dict(expected=dict(pid=10, start_ticks=50),
                original_identity_absent=True, original_identity_running=False, zombie=False)] * 20,
            epmd_query_exit_status=0, matching_known_erlang_names=[], exact_known_erlang_names={'ess_data_a': []},
            exact_known_listener_ports={'9001': {'listeners': 0}},
            fixture_container_namespaces=[dict(fixture_name_pattern=p, absent=True,
                query_exit_status=0, matching_names=[]) for p in module.CONTAINER_PATTERNS])
        with mock.patch.object(module.os, 'readlink', return_value='pid:[synthetic]'), \
             mock.patch.object(module, 'query', return_value=''), \
             mock.patch.object(module, 'read_regular', return_value=b'header\n'), \
             mock.patch.object(module, 'pid_identity', return_value={'start_ticks': 51}) as identity:
            self.assertEqual(module.fresh_probes(cleanup)['status'], 'passed')
            for state in ('R', 'Z'):
                identity.return_value = {'start_ticks': 50, 'state': state}
                with self.assertRaisesRegex(ValueError, 'identity remains'):
                    module.fresh_probes(cleanup)

    def test_proc_mount_probe_uses_numeric_pid_and_decodes_escaped_paths(self):
        with mock.patch.object(module.os, 'getpid', return_value=42), \
             mock.patch.object(module, 'read_regular', return_value=b'1 2 0:1 / /tmp/a\\040b rw - tmpfs tmpfs rw\n') as read:
            self.assertEqual(module.mount_points(), [Path('/tmp/a b')])
            self.assertEqual(read.call_args.args[0], '/proc/42/mountinfo')


if __name__ == '__main__':
    unittest.main(verbosity=2)
