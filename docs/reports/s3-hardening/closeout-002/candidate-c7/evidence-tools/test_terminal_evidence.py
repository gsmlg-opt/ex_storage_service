"""Synthetic only; never execute real selector/cleanup or any service command."""
import argparse
import contextlib
import copy
import datetime
import importlib.util
import io
import json
import os
from pathlib import Path
import runpy
import sys
import tempfile
import types
import unittest
from unittest import mock

HERE = Path(__file__).resolve().parent
REPO = Path('/home/gao/Workspace/gsmlg-opt/ex_storage_service')
SOURCE_API = runpy.run_path(str(REPO / 'e2e/hardening_acceptance.py'))
FREEZE = json.loads((REPO / 'docs/reports/s3-hardening/closeout-002/candidate-c7/freeze.json').read_text())
LEGACY = '--legacy-selector' in sys.argv
if LEGACY:
    sys.argv.remove('--legacy-selector')
else:
    import terminal_evidence as helper


class Fixtures(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory(prefix='ess-terminal-synthetic-')
        self.addCleanup(self.temp.cleanup)
        self.repo = Path(self.temp.name) / 'repo'
        self.base = self.repo / 'docs/reports/s3-hardening/closeout-002/candidate-c7'
        self.base.mkdir(parents=True)
        self.freeze = copy.deepcopy(FREEZE)
        self.api = dict(SOURCE_API, REPO=self.repo, sha=lambda: self.freeze['candidate_sha'],
                        source_fingerprint=lambda: self.freeze['source_sha256'])
        self.args = argparse.Namespace(python='/tmp/ess-s3-hardening-python/bin/python',
            mc='/tmp/ess-s3-hardening-tools/mc', label='closeout002-c7', output=self.base, timeout=10800)
        self.started = (datetime.datetime.fromisoformat(self.freeze['frozen_at']) + datetime.timedelta(seconds=1)).isoformat()
        self.ended = (datetime.datetime.fromisoformat(self.started) + datetime.timedelta(seconds=1)).isoformat()
        self.invocation = dict(candidate_sha=self.freeze['candidate_sha'], source_sha256=self.freeze['source_sha256'],
            command=self.freeze['namespace_command'] + ['--'] + self.freeze['authoritative_command'],
            started_at=self.started, ended_at=self.ended, status='failed', exit_status=1, child_exit_status=1,
            ending_sha=self.freeze['candidate_sha'], ending_source_sha256=self.freeze['source_sha256'],
            duration_seconds=1.0, freeze_failures=[], resource_envelope_unchanged=True, runner_inputs_unchanged=True)
        self.write('freeze.json', self.freeze)
        self.write('invocation.json', self.invocation)
        self.lanes = {}
        for profile in SOURCE_API['PROFILES']:
            steps = list(SOURCE_API['commands'](profile, self.args, self.args.label + '-' + profile))
            if profile not in ('correctness', 'release'):
                steps.insert(0, ('prod', ['mix', 'compile', '--force', '--warnings-as-errors']))
            commands = []
            for i, (mode, argv) in enumerate(steps):
                log = self.base / f'{profile}-{i}.log'
                log.write_text('synthetic terminal log\n')
                commands.append(dict(argv=argv, mix_env=mode, exit_status=0, duration_seconds=0.01,
                                     log=str(log.relative_to(self.repo))))
            lane = dict(profile=profile, status='passed', starting_sha=self.freeze['candidate_sha'],
                ending_sha=self.freeze['candidate_sha'], source_sha256=self.freeze['source_sha256'],
                commands=commands, started_at=self.started)
            self.lanes[profile] = lane
            self.write(profile + '.json', lane)

    def write(self, name, value):
        (self.base / name).write_text(json.dumps(value))

    def selector(self):
        name = 'select_final_lanes.py.pre-terminal-hardening-private' if LEGACY else 'select_final_lanes.py'
        namespace = runpy.run_path(str(HERE / name))
        function = namespace['main']
        function.__globals__.update(REPO=self.repo, BASE=self.base, OUT=self.base / 'final-lanes',
                                   API=self.api, FREEZE=self.freeze)
        with contextlib.redirect_stdout(io.StringIO()):
            function()

    def test_selector_rejects_passed_with_failed_child(self):
        lane = self.lanes['gateway']
        lane['commands'][-1]['exit_status'] = 1
        self.write('gateway.json', lane)
        with self.assertRaises((RuntimeError, ValueError)):
            self.selector()

    def test_selector_rejects_passed_compile_only(self):
        lane = self.lanes['gateway']
        lane['commands'] = lane['commands'][:1]
        self.write('gateway.json', lane)
        with self.assertRaises((RuntimeError, ValueError)):
            self.selector()

    @unittest.skipIf(LEGACY, 'new shared validator only')
    def test_legitimate_blocked_independent_index_and_resources(self):
        for profile in ('index', 'resources'):
            lane = copy.deepcopy(self.lanes[profile])
            lane.update(status='blocked', reason='command exited 1')
            lane['commands'][1]['exit_status'] = 1
            summary = helper.validate_lane(lane, profile, self.freeze, self.api, self.base)
            self.assertTrue(summary['complete'])
            self.assertIn(2, summary['successful_command_indices'])
            self.assertEqual(summary['failed_command_indices'], [1])

    @unittest.skipIf(LEGACY, 'new shared validator only')
    def test_blocked_compile_failure_prefix_allowed(self):
        lane = copy.deepcopy(self.lanes['gateway'])
        lane.update(status='blocked', reason='compile failed')
        lane['commands'] = lane['commands'][:1]
        lane['commands'][0]['exit_status'] = 1
        summary = helper.validate_lane(lane, 'gateway', self.freeze, self.api, self.base)
        self.assertFalse(summary['complete'])
        self.assertEqual(summary['unexecuted_command_indices'], [1])

    @unittest.skipIf(LEGACY, 'new shared validator only')
    def test_blocked_exception_after_successful_prefix_allowed(self):
        lane = copy.deepcopy(self.lanes['gateway'])
        lane.update(status='blocked', reason='subprocess launch failed')
        lane['commands'] = lane['commands'][:1]
        self.assertFalse(helper.validate_lane(lane, 'gateway', self.freeze, self.api, self.base)['complete'])

    @unittest.skipIf(LEGACY, 'new shared validator only')
    def test_non_independent_continuation_after_failure_rejected(self):
        lane = copy.deepcopy(self.lanes['standalone'])
        lane.update(status='blocked', reason='failed child')
        lane['commands'][1]['exit_status'] = 1
        with self.assertRaises(helper.TerminalEvidenceError):
            helper.validate_lane(lane, 'standalone', self.freeze, self.api, self.base)

    @unittest.skipIf(LEGACY, 'new shared validator only')
    def test_any_profile_compile_failure_cannot_continue(self):
        lane = copy.deepcopy(self.lanes['resources'])
        lane.update(status='blocked', reason='failed compile')
        lane['commands'][0]['exit_status'] = 1
        with self.assertRaises(helper.TerminalEvidenceError):
            helper.validate_lane(lane, 'resources', self.freeze, self.api, self.base)

    @unittest.skipIf(LEGACY, 'new shared validator only')
    def test_invocation_valid_failed_acceptance_is_terminal(self):
        helper.validate_invocation(self.invocation, self.freeze)

    @unittest.skipIf(LEGACY, 'new shared validator only')
    def test_invocation_rejects_null_bool_unfinished_stale(self):
        for field, value in [('exit_status', None), ('exit_status', True), ('ended_at', None),
                             ('status', 'in_progress'), ('candidate_sha', 'a' * 40),
                             ('ending_source_sha256', 'b' * 64), ('child_exit_status', None),
                             ('runner_inputs_unchanged', False)]:
            with self.subTest(field=field, value=value):
                record = copy.deepcopy(self.invocation)
                record[field] = value
                with self.assertRaises(helper.TerminalEvidenceError):
                    helper.validate_invocation(record, self.freeze)

    @unittest.skipIf(LEGACY, 'new shared validator only')
    def test_lane_rejects_profile_source_environment_argv_log_exit(self):
        changes = [('profile', 'cluster'), ('starting_sha', 'a' * 40),
                   ('ending_sha', 'a' * 40), ('source_sha256', 'b' * 64), ('status', 'unknown')]
        for field, value in changes:
            with self.subTest(field=field):
                lane = copy.deepcopy(self.lanes['gateway']);lane[field] = value
                with self.assertRaises(helper.TerminalEvidenceError):
                    helper.validate_lane(lane, 'gateway', self.freeze, self.api, self.base)
        for field, value in [('exit_status', True), ('mix_env', 'test'), ('argv', ['true']),
                             ('duration_seconds', float('nan')), ('log', 'outside.log')]:
            with self.subTest(command_field=field):
                lane = copy.deepcopy(self.lanes['gateway']);lane['commands'][0][field] = value
                with self.assertRaises(helper.TerminalEvidenceError):
                    helper.validate_lane(lane, 'gateway', self.freeze, self.api, self.base)

    @unittest.skipIf(LEGACY, 'new shared validator only')
    def test_missing_lane_and_symlink_receipt_rejected(self):
        path = self.base / 'gateway.json';raw = path.read_bytes();path.unlink()
        with self.assertRaises(helper.TerminalEvidenceError):
            helper.validate_all_lanes(self.base, self.freeze, self.api)
        outside = Path(self.temp.name) / 'receipt.json';outside.write_bytes(raw);path.symlink_to(outside)
        with self.assertRaises(helper.TerminalEvidenceError):
            helper.validate_all_lanes(self.base, self.freeze, self.api)

    @unittest.skipIf(LEGACY, 'new shared validator only')
    def test_safe_reads_refuse_traversal_symlinks_private_and_nonregular(self):
        scope = Path(self.temp.name) / 'scope';scope.mkdir()
        outside = Path(self.temp.name) / 'outside';outside.mkdir()
        (outside / 'target.log').write_text('outside')
        (scope / 'file-link.log').symlink_to(outside / 'target.log')
        (scope / 'parent-link').symlink_to(outside, target_is_directory=True)
        (scope / 'credentials.json').write_text('{}')
        (scope / 'samples.jsonl.gz').write_bytes(b'not opened')
        os.mkfifo(scope / 'fifo.log')
        bad = [scope / 'file-link.log', scope / 'parent-link' / 'target.log',
               scope / '..' / 'outside' / 'target.log', scope / 'credentials.json',
               scope / 'samples.jsonl.gz', scope / 'fifo.log']
        for path in bad:
            with self.subTest(path=str(path)):
                with self.assertRaises(helper.TerminalEvidenceError):
                    helper.bounded_read(path, [scope])

    @unittest.skipIf(LEGACY, 'new shared validator only')
    def test_safe_read_limits_actual_bytes_not_stat_only(self):
        path = self.base / 'bounded.log';path.write_bytes(b'12345')
        with self.assertRaises(helper.TerminalEvidenceError):
            helper.bounded_read(path, [self.base], max_bytes=4)
        self.assertEqual(helper.bounded_read(path, [self.base], max_bytes=5), b'12345')

    @unittest.skipIf(LEGACY, 'new shared validator only')
    def test_read_growth_is_bounded_even_when_initial_stat_underreports(self):
        path = self.base / 'growth.log';path.write_bytes(b'12345')
        real_fstat = helper.os.fstat
        calls = 0
        def stale_initial_size(descriptor):
            nonlocal calls
            info = real_fstat(descriptor);calls += 1
            if calls == 1:
                names = ('st_mode', 'st_dev', 'st_ino', 'st_mtime_ns', 'st_ctime_ns')
                return types.SimpleNamespace(st_size=0, **{name:getattr(info,name) for name in names})
            return info
        with mock.patch.object(helper.os, 'fstat', side_effect=stale_initial_size):
            with self.assertRaisesRegex(helper.TerminalEvidenceError, 'actual metadata bytes exceed read bound'):
                helper.bounded_read(path, [self.base], max_bytes=4)

    @unittest.skipIf(LEGACY, 'new shared validator only')
    def test_directory_boundary_refuses_symlink_and_allows_real_directory(self):
        target = Path(self.temp.name) / 'directory';target.mkdir()
        link = self.base / 'directory-link';link.symlink_to(target, target_is_directory=True)
        self.assertEqual(helper.safe_directory_path(self.base, [self.repo]), self.base)
        with self.assertRaises(helper.TerminalEvidenceError):
            helper.safe_directory_path(link, [self.base])

    @unittest.skipIf(LEGACY, 'new shared validator only')
    def test_selector_keeps_valid_blocked_receipt_byte_identical(self):
        lane = self.lanes['index'];lane.update(status='blocked', reason='A28 child failed')
        lane['commands'][1]['exit_status'] = 1;self.write('index.json', lane)
        before = (self.base / 'index.json').read_bytes()
        self.selector()
        self.assertEqual((self.base / 'final-lanes/index.json').read_bytes(), before)
        provenance = json.loads((self.base / 'final-lanes/provenance.json').read_text())
        selected = next(x for x in provenance['selections'] if x['profile'] == 'index')
        self.assertEqual(selected['selected_status'], 'blocked')
        self.assertEqual(selected['actual_command_exit_statuses'], [0, 1, 0])


if __name__ == '__main__':
    unittest.main(verbosity=2)
