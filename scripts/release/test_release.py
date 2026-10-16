"""Release acceptance helpers operate only on disposable local Git fixtures."""

import io
import json
from pathlib import Path
import subprocess
import tarfile
import tempfile
import unittest
from unittest.mock import patch

import core_package
import release


class ReleaseTests(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.addCleanup(self.temp.cleanup)
        self.root = Path(self.temp.name) / 'repo'
        self.root.mkdir()
        self.remote = Path(self.temp.name) / 'origin.git'
        self.run_git('init', '--bare', str(self.remote))
        self.run_git('init', '-b', 'main')
        self.run_git('config', 'user.name', 'Release Test')
        self.run_git('config', 'user.email', 'release@example.invalid')
        for name in release.VERSION_FILES:
            path = self.root / name
            path.parent.mkdir(parents=True, exist_ok=True)
            path.write_text('@version "0.6.7"\n' if '_cli/' in name else 'version: "0.6.7"\n')
        self.run_git('add', '.')
        self.run_git('commit', '-m', 'fixture')
        self.run_git('remote', 'add', 'origin', str(self.remote))
        self.run_git('push', '-u', 'origin', 'main')
        self.source = self.run_git('rev-parse', 'HEAD')
        self.prepared = Path(self.temp.name) / 'prepared.json'
        self.output = Path(self.temp.name) / 'result.json'
        self.addCleanup(patch.stopall)
        patch.object(release, 'ROOT', self.root).start()
        patch.dict(release.os.environ, {'GITHUB_OUTPUT': ''}).start()

    def run_git(self, *args):
        return subprocess.run(['git', *args], cwd=self.root, check=True,
                              capture_output=True, text=True).stdout.strip()

    def test_synchronized_local_commit_then_atomic_explicit_publication(self):
        release.prepare('0.6.8', self.prepared)
        state = json.loads(self.prepared.read_text())
        self.assertEqual(state['source_sha'], self.source)
        self.assertNotEqual(state['release_sha'], self.source)
        self.assertEqual(set(release.versions(self.root).values()), {'0.6.8'})
        self.assertEqual(self.run_git('ls-remote', 'origin', 'refs/heads/main').split()[0], self.source)
        self.assertFalse(self.run_git('ls-remote', 'origin', 'refs/tags/v0.6.8'))
        with patch.object(release, 'git', wraps=release.git) as commands:
            release.commit('0.6.8', self.prepared, self.output)
        commands.assert_any_call('push', '--atomic', 'origin', 'HEAD:refs/heads/main', 'refs/tags/v0.6.8')
        self.assertEqual(self.run_git('ls-remote', 'origin', 'refs/heads/main').split()[0], state['release_sha'])
        self.assertEqual(self.run_git('ls-remote', 'origin', 'refs/tags/v0.6.8').split()[0], state['release_sha'])

    def test_retry_accepts_verified_original_source_and_reuses_commit(self):
        release.prepare('0.6.8', self.prepared)
        release.commit('0.6.8', self.prepared, self.output)
        committed = self.run_git('rev-parse', 'HEAD')
        self.run_git('checkout', '--detach', self.source)
        release.prepare('0.6.8', self.prepared)
        state = json.loads(self.prepared.read_text())
        self.assertTrue(state['reused'])
        self.assertEqual(state['source_sha'], self.source)
        self.assertEqual(state['release_sha'], committed)
        release.commit('0.6.8', self.prepared, self.output)

    def test_retry_rejects_unrelated_requested_source(self):
        release.prepare('0.6.8', self.prepared)
        release.commit('0.6.8', self.prepared, self.output)
        self.run_git('commit', '--allow-empty', '-m', 'unrelated')
        with self.assertRaisesRegex(RuntimeError, 'Requested source differs'):
            release.prepare('0.6.8', self.prepared)

    def test_source_trailer_cannot_hide_nonversion_changes(self):
        release.set_versions(self.root, '0.6.8')
        (self.root / 'payload').write_text('unexpected')
        self.run_git('add', '.')
        self.run_git('commit', '-m', 'unsafe', '-m', f'Release-Source: {self.source}')
        self.run_git('tag', 'v0.6.8')
        self.run_git('push', 'origin', 'refs/tags/v0.6.8')
        with self.assertRaisesRegex(RuntimeError, 'outside component versions'):
            release.prepare('0.6.8', self.prepared)

    def test_main_race_fails_before_publishing_tag(self):
        release.prepare('0.6.8', self.prepared)
        prepared = self.run_git('rev-parse', 'HEAD')
        self.run_git('checkout', '--detach', self.source)
        self.run_git('commit', '--allow-empty', '-m', 'concurrent main')
        self.run_git('push', 'origin', 'HEAD:refs/heads/main')
        self.run_git('checkout', '--detach', prepared)
        with self.assertRaisesRegex(RuntimeError, 'origin/main advanced'):
            release.commit('0.6.8', self.prepared, self.output)
        self.assertFalse(self.run_git('ls-remote', 'origin', 'refs/tags/v0.6.8'))

    def test_source_trailer_rejects_extra_whitespace_in_version_file(self):
        release.set_versions(self.root, '0.6.8')
        path = self.root / release.VERSION_FILES[0]
        path.write_text(path.read_text() + '\n')
        self.run_git('add', '.')
        self.run_git('commit', '-m', 'unsafe whitespace', '-m', f'Release-Source: {self.source}')
        self.run_git('tag', 'v0.6.8')
        self.run_git('push', 'origin', 'refs/tags/v0.6.8')
        with self.assertRaisesRegex(RuntimeError, 'only version replacements'):
            release.prepare('0.6.8', self.prepared)

    def test_validation_must_leave_checkout_clean(self):
        release.prepare('0.6.8', self.prepared)
        (self.root / 'unexpected').write_text('change')
        with self.assertRaisesRegex(RuntimeError, 'changed the prepared source'):
            release.commit('0.6.8', self.prepared, self.output)
        self.assertFalse(self.run_git('ls-remote', 'origin', 'refs/tags/v0.6.8'))

    def test_parse_failure_does_not_partially_update_versions(self):
        (self.root / release.VERSION_FILES[-1]).write_text('missing version')
        before = {name: (self.root / name).read_bytes() for name in release.VERSION_FILES}
        with self.assertRaisesRegex(RuntimeError, 'Expected one project version'):
            release.set_versions(self.root, '0.6.8')
        self.assertEqual(before, {name: (self.root / name).read_bytes() for name in release.VERSION_FILES})


class CoreSourceTests(unittest.TestCase):
    @staticmethod
    def archive(items):
        output = io.BytesIO()
        with tarfile.open(fileobj=output, mode='w:gz') as archive:
            for name, data in items:
                member = tarfile.TarInfo(name)
                member.size = len(data)
                archive.addfile(member, io.BytesIO(data))
        return output.getvalue()

    def test_source_entries_are_bounded_and_unique(self):
        self.assertEqual(core_package.entries(self.archive([('lib/a.ex', b'code')])), {'lib/a.ex': b'code'})
        with self.assertRaisesRegex(RuntimeError, 'Invalid or oversized'):
            core_package.entries(self.archive([('lib/a.ex', b'a'), ('lib/a.ex', b'b')]))
        with patch.object(core_package, 'PACKAGE_LIMIT', 1):
            with self.assertRaisesRegex(RuntimeError, 'Invalid or oversized'):
                core_package.entries(self.archive([('lib/a.ex', b'xx')]))

    def test_source_entries_reject_traversal(self):
        for name in ('../outside', '/absolute', 'lib/../outside', './mix.exs'):
            with self.subTest(name=name), self.assertRaisesRegex(RuntimeError, 'Invalid or oversized'):
                core_package.entries(self.archive([(name, b'bad')]))

    def test_manifest_compares_every_file_with_committed_source(self):
        contents = self.archive([('mix.exs', b'version: "0.6.8"\n')])
        with tempfile.TemporaryDirectory() as folder:
            package = Path(folder) / 'source.tar'
            package.write_bytes(b'fixture')
            output = Path(folder) / 'manifest.json'
            with patch.object(core_package, 'read_package', return_value=('checksum', contents)), \
                    patch.object(core_package.subprocess, 'check_output', side_effect=[
                        'a' * 40, 'apps/ex_storage_service/mix.exs\n', b'version: "0.6.8"\n']):
                core_package.manifest(package, '0.6.8', output)
            self.assertEqual(json.loads(output.read_text())['source_files_compared'], 1)
            with patch.object(core_package, 'read_package', return_value=('checksum', contents)), \
                    patch.object(core_package.subprocess, 'check_output', side_effect=[
                        'a' * 40, 'apps/ex_storage_service/mix.exs\n', b'version: "0.6.7"\n']):
                with self.assertRaisesRegex(RuntimeError, 'differs from committed source'):
                    core_package.manifest(package, '0.6.8', output)


if __name__ == '__main__':
    unittest.main()
