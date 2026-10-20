import datetime
import hashlib
import json
import os
from pathlib import Path
import runpy
import shutil
import signal
import subprocess
import sys
import time

repo = Path('/home/gao/Workspace/gsmlg-opt/ex_storage_service')
runner = Path(__file__).resolve().parent
candidate = repo / 'docs/reports/s3-hardening/closeout-002/candidate-c6'
original = json.loads((candidate / 'invocation.json').read_text())
if original.get('status') not in ('passed', 'failed') or 'exit_status' not in original:
    raise RuntimeError('original serial driver must finish before cache copy or retries')
acceptance = runpy.run_path(str(repo / 'e2e/hardening_acceptance.py'))
freeze = json.loads((candidate / 'freeze.json').read_text())
if acceptance['sha']() != freeze['candidate_sha'] or acceptance['source_fingerprint']() != freeze['source_sha256']:
    raise RuntimeError('candidate source changed')
for name in acceptance['PROFILES']:
    if json.loads((candidate / (name + '.json')).read_text())['status'] == 'in_progress':
        raise RuntimeError('an original lane is still active')
environment = dict(os.environ, PAGER='cat', ERL_FLAGS='+S 4:4 +SDcpu 2 +SDio 2')
for name in ('AWS_REQUEST_CHECKSUM_CALCULATION', 'AWS_RESPONSE_CHECKSUM_VALIDATION'):
    if environment.get(name) is not None:
        raise RuntimeError('checksum override must remain unset')
output = candidate / 'cache-prerequisite-retry'
output.mkdir(exist_ok=False)
source_cache = Path('/home/gao/.npm_ex')
private_cache = runner / 'npm-cache-r3'
if not source_cache.is_dir() or source_cache.is_symlink() or private_cache.exists():
    raise RuntimeError('expected existing real cache and new exclusively owned private copy')

def tree(root):
    result = hashlib.sha256()
    count = size = 0
    for path in sorted(root.rglob('*')):
        relative = str(path.relative_to(root)).encode()
        mode = str(path.lstat().st_mode).encode()
        result.update(relative + b'\0' + mode + b'\0')
        if path.is_symlink():
            result.update(os.readlink(path).encode())
        elif path.is_file():
            count += 1
            with path.open('rb') as data:
                while chunk := data.read(1024 * 1024):
                    size += len(chunk)
                    result.update(chunk)
        elif not path.is_dir():
            raise RuntimeError('unsupported cache entry')
        result.update(b'\0')
    return dict(sha256=result.hexdigest(), files=count, file_bytes=size)

def save(name, value):
    (output / name).write_text(json.dumps(value, indent=2) + '\n')

before = tree(source_cache)
shutil.copytree(source_cache, private_cache, symlinks=True, copy_function=shutil.copy2)
copied = tree(private_cache)
if tree(source_cache) != before or copied != before:
    raise RuntimeError('cache changed during private copy')
private_cache.chmod(0o700)
namespace = json.loads((runner / 'namespace-command-r2.json').read_text()) + ['--bind', str(private_cache), str(source_cache)]
shutil.copyfile(Path(__file__), output / 'retry_runner.py')
save('namespace-command-r3.json', namespace)
save('runner-prerequisite.json', dict(
    candidate_sha=freeze['candidate_sha'], source_sha256=freeze['source_sha256'],
    recorded_at=datetime.datetime.now(datetime.timezone.utc).isoformat(),
    original_invocation_exit_status=original['exit_status'],
    original_failure='read-only ~/.npm_ex blocked mix assets.deploy before retained standalone setup; same prerequisite affects release',
    source_cache=str(source_cache), private_cache=str(private_cache), source_cache_tree=before,
    private_copy_tree=copied, symlinks_and_modes_preserved=True, host_cache_not_written=True,
    namespace_command=namespace, production_configuration_unchanged=True,
    policy='Only the task-owned private copy is writable at the normal cache path; HOME, dependency versions, auth/checksums, RF/W, sizes, concurrency and coverage remain unchanged. Original failed receipts remain immutable.',
    wrapper_sha256=hashlib.sha256(Path(__file__).read_bytes()).hexdigest()))
python = '/tmp/ess-s3-hardening-python/bin/python'
probe = "from pathlib import Path;p=Path('/home/gao/.npm_ex/.ess-closeout-r3-write-probe');p.write_text('owned private cache probe');p.unlink();print('private cache write passed')"
check = subprocess.run(namespace + ['--', python, '-c', probe], cwd=repo, env=environment,
                       capture_output=True, text=True, timeout=15)
save('cache-write-probe.json', dict(exit_status=check.returncode, stdout=check.stdout, stderr=check.stderr))
check.check_returncode()
record = dict(starting_sha=acceptance['sha'](), source_sha256=acceptance['source_fingerprint'](),
    started_at=datetime.datetime.now(datetime.timezone.utc).isoformat(), commands=[], status='in_progress')
save('invocation.json', record)
for profile in ('standalone', 'release'):
    command = namespace + ['--', python, 'e2e/hardening_acceptance.py', 'run', '--profile', profile,
        '--python', python, '--mc', '/tmp/ess-s3-hardening-tools/mc',
        '--label', 'closeout002-c6-cache-r3', '--output', str(output.relative_to(repo))]
    started = time.monotonic()
    with (output / (profile + '-entry.log')).open('xb') as log:
        process = subprocess.Popen(command, cwd=repo, env=environment, stdout=log,
                                   stderr=subprocess.STDOUT, start_new_session=True)
        try:
            code = process.wait()
        except BaseException:
            if process.poll() is None:
                os.killpg(process.pid, signal.SIGTERM)
                try:
                    process.wait(timeout=45)
                except subprocess.TimeoutExpired:
                    os.killpg(process.pid, signal.SIGKILL)
                    process.wait(timeout=10)
            raise
    record['commands'].append(dict(profile=profile, argv=command, exit_status=code,
                                   duration_seconds=time.monotonic() - started))
    save('invocation.json', record)
    print(profile + ' retry actual exit ' + str(code), flush=True)
record.update(ending_sha=acceptance['sha'](), ending_source_sha256=acceptance['source_fingerprint'](),
    ended_at=datetime.datetime.now(datetime.timezone.utc).isoformat(),
    private_cache_final_tree=tree(private_cache),
    host_cache_tree_unchanged=tree(source_cache) == before,
    status='passed' if all(row['exit_status'] == 0 for row in record['commands']) else 'failed')
if record['ending_source_sha256'] != record['source_sha256']:
    record.update(status='failed', reason='source changed during retry')
record['exit_status'] = 0 if record['status'] == 'passed' else 1
save('invocation.json', record)
print(json.dumps(record), flush=True)
sys.exit(record['exit_status'])
