import datetime
import hashlib
import json
import os
import pathlib
import shutil
import subprocess

repo = pathlib.Path('/home/gao/Workspace/gsmlg-opt/ex_storage_service')
base = repo / 'docs/reports/s3-hardening/closeout-002/candidate-c5'
out = base / 'fixture-cleanup.json'
assert not out.exists()
assert json.loads((repo / 'docs/reports/s3-hardening/closeout002-c5-copy-diag-r3/diagnostic.json').read_text())['exit_status'] == 0
assert json.loads((base / 'observation-archives/manifest.json').read_text())['archives']
names = subprocess.check_output(['epmd', '-names'], text=True).splitlines()
containers = subprocess.check_output(['docker', 'ps', '--format', '{{.Names}}'], text=True).splitlines()
assert not [n for n in names if any(s in n for s in ['ess_gateway_', 'ess_hardening_', 'ess_resource_'])]
assert not [n for n in containers if n.startswith('ess-hardening-')]
identities = []
for root in [repo / 'docs/reports/s3-hardening/closeout-002/candidate-c5', *sorted((repo / 'docs/reports/s3-hardening').glob('closeout002-c5*'))]:
    for manifest in root.rglob('observer-*.json'):
        value = json.loads(manifest.read_text())
        if not isinstance(value, dict) or not {'beam', 'observer'} <= value.keys():
            continue
        for role in ['beam', 'observer']:
            old = value[role]
            try:
                fields = pathlib.Path(f"/proc/{old['pid']}/stat").read_text().rsplit(')', 1)[1].split()
                actual = dict(pid=old['pid'], start_ticks=int(fields[19]), state=fields[0])
            except FileNotFoundError:
                actual = None
            assert actual is None or actual['start_ticks'] != old['start_ticks']
            identities.append(dict(manifest=str(manifest.relative_to(repo)), role=role, expected=old, observed=actual, stopped=True))
private = pathlib.Path('/fast/build/ess-closeout-c5-private-forensics')
private.mkdir(mode=0o700, exist_ok=False)
retained = []
sources = [
    ('original-copy-replica.wal', pathlib.Path('/fast/build/ess-closeout-c5-nvme-resources/tmp/ess-hardening-resource-closeout002-c5-nvme-r2-resources/data/metadata/vsr/ess_gateway_311c3b40d5_127.0.0.1/replica.wal')),
    ('original-cluster-resource-control.log', pathlib.Path('/tmp/ess-s3-hardening-cluster-sfQ8N1mW/cluster-resource-control.log')),
]
for name, source in sources:
    assert source.is_file() and source.stat().st_size < 10 * 1024 * 1024
    data = source.read_bytes()
    target = private / name
    with os.fdopen(os.open(target, os.O_WRONLY | os.O_CREAT | os.O_EXCL, 0o600), 'wb') as sink:
        sink.write(data)
    assert hashlib.sha256(target.read_bytes()).digest() == hashlib.sha256(data).digest()
    retained.append(dict(path=str(target), bytes=len(data), sha256=hashlib.sha256(data).hexdigest(), publication='private local only; contains raw fixture commands/logs and is intentionally excluded from Git/public evidence'))
roots = [
    '/tmp/ess-hardening-resource-closeout002-c5-resources',
    '/tmp/ess-hardening-resource-closeout002-c5-observer-diag-baseline-r1',
    '/tmp/ess-hardening-resource-closeout002-c5-observer-diag-baseline-r2',
    '/tmp/ess-s3-hardening-cluster-sfQ8N1mW',
    '/fast/build/ess-closeout-c5-nvme-resources/tmp/ess-hardening-resource-closeout002-c5-nvme-r2-resources',
    '/fast/build/ess-closeout-c5-nvme-resources/tmp/ess-s3-hardening-cluster-xLZxLn22',
    '/fast/build/ess-closeout-c5-nvme-resources/tmp/ess-s3-hardening-closeout002-c5-nvme-r2-resources-tests',
    *[f'/fast/build/ess-closeout-c5-copy-diagnostic/tmp/ess-hardening-resource-closeout002-c5-copy-diag-r{i}' for i in range(1, 4)],
]
result = dict(schema=1, status='in_progress', started_at=datetime.datetime.now(datetime.timezone.utc).isoformat(),
              scope='Only exact recorded owned disposable C5 resource/observer/copy fixtures; process start identities verified stopped before removal. Build caches, toolchains and unrelated paths untouched.',
              input_script_sha256=hashlib.sha256(pathlib.Path(__file__).read_bytes()).hexdigest(), stopped_identities=identities,
              private_forensics=retained, removed_roots=[],
              acceptance_effect='None. Explicit fixture disposal after diagnosis does not repair the original post-idle4GiB retention or promote failed/incomplete resource results.')
out.write_text(json.dumps(result, indent=2) + '\n')
for value in roots:
    path = pathlib.Path(value)
    assert not path.is_symlink() and path.resolve() == path
    row = dict(path=value, existed=path.exists())
    if path.exists():
        assert path.is_dir()
        files = [p for p in path.rglob('*') if p.is_file() and not p.is_symlink()]
        row.update(files=len(files), logical_file_bytes=sum(p.stat().st_size for p in files))
        shutil.rmtree(path)
    assert not path.exists()
    row['absent_after'] = True
    result['removed_roots'].append(row)
    out.write_text(json.dumps(result, indent=2) + '\n')
result.update(status='passed', finished_at=datetime.datetime.now(datetime.timezone.utc).isoformat())
out.write_text(json.dumps(result, indent=2) + '\n')
print(json.dumps(dict(status=result['status'], roots=len(result['removed_roots']), checked_identities=len(identities), private_forensics=len(retained)), indent=2))
