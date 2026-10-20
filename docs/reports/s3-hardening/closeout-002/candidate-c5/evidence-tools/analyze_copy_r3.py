import collections
import datetime
import hashlib
import json
import pathlib
import subprocess
import sys

repo = pathlib.Path('/home/gao/Workspace/gsmlg-opt/ex_storage_service')
sys.path.insert(0, str(repo / 'e2e'))
from resource_cases import manifest

root = repo / 'docs/reports/s3-hardening/closeout002-c5-copy-diag-r3'
assert not (root / 'analysis.json').exists()
terminal = json.loads((root / 'diagnostic.json').read_text())
assert terminal['exit_status'] == 0 and terminal['source_sha256'] == terminal['source_after']
assert terminal['candidate_sha'] == terminal['ending_sha']
inputs = []
for name, expected in terminal['inputs'].items():
    path = root / 'inputs' / name
    digest = hashlib.sha256(path.read_bytes()).hexdigest()
    assert digest == expected
    inputs.append(dict(path=str(path.relative_to(repo)), sha256=digest))
path = root / 'workload/results.json'
result = json.loads(path.read_text())
identity = lambda row: (row['name'], row['size'], row['concurrency'])
expected = manifest('all')[:52]
assert result['required_cases'] == expected and list(map(identity, result['groups'])) == list(map(identity, expected))
assert result['status'] == 'passed' and all(not g['failures'] for g in result['groups'])
assert result['acceptance'] is False
trace = [json.loads(line) for line in (root / 'copy-returns.jsonl').read_text().splitlines()]
assert trace[0] == dict(coverage='started')
events = trace[1:]
assert [r['ordinal'] for r in events] == list(range(1, len(events) + 1))
assert len(events) < 4096 and all(r['result'] == 'ok' and r['type'] == 'return_from' for r in events)
errors = [json.loads(line) for line in (root / 'storage-errors.jsonl').read_text().splitlines()]
assert len(errors) == 1 and errors[0]['coverage'] == 'started'
target = result['groups'][-1]
target_events = [e for e in events if target['start_ms'] <= e['wall_ms'] <= target['end_ms']]
completed = [e for e in target_events if e['module'] == 'Elixir.ExStorageServiceS3.Handlers.Object.CloudCopy' and e['function'] == 'copy']
assert len(completed) == 4 and len({e['pid'] for e in completed}) == 4
exports = []
for path in (root / 'workload').rglob('resource-export-*.json'):
    value = json.loads(path.read_text())
    digest = hashlib.sha256()
    size = count = 0
    with (path.parent / 'samples.jsonl').open('rb') as stream:
        stream.seek(value['output_start'])
        while size < value['output_bytes']:
            line = stream.readline(value['row_byte_limit'] + 1)
            assert line.endswith(b'\n') and len(line) <= value['row_byte_limit']
            row = json.loads(line)
            assert str(row['sampler_id']) == str(value['generation']) and row['sequence'] == count + value['first_sequence']
            digest.update(line)
            size += len(line)
            count += 1
    assert value['status'] == 'passed' and size == value['output_bytes'] and count == value['admitted_rows'] == value['persisted_rows']
    assert digest.hexdigest() == value['sha256'] and count + value['first_sequence'] - 1 == value['last_sequence']
    exports.append(dict(manifest=str(path.relative_to(repo)), rows=count, bytes=size, sha256=digest.hexdigest()))
assert len(exports) == 1
identities = []
for path in (root / 'workload').rglob('observer-*.json'):
    value = json.loads(path.read_text())
    if not isinstance(value, dict) or 'observer' not in value:
        continue
    for role in ['beam', 'observer']:
        old = value[role]
        proc = pathlib.Path(f"/proc/{old['pid']}/stat")
        observed = None
        try:
            fields = proc.read_text().rsplit(')', 1)[1].split()
            observed = dict(pid=old['pid'], start_ticks=int(fields[19]), state=fields[0])
        except FileNotFoundError:
            pass
        assert observed is None or observed['start_ticks'] != old['start_ticks']
        identities.append(dict(role=role, expected=old, observed=observed, stopped=True))
assert len(identities) == 2 and not terminal['terminal_staging']
names = subprocess.check_output(['epmd', '-names'], text=True).splitlines()
containers = subprocess.check_output(['docker', 'ps', '--format', '{{.Names}}'], text=True).splitlines()
assert not [n for n in names if 'ess_gateway_' in n]
assert not [n for n in containers if n.startswith('ess-hardening-')]
analysis = dict(schema=1, acceptance=False, candidate_sha=terminal['candidate_sha'], source_sha256=terminal['source_sha256'],
                verified_at=datetime.datetime.now(datetime.timezone.utc).isoformat(), actual_exit_status=terminal['exit_status'],
                scope='Intrusive bounded trace of unchanged original resource prefix through group52; four remaining required groups omitted explicitly. This cannot certify A30 or replace the failed untraced resource suite.',
                verified_input_hashes=inputs, groups=len(result['groups']), return_events=len(events), target_completed_requests=len(completed),
                target_return_counts=dict(collections.Counter(e['function'] for e in target_events)), storage_error_events=len(errors) - 1,
                trace_limit_reached=False, exports=exports, stopped_identities=identities, terminal_staging=terminal['terminal_staging'],
                maximum_probe_ms=max(c['maximum_probe_ns'] for g in result['groups'] for c in g['cadence'].values()) / 1e6,
                maximum_gap_ms=max(c['maximum_gap_ns'] for g in result['groups'] for c in g['cadence'].values()) / 1e6,
                conclusion='The original all-four CopyObject failure was not reproduced even with original preceding history. No exact error or service repair is inferred from this diagnostic pass.')
(root / 'analysis.json').write_text(json.dumps(analysis, indent=2) + '\n')
print(json.dumps(analysis, indent=2))
