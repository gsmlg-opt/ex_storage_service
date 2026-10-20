import datetime
import hashlib
import importlib.util
import json
import pathlib
import time

repo = pathlib.Path('/home/gao/Workspace/gsmlg-opt/ex_storage_service')
fixture = pathlib.Path('/fast/build/ess-closeout-c5-nvme-resources/tmp/ess-hardening-resource-closeout002-c5-nvme-r2-resources')
out = repo / 'docs/reports/s3-hardening/closeout-002/candidate-c5/nvme-resources-r2/copy-retained-bytes.json'
spec = importlib.util.spec_from_file_location('acceptance', repo / 'e2e/hardening_acceptance.py')
acceptance = importlib.util.module_from_spec(spec)
spec.loader.exec_module(acceptance)
assert not out.exists()
assert json.loads((repo / 'docs/reports/s3-hardening/closeout002-c5-copy-diag-r3/diagnostic.json').read_text())['exit_status'] == 0
rows = []
stages = sorted((fixture / 'data/tmp/uploads').glob('ess1-h-*'))
assert len(stages) == 4
hashes = {p.name.rsplit('-', 1)[-1] for p in stages}
assert len(hashes) == 1
expected = next(iter(hashes))
cas = fixture / 'data/blob/objects/sha256' / expected[:2] / expected[2:]
for role, path in [('existing_cas', cas), *[('hosted_protected_stage', p) for p in stages]]:
    start = time.monotonic_ns()
    digest = hashlib.sha256()
    size = 0
    with path.open('rb') as stream:
        while block := stream.read(1024 * 1024):
            digest.update(block)
            size += len(block)
    rows.append(dict(role=role, path=str(path.relative_to(fixture)), bytes=size,
                     sha256=digest.hexdigest(), matches_named_hash=digest.hexdigest() == expected,
                     duration_ns=time.monotonic_ns() - start))
assert all(r['bytes'] == 1073741824 and r['matches_named_hash'] for r in rows)
result = dict(schema=1, kind='read-only-retained-copy-byte-inspection', acceptance=False,
              candidate_sha=acceptance.sha(), source_sha256=acceptance.source_fingerprint(),
              inspected_at=datetime.datetime.now(datetime.timezone.utc).isoformat(),
              method='Independent Python SHA256 in bounded1MiB chunks after all owned heavy workloads stopped; original fixture read only.',
              input_script_sha256=hashlib.sha256(pathlib.Path(__file__).read_bytes()).hexdigest(),
              rows=rows, bytes_verified=sum(r['bytes'] for r in rows),
              conclusion='Existing CAS and all four retained protected stages are complete and independently match their named SHA256. This excludes present-day byte corruption at these paths, but does not prove that commit or intent-open ran or identify the original error.')
out.write_text(json.dumps(result, indent=2) + '\n')
print(json.dumps(dict(actual_exit_status=0, files=len(rows), bytes_verified=result['bytes_verified'], conclusion=result['conclusion']), indent=2))
