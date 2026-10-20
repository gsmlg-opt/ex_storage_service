import datetime
import gzip
import hashlib
import json
import pathlib

repo = pathlib.Path('/home/gao/Workspace/gsmlg-opt/ex_storage_service')
reports = repo / 'docs/reports/s3-hardening'
out = reports / 'closeout-002/candidate-c5/observation-archives'
assert not out.exists()
assert json.loads((reports / 'closeout002-c5-copy-diag-r3/diagnostic.json').read_text())['exit_status'] == 0
out.mkdir()
rows = []
for root in sorted(reports.glob('closeout002-c5*')):
    for source in sorted(root.rglob('samples.jsonl')):
        assert source.stat().st_size < 128 * 1024 * 1024
        target = out / (str(source.relative_to(reports)).replace('/', '__') + '.gz')
        raw = hashlib.sha256()
        raw_bytes = 0
        with source.open('rb') as reader, target.open('xb') as sink:
            with gzip.GzipFile(filename='', mode='wb', fileobj=sink, mtime=0, compresslevel=6) as writer:
                while block := reader.read(1024 * 1024):
                    raw.update(block)
                    raw_bytes += len(block)
                    writer.write(block)
        restored = hashlib.sha256()
        restored_bytes = 0
        with gzip.open(target, 'rb') as reader:
            while block := reader.read(1024 * 1024):
                restored.update(block)
                restored_bytes += len(block)
        assert restored_bytes == raw_bytes and restored.hexdigest() == raw.hexdigest()
        compressed = hashlib.sha256()
        with target.open('rb') as reader:
            while block := reader.read(1024 * 1024):
                compressed.update(block)
        rows.append(dict(raw_path=str(source.relative_to(repo)), raw_bytes=raw_bytes,
                         raw_sha256=raw.hexdigest(), archive_path=str(target.relative_to(repo)),
                         archive_bytes=target.stat().st_size, archive_sha256=compressed.hexdigest(),
                         roundtrip_verified=True))
assert rows
manifest = dict(schema=1, kind='lossless-observation-evidence-archives', created_at=datetime.datetime.now(datetime.timezone.utc).isoformat(),
                candidate_sha='83ddc379be879b947e19aecda45e615f4f45b530',
                source_sha256='64d21d0863ca538bef84ac297733ef2f5dd97746b4f1bd818ade52f0931e6288',
                acceptance_effect='None. Failed and diagnostic evidence keeps its original status and scope. Original raw samples remain unchanged locally.',
                restoration='For each row, stream-decompress archive_path to raw_path, then verify raw_bytes/raw_sha256 before running the original manifest/export analysis. Never infer missing observations.',
                input_script_sha256=hashlib.sha256(pathlib.Path(__file__).read_bytes()).hexdigest(), archives=rows)
(out / 'manifest.json').write_text(json.dumps(manifest, indent=2) + '\n')
print(json.dumps(dict(archives=len(rows), raw_bytes=sum(r['raw_bytes'] for r in rows), archive_bytes=sum(r['archive_bytes'] for r in rows), roundtrip_verified=True), indent=2))
