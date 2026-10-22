import datetime
import hashlib
import importlib.util
import json
import os
from pathlib import Path
import signal
import subprocess
import sys
import tempfile
import time

REPO = Path('/home/gao/Workspace/gsmlg-opt/ex_storage_service')
OUTPUT = Path(__file__).parent
spec = importlib.util.spec_from_file_location('hardening_acceptance', REPO / 'e2e/hardening_acceptance.py')
acceptance = importlib.util.module_from_spec(spec)
spec.loader.exec_module(acceptance)

for name, script, arguments in [
    ('minimal', OUTPUT / 'runtime_probe.exs', []),
    ('production', REPO / 'e2e/scripts/index_setup_probe.exs', ['100,500,1000']),
]:
    source = acceptance.source_fingerprint()
    head = acceptance.sha()
    root = Path(tempfile.mkdtemp(prefix=f'ess-concord315-{name}-'))
    receipt = OUTPUT / f'{name}-r1.json'
    argv = ['mix', 'run', '--no-compile', '--no-start', str(script), str(receipt), *arguments]
    environment = os.environ.copy()
    environment.update({
        'PAGER': 'cat', 'ERL_FLAGS': '+S 4:4 +SDcpu 2 +SDio 2', 'MIX_ENV': 'prod',
        'ESS_DATA_ROOT': str(root), 'ESS_BLOB_ROOT': str(root / 'blob'),
        'ESS_TMP_ROOT': str(root / 'tmp'), 'ESS_RA_ROOT': str(root / 'ra'),
        'ESS_METADATA_ROOT': str(root / 'metadata'), 'ESS_MODE': 'standalone',
        'ESS_AUTO_START': 'true', 'ESS_WEB_ENABLED': 'false', 'ESS_PUBLIC_S3_ENABLED': 'false',
        'ESS_REPAIR_ENABLED': 'false', 'ESS_SCRUB_ENABLED': 'false',
        'ESS_CONCORD_COMMAND_VERSION': '0', 'ESS_MASTER_KEY': 'disposable-concord315-probe-key',
        'ESS_INDEX_SOURCE_SHA256': source, 'ESS_INDEX_SOURCE_HEAD': head,
        'ESS_INDEX_PROGRESS': str(OUTPUT / f'{name}-r1-progress.jsonl'),
    })
    started = time.monotonic()
    with (OUTPUT / f'{name}-r1.log').open('x') as log:
        process = subprocess.Popen(argv, cwd=REPO, env=environment,
                                   stdout=log, stderr=subprocess.STDOUT, start_new_session=True)
        timed_out = False
        try:
            status = process.wait(timeout=300)
        except subprocess.TimeoutExpired:
            timed_out = True
            os.killpg(process.pid, signal.SIGTERM)
            try:
                status = process.wait(timeout=10)
            except subprocess.TimeoutExpired:
                os.killpg(process.pid, signal.SIGKILL)
                status = process.wait(timeout=10)
    try:
        os.killpg(process.pid, 0)
    except ProcessLookupError:
        group_stopped = True
    else:
        group_stopped = False
    after = acceptance.source_fingerprint()
    result = {
        'probe': name, 'argv': argv, 'root': str(root), 'pid': process.pid,
        'deadline_seconds': 300, 'elapsed_seconds': time.monotonic() - started,
        'exit_status': status, 'timed_out': timed_out, 'reaped': process.returncode is not None,
        'process_group_stopped': group_stopped, 'diagnostic_only': True, 'acceptance_promotion': False,
        'source_sha256': source, 'source_after_sha256': after, 'source_head': head,
        'probe_sha256': hashlib.sha256(script.read_bytes()).hexdigest(),
        'completed_at': datetime.datetime.now(datetime.timezone.utc).isoformat(),
    }
    (OUTPUT / f'{name}-r1-execution.json').write_text(json.dumps(result, indent=2) + '\n')
    print(json.dumps(result), flush=True)
    if status or timed_out or not group_stopped or after != source:
        sys.exit(1)
