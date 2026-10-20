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
output = repo / 'docs/reports/s3-hardening/closeout-002/candidate-c6'
python = '/tmp/ess-s3-hardening-python/bin/python'
mc = '/tmp/ess-s3-hardening-tools/mc'
namespace = json.loads((runner / 'namespace-command-r2.json').read_text())
environment = dict(os.environ, PAGER='cat', ERL_FLAGS='+S 4:4 +SDcpu 2 +SDio 2')
for name in ('AWS_REQUEST_CHECKSUM_CALCULATION', 'AWS_RESPONSE_CHECKSUM_VALIDATION'):
    if environment.get(name) is not None:
        raise RuntimeError('checksum override must remain unset')
acceptance = runpy.run_path(str(repo / 'e2e/hardening_acceptance.py'))
dirty = subprocess.check_output(['git', 'diff', '--name-only', 'HEAD'], cwd=repo, text=True).splitlines()
untracked = subprocess.check_output(['git', 'ls-files', '--others', '--exclude-standard'], cwd=repo, text=True).splitlines()
if any(acceptance['executable_input'](Path(name)) for name in dirty + untracked):
    raise RuntimeError('uncommitted executable input before candidate freeze')
if output.exists():
    raise RuntimeError('refusing to replace candidate evidence')
output.mkdir()
inputs = output / 'runner-inputs'
inputs.mkdir()
for path in (Path(__file__), runner / 'namespace-command-r2.json', runner / 'prerequisite.json', runner / 'prerequisite-r2.json'):
    shutil.copyfile(path, inputs / path.name)

def digest(path):
    return hashlib.sha256(Path(path).read_bytes()).hexdigest()

def save(name, value):
    (output / name).write_text(json.dumps(value, indent=2) + '\n')

candidate = acceptance['sha']()
fingerprint = acceptance['source_fingerprint']()
command = [python, 'e2e/hardening_acceptance.py', 'run', '--profile', 'all',
           '--python', python, '--mc', mc, '--label', 'closeout002-c6',
           '--output', str(output.relative_to(repo))]
external_inputs = {str(path.relative_to(output)): digest(path) for path in inputs.iterdir()}
freeze = dict(task='ESS-S3-HARDENING-CLOSEOUT-002',
    starting_sha='b17282d96a58eff962a35d807c9e7adb39af4774', candidate_sha=candidate,
    source_sha256=fingerprint, frozen_at=datetime.datetime.now(datetime.timezone.utc).isoformat(),
    tracked_worktree_clean='all executable inputs committed; report-only writes permitted',
    fingerprint_rule=acceptance['source_fingerprint'].__doc__,
    fingerprint_definition='e2e/hardening_acceptance.py:executable_input/source_fingerprint; sorted cached and nonignored untracked names; name NUL contents NUL; apps/config/e2e/scripts/.github, Mix/npm manifests/locks, Docker inputs, formatter and docs/cli/compatibility JSON included; other reports/docs excluded. Runner invocation inputs separately archived and hashed below; installed dependency versions constrained by mix.lock.',
    authoritative_command=command, namespace_command=namespace,
    runner_input_sha256=external_inputs, lock_sha256=digest(repo / 'mix.lock'),
    resource_envelope_sha256=digest(repo / 'docs/plans/s3-hardening-resource-envelope.md'),
    python=dict(path=python, executable_sha256=digest(python),
                packages=subprocess.check_output([python, '-m', 'pip', 'freeze'], text=True).splitlines()),
    mc=dict(path=mc, sha256=digest(mc)),
    previous_candidate=dict(sha='83ddc379be879b947e19aecda45e615f4f45b530',
        source_sha256='64d21d0863ca538bef84ac297733ef2f5dd97746b4f1bd818ade52f0931e6288',
        actual_run_exit_status=1, actual_aggregate_exit_status=1,
        evidence='docs/reports/s3-hardening/closeout-002/candidate-c5'),
    input_changes='Failure-only bounded storage error classification logging; reclaim only active standalone LocalCAS stage after raw commit failure and positive matching abort ack; wrapped uncertain outcomes retain. No dependency, auth, checksum, RF/W, dataset, concurrency, profile, cadence or envelope changes.',
    environment_policy=dict(PAGER='cat', ERL_FLAGS=environment['ERL_FLAGS'],
        checksum_overrides='unset', task_owned_heavy_workloads='serial',
        disk='private /tmp mapped to /fast/build/ess-closeout-c6-acceptance/tmp (NVMe ext4); host PID namespace retained for observer identity; nested restore namespaces permitted by disposable proc mount',
        existing_database_command_version='0 unchanged', empty_restore_targets_command_version='1 through every restart/activation',
        thresholds='original 100 ms coverage and predeclared envelopes unchanged',
        intrusive_tracing='absent'), publication='not_attempted; full acceptance prerequisite')
save('freeze.json', freeze)
probe = "import sys,json,pathlib,subprocess;sys.path.insert(0,'e2e');from resource_cases import runner_details;v=runner_details('/tmp');v['disk_identity']=subprocess.check_output(['lsblk','-o','NAME,TYPE,FSTYPE,SIZE,ROTA,MOUNTPOINTS'],text=True);v['process_load']=subprocess.check_output(['ps','-eo','pid,comm,pcpu,rss','--sort=-pcpu'],text=True).splitlines()[:25];print(json.dumps(v,indent=2))"
details = subprocess.run(namespace + ['--', python, '-c', probe], cwd=repo, env=environment,
                         capture_output=True, text=True, timeout=30, check=True)
save('runner-before.json', json.loads(details.stdout))
record = dict(candidate_sha=candidate, source_sha256=fingerprint, command=namespace + ['--'] + command,
    started_at=datetime.datetime.now(datetime.timezone.utc).isoformat(), status='in_progress')
save('invocation.json', record)
print(json.dumps(record), flush=True)
started = time.monotonic()
with (output / 'authoritative-run.log').open('xb') as log:
    process = subprocess.Popen(namespace + ['--'] + command, cwd=repo, env=environment,
                               stdout=log, stderr=subprocess.STDOUT, start_new_session=True)
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
record.update(status='passed' if code == 0 else 'failed', exit_status=code,
    ending_sha=acceptance['sha'](), ending_source_sha256=acceptance['source_fingerprint'](),
    duration_seconds=time.monotonic() - started,
    ended_at=datetime.datetime.now(datetime.timezone.utc).isoformat(),
    resource_envelope_unchanged=digest(repo / 'docs/plans/s3-hardening-resource-envelope.md') == freeze['resource_envelope_sha256'],
    runner_inputs_unchanged=all(digest(output / name) == expected for name, expected in external_inputs.items()))
save('invocation.json', record)
print(json.dumps(record), flush=True)
sys.exit(code)
