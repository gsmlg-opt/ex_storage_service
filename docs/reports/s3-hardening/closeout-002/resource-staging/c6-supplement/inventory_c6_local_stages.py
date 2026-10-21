#!/usr/bin/env python3
"""Read-only exact C6 local/gateway terminal staging metadata inventory.

Default invocation describes scope only. --execute is required. This does not
run services/tests, open regular fixture files, read secrets, follow symlinks,
remove fixtures, reconstruct old samples or certify overall acceptance.
"""
import argparse
import datetime
import hashlib
import json
import os
from pathlib import Path
import stat
import subprocess

REPO = Path('/home/gao/Workspace/gsmlg-opt/ex_storage_service')
EXTERNAL = Path('/fast/build/ess-closeout-c6-acceptance')
VIRTUAL = '/tmp/ess-hardening-resource-closeout002-c6-resources'
PHYSICAL = EXTERNAL / 'tmp/ess-hardening-resource-closeout002-c6-resources'
CANDIDATE = '69371bc0c2d00f63fd8f3ff0edc751c28a02d441'
FINGERPRINT = '97e56e9eb014454095aff5cc5501f3acc132dbbdcbd04d7d4555d46226031bcb'
PINS = {'docs/reports/s3-hardening/closeout-002/candidate-c6/resources.json': '2b9a493ba3ce027698404d3e4ad7e3f4d8be704d24b931ec08fee0469b1ab061', 'docs/reports/s3-hardening/closeout-002/candidate-c6/resource-observation-analysis.json': 'b5762cc141e850bc10ed2f5fca93d101e46d7245b3f2d4d8d49ff2c5002c9775', 'docs/reports/s3-hardening/closeout-002/candidate-c6/terminal-cleanup.json': '5e4e0c48437d8f6fcbe0805cf193310a86e16578f21deaa083054ce05370fb57', 'docs/reports/s3-hardening/closeout-002/candidate-c6/invocation.json': 'e60eafe64831ef309bb679df2e2ee13d756aac18178e97c47bc9e51813c97d5b', 'docs/reports/s3-hardening/closeout-002/candidate-c6/cache-prerequisite-retry/invocation.json': '1d01d8331b4cdcd20fa36653e78f52c7cdfe54c25e6fbac92e685aab8b730a1b', 'docs/reports/s3-hardening/closeout002-c6-resources/results.json': '2422284678b9d5927cbd136115856f16d6e9f77b943a195bace31a4fbe133fcd', 'e2e/gateway_hardening.py': '4597482ec736d4a19ac75881907c2ebecd07c538ae9a940990935ca1d4c8e37a', 'e2e/s3_hardening_resources.py': '295e3bf45d3c3ba548f35566b1fd1589996746b4eae704e33e11e094438430f7', 'e2e/scripts/resource_sampler.exs': '306407303867b029c832def64a635eabf37e5618a928791e78ed6321eaa18e90', 'apps/ex_storage_service/lib/ex_storage_service/cloud_cache/local_store.ex': '29987fc645687926250e7f0e7bf4c3a595a3199a21e39fcff49b012aa1d555a7'}
MAX_ENTRIES = 4096
MAX_DEPTH = 16
FLAGS = os.O_RDONLY | os.O_DIRECTORY | os.O_NOFOLLOW
SOURCE_FROM_GIT = {path for path in PINS if not path.startswith('docs/')}


def require(ok, code):
    if not ok:
        raise RuntimeError(code)


def read_pinned(relative):
    require(relative in PINS, 'unregistered_provenance_path')
    if relative in SOURCE_FROM_GIT:
        raw = subprocess.check_output(['git', 'show', f'{CANDIDATE}:{relative}'],
                                      cwd=REPO, stderr=subprocess.DEVNULL)
    else:
        # Explicit retained receipt paths only; no private fixture contents.
        raw = (REPO / relative).read_bytes()
    require(hashlib.sha256(raw).hexdigest() == PINS[relative], 'provenance_hash_changed')
    return raw


def read_receipt(relative):
    require(relative not in SOURCE_FROM_GIT, 'source_is_not_a_receipt')
    return json.loads(read_pinned(relative))


def anchored_directory(path):
    current = os.open('/', FLAGS)
    try:
        for part in path.parts[1:]:
            child = os.open(part, FLAGS, dir_fd=current)
            os.close(current)
            current = child
        return current
    except BaseException:
        os.close(current)
        raise


def child_directory(parent, parts):
    current = os.dup(parent)
    try:
        for part in parts:
            child = os.open(part, FLAGS, dir_fd=current)
            os.close(current)
            current = child
        return current
    except BaseException:
        os.close(current)
        raise


def pid_identity(pid):
    # Exact recorded OS identity only; never argv/environ or arbitrary PID scans.
    try:
        fields = Path(f'/proc/{pid}/stat').read_text().rsplit(')', 1)[1].split()
        return {'pid': int(pid), 'start_ticks': int(fields[19]), 'state': fields[0]}
    except FileNotFoundError:
        return None


def inventory(directory, choose, recursive):
    result = {'files': 0, 'bytes': 0, 'zero_byte_files': 0,
              'directories_visited': 0, 'entries_visited': 0, 'files_metadata': []}

    def scan(fd, prefix, depth):
        require(depth <= MAX_DEPTH, 'inventory_depth_bound')
        before = os.fstat(fd)
        result['directories_visited'] += 1
        with os.scandir(fd) as entries:
            for entry in entries:
                result['entries_visited'] += 1
                require(result['entries_visited'] <= MAX_ENTRIES, 'inventory_entry_bound')
                if not recursive and not choose(entry.name):
                    continue
                info = entry.stat(follow_symlinks=False)
                require(not stat.S_ISLNK(info.st_mode), 'inventory_symlink_refused')
                relative = prefix + entry.name
                if stat.S_ISDIR(info.st_mode):
                    if recursive:
                        child = os.open(entry.name, FLAGS, dir_fd=fd)
                        try:
                            actual = os.fstat(child)
                            require((actual.st_dev, actual.st_ino) == (info.st_dev, info.st_ino),
                                    'inventory_directory_identity_changed')
                            scan(child, relative + '/', depth + 1)
                        finally:
                            os.close(child)
                elif stat.S_ISREG(info.st_mode):
                    if choose(entry.name):
                        result['files'] += 1
                        result['bytes'] += info.st_size
                        result['zero_byte_files'] += int(info.st_size == 0)
                        result['files_metadata'].append({'relative_path': relative,
                            'bytes': info.st_size, 'device': info.st_dev, 'inode': info.st_ino,
                            'mtime_ns': info.st_mtime_ns})
                else:
                    require(False, 'inventory_nonregular_entry_refused')
        after = os.fstat(fd)
        require((before.st_dev, before.st_ino, before.st_mtime_ns, before.st_ctime_ns) ==
                (after.st_dev, after.st_ino, after.st_mtime_ns, after.st_ctime_ns),
                'inventory_directory_changed_during_observation')

    scan(directory, '', 0)
    result['files_metadata'].sort(key=lambda x: x['relative_path'])
    return result


def execute():
    lane = read_receipt('docs/reports/s3-hardening/closeout-002/candidate-c6/resources.json')
    require(lane['status'] == 'passed' and all(c['exit_status'] == 0 for c in lane['commands']),
            'resource_lane_not_passed')
    require(lane['starting_sha'] == lane['ending_sha'] == CANDIDATE and
            lane['source_sha256'] == FINGERPRINT, 'resource_candidate_mismatch')
    for relative in PINS:
        read_pinned(relative)
    cleanup = read_receipt('docs/reports/s3-hardening/closeout-002/candidate-c6/terminal-cleanup.json')
    require(cleanup['status'] == 'passed' and cleanup['exit_status'] == 0, 'terminal_cleanup_failed')
    for relative in ['docs/reports/s3-hardening/closeout-002/candidate-c6/invocation.json',
                     'docs/reports/s3-hardening/closeout-002/candidate-c6/cache-prerequisite-retry/invocation.json']:
        driver = read_receipt(relative)
        require(isinstance(driver.get('exit_status'), int) and driver.get('ended_at'),
                'driver_not_terminal')
    require(os.readlink('/proc/self/ns/pid') == cleanup['verifier_pid_namespace'],
            'pid_namespace_changed')
    known = [x for x in cleanup['process_identity_checks'] if
             '/ess-hardening-resource-closeout002-c6-resources/observer-' in x['source']]
    require(len(known) == 2 and {x['role'] for x in known} == {'beam', 'observer'},
            'exact_resource_identities_missing')
    identities = []
    for item in known:
        expected = item['expected']
        observed = pid_identity(expected['pid'])
        require(observed is None or observed['start_ticks'] != expected['start_ticks'],
                'original_resource_identity_not_absent')
        identities.append({'role': item['role'], 'expected': expected, 'observed': observed,
                           'original_identity_absent': True})
    historical = [x for x in cleanup['disposable_root_inventory'] if
                  x['virtual_root'] == VIRTUAL and x['physical_root'] == str(PHYSICAL)]
    require(len(historical) == 1 and historical[0]['exists'] and not historical[0]['symlink'],
            'retained_root_provenance_missing')
    rootfd = anchored_directory(PHYSICAL)
    try:
        before = os.fstat(rootfd)
        require((before.st_dev, before.st_ino) ==
                (historical[0]['device'], historical[0]['inode']), 'retained_root_identity_changed')
        inventories = []
        selections = [('all_owned_tmp_files', ('data', 'tmp'), lambda name: True, True),
                      ('gateway_cache_tmp_suffix', ('data', 'resource-gateway', 'cache'),
                       lambda name: name.endswith('.tmp'), True),
                      ('inline_cache_stage_prefix', ('data',),
                       lambda name: name.startswith('.cache-stage-'), False)]
        for label, parts, choose, recursive in selections:
            try:
                fd = child_directory(rootfd, parts)
            except FileNotFoundError:
                inventories.append({'scope': label, 'relative_root': '/'.join(parts),
                    'state': 'absent_at_observation', 'files': 0, 'bytes': 0,
                    'zero_byte_files': 0, 'entries_visited': 0, 'directories_visited': 0,
                    'files_metadata': []})
                continue
            try:
                inventories.append({'scope': label, 'relative_root': '/'.join(parts),
                                    'state': 'observed', **inventory(fd, choose, recursive)})
            finally:
                os.close(fd)
        after = os.fstat(rootfd)
        require((before.st_dev, before.st_ino, before.st_mtime_ns, before.st_ctime_ns) ==
                (after.st_dev, after.st_ino, after.st_mtime_ns, after.st_ctime_ns),
                'retained_root_changed_during_observation')
    finally:
        os.close(rootfd)
    require(all(x['files'] == 0 for x in inventories), 'terminal_stage_files_present')
    return {'status': 'passed', 'root_device': before.st_dev, 'root_inode': before.st_ino,
            'identity_checks': identities, 'inventories': inventories,
            'terminal_stage_files': sum(x['files'] for x in inventories),
            'terminal_stage_bytes': sum(x['bytes'] for x in inventories)}


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--execute', action='store_true')
    parser.add_argument('--output', type=Path)
    args = parser.parse_args()
    if not args.execute:
        print(json.dumps({'execute': False, 'physical_root': str(PHYSICAL),
            'scope': ['data/tmp/** metadata', 'data/resource-gateway/cache/**/*.tmp metadata',
                      'data/.cache-stage-* metadata'], 'maximum_entries_per_scope': MAX_ENTRIES,
            'regular_fixture_files_opened': 0, 'symlinks_followed': False}))
        return 1
    require(args.output is not None and args.output.parent == EXTERNAL,
            'external_output_path_required')
    require(not args.output.exists(), 'refuse_output_overwrite')
    result = {'task': 'ESS-S3-HARDENING-CLOSEOUT-002', 'candidate_sha': CANDIDATE,
        'source_sha256': FINGERPRINT, 'acceptance': False,
        'purpose': 'supplemental read-only terminal facts for the original C6 resource root',
        'recorded_at_utc': datetime.datetime.now(datetime.timezone.utc).isoformat(),
        'virtual_root': VIRTUAL, 'physical_root': str(PHYSICAL), 'provenance': PINS,
        'source_provenance': {'method': 'git show candidate:path; current source never used',
                              'commit': CANDIDATE, 'paths': sorted(SOURCE_FROM_GIT)},
        'limits': ['No regular fixture files or credentials read; metadata only.',
                   'Exact original BEAM/observer identities only; no other descendants verified.',
                   'No per-case historical file counts reconstructed; original post-idle byte samples remain separate.',
                   'Cache final objects excluded; only owned tmp, cache .tmp and .cache-stage- stages counted.',
                   'No resources workload, A30/full aggregate or release pass is inferred.',
                   'No fixtures, containers, connections or processes changed or removed.']}
    try:
        result.update(execute())
    except Exception as error:
        result.update(status='failed', failure_type=type(error).__name__,
                      failure_code=str(error) if isinstance(error, RuntimeError) else 'read_only_observation_failed')
    result['exit_status'] = 0 if result['status'] == 'passed' else 1
    with args.output.open('x') as output:
        json.dump(result, output, indent=2)
        output.write('\n')
    print(json.dumps({'path': str(args.output), 'status': result['status'],
                      'sha256': hashlib.sha256(args.output.read_bytes()).hexdigest()}))
    return result['exit_status']


if __name__ == '__main__':
    raise SystemExit(main())
