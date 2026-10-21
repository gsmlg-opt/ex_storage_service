#!/usr/bin/env python3
"""Explicit, receipt-bound C7 disposal. Never promotes acceptance or kills processes.

No default action. The coordinator must finish independent evidence/privacy archives
before invoking --execute. The additional archive here contains private metadata only.
"""
import argparse
import contextlib
import hashlib
import json
import os
from pathlib import Path
import re
import runpy
import shutil
import stat
import subprocess
import tarfile
from datetime import datetime, timezone

REPO = Path('/home/gao/Workspace/gsmlg-opt/ex_storage_service')
RUNNER = Path('/fast/build/ess-closeout-c7-acceptance')
TMP = RUNNER / 'tmp'
BASE = REPO / 'docs/reports/s3-hardening/closeout-002/candidate-c7'
SHA = 'fcc02e148ae31f00022d563ebce2b039be5d32dd'
FINGERPRINT = 'ee754b0345b3198e84abd78e898b121e966787c1faa563642097ca3151653913'
CLEANUP_HASH = '9fdf14e6ba3369032e613fd62972e0b2db85d5486373a852b2c76cc43d8e40ee'
ROOT_NAMES = frozenset(('ess-hardening-backlog-closeout002-c7-resilience-backlog',
    'ess-hardening-crash-closeout002-c7-crashes', 'ess-hardening-fault-closeout002-c7-faults',
    'ess-hardening-resource-closeout002-c7-resources', 'ess-hardening-restore-1xRzf8Uf',
    'ess-hardening-restore-szGeNHMX', 'ess-hardening-retained-7gePoRIW',
    'ess-s3-hardening-closeout002-c7-gateway', 'ess-s3-hardening-cluster-1jFD61tA',
    'ess-s3-hardening-cluster-80Yyu0dG', 'ess-s3-hardening-cluster-MQ8AmX6A',
    'ess-s3-hardening-cluster-YdJzKXun', 'ess-s3-hardening-cluster-ceO2GjHQ',
    'ess-s3-hardening-standalone-P0360CTt'))
CONTAINER_PATTERNS = ('ess-gateway-[a-f0-9]{12}', 'ess-hardening-resource-[a-f0-9]{12}',
    'ess-hardening-fault-[a-f0-9]{12}', 'ess-release-[a-f0-9]{32}')
MAX_INPUT = 16 * 1024 * 1024
MAX_FILE = 256 * 1024 * 1024
MAX_TOTAL = 1024 * 1024 * 1024
MAX_FILES = 100000
MAX_ENTRIES = 1000000
DIR_FLAGS = os.O_RDONLY | os.O_DIRECTORY | os.O_NOFOLLOW


def require(condition, message):
    if not condition:
        raise ValueError(message)


@contextlib.contextmanager
def directory(path):
    path = Path(path)
    require(path.is_absolute() and '..' not in path.parts, 'absolute non-traversing path required')
    fd = os.open('/', DIR_FLAGS)
    try:
        for part in path.parts[1:]:
            new = os.open(part, DIR_FLAGS, dir_fd=fd)
            os.close(fd)
            fd = new
        yield fd
    finally:
        os.close(fd)


def read_regular(path, cap=MAX_INPUT):
    path = Path(path)
    with directory(path.parent) as parent:
        fd = os.open(path.name, os.O_RDONLY | os.O_NOFOLLOW | os.O_NONBLOCK, dir_fd=parent)
        with os.fdopen(fd, 'rb') as stream:
            require(stat.S_ISREG(os.fstat(stream.fileno()).st_mode), 'regular input required')
            raw = stream.read(cap + 1)
            require(len(raw) <= cap, 'input exceeds cap')
            return raw


def load_inputs():
    raw = read_regular(BASE / 'terminal-cleanup.json')
    require(hashlib.sha256(raw).hexdigest() == CLEANUP_HASH, 'terminal cleanup receipt changed')
    cleanup = json.loads(raw)
    freeze = json.loads(read_regular(BASE / 'freeze.json'))
    invocation = json.loads(read_regular(BASE / 'invocation.json'))
    import terminal_evidence
    terminal_evidence.validate_invocation(invocation, freeze)
    require(freeze['candidate_sha'] == SHA and freeze['source_sha256'] == FINGERPRINT,
            'unexpected freeze')
    api = runpy.run_path(str(REPO / 'e2e/hardening_acceptance.py'))
    require(api['sha']() == SHA and api['source_fingerprint']() == FINGERPRINT,
            'current checkout differs from frozen candidate')
    require(cleanup['status'] == 'passed' and cleanup['exit_status'] == 0
            and cleanup['acceptance'] is False and cleanup['failures'] == []
            and cleanup['candidate_sha'] == SHA and cleanup['source_sha256'] == FINGERPRINT,
            'cleanup prerequisite not passed')
    require(cleanup['current_executable_fingerprint_verified'] is True,
            'cleanup source prerequisite missing')
    require(cleanup['terminal_drivers'] == {str(BASE): {
        'status': invocation['status'], 'exit_status': invocation['exit_status']}},
        'terminal driver mismatch')
    require(len(cleanup['lane_validation']) == 11 and all(
        lane['complete'] is True and not lane['unexecuted_command_indices']
        for lane in cleanup['lane_validation'].values()), 'nonterminal lane')
    return cleanup


def mount_points():
    text = read_regular(f'/proc/{os.getpid()}/mountinfo', 8 * 1024 * 1024).decode()
    return [Path(re.sub(r'\\([0-7]{3})', lambda m: chr(int(m[1], 8)), line.split()[4]))
            for line in text.splitlines()]


def reject_mounts(rows, mounts):
    for row in rows:
        root = Path(row['physical_root'])
        require(not any(m == root or m.is_relative_to(root) for m in mounts),
                'mount at or below owned root: ' + root.name)


def root_identity(parent, row):
    path = Path(row['physical_root'])
    require(path.parent == TMP and path.name in ROOT_NAMES, 'root outside exact allowlist')
    require(row['exists'] is True and row['symlink'] is False, 'invalid original root')
    require(type(row['device']) is int and type(row['inode']) is int, 'missing original identity')
    info = os.stat(path.name, dir_fd=parent, follow_symlinks=False)
    require(stat.S_ISDIR(info.st_mode) and (info.st_dev, info.st_ino) ==
            (row['device'], row['inode']), 'root identity changed: ' + path.name)
    fd = os.open(path.name, DIR_FLAGS, dir_fd=parent)
    try:
        opened = os.fstat(fd)
        require((opened.st_dev, opened.st_ino) == (info.st_dev, info.st_ino),
                'root changed while opening')
    finally:
        os.close(fd)


def validate_roots(cleanup, parent, mounts):
    rows = cleanup['disposable_root_inventory']
    require(len(rows) == 14 and len({r['physical_root'] for r in rows}) == 14
            and {Path(r['physical_root']).name for r in rows} == ROOT_NAMES,
            'exact fourteen-root inventory required')
    reject_mounts(rows, mounts)
    for row in rows:
        root_identity(parent, row)
    return rows


def pid_identity(pid):
    try:
        fields = read_regular(f'/proc/{pid}/stat', 65536).decode().rsplit(')', 1)[1].split()
        return dict(pid=pid, start_ticks=int(fields[19]), state=fields[0])
    except (FileNotFoundError, ProcessLookupError):
        return None


def query(argv):
    result = subprocess.run(argv, capture_output=True, text=True, timeout=15, check=False)
    require(result.returncode == 0 and len(result.stdout) <= 1024 * 1024,
            'fresh read-only query failed: ' + argv[0])
    return result.stdout


def fresh_probes(cleanup):
    require(os.readlink('/proc/self/ns/pid') == cleanup['verifier_pid_namespace'],
            'PID namespace differs from terminal observation')
    observations = cleanup['process_identity_checks']
    require(len(observations) == 20 and all(o['original_identity_absent'] is True
            and o['original_identity_running'] is False and o['zombie'] is False
            for o in observations), 'twenty stopped observations required')
    identities = []
    for observation in observations:
        expected = observation['expected']
        require(type(expected['pid']) is int and expected['pid'] > 0
                and type(expected['start_ticks']) is int and expected['start_ticks'] > 0,
                'invalid recorded PID identity')
        current = pid_identity(expected['pid'])
        require(current is None or current['start_ticks'] != expected['start_ticks'],
                'recorded process identity remains, including zombie')
        identities.append(dict(expected=expected, observed=current, original_identity_absent=True))
    require(cleanup['epmd_query_exit_status'] == 0 and not cleanup['matching_known_erlang_names'],
            'original node check failed')
    names = set(cleanup['exact_known_erlang_names'])
    registered = set(re.findall(r'^name ([A-Za-z0-9_-]+) at port [0-9]+$',
                               query(['epmd', '-names']), re.M))
    require(not names.intersection(registered), 'known Erlang node remains')
    ports = {int(p): 0 for p in cleanup['exact_known_listener_ports']}
    require(ports and all(v['listeners'] == 0 for v in cleanup['exact_known_listener_ports'].values()),
            'original listener check failed')
    for file in (f'/proc/{os.getpid()}/net/tcp', f'/proc/{os.getpid()}/net/tcp6'):
        for line in read_regular(file, 8 * 1024 * 1024).decode().splitlines()[1:]:
            fields = line.split()
            port = int(fields[1].split(':')[1], 16)
            if port in ports and fields[3] == '0A':
                ports[port] += 1
    require(not any(ports.values()), 'known fixture listener occupied')
    containers = cleanup['fixture_container_namespaces']
    require({c['fixture_name_pattern'] for c in containers} == set(CONTAINER_PATTERNS)
            and all(c['absent'] is True and c['query_exit_status'] == 0
                    and not c['matching_names'] for c in containers), 'original container check failed')
    for pattern in CONTAINER_PATTERNS:
        output = query(['docker', 'ps', '--all', '--filter', 'name=' + pattern.split('[')[0],
                        '--format', '{{.Names}}'])
        require(not any(re.fullmatch(pattern, name) for name in output.splitlines()),
                'known fixture container namespace occupied')
    return dict(status='passed', process_observations=identities, known_nodes_absent=True,
                listener_counts=ports, fixture_container_namespaces_absent=True)


class HashingReader:
    def __init__(self, stream):
        self.stream, self.digest = stream, hashlib.sha256()

    def read(self, size):
        data = self.stream.read(size)
        self.digest.update(data)
        return data


def metadata_files(fd, prefix, budget, depth=0):
    require(depth <= 128, 'metadata directory depth cap exceeded')
    device = os.fstat(fd).st_dev
    with os.scandir(fd) as entries:
        for entry in entries:
            budget['entries'] += 1
            require(budget['entries'] <= MAX_ENTRIES, 'directory entry cap exceeded')
            info = entry.stat(follow_symlinks=False)
            require(info.st_dev == device, 'foreign device inside fixture')
            name = prefix + '/' + entry.name
            if stat.S_ISLNK(info.st_mode):
                budget['skipped_symlinks'] += 1
            elif stat.S_ISDIR(info.st_mode):
                child = os.open(entry.name, DIR_FLAGS, dir_fd=fd)
                try:
                    opened = os.fstat(child)
                    require((opened.st_dev, opened.st_ino) == (info.st_dev, info.st_ino),
                            'directory changed while opening')
                    yield from metadata_files(child, name, budget, depth + 1)
                finally:
                    os.close(child)
            elif stat.S_ISREG(info.st_mode) and entry.name.endswith(('.log', '.json', '.jsonl')):
                budget['files'] += 1
                budget['bytes'] += info.st_size
                require(info.st_size <= MAX_FILE and budget['bytes'] <= MAX_TOTAL
                        and budget['files'] <= MAX_FILES, 'metadata archive cap exceeded')
                file_fd = os.open(entry.name, os.O_RDONLY | os.O_NOFOLLOW | os.O_NONBLOCK, dir_fd=fd)
                with os.fdopen(file_fd, 'rb') as stream:
                    opened = os.fstat(stream.fileno())
                    require(stat.S_ISREG(opened.st_mode) and (opened.st_dev, opened.st_ino,
                        opened.st_size) == (info.st_dev, info.st_ino, info.st_size),
                        'metadata identity changed')
                    yield name, stream, opened
                    after = os.fstat(stream.fileno())
                    require((after.st_size, after.st_mtime_ns, after.st_ctime_ns) ==
                        (opened.st_size, opened.st_mtime_ns, opened.st_ctime_ns)
                        and not stream.read(1), 'metadata changed during archive')


def verify_archive(stream, manifest):
    stream.seek(0)
    expected = {m['name']: m for m in manifest}
    seen = set()
    with tarfile.open(fileobj=stream, mode='r|') as archive:
        for member in archive:
            require(member.isreg() and member.name in expected and member.name not in seen,
                    'unexpected or nonregular archive member')
            record = expected[member.name]
            require(member.size == record['bytes'], 'archive member size mismatch')
            digest = hashlib.sha256()
            with archive.extractfile(member) as source:
                while chunk := source.read(1024 * 1024):
                    digest.update(chunk)
            require(digest.hexdigest() == record['sha256'], 'archive member hash mismatch')
            seen.add(member.name)
    require(seen == set(expected), 'archive member missing')


def archive_metadata(parent, rows, archive_fd):
    budget = dict(entries=0, files=0, bytes=0, skipped_symlinks=0)
    manifest = []
    with os.fdopen(os.dup(archive_fd), 'r+b') as stream:
        with tarfile.open(fileobj=stream, mode='w') as archive:
            for row in rows:
                root_identity(parent, row)
                name = Path(row['physical_root']).name
                fd = os.open(name, DIR_FLAGS, dir_fd=parent)
                try:
                    for member, source, info in metadata_files(fd, name, budget):
                        header = tarfile.TarInfo(member)
                        header.size, header.mode = info.st_size, 0o600
                        reader = HashingReader(source)
                        archive.addfile(header, reader)
                        manifest.append(dict(name=member, bytes=info.st_size,
                                             sha256=reader.digest.hexdigest()))
                finally:
                    os.close(fd)
        stream.flush()
        os.fsync(stream.fileno())
        verify_archive(stream, manifest)
        stream.seek(0)
        digest = hashlib.sha256()
        while chunk := stream.read(1024 * 1024):
            digest.update(chunk)
    return dict(status='verified', sha256=digest.hexdigest(), manifest=manifest, **budget)


def private_outputs(archive, receipt):
    archive, receipt = Path(archive), Path(receipt)
    require(archive.parent == receipt.parent and archive != receipt
            and archive.parent.parent == RUNNER and archive.parent != TMP
            and archive.is_absolute() and '..' not in archive.parts,
            'outputs must share one private immediate runner subdirectory outside tmp')
    with directory(RUNNER) as runner:
        try:
            os.mkdir(archive.parent.name, mode=0o700, dir_fd=runner)
        except FileExistsError:
            pass
    with directory(archive.parent) as parent:
        info = os.fstat(parent)
        require(info.st_uid == os.getuid() and stat.S_IMODE(info.st_mode) == 0o700,
                'archive parent must be owned mode700')
        for path in (archive, receipt):
            try:
                os.stat(path.name, dir_fd=parent, follow_symlinks=False)
            except FileNotFoundError:
                continue
            raise ValueError('refusing to overwrite output: ' + path.name)
        receipt_fd = os.open(receipt.name, os.O_WRONLY | os.O_CREAT | os.O_EXCL | os.O_NOFOLLOW,
                             0o600, dir_fd=parent)
        try:
            archive_fd = os.open(archive.name, os.O_RDWR | os.O_CREAT | os.O_EXCL | os.O_NOFOLLOW,
                                 0o600, dir_fd=parent)
        except BaseException:
            os.close(receipt_fd)
            raise
        os.fchmod(archive_fd, 0o600)
        os.fchmod(receipt_fd, 0o600)
    return archive_fd, receipt_fd


def dispose(cleanup, archive, receipt, *, execute=False):
    require(execute is True, 'explicit --execute required; no fixture scanned or removed')
    require(shutil.rmtree.avoids_symlink_attacks, 'fd-relative symlink-resistant rmtree unavailable')
    result = dict(task='ESS-S3-HARDENING-CLOSEOUT-002', candidate_sha=SHA,
        source_sha256=FINGERPRINT, acceptance=False, status='failed', removed_roots=[],
        attempted_roots=[], archive=None, failures=[],
        scope='Only exact14receipt-bound C7 private roots; no acceptance promotion or process killing.',
        limits=['Metadata-only private tar;regular .log/.json/.jsonl;symlinks never followed.',
            'Per-file256MiB,total1GiB,100000metadata files,1000000visited entries,depth128.',
            'Original observer/PID namespace limitations retained;unrecorded descendants not certified.',
            'Requires exclusive stopped roots;concurrent privileged mount/tree mutation is not supported.',
            'No C5/C6,unreferenced runner roots,original caches,containers or production data removed.'])
    archive_fd = receipt_fd = None
    try:
        archive_fd, receipt_fd = private_outputs(archive, receipt)
        result['archive'] = dict(path=str(archive), status='unverified')
        with directory(TMP) as parent:
            rows = validate_roots(cleanup, parent, mount_points())
            result['pre_archive_probes'] = fresh_probes(cleanup)
            result['archive'] = archive_metadata(parent, rows, archive_fd)
            result['archive']['path'] = str(archive)
            # Nothing is removed until the entire metadata archive verifies and ALL
            # identities/probes are checked again after the potentially long copy.
            require(load_inputs() == cleanup, 'terminal inputs changed during archive')
            validate_roots(cleanup, parent, mount_points())
            result['pre_delete_probes'] = fresh_probes(cleanup)
            for row in rows:
                reject_mounts(rows, mount_points())
                root_identity(parent, row)
                path = row['physical_root']
                result['attempted_roots'].append(path)
                shutil.rmtree(Path(path).name, dir_fd=parent)
                result['removed_roots'].append(path)
            result['status'] = 'disposed'
    except Exception as error:
        result['failures'].append(type(error).__name__ + ': ' + str(error))
    finally:
        result['finished_at'] = datetime.now(timezone.utc).isoformat()
        result['exit_status'] = 0 if result['status'] == 'disposed' else 1
        if archive_fd is not None:
            os.close(archive_fd)
        if receipt_fd is not None:
            with os.fdopen(receipt_fd, 'w') as stream:
                json.dump(result, stream, indent=2)
                stream.write('\n')
                stream.flush()
                os.fsync(stream.fileno())
    return result


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--execute', action='store_true')
    parser.add_argument('--archive', type=Path, required=True)
    parser.add_argument('--receipt', type=Path, required=True)
    args = parser.parse_args()
    require(args.execute, 'explicit --execute required')
    result = dispose(load_inputs(), args.archive, args.receipt, execute=True)
    print(json.dumps({k: result[k] for k in ('status', 'exit_status', 'acceptance',
                                            'removed_roots', 'failures')}))
    return result['exit_status']


if __name__ == '__main__':
    raise SystemExit(main())
