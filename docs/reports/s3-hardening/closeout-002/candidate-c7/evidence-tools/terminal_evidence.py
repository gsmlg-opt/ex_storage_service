"""External C7 terminal receipt validation; no service/process/sample probes.

All validators raise TerminalEvidenceError. Blocked lane prefixes remain negative
execution evidence; successful independent index/resource children stay visible.
"""
import contextlib
import datetime
import json
import math
import os
from pathlib import Path
import stat
from types import SimpleNamespace

CANDIDATE_SHA = 'fcc02e148ae31f00022d563ebce2b039be5d32dd'
SOURCE_FINGERPRINT = 'ee754b0345b3198e84abd78e898b121e966787c1faa563642097ca3151653913'
PROFILES = ('correctness', 'standalone', 'gateway', 'cluster', 'index', 'resources',
            'faults', 'crashes', 'resilience', 'restore', 'release')
MAX_METADATA = 16 * 1024 * 1024


class TerminalEvidenceError(RuntimeError):
    pass


def require(condition, message):
    if not condition:
        raise TerminalEvidenceError(message)


def _path(path, allowed_roots):
    path = Path(path)
    roots = [Path(root) for root in allowed_roots]
    require(path.is_absolute() and '..' not in path.parts, 'absolute normalized input required')
    require(roots and all(root.is_absolute() and '..' not in root.parts for root in roots),
            'explicit absolute input boundaries required')
    require(any(path.is_relative_to(root) for root in roots), 'input outside allowed boundary')
    require(path.name != 'credentials.json' and '.jsonl' not in path.suffixes,
            'credential and JSONL inputs forbidden')
    return path


@contextlib.contextmanager
def _open_regular(path, allowed_roots):
    path = _path(path, allowed_roots)
    directory = descriptor = None
    try:
        flags = os.O_RDONLY | os.O_DIRECTORY | os.O_NOFOLLOW
        directory = os.open('/', flags)
        for component in path.parts[1:-1]:
            child = os.open(component, flags, dir_fd=directory)
            os.close(directory)
            directory = child
        descriptor = os.open(path.name, os.O_RDONLY | os.O_NOFOLLOW | os.O_NONBLOCK,
                             dir_fd=directory)
        before = os.fstat(descriptor)
        require(stat.S_ISREG(before.st_mode), 'non-regular input refused')
        yield descriptor, before
    except OSError as error:
        raise TerminalEvidenceError('missing, inaccessible or symlink input refused') from error
    finally:
        if descriptor is not None:
            os.close(descriptor)
        if directory is not None:
            os.close(directory)


def safe_regular_path(path, allowed_roots):
    """Validate current path only. Use bounded_read for the actual anchored read."""
    path = _path(path, allowed_roots)
    with _open_regular(path, allowed_roots):
        pass
    return path


def safe_directory_path(path, allowed_roots):
    path = _path(path, allowed_roots)
    descriptor = None
    try:
        flags = os.O_RDONLY | os.O_DIRECTORY | os.O_NOFOLLOW
        descriptor = os.open('/', flags)
        for component in path.parts[1:]:
            child = os.open(component, flags, dir_fd=descriptor)
            os.close(descriptor)
            descriptor = child
    except OSError as error:
        raise TerminalEvidenceError('missing, inaccessible or symlink directory refused') from error
    finally:
        if descriptor is not None:
            os.close(descriptor)
    return path


def bounded_read(path, allowed_roots, max_bytes=MAX_METADATA):
    """Reject symlinks/private payloads and bound actual bytes, including growth."""
    require(type(max_bytes) is int and max_bytes >= 0, 'invalid read bound')
    with _open_regular(path, allowed_roots) as (descriptor, before):
        require(before.st_size <= max_bytes, 'metadata input exceeds read bound')
        chunks, size = [], 0
        while True:
            chunk = os.read(descriptor, min(65536, max_bytes + 1 - size))
            if not chunk:
                break
            size += len(chunk)
            require(size <= max_bytes, 'actual metadata bytes exceed read bound')
            chunks.append(chunk)
        after = os.fstat(descriptor)
        fields = ('st_dev', 'st_ino', 'st_size', 'st_mtime_ns', 'st_ctime_ns')
        require(all(getattr(before, name) == getattr(after, name) for name in fields),
                'metadata changed during observation')
        return b''.join(chunks)


def read_json(path, allowed_roots):
    try:
        return json.loads(bounded_read(path, allowed_roots))
    except (ValueError, UnicodeError) as error:
        raise TerminalEvidenceError('invalid metadata JSON') from error


def _time(value):
    require(isinstance(value, str) and value, 'terminal timestamp required')
    try:
        parsed = datetime.datetime.fromisoformat(value)
    except ValueError as error:
        raise TerminalEvidenceError('invalid terminal timestamp') from error
    require(parsed.tzinfo is not None, 'timezone-aware timestamp required')
    return parsed


def _duration(value):
    require(type(value) in (int, float) and math.isfinite(value) and value >= 0,
            'finite nonnegative command duration required')


def validate_freeze(freeze):
    require(isinstance(freeze, dict) and freeze.get('candidate_sha') == CANDIDATE_SHA
            and freeze.get('source_sha256') == SOURCE_FINGERPRINT, 'unexpected C7 freeze identity')
    require(isinstance(freeze.get('authoritative_command'), list)
            and isinstance(freeze.get('namespace_command'), list), 'frozen command inventory required')
    _time(freeze.get('frozen_at'))


def validate_invocation(record, freeze):
    """Validate the actual terminal C7 driver, including negative acceptance exit."""
    validate_freeze(freeze)
    require(isinstance(record, dict), 'invocation object required')
    require(record.get('candidate_sha') == freeze['candidate_sha']
            and record.get('source_sha256') == freeze['source_sha256']
            and record.get('ending_sha') == freeze['candidate_sha']
            and record.get('ending_source_sha256') == freeze['source_sha256'],
            'invocation candidate/source mismatch')
    status, code = record.get('status'), record.get('exit_status')
    require(status in ('passed', 'failed') and type(code) is int,
            'integer terminal invocation exit required')
    require((status == 'passed') == (code == 0), 'invocation status/exit mismatch')
    require(type(record.get('child_exit_status')) is int and record['child_exit_status'] == code,
            'terminal child exit mismatch')
    require(record.get('freeze_failures') == [] and record.get('resource_envelope_unchanged') is True
            and record.get('runner_inputs_unchanged') is True, 'invocation frozen inputs changed')
    require(record.get('command') == freeze['namespace_command'] + ['--'] + freeze['authoritative_command'],
            'invocation command differs from freeze')
    started, ended = _time(record.get('started_at')), _time(record.get('ended_at'))
    require(_time(freeze['frozen_at']) <= started <= ended, 'invocation time boundary mismatch')
    _duration(record.get('duration_seconds'))
    return record


def expected_commands(profile, freeze, api, base):
    """Use the frozen producer's commands API and run_profile prepend/log policy."""
    validate_freeze(freeze)
    require(tuple(api.get('PROFILES', ())) == PROFILES and profile in PROFILES,
            'unexpected application profile inventory')
    repo, base = Path(api['REPO']), Path(base)
    require(base.is_absolute() and base.is_relative_to(repo), 'candidate base outside repository')
    argv = freeze['authoritative_command']
    require(len(argv) >= 3 and argv[1:3] == ['e2e/hardening_acceptance.py', 'run'],
            'unexpected authoritative entry point')
    def option(flag):
        require(argv.count(flag) == 1 and argv.index(flag) + 1 < len(argv), 'frozen option missing or repeated')
        return argv[argv.index(flag) + 1]
    require(option('--profile') == 'all', 'full authoritative profile required')
    output = Path(option('--output'))
    require((output if output.is_absolute() else repo / output) == base, 'frozen output boundary mismatch')
    args = SimpleNamespace(python=option('--python'), mc=option('--mc'), output=base,
                           label=option('--label'), timeout=10800)
    steps = list(api['commands'](profile, args, args.label + '-' + profile))
    if profile not in ('correctness', 'release'):
        steps.insert(0, ('prod', ['mix', 'compile', '--force', '--warnings-as-errors']))
    return [(mode, command, str((base / f'{profile}-{index}.log').relative_to(repo)))
            for index, (mode, command) in enumerate(steps)]


def validate_lane(record, profile, freeze, api, base):
    """Return executed/failed/unexecuted indices; never promote a blocked prefix."""
    steps = expected_commands(profile, freeze, api, base)
    require(isinstance(record, dict) and record.get('profile') == profile, 'lane profile mismatch')
    require(record.get('starting_sha') == record.get('ending_sha') == freeze['candidate_sha']
            and record.get('source_sha256') == freeze['source_sha256'], 'lane candidate/source mismatch')
    require(_time(record.get('started_at')) >= _time(freeze['frozen_at']), 'lane predates candidate freeze')
    status = record.get('status')
    require(status in ('passed', 'blocked'), 'terminal lane state required')
    commands = record.get('commands')
    require(isinstance(commands, list) and len(commands) <= len(steps), 'invalid command prefix inventory')
    failures = []
    for index, command in enumerate(commands):
        require(isinstance(command, dict), 'command record required')
        mode, argv, log = steps[index]
        require(command.get('argv') == argv and command.get('mix_env') == mode
                and command.get('log') == log, 'command argv/environment/log inventory mismatch')
        code = command.get('exit_status')
        require(type(code) is int, 'true integer terminal child exit required')
        _duration(command.get('duration_seconds'))
        safe_regular_path(Path(api['REPO']) / log, [Path(base)])
        if code:
            failures.append(index)
            require(index == len(commands) - 1 or (profile in ('index', 'resources') and index > 0),
                    'producer cannot continue after this failed command')
    complete = len(commands) == len(steps)
    if status == 'passed':
        require(complete and not failures, 'passed lane requires complete successful commands')
    else:
        require(isinstance(record.get('reason'), str) and record['reason'], 'blocked lane reason required')
    return dict(profile=profile, status=status, complete=complete,
                expected_command_count=len(steps), executed_command_count=len(commands),
                successful_command_indices=[i for i, command in enumerate(commands) if command['exit_status'] == 0],
                failed_command_indices=failures,
                unexecuted_command_indices=list(range(len(commands), len(steps))))


def validate_all_lanes(base, freeze, api):
    """Require all eleven source-bound terminal records, including blocked ones."""
    require(tuple(api.get('PROFILES', ())) == PROFILES, 'all eleven application lanes required')
    result = {}
    for profile in PROFILES:
        record = read_json(Path(base) / (profile + '.json'), [Path(base)])
        validate_lane(record, profile, freeze, api, base)
        result[profile] = record
    return result
