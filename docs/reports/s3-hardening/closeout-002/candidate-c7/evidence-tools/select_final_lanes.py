"""Select validated byte-identical terminal C7 receipts; preserve blocked prefixes."""
import hashlib
import json
from pathlib import Path
import runpy

import terminal_evidence as helper

REPO = Path('/home/gao/Workspace/gsmlg-opt/ex_storage_service')
BASE = REPO / 'docs/reports/s3-hardening/closeout-002/candidate-c7'
OUT = BASE / 'final-lanes'
API = runpy.run_path(str(REPO / 'e2e/hardening_acceptance.py'))


def relative(path):
    return str(path.relative_to(REPO))


def info(path, raw):
    return dict(path=relative(path), bytes=len(raw), sha256=hashlib.sha256(raw).hexdigest())


def main():
    freeze = helper.read_json(BASE / 'freeze.json', [BASE])
    helper.validate_freeze(freeze)
    helper.require(API['sha']() == freeze['candidate_sha'], 'current candidate SHA differs')
    helper.require(API['source_fingerprint']() == freeze['source_sha256'], 'current executable fingerprint differs')
    invocation_raw = helper.bounded_read(BASE / 'invocation.json', [BASE])
    invocation = json.loads(invocation_raw)
    helper.validate_invocation(invocation, freeze)
    helper.validate_all_lanes(BASE, freeze, API)
    choices = []
    for profile in API['PROFILES']:
        path = BASE / (profile + '.json')
        raw = helper.bounded_read(path, [BASE])
        record = json.loads(raw)
        validation = helper.validate_lane(record, profile, freeze, API, BASE)
        choices.append((profile, path, raw, record, validation))
    OUT.mkdir(exist_ok=False)
    catalog = dict(schema=1, task='ESS-S3-HARDENING-CLOSEOUT-002',
        candidate_sha=freeze['candidate_sha'], source_sha256=freeze['source_sha256'],
        policy='All selected lane bytes are validated snapshots of the single actual terminal C7 authoritative run. Passed lanes require the complete producer command inventory and zero exits; blocked prefixes and their unexecuted commands remain explicit negative evidence. This selection does not certify semantic clauses or aggregate acceptance.',
        terminal_invocations=[info(BASE / 'invocation.json', invocation_raw) |
                              dict(status=invocation['status'], exit_status=invocation['exit_status'])],
        selections=[])
    for profile, source, raw, record, validation in choices:
        helper.require(helper.bounded_read(source, [BASE]) == raw, 'source lane changed after validation')
        destination = OUT / (profile + '.json')
        with destination.open('xb') as stream:
            stream.write(raw)
        helper.require(helper.bounded_read(destination, [OUT]) == raw, 'selected bytes differ')
        source_info = info(source, raw)
        catalog['selections'].append(dict(profile=profile, original=source_info,
            original_status=record['status'], selected_source=source_info,
            selected=info(destination, raw), selected_status=record['status'],
            actual_command_exit_statuses=[c['exit_status'] for c in record['commands']],
            validation=validation, used_retry=False))
    for _, source, raw, _, _ in choices:
        helper.require(helper.bounded_read(source, [BASE]) == raw, 'source lane changed during selection')
    helper.require(helper.bounded_read(BASE / 'invocation.json', [BASE]) == invocation_raw,
                   'driver receipt changed during selection')
    with (OUT / 'provenance.json').open('x') as stream:
        json.dump(catalog, stream, indent=2)
        stream.write('\n')
    print(json.dumps({row['profile']: row['selected_status'] for row in catalog['selections']}, indent=2))


if __name__ == '__main__':
    main()
