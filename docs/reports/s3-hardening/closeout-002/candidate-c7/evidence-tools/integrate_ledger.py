"""Reconcile actual terminal C7 clause audits without importing historical passes."""
import collections
import copy
import hashlib
import json
from pathlib import Path
import re
import runpy
import terminal_evidence

REPO = Path('/home/gao/Workspace/gsmlg-opt/ex_storage_service')
BASE = REPO / 'docs/reports/s3-hardening/closeout-002/candidate-c7'
LEDGER = REPO / 'docs/plans/s3-hardening-results.json'
API = runpy.run_path(str(REPO / 'e2e/hardening_acceptance.py'))


def digest(path):
    return hashlib.sha256(path.read_bytes()).hexdigest()


def relative(path):
    return str(path.relative_to(REPO))


def safe_path(value):
    path = terminal_evidence.safe_regular_path(REPO / value, [REPO])
    assert path.stat().st_size, value
    return path


def normalize(item):
    evidence = copy.deepcopy(item)
    if 'receipt' in evidence:
        evidence['path'] = evidence['receipt']
    if 'path' not in evidence and 'source' in evidence:
        match = re.fullmatch(r'(.*?):([0-9]+)(?:-([0-9]+))?', evidence['source'])
        evidence['path'] = match.group(1) if match else evidence['source']
        evidence['evidence_kind'] = 'source semantics; execution requires separate current-candidate receipt'
        if match:
            lines = safe_path(evidence['path']).read_text().splitlines()
            assert 1 <= int(match.group(2)) <= int(match.group(3) or match.group(2)) <= len(lines)
    path = safe_path(evidence['path'])
    evidence['path'] = relative(path)
    if evidence.get('sha256'):
        assert evidence['sha256'] == digest(path), evidence['path']
    else:
        evidence['sha256'] = digest(path)
    if evidence.get('json_pointer'):
        value = json.loads(path.read_text())
        assert evidence['json_pointer'].startswith('/')
        for token in evidence['json_pointer'][1:].split('/'):
            token = token.replace('~1', '/').replace('~0', '~')
            value = value[int(token)] if isinstance(value, list) else value[token]
    return evidence


def selected_output(item, mode, index):
    """Only outputs from this selected command can substantiate its execution."""
    path = item['path']
    if path == relative(BASE / f'{mode}-{index}.log'):
        return True
    prefixes = {
        ('correctness', 2): [relative(BASE / 'suite-selection') + '/'],
        ('gateway', 1): [relative(BASE / 'gateway-artifacts') + '/'],
        ('index', 2): ['docs/reports/s3-hardening/closeout002-c7-index-cluster/'],
        ('resources', 1): ['docs/reports/s3-hardening/closeout002-c7-resources/'],
        ('resources', 2): ['docs/reports/s3-hardening/closeout002-c7-resources-cluster/'],
        ('faults', 1): ['docs/reports/s3-hardening/closeout002-c7-faults/'],
        ('crashes', 1): ['docs/reports/s3-hardening/closeout002-c7-crashes/'],
        ('crashes', 2): ['docs/reports/s3-hardening/closeout002-c7-crashes-replication/'],
        ('resilience', 1): [relative(BASE / 'resilience-artifacts') + '/'],
        ('resilience', 2): ['docs/reports/s3-hardening/closeout002-c7-resilience-backlog/'],
        ('restore', 1): ['docs/reports/s3-hardening/closeout002-c7-restore-standalone/',
                         'docs/reports/s3-hardening/closeout002-c7-restore-cluster/'],
        ('release', 0): ['docs/reports/s3-hardening/closeout002-c7-release/'],
    }
    return any(path.startswith(prefix) for prefix in prefixes.get((mode, index), []))


def preserve_required_modes(target, clause, missing_mode_map):
    """Preserve original modes; require a separately reviewed map for absent ones."""
    expected = target.get('required_modes')
    if expected is None:
        assert target['id'] in missing_mode_map, target['id'] + ': no reviewed mode map'
        expected = missing_mode_map[target['id']]['required_modes']
    actual = clause.get('required_modes')
    assert isinstance(expected, list) and expected and len(set(expected)) == len(expected)
    assert isinstance(actual, list) and actual and len(set(actual)) == len(actual)
    assert set(actual) == set(expected), clause['id'] + ': mandatory modes changed'


def validate_mode_map(data, document, baseline_digest):
    assert document['baseline_ledger_sha256'] == baseline_digest
    # A28 is separately retained as blocked; none of its clauses can pass or
    # acquire inferred modes from this supplemental A29–A38 map.
    missing = {c['id']: c for r in data['scenarios'] if r['id'] != 'A28'
               for c in r['clauses'] if c.get('required_modes') is None}
    mapping = document['clauses']
    assert set(mapping) == set(missing)
    assert all(re.fullmatch(r'A(?:29|3[0-8])\.[0-9]+', name) for name in missing)
    for name, item in mapping.items():
        assert item.get('rationale') and item['requirement'] == missing[name]['requirement']
    return mapping


def clause_bindings(clause, items, profiles, freeze):
    """Check execution bindings; semantic assertions still require the auditor."""
    if clause['status'] not in ('passed', 'verified'):
        return {}
    assert clause.get('remaining') == [], clause['id'] + ': unfinished passed clause'
    assert isinstance(clause.get('assertion_review'), str) and clause['assertion_review'].strip()
    required = clause.get('required_modes')
    assert isinstance(required, list) and required and len(required) == len(set(required))
    assert items, clause['id'] + ': no evidence'
    assert all(item.get('mode') in set(profiles) | {'documentation', 'remote_correctness'}
               for item in items if item.get('mode')), clause['id'] + ': unknown evidence mode'
    bindings = {}
    # Supplemental execution claims receive the same checks as mandatory modes.
    for mode in sorted(set(required) | {item['mode'] for item in items if item.get('mode')}):
        evidence = [item for item in items if item.get('mode') == mode]
        assert evidence, clause['id'] + ': missing mode ' + mode
        if mode == 'documentation':
            documentation = [item for item in evidence if item['path'] == 'docs/s3-compatibility.md']
            assert documentation and all(item.get('verified_assertions') for item in documentation)
            content = safe_path('docs/s3-compatibility.md').read_text()
            assert freeze['candidate_sha'] in content and freeze['source_sha256'] in content
            continue
        if mode == 'remote_correctness':
            summary_path = relative(BASE / 'hosted-ci/summary.json')
            assert any(item['path'] == summary_path for item in evidence)
            summary = json.loads(safe_path(summary_path).read_text())
            assert summary['candidate_sha'] == freeze['candidate_sha']
            assert summary['source_sha256'] == freeze['source_sha256'] and summary['status'] == 'passed'
            for name, expected in summary['evidence_hashes'].items():
                path = safe_path(relative(BASE / 'hosted-ci' / name))
                assert digest(path) == expected
            for key in ('push_test_run', 'pr_test_run', 'push_compile_run', 'pr_compile_run'):
                name = relative(BASE / 'hosted-ci' / (str(summary[key]) + '.json'))
                assert any(item['path'] == name for item in evidence)
                run = json.loads(safe_path(name).read_text())
                assert run['headSha'] == freeze['candidate_sha']
                assert run['status'] == 'completed' and run['conclusion'] == 'success' and run['jobs']
                for job in run['jobs']:
                    assert job['status'] == 'completed' and job['conclusion'] == 'success' and job['steps']
                    assert all(step['status'] == 'completed' and step['conclusion'] == 'success'
                               for step in job['steps'])
            assert all(type(summary[key]) is int and summary[key] > 0
                       for key in ('server_tests', 'cli_contract_tests', 'python_harness_tests'))
            continue
        assert mode in profiles, clause['id'] + ': unknown required mode'
        lane = profiles[mode]
        lane_names = {relative(BASE / (mode + '.json')), relative(BASE / 'final-lanes' / (mode + '.json'))}
        refs = [item for item in evidence if item['path'] in lane_names]
        assert refs, clause['id'] + ': no current lane binding'
        indices = set()
        for item in refs:
            selected = item.get('command_indices')
            assert isinstance(selected, list) and selected and len(selected) == len(set(selected))
            for index in selected:
                assert type(index) is int and 0 <= index < len(lane['commands'])
                command = lane['commands'][index]
                assert type(command['exit_status']) is int and command['exit_status'] == 0
                assert command['argv'][:2] not in (['mix', 'compile'], ['mix', 'format'])
                indices.add(index)
        if lane['status'] != 'passed':
            # A29's executed migration fixture also supports its upgrade clause;
            # it cannot certify A28 scale or the full A38 aggregate.
            assert (clause['id'].startswith('A29.') or clause['id'] == 'A38.5')
            assert mode == 'index' and indices == {2}
            assert lane['commands'][0]['exit_status'] == 0
        for index in indices:
            assert any(item.get('verified_assertions') and selected_output(item, mode, index)
                       for item in evidence if item['path'] not in lane_names), \
                clause['id'] + ': no output from selected command ' + mode + ':' + str(index)
        bindings[mode] = sorted(indices)
    return bindings


def main():
    freeze = json.loads((BASE / 'freeze.json').read_text())
    assert API['sha']() == freeze['candidate_sha']
    assert API['source_fingerprint']() == freeze['source_sha256']
    original = json.loads((BASE / 'invocation.json').read_text())
    terminal_evidence.validate_invocation(original, freeze)
    data = json.loads(LEDGER.read_text())
    assert data['tested_sha'] == '83ddc379be879b947e19aecda45e615f4f45b530'
    mode_document = json.loads((BASE / 'required-modes-a29-a38.json').read_text())
    missing_mode_map = validate_mode_map(data, mode_document, digest(LEDGER))
    start = BASE / 'ledger-start.json'
    assert not start.exists()
    profiles = {name: json.loads((BASE / 'final-lanes' / (name + '.json')).read_text())
                for name in API['PROFILES']}
    provenance = json.loads((BASE / 'final-lanes/provenance.json').read_text())
    assert provenance['candidate_sha'] == freeze['candidate_sha'] and provenance['source_sha256'] == freeze['source_sha256']
    selections = {item['profile']: item for item in provenance['selections']}
    assert len(selections) == len(provenance['selections']) == len(profiles)
    for name, record in profiles.items():
        terminal_evidence.validate_lane(record, name, freeze, API, BASE)
        selection = selections[name]
        source = safe_path(selection['selected_source']['path'])
        selected = safe_path(selection['selected']['path'])
        assert source == BASE / (name + '.json') and selected == BASE / 'final-lanes' / (name + '.json')
        assert source.read_bytes() == selected.read_bytes()
        assert digest(source) == selection['selected_source']['sha256'] == selection['selected']['sha256']
    resource = json.loads((REPO / 'docs/reports/s3-hardening/closeout002-c7-resources/results.json').read_text())
    toolchain = dict(elixir='1.18.5', otp='28', python=resource['runner']['python'],
        boto3=resource['versions']['boto3'], botocore=resource['versions']['botocore'],
        aws_cli='2.34.24', mc='RELEASE.2025-08-13T08-35-41Z', dependencies=resource['runner']['dependencies'])
    # Prior ledger snapshots retain historical rows byte-for-byte; current rows
    # contain only the current candidate rather than nesting repeated history.
    for row in data['scenarios']:
        row.pop('historical_record', None)
        row.pop('prior_candidate_record', None)
    data['previous_frozen_candidate'] = dict(
        candidate_sha='69371bc0c2d00f63fd8f3ff0edc751c28a02d441',
        source_sha256='97e56e9eb014454095aff5cc5501f3acc132dbbdcbd04d7d4555d46226031bcb',
        freeze='docs/reports/s3-hardening/closeout-002/candidate-c6/freeze.json',
        original_run_exit_status=1, original_retry_exit_status=1)
    rows = {row['id']: row for row in data['scenarios']}
    assert len(rows) == len(data['scenarios']) == 38
    audited = set()
    for name in ('clause-audit-a01-a27.json', 'clause-audit-a29-a38.json'):
        path = BASE / name
        audit = json.loads(path.read_text())
        assert audit['candidate_sha'] == freeze['candidate_sha'] and audit['source_sha256'] == freeze['source_sha256']
        for ri, review in enumerate(audit['rows']):
            assert review['id'] not in audited
            audited.add(review['id'])
            row = rows[review['id']]
            clauses = {clause['id']: clause for clause in row['clauses']}
            assert len(clauses) == len(row['clauses']) == len(review['clauses'])
            assert set(clauses) == {clause['id'] for clause in review['clauses']}
            modes, assertions, selected_commands = set(review.get('lanes', [])), [], {}
            evidence = [normalize(dict(path=relative(path), json_pointer=f'/rows/{ri}'))]
            for ci, clause in enumerate(review['clauses']):
                target = clauses[clause['id']]
                preserve_required_modes(target, clause, missing_mode_map)
                status = clause['status']
                target['status'] = 'passed' if status in ('passed', 'verified') else 'failed' if status == 'failed' else 'blocked'
                items = [normalize(item) for item in clause['evidence']]
                for mode, indices in clause_bindings(clause, items, profiles, freeze).items():
                    selected_commands.setdefault(mode, set()).update(indices)
                target['evidence'] = [normalize(dict(path=relative(path), json_pointer=f'/rows/{ri}/clauses/{ci}')), *items]
                target['remaining'] = clause.get('remaining', [])
                if clause.get('required_modes'):
                    target['required_modes'] = clause['required_modes']
                statements = [clause['assertion_review']] if clause.get('assertion_review') else []
                for item in items:
                    if item.get('mode'):
                        modes.add(item['mode'])
                    statements.extend(item.get('verified_assertions', []))
                    if item.get('assertion'):
                        statements.append('Source assertion mapped to separate execution: ' + item['assertion'])
                    if item['path'] not in {e['path'] for e in evidence}:
                        evidence.append(item)
                assert statements or target['status'] != 'passed', clause['id']
                target['assertion_audit'] = statements
                assertions.append(clause['id'] + ': ' + ' '.join(dict.fromkeys(statements))
                    + (' Remaining: ' + '; '.join(target['remaining']) if target['remaining'] else ''))
            row['status'] = 'passed' if all(c['status'] == 'passed' for c in row['clauses']) else 'failed' if any(c['status'] == 'failed' for c in row['clauses']) else 'blocked'
            row['evidence'], row['assertion_audit'] = evidence, assertions
            commands, failed = [], []
            for mode in sorted(modes):
                if mode not in profiles:
                    continue
                record = profiles[mode]
                assert record['source_sha256'] == freeze['source_sha256']
                for ci, command in enumerate(record['commands']):
                    if ci in selected_commands.get(mode, set()):
                        commands.append(command['argv'])
                    if command['exit_status'] != 0:
                        failed.append(dict(profile=mode, command_index=ci, exit_status=command['exit_status']))
            row['verification'] = dict(command=commands, mode=sorted(modes), toolchain=toolchain,
                starting_sha=freeze['candidate_sha'], ending_sha=freeze['candidate_sha'],
                source_sha256=freeze['source_sha256'], exit_status=0 if row['status'] == 'passed' else 1,
                scope='Substantive clause audit of fresh C7 evidence. Original failed invocations remain unchanged; the single terminal authoritative run has explicit byte-identical lane provenance. No full acceptance or publication inferred.',
                failed_parent_commands=failed, lane_provenance=relative(BASE / 'final-lanes/provenance.json'))
    assert audited == set(rows) - {'A28'}
    blocked = rows['A28']
    assert profiles['index']['status'] == 'blocked'
    assert len(profiles['index']['commands']) == 3
    assert [command['exit_status'] for command in profiles['index']['commands']] == [0, 1, 0]
    assert json.loads(safe_path('docs/reports/s3-hardening/closeout002-c7-index-cluster/results.json').read_text())['status'] == 'passed'
    blocked['status'] = 'blocked'
    blocked['evidence'] = [normalize(dict(path=relative(BASE / name))) for name in ('upstream-inputs.json', 'index.json')]
    blocked['assertion_audit'] = [
        'Current candidate preflight exited1 before mandatory10k/100k datasets. No scale p50/p95/allocation measurement is certified.',
        'Concord3.1.4 already incorporates#99/#100; current upstream source still lacks#101 singleton traversal and#102 atomic transaction materialization repairs.',
        'The independent current A29 command passed; no local dependency workaround or unchanged multi-hour workload was run. Historical diagnosis retains its original source/scope.']
    for clause in blocked['clauses']:
        clause.update(status='blocked', evidence=copy.deepcopy(blocked['evidence']),
            remaining=['Adopt validated released upstream#101/#102 repairs, then run original10k/100k datasets with actual native records/bytes/allocation, setup/list timings, p50/p95, mutation and affected migration invariants.'],
            assertion_audit=['Current mandatory scale lane remains blocked; narrow prior-source diagnostics are not acceptance.'])
    blocked['verification'] = dict(command=[c['argv'] for c in profiles['index']['commands']],
        mode=['index', 'correctness'], toolchain=toolchain, starting_sha=freeze['candidate_sha'],
        ending_sha=freeze['candidate_sha'], source_sha256=freeze['source_sha256'], exit_status=1,
        scope='Actual current A28 preflight failed; independent A29 succeeded; mandatory datasets unexecuted.')
    data.update(tested_sha=freeze['candidate_sha'], ending_sha=freeze['candidate_sha'], source_sha256=freeze['source_sha256'],
        candidate_freeze=relative(BASE / 'freeze.json'), released_sha=None, release_artifacts=[],
        scenario_counts=dict(collections.Counter(r['status'] for r in rows.values())),
        current_source_passed_rows=[r['id'] for r in rows.values() if r['status'] == 'passed'],
        current_passed_profiles=[name for name, record in profiles.items() if record['status'] == 'passed'],
        full_hardening_acceptance=False,
        current_runtime_evidence_status='Terminal C7 execution with original command outcomes preserved byte-identically. Current semantic audits and selected-lane provenance are explicit; unresolved rows: ' + ', '.join(r['id'] for r in rows.values() if r['status'] != 'passed') + '. No closeout patch release/tag, Hex package or versioned/latest image was published; automatic edge builds remain separate.',
        previous_candidate=dict(candidate_sha='83ddc379be879b947e19aecda45e615f4f45b530',
            source_sha256='64d21d0863ca538bef84ac297733ef2f5dd97746b4f1bd818ade52f0931e6288',
            ledger='docs/reports/s3-hardening/closeout-002/candidate-c5/ledger-final.json',
            actual_aggregate_exit_status=1, preservation='Immutable original execution and partial resource outcomes remain separate.'))
    data['aggregate_verification'] = dict(status='pending_actual_terminal_verification', exit_status=None,
        receipt=relative(BASE / 'final-lanes/aggregate.json'), authoritative_run_exit_status=original['exit_status'],
        provenance=relative(BASE / 'final-lanes/provenance.json'))
    data['delivery_verification']['status'] = 'blocked_full_acceptance_not_passed'
    start.write_bytes(LEDGER.read_bytes())
    LEDGER.write_text(json.dumps(data, indent=2) + '\n')
    print(json.dumps(dict(rows=data['scenario_counts'], clauses=dict(collections.Counter(c['status'] for r in rows.values() for c in r['clauses'])), passed_profiles=data['current_passed_profiles']), indent=2))


if __name__ == '__main__':
    main()
