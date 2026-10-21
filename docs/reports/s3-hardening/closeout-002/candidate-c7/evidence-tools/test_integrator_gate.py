"""Synthetic clause-binding tests; no service or real resource data is read."""
import copy
import importlib.util
import json
from pathlib import Path
import tempfile
import unittest
from unittest import mock

spec = importlib.util.spec_from_file_location('integrator', Path(__file__).with_name('integrate_ledger.py'))
module = importlib.util.module_from_spec(spec)
spec.loader.exec_module(module)


class ClauseBindingsTest(unittest.TestCase):
    def setUp(self):
        self.temporary = tempfile.TemporaryDirectory()
        self.addCleanup(self.temporary.cleanup)
        self.repo = Path(self.temporary.name)
        self.base = self.repo / 'docs/reports/s3-hardening/closeout-002/candidate-c7'
        self.base.mkdir(parents=True)
        self.patches = [mock.patch.object(module, 'REPO', self.repo),
                        mock.patch.object(module, 'BASE', self.base)]
        for patch in self.patches:
            patch.start()
            self.addCleanup(patch.stop)
        self.freeze = dict(candidate_sha='a' * 40, source_sha256='b' * 64)
        self.profile = dict(status='passed', commands=[
            dict(argv=['mix', 'format'], exit_status=0),
            dict(argv=['mix', 'compile'], exit_status=0),
            dict(argv=['python', 'suite.py'], exit_status=0)])
        self.clause = dict(id='A01.1', status='passed', remaining=[],
                           assertion_review='Reviewed concrete assertion and its selected execution.',
                           required_modes=['correctness'])
        self.lane = str((self.base / 'correctness.json').relative_to(self.repo))
        self.output = str((self.base / 'correctness-2.log').relative_to(self.repo))
        (self.repo / self.output).write_text('selected case passed\n')
        self.items = [dict(path=self.lane, mode='correctness', command_indices=[2]),
                      dict(path=self.output, mode='correctness', verified_assertions=['Selected case passed.'])]

    def bind(self, clause=None, items=None, profile=None, name='correctness'):
        return module.clause_bindings(clause or self.clause,
                                      self.items if items is None else items,
                                      {name: profile or self.profile}, self.freeze)

    def test_selected_successful_execution(self):
        self.assertEqual(self.bind(), {'correctness': [2]})

    def test_remaining_requirement_blocks_pass(self):
        self.assertRaises(AssertionError, self.bind, dict(self.clause, remaining=['Unexecuted case']))

    def test_source_only_evidence_is_not_execution(self):
        self.assertRaises(AssertionError, self.bind, items=[dict(path='source.ex', verified_assertions=['Assertion source'])])

    def test_missing_or_unknown_required_mode(self):
        for modes in ([], ['correctness', 'gateway'], ['invented']):
            with self.subTest(modes=modes):
                self.assertRaises(AssertionError, self.bind, dict(self.clause, required_modes=modes))

    def test_binding_requires_indices_and_substantive_output(self):
        for indices in ([], [0], [1], [True], [9], [2, 2]):
            with self.subTest(indices=indices):
                self.assertRaises(AssertionError, self.bind,
                    items=[dict(self.items[0], command_indices=indices), self.items[1]])
        self.assertRaises(AssertionError, self.bind, items=[self.items[0]])
        self.assertRaises(AssertionError, self.bind, items=[self.items[1]])

    def test_failed_child_is_not_pass(self):
        profile = copy.deepcopy(self.profile)
        profile['commands'][2]['exit_status'] = 1
        self.assertRaises(AssertionError, self.bind, profile=profile)

    def test_unrelated_candidate_files_cannot_substantiate_execution(self):
        for name in ('freeze.json', 'gateway-1.log', 'correctness-3.log'):
            with self.subTest(name=name):
                output = self.base / name
                output.write_text('unrelated output\n')
                self.assertRaises(AssertionError, self.bind, items=[self.items[0],
                    dict(path=str(output.relative_to(self.repo)), mode='correctness',
                         verified_assertions=['Wrong selected command.'])])

    def test_supplemental_runtime_claims_are_verified_and_retained(self):
        standalone = dict(status='passed', commands=[dict(argv=['bash', 'profile.sh'], exit_status=0)])
        output = self.base / 'standalone-0.log'
        output.write_text('specific standalone assertion passed\n')
        items = self.items + [dict(path=str((self.base / 'standalone.json').relative_to(self.repo)),
                                  mode='standalone', command_indices=[0]),
                              dict(path=str(output.relative_to(self.repo)), mode='standalone',
                                   verified_assertions=['Supplemental standalone assertion passed.'])]
        profiles = {'correctness': self.profile, 'standalone': standalone}
        self.assertEqual(module.clause_bindings(self.clause, items, profiles, self.freeze),
                         {'correctness': [2], 'standalone': [0]})
        self.assertRaises(AssertionError, module.clause_bindings,
                          self.clause, items[:-1], profiles, self.freeze)

    def test_blocked_index_only_selected_a29_child(self):
        profile = dict(status='blocked', commands=[
            dict(argv=['mix', 'compile'], exit_status=0),
            dict(argv=['bash', 'a28.sh'], exit_status=1),
            dict(argv=['bash', 'a29.sh'], exit_status=0)])
        output = str((self.base / 'index-2.log').relative_to(self.repo))
        (self.repo / output).write_text('independent A29 assertions passed\n')
        items = [dict(path=str((self.base / 'index.json').relative_to(self.repo)),
                      mode='index', command_indices=[2]),
                 dict(path=output, mode='index', verified_assertions=['A29 exact checkpoint and fence.'])]
        clause = dict(self.clause, id='A29.1', required_modes=['index'])
        self.assertEqual(self.bind(clause, items, profile, 'index'), {'index': [2]})
        self.assertEqual(self.bind(dict(clause, id='A38.5'), items, profile, 'index'), {'index': [2]})
        for clause_id, indices in (('A28.1', [2]), ('A30.1', [2]), ('A38.1', [2]),
                                   ('A38.6', [2]), ('A29.1', [0]), ('A29.1', [1])):
            with self.subTest(clause=clause_id, indices=indices):
                self.assertRaises(AssertionError, self.bind, dict(clause, id=clause_id),
                    [dict(items[0], command_indices=indices), items[1]], profile, 'index')

    def test_existing_modes_cannot_be_shrunk_or_overridden_by_new_map(self):
        target = dict(id='A01.1', required_modes=['correctness', 'cluster'])
        mapping = {'A01.1': {'required_modes': ['correctness']}}
        self.assertRaises(AssertionError, module.preserve_required_modes, target,
                          dict(target, required_modes=['correctness']), mapping)
        module.preserve_required_modes(target, target, mapping)

    def test_absent_modes_require_explicit_complete_reviewed_map(self):
        target = dict(id='A29.1')
        clause = dict(target, required_modes=['correctness', 'index'])
        self.assertRaises(AssertionError, module.preserve_required_modes, target, clause, {})
        mapping = {'A29.1': {'required_modes': ['correctness', 'index']}}
        module.preserve_required_modes(target, clause, mapping)
        self.assertRaises(AssertionError, module.preserve_required_modes, target,
                          dict(clause, required_modes=['index']), mapping)

    def test_blocked_a28_remains_outside_a29_a38_mode_additions(self):
        data = {'scenarios': [{'id': 'A28', 'clauses': [{'id': 'A28.1', 'requirement': 'Scale'}]},
                              {'id': 'A29', 'clauses': [{'id': 'A29.1', 'requirement': 'Migration'}]}]}
        document = {'baseline_ledger_sha256': 'baseline', 'clauses': {
            'A29.1': {'requirement': 'Migration', 'rationale': 'Actual migration fixture',
                      'required_modes': ['index']}}}
        self.assertEqual(module.validate_mode_map(data, document, 'baseline'), document['clauses'])
        self.assertRaises(AssertionError, module.validate_mode_map, data, document, 'changed baseline')
        for mutation in ({}, dict(document['clauses'], **{'A28.1': {'requirement': 'Scale'}}),
                         {'A29.1': dict(document['clauses']['A29.1'], requirement='Reduced requirement')}):
            self.assertRaises(AssertionError, module.validate_mode_map, data,
                              dict(document, clauses=mutation), 'baseline')

    def test_unpassed_clause_never_produces_pass_bindings(self):
        self.assertEqual(self.bind(dict(self.clause, status='blocked', remaining=['Not executed']), []), {})

    def test_documentation_requires_current_candidate_identity(self):
        path = self.repo / 'docs/s3-compatibility.md'
        path.write_text('Prior candidate evidence only\n')
        items = [dict(path='docs/s3-compatibility.md', mode='documentation', verified_assertions=['Reviewed prose.'])]
        clause = dict(self.clause, required_modes=['documentation'])
        self.assertRaises(AssertionError, self.bind, clause, items)
        path.write_text(self.freeze['candidate_sha'] + '\n' + self.freeze['source_sha256'])
        self.assertEqual(self.bind(clause, items), {})


if __name__ == '__main__':
    unittest.main(verbosity=2)
