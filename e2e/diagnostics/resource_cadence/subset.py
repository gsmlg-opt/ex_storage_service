"""Original cluster-resource prefix through group6; never full acceptance."""
import ast
import copy
import json
from pathlib import Path
import sys
import time


class DiagnosticScopeComplete(Exception):
    pass


def prepare(source, name):
    original = ast.parse(source, filename=name)
    tree = copy.deepcopy(original)
    main = next(node for node in tree.body if isinstance(node, ast.FunctionDef) and node.name == 'main')
    measured = next(node for node in main.body if isinstance(node, ast.FunctionDef) and node.name == 'measured')
    assert isinstance(measured.body[-1], ast.Assert)
    measured.body.append(ast.parse('_diagnostic_stop(name, size, concurrency)').body[0])
    reverted = copy.deepcopy(tree)
    reverted_main = next(node for node in reverted.body if isinstance(node, ast.FunctionDef) and node.name == 'main')
    reverted_measured = next(node for node in reverted_main.body if isinstance(node, ast.FunctionDef) and node.name == 'measured')
    reverted_measured.body.pop()
    assert ast.dump(reverted, include_attributes=False) == ast.dump(original, include_attributes=False)
    return ast.fix_missing_locations(tree)


def main():
    original = Path(sys.argv[1]).resolve()
    evidence = Path(sys.argv[sys.argv.index('--evidence') + 1]).resolve()
    def stop(name, size, concurrency):
        if (name, size, concurrency) == ('strict-quorum-replica-put', 67108864, 4):
            raise DiagnosticScopeComplete()
    tree = prepare(original.read_text(), str(original))
    sys.path.insert(0, str(original.parent))
    sys.argv = sys.argv[1:]
    outcome = 'failed'
    try:
        exec(compile(tree, str(original), 'exec'), dict(__name__='__main__', __file__=str(original),
             _diagnostic_stop=stop, DiagnosticScopeComplete=DiagnosticScopeComplete))
        raise RuntimeError('subset stop not reached; original full phase must not execute')
    except DiagnosticScopeComplete:
        result = json.loads((evidence / 'results.json').read_text())
        assert len(result['groups']) == 6
        assert all(not row['failures'] for row in result['groups'])
        assert result['error_type'] == 'DiagnosticScopeComplete' and result['status'] == 'failed'
        outcome = 'diagnostic_subset_complete'
    finally:
        if evidence.is_dir():
            (evidence / 'diagnostic-scope.json').write_text(json.dumps(dict(
                status=outcome, acceptance_status='incomplete', acceptance_promotion=False,
                source=str(original), original_group_count=20, selected_prefix_groups=6,
                target=dict(name='strict-quorum-replica-put', size=67108864, concurrency=4),
                excluded_groups=list(range(7, 21)), wall_ns=time.time_ns()), indent=2) + '\n')


if __name__ == '__main__':
    main()
