# Native no-color

Native `--no-color` suppresses application-owned ANSI styling for the current
invocation. It covers legacy success/error/warning output, bucket tables,
interactive configuration, server information, and legacy command help routed
through the native runner. The next invocation retains the existing colored
default unless it also requests `--no-color`.

The output boundary stores this policy in the invoking process and restores its
previous value in an `after` block, including exceptions and nested invocations.
It does not change environment variables, the VM-wide ANSI setting, other
processes, structured output, or arbitrary message/object bytes. ANSI bytes in
user-provided data are preserved; output is never filtered with an escape-code
regular expression.

`native_no_color_test.exs` exercises real native bucket success/service errors,
bucket tables, server information and all ten legacy help routes, plus nested,
exception and concurrent-process isolation. This is scoped ESS styling evidence;
the mc environment alias `MC_NO_COLOR` and exact default/theme/output equivalence
remain outside this implementation.

Validation from `apps/ex_storage_service_cli`:

```sh
PAGER=cat mix test test/ex_storage_service_cli/cli/native_no_color_test.exs test/ex_storage_service_cli/cli/runner_test.exs test/ex_storage_service_cli/cli/native_globals_test.exs test/ex_storage_service_cli/cli/output_session_test.exs test/ex_storage_service_cli/cli/error_output_test.exs test/ex_storage_service_cli/cli/native_routes_test.exs --seed 628644
PAGER=cat mix compile --warnings-as-errors
```

The focused run passed 31 tests with zero failures, and strict compilation
passed. Modified Elixir files were formatted. The original gap was established
from the source: the parsed `no_color` option had no output effect and the legacy
formatters emitted ANSI tokens directly. Tests were authored before the fix,
but no red run was captured because the coordinating agent owned the shared
Mix build window until after these source edits. No pre-fix passing claim is
made.
