# JMESPath implementation decision

The CLI carries a private reviewed port of the MIT **jmes 0.4.1** evaluator,
builtin functions and OTP leex/yecc grammars. It adds no Hex dependency and uses
Elixir's built-in JSON module. The complete upstream license and package checksum
are retained in `lib/ex_storage_service_cli/query/NOTICE.txt` and the licensed
fixture snapshot. Grammars are source-authoritative; Mix's leex/yecc compilers
produce the private Erlang scanner/parser modules. The existing package's `lib`
inclusion carries the grammars and notice; compiled modules enter the escript.

The removed upstream atom-key fallback is replaced by binary-key-only lookup.
Lexer tokens and grammar AST tags are fixed source atoms. User identifiers,
function names and multiselect keys remain binaries. Lexer matching uses a
generated DFA rather than runtime backtracking regex. Quoted identifiers use
strict JSON escape validation. JSON literals are decoded during parsing, even in
unevaluated boolean branches.

Correctness changes include boolean short circuit, JSON numeric equality,
exact integer conversion, empty projection/aggregate behavior, tagged expression
references, codepoint-based Unicode length/reverse, and clamped Python-compatible
slice boundaries. These are covered by both upstream fixtures and additional
regressions. No arbitrary module/function dispatch or atom construction is used.

`ExStorageServiceCli.Query.search/3` returns `{:ok, json_value}` or a safe typed
CLI error with the JMESPath error code. Error text excludes expression/data and
exception contents. Defaults cap expression bytes at **8192**, materialized
input/output at **100000 values/keys**, **8 MiB of scalar content**, depth **128**,
evaluation time **1000 ms**, and the evaluator heap at **8388608 words** (64 MiB
on this 64-bit runtime). Node/content budgets are logical materialization bounds,
not a claim about total BEAM resident memory. Input is validated before copying
into the isolated worker. The guardian kills that worker on deadline, caller
death or completion; heap-limit termination cannot crash the invoking process.
Results are explicitly rejected when oversized, never silently truncated.

Validation on 2026-10-15:

- **863 scoped tests pass**: 837 licensed package corpus cases, 15 source-byte
  integrity checks, and 11 safety/correctness checks.
- **892/892** semantic/error cases pass in the independent official corpus at
  `jmespath/jmespath.test` revision `53abcc37901891cf4308fcd910eab287416c4609`.
  Its 16 benchmark-only cases provide no expected results and are not semantic
  acceptance checks. The official repository declares no explicit license; the
  newer corpus is used externally rather than redistributed.

Frontend query integration, per-page versus aggregated query behavior, output
formats and standalone package consumers are separate acceptance surfaces. This
decision establishes an adequate full-grammar evaluator within explicit budgets;
it does not establish completion of M5 or broad AWS CLI compatibility.
