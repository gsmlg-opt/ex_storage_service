# Safe YAML implementation decision

The CLI privately ports the BSD-2-Clause **yamerl 0.10.0** parser and MIT
**ymlr 5.1.6** scalar/layout emitter. No Hex dependency, application process,
protocol extension or runtime configuration hook is added. Full copyright and
license notices appear in `lib/ex_storage_service_cli/yaml/NOTICE.txt` and
`priv/compatibility/NOTICE-YAML.txt`, which accompanies binary distributions.

Only standard JSON-compatible YAML shapes are accepted. Standard date,
timestamp and binary tags retain tagged values for the modeled input layer;
dates do not silently become arbitrary modeled strings. Unknown/custom tags,
duplicate keys, non-string keys, non-finite numbers, multiple documents and
recursive aliases fail explicitly. YAML 1.2 is the default; a YAML 1.1 directive
selects its standard schema. Anchors and merges support explicit-key precedence
and first-map precedence within merge sequences. Quoted `"<<"` is an ordinary
key. Erlang atom/function constructors and other extension modules are omitted.

Defaults bound input/output text to 8 MiB, 100000 values/keys, depth 128,
100000 parser tokens, 128 aliases, 1000 ms, and an isolated worker heap of
8388608 words. Worker termination cannot crash the caller. Caller death and
deadlines kill the parser worker; oversized results fail rather than truncate.
Logical budgets do not claim a total BEAM resident-memory ceiling.

The fixed, packaged parser/constructor modules and safe scalar implementation
load before the input-execution timer starts. A full-suite diagnostic confirmed
that a tiny input could otherwise spend its 1000 ms waiting for the code server
to load a cold scalar module, with only 2222 worker reductions and 34 KiB of
process memory. Trusted code loading is independent of input, and no module
name comes from YAML. Text, logical, heap, alias and execution limits remain
unchanged. A disposable-VM regression delays only its own code server for
1200 ms: the previous runtime fails and the fixed runtime parses successfully.
The focused YAML/AWSInput checks pass 111 tests after this fix; the final
integrated report remains a separate gate.

Output is deterministic block YAML with sorted binary keys, indentless mapped
lists, explicit nulls, safe scalar quoting, Unicode escapes, and folded long
scalars. Root scalar output uses JSON spelling as AWS does. Very long mapping
keys use a valid flow JSON fallback. Those styling choices are independently
compared with the actual AWS CLI 2.31.18 `YAMLDumper` using its pinned
ruamel.yaml 0.17.21. This is evidence for the included vectors, not a claim that
every possible ruamel stylistic choice is byte-identical.

The scoped YAML suite passes 75 tests: 48 input vectors from actual
`CliInputYAMLArgument`, 19 byte-exact output vectors, and eight safety checks.
Additional Presenter vectors exercise YAML streams, long scalars, Unicode and
float spelling. AWSInput has 34 passing tests including bounded file/fileb input,
CLI-over-structured-input precedence, timestamp coercion and YAML skeletons.
Fixtures record immutable source hashes; Python is used only to regenerate
reference evidence, never by the executable.
