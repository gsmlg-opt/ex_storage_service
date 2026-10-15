# Native mirror and diff policy options

Scope: `9b8661e` plus the CLI implementation diff. Native mirror and diff use
their own policy contracts; AWS sync retains its pinned AWS option grammar.

The native Registry previously inherited AWS sync's `--size-only`,
`--exact-timestamps` and `--delete` switches for mirror even though mirror's
planner uses its explicit overwrite/remove policy. Diff also accepted
`--delete` without any deletion effect. These accepted switches could mislead
an operator about the intended plan.

The parser now rejects those mirror switches and diff's deletion switch before
credentials, enumeration or mutations. Direct HighLevel invocations also reject
their explicit presence, including false values, so callers cannot bypass the
parser and silently lose a policy. Native mirror continues to use `--overwrite`
and `--remove`. AWS `s3 sync` still accepts its size/timestamp/delete options.

The three regression tests cover parser rejection, direct invocation before
inventory/network effects, and unchanged AWS sync grammar. The original product
failed two of the three tests in `/tmp/ess-native-policy-flags-red.log`. The
corrected tests passed in the 87-test transfer/policy integration recorded in
`/tmp/ess-move-sse-scoped.log`. These are scoped local tests; they do not establish
full mc argument, default or output parity.
