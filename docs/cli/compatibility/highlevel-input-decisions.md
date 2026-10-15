# High-level S3 argument grammar

`CLI.S3Options` defines all 137 flag entries across the nine locked AWS CLI
2.31.18 S3 commands. Definitions, enum values, optional-value constants and
command allowlists come from the immutable `aws-s3-commands.json` inventory.
Keys are fixed source atoms; unknown argv names never become atoms. `flags/1`
returns Registry grammar and `defaults/1` returns source defaults separately,
so execution can distinguish omitted parameters.

Parser selects frontend-specific option tables before scanning. Native presign's
Expires remains an integer while S3 transfer Expires remains a timestamp string.
Encryption and request-payer flags preserve optional argument constants; grants
preserve a nonempty list; include/exclude preserve ordered repeated filters.
AWS booleans reject inline values and only explicit source negations are
accepted. Enum failures expose the flag name without echoing its value.
Store-false flags use logical keys (`follow_symlinks`, `guess_mime_type`,
`progress`) with false values; execution must merge source defaults, not infer
behavior from a `no_` spelling. Metadata and SSE-C blob strings remain raw until
the bounded execution input layer applies their modeled semantics.

Native global TLS flags include CA verification and paired client certificate/
key paths. Paths are never opened during parsing. Native request/concurrency
limits and memory budget (integer bytes) must be positive. Legacy `--json`
remains supported and conflicts with an explicit non-JSON output choice.
`CLI.OptionHelp` exposes value hints and choices from these exact grammar types;
Runner owns help/completion rendering and execution owns actual effects.

The independent generator imports original AWS S3 command classes and sync
strategy registrations, then actual `ArgTableArgParser`. AWS argparse keeps
integer tokens as strings; its actual `unpack_cli_arg` performs scalar coercion
before handlers. Fixtures capture that final typed value rather than treating
argparse's intermediate string as the command contract. All 183 independent
flag/default/enum vectors pass. The combined parser/S3Options/AWSInput check
passes 230 tests, and strict CLI compilation passes.

This closes the discoverable pure grammar surface for those flags. Wire effects,
config precedence, credential loading, transfer policies and complete executable
parity remain separate acceptance surfaces owned by their handlers.
