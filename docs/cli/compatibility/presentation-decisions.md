# Structured output compatibility

Presenter removes top-level ResponseMetadata before running JMESPath, and
formats the resulting data as JSON, text, table, YAML, or yaml-stream. `off` is a
native ESS extension; the pinned AWS CLI inventory correctly excludes it.

The private text/table algorithms are translated from AWS CLI 2.31.18 under
Apache-2.0, with full notices in source and binary `priv`. No Python runtime is
shipped. Text aligns scalar keys across lists of records, emits bare scalar
lists on one tab-separated row, and emits named scalar lists one value per row.
Tables preserve reference sections, indentation, proportional column widths,
bankers rounding, single-record vertical conversion and on/off/TTY-auto color.
Unicode widths use Python Unicode 15.1.0 W/F/A properties, including the
reference's width-two ambiguous characters and codepoint treatment of combining
marks. Unknown output types and malformed/ragged tables return typed failures
before emitting bytes.

JSON uses four-space indentation and Unicode-preserving scalar encoding. Empty
maps emit no bytes for JSON/YAML/nonpaginated yaml-stream, as in AWS; other empty
or scalar query results retain format-specific behavior. Timestamp values use
ISO UTC `+00:00`. YAML-stream emits each queried paginator page as a one-item
list, including empty pages; later pages are not buffered by Presenter.
API owns pagination: text queries its projected page; JSON/table/YAML query the
aggregate; yaml-stream queries each raw page.

Two deliberate boundaries are explicit. Elixir response maps do not retain
service insertion order, so JSON object keys are emitted lexically. Byte parity
is tested with lexically ordered reference maps; arbitrary JSON map outputs
have semantic parity. Human text/table output escapes untrusted C0/C1 controls,
DEL and bidi override/isolate characters, including tabs/newlines embedded in
values. Delimiter tabs/newlines and formatter-owned color remain intact.
Machine formats retain values through encoding. AWS may emit terminal controls
literally; ESS applies the design's terminal-safety requirement.

The independent fixture generator checks original formatter/text/table source
hashes and invokes actual AWS formatters, without ESS imports. Root scalar cases
use a valid map response plus JMESPath query, because AWS's metadata-removal
helper expects an API response collection. All 138 reference vectors and seven
integration/safety tests pass (145 scoped tests). The combined Presenter/YAML/
AWSInput suite passes 254 tests; strict CLI compilation passes. This evidence
covers included outputs and does not independently close all M5/M9 executable
or provider compatibility surfaces.
