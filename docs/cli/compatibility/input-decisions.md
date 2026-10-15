# Modeled AWS input decisions

The CLI uses the packaged, pinned S3 model and SDK customization overlay for
every operation member and flag. `AWSInput.flags/1` maps CLI flag strings to
binary modeled member names; no user input becomes an atom. Explicit CLI flags
override `--cli-input-json` or `--cli-input-yaml` values. Lists retain order, integers retain exact
64-bit values, and absent, empty and false values remain distinct. Model
validation rejects unknown members, wrong types, required omissions and enum
or size violations. Model-defined floating-point values use IEEE floats.

The input parser supports JSON, nested AWS shorthand, escaped commas and quotes,
`@=` nested file references, bounded `file://` control inputs, `fileb://` binary
inputs, and binary-format selection. The pinned S3 input catalog's only blob
shape is the streaming object Body; its path is passed to the transfer layer as
`streaming_body_path`, and the parser never reads or buffers that object.
Nonstreaming file references are limited to 8 MiB by default. Explicit stdin
streaming is rejected until replayable spool integration is available.

Timestamp inputs accept ISO 8601 with offsets, UTC-naive ISO/date-only values,
RFC1123 GMT/UTC dates, and integer or fractional Unix seconds. Arbitrary
dateutil-language date expressions and named non-UTC timezone abbreviations
are not claimed as supported.

`--cli-input-yaml` uses the private reviewed safe parser described in
`yaml-decisions.md`. JSON and YAML input options are mutually exclusive and
share the bounded file loader. Standard YAML timestamps retain their type until
modeled timestamp coercion, so a YAML date cannot silently satisfy a string
member. JSON/YAML input and JSON output skeletons are pure, packaged
model operations. Input skeletons and help require no credentials or object
file access. Output skeleton invocation validates provided modeled input
before the coordinating runner emits the pure result.

AWS global and paginator switches are parsed into fixed known atoms for the
coordinating runner to apply. Their parsing does not prove presentation,
pagination, auto-prompt or network effects; those need separate integration
checks.

Independent fixtures under `test/fixtures/aws_input` use AWS CLI 2.31.18's actual
`ParamShorthandParser`, `BackCompatVisitor`, `botocore.xform_name`, and
`ArgumentGenerator`. The generator checks exact parser-source SHA256 values
from source revision `f4f66e65e70f23f1bf232ecb4a6d8696f4dda3bc`; it imports
no ESS implementation. The Elixir suite consumes committed fixtures offline.
