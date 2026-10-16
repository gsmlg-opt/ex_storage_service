# Native SQL convenience scope

Pinned mc: `77f82e18b5401a65958f1619df6ebb994634bd88`. Source: `cmd/sql-main.go:42-75,147-292,345-489` and `cmd/client-s3.go:545-712`. Private scoped validation passes 24 tests with zero failures (16 new native SQL tests plus existing native routes and shared Select tests), seed 628644. Strict compile, scoped format and shared patch apply-check pass. Parent integration and live provider acceptance remain separate.

## Implemented portable slice

- One remote target plus `--query` expression (default `select * from s3object`), `--recursive`, CSV/JSON input/output key-value serializations, compression selection and explicit nonempty CSV output header. Native SQL stores its expression in `:sql_query`; AWS `:query` remains JMESPath.
- Existing target/expression/outfile remains an atomic file pipeline, preserving existing Select summary fields. Mixing its positional expression with --query fails explicitly.
- Convenience output streams raw Records through the existing bounded Select/EventStream adapter, requiring a validated End event. Stream failure reports whether raw bytes were emitted; file failures remove staging and preserve the destination.
- Complete disk-backed bucket/prefix inventory and complete serialization planning precede all Select requests. A trailing slash selects a prefix; an explicit object stays exact even with --recursive. Nonrecursive listings select immediate children.
- One invocation queries at most --limit objects (default 1000, range 0..10000); returned MatchedObjects, QueriedObjects and Partial are explicit. Raw stdout gets an stderr notice when the limit truncates selection. Records remain streamed without accumulating complete objects/results.
- Case-insensitive pinned abbreviated/long serialization keys, escaped newline/tab/carriage return and literal-comma delimiters are bounded. Repeated aliases of the same logical field, unknown fields and invalid enum/boolean/delimiter values fail before network. qrd/QuotedRecordDelimiter maps to the real S3 AllowQuotedRecordDelimiter boolean; pinned mc accepts qrd but its S3 mapping does not consume it.
- CSV/JSON/Parquet extension inference, CSV USE header default, CSV/JSON output defaults, gzip/bzip2 inference and explicit compression follow the pinned shared S3 client. The --json global selects JSON Records for convenience syntax; structured --output presentation is rejected for raw Records.

## Still-open exact contracts

- Empty --csv-output-header requests source-header inference in pinned mc. This slice explicitly rejects it; no unbounded source read or decompression substitutes for a bounded header implementation. Nonempty headers are emitted once, including an empty successful result.
- Multi-target grammar, -e short alias, local/stdin SQL and --enc-c prefix-key grammar remain separate work. Exactly three operands retain the legacy expression/outfile interpretation.
- Prefix arguments require a trailing slash. This slice does not HEAD-probe a non-slash missing object and reinterpret it as a prefix.
- Discovery uses known object extensions (CSV, JSON/JSONL/NDJSON, Parquet and compressed forms) or an explicit input format. Pinned mc additionally checks content-type metadata and has different supported-discovery filtering; these exact discovery rules are not claimed equivalent. Unsupported-extension inventory entries are skipped unless input serialization is explicit; a single object with unknown format fails explicitly.
- Complete inventory precedes all queries, and model/serialization errors in later selected objects preserve all output before effects. This is a deliberate failure-policy difference from pinned mc continuing after per-object errors.
- Output/default/grammar slices do not close the whole MC:sql contract. No live provider SQL support is inferred from wire fixtures.
