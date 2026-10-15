# Pinned AWS YAML oracle

`generate.py` imports the actual unmodified AWS CLI **2.31.18** input loader
and output dumper at source revision
`f4f66e65e70f23f1bf232ecb4a6d8696f4dda3bc`; it never imports ESS. Source SHA256
checks and ruamel.yaml **0.17.21** version checks fail before capture on drift.
`vectors.json` contains 48 input cases and 19 output cases. Timestamp/date/binary
results use explicit fixture tags instead of lossy JSON coercion.

Regenerate from the repository root with the source package and its exact Python
dependencies on `PYTHONPATH`:

```sh
PAGER=cat env PYTHONPATH=/tmp/ess-cli-reference-m0/aws-cli-2.31.18:/tmp/ess-cli-reference-m0/aws-cli-2.31.18/awscli:/tmp/ess-sigv4-reference-deps python3 apps/ex_storage_service_cli/test/fixtures/yaml/generate.py
```

Run `mix test test/ex_storage_service_cli/yaml/yaml_test.exs` from the CLI app.
Behavioral fixtures are reference output data; no AWS parser implementation is
copied. The separately ported implementation retains BSD-2-Clause and MIT
notices under `lib/ex_storage_service_cli/yaml/NOTICE.txt` and binary `priv`.
Known input boundaries and runtime budgets are documented in
`docs/cli/compatibility/yaml-decisions.md`.
