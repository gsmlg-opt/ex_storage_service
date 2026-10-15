# Independent AWS output oracle

`generate.py` checks the SHA256 of actual AWS CLI 2.31.18 formatter/text/table
sources, then invokes their formatter classes against independent sample service
data. It never imports ESS. The resulting 138 vectors cover empty/scalar/list/
nested results, unions of record keys, long tables and YAML values, Unicode,
scientific numbers, metadata removal, JMESPath and explicit/auto color. Root
scalar cases are produced through queries on valid map responses.

Regenerate with AWS CLI source revision
`f4f66e65e70f23f1bf232ecb4a6d8696f4dda3bc` and pinned Python dependencies:

```sh
PAGER=cat env PYTHONPATH=/tmp/ess-cli-reference-m0/aws-cli-2.31.18:/tmp/ess-cli-reference-m0/aws-cli-2.31.18/awscli:/tmp/ess-sigv4-reference-deps python3 apps/ex_storage_service_cli/test/fixtures/presentation/generate.py
```

Run `mix test test/ex_storage_service_cli/presenter_test.exs` from the CLI app.
Terminal-safety and JSON key-order deviations are explicitly documented in
`docs/cli/compatibility/presentation-decisions.md`. Behavioral output fixtures
contain no copied AWS implementation. Translated private formatter algorithms
retain Apache-2.0 notices in source and binary `priv`.
