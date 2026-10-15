# Pinned high-level S3 option parser

`generate.py` uses the actual AWS CLI 2.31.18 `ArgTableArgParser`, original S3
command classes and registered sync strategies. Integer vectors also invoke the
actual AWS `unpack_cli_arg` scalar coercer, which runs after argparse before AWS
command handlers; ESS combines those phases into its typed pure invocation. It checks immutable source hashes
and does not import ESS. The 183 vectors cover all 137 command flag entries,
optional-value defaults and enum rejection. Metadata/blob loading and actual
request effects are separate execution-layer checks.

Regenerate from repository root with the pinned source and capture dependencies:

```sh
PAGER=cat env PYTHONPATH=/tmp/ess-cli-reference-m0/aws-cli-2.31.18:/tmp/ess-cli-reference-m0/aws-cli-2.31.18/awscli:/tmp/ess-highlevel-reference-deps:/tmp/ess-sigv4-reference-deps python3 apps/ex_storage_service_cli/test/fixtures/s3-options/generate.py
```

The extra capture-only dependencies are docutils 0.19, awscrt 0.27.6,
prompt-toolkit 3.0.51, and distro 1.8.0. ESS has no Python runtime dependency.
Run `mix test test/ex_storage_service_cli/cli/s3_options_test.exs` from the CLI app.
Reference output data is generated independently; no parser code is redistributed
in these fixtures.
