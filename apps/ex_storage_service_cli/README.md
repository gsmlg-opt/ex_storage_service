# ExStorageServiceCli

[![Hex.pm](https://img.shields.io/hexpm/v/ex_storage_service_cli.svg)](https://hex.pm/packages/ex_storage_service_cli)

`ess` is the standalone S3 client from [ExStorageService](https://github.com/gsmlg-opt/ex_storage_service). It provides native named-endpoint workflows, an AWS-style `s3` namespace, and model-backed `s3api` commands. The compatibility baseline is AWS CLI 2.31.18; implementation and provider evidence remain scoped. A command appearing in help is not proof of full AWS or mc parity.

## Install

```sh
mix escript.install hex ex_storage_service_cli
```

Add `~/.mix/escripts` to `PATH`. Building/installing requires Elixir 1.18+, Erlang/OTP, a C11 compiler (`CC`, or `cc` on PATH), and OTP development headers. The escript embeds the compiled native library and pager helper. Running it requires Erlang/OTP with the matching native OS/CPU/OTP major, but no C compiler, Mix project, storage server, Phoenix, or database. Build on the target platform. Package/runtime probes pass on Linux x86_64 with Elixir 1.18.0/OTP 27, 1.18.5/OTP 28 and 1.19.6/OTP 28, and macOS arm64 with Elixir 1.19.6/OTP 28. Other combinations remain unverified. Interactive paging is currently Linux-only.

## Choose a frontend

```sh
ess version
ess --help
ess s3 --help
ess s3api head-object --help

# Native legacy profile configuration remains supported.
ess configure
ess mb example-bucket
ess cp ./file.txt s3://example-bucket/file.txt
ess ls example-bucket

# Named endpoints can refer to an existing AWS profile without copying secrets.
ess alias set archive https://s3.us-east-1.amazonaws.com --provider aws-profile --profile archive
ess ls archive/example-bucket
ess cp archive/example-bucket/file.txt ./downloaded.txt

# AWS namespaces use AWS shared configuration independently of ESS config.
ess s3 ls s3://example-bucket --profile archive
ess s3api head-object --bucket example-bucket --key file.txt --profile archive
ess s3api list-objects-v2 --bucket example-bucket --query 'Contents[].Key' --output json
```

Use command-specific `--help` for accepted flags. Native commands include alias import/export, transfer inspection/resume/abort, recursive transfers, synchronization/diff/mirror, versioning, object metadata and policy controls. `ess completion bash`, `ess completion zsh`, and `ess completion fish` generate shell completions. `ess sso login --profile archive` and `ess sso logout --profile archive` operate on the selected IAM Identity Center session; interactive login reports its authorization URL and attempts the Linux desktop browser launcher, with URL fallback when unavailable.

AWS namespaces preserve their S3 URI grammar and pinned model options. They do not interpret native aliases as AWS targets. Native commands take local paths, `s3://bucket/key`, and configured `alias/bucket/key` targets. Quote paths and keys containing spaces or shell metacharacters.

Native `cp`, `mv`, and `get` accept `--version-id` for one remote source object.
The provider must confirm that version before the transfer; ignored version
selection fails without replacing a local destination. Recursive/version
combinations are rejected. `mv` deletes only the confirmed selected version.

Native `cp --rewind TIME` copies a single object or a recursive prefix as it
existed strictly before TIME. A selected delete marker hides older data. The
complete version inventory must succeed before transfers begin, and historical
identity is checked before publication. Choose either rewind or an explicit
version ID; rewind requires a remote source. Absolute dates use UTC (including
date-only input), unlike mc's host-local date interpretation.

`cat`/`head` accept `--rewind` and `--vid`; `cat` also accepts `--offset`,
`--tail`, and `--part-number`, and `head --n` limits lines. `ls`, `stat`, and
`tree` expose bounded historical snapshots. `tree --files --depth N` projects
files and directories. `du --depth N` enables descendant summaries and
`du --rewind TIME` totals every older data version, excluding delete markers.
Structured inspection marks incomplete result projections with `Partial`.

`ls --versions` applies literal prefix and delimiter selection and uses
`--max-keys` on every page. Its default folder delimiter is `/`; use
`--recursive` for a flat version listing. Historical `--rewind` views stay flat
and reject an explicit delimiter before requesting metadata.

`pipe TARGET --dryrun` reports the upload without reading stdin or creating a
spool. Its accepted options control the destination upload, progress and expected
size; copy-source, filter and listing options fail before I/O.

`anonymous get-json` retains the full bucket policy. `anonymous set-json FILE TARGET`
requires `--replace` when a policy exists; inspect its `--dry-run` first. `list`
reports recognizable policy grants, and `links` requires an unambiguous policy
and complete listing. These are policy inspections, without a claim that IAM,
ACLs, or public-access restrictions permit anonymous access.

Native lifecycle rules support portable mc date/day aliases; replication
conveniences require an explicit role ARN and destination bucket ARN. MinIO
administration extensions retain separate provider boundaries. The compatibility
decision reports record the exact supported spellings and behavior differences.

`ilm restore TARGET` restores selected objects with a default one-day Expedited
request. It accepts recursive/version selection and polls completion with a
bounded attempt count; cancellation and partial failures return nonzero. It
requires a provider that supports S3 restore.

Native `mb` supports `--ignore-existing`, `--with-lock` and `--with-versioning`.
Lock is requested during creation; versioning is a separate operation. A later
versioning error preserves completed creation and does not delete the bucket.
Deprecated `ilm add|edit|ls|rm|export|import` and `encrypt clear|info` reuse the
corresponding tested rule/encryption handlers.

`legalhold set|clear|info TARGET` and
`retention set governance 30d TARGET` support bounded version-aware selection.
Retention also accepts compliance mode and calendar years such as `1y`;
`--default` edits an already-enabled bucket lock configuration. Bypass must be
explicit. Full-document set forms remain available and reject convenience-only
selectors. Enumerated mutations require confirmed immutable object versions.

Native shares accept `--expire 1h30m` or legacy `--expires` seconds. Download
shares support selected versions and bounded recursive inventories; recursive
uploads generate POST grants for a nonempty key prefix. Private history is
limited to 1000 active entries and 1 MiB. `share list download|upload` is offline
and redacted by default; `--reveal` prints the signed capability. History uses
the selected config directory with private permissions. Expired entries are
hidden and pruned on the next successful write; local deletion is not revocation.

## Shell completion

Completion is generated from the same offline registry as parsing/help. It follows
native command groups, `s3`, modeled `s3api` operations and waiters, and the
options applicable to the selected command. Enumerated registry values complete
with separate or `--option=value` syntax. Option values are not interpreted as
commands, and `--` ends option completion.

```sh
# Bash, for the current shell:
source <(ess completion bash)

# Zsh, after initializing its completion system:
autoload -Uz compinit
compinit
source <(ess completion zsh)

# Fish, persistently:
mkdir -p ~/.config/fish/completions
ess completion fish > ~/.config/fish/completions/ess.fish
```

Generate again after upgrading. Generation and completion do not load credentials,
run credential_process, read aliases/profiles, or make requests. Remote bucket/key,
profile-name, and filesystem-value discovery are not offered. Scripts treat token
text literally and do not evaluate it as shell commands.

## Configuration and credentials

Native configuration remains at `~/.config/ess/config.toml`; `ESS_CONFIG_DIR` overrides it, otherwise `XDG_CONFIG_HOME/ess` is used when set. Existing `[default]` and `[profiles.<name>]` tables remain readable. Updates preserve unrelated TOML and use private atomic writes. Named endpoint records may refer to ESS profiles, AWS profiles, environment credentials, anonymous access, or explicit static credentials. Prefer profile/environment references to embedding secrets.

Native `--config-dir` / `-C` selects a configuration directory for one invocation.
`--disable-pager` / `--dp` disables paging. `--insecure` disables TLS verification
with a stderr warning. Repeated `--custom-header` / `-H` values use `Name: value`
syntax and are validated before signing; authentication and framing fields are
reserved. `--limit-upload` and `--limit-download` accept rates such as `10MiB`
per second, with zero meaning unlimited. Concurrent requests share each endpoint's
budget; source and destination have independent limits. Socket inactivity flags
remain unavailable until the transport dependency supports their exact contract.
Repeated `--resolve HOST[:PORT]=literal-IP` entries pin socket addresses while
preserving the signed Host, TLS SNI and certificate identity. A port-specific
entry wins over a host-only entry, and failed pins do not fall back to DNS.

AWS namespaces read `AWS_CONFIG_FILE`/`AWS_SHARED_CREDENTIALS_FILE`, defaulting to `~/.aws/config` and `~/.aws/credentials`. Explicit command options take precedence over applicable environment and selected-profile settings. `--profile`, `AWS_PROFILE`, and `AWS_DEFAULT_PROFILE` choose the profile. Explicit profile selection changes credential precedence; providers keep access key, secret, and token as one record. Supported provider paths include environment/shared credentials, role/web identity, credential_process, IAM Identity Center, ECS, and IMDS. `--no-sign-request` skips credential loading. See the repository credential compatibility report for exact tested modes and remaining live-service gaps.

| Variable | Effect/default |
| --- | --- |
| `ESS_CONFIG_DIR`, `XDG_CONFIG_HOME` | Native configuration directory overrides |
| `ESS_STATE_DIR` | Native durable transfer state root; takes precedence over XDG |
| `XDG_STATE_HOME` | State root is `<value>/ess`; otherwise `~/.local/state/ess` |
| `AWS_ACCESS_KEY_ID`, `AWS_SECRET_ACCESS_KEY`, `AWS_SESSION_TOKEN` | Coherent temporary/static environment credentials |
| `AWS_PROFILE`, `AWS_DEFAULT_PROFILE` | Selected AWS profile; default `default` |
| `AWS_REGION`, `AWS_DEFAULT_REGION` | AWS region settings |
| `AWS_CONFIG_FILE`, `AWS_SHARED_CREDENTIALS_FILE` | Shared configuration/credentials paths |
| `AWS_ENDPOINT_URL_S3`, `AWS_ENDPOINT_URL` | Applicable explicit service/global endpoint settings |
| `AWS_ENDPOINT_URL_S3_CONTROL`, `AWS_ENDPOINT_URL_STS` | Independent control endpoints for access-point move validation; data `--endpoint-url` does not override them |
| `AWS_IGNORE_CONFIGURED_ENDPOINT_URLS` | Ignore configured service/global endpoints when true |
| `AWS_CLI_S3_MV_VALIDATE_SAME_S3_PATHS` | Enable underlying access-point/MRAP path checks for AWS moves with the exact case-insensitive value `true` |
| `AWS_S3_USE_ARN_REGION` | Allow the applicable Outposts ARN region policy |
| `AWS_CA_BUNDLE` | Trusted CA bundle; TLS peer verification is enabled by default |
| `AWS_DEFAULT_OUTPUT` | Structured output format when no command option overrides it |
| `AWS_RETRY_MODE` | `standard` (default) or `adaptive`; pinned `legacy` is rejected |
| `AWS_MAX_ATTEMPTS` | Total attempts including the initial request; default 3 |
| `AWS_USE_FIPS_ENDPOINT`, `AWS_USE_DUALSTACK_ENDPOINT` | Boolean endpoint-resolution settings |
| `AWS_REQUEST_CHECKSUM_CALCULATION` | `when_supported` or `when_required` |
| `AWS_RESPONSE_CHECKSUM_VALIDATION` | `when_supported` or `when_required` |
| `AWS_EC2_METADATA_DISABLED` | Disables instance metadata credentials when true |
| `AWS_EC2_METADATA_V1_DISABLED` | Forbids permitted IMDSv1 fallback when true |
| `AWS_CLI_AUTO_PROMPT` | Explicit `on`, `on-partial`, or `off` (default); requires a terminal |
| `AWS_PAGER` | TTY pager command; empty disables; precedes profile `cli_pager`, `PAGER`, then `less` |
| `PAGER`, `LESS` | Pager fallback; `LESS=FRX` supplied only when absent |
| `CC` | Build-time C compiler executable; defaults to `cc` |
| `ESS_RESOURCE_LARGE_BYTES` | Test-only transfer acceptance object size in bytes; default `67108864`, multi-GiB acceptance `2147483648` |

Selected-profile `cli_binary_format` is resolved before parsing blob inputs; `--cli-binary-format` overrides it, default `base64`. Use `fileb://path` for raw bytes. No `AWS_CLI_BINARY_FORMAT` environment variable is introduced. JSON/YAML/file/shorthand inputs, skeleton generation, JMESPath queries, and structured output use pinned packaged models and parsers. Auto-prompt asks required questions and confirmation on stderr; its bounded question UI differs from AWS's full-screen editor. Sensitive input disables echo before showing its field label.

## Output and safe operations

Stdout contains requested data; diagnostics, prompts, and progress belong on stderr. Raw `cat`, `pipe`, and stdout downloads preserve binary bytes and bypass structured paging. `--no-cli-pager` disables paging; auto color is disabled for redirected output. Interactive pager output uses a private 64 MiB bounded spool and a five-minute pager deadline. A missing, failed, or timed-out pager returns an error and cleans its owned processes/files.

Inspect synchronization with `ess sync SOURCE DEST --dryrun` before enabling deletion. Exclusions protect destination entries, and incomplete enumeration or failed transfers prevent the deletion phase. Native alias roots are not implicit delete-all permission. Retain local source and durable transfer records until multipart completion is confirmed; inspect `ess transfer --help` for supported recovery operations. A failed abort can leave an upstream upload that needs explicit cleanup. Never reuse a journal against a changed source or changed provider identity.

For alias backup use `ess alias export > aliases.json`; secrets are redacted by default. Restore with `ess alias import aliases.json --dry-run` before applying. Secret-bearing export/import and replacement require explicit flags. A redacted static-credential record cannot restore credentials; profile references can. Preserve native config and state directories before upgrades; this client does not automatically import mc config or alter AWS config.

The source repository includes `docs/cli/compatibility/` with generated command inventories, provider limitations, evidence, and deliberate differences, plus `docs/cli/design.md` and `docs/cli/implement-plan.md`. These reports must agree with any release claim. Server metadata migration, cluster control, repair, and node drain use server Mix tasks/release RPC, outside this client.

## License

MIT — see [LICENSE](LICENSE). Vendored reference/model/parser notices are included under `priv/compatibility/NOTICE.txt` and the relevant source directories.
