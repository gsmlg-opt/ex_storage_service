# Legacy ESS CLI baseline

Captured on 2026-10-15 from `main` at
`9b8661e58107eddb24cb8f15495cee4ac979219a`, CLI `0.6.7`.
This records existing behavior, not complete AWS or mc compatibility.

## Executed evidence

- Original standalone suite: `PAGER=cat mix test`, working directory
  `apps/ex_storage_service_cli`: **40 tests, 0 failures**.
- Added actual-escript baseline, HTTP transport spikes, and initial target tests:
  the same scoped command: **59 tests, 0 failures**, confirmed by the coordinating
  agent after correcting the baseline's tree-root fixture to the observed
  `s3://sample/docs/` spelling.
- `mix test apps/ex_storage_service_cli/test` from the umbrella root stopped
  before tests because unrelated web `build_info` had compiled SHA `364fbe8`
  versus runtime SHA `9b8661e`. That invocation is not counted as passing;
  no validation flag was disabled and no web code was changed to obtain a pass.
- `unbuffer` is unavailable; Mix ran with a PTY instead.

`executable_baseline_test.exs` builds the actual package escript and invokes it
in child Erlang VMs with unique temporary `HOME` directories. Raw TCP fixtures
serve deterministic replies, independently record request methods/paths/bodies,
and require no ESS server, cloud account, AWS executable, or mc executable.
Subprocess output combines stdout/stderr for capture; separate stream ownership
still needs M1 acceptance tests.

## Valid forms and structured results

Global endpoint/profile/credential/region/JSON options currently work **before**
the command. Command-specific flags work after the command. All eleven commands
have executable `ess --help <command>` coverage. JSON examples below list field
names and stable values; timestamps, local paths, and presigned signatures vary.

| Command | Captured valid forms | JSON shape/effect |
| --- | --- | --- |
| `configure` | `configure --profile fixture`, four prompted fields | Writes the named profile; success message names the path; entered secret is not printed |
| `mb` | `--json mb sample` | `{status:"ok", bucket:"sample"}`, PUT bucket |
| `rb` | `--json rb s3://sample/` | `{status:"ok", bucket:"sample"}`, DELETE bucket |
| `ls` | `--json ls`; `--json ls sample/docs/` | Bucket list `{name,creation_date}`; object page `{contents,common_prefixes,is_truncated,next_continuation_token,key_count}` |
| `tree` | `--json tree sample/docs/` | `{bucket,prefix,tree,objects}`; tree root retains `s3://`, then branch lines, blank line, directory/file summary |
| `cp` | Local upload to `s3://sample/docs/`; object download to local path; object copy to another bucket/prefix | `{status,operation,source,destination,size,etag}` for upload; download omits `etag`; copy omits size and resolves destination basename |
| `rm` | `--json rm s3://sample/docs/file.txt` | `{status:"ok",bucket,key}`, DELETE object |
| `mv` | Two `s3://` object targets; destination prefix | `{status:"ok",operation:"move",source,destination}`; COPY followed by source DELETE |
| `presign` | Named profile, `--expires 60 --method HEAD`; explicit endpoint override | `{url,method,expires,bucket,key}`; profile region/access key appear in the signature scope; secret does not appear |
| `info` | `--json info` with explicit endpoint | Unmodified parsed ESS health JSON |
| `version` | `--json version`; plain `version` | `{version:"0.6.7"}` or `ess version 0.6.7`; remains CLI version |

Existing config uses `[default]` and `[profiles.<name>]` TOML sections under
`~/.config/ess/config.toml`. A named profile supplies endpoint, access key,
secret, and region; explicit leading global options override those fields.
The baseline fixture proves named selection and endpoint override. No AWS
environment/shared-profile behavior is implied by these legacy ESS profiles.

Successful captured commands exit 0. Unknown command, missing bucket/object key,
local-to-local copy, missing presign credentials, and an HTTP 403 S3 error exit 1.
Errors contain `Error:` and the service message without printing the fixture
secret. Missing arguments may also print help. The runner currently calls
`System.halt/1` inside handlers; subprocess coverage owns that behavior safely.

## Existing defects and unverified surfaces

These are defects to correct, not compatibility requirements to preserve:

- Source inspection: invalid options are discarded, and appended global options
  (including documented `ls sample --json` / `info --endpoint ...`) do not update
  the global context. `--version` is not implemented. `rb --force` is parsed but
  has no force-delete effect. `version` ignores unexpected arguments.
- Source inspection: configuration read/parse errors become an empty profile;
  writes interpolate unescaped strings, overwrite directly, and rebuild only
  known sections. There are no private-permission, corruption-preservation, or
  locking guarantees.
- Source inspection: upload/download and recursive inventories buffer complete
  objects/lists; recursive destination mapping is not yet secured. `CopyObject`
  accepts HTTP 200 without detecting an embedded service error.
- Existing unit tests cover copy path helpers, tree formatting, signer helpers,
  XML/output helpers, and raw S3 response regressions. They are not complete
  executable coverage of every recursive command/flag/configuration combination.
- Differential AWS/mc executable tests, real-provider evidence, multi-GiB memory
  envelopes, stdout/stderr separation, broken pipe/signal lifecycle, malicious
  XML, and all valid option permutations remain assigned to later milestones.

No baseline fixture authorizes preserving silent option acceptance, unsafe
configuration writes, or successful reporting after an indeterminate transfer.
