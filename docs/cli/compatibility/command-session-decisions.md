# CLI command sessions

Reference: AWS CLI 2.31.18, source revision `f4f66e65e70f23f1bf232ecb4a6d8696f4dda3bc`, `clidriver.py`, `utils.py`, `compat.py`, and `autoprompt/core.py`. This is scoped implementation evidence, not a full compatibility claim.

## Inputs and prompting

The selected shared-config profile is read without loading credentials before blob parsing. Explicit `--cli-binary-format` wins over selected-profile `cli_binary_format`; the default is `base64`. There is no invented `AWS_CLI_BINARY_FORMAT` environment variable. `fileb://` remains raw. Help and skeleton paths skip this configuration read and all credentials/network resolution.

Auto-prompt is opt-in through `--cli-auto-prompt`, `AWS_CLI_AUTO_PROMPT`, or selected-profile `cli_auto_prompt`. Supported modes are `on`, `on-partial`, and `off`; `--no-cli-auto-prompt` overrides inherited configuration. Conflicting positive/negative flags fail. Required questions use the pinned operation model or command registry; confirmation defaults to no. Redirected input fails without execution. Prompts use stderr, input is limited to 64 KiB, and sensitive model/name traits suppress terminal echo **before the label is emitted**. A monitored native resource restores full termios on normal completion and owner death.

The bounded question/confirmation UI intentionally differs from AWS CLI's full prompt-toolkit editor: interactive editing, contextual completion, history, and equivalent screen layout are not implemented. No exact auto-prompt UI parity is claimed.

## Output and pager ownership

Pager precedence is explicit `--no-cli-pager`/raw output, then TTY eligibility, `AWS_PAGER`, selected-profile `cli_pager`, `PAGER`, and `less`. Empty configured values disable it. Unix command parsing produces literal argv without a shell. `LESS=FRX` is supplied only when absent. Auto color becomes off for redirected output; explicit on/off remains explicit. Raw byte output bypasses formatting and paging.

Structured pager output is spooled to an unpredictable mode-0700 directory and mode-0600 file with a 64 MiB limit. The pager starts lazily at finish, reads that file as stdin, and inherits terminal stdout/stderr. Its default session deadline is five minutes. This differs from the reference's streaming stdin pipe: oversized formatted output fails with a typed spool-limit error rather than consuming unlimited local space. Direct output preserves bytes and reports broken pipes as typed errors; the pinned pager code normally ignores its broken pipe.

Erlang Port launch was rejected after a real controlling-PTY probe proved ERTS creates a new session and `/dev/tty` becomes unavailable. The Linux implementation uses `posix_spawn` from the existing NIF, with a separate process group and private lifetime/status pipes. It resets inherited signal masks/dispositions, including SIGCHLD, without changing BEAM's global SIGCHLD handler. A dedicated detached reaper owns only this child. The external single-threaded helper gives the pager foreground ownership, kills its process group on cancellation or lifetime-pipe EOF, restores foreground/termios, and only then publishes completion. Session owner death cancels the pager; files are removed after completion. Native code never forks the multithreaded BEAM.

Packaged helper assets carry source/binary SHA256 plus OS/CPU identity; NIF assets additionally carry OTP-major identity. Extraction verifies bytes and uses private files. Runtime paging is enabled only on Linux. Darwin build code for signal/directory/terminal resources remains present, but Darwin execution and paging are unverified; no macOS pager support claim is made.

## Validation

Linux x86_64, Elixir 1.18.5, OTP 28:

- `mix cmd --app ex_storage_service_cli mix test test/ex_storage_service_cli/cli/aws_input_test.exs test/ex_storage_service_cli/cli/auto_prompt_test.exs test/ex_storage_service_cli/cli/output_session_test.exs --no-start`: **50 tests, 0 failures**.
- `mix cmd --app ex_storage_service_cli mix compile --warnings-as-errors`: passed after native launcher recovery; later shared-file compilation is owned by integration.
- Included real-PTY Python acceptance verifies foreground ownership, exact binary stdin, normal finish, cancellation, active owner death, complete termios restoration, spool removal, and sensitive input absent from terminal output.
- Unit/process checks cover empty/env/profile pager precedence, non-TTY behavior, color, literal shell metacharacters, descendant cleanup, missing/failed/timed-out pager, private spool bounds, direct broken pipes, model/registry prompting, sensitive traits, and offline binary-format selection.

The initial M5 parser/session work used two repair rounds and reached 46 passing checks. The authorized native launcher recovery used two rounds: initial posix_spawn proved terminal access but exposed inherited SIGCHLD behavior; resetting it passed actual PTY and the expanded 50 checks. A PTY fixture ioctl was corrected to inspect the master from its external parent. No macOS/other-architecture runtime, minimum-OTP matrix, or full pinned-executable prompt differential is represented as passing here.
