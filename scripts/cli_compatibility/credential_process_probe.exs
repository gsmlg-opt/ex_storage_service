[escript, tests | options] = System.argv()
true = options in [[], ["daily"]]
{:ok, sections} = :escript.extract(String.to_charlist(escript), [])
{:archive, archive} = List.keyfind(sections, :archive, 0)
{:ok, entries} = :zip.extract(archive, [:memory])

# Load only these actual installed artifact modules, without source/build paths.
for module <- [
      ExStorageServiceCli.Error,
      ExStorageServiceCli.Credentials.Record,
      Inspect.ExStorageServiceCli.Credentials.Record,
      ExStorageServiceCli.Credentials.Process
    ] do
  false = :code.is_loaded(module)
  filename = Atom.to_string(module) <> ".beam"

  [{_path, binary}] =
    Enum.filter(entries, fn {path, _binary} ->
      path |> List.to_string() |> Path.basename() == filename
    end)

  {:module, ^module} = :code.load_binary(module, String.to_charlist(escript), binary)
end

if options == ["daily"] do
  root = Path.join(File.cwd!(), "credential-process-daily")
  File.mkdir!(root)
  helper = Path.join(root, "normal helper.sh")
  arguments_file = Path.join(root, "arguments")
  arguments = ["two words", "", "literal $(not-a-command); *", "quote'argument"]

  response =
    ~s({"Version":1,"AccessKeyId":"process-access","SecretAccessKey":"process-secret","SessionToken":"process-token","Expiration":"2030-01-01T00:00:00Z"})

  try do
    File.write!(helper, """
    #!/bin/sh
    record=$1
    shift
    printf '%s\\n' "$@" > "$record"
    printf '%s' '#{response}'
    printf '%s' 'stderr-secret' >&2
    """)

    File.chmod!(helper, 0o700)
    quote_arg = fn value -> "'" <> String.replace(value, "'", "'\\''") <> "'" end
    command = Enum.map_join([helper, arguments_file | arguments], " ", quote_arg)

    {:ok, %{__struct__: ExStorageServiceCli.Credentials.Record} = record} =
      ExStorageServiceCli.Credentials.Process.run(command, now: ~U[2026-10-15 00:00:00Z])

    %{
      access_key_id: "process-access",
      secret_access_key: "process-secret",
      session_token: "process-token",
      expires_at: ~U[2030-01-01 00:00:00Z]
    } = record

    true = File.read!(arguments_file) == Enum.join(arguments, "\n") <> "\n"
    rendered = inspect(record)
    true = String.contains?(rendered, "redacted")

    for secret <- ["process-access", "process-secret", "process-token"] do
      false = String.contains?(rendered, secret)
    end

    evidence = %{
      verification_profile: "daily",
      module_source: "installed-escript",
      os: :os.type() |> elem(1) |> Atom.to_string(),
      architecture: :erlang.system_info(:system_architecture) |> List.to_string(),
      otp: :erlang.system_info(:otp_release) |> List.to_string(),
      total: 1,
      failures: 0,
      skipped: 0,
      excluded: 0,
      checks: ["literal-argv", "typed-credentials", "stderr-discard", "record-redaction"]
    }

    IO.puts("ESS_CREDENTIAL_PROCESS_PROBE=" <> JSON.encode!(evidence))
  after
    File.rm_rf!(root)
  end

  System.halt(0)
end

ExUnit.start(autorun: false, seed: 628_644)
Code.require_file(tests)
summary = ExUnit.run()

evidence = %{
  module_source: "installed-escript",
  os: :os.type() |> elem(1) |> Atom.to_string(),
  architecture: :erlang.system_info(:system_architecture) |> List.to_string(),
  otp: :erlang.system_info(:otp_release) |> List.to_string(),
  total: summary.total,
  failures: summary.failures,
  skipped: summary.skipped,
  excluded: summary.excluded,
  checks: [
    "literal-argv",
    "stderr-discard",
    "bounded-output",
    "child-group-isolation",
    "timeout-descendant-cleanup",
    "owner-death-descendant-cleanup",
    "overflow-descendant-cleanup",
    "prelaunch-owner-monitor"
  ]
}

IO.puts("ESS_CREDENTIAL_PROCESS_PROBE=" <> JSON.encode!(evidence))
if summary.failures != 0 or summary.total < 11, do: System.halt(1)
