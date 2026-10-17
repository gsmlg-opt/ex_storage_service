[escript, tests] = System.argv()
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
