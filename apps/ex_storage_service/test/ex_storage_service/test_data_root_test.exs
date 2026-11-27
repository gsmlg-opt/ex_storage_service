defmodule ExStorageService.TestDataRootTest do
  use ExUnit.Case, async: true

  @project Path.expand("../../../../mix.exs", __DIR__)
  @probe """
  [project] = System.argv()
  Mix.start()
  Mix.env(:test)
  Code.require_file(project)
  runtime = Path.join(Path.dirname(project), "config/runtime.exs")
  Mix.Tasks.Loadconfig.load_runtime(runtime)
  [prepare | _] = Mix.Project.config()[:aliases][:test]
  prepare.([])

  root = System.fetch_env!("ESS_DATA_ROOT")
  config = Application.fetch_env!(:ex_storage_service, :instance_config)
  for {key, suffix} <- [data_root: "", blob_root: "cas", tmp_root: "cas/tmp",
                         ra_root: "ra", metadata_root: "concord"] do
    expected = Path.expand(suffix, root)
    ^expected = Keyword.fetch!(config, key)
    ^expected = Application.fetch_env!(:ex_storage_service, key)
  end
  true = File.dir?(root)
  true = Application.fetch_env!(:concord, :data_dir) == Path.join(root, "concord")
  for key <- ~w(ESS_BLOB_ROOT ESS_TMP_ROOT ESS_RA_ROOT ESS_METADATA_ROOT) do
    nil = System.get_env(key)
  end
  IO.write(JSON.encode!(root))
  """

  @tag :tmp_dir
  test "concurrent umbrella tests own distinct roots and preserve all supplied directories", %{
    tmp_dir: tmp
  } do
    env =
      for name <- ~w(ESS_DATA_ROOT ESS_BLOB_ROOT ESS_TMP_ROOT ESS_RA_ROOT ESS_METADATA_ROOT) do
        path = Path.join(tmp, name)
        File.mkdir!(path)
        File.write!(Path.join(path, "sentinel"), name)
        {name, path}
      end

    roots =
      1..2
      |> Task.async_stream(
        fn _ ->
          System.cmd(System.find_executable("elixir"), ["-e", @probe, "--", @project],
            env: [{"ERL_FLAGS", "+S 2:2 +SDcpu 1 +SDio 1"} | env],
            stderr_to_stdout: true
          )
        end,
        timeout: 30_000
      )
      |> Enum.map(fn {:ok, {output, status}} ->
        assert status == 0, output
        root = JSON.decode!(output)
        refute File.exists?(root)
        root
      end)

    assert length(Enum.uniq(roots)) == 2
    for {name, path} <- env, do: assert(File.read!(Path.join(path, "sentinel")) == name)
  end
end
