defmodule ExStorageService.BlobStore.LocalCASStagingRecoveryTest do
  use ExUnit.Case, async: false

  alias ExStorageService.BlobStore.LocalCAS

  defmodule CollidingFileSystem do
    alias ExStorageService.BlobStore.LocalCAS.FileSystem
    defdelegate mkdir_p(path), to: FileSystem
    defdelegate stat(path), to: FileSystem
    defdelegate rm(path), to: FileSystem
    defdelegate write(io, data), to: FileSystem
    defdelegate close(io), to: FileSystem

    def open(path, modes) do
      File.write!(path, "existing protected stage")
      Process.put(:colliding_stage, path)
      FileSystem.open(path, modes)
    end
  end

  @tag :tmp_dir
  test "exclusive creation failure preserves a stage created by another owner", %{tmp_dir: root} do
    opts = [tmp_dir: root, fs_module: CollidingFileSystem]
    assert {:error, {:stage, :eexist}} = LocalCAS.stage("replacement", opts)
    assert File.read!(Process.get(:colliding_stage)) == "existing protected stage"

    reader = fn state -> {:ok, "replacement", state + 1} end
    assert {:error, {:stage, :eexist}, 0} = LocalCAS.stage_from_reader(reader, 0, opts)
    assert File.read!(Process.get(:colliding_stage)) == "existing protected stage"
  end

  @tag :tmp_dir
  test "a fresh BEAM never truncates a completed stage owned by an earlier BEAM", %{tmp_dir: root} do
    first = stage_in_beam(root, "recoverable journal bytes")
    second = stage_in_beam(root, "new upload")

    assert File.read!(first) == "recoverable journal bytes"
    assert second != first
    assert File.read!(second) == "new upload"
  end

  @tag :tmp_dir
  test "a fresh BEAM never overwrites a partial stage left by whole-BEAM failure", %{
    tmp_dir: root
  } do
    script = ~S"""
    chunks = Stream.map([:first, :crash], fn
      :first -> "partial bytes before crash"
      :crash -> System.halt(137)
    end)
    ExStorageService.BlobStore.LocalCAS.stage(chunks, tmp_dir: System.fetch_env!("ESS_STAGE_TEST_ROOT"))
    """

    assert {_output, 137} = beam(script, root)
    [name] = File.ls!(root)
    partial = Path.join(root, name)
    next = stage_in_beam(root, "later upload")

    assert File.read!(partial) == "partial bytes before crash"
    assert next != partial
    assert File.read!(next) == "later upload"
  end

  defp stage_in_beam(root, body) do
    script = ~S"""
    {:ok, staged} = ExStorageService.BlobStore.LocalCAS.stage(
      System.fetch_env!("ESS_STAGE_TEST_BODY"), tmp_dir: System.fetch_env!("ESS_STAGE_TEST_ROOT"))
    IO.puts(staged.path)
    """

    assert {output, 0} = beam(script, root, [{"ESS_STAGE_TEST_BODY", body}])
    String.trim(output)
  end

  defp beam(script, root, extra_env \\ []) do
    paths =
      Enum.flat_map([:ex_storage_service, :telemetry], fn app ->
        ["-pa", app |> :code.lib_dir() |> to_string() |> Path.join("ebin")]
      end)

    System.cmd(System.find_executable("elixir"), paths ++ ["-e", script],
      env: [{"ERL_FLAGS", "+S 1:1 +SDcpu 1 +SDio 1"}, {"ESS_STAGE_TEST_ROOT", root}] ++ extra_env,
      stderr_to_stdout: true
    )
  end
end
