Code.require_file("index_setup_diagnostics.exs", __DIR__)
ExUnit.start()

defmodule ESSIndexSetupTest do
  use ExUnit.Case, async: false

  setup do
    path =
      Path.join(
        System.tmp_dir!(),
        "ess-index-progress-#{System.unique_integer([:positive])}.jsonl"
      )

    previous = System.get_env("ESS_INDEX_PROGRESS")
    System.put_env("ESS_INDEX_PROGRESS", path)

    on_exit(fn ->
      if previous,
        do: System.put_env("ESS_INDEX_PROGRESS", previous),
        else: System.delete_env("ESS_INDEX_PROGRESS")

      File.rm(path)
    end)

    %{path: path}
  end

  test "progress confirms only successful operations and retains a bounded latency window", %{
    path: path
  } do
    result =
      ESSIndexSetup.phase("test", 205, fn ordinal ->
        ESSIndexSetup.backend(:get, "bucket:test", fn -> {:ok, %{value: ordinal}} end)
        ESSIndexSetup.backend(:transaction, %{}, fn -> {:ok, %{succeeded: ordinal != 1}} end)
        {:ok, ordinal}
      end)

    assert result.completed == 205
    assert result.backend["get:bucket"].calls == 205
    assert result.backend["transaction:specification"].calls == 205
    assert result.backend["transaction:specification"].conflicts == 1
    assert result.backend["get:bucket"].returned_bytes > 0

    rows = path |> File.read!() |> String.split("\n", trim: true) |> Enum.map(&JSON.decode!/1)
    progress = Enum.filter(rows, &(&1["event"] == "progress"))
    assert Enum.map(progress, & &1["completed"]) == [100, 200, 205]
    assert Enum.map(progress, & &1["window"]["count"]) == [100, 100, 5]
    refute Enum.any?(rows, &(&1["event"] == "backend_error"))
  end

  test "a failed commit never emits a successful final progress row", %{path: path} do
    assert_raise MatchError, fn ->
      ESSIndexSetup.phase("test-failure", 2, fn _ -> {:error, :injected} end)
    end

    [start, failure] =
      path |> File.read!() |> String.split("\n", trim: true) |> Enum.map(&JSON.decode!/1)

    assert start["event"] == "start"
    assert failure["event"] == "error"
    assert failure["ordinal"] == 1
    assert failure["completed"] == 0
    assert failure["error"] == %{"class" => "error", "reason" => "redacted"}
  end

  test "failed backend returns preserve their result and report the exact failed ordinal", %{
    path: path
  } do
    failure = {:error, :timeout}

    exception =
      assert_raise MatchError, fn ->
        ESSIndexSetup.phase("failure-ordinal", 10, fn ordinal ->
          ESSIndexSetup.backend(:get, "bucket:private-key", fn ->
            if ordinal == 3, do: failure, else: {:ok, %{value: "private-value"}}
          end)
        end)
      end

    assert exception.term == failure

    [start, backend, phase] =
      path |> File.read!() |> String.split("\n", trim: true) |> Enum.map(&JSON.decode!/1)

    assert start["event"] == "start"
    assert backend["event"] == "backend_error"
    assert backend["kind"] == "get"
    assert backend["category"] == "bucket"
    assert backend["duration_us"] >= 0
    assert backend["error"] == %{"class" => "error", "reason" => "timeout"}
    assert phase["event"] == "error"
    assert phase["ordinal"] == 3
    assert phase["completed"] == 2
    assert phase["planned"] == 10
    assert phase["duration_us"] >= backend["duration_us"]
    assert phase["backend"]["get:bucket"]["calls"] == 3
    assert phase["error"] == backend["error"]
    refute File.read!(path) =~ "private-key"
    refute File.read!(path) =~ "private-value"
  end

  test "failure diagnostics redact arbitrary returns and stop at the setup boundary", %{
    path: path
  } do
    secret = String.duplicate("private-fixture-value", 1_000)

    for result <- [{:error, {:private_reason, secret}}, {:unexpected, %{secret: secret}}] do
      assert ESSIndexSetup.backend(:private_kind, secret, fn -> result end) == result
      refute File.exists?(path)

      exception =
        assert_raise MatchError, fn ->
          ESSIndexSetup.phase("private-failure", 1, fn _ ->
            ESSIndexSetup.backend(:private_kind, %{private_spec: secret}, fn -> result end)
          end)
        end

      assert exception.term == result
      contents = File.read!(path)

      [start, backend, phase] =
        contents |> String.split("\n", trim: true) |> Enum.map(&JSON.decode!/1)

      assert start["event"] == "start"
      assert backend["kind"] == "other"
      assert backend["category"] == "specification"
      assert backend["error"]["reason"] == "redacted"
      assert phase["error"] == backend["error"]
      assert map_size(phase["backend"]) == 1
      assert phase["backend"]["other:specification"]["calls"] == 1
      assert Enum.all?(String.split(contents, "\n", trim: true), &(byte_size(&1) <= 65_536))
      refute contents =~ "private-fixture-value"
      refute contents =~ "private_kind"
      refute contents =~ "private_reason"
      refute contents =~ "private_spec"
      assert Process.get(:index_setup_phase) == nil
      assert Process.get(:index_setup_backend) == nil
      assert ESSIndexSetup.backend(:private_kind, secret, fn -> result end) == result
      assert File.read!(path) == contents
      File.rm!(path)
    end
  end

  test "raised failures retain their exception and clear setup diagnostic state", %{path: path} do
    assert_raise RuntimeError, "private-exception", fn ->
      ESSIndexSetup.phase("raised-failure", 1, fn _ -> raise "private-exception" end)
    end

    assert Process.get(:index_setup_phase) == nil
    assert Process.get(:index_setup_backend) == nil
    refute File.read!(path) =~ "private-exception"
  end

  test "latency summaries use only actual window samples" do
    assert %{count: 5, p50_us: 3, p95_us: 100, maximum_us: 100} =
             ESSIndexSetup.latency([4, 1, 100, 2, 3])
  end

  test "zero-sized phases are rejected rather than invoking a descending range" do
    assert_raise FunctionClauseError, fn ->
      ESSIndexSetup.phase("zero", 0, fn _ -> flunk() end)
    end
  end

  test "persistence observes the published sanitized replica directory", %{path: path} do
    root = path <> "-storage"
    configure_storage(root, replica_id: :"index@127.0.0.1", storage: :file)
    storage = Path.join([root, "vsr", "index_127.0.0.1"])
    File.mkdir_p!(storage)
    File.write!(Path.join(storage, "replica.wal"), "wal")
    File.write!(Path.join(storage, "checkpoint.vsr"), "checkpoint")

    assert ESSIndexSetup.runtime().persistence == %{
             "replica.wal" => 3,
             "checkpoint.vsr" => 10
           }

    File.rm!(Path.join(storage, "checkpoint.vsr"))
    assert ESSIndexSetup.runtime().persistence == %{"replica.wal" => 3, "checkpoint.vsr" => nil}

    Application.put_env(:concord, :vsr, storage: :file)
    replica = node() |> to_string() |> String.replace(~r/[^a-zA-Z0-9_.-]/, "_")
    default_storage = Path.join([root, "vsr", replica])
    File.mkdir_p!(default_storage)
    File.write!(Path.join(default_storage, "replica.wal"), "default-wal")

    assert ESSIndexSetup.runtime().persistence == %{
             "replica.wal" => 11,
             "checkpoint.vsr" => nil
           }
  end

  test "persistence honors configured file paths and skips non-file storage", %{path: path} do
    root = path <> "-storage"
    storage = Path.join(root, "custom")
    File.mkdir_p!(storage)
    File.write!(Path.join(storage, "replica.wal"), "custom-wal")

    configure_storage(root, storage: :file, storage_path: storage)
    assert ESSIndexSetup.runtime().persistence == %{"replica.wal" => 10, "checkpoint.vsr" => nil}

    Application.put_env(:concord, :vsr,
      storage: {ViewstampedReplication.Storage.File, path: storage}
    )

    assert ESSIndexSetup.runtime().persistence == %{"replica.wal" => 10, "checkpoint.vsr" => nil}

    Application.put_env(:concord, :vsr, storage: :memory, storage_path: storage)
    assert ESSIndexSetup.runtime().persistence == nil
  end

  defp configure_storage(root, opts) do
    previous_root = Application.fetch_env(:concord, :data_dir)
    previous_vsr = Application.fetch_env(:concord, :vsr)
    Application.put_env(:concord, :data_dir, root)
    Application.put_env(:concord, :vsr, opts)

    on_exit(fn ->
      restore_env(:data_dir, previous_root)
      restore_env(:vsr, previous_vsr)
      File.rm_rf!(root)
    end)
  end

  defp restore_env(key, {:ok, value}), do: Application.put_env(:concord, key, value)
  defp restore_env(key, :error), do: Application.delete_env(:concord, key)
end
