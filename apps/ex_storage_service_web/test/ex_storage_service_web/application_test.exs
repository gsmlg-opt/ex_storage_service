defmodule ExStorageServiceWeb.ApplicationTest do
  use ExUnit.Case, async: true

  alias ExStorageServiceWeb.Application

  test "includes the endpoint by default" do
    assert Application.children(enabled: true) == [ExStorageServiceWeb.Endpoint]
  end

  test "omits the endpoint when the web listener is disabled" do
    assert Application.children(enabled: false) == []
  end

  test "metadata role cannot start the web endpoint" do
    assert Application.children(enabled: true, node_role: :metadata) == []
  end

  @tag :tmp_dir
  test "a host can supervise core while both listeners are disabled", %{tmp_dir: tmp_dir} do
    instance = "listener-free-#{System.unique_integer([:positive])}"

    workers =
      ExStorageService.InstanceConfig.worker_defaults()
      |> Map.new(fn {worker, _enabled} -> {worker, false} end)

    core_opts = [
      instance: instance,
      auto_start: false,
      public_s3_enabled: false,
      web_enabled: false,
      data_root: Path.join(tmp_dir, "data"),
      blob_root: Path.join(tmp_dir, "blobs"),
      tmp_root: Path.join(tmp_dir, "staging"),
      ra_root: Elixir.Application.fetch_env!(:ex_storage_service, :ra_root),
      metadata_root: Elixir.Application.fetch_env!(:ex_storage_service, :metadata_root),
      workers: workers
    ]

    children =
      ExStorageServiceS3.Application.children(enabled: false) ++
        Application.children(enabled: false) ++ [{ExStorageService, core_opts}]

    assert [{ExStorageService, ^core_opts}] = children
    assert {:ok, host} = Supervisor.start_link(children, strategy: :one_for_one)
    Process.unlink(host)

    on_exit(fn ->
      if Process.alive?(host), do: Supervisor.stop(host)
    end)

    assert [{{ExStorageService, ^instance}, instance_pid, :supervisor, [ExStorageService]}] =
             Supervisor.which_children(host)

    assert Process.alive?(instance_pid)
    assert Process.alive?(host)
  end
end
