defmodule ExStorageService.Application do
  @moduledoc false

  use Application
  require Logger

  @impl true
  def start(_type, _args) do
    s3_port = Application.get_env(:ex_storage_service, :s3_port, 9000)
    data_root = Application.get_env(:ex_storage_service, :data_root, "/tmp/ex_storage_service/data")

    # Ensure data directories exist
    File.mkdir_p!(data_root)

    # Ensure Ra default system is started (needed for Concord)
    ensure_ra_system()

    # Wait for Concord/Ra to be ready before starting services
    wait_for_concord()

    children = [
      {ExStorageService.Storage.Engine, data_root: data_root},
      {Bandit, plug: ExStorageService.S3.Router, port: s3_port, scheme: :http},
      {Phoenix.PubSub, name: ExStorageService.PubSub},
      ExStorageServiceWeb.Endpoint,
      ExStorageService.Storage.MultipartGC,
      ExStorageService.Replication.JobQueue,
      ExStorageService.Replication.Sync
    ]

    opts = [strategy: :one_for_one, name: ExStorageService.Supervisor]
    Supervisor.start_link(children, opts)
  end

  defp ensure_ra_system do
    case :ra_system.fetch(:default) do
      %{} ->
        Logger.info("Ra default system already running")

      _ ->
        Logger.info("Starting Ra default system")
        :ra_system.start_default()
    end
  end

  defp wait_for_concord(attempts \\ 50) do
    case Concord.get("__health_check__") do
      {:ok, _} ->
        Logger.info("Concord cluster ready")

      {:error, :cluster_not_ready} when attempts > 0 ->
        Process.sleep(100)
        wait_for_concord(attempts - 1)

      other ->
        Logger.warning("Concord readiness check result: #{inspect(other)}, proceeding anyway")
    end
  end

  @impl true
  def config_change(changed, _new, removed) do
    ExStorageServiceWeb.Endpoint.config_change(changed, removed)
    :ok
  end
end
