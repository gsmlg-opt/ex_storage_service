defmodule ExStorageService.Application do
  @moduledoc false

  use Application
  require Logger

  @impl true
  def start(_type, _args) do
    data_root =
      Application.get_env(:ex_storage_service, :data_root, "/tmp/ex_storage_service/data")

    # Ensure data directories exist
    File.mkdir_p!(data_root)

    # Ensure Ra default system is started (needed for Concord)
    ensure_ra_system()

    # Handle stale Ra state from prior runs (e.g., :not_new error)
    maybe_recover_concord()

    # Wait for Concord/Ra to be ready before starting services
    wait_for_concord()

    # Initialize metrics collection
    ExStorageService.Metrics.setup()

    children = [
      {ExStorageService.Storage.Engine, data_root: data_root},
      {Phoenix.PubSub, name: ExStorageService.PubSub},
      ExStorageService.Storage.MultipartGC,
      ExStorageService.Storage.ContentGC,
      ExStorageService.Replication.JobQueue,
      ExStorageService.Replication.Sync,
      {Task.Supervisor, name: ExStorageService.NotificationTaskSupervisor},
      ExStorageService.Storage.Lifecycle
    ]

    opts = [strategy: :one_for_one, name: ExStorageService.Supervisor]
    Supervisor.start_link(children, opts)
  end

  # If Concord failed to start (e.g., :not_new from stale Ra state),
  # attempt recovery by restarting the existing Ra server.
  defp maybe_recover_concord do
    case Concord.get("__health_check__") do
      {:ok, _} ->
        :ok

      {:error, :cluster_not_ready} ->
        cluster_name = Application.get_env(:concord, :cluster_name, :concord_cluster)
        node_id = {cluster_name, node()}

        case :ra.restart_server(node_id) do
          :ok ->
            Logger.info("Recovered Concord cluster via Ra restart")
            :ra.trigger_election(node_id)

          {:ok, _} ->
            Logger.info("Recovered Concord cluster via Ra restart")
            :ra.trigger_election(node_id)

          {:error, reason} ->
            Logger.warning("Concord recovery attempt failed: #{inspect(reason)}")
        end

      _ ->
        :ok
    end
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
end
