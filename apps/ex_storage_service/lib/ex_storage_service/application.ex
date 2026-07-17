defmodule ExStorageService.Application do
  @moduledoc false

  use Application
  require Logger

  @impl true
  def start(_type, _args) do
    validate_instance_config!()

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

    # Single-node deployment: stop Concord's gossip discovery
    stop_unwanted_clustering()

    # Initialize metrics collection
    ExStorageService.Metrics.setup()

    children = [
      {ExStorageService.Storage.Engine, data_root: data_root},
      {Phoenix.PubSub, name: ExStorageService.PubSub},
      ExStorageService.Storage.MultipartGC,
      ExStorageService.Storage.ContentGC,
      ExStorageService.Storage.CasGC,
      ExStorageService.Storage.Packer,
      ExStorageService.Replication.JobQueue,
      ExStorageService.Replication.Sync,
      {Task.Supervisor, name: ExStorageService.NotificationTaskSupervisor},
      ExStorageService.Storage.Lifecycle
    ]

    opts = [strategy: :one_for_one, name: ExStorageService.Supervisor]
    res = Supervisor.start_link(children, opts)

    if Code.ensure_loaded?(Mix) and Mix.env() == :dev do
      Task.start(fn -> seed_dev_keys() end)
    end

    res
  end

  defp validate_instance_config! do
    case ExStorageService.InstanceConfig.from_application_env() do
      {:ok, _config} ->
        :ok

      {:error, message} ->
        raise ArgumentError, "invalid ExStorageService configuration: #{message}"
    end
  end

  defp seed_dev_keys do
    alias ExStorageService.IAM.{User, Policy, AccessKey}

    dev_user_name = "dev-user"
    access_key_id = "AKIA-DEV-ACCESS-KEY"
    secret_access_key = "DEV-SECRET-ACCESS-KEY-DO-NOT-USE"

    # Check if the fixed dev access key already exists — if so, seeding is done.
    case AccessKey.lookup_by_access_key_id(access_key_id) do
      {:ok, _key} ->
        Logger.debug("Dev access key already seeded, skipping.")

      _ ->
        {:ok, user} = User.create_user(dev_user_name)

        policy =
          case Policy.get_policy("dev-full-access") do
            {:ok, p} ->
              p

            {:error, :not_found} ->
              {:ok, p} = Policy.create_policy("dev-full-access", Policy.full_access_statements())
              p
          end

        Policy.attach_policy(user.id, policy.id)
        AccessKey.create_fixed_access_key(user.id, access_key_id, secret_access_key)

        Logger.info(
          "Seeded dev user '#{dev_user_name}' (#{user.id}) with access key: #{access_key_id}"
        )
    end
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

        case :ra.restart_server(:default, node_id) do
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

  # WORKAROUND(upstream): gsmlg-dev/concord#11
  # Concord hardcodes a libcluster Gossip topology (no secret, default multicast
  # group) and exposes no config to disable it, so it discovers unrelated nodes
  # on the LAN and logs repeated "not part of network" warnings — with a latent
  # risk of joining a foreign cluster. This is a single-node deployment, so we
  # shut the discovery supervisor down once Concord is up. Remove when concord#11
  # ships a config knob to disable/isolate clustering.
  defp stop_unwanted_clustering do
    with pid when is_pid(pid) <- Process.whereis(Concord.ClusterSupervisor),
         :ok <- Supervisor.terminate_child(Concord.Supervisor, Cluster.Supervisor) do
      _ = Supervisor.delete_child(Concord.Supervisor, Cluster.Supervisor)
      Logger.info("Stopped Concord libcluster gossip discovery (single-node deployment)")
    else
      _ -> :ok
    end
  end

  defp ensure_ra_system do
    case :ra_system.fetch(:default) do
      {:ok, _} ->
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
