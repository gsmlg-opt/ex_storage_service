defmodule ExStorageService.Application do
  @moduledoc false

  use Application
  require Logger

  @impl true
  def start(_type, _args) do
    config = instance_config!()

    # Wait for Concord/VSR to be ready before starting services.
    wait_for_concord()

    # Initialize metrics collection
    ExStorageService.Metrics.setup()

    opts = [strategy: :one_for_one, name: ExStorageService.Supervisor]
    result = Supervisor.start_link(children(config), opts)

    if config.auto_start and Code.ensure_loaded?(Mix) and Mix.env() == :dev do
      Task.start(fn -> seed_dev_keys() end)
    end

    result
  end

  @doc false
  def children(config) do
    infrastructure = [
      {Registry, keys: :unique, name: ExStorageService.Names.registry()},
      {Phoenix.PubSub, name: ExStorageService.PubSub},
      {Task.Supervisor, name: ExStorageService.NotificationTaskSupervisor}
    ]

    if config.auto_start do
      infrastructure ++ [{ExStorageService, config}]
    else
      infrastructure
    end
  end

  defp instance_config! do
    case ExStorageService.InstanceConfig.from_application_env() do
      {:ok, config} ->
        config

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

  defp wait_for_concord(attempts \\ 50) do
    case Concord.get("__health_check__") do
      {:ok, _} ->
        Logger.info("Concord VSR metadata store ready")

      {:error, :not_found} ->
        Logger.info("Concord VSR metadata store ready")

      {:error, :cluster_not_ready} when attempts > 0 ->
        Process.sleep(100)
        wait_for_concord(attempts - 1)

      other ->
        Logger.warning("Concord readiness check result: #{inspect(other)}, proceeding anyway")
    end
  end
end
