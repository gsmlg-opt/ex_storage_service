defmodule ExStorageService do
  @moduledoc """
  ExStorageService - An S3-compatible object storage server.

  Provides an embeddable storage supervision tree plus the standalone S3
  applications shipped by this umbrella.

  A host can configure `auto_start: false` and supervise
  `ExStorageService.child_spec/1` itself. Concord and Ra remain shared,
  one-per-BEAM infrastructure in this phase.
  """

  alias ExStorageService.{Context, InstanceConfig, InstanceSupervisor}

  @spec child_spec(keyword() | map() | InstanceConfig.t()) :: Supervisor.child_spec()
  def child_spec(opts) do
    instance =
      case opts do
        %InstanceConfig{instance: instance} -> instance
        map when is_map(map) -> Map.get(map, :instance, :default)
        keyword when is_list(keyword) -> Keyword.get(keyword, :instance, :default)
      end

    %{
      id: {__MODULE__, instance},
      start: {__MODULE__, :start_link, [opts]},
      type: :supervisor,
      restart: :permanent,
      shutdown: :infinity
    }
  end

  @spec start_link(keyword() | map() | InstanceConfig.t()) :: Supervisor.on_start()
  def start_link(opts) do
    with {:ok, config} <- InstanceConfig.new(opts),
         context = Context.new(config),
         :ok <- Context.validate_shared_metadata_roots(context) do
      InstanceSupervisor.start_link(context)
    else
      {:error, reason} -> {:error, {:invalid_instance_config, reason}}
    end
  end

  @spec context(keyword() | map() | InstanceConfig.t()) ::
          {:ok, Context.t()} | {:error, String.t()}
  def context(opts) do
    with {:ok, config} <- InstanceConfig.new(opts) do
      {:ok, Context.new(config)}
    end
  end
end
