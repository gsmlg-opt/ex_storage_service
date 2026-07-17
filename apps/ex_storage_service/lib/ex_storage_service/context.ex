defmodule ExStorageService.Context do
  @moduledoc """
  Immutable runtime context for one locally supervised storage instance.

  Concord and Ra remain shared application infrastructure in Phase 3, so one
  BEAM may supervise only contexts that use the already configured metadata
  roots.
  """

  alias ExStorageService.InstanceConfig

  @enforce_keys [
    :instance,
    :config,
    :data_root,
    :blob_root,
    :tmp_root,
    :ra_root,
    :metadata_root,
    :notification_task_supervisor
  ]
  defstruct @enforce_keys

  @type t :: %__MODULE__{
          instance: atom() | String.t(),
          config: InstanceConfig.t(),
          data_root: String.t(),
          blob_root: String.t(),
          tmp_root: String.t(),
          ra_root: String.t(),
          metadata_root: String.t(),
          notification_task_supervisor: atom() | tuple()
        }

  @spec new(InstanceConfig.t()) :: t()
  def new(%InstanceConfig{} = config) do
    %__MODULE__{
      instance: config.instance,
      config: config,
      data_root: Path.expand(config.data_root),
      blob_root: Path.expand(config.blob_root),
      tmp_root: Path.expand(config.tmp_root),
      ra_root: Path.expand(config.ra_root),
      metadata_root: Path.expand(config.metadata_root),
      notification_task_supervisor: ExStorageService.NotificationTaskSupervisor
    }
  end

  @spec default() :: {:ok, t()} | {:error, String.t()}
  def default do
    with {:ok, config} <- InstanceConfig.from_application_env() do
      {:ok, new(config)}
    end
  end

  @spec blob_store_options(t()) :: keyword()
  def blob_store_options(%__MODULE__{} = context) do
    [
      root: context.blob_root,
      tmp_dir: Path.join(context.tmp_root, "uploads"),
      data_root: context.data_root
    ]
  end

  @spec validate_shared_metadata_roots(t()) :: :ok | {:error, String.t()}
  def validate_shared_metadata_roots(%__MODULE__{} = context) do
    with :ok <- compare_root(:ra_root, context.ra_root),
         :ok <- compare_root(:metadata_root, context.metadata_root) do
      :ok
    end
  end

  defp compare_root(key, requested) do
    configured =
      Application.get_env(:ex_storage_service, key, requested)
      |> to_string()
      |> Path.expand()

    if configured == requested do
      :ok
    else
      {:error,
       "#{key} is application infrastructure and cannot differ per child " <>
         "(configured #{configured}, requested #{requested})"}
    end
  end
end
