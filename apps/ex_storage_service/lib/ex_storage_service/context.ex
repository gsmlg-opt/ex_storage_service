defmodule ExStorageService.Context do
  @moduledoc """
  Immutable runtime context for one locally supervised storage instance.

  Concord/VSR remains shared application infrastructure in Phase 3, so one
  BEAM may supervise only contexts that use the already configured metadata
  roots.
  """

  alias ExStorageService.InstanceConfig

  @filesystem_workers [
    :multipart_gc,
    :content_gc,
    :cas_gc,
    :packer,
    :lifecycle,
    :cross_cluster_replication
  ]

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
    with {:ok, config} <- InstanceConfig.from_application_env(),
         context = new(config),
         :ok <- validate_shared_metadata_roots(context) do
      {:ok, context}
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

  @doc false
  @spec validate_worker_roots(t()) :: :ok | {:error, String.t()}
  def validate_worker_roots(%__MODULE__{} = context) do
    enabled =
      Enum.filter(@filesystem_workers, &InstanceConfig.worker_enabled?(context.config, &1))

    with :ok <- compare_worker_root(:data_root, context.data_root, enabled),
         :ok <- compare_worker_root(:blob_root, context.blob_root, enabled),
         :ok <- compare_worker_root(:tmp_root, context.tmp_root, enabled) do
      :ok
    end
  end

  defp compare_root(key, requested) do
    configured_root =
      case key do
        :metadata_root ->
          Application.get_env(
            :concord,
            :data_dir,
            Application.get_env(:ex_storage_service, key, requested)
          )

        _ ->
          Application.get_env(:ex_storage_service, key, requested)
      end

    configured =
      configured_root
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

  defp compare_worker_root(_key, _requested, []), do: :ok

  defp compare_worker_root(key, requested, enabled) do
    configured =
      Application.get_env(:ex_storage_service, key, requested)
      |> to_string()
      |> Path.expand()

    if configured == requested do
      :ok
    else
      {:error,
       "#{key} cannot differ from application infrastructure while filesystem workers " <>
         "are enabled (#{inspect(enabled)}); configure the application root or disable those workers"}
    end
  end
end
