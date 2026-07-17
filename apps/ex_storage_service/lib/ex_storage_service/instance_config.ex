defmodule ExStorageService.InstanceConfig do
  @moduledoc """
  Validated configuration for one storage instance.

  Phase 0 keeps the application in standalone mode. Cluster values are parsed
  now so an unsafe public cluster writer cannot be enabled accidentally before
  the data plane is implemented.
  """

  @enforce_keys [
    :instance,
    :auto_start,
    :data_root,
    :blob_root,
    :tmp_root,
    :ra_root,
    :metadata_root,
    :web_enabled,
    :workers,
    :mode,
    :replication_factor,
    :write_quorum,
    :allow_degraded_writes,
    :cluster_data_plane_enabled,
    :public_s3_enabled,
    :metadata_schema
  ]
  defstruct @enforce_keys

  @worker_defaults %{
    multipart_gc: true,
    content_gc: true,
    cas_gc: true,
    packer: true,
    lifecycle: true,
    cross_cluster_replication: true,
    repair: false,
    scrub: false
  }

  @type mode :: :standalone | :cluster
  @type metadata_schema :: :v1 | :v2

  @type t :: %__MODULE__{
          instance: atom() | String.t(),
          auto_start: boolean(),
          data_root: String.t(),
          blob_root: String.t(),
          tmp_root: String.t(),
          ra_root: String.t(),
          metadata_root: String.t(),
          web_enabled: boolean(),
          workers: %{required(atom()) => boolean()},
          mode: mode(),
          replication_factor: pos_integer(),
          write_quorum: pos_integer(),
          allow_degraded_writes: boolean(),
          cluster_data_plane_enabled: boolean(),
          public_s3_enabled: boolean(),
          metadata_schema: metadata_schema()
        }

  @spec from_application_env() :: {:ok, t()} | {:error, String.t()}
  def from_application_env do
    configured = Application.get_env(:ex_storage_service, :instance_config, [])

    application_roots =
      [
        data_root: Application.get_env(:ex_storage_service, :data_root),
        blob_root: Application.get_env(:ex_storage_service, :blob_root),
        tmp_root: Application.get_env(:ex_storage_service, :tmp_root),
        ra_root: Application.get_env(:ex_storage_service, :ra_root),
        metadata_root: Application.get_env(:ex_storage_service, :metadata_root)
      ]
      |> Enum.reject(fn {_key, value} -> is_nil(value) end)

    application_roots
    |> Keyword.merge(configured)
    |> new()
  end

  @spec new(keyword() | map()) :: {:ok, t()} | {:error, String.t()}
  def new(%__MODULE__{} = config), do: validate(config)
  def new(opts) when is_map(opts), do: opts |> Map.to_list() |> new()

  def new(opts) when is_list(opts) do
    data_root =
      Keyword.get(
        opts,
        :data_root,
        Application.get_env(:ex_storage_service, :data_root, "/tmp/ex_storage_service/data")
      )

    use_application_roots? = not Keyword.has_key?(opts, :data_root)

    blob_root =
      Keyword.get(
        opts,
        :blob_root,
        application_root(:blob_root, Path.join(data_root, "cas"), use_application_roots?)
      )

    workers =
      @worker_defaults
      |> Map.merge(opts |> Keyword.get(:workers, %{}) |> Map.new())

    config = %__MODULE__{
      instance: Keyword.get(opts, :instance, :default),
      auto_start: Keyword.get(opts, :auto_start, true),
      data_root: data_root,
      blob_root: blob_root,
      tmp_root:
        Keyword.get(
          opts,
          :tmp_root,
          application_root(:tmp_root, Path.join(blob_root, "tmp"), use_application_roots?)
        ),
      ra_root:
        Keyword.get(
          opts,
          :ra_root,
          application_root(:ra_root, Path.join(data_root, "ra"), use_application_roots?)
        ),
      metadata_root:
        Keyword.get(
          opts,
          :metadata_root,
          application_root(
            :metadata_root,
            Path.join(data_root, "concord"),
            use_application_roots?
          )
        ),
      web_enabled: Keyword.get(opts, :web_enabled, true),
      workers: workers,
      mode: Keyword.get(opts, :mode, :standalone),
      replication_factor: Keyword.get(opts, :replication_factor, 1),
      write_quorum: Keyword.get(opts, :write_quorum, 1),
      allow_degraded_writes: Keyword.get(opts, :allow_degraded_writes, false),
      cluster_data_plane_enabled: Keyword.get(opts, :cluster_data_plane_enabled, false),
      public_s3_enabled: Keyword.get(opts, :public_s3_enabled, true),
      metadata_schema: Keyword.get(opts, :metadata_schema, :v2)
    }

    validate(config)
  end

  @spec worker_enabled?(t(), atom()) :: boolean()
  def worker_enabled?(%__MODULE__{workers: workers}, worker),
    do: Map.get(workers, worker, false)

  @spec worker_defaults() :: map()
  def worker_defaults, do: @worker_defaults

  defp validate(%__MODULE__{instance: instance})
       when not is_atom(instance) and not is_binary(instance),
       do: {:error, "instance must be an atom or non-empty string"}

  defp validate(%__MODULE__{instance: ""}),
    do: {:error, "instance must be an atom or non-empty string"}

  defp validate(%__MODULE__{auto_start: value}) when not is_boolean(value),
    do: {:error, "auto start must be a boolean"}

  defp validate(%__MODULE__{web_enabled: value}) when not is_boolean(value),
    do: {:error, "web enabled must be a boolean"}

  defp validate(%__MODULE__{} = config) do
    with :ok <- validate_paths(config),
         :ok <- validate_workers(config.workers) do
      validate_storage(config)
    end
  end

  defp validate_paths(config) do
    [:data_root, :blob_root, :tmp_root, :ra_root, :metadata_root]
    |> Enum.find(fn key ->
      value = Map.fetch!(config, key)
      not is_binary(value) or value == ""
    end)
    |> case do
      nil -> :ok
      key -> {:error, "#{key} must be a non-empty path"}
    end
  end

  defp validate_workers(workers) when is_map(workers) do
    unknown = Map.keys(workers) -- Map.keys(@worker_defaults)

    cond do
      unknown != [] ->
        {:error, "unknown workers: #{inspect(Enum.sort(unknown))}"}

      Enum.any?(workers, fn {_worker, enabled} -> not is_boolean(enabled) end) ->
        {:error, "worker values must be booleans"}

      true ->
        :ok
    end
  end

  defp validate_workers(_workers), do: {:error, "workers must be a map or keyword list"}

  defp application_root(key, fallback, true),
    do: Application.get_env(:ex_storage_service, key, fallback)

  defp application_root(_key, fallback, false), do: fallback

  defp validate_storage(%__MODULE__{mode: mode}) when mode not in [:standalone, :cluster],
    do: {:error, "mode must be :standalone or :cluster, got: #{inspect(mode)}"}

  defp validate_storage(%__MODULE__{metadata_schema: schema}) when schema not in [:v1, :v2],
    do: {:error, "metadata schema must be :v1 or :v2, got: #{inspect(schema)}"}

  defp validate_storage(%__MODULE__{replication_factor: rf}) when not is_integer(rf) or rf < 1,
    do: {:error, "replication factor must be an integer greater than or equal to 1"}

  defp validate_storage(%__MODULE__{write_quorum: quorum})
       when not is_integer(quorum) or quorum < 1,
       do: {:error, "write quorum must be an integer greater than or equal to 1"}

  defp validate_storage(%__MODULE__{allow_degraded_writes: value}) when not is_boolean(value),
    do: {:error, "allow degraded writes must be a boolean"}

  defp validate_storage(%__MODULE__{cluster_data_plane_enabled: value})
       when not is_boolean(value),
       do: {:error, "cluster data plane enabled must be a boolean"}

  defp validate_storage(%__MODULE__{public_s3_enabled: value}) when not is_boolean(value),
    do: {:error, "public S3 enabled must be a boolean"}

  defp validate_storage(%__MODULE__{replication_factor: rf, write_quorum: quorum})
       when quorum > rf,
       do: {:error, "write quorum must satisfy 1 <= W <= RF (got W=#{quorum}, RF=#{rf})"}

  defp validate_storage(%__MODULE__{
         mode: :cluster,
         public_s3_enabled: true,
         cluster_data_plane_enabled: false
       }),
       do:
         {:error,
          "cluster mode cannot expose the public S3 writer while the cluster data plane is disabled"}

  defp validate_storage(config), do: {:ok, config}
end
