defmodule ExStorageService.InstanceConfig do
  @moduledoc """
  Validated configuration for one storage instance.

  Phase 0 keeps the application in standalone mode. Cluster values are parsed
  now so an unsafe public cluster writer cannot be enabled accidentally before
  the data plane is implemented.
  """

  @enforce_keys [
    :mode,
    :replication_factor,
    :write_quorum,
    :allow_degraded_writes,
    :cluster_data_plane_enabled,
    :public_s3_enabled,
    :metadata_schema
  ]
  defstruct @enforce_keys

  @type mode :: :standalone | :cluster
  @type metadata_schema :: :v1 | :v2

  @type t :: %__MODULE__{
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
    :ex_storage_service
    |> Application.get_env(:instance_config, [])
    |> new()
  end

  @spec new(keyword() | map()) :: {:ok, t()} | {:error, String.t()}
  def new(opts) when is_map(opts), do: opts |> Map.to_list() |> new()

  def new(opts) when is_list(opts) do
    config = %__MODULE__{
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

  defp validate(%__MODULE__{mode: mode}) when mode not in [:standalone, :cluster],
    do: {:error, "mode must be :standalone or :cluster, got: #{inspect(mode)}"}

  defp validate(%__MODULE__{metadata_schema: schema}) when schema not in [:v1, :v2],
    do: {:error, "metadata schema must be :v1 or :v2, got: #{inspect(schema)}"}

  defp validate(%__MODULE__{replication_factor: rf}) when not is_integer(rf) or rf < 1,
    do: {:error, "replication factor must be an integer greater than or equal to 1"}

  defp validate(%__MODULE__{write_quorum: quorum}) when not is_integer(quorum) or quorum < 1,
    do: {:error, "write quorum must be an integer greater than or equal to 1"}

  defp validate(%__MODULE__{allow_degraded_writes: value}) when not is_boolean(value),
    do: {:error, "allow degraded writes must be a boolean"}

  defp validate(%__MODULE__{cluster_data_plane_enabled: value}) when not is_boolean(value),
    do: {:error, "cluster data plane enabled must be a boolean"}

  defp validate(%__MODULE__{public_s3_enabled: value}) when not is_boolean(value),
    do: {:error, "public S3 enabled must be a boolean"}

  defp validate(%__MODULE__{replication_factor: rf, write_quorum: quorum})
       when quorum > rf,
       do: {:error, "write quorum must satisfy 1 <= W <= RF (got W=#{quorum}, RF=#{rf})"}

  defp validate(%__MODULE__{
         mode: :cluster,
         public_s3_enabled: true,
         cluster_data_plane_enabled: false
       }),
       do:
         {:error,
          "cluster mode cannot expose the public S3 writer while the cluster data plane is disabled"}

  defp validate(config), do: {:ok, config}
end
