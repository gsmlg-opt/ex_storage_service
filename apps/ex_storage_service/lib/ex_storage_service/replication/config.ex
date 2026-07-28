defmodule ExStorageService.Replication.Config do
  @moduledoc """
  Compatibility facade for external S3 disaster-recovery configuration.

  New code should use `ExStorageService.CrossClusterReplication.Config`.
  """

  alias ExStorageService.CrossClusterReplication.Config

  defmodule Replica do
    @moduledoc "Compatibility replica struct."
    defstruct [:endpoint, :access_key, :secret_key_enc, :bucket]

    @type t :: %__MODULE__{
            endpoint: String.t(),
            access_key: String.t() | nil,
            secret_key_enc: String.t() | nil,
            bucket: String.t() | nil
          }
  end

  @spec get_bucket_replicas(String.t()) :: {:ok, [Replica.t()]} | {:error, term()}
  def get_bucket_replicas(bucket) do
    with {:ok, replicas} <- Config.get_bucket_replicas(bucket) do
      {:ok, Enum.map(replicas, &struct!(Replica, Map.from_struct(&1)))}
    end
  end

  @spec set_bucket_replicas(String.t(), [Replica.t() | map()]) :: :ok | {:error, term()}
  defdelegate set_bucket_replicas(bucket, replicas), to: Config

  @spec remove_bucket_replicas(String.t()) :: :ok | {:error, term()}
  defdelegate remove_bucket_replicas(bucket), to: Config
end
