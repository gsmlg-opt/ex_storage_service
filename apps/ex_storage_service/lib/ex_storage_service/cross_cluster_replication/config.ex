defmodule ExStorageService.CrossClusterReplication.Config do
  @moduledoc """
  Configuration for eventual external S3 disaster-recovery targets.

  These targets are independent of the in-cluster RF/W placement policy.
  """

  defmodule Replica do
    @moduledoc "One eventual external replication target."
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
    case Concord.get("replication:#{bucket}") do
      {:ok, nil} -> {:ok, []}
      {:error, :not_found} -> {:ok, []}
      {:ok, replicas} when is_list(replicas) -> {:ok, Enum.map(replicas, &to_struct/1)}
      {:ok, _invalid} -> {:ok, []}
      {:error, reason} -> {:error, reason}
    end
  end

  @spec set_bucket_replicas(String.t(), [Replica.t() | map()]) :: :ok | {:error, term()}
  def set_bucket_replicas(bucket, replicas) when is_list(replicas) do
    serializable =
      Enum.map(replicas, fn replica ->
        %{
          endpoint: field(replica, :endpoint),
          access_key: field(replica, :access_key),
          secret_key_enc: field(replica, :secret_key_enc),
          bucket: field(replica, :bucket)
        }
      end)

    normalize_write(Concord.put("replication:#{bucket}", serializable))
  end

  @spec remove_bucket_replicas(String.t()) :: :ok | {:error, term()}
  def remove_bucket_replicas(bucket),
    do: normalize_write(Concord.delete("replication:#{bucket}"))

  defp to_struct(%Replica{} = replica), do: replica

  defp to_struct(replica) do
    struct!(Replica,
      endpoint: field(replica, :endpoint),
      access_key: field(replica, :access_key),
      secret_key_enc: field(replica, :secret_key_enc),
      bucket: field(replica, :bucket)
    )
  end

  defp field(map, key), do: Map.get(map, key) || Map.get(map, to_string(key))

  defp normalize_write(:ok), do: :ok
  defp normalize_write({:ok, _result}), do: :ok
  defp normalize_write({:error, reason}), do: {:error, reason}
end
