defmodule ExStorageService.Replication.Config do
  @moduledoc """
  Replication configuration backed by Concord KV store.

  Replica configurations are stored as part of bucket metadata under
  the key `"replication:{bucket}"`.
  """

  defmodule Replica do
    @moduledoc """
    Represents a single replication target.
    """
    defstruct [:endpoint, :access_key, :secret_key_enc, :bucket]

    @type t :: %__MODULE__{
            endpoint: String.t(),
            access_key: String.t(),
            secret_key_enc: String.t(),
            bucket: String.t()
          }
  end

  @doc """
  Get the list of replica configurations for a bucket.

  Returns `{:ok, [%Replica{}, ...]}` or `{:ok, []}` if none configured.
  """
  @spec get_bucket_replicas(String.t()) :: {:ok, [Replica.t()]} | {:error, term()}
  def get_bucket_replicas(bucket) do
    case Concord.get("replication:#{bucket}") do
      {:ok, nil} -> {:ok, []}
      {:ok, replicas} when is_list(replicas) -> {:ok, to_structs(replicas)}
      {:ok, replicas} -> {:ok, to_structs(replicas)}
      {:error, reason} -> {:error, reason}
    end
  end

  @doc """
  Set the replica configurations for a bucket.

  `replicas` is a list of `%Replica{}` structs or maps with the same keys.
  """
  @spec set_bucket_replicas(String.t(), [Replica.t() | map()]) :: :ok | {:error, term()}
  def set_bucket_replicas(bucket, replicas) when is_list(replicas) do
    serializable =
      Enum.map(replicas, fn replica ->
        %{
          endpoint: replica_field(replica, :endpoint),
          access_key: replica_field(replica, :access_key),
          secret_key_enc: replica_field(replica, :secret_key_enc),
          bucket: replica_field(replica, :bucket)
        }
      end)

    case Concord.put("replication:#{bucket}", serializable) do
      {:ok, _} -> :ok
      :ok -> :ok
      {:error, reason} -> {:error, reason}
    end
  end

  @doc """
  Remove all replica configurations for a bucket.
  """
  @spec remove_bucket_replicas(String.t()) :: :ok | {:error, term()}
  def remove_bucket_replicas(bucket) do
    case Concord.delete("replication:#{bucket}") do
      {:ok, _} -> :ok
      :ok -> :ok
      {:error, reason} -> {:error, reason}
    end
  end

  ## Private

  defp to_structs(replicas) when is_list(replicas) do
    Enum.map(replicas, &to_struct/1)
  end

  defp to_structs(_), do: []

  defp to_struct(%Replica{} = r), do: r

  defp to_struct(map) when is_map(map) do
    %Replica{
      endpoint: map_get(map, :endpoint),
      access_key: map_get(map, :access_key),
      secret_key_enc: map_get(map, :secret_key_enc),
      bucket: map_get(map, :bucket)
    }
  end

  defp map_get(map, key) do
    Map.get(map, key) || Map.get(map, to_string(key))
  end

  defp replica_field(%Replica{} = r, key), do: Map.get(r, key)
  defp replica_field(map, key) when is_map(map), do: map_get(map, key)
end
