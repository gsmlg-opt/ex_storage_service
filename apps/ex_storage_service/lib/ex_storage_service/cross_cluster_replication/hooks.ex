defmodule ExStorageService.CrossClusterReplication.Hooks do
  @moduledoc """
  Builds deterministic eventual-DR events for object metadata transactions.

  These events are separate from the RF/W cluster data plane. They are stored
  in the same Concord transaction as the object operation and dispatched with
  at-least-once semantics after commit.
  """

  alias ExStorageService.CrossClusterReplication.Config

  @spec events_for_put(String.t(), String.t(), map(), keyword()) ::
          {:ok, [map()]} | {:error, term()}
  def events_for_put(bucket, key, object, opts \\ []) do
    build_events(:cross_cluster_put, bucket, key, object, opts)
  end

  @spec events_for_delete(String.t(), String.t(), keyword()) ::
          {:ok, [map()]} | {:error, term()}
  def events_for_delete(bucket, key, opts \\ []) do
    build_events(:cross_cluster_delete, bucket, key, nil, opts)
  end

  defp build_events(kind, bucket, key, object, opts) do
    operation_id =
      Keyword.get_lazy(opts, :operation_id, fn ->
        "legacy-" <> Base.url_encode64(:crypto.strong_rand_bytes(12), padding: false)
      end)

    with {:ok, replicas} <- config(opts).get_bucket_replicas(bucket) do
      events =
        replicas
        |> Enum.map(&event(kind, operation_id, bucket, key, object, &1))
        |> Enum.sort_by(& &1.id)

      {:ok, events}
    end
  end

  defp event(kind, operation_id, bucket, key, object, replica) do
    replica = replica_map(replica)

    %{
      id: event_id({operation_id, kind, bucket, key, object_identity(object), replica}),
      kind: kind,
      state: :pending,
      attempts: 0,
      max_attempts: 3,
      payload: %{
        operation_id: operation_id,
        bucket: bucket,
        key: key,
        object: object,
        replica: replica
      }
    }
  end

  defp object_identity(nil), do: nil

  defp object_identity(object) do
    Map.take(object, [:version_id, :content_hash, :etag, :size, :content_type])
  end

  defp replica_map(replica) do
    %{
      endpoint: field(replica, :endpoint),
      access_key: field(replica, :access_key),
      secret_key_enc: field(replica, :secret_key_enc),
      bucket: field(replica, :bucket)
    }
  end

  defp field(map, key), do: Map.get(map, key) || Map.get(map, to_string(key))

  defp event_id(value) do
    value
    |> :erlang.term_to_binary([:deterministic])
    |> then(&:crypto.hash(:sha256, &1))
    |> Base.url_encode64(padding: false)
  end

  defp config(opts), do: Keyword.get(opts, :config, Config)
end
