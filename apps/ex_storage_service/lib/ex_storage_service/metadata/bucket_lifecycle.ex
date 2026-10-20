defmodule ExStorageService.Metadata.BucketLifecycle do
  @moduledoc """
  Serializes bucket deletion with object publication using the bucket revision.

  Object commits compare and touch that revision in their own transaction.
  Staged work carries the bucket generation, preventing publication into a
  recreated bucket. Legacy buckets use their creation timestamp as identity.
  """
  alias ExStorageService.Metadata.Backend.Concord, as: Backend
  alias ExStorageService.Metadata.ListIndex

  def generation(bucket), do: Map.get(bucket, :generation, Map.get(bucket, :creation_date))

  def guard(backend, bucket, opts) do
    with {:ok, record} <- backend.get("bucket:#{bucket}", read_opts(opts)) do
      case record do
        nil ->
          {:error, :bucket_not_found}

        %{value: value, mod_revision: revision} ->
          if Keyword.has_key?(opts, :bucket_generation) and
               opts[:bucket_generation] != generation(value) do
            {:error, :bucket_generation_changed}
          else
            with :ok <- ListIndex.mutation_ready(backend, bucket, generation(value), opts) do
              {:ok,
               %{
                 compare: [{:mod_revision, "bucket:#{bucket}", :==, revision}],
                 success: [{:put, "bucket:#{bucket}", value, %{}}]
               }}
            end
          end
      end
    end
  end

  def create(name) do
    meta = %{
      name: name,
      creation_date: DateTime.to_iso8601(DateTime.utc_now()),
      generation: Base.url_encode64(:crypto.strong_rand_bytes(16), padding: false)
    }

    case Backend.transaction(%{
           compare: [{:exists, "bucket:#{name}", :==, false}],
           success: [
             {:put, "bucket:#{name}", meta, %{}},
             {:put, ListIndex.root_key(name, meta.generation), ListIndex.empty(meta.generation),
              %{}}
           ],
           failure: []
         }) do
      {:ok, %{succeeded: true}} -> :ok
      {:ok, %{succeeded: false}} -> {:error, :bucket_exists}
      error -> error
    end
  end

  def delete(name, attempts \\ 32), do: delete(name, attempts, [])
  def delete(_name, 0, _opts), do: {:error, :conflict}

  def delete(name, attempts, opts) do
    backend = Keyword.get(opts, :backend, Backend)

    with {:ok, %{mod_revision: revision, value: bucket}} <- existing(backend, name),
         index_key = ListIndex.root_key(name, generation(bucket)),
         {:ok, index} <- backend.get(index_key, consistency: :linearizable),
         :ok <- ListIndex.ready(index, generation(bucket)),
         :ok <- empty?(index.value) do
      spec = %{
        compare: [
          {:mod_revision, "bucket:#{name}", :==, revision},
          {:mod_revision, index_key, :==, index.mod_revision}
        ],
        success: [{:delete, {:key, "bucket:#{name}"}, %{}} | config_deletes(name)],
        failure: []
      }

      case backend.transaction(spec) do
        {:ok, %{succeeded: true}} -> :ok
        {:ok, %{succeeded: false}} -> delete(name, attempts - 1, opts)
        error -> error
      end
    end
  end

  defp existing(backend, name) do
    case backend.get("bucket:#{name}", consistency: :linearizable) do
      {:ok, nil} -> {:error, :not_found}
      result -> result
    end
  end

  defp empty?(%{version_count: 0, visible_count: 0, active_upload_count: 0}), do: :ok
  defp empty?(_), do: {:error, :bucket_not_empty}

  defp config_deletes(name) do
    for prefix <-
          ~w(bucket_versioning lifecycle notification bucket_cors bucket_tagging cloud_cache cloud_cache_index) do
      {:delete, {:key, "#{prefix}:#{name}"}, %{}}
    end
  end

  defp read_opts(opts),
    do:
      opts
      |> Keyword.take([:engine, :timeout, :barrier])
      |> Keyword.put(:consistency, :linearizable)
end
