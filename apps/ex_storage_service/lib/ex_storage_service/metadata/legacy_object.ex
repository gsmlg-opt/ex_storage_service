defmodule ExStorageService.Metadata.LegacyObject do
  @moduledoc false
  alias ExStorageService.Metadata.{BucketLifecycle, Keys}

  # Normalize only the object explicitly being mutated. The source records remain
  # intact for rollback; publication is fenced against every observed revision.
  # Larger histories require the quiesced operator migration, never truncation.
  @max_versions 256

  def prepare(backend, bucket, key, opts) do
    if Keyword.get(opts, :bucket_versioning, false) do
      with {:ok, _guard} <- BucketLifecycle.guard(backend, bucket, opts) do
        if is_binary(Keyword.get(opts, :index_build_token)),
          do: do_prepare(backend, bucket, key, opts),
          else: :ok
      end
    else
      :ok
    end
  end

  defp do_prepare(backend, bucket, key, opts) do
    read_opts = Keyword.take(opts, [:timeout, :engine, :barrier]) ++ [consistency: :linearizable]
    head_key = Keys.object_head(bucket, key)

    with {:ok, nil} <- backend.get(head_key, read_opts),
         {:ok, candidates} <- backend.prefix_scan("obj_ver:#{bucket}:#{key}:", read_opts),
         {:ok, current} <- backend.get("obj:#{bucket}:#{key}", read_opts),
         {:ok, list} <- backend.get("obj_ver_list:#{bucket}:#{key}", read_opts) do
      rows =
        Enum.filter(candidates, fn {record_key, _} ->
          suffix = String.replace_prefix(record_key, "obj_ver:#{bucket}:", "")
          suffix |> String.split(":") |> Enum.drop(-1) |> Enum.join(":") == key
        end)

      if length(rows) > @max_versions do
        {:error, :legacy_object_migration_required}
      else
        normalize(backend, bucket, key, current, list, rows, opts, read_opts)
      end
    else
      {:ok, _head} -> :ok
      {:error, _} = error -> error
    end
  end

  defp normalize(_backend, _bucket, _key, nil, _list, [], _opts, _read_opts), do: :ok

  defp normalize(backend, bucket, key, current, list, rows, opts, read_opts) do
    observed =
      Enum.reduce_while(rows, {:ok, []}, fn {record_key, _}, {:ok, acc} ->
        case backend.get(record_key, read_opts) do
          {:ok, nil} -> {:halt, {:error, :compare_retry_exhausted}}
          {:ok, record} -> {:cont, {:ok, [{record_key, record} | acc]}}
          error -> {:halt, error}
        end
      end)

    with {:ok, observed} <- observed,
         {:ok, guard} <- BucketLifecycle.guard(backend, bucket, opts) do
      versions =
        Map.new(observed, fn {record_key, record} ->
          id = record_key |> String.split(":") |> List.last()
          {id, record.value}
        end)

      current_id = if current, do: Map.get(current.value, :version_id, "null")
      versions = if current, do: Map.put_new(versions, current_id, current.value), else: versions

      ids =
        case list do
          %{value: values} when is_list(values) -> values
          _ -> []
        end

      latest =
        Enum.find(ids, &Map.has_key?(versions, &1)) || current_id ||
          versions
          |> Enum.max_by(fn {id, value} -> {Map.get(value, :created_at, ""), id} end)
          |> elem(0)

      now = DateTime.utc_now() |> DateTime.to_iso8601()

      writes =
        Enum.map(versions, fn {id, value} ->
          value =
            value
            |> Map.merge(%{schema: 2, bucket: bucket, key: key, version_id: id})
            |> Map.put_new(:created_at, now)
            |> Map.put_new(:delete_marker, Map.get(value, :is_delete_marker, false))
            |> Map.put_new(:is_delete_marker, Map.get(value, :delete_marker, false))

          {:put, Keys.object_version(bucket, key, id), value, %{}}
        end)

      selected = Map.fetch!(versions, latest)

      head = %{
        schema: 2,
        bucket: bucket,
        key: key,
        version_id: latest,
        delete_marker:
          Map.get(selected, :is_delete_marker, Map.get(selected, :delete_marker, false)),
        etag: Map.get(selected, :etag),
        updated_at: Map.get(selected, :created_at, now)
      }

      compare =
        [{:exists, Keys.object_head(bucket, key), :==, false}] ++
          [
            revision("obj:#{bucket}:#{key}", current),
            revision("obj_ver_list:#{bucket}:#{key}", list)
          ] ++
          Enum.map(observed, fn {record_key, record} -> revision(record_key, record) end) ++
          Enum.map(versions, fn {id, _} ->
            {:exists, Keys.object_version(bucket, key, id), :==, false}
          end)

      spec = %{
        compare: compare ++ guard.compare,
        success: writes ++ [{:put, Keys.object_head(bucket, key), head, %{}}] ++ guard.success,
        failure: []
      }

      case backend.transaction(spec, Keyword.take(opts, [:timeout, :engine, :barrier])) do
        {:ok, %{succeeded: true}} -> :ok
        {:ok, %{succeeded: false}} -> {:error, :compare_retry_exhausted}
        {:error, _} = error -> error
      end
    end
  end

  defp revision(key, nil), do: {:exists, key, :==, false}
  defp revision(key, record), do: {:mod_revision, key, :==, record.mod_revision}
end
