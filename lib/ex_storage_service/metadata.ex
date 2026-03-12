defmodule ExStorageService.Metadata do
  @moduledoc """
  Metadata operations backed by Concord key-value store.

  Keys are namespaced:
  - Buckets: `"bucket:{name}"`
  - Objects: `"obj:{bucket}:{key}"`
  """

  ## Bucket operations

  def create_bucket(name) do
    meta = %{
      name: name,
      creation_date: DateTime.utc_now() |> DateTime.to_iso8601()
    }

    Concord.put("bucket:#{name}", meta)
  end

  def delete_bucket(name) do
    Concord.delete("bucket:#{name}")
  end

  def get_bucket(name) do
    case Concord.get("bucket:#{name}") do
      {:ok, nil} -> {:error, :not_found}
      {:ok, value} -> {:ok, value}
      error -> error
    end
  end

  def head_bucket(name) do
    case Concord.get("bucket:#{name}") do
      {:ok, nil} -> {:error, :not_found}
      {:ok, _value} -> :ok
      error -> error
    end
  end

  def list_buckets do
    case Concord.get_all() do
      {:ok, all} ->
        buckets =
          all
          |> Enum.filter(fn {k, _v} -> String.starts_with?(k, "bucket:") end)
          |> Enum.map(fn {_k, v} -> v end)

        {:ok, buckets}

      error ->
        error
    end
  end

  ## Object metadata operations

  def put_object_meta(bucket, key, meta) do
    Concord.put("obj:#{bucket}:#{key}", meta)
  end

  def get_object_meta(bucket, key) do
    case Concord.get("obj:#{bucket}:#{key}") do
      {:ok, nil} -> {:error, :not_found}
      {:ok, value} -> {:ok, value}
      error -> error
    end
  end

  def delete_object_meta(bucket, key) do
    Concord.delete("obj:#{bucket}:#{key}")
  end

  @doc """
  List objects in a bucket with support for prefix, delimiter, max_keys,
  and continuation_token (pagination).
  """
  def list_objects(bucket, opts \\ []) do
    prefix = Keyword.get(opts, :prefix, "")
    delimiter = Keyword.get(opts, :delimiter, nil)
    max_keys = Keyword.get(opts, :max_keys, 1000)
    continuation_token = Keyword.get(opts, :continuation_token, nil)

    obj_prefix = "obj:#{bucket}:"

    case Concord.get_all() do
      {:ok, all} ->
        entries =
          all
          |> Enum.filter(fn {k, _v} -> String.starts_with?(k, obj_prefix) end)
          |> Enum.map(fn {k, v} ->
            object_key = String.replace_prefix(k, obj_prefix, "")
            {object_key, v}
          end)
          |> Enum.filter(fn {object_key, _v} -> String.starts_with?(object_key, prefix) end)
          |> Enum.sort_by(fn {object_key, _v} -> object_key end)

        # Apply continuation token
        entries =
          if continuation_token do
            Enum.drop_while(entries, fn {key, _v} -> key <= continuation_token end)
          else
            entries
          end

        # Handle delimiter for common prefixes
        {keys, common_prefixes} =
          if delimiter do
            extract_with_delimiter(entries, prefix, delimiter)
          else
            {entries, []}
          end

        common_prefixes = common_prefixes |> Enum.uniq() |> Enum.sort()

        total_results = length(keys) + length(common_prefixes)
        is_truncated = total_results > max_keys
        keys = Enum.take(keys, max_keys)

        next_continuation_token =
          if is_truncated do
            case List.last(keys) do
              {key, _v} -> key
              nil -> nil
            end
          else
            nil
          end

        {:ok,
         %{
           keys: keys,
           common_prefixes: common_prefixes,
           is_truncated: is_truncated,
           next_continuation_token: next_continuation_token
         }}

      error ->
        error
    end
  end

  ## Private

  defp extract_with_delimiter(entries, prefix, delimiter) do
    prefix_len = String.length(prefix)

    Enum.reduce(entries, {[], []}, fn {key, meta}, {keys_acc, prefixes_acc} ->
      suffix = String.slice(key, prefix_len..-1//1)

      case String.split(suffix, delimiter, parts: 2) do
        [before_delim, _after] ->
          common_prefix = prefix <> before_delim <> delimiter
          {keys_acc, [common_prefix | prefixes_acc]}

        [_no_delimiter] ->
          {keys_acc ++ [{key, meta}], prefixes_acc}
      end
    end)
  end
end
