defmodule ExStorageService.Metadata do
  @moduledoc """
  Metadata operations backed by Concord key-value store.

  Legacy keys are namespaced:
  - Buckets: `"bucket:{name}"`
  - Objects: `"obj:{bucket}:{key}"`

  Object reads prefer the atomic v2 schema and fall back to these v1 records.
  """

  alias ExStorageService.Metadata.Keys
  alias ExStorageService.Metadata.ObjectCommit

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
    case ObjectCommit.get_head(bucket, key) do
      {:ok, %{is_delete_marker: true}} -> {:error, :not_found}
      {:ok, value} -> {:ok, normalize_public_object_meta(bucket, value)}
      {:error, :not_found} -> get_v1_object_meta(bucket, key)
      error -> error
    end
  end

  @doc false
  def get_v1_object_meta(bucket, key) do
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

    # Retain one full get_all/0 scan so v1 records and v2 heads/versions share a
    # consistent snapshot for the compatibility merge. Acceptable for < 50K keys.
    case Concord.get_all() do
      {:ok, all} ->
        v1_entries =
          all
          |> Enum.filter(fn {k, _v} -> String.starts_with?(k, obj_prefix) end)
          |> Enum.map(fn {k, v} ->
            object_key = String.replace_prefix(k, obj_prefix, "")
            {object_key, v}
          end)
          |> Enum.filter(fn {object_key, _v} -> String.starts_with?(object_key, prefix) end)

        v2_entries =
          all
          |> Enum.filter(fn {metadata_key, _value} ->
            String.starts_with?(metadata_key, "ess:v2:object_head:")
          end)
          |> Enum.reduce([], fn {metadata_key, head}, entries ->
            case decode_v2_head_key(metadata_key) do
              {:ok, ^bucket, object_key}
              when not head.delete_marker and is_binary(object_key) ->
                case find_v2_version(all, bucket, object_key, head.version_id) do
                  {:ok, metadata} when is_map(metadata) ->
                    [{object_key, metadata} | entries]

                  _ ->
                    entries
                end

              _ ->
                entries
            end
          end)
          |> Enum.filter(fn {object_key, _v} -> String.starts_with?(object_key, prefix) end)

        entries =
          v1_entries
          |> Map.new()
          |> Map.merge(Map.new(v2_entries))
          |> Map.to_list()

        # Collapse keys under their common prefix when a delimiter is set, then
        # treat keys and common prefixes as one ordered, paginated result set so
        # truncation and the continuation cursor stay consistent across pages.
        items =
          if delimiter do
            group_with_delimiter(entries, prefix, delimiter)
          else
            Enum.map(entries, fn {key, meta} -> {:key, key, meta} end)
          end

        sorted = Enum.sort_by(items, &item_sort_key/1)

        remaining =
          if continuation_token do
            Enum.drop_while(sorted, fn item -> item_sort_key(item) <= continuation_token end)
          else
            sorted
          end

        page = Enum.take(remaining, max_keys)
        is_truncated = length(remaining) > max_keys

        next_continuation_token =
          if is_truncated do
            page |> List.last() |> item_sort_key()
          else
            nil
          end

        keys = for {:key, key, meta} <- page, do: {key, meta}
        common_prefixes = for {:prefix, p} <- page, do: p

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

  ## Blob metadata operations (global CAS)
  # Key schema: "blob:sha256:{hash}" — see docs/prd/git-style-data-model.md §7.4

  def put_blob_meta(content_hash, meta) do
    Concord.put("blob:sha256:#{content_hash}", meta)
  end

  def get_blob_meta(content_hash) do
    case Concord.get("blob:sha256:#{content_hash}") do
      {:ok, nil} -> {:error, :not_found}
      {:ok, value} -> {:ok, value}
      error -> error
    end
  end

  @doc """
  Creates the blob metadata record if it does not exist yet. Dedup hits
  (same content committed again) keep the original record.
  """
  def ensure_blob_meta(content_hash, size) do
    case get_blob_meta(content_hash) do
      {:ok, _meta} ->
        :ok

      {:error, :not_found} ->
        <<prefix::binary-size(2), rest::binary>> = content_hash
        now = DateTime.utc_now() |> DateTime.to_iso8601()

        put_blob_meta(content_hash, %{
          hash: "sha256:#{content_hash}",
          size: size,
          physical_path: Path.join(["cas", "objects", "sha256", prefix, rest]),
          state: :active,
          created_at: now,
          last_seen_at: now
        })
    end
  end

  ## Private

  # Builds a deduplicated list mixing object keys and the common prefixes that
  # collapse keys containing the delimiter beyond `prefix`.
  defp group_with_delimiter(entries, prefix, delimiter) do
    prefix_len = String.length(prefix)

    {items, _seen} =
      Enum.reduce(entries, {[], MapSet.new()}, fn {key, meta}, {acc, seen} ->
        suffix = String.slice(key, prefix_len..-1//1)

        case String.split(suffix, delimiter, parts: 2) do
          [before_delim, _after] ->
            common_prefix = prefix <> before_delim <> delimiter

            if MapSet.member?(seen, common_prefix) do
              {acc, seen}
            else
              {[{:prefix, common_prefix} | acc], MapSet.put(seen, common_prefix)}
            end

          [_no_delimiter] ->
            {[{:key, key, meta} | acc], seen}
        end
      end)

    items
  end

  defp item_sort_key({:key, key, _meta}), do: key
  defp item_sort_key({:prefix, prefix}), do: prefix

  defp decode_v2_head_key("ess:v2:object_head:" <> encoded) do
    case String.split(encoded, ":", parts: 2) do
      [bucket64, key64] ->
        with {:ok, bucket} <- Keys.decode_component(bucket64),
             {:ok, key} <- Keys.decode_component(key64) do
          {:ok, bucket, key}
        end

      _ ->
        :error
    end
  end

  defp decode_v2_head_key(_key), do: :error

  defp find_v2_version(all, bucket, key, version_id) do
    version_key = Keys.object_version(bucket, key, version_id)

    case Enum.find(all, fn {key, _value} -> key == version_key end) do
      {^version_key, metadata} -> {:ok, metadata}
      nil -> {:error, :not_found}
    end
  end

  defp normalize_public_object_meta(bucket, metadata) do
    case Concord.get("bucket_versioning:#{bucket}") do
      {:ok, state} when state in [:enabled, "enabled", "Enabled"] ->
        metadata

      _ ->
        Map.delete(metadata, :version_id)
    end
  end
end
