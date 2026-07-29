defmodule ExStorageService.Metadata.BlobCatalog do
  @moduledoc """
  Bounded, typed access to v2 blob descriptors.

  Pages are a live view because the current Concord release cannot pin a scan
  revision. Consumers must therefore re-read and compare metadata before
  applying a plan derived from a page.
  """

  alias ExStorageService.Metadata.Backend.Concord, as: ConcordBackend
  alias ExStorageService.Metadata.Keys
  alias ExStorageService.Metadata.Models.Blob

  @type record :: %{key: binary(), descriptor: Blob.t(), mod_revision: non_neg_integer()}

  @spec get(binary(), keyword()) :: {:ok, record()} | {:error, :not_found | term()}
  def get(hash, opts \\ []) when is_binary(hash) do
    key = Keys.blob(hash)

    case backend(opts).get(key, read_opts(opts)) do
      {:ok, nil} ->
        {:error, :not_found}

      {:ok, %{value: value, mod_revision: revision}} ->
        with {:ok, descriptor} <- cast(value, hash) do
          {:ok, %{key: key, descriptor: descriptor, mod_revision: revision}}
        end

      {:error, reason} ->
        {:error, reason}
    end
  end

  @spec list_page(binary() | nil, binary() | nil, pos_integer(), keyword()) ::
          {:ok, %{records: [record()], next_cursor: binary() | nil}} | {:error, term()}
  def list_page(shard, cursor \\ nil, limit \\ 100, opts \\ [])

  def list_page(shard, cursor, limit, opts)
      when (is_nil(shard) or is_binary(shard)) and
             (is_nil(cursor) or is_binary(cursor)) and is_integer(limit) and limit > 0 do
    prefix = if shard, do: Keys.blob_shard_prefix(shard), else: Keys.blob_prefix()

    with {:ok, page} <- backend(opts).list_page(prefix, cursor, limit, read_opts(opts)),
         {:ok, records} <- cast_entries(page.entries) do
      {:ok, %{records: records, next_cursor: page.next_cursor}}
    end
  end

  defp cast_entries(entries) do
    Enum.reduce_while(entries, {:ok, []}, fn entry, {:ok, records} ->
      hash = String.replace_prefix(entry.key, Keys.blob_prefix(), "")

      case cast(entry.value, hash) do
        {:ok, descriptor} ->
          record = %{key: entry.key, descriptor: descriptor, mod_revision: entry.mod_revision}
          {:cont, {:ok, [record | records]}}

        {:error, reason} ->
          {:halt, {:error, reason}}
      end
    end)
    |> case do
      {:ok, records} -> {:ok, Enum.reverse(records)}
      error -> error
    end
  end

  defp cast(%Blob{} = descriptor, hash), do: validate(descriptor, hash)

  defp cast(value, hash) when is_map(value) do
    descriptor =
      struct(Blob, %{
        schema: field(value, :schema, 2),
        hash: field(value, :hash),
        algorithm: field(value, :algorithm, :sha256),
        size: field(value, :size),
        created_at: field(value, :created_at),
        desired_replication_factor: field(value, :desired_replication_factor, 1)
      })

    validate(descriptor, hash)
  end

  defp cast(_value, _hash), do: {:error, :invalid_blob_descriptor}

  defp validate(
         %Blob{
           schema: 2,
           hash: hash,
           algorithm: :sha256,
           size: size,
           desired_replication_factor: replication_factor
         } = descriptor,
         hash
       )
       when is_binary(hash) and hash != "" and is_integer(size) and size >= 0 and
              is_integer(replication_factor) and replication_factor > 0,
       do: {:ok, descriptor}

  defp validate(_descriptor, _hash), do: {:error, :invalid_blob_descriptor}

  defp field(map, key, default \\ nil),
    do: Map.get(map, key, Map.get(map, Atom.to_string(key), default))

  defp read_opts(opts) do
    opts
    |> Keyword.take([:consistency, :timeout, :engine, :barrier, :revision])
    |> Keyword.put_new(:consistency, :strong)
  end

  defp backend(opts), do: Keyword.get(opts, :backend, ConcordBackend)
end
