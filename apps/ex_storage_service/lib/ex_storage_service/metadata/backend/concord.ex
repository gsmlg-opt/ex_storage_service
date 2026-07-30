defmodule ExStorageService.Metadata.Backend.Concord do
  @moduledoc """
  Concord implementation of the object metadata backend.

  Prefix reads and transaction outcome resolution use Concord's native APIs.
  Transactions remain native Concord compare/success/failure transactions and
  are never emulated with sequential writes.
  """

  @behaviour ExStorageService.Metadata.Backend

  alias Concord.KV.{Record, Selector}

  @impl true
  def get(key, opts \\ []) do
    case Concord.KV.get(key, Keyword.put(opts, :metadata, true)) do
      {:ok, %Record{value: value, mod_revision: revision}} ->
        {:ok, %{value: value, mod_revision: revision}}

      {:error, :not_found} ->
        {:ok, nil}

      {:ok, nil} ->
        {:ok, nil}

      {:error, reason} ->
        {:error, reason}
    end
  end

  @impl true
  def put(key, value, opts \\ []), do: Concord.put(key, value, opts)

  @impl true
  def delete(key, opts \\ []), do: Concord.delete(key, opts)

  @impl true
  def get_all(opts \\ []) do
    Concord.get_all(opts)
  end

  @impl true
  def prefix_scan(prefix, opts \\ []) when is_binary(prefix) do
    with {:ok, entries} <- Concord.prefix_scan(prefix, opts) do
      {:ok, Enum.sort_by(entries, &elem(&1, 0))}
    end
  end

  @impl true
  def scan(prefix, opts \\ []) when is_binary(prefix), do: prefix_scan(prefix, opts)

  @impl true
  def list_page(prefix, cursor, limit, opts \\ [])
      when is_binary(prefix) and (is_binary(cursor) or is_nil(cursor)) and is_integer(limit) and
             limit > 0 do
    list_opts =
      opts
      |> Keyword.take([:consistency, :timeout, :engine, :revision])
      |> Keyword.put(:limit, limit)
      |> page_selector(prefix, cursor)

    with {:ok, records, page} <- Concord.KV.list(list_opts) do
      entries =
        Enum.map(records, fn record ->
          %{
            key: Map.fetch!(record, :key),
            value: record.value,
            mod_revision: record.mod_revision
          }
        end)

      next_cursor = if page.has_more, do: page.last_key, else: nil
      {:ok, %{entries: entries, next_cursor: next_cursor}}
    end
  end

  @impl true
  def transaction(spec, opts \\ []) do
    Concord.Txn.commit(spec, opts)
  end

  @impl true
  def resolve_transaction(idempotency_key, opts \\ []) do
    Concord.Txn.resolve(idempotency_key, opts)
  end

  @impl true
  def resolve_operation(operation_key, opts \\ []) do
    get(operation_key, opts)
  end

  defp page_selector(opts, prefix, nil), do: Keyword.put(opts, :prefix, prefix)

  defp page_selector(opts, prefix, cursor) do
    Keyword.put(opts, :range, {cursor <> <<0>>, Selector.prefix_end(prefix)})
  end
end
