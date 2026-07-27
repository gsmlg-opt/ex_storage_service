defmodule ExStorageService.Metadata.Backend.Concord do
  @moduledoc """
  Concord implementation of the object metadata backend.

  Prefix reads and transaction outcome resolution use Concord's native APIs.
  Transactions remain native Concord compare/success/failure transactions and
  are never emulated with sequential writes.
  """

  @behaviour ExStorageService.Metadata.Backend

  alias Concord.KV.Record

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
end
