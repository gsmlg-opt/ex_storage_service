defmodule ExStorageService.Operations.OperationStatus do
  @moduledoc """
  Exact-ID, read-only operation-intent and gateway-journal diagnostics.

  One call performs one exact metadata read. It never lists metadata, contacts
  upstreams or changes protection/reconciliation state. Prepared gateway deletes
  can have an unknown upstream outcome without changing state to `:unknown`.
  Absence does not prove that an upstream mutation failed or is safe to repeat.
  """

  alias ExStorageService.Metadata.Backend.Concord, as: Backend
  alias ExStorageService.Metadata.Keys
  alias ExStorageService.Metadata.Models.OperationIntent

  @sources [:hosted, :gateway_write, :gateway_delete]

  @doc """
  Looks up one known operation ID in one fixed namespace.

  Returns only operation ID and durable state, omitting object keys, paths,
  endpoints, hashes and credentials. This is not backlog discovery or a global
  snapshot; use the operation ID retained by the caller/operator after failure.
  """
  def get(source, operation_id, opts \\ [])

  def get(source, operation_id, opts)
      when source in @sources and is_binary(operation_id) and byte_size(operation_id) in 1..1024 do
    backend = Keyword.get(opts, :backend, Backend)

    read_opts =
      opts |> Keyword.take([:timeout, :engine]) |> Keyword.put(:consistency, :linearizable)

    result =
      case backend.get(key(source, operation_id), read_opts) do
        {:ok, nil} -> {:ok, nil}
        {:ok, %{value: value}} -> summary(source, operation_id, value)
        {:error, _} = error -> error
      end

    {outcome, state} =
      case result do
        {:ok, nil} -> {:ok, :absent}
        {:ok, summary} -> {:ok, summary.state}
        {:error, _} -> {:error, :other}
      end

    :telemetry.execute([:ex_storage_service, :operation_status, :read], %{count: 1}, %{
      source: source,
      result: outcome,
      state: state
    })

    result
  end

  def get(_, _, _), do: {:error, :invalid_operation_lookup}

  defp summary(:hosted, id, value) do
    case OperationIntent.cast(value) do
      {:ok, %{operation_id: ^id} = intent} -> {:ok, Map.take(intent, [:operation_id, :state])}
      _ -> {:error, :invalid_operation_record}
    end
  end

  defp summary(source, id, %{operation_id: stored_id, state: state}) when stored_id == id do
    allowed =
      if source == :gateway_write, do: [:prepared, :unknown, :local_committed], else: [:prepared]

    if state in allowed,
      do: {:ok, %{operation_id: id, state: state}},
      else: {:error, :invalid_operation_record}
  end

  defp summary(_, _, _), do: {:error, :invalid_operation_record}
  defp key(:hosted, id), do: Keys.operation_intent(id)
  defp key(:gateway_write, id), do: "ess:gateway-write:" <> Base.url_encode64(id, padding: false)

  defp key(:gateway_delete, id),
    do: "ess:gateway-delete:" <> Base.url_encode64(id, padding: false)
end
