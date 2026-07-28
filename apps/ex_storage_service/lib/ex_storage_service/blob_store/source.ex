defmodule ExStorageService.BlobStore.Source do
  @moduledoc """
  A servable blob source.

  File sources retain an explicit offset and length so callers can use
  `send_file` for loose, legacy, packed, and ranged reads. Stateful stream
  producers thread adapter state such as `Plug.Conn` without an auxiliary
  process or object-sized buffering.
  """

  @file_chunk_size 262_144

  @type reduce_result(acc) ::
          {:cont, acc} | {:halt, term(), acc}
  @type reducer(acc) :: (binary(), acc -> reduce_result(acc))
  @type producer(acc) :: (acc, reducer(acc) ->
                            {:ok, acc} | {:error, term(), acc})

  @type t ::
          {:file, String.t(), non_neg_integer(), non_neg_integer()}
          | {:stream, Enumerable.t() | function() | {:stateful, function()}, non_neg_integer()}

  @spec file(String.t(), non_neg_integer(), non_neg_integer()) :: t()
  def file(path, offset, length), do: {:file, path, offset, length}

  @spec stream(Enumerable.t() | function() | {:stateful, function()}, non_neg_integer()) :: t()
  def stream(enumerable_or_callback, content_length),
    do: {:stream, enumerable_or_callback, content_length}

  @spec stateful_stream(producer(acc), non_neg_integer()) :: t() when acc: term()
  def stateful_stream(producer, content_length) when is_function(producer, 2),
    do: {:stream, {:stateful, producer}, content_length}

  @doc "Returns the exact number of bytes exposed by a source."
  @spec content_length(t()) :: non_neg_integer()
  def content_length({:file, _path, _offset, length}), do: length
  def content_length({:stream, _producer, length}), do: length

  @doc """
  Adapts a source to a bounded request-body enumerable.

  The adapter is intended for HTTP/1 clients that consume request enumerables
  without suspension. Each upstream chunk is passed directly to the HTTP
  reducer; no object-sized binary is assembled.
  """
  @spec request_body(t()) :: Enumerable.t()
  def request_body(source), do: struct!(__MODULE__.RequestBody, source: source)

  @doc """
  Reduces a stream source while preserving caller state between chunks.

  `{:halt, reason, state}` cancels a callback-backed upstream immediately and
  returns the final caller state alongside the reason.
  """
  @spec reduce(t(), acc, reducer(acc)) ::
          {:ok, acc} | {:error, term(), acc}
        when acc: term()
  def reduce({:file, path, offset, length}, initial, reducer) do
    case :file.open(String.to_charlist(path), [:read, :raw, :binary]) do
      {:ok, io} ->
        try do
          case :file.position(io, offset) do
            {:ok, ^offset} -> reduce_file(io, length, initial, reducer)
            {:ok, actual} -> {:error, {:invalid_file_offset, actual}, initial}
            {:error, reason} -> {:error, {:file_position, reason}, initial}
          end
        after
          :file.close(io)
        end

      {:error, reason} ->
        {:error, {:file_open, reason}, initial}
    end
  end

  def reduce({:stream, {:stateful, producer}, _length}, initial, reducer),
    do: producer.(initial, reducer)

  def reduce({:stream, producer, _length}, initial, reducer) when is_function(producer, 1) do
    table = :ets.new(:blob_source_reducer, [:set, :private])
    true = :ets.insert(table, {:state, initial})

    try do
      result =
        producer.(fn chunk ->
          [{:state, current}] = :ets.lookup(table, :state)

          case reducer.(chunk, current) do
            {:cont, next} ->
              true = :ets.insert(table, {:state, next})
              :ok

            {:halt, reason, next} ->
              true = :ets.insert(table, {:state, next})
              {:error, reason}
          end
        end)

      [{:state, final}] = :ets.lookup(table, :state)

      case result do
        :ok -> {:ok, final}
        {:error, reason} -> {:error, reason, final}
      end
    after
      :ets.delete(table)
    end
  end

  def reduce({:stream, enumerable, _length}, initial, reducer) do
    enumerable
    |> Enum.reduce_while({:ok, initial}, fn chunk, {:ok, acc} ->
      case reducer.(chunk, acc) do
        {:cont, next} -> {:cont, {:ok, next}}
        {:halt, reason, next} -> {:halt, {:error, reason, next}}
      end
    end)
  end

  defp reduce_file(_io, 0, acc, _reducer), do: {:ok, acc}

  defp reduce_file(io, remaining, acc, reducer) do
    case :file.read(io, min(remaining, @file_chunk_size)) do
      {:ok, data} ->
        case reducer.(data, acc) do
          {:cont, next} -> reduce_file(io, remaining - byte_size(data), next, reducer)
          {:halt, reason, next} -> {:error, reason, next}
        end

      :eof ->
        {:error, :unexpected_eof, acc}

      {:error, reason} ->
        {:error, {:file_read, reason}, acc}
    end
  end
end

defmodule ExStorageService.BlobStore.Source.RequestBody do
  @moduledoc false

  @enforce_keys [:source]
  defstruct [:source]
end

defmodule ExStorageService.BlobStore.Source.RequestBodyError do
  @moduledoc false

  defexception [:reason]

  @impl true
  def message(%__MODULE__{reason: reason}),
    do: "blob source request body failed: #{inspect(reason)}"
end

defimpl Enumerable, for: ExStorageService.BlobStore.Source.RequestBody do
  alias ExStorageService.BlobStore.Source
  alias ExStorageService.BlobStore.Source.RequestBodyError

  def reduce(_body, {:halt, acc}, _reducer), do: {:halted, acc}

  def reduce(body, {:suspend, acc}, reducer),
    do: {:suspended, acc, &reduce(body, &1, reducer)}

  def reduce(%{source: source}, {:cont, initial}, reducer) do
    case Source.reduce(source, initial, fn chunk, current ->
           case reducer.(chunk, current) do
             {:cont, next} -> {:cont, next}
             {:halt, next} -> {:halt, :request_body_halted, next}
             {:suspend, _next} -> raise ArgumentError, "request body suspension is unsupported"
           end
         end) do
      {:ok, final} ->
        {:done, final}

      {:error, :request_body_halted, final} ->
        {:halted, final}

      {:error, reason, _final} ->
        raise RequestBodyError, reason: reason
    end
  end

  def member?(_body, _value), do: {:error, __MODULE__}
  def count(_body), do: {:error, __MODULE__}
  def slice(_body), do: {:error, __MODULE__}
end
