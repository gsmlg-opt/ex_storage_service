defmodule ExStorageService.BlobStore.Source do
  @moduledoc """
  A servable blob source.

  File sources retain an explicit offset and length so callers can use
  `send_file` for loose, legacy, packed, and ranged reads. Stateful stream
  producers thread adapter state such as `Plug.Conn` without an auxiliary
  process or object-sized buffering.
  """

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

  @doc """
  Reduces a stream source while preserving caller state between chunks.

  `{:halt, reason, state}` cancels a callback-backed upstream immediately and
  returns the final caller state alongside the reason.
  """
  @spec reduce(t(), acc, reducer(acc)) ::
          {:ok, acc} | {:error, term(), acc}
        when acc: term()
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
end
