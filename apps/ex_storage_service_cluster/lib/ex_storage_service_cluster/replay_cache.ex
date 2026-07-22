defmodule ExStorageServiceCluster.ReplayCache do
  @moduledoc """
  Owns the ETS table used to reject replayed internal requests.

  Request processes claim identifiers directly with `claim/3`; the GenServer
  exists only to own the table and periodically remove expired claims.
  """

  use GenServer

  @default_sweep_interval 60_000

  @type table :: :ets.table()

  @spec start_link(keyword()) :: GenServer.on_start()
  def start_link(opts \\ []) do
    case Keyword.get(opts, :name, __MODULE__) do
      nil -> GenServer.start_link(__MODULE__, opts)
      name -> GenServer.start_link(__MODULE__, opts, name: name)
    end
  end

  @doc "Returns the ETS table owned by a replay-cache process."
  @spec table(GenServer.server()) :: table()
  def table(server \\ __MODULE__), do: GenServer.call(server, :table)

  @doc """
  Atomically claims a request id until the supplied monotonic deadline.

  This function writes ETS directly so authentication is not serialized
  through the cache owner.
  """
  @spec claim(table(), binary(), integer()) ::
          :ok | {:error, :replayed_request | :cache_unavailable}
  def claim(table, request_id, expires_at_ms)
      when is_binary(request_id) and byte_size(request_id) > 0 and is_integer(expires_at_ms) do
    if :ets.insert_new(table, {request_id, expires_at_ms}) do
      :ok
    else
      {:error, :replayed_request}
    end
  rescue
    ArgumentError -> {:error, :cache_unavailable}
  end

  @impl true
  def init(opts) do
    table = create_table(Keyword.get(opts, :table))
    sweep_interval = Keyword.get(opts, :sweep_interval, @default_sweep_interval)
    now_ms = Keyword.get(opts, :now_ms, fn -> System.monotonic_time(:millisecond) end)

    state = %{table: table, sweep_interval: sweep_interval, now_ms: now_ms}
    schedule_sweep(sweep_interval)
    {:ok, state}
  end

  @impl true
  def handle_call(:table, _from, state), do: {:reply, state.table, state}

  @impl true
  def handle_info(:sweep, state) do
    delete_expired(state.table, state.now_ms.())
    schedule_sweep(state.sweep_interval)
    {:noreply, state}
  end

  defp create_table(nil) do
    :ets.new(__MODULE__, [:set, :public, read_concurrency: true, write_concurrency: true])
  end

  defp create_table(name) when is_atom(name) do
    :ets.new(name, [
      :set,
      :public,
      :named_table,
      read_concurrency: true,
      write_concurrency: true
    ])
  end

  defp delete_expired(table, now_ms) do
    :ets.select_delete(table, [{{:"$1", :"$2"}, [{:"=<", :"$2", now_ms}], [true]}])
  end

  defp schedule_sweep(interval) when is_integer(interval) and interval > 0 do
    Process.send_after(self(), :sweep, interval)
  end
end
