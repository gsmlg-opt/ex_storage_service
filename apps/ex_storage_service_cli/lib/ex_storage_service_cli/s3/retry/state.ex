defmodule ExStorageServiceCli.S3.Retry.State do
  @moduledoc "Invocation-owned retry quota and pinned adaptive CUBIC token bucket."
  use GenServer

  def start_link(opts \\ []),
    do: GenServer.start_link(__MODULE__, Keyword.put_new(opts, :owner, self()))

  def token(server, now), do: GenServer.call(server, {:token, now})
  def response(server, throttled, now), do: GenServer.call(server, {:response, throttled, now})
  def acquire(server, cost), do: GenServer.call(server, {:acquire, cost})
  def release(server, cost), do: GenServer.call(server, {:release, cost})
  def snapshot(server), do: GenServer.call(server, :snapshot)
  def region(server, key), do: GenServer.call(server, {:region, key})
  def put_region(server, key, region), do: GenServer.call(server, {:put_region, key, region})

  @impl true
  def init(opts) do
    owner = opts[:owner] || self()
    now = opts[:now] || System.monotonic_time(:millisecond)

    {:ok,
     %{
       owner: Process.monitor(owner),
       quota: opts[:quota] || 500,
       enabled: false,
       rate: 1.0,
       capacity: 1.0,
       available: 0.0,
       timestamp: now,
       measured: 0.0,
       bucket: Float.floor(now / 1000),
       count: 0,
       w_max: 0.0,
       k: 0.0,
       last_fail: now,
       regions: %{},
       region_order: :queue.new()
     }}
  end

  @impl true
  def handle_call({:token, _now}, _from, %{enabled: false} = state),
    do: {:reply, :ok, state}

  def handle_call({:token, now}, _from, state) do
    state = refill(state, now)

    if state.available >= 1 do
      {:reply, :ok, %{state | available: state.available - 1}}
    else
      {:reply, {:wait, max(ceil((1 - state.available) / state.rate * 1000), 1)}, state}
    end
  end

  def handle_call({:response, throttled, now}, _from, state) do
    state = state |> measure(now) |> refill(now)

    {new_rate, state} =
      if throttled do
        rate = if state.enabled, do: min(state.measured, state.rate), else: state.measured

        {rate * 0.7,
         %{
           state
           | enabled: true,
             w_max: rate,
             k: :math.pow(rate * 0.3 / 0.4, 1 / 3),
             last_fail: now
         }}
      else
        {0.4 * :math.pow((now - state.last_fail) / 1000 - state.k, 3) + state.w_max, state}
      end

    rate = min(new_rate, 2 * state.measured)
    capacity = max(rate, 1.0)

    {:reply, :ok,
     %{
       state
       | rate: max(rate, 0.5),
         capacity: capacity,
         available: min(state.available, capacity)
     }}
  end

  def handle_call({:acquire, cost}, _from, state) do
    if state.quota >= cost,
      do: {:reply, true, %{state | quota: state.quota - cost}},
      else: {:reply, false, state}
  end

  def handle_call({:release, cost}, _from, state),
    do: {:reply, :ok, %{state | quota: min(state.quota + cost, 500)}}

  def handle_call(:snapshot, _from, state), do: {:reply, Map.delete(state, :owner), state}

  def handle_call({:region, key}, _from, state),
    do: {:reply, Map.fetch(state.regions, key), state}

  def handle_call({:put_region, key, region}, _from, state) do
    if cache_entry?(key, region) do
      {regions, order} =
        if Map.has_key?(state.regions, key) do
          {state.regions, state.region_order}
        else
          if map_size(state.regions) == 128 do
            {{:value, oldest}, order} = :queue.out(state.region_order)
            {Map.delete(state.regions, oldest), :queue.in(key, order)}
          else
            {state.regions, :queue.in(key, state.region_order)}
          end
        end

      {:reply, :ok, %{state | regions: Map.put(regions, key, region), region_order: order}}
    else
      {:reply, {:error, :invalid_region_cache_entry}, state}
    end
  end

  @impl true
  def handle_info({:DOWN, ref, :process, _pid, _reason}, %{owner: ref} = state),
    do: {:stop, :normal, state}

  defp refill(state, now) do
    available =
      min(state.capacity, state.available + max(now - state.timestamp, 0) / 1000 * state.rate)

    %{state | available: available, timestamp: now}
  end

  defp measure(state, now) do
    bucket = Float.floor(now / 500) / 2
    count = state.count + 1

    if bucket > state.bucket do
      measured = count / (bucket - state.bucket) * 0.8 + state.measured * 0.2
      %{state | measured: measured, bucket: bucket, count: 0}
    else
      %{state | count: count}
    end
  end

  defp cache_entry?({endpoint, addressing, initial_region, bucket}, region)
       when addressing in [:path, :virtual] and is_binary(bucket) and byte_size(bucket) in 1..2048 do
    endpoint_ok =
      is_nil(endpoint) or
        (is_binary(endpoint) and byte_size(endpoint) <= 8192 and
           ExStorageServiceCli.Context.validate_endpoint(endpoint) == :ok)

    endpoint_ok and valid_region?(initial_region) and valid_region?(region) and
      not String.starts_with?(bucket, "arn:")
  end

  defp cache_entry?(_, _), do: false

  defp valid_region?(region),
    do:
      is_binary(region) and byte_size(region) <= 63 and
        Regex.match?(~r/^[a-z]{2,4}(?:-[a-z0-9]+)+-[0-9]+$/, region)
end
