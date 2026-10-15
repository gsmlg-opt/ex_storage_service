defmodule ExStorageServiceCli.Transfer.RateLimiter do
  @moduledoc "Invocation-owned aggregate endpoint token buckets and bounded entity-byte pacing."
  use GenServer
  alias ExStorageServiceCli.Error
  @scale 1_000_000_000
  @max_rate 9_223_372_036_854_775_807
  @chunk 65_536

  def child_spec(opts),
    do: %{id: __MODULE__, start: {__MODULE__, :start_link, [opts]}, restart: :temporary}

  def start_link(opts \\ []), do: GenServer.start_link(__MODULE__, opts)

  def parse_rate(value) when is_integer(value) and value in 0..@max_rate, do: {:ok, value}

  def parse_rate(value) when is_binary(value) and byte_size(value) <= 128 do
    case if(String.valid?(value), do: Regex.run(~r/\A([0-9]+(?:\.[0-9]+)?)\s*([a-z]*)\z/i, value)) do
      [_, number, unit] ->
        factor =
          Enum.find_value(Enum.with_index(["", "k", "m", "g", "t", "p", "e"]), fn {suffix,
                                                                                   exponent} ->
            cond do
              String.downcase(unit) in [suffix, suffix <> "b"] ->
                Integer.pow(1000, exponent)

              suffix != "" and String.downcase(unit) in [suffix <> "i", suffix <> "ib"] ->
                Integer.pow(1024, exponent)

              true ->
                nil
            end
          end)

        [whole | fractional] = String.split(number, ".", parts: 2)
        fraction = List.first(fractional) || ""
        numerator = String.to_integer(whole <> fraction)
        bytes = if factor, do: div(numerator * factor, Integer.pow(10, byte_size(fraction)))

        if bytes != nil and bytes <= @max_rate and (bytes > 0 or numerator == 0),
          do: {:ok, bytes},
          else: invalid_rate()

      _ ->
        invalid_rate()
    end
  end

  def parse_rate(_), do: invalid_rate()

  defp invalid_rate,
    do:
      failure(
        :syntax,
        :invalid_rate,
        "Rate limits require nonnegative byte counts or SI/IEC sizes within signed 64-bit bytes; 0 means unlimited"
      )

  @doc "Sanitized endpoint identity; credentials, query parameters, and fragments are never bucket keys."
  def endpoint(value) do
    uri = URI.parse(value)

    {uri.scheme, String.downcase(uri.host || ""), uri.port,
     String.trim_trailing(uri.path || "", "/")}
  end

  def acquire(pid, endpoint, direction, bytes, rate, opts \\ [])
  def acquire(_pid, _endpoint, _direction, _bytes, 0, _opts), do: :ok

  def acquire(pid, endpoint, direction, bytes, rate, opts)
      when is_pid(pid) and direction in [:upload, :download] and is_integer(rate) and rate > 0 and
             is_integer(bytes) and bytes >= 0 and bytes <= rate and bytes <= @chunk do
    with :ok <- check(opts) do
      reference = make_ref()

      case GenServer.call(pid, {:take, endpoint, direction, bytes, rate, reference, opts}) do
        :ok -> :ok
        :queued -> await(pid, reference, opts)
        error -> error
      end
    end
  catch
    :exit, _ -> failure(:interrupted, :rate_owner_closed, "Rate-limit ownership ended")
  end

  def acquire(_, _, _, _, _, _),
    do:
      failure(
        :configuration,
        :rate_owner_required,
        "A rate limit requires an invocation-owned limiter and a bounded byte slice"
      )

  defp await(pid, reference, opts) do
    monitor = Process.monitor(pid)
    guards = Enum.map(opts[:guards] || [], &{&1, Process.monitor(&1)})

    try do
      await_loop(pid, reference, monitor, Map.new(guards, &{elem(&1, 1), true}), opts)
    after
      Process.demonitor(monitor, [:flush])
      Enum.each(guards, &Process.demonitor(elem(&1, 1), [:flush]))
    end
  end

  defp await_loop(pid, reference, monitor, guards, opts) do
    case check(opts) do
      :ok ->
        receive do
          {:rate_grant, ^reference} ->
            check(opts)

          {:rate_error, ^reference, error} ->
            {:error, error}

          {:DOWN, ref, :process, _, _} when ref == monitor or is_map_key(guards, ref) ->
            cancel(pid, reference)
            failure(:interrupted, :rate_owner_closed, "Rate-limit ownership ended")

          {:ess_cancel, _} ->
            cancel(pid, reference)
            failure(:interrupted, :cancelled, "Rate-limited transfer interrupted")
        after
          25 -> await_loop(pid, reference, monitor, guards, opts)
        end

      error ->
        cancel(pid, reference)
        error
    end
  end

  defp cancel(pid, reference) do
    GenServer.call(pid, {:cancel, reference})

    receive do
      {:rate_grant, ^reference} -> :ok
    after
      0 -> :ok
    end
  catch
    :exit, _ -> :ok
  end

  defp check(opts) do
    cond do
      opts[:deadline] != nil and System.monotonic_time(:millisecond) >= opts[:deadline] ->
        failure(
          :transport,
          :request_timeout,
          "Rate-limited transfer exceeded its request deadline"
        )

      Enum.any?(opts[:guards] || [], &(not Process.alive?(&1))) ->
        failure(:interrupted, :rate_owner_closed, "Rate-limit ownership ended")

      opts[:signal] != nil and aborted?(opts[:signal]) ->
        failure(:interrupted, :aborted, "Rate-limited transfer interrupted")

      true ->
        :ok
    end
  end

  defp aborted?(signal) do
    HTTP.AbortController.aborted?(signal)
  catch
    :exit, _ -> true
  end

  @impl true
  def init(opts) do
    owner = opts[:owner] || self()

    {:ok,
     %{
       owner: Process.monitor(owner),
       buckets: %{},
       waiting: %{},
       timer: nil,
       clock: opts[:clock] || fn -> System.monotonic_time(:nanosecond) end,
       maximum: opts[:max_waiters] || 1024
     }}
  end

  @impl true
  def handle_call({:take, endpoint, direction, bytes, rate, reference, opts}, {caller, _}, state) do
    key = {endpoint, direction}
    now = state.clock.()

    bucket =
      Map.get(state.buckets, key, %{
        rate: rate,
        credit: rate * @scale,
        updated: now,
        queue: :queue.new()
      })

    bucket = refill(bucket, now)

    cond do
      bucket.rate != rate ->
        {:reply,
         failure(
           :configuration,
           :rate_conflict,
           "One endpoint cannot have conflicting invocation rate limits"
         ), state}

      not Map.has_key?(state.buckets, key) and map_size(state.buckets) >= 512 ->
        {:reply,
         failure(:client, :rate_endpoint_limit, "Invocation rate-limit endpoint bound exceeded"),
         state}

      :queue.is_empty(bucket.queue) and bucket.credit >= bytes * @scale ->
        {:reply, :ok,
         %{
           state
           | buckets:
               Map.put(state.buckets, key, %{bucket | credit: bucket.credit - bytes * @scale})
         }}

      map_size(state.waiting) >= state.maximum ->
        {:reply,
         failure(:client, :rate_queue_full, "Invocation rate-limit waiting bound exceeded"),
         state}

      true ->
        item = %{
          caller: caller,
          monitor: Process.monitor(caller),
          key: key,
          bytes: bytes,
          opts: opts
        }

        bucket = %{bucket | queue: :queue.in(reference, bucket.queue)}

        state = %{
          state
          | buckets: Map.put(state.buckets, key, bucket),
            waiting: Map.put(state.waiting, reference, item)
        }

        {:reply, :queued, schedule(state)}
    end
  end

  def handle_call({:cancel, reference}, _from, state),
    do: {:reply, :ok, remove(state, reference) |> advance()}

  def handle_call(:stats, _from, state),
    do: {:reply, %{waiting: map_size(state.waiting), buckets: map_size(state.buckets)}, state}

  @impl true
  def handle_info({:DOWN, reference, :process, _, _}, %{owner: reference} = state),
    do: {:stop, :normal, state}

  def handle_info({:DOWN, monitor, :process, _, _}, state) do
    reference =
      Enum.find_value(state.waiting, fn {reference, item} ->
        if item.monitor == monitor, do: reference
      end)

    {:noreply, remove(state, reference) |> advance()}
  end

  def handle_info(:tick, state), do: {:noreply, advance(%{state | timer: nil})}

  @impl true
  def terminate(_, state) do
    if state.timer, do: Process.cancel_timer(state.timer)
    :ok
  end

  defp remove(state, reference) do
    case Map.pop(state.waiting, reference) do
      {nil, _} ->
        state

      {item, waiting} ->
        Process.demonitor(item.monitor, [:flush])
        bucket = state.buckets[item.key]

        bucket = %{
          bucket
          | queue:
              bucket.queue
              |> :queue.to_list()
              |> Enum.reject(&(&1 == reference))
              |> :queue.from_list()
        }

        %{state | waiting: waiting, buckets: Map.put(state.buckets, item.key, bucket)}
    end
  end

  defp refill(bucket, now),
    do: %{
      bucket
      | credit:
          min(bucket.rate * @scale, bucket.credit + max(now - bucket.updated, 0) * bucket.rate),
        updated: now
    }

  defp advance(state) do
    state =
      Enum.reduce(Map.keys(state.buckets), state, fn key, acc -> grant(acc, key, acc.clock.()) end)

    schedule(state)
  end

  defp grant(state, key, now) do
    bucket = refill(state.buckets[key], now)
    state = %{state | buckets: Map.put(state.buckets, key, bucket)}

    case :queue.peek(bucket.queue) do
      {:value, reference} ->
        item = state.waiting[reference]

        cond do
          not Process.alive?(item.caller) ->
            grant(remove(state, reference), key, now)

          check(item.opts) != :ok ->
            {:error, error} = check(item.opts)
            send(item.caller, {:rate_error, reference, error})
            grant(remove(state, reference), key, now)

          bucket.credit >= item.bytes * @scale ->
            send(item.caller, {:rate_grant, reference})
            state = remove(state, reference)
            bucket = state.buckets[key]

            grant(
              %{
                state
                | buckets:
                    Map.put(state.buckets, key, %{
                      bucket
                      | credit: bucket.credit - item.bytes * @scale
                    })
              },
              key,
              now
            )

          true ->
            state
        end

      :empty ->
        state
    end
  end

  defp schedule(state) do
    if state.timer, do: Process.cancel_timer(state.timer)

    delays =
      for {_key, bucket} <- state.buckets, {:value, reference} <- [:queue.peek(bucket.queue)] do
        deficit = max(state.waiting[reference].bytes * @scale - bucket.credit, 0)
        max(div(deficit + bucket.rate * 1_000_000 - 1, bucket.rate * 1_000_000), 1)
      end

    %{
      state
      | timer:
          if(delays == [], do: nil, else: Process.send_after(self(), :tick, Enum.min(delays)))
    }
  end

  @doc "Charges bounded slices before delivering raw bytes, retaining the caller's reducer contract."
  def reduce(bytes, nil, state, reducer), do: reducer.(bytes, state)
  def reduce("", _limit, state, reducer), do: reducer.("", state)

  def reduce(bytes, limit, state, reducer) do
    size = min(byte_size(bytes), min(limit.rate, @chunk))
    <<piece::binary-size(size), rest::binary>> = bytes

    with :ok <- acquire(limit.pid, limit.endpoint, limit.direction, size, limit.rate, limit.opts),
         {:ok, state} <- reducer.(piece, state) do
      if rest == "", do: {:ok, state}, else: reduce(rest, limit, state, reducer)
    end
  end

  @doc "Wraps an existing body without changing its length or wire bytes; proxy ownership is explicit."
  def upload(body, nil), do: {:ok, body, nil}
  def upload(body, _limit) when body in [nil, ""], do: {:ok, body, nil}

  def upload(body, limit) when is_binary(body) or is_pid(body) do
    owner = self()
    reference = make_ref()
    guardian = spawn(fn -> upload_owner(owner, reference, body, limit) end)
    monitor = Process.monitor(guardian)

    receive do
      {^reference, stream} ->
        Process.demonitor(monitor, [:flush])
        {:ok, stream, guardian}

      {:DOWN, ^monitor, :process, _, _} ->
        failure(:transport, :rate_upload_start, "Cannot start owned rate-limited upload")
    end
  end

  def upload(_, _),
    do:
      failure(
        :client,
        :rate_body_type,
        "Rate-limited request bodies must be binary or acknowledged streams"
      )

  def close_upload(nil), do: :ok

  def close_upload(pid) do
    monitor = Process.monitor(pid)
    send(pid, :close)

    receive do
      {:DOWN, ^monitor, :process, _, _} -> :ok
    after
      1000 -> Process.exit(pid, :kill)
    end

    Process.demonitor(monitor, [:flush])
    :ok
  end

  defp upload_owner(owner, reference, body, limit) do
    Process.flag(:trap_exit, true)
    owner_ref = Process.monitor(owner)
    {:ok, stream} = HTTP.Stream.start_link(0, [], telemetry: false)
    stream_ref = Process.monitor(stream)

    worker =
      spawn_link(fn ->
        produce(stream, body, %{limit | opts: Keyword.put(limit.opts, :guards, [owner, stream])})
      end)

    send(owner, {reference, stream})

    receive do
      {:DOWN, ref, :process, _, _} when ref == owner_ref or ref == stream_ref ->
        :ok

      :close ->
        :ok

      {:EXIT, ^worker, :normal} ->
        receive do
          {:DOWN, ref, :process, _, _} when ref == owner_ref or ref == stream_ref -> :ok
          :close -> :ok
        end

      {:EXIT, _, _} ->
        :ok
    end

    if Process.alive?(worker), do: Process.exit(worker, :kill)
    if Process.alive?(stream), do: Process.exit(stream, :kill)
  end

  defp produce(stream, body, limit) do
    result =
      if is_binary(body), do: deliver(stream, body, limit), else: source(stream, body, limit)

    case result do
      :ok -> HTTP.Stream.finish(stream)
      {:ok, trailers} -> HTTP.Stream.finish(stream, trailers)
      {:error, %Error{code: code}} -> HTTP.Stream.error(stream, code)
    end
  rescue
    _ -> HTTP.Stream.error(stream, :rate_upload_failed)
  end

  defp deliver(stream, bytes, limit) do
    case reduce(bytes, limit, :ok, fn piece, :ok ->
           case HTTP.Stream.chunk(stream, piece, :infinity) do
             :ok ->
               {:ok, :ok}

             {:error, _} ->
               failure(:interrupted, :rate_upload_closed, "Rate-limited upload closed")
           end
         end) do
      {:ok, :ok} -> :ok
      error -> error
    end
  end

  defp source(stream, source, limit) do
    monitor = Process.monitor(source)
    send(source, {:read_chunk, self(), :ack})

    try do
      source_chunks(stream, source, monitor, limit, [])
    after
      Process.demonitor(monitor, [:flush])
    end
  end

  defp source_chunks(stream, source, monitor, limit, trailers) do
    receive do
      {:stream_chunk, ^source, bytes, reference} when byte_size(bytes) <= @chunk ->
        with :ok <- deliver(stream, bytes, limit) do
          send(source, {:stream_chunk_ack, reference})
          source_chunks(stream, source, monitor, limit, trailers)
        end

      {:stream_chunk, ^source, bytes} when byte_size(bytes) <= @chunk ->
        with :ok <- deliver(stream, bytes, limit),
             do: source_chunks(stream, source, monitor, limit, trailers)

      {:stream_trailers, ^source, fields} ->
        source_chunks(stream, source, monitor, limit, fields)

      {:stream_end, ^source} ->
        {:ok, trailers}

      {:stream_error, ^source, _} ->
        failure(:transport, :rate_source_error, "Rate-limited upload source failed")

      {:DOWN, ^monitor, :process, _, _} ->
        failure(:transport, :rate_source_closed, "Rate-limited upload source ended")

      _ ->
        failure(
          :transport,
          :rate_source_chunk,
          "Rate-limited upload source exceeded its chunk bound"
        )
    end
  end

  defp failure(category, code, message), do: {:error, Error.new(category, message, code: code)}
end
