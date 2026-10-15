defmodule ExStorageServiceCli.Transfer.RateLimiterTest do
  use ExUnit.Case, async: true
  alias ExStorageServiceCli.{Error, Transfer.RateLimiter}

  test "rate grammar parses decimal SI IEC and unlimited values without float overflow" do
    for {value, bytes} <- [
          {"0", 0},
          {"0.0B", 0},
          {0, 0},
          {"1.5KiB", 1536},
          {"1.5 MB", 1_500_000},
          {"2GiB", 2_147_483_648},
          {"1m", 1_000_000},
          {"9223372036854775807", 9_223_372_036_854_775_807}
        ] do
      assert {:ok, ^bytes} = RateLimiter.parse_rate(value)
    end

    for value <- [
          "",
          "-1",
          "0.1B",
          "1KiB/s",
          "1e3",
          "9223372036854775808",
          "8EiB",
          "bad",
          String.duplicate("1", 129),
          nil
        ] do
      assert {:error, %Error{category: :syntax}} = RateLimiter.parse_rate(value)
    end
  end

  test "endpoint bucket identity never includes credentials query or fragments" do
    assert RateLimiter.endpoint(
             "https://fixture:secret@example.test:9443/base?token=secret#fragment"
           ) ==
             {"https", "example.test", 9443, "/base"}

    assert RateLimiter.endpoint("https://EXAMPLE.test/base/") ==
             RateLimiter.endpoint("https://example.test/base")

    assert RateLimiter.endpoint("https://example.test/") ==
             RateLimiter.endpoint("https://example.test")
  end

  test "one second burst is shared by concurrent callers in FIFO order" do
    {limiter, clock} = limiter()
    assert :ok = take(limiter, 1000)
    first = waiter(limiter, 600)
    await_count(limiter, 1)
    second = waiter(limiter, 400)
    await_count(limiter, 2)
    advance(limiter, clock, 400_000_000)
    refute_receive {:taken, _, _}
    advance(limiter, clock, 200_000_000)
    assert_receive {:taken, ^first, :ok}
    refute_receive {:taken, ^second, _}
    advance(limiter, clock, 400_000_000)
    assert_receive {:taken, ^second, :ok}
  end

  test "directions and distinct endpoints have separate one second buckets" do
    {limiter, _clock} = limiter()
    assert :ok = take(limiter, 1000)
    assert :ok = RateLimiter.acquire(limiter, :endpoint, :download, 1000, 1000)
    assert :ok = RateLimiter.acquire(limiter, :other_endpoint, :upload, 1000, 1000)

    assert {:error, %Error{code: :rate_conflict}} =
             RateLimiter.acquire(limiter, :endpoint, :upload, 1, 2000)

    assert :ok = RateLimiter.acquire(nil, :endpoint, :upload, 1000, 0)
  end

  test "caller death removes pending credit without charging later callers" do
    {limiter, clock} = limiter()
    assert :ok = take(limiter, 1000)
    dead = waiter(limiter, 1000)
    await_count(limiter, 1)
    Process.exit(dead, :kill)
    await_count(limiter, 0)
    advance(limiter, clock, 1_000_000_000)
    assert :ok = take(limiter, 1000)
  end

  test "deadline cancellation removes an unspent queue reservation" do
    {limiter, clock} = limiter()
    assert :ok = take(limiter, 1000)
    deadline = System.monotonic_time(:millisecond) + 10
    task = waiter(limiter, 1000, deadline: deadline)
    assert_receive {:taken, ^task, {:error, %Error{code: :request_timeout}}}, 1000
    await_count(limiter, 0)
    advance(limiter, clock, 1_000_000_000)
    assert :ok = take(limiter, 1000)
  end

  test "cancelled signal and guard death remove waiters without future credit debt" do
    {limiter, clock} = limiter()
    assert :ok = take(limiter, 1000)
    signal = HTTP.AbortController.new()
    on_exit(fn -> if Process.alive?(signal), do: Agent.stop(signal) end)
    task = waiter(limiter, 1000, signal: signal)
    await_count(limiter, 1)
    HTTP.AbortController.abort(signal)
    assert_receive {:taken, ^task, {:error, %Error{category: :interrupted}}}, 1000
    await_count(limiter, 0)

    guard =
      spawn(fn ->
        receive do
          :stop -> :ok
        end
      end)

    task = waiter(limiter, 1000, guards: [guard])
    await_count(limiter, 1)
    send(guard, :stop)
    assert_receive {:taken, ^task, {:error, %Error{category: :interrupted}}}, 1000
    await_count(limiter, 0)
    advance(limiter, clock, 1_000_000_000)
    assert :ok = take(limiter, 1000)
  end

  test "invocation owner death stops limiter and wakes queued callers" do
    owner =
      spawn(fn ->
        receive do
          :stop -> :ok
        end
      end)

    {:ok, limiter} = RateLimiter.start_link(owner: owner)
    assert :ok = take(limiter, 1000)
    task = waiter(limiter, 1000)
    await_count(limiter, 1)
    monitor = Process.monitor(limiter)
    send(owner, :stop)
    assert_receive {:DOWN, ^monitor, :process, ^limiter, :normal}
    assert_receive {:taken, ^task, {:error, %Error{category: :interrupted}}}
  end

  test "pending queue obeys its invocation bound" do
    {limiter, _clock} = limiter(max_waiters: 1)
    assert :ok = take(limiter, 1000)
    task = waiter(limiter, 1000)
    await_count(limiter, 1)
    assert {:error, %Error{code: :rate_queue_full}} = take(limiter, 1)
    Process.exit(task, :kill)
  end

  test "chunk reduction is bounded and preserves exact bytes" do
    {limiter, _clock} = limiter()
    limit = %{pid: limiter, endpoint: :endpoint, direction: :download, rate: 1_000_000, opts: []}
    bytes = :crypto.strong_rand_bytes(180_000)

    assert {:ok, chunks} =
             RateLimiter.reduce(bytes, limit, [], fn chunk, acc ->
               assert byte_size(chunk) <= 65_536
               {:ok, [chunk | acc]}
             end)

    assert IO.iodata_to_binary(Enum.reverse(chunks)) == bytes
  end

  defp limiter(opts \\ []) do
    clock = start_supervised!({Agent, fn -> 0 end})

    limiter =
      start_supervised!(
        {RateLimiter,
         Keyword.merge([owner: self(), clock: fn -> Agent.get(clock, & &1) end], opts)}
      )

    {limiter, clock}
  end

  defp take(limiter, count, opts \\ []),
    do: RateLimiter.acquire(limiter, :endpoint, :upload, count, 1000, opts)

  defp waiter(limiter, count, opts \\ []) do
    parent = self()
    spawn(fn -> send(parent, {:taken, self(), take(limiter, count, opts)}) end)
  end

  defp advance(limiter, clock, nanoseconds) do
    Agent.update(clock, &(&1 + nanoseconds))
    send(limiter, :tick)
    GenServer.call(limiter, :stats)
  end

  defp await_count(limiter, expected, attempts \\ 100) do
    case GenServer.call(limiter, :stats) do
      %{waiting: ^expected} ->
        :ok

      _ when attempts > 0 ->
        Process.sleep(1)
        await_count(limiter, expected, attempts - 1)

      result ->
        flunk("Unexpected limiter state #{inspect(result)}")
    end
  end
end
