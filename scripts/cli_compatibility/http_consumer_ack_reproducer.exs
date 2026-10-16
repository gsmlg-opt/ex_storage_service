# Run from the repository root:
#   mix run --no-start scripts/cli_compatibility/http_consumer_ack_reproducer.exs
# Or from apps/ex_storage_service_cli:
#   mix run --no-start ../../scripts/cli_compatibility/http_consumer_ack_reproducer.exs
# A 512-byte download at 8 bytes/second takes about 63 seconds after the initial
# burst. Failure remains a nonzero exit; this is a diagnostic, not an acceptance
# exception. See https://github.com/gsmlg-dev/http_fetch/issues/131.

defmodule HTTPConsumerAckReproducer do
  alias ExStorageServiceCli.{Context, Error, S3.Client, Transport.HTTPFetch}
  alias ExStorageServiceCli.CLI.Parser
  alias ExStorageServiceCli.Transfer.RateLimiter

  @payload :binary.copy(<<0, 255, 43, 37>>, 128)
  @rate 8
  @run_timeout 90_000

  def run do
    {:ok, _} = Application.ensure_all_started(:http_fetch)
    default_timeout = HTTP.Config.streaming_timeout()
    started = System.monotonic_time(:millisecond)
    {:ok, limiter} = RateLimiter.start_link(owner: self())

    result =
      try do
        probe(limiter)
      after
        stop_owned(limiter)
      end

    result =
      Map.merge(result, %{
        http_fetch_version: Application.spec(:http_fetch, :vsn) |> to_string(),
        global_streaming_timeout_before_ms: default_timeout,
        global_streaming_timeout_after_ms: HTTP.Config.streaming_timeout(),
        duration_ms: System.monotonic_time(:millisecond) - started,
        expected_bytes: byte_size(@payload),
        limit_download_bytes_per_second: @rate,
        diagnostic_timeout_ms: @run_timeout
      })

    result =
      cond do
        default_timeout != result.global_streaming_timeout_after_ms ->
          %{result | status: "global_timeout_changed"}

        result.status == "success" and not result.exact_bytes ->
          %{result | status: "byte_mismatch"}

        true ->
          result
      end

    IO.puts(JSON.encode!(result))
    if result.status == "success", do: 0, else: 1
  end

  defp probe(limiter) do
    {:ok, listener} = :gen_tcp.listen(0, [:binary, active: false, ip: {127, 0, 0, 1}])
    {:ok, {_, port}} = :inet.sockname(listener)
    server = spawn(fn -> serve(listener) end)
    owner = self()
    reference = make_ref()
    task = Task.async(fn -> transfer(port, limiter, owner, reference) end)

    try do
      result =
        case Task.yield(task, @run_timeout) do
          {:ok, result} -> result
          nil -> %{status: "diagnostic_timeout"}
        end

      bytes = delivered(reference, <<>>)
      Map.merge(result, %{delivered_bytes: byte_size(bytes), exact_bytes: bytes == @payload})
    after
      Task.shutdown(task, :brutal_kill)
      :gen_tcp.close(listener)
      stop_owned(server)
    end
  end

  defp serve(listener) do
    {:ok, socket} = :gen_tcp.accept(listener, 5_000)

    try do
      {:ok, _} = :gen_tcp.recv(socket, 0, 5_000)

      :ok =
        :gen_tcp.send(
          socket,
          "HTTP/1.1 200 OK\r\nContent-Length: 512\r\nConnection: close\r\n\r\n" <> @payload
        )
    after
      :gen_tcp.close(socket)
    end
  end

  defp transfer(port, limiter, owner, reference) do
    endpoint = "http://127.0.0.1:#{port}"
    {:ok, invocation} = Parser.parse(["ls", "--endpoint", endpoint, "--no-sign-request"])
    {:ok, context} = Context.native(nil, invocation.options, %{})

    result =
      case Client.call(context, "GetObject", %{"Bucket" => "b", "Key" => "k"},
             mode: :stream,
             limit_download: @rate,
             rate_limiter: limiter
           ) do
        {:ok, response} ->
          try do
            HTTPFetch.consume(response, 0, fn bytes, count ->
              send(owner, {reference, bytes})
              {:ok, count + byte_size(bytes)}
            end)
          after
            HTTPFetch.close(response)
          end

        error ->
          error
      end

    Map.merge(status(result), %{
      overall_timeout: context.timeout,
      read_idle_timeout_ms: context.read_idle_timeout,
      write_idle_timeout_ms: context.write_idle_timeout
    })
  rescue
    error -> %{status: "diagnostic_error", error_type: inspect(error.__struct__)}
  catch
    kind, _reason -> %{status: "diagnostic_error", error_type: to_string(kind)}
  end

  defp status({:ok, _count, _metadata}), do: %{status: "success"}

  defp status({:error, %Error{category: category, code: code}}),
    do: %{status: "transfer_failed", error_category: category, error_code: code}

  defp delivered(reference, bytes) do
    receive do
      {^reference, piece} -> delivered(reference, bytes <> piece)
    after
      0 -> bytes
    end
  end

  defp stop_owned(pid) do
    Process.unlink(pid)
    monitor = Process.monitor(pid)
    Process.exit(pid, :shutdown)

    receive do
      {:DOWN, ^monitor, :process, ^pid, _} -> :ok
    after
      1_000 ->
        Process.exit(pid, :kill)

        receive do
          {:DOWN, ^monitor, :process, ^pid, _} -> :ok
        after
          1_000 -> raise "Owned diagnostic process did not stop"
        end
    end
  end
end

System.halt(HTTPConsumerAckReproducer.run())
