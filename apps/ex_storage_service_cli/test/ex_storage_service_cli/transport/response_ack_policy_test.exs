Code.require_file("../../support/wire_server.exs", __DIR__)

defmodule ExStorageServiceCli.Transport.ResponseAckPolicyTest do
  use ExUnit.Case, async: true

  alias ExStorageServiceCli.S3.Request
  alias ExStorageServiceCli.Test.WireServer
  alias ExStorageServiceCli.Transport.HTTPFetch

  test "adapter pacing leaves a concurrent request's finite acknowledgement policy intact" do
    assert HTTP.Config.streaming_timeout() == 60_000
    body = :zlib.gzip(:crypto.strong_rand_bytes(96))
    {adapter_endpoint, adapter_peer} = peer(:adapter, body)
    {finite_endpoint, finite_peer} = peer(:finite, body)
    finite_ack_timeout = 200

    assert {:ok, response} =
             HTTPFetch.request(adapter_endpoint, %Request{method: "GET"},
               mode: :stream,
               timeout: 3_000
             )

    on_exit(fn -> HTTPFetch.close(response) end)

    monitors =
      for pid <- [
            response.body,
            response.controller,
            response.cancellation,
            adapter_peer,
            finite_peer
          ],
          do: {pid, Process.monitor(pid)}

    owner = self()
    gate = make_ref()
    started = System.monotonic_time(:millisecond)

    consumer =
      Task.async(fn ->
        HTTPFetch.consume(response, [], fn bytes, chunks ->
          assert byte_size(bytes) <= 32

          if chunks == [] do
            # The adapter has received a chunk but cannot acknowledge it until
            # this application sink returns.
            send(owner, {:adapter_pending_ack, self()})

            receive do
              {:release_sink, ^gate} -> :ok
            after
              2_000 -> flunk("application sink was not released")
            end
          end

          {:ok, [bytes | chunks]}
        end)
      end)

    assert_receive {:adapter_pending_ack, consumer_pid}, 1_000
    assert consumer_pid == consumer.pid

    promise =
      HTTP.fetch(finite_endpoint,
        stream_response: true,
        http_version: :http1,
        response_stream_idle_timeout: :infinity,
        response_stream_ack_timeout: finite_ack_timeout,
        timeout: 3_000,
        decode_body: false,
        redirect: :manual
      )

    assert %HTTP.Response{status: 200, body: finite_stream} = HTTP.Promise.await(promise)
    finite_monitor = Process.monitor(finite_stream)
    send(finite_stream, {:read_chunk, self(), :ack})
    assert_receive {:stream_chunk, ^finite_stream, first, reference}, 1_000
    assert is_reference(reference)
    assert first == binary_part(body, 0, 32)

    # Leave the direct request's real acknowledgement outstanding while the
    # adapter is independently waiting for its application sink.
    assert_receive {:stream_error, ^finite_stream, :timeout}, 1_000
    assert_receive {:DOWN, ^finite_monitor, :process, ^finite_stream, :normal}, 1_000
    assert :ok = HTTP.RequestCompletion.await(HTTP.Promise.completion(promise), 1_000)
    assert_receive {:peer_closed, :finite, {:error, :closed}}, 1_000
    assert System.monotonic_time(:millisecond) - started >= finite_ack_timeout
    assert Process.alive?(response.body)
    assert Task.yield(consumer, 0) == nil
    assert HTTP.Config.streaming_timeout() == 60_000

    send(consumer.pid, {:release_sink, gate})
    assert {:ok, chunks, %{bytes: size}} = Task.await(consumer, 2_000)
    assert size == byte_size(body)
    assert IO.iodata_to_binary(Enum.reverse(chunks)) == body
    assert_receive {:peer_closed, :adapter, {:error, :closed}}, 1_000

    for {pid, monitor} <- monitors do
      assert_receive {:DOWN, ^monitor, :process, ^pid, :normal}, 1_000
      refute Process.alive?(pid)
    end

    assert HTTP.Config.streaming_timeout() == 60_000
  end

  defp peer(name, body) do
    WireServer.start(fn listener, owner ->
      {:ok, socket} = :gen_tcp.accept(listener, 3_000)

      try do
        request = WireServer.request(socket)
        assert request.method == "GET"

        chunks =
          for <<chunk::binary-size(32) <- body>>,
            do: [Integer.to_string(byte_size(chunk), 16), "\r\n", chunk, "\r\n"]

        remaining = binary_part(body, div(byte_size(body), 32) * 32, rem(byte_size(body), 32))

        tail =
          if remaining == "",
            do: [],
            else: [Integer.to_string(byte_size(remaining), 16), "\r\n", remaining, "\r\n"]

        :ok =
          :gen_tcp.send(socket, [
            "HTTP/1.1 200 OK\r\nContent-Encoding: gzip\r\nTransfer-Encoding: chunked\r\nConnection: close\r\n\r\n",
            chunks,
            tail,
            "0\r\n\r\n"
          ])

        send(owner, {:peer_closed, name, :gen_tcp.recv(socket, 0, 3_000)})
      after
        :gen_tcp.close(socket)
      end
    end)
  end
end
