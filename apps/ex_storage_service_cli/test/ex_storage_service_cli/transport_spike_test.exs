Code.require_file("../support/wire_server.exs", __DIR__)

defmodule ExStorageServiceCli.TransportSpikeTest do
  use ExUnit.Case, async: true

  alias ExStorageServiceCli.Test.WireServer

  @moduletag :tmp_dir

  test "file ranges upload incrementally with exact Content-Length and encoded bytes", %{
    tmp_dir: directory
  } do
    encoded = :zlib.gzip(:binary.copy("raw object", 20_000))
    path = Path.join(directory, "source")
    File.write!(path, "prefix" <> encoded <> "suffix")
    owner = self()

    range =
      Stream.resource(
        fn ->
          {:ok, file} = File.open(path, [:read, :binary])
          {:ok, 6} = :file.position(file, 6)
          {file, byte_size(encoded)}
        end,
        fn
          {file, 0} ->
            {:halt, {file, 0}}

          {file, remaining} ->
            chunk = IO.binread(file, min(64, remaining))
            send(owner, {:source_chunk, byte_size(chunk)})
            {[chunk], {file, remaining - byte_size(chunk)}}
        end,
        fn {file, _remaining} ->
          File.close(file)
          send(owner, :source_closed)
        end
      )

    {:ok, body} = HTTP.Stream.from_enumerable(range, telemetry: false)
    body_monitor = Process.monitor(body)
    {endpoint, _server} = WireServer.fixture([{200, [], ""}])

    assert %HTTP.Response{status: 200} =
             endpoint
             |> HTTP.fetch(
               method: :put,
               body: body,
               duplex: :half,
               headers: [
                 {"Content-Length", to_string(byte_size(encoded))},
                 {"Content-Encoding", "gzip"}
               ],
               decode_body: false,
               redirect: :manual,
               timeout: 5_000
             )
             |> HTTP.Promise.await()

    assert_receive {:wire_request, %{headers: headers, body: ^encoded}}
    assert headers["content-length"] == to_string(byte_size(encoded))
    assert headers["content-encoding"] == "gzip"
    refute Map.has_key?(headers, "transfer-encoding")
    assert_receive {:source_chunk, 64}
    assert_receive :source_closed
    assert_receive {:DOWN, ^body_monitor, :process, ^body, :normal}
  end

  test "forced download streams preserve raw Content-Encoding and release resources", %{
    tmp_dir: directory
  } do
    encoded = :zlib.gzip(:binary.copy("raw object", 20_000))
    {endpoint, _server} = WireServer.fixture([{200, [{"Content-Encoding", "gzip"}], encoded}])
    assert %HTTP.Response{body: stream} = fetch(endpoint, stream_response: true)
    assert is_pid(stream)
    monitor = Process.monitor(stream)
    output = Path.join(directory, "download.gz")
    assert :ok = HTTP.Response.write_to(%HTTP.Response{body: stream}, output)
    assert File.read!(output) == encoded
    assert_receive {:DOWN, ^monitor, :process, ^stream, :normal}
  end

  test "acknowledged response stalls later chunks until consumer acknowledges" do
    {endpoint, server} =
      WireServer.start(fn listener, owner ->
        {:ok, socket} = :gen_tcp.accept(listener, 5_000)
        _request = WireServer.request(socket)

        :ok =
          :gen_tcp.send(
            socket,
            "HTTP/1.1 200 OK\r\nTransfer-Encoding: chunked\r\nConnection: close\r\n\r\n3\r\none\r\n"
          )

        send(owner, :first_sent)

        receive do
          :next -> :ok
        after
          5_000 -> raise "consumer never advanced"
        end

        :ok = :gen_tcp.send(socket, "3\r\ntwo\r\n0\r\n\r\n")
        :gen_tcp.close(socket)
      end)

    assert %HTTP.Response{body: stream} = fetch(endpoint, stream_response: true)
    send(stream, {:read_chunk, self(), :ack})
    assert_receive :first_sent
    assert_receive {:stream_chunk, ^stream, "one", reference}
    send(server, :next)
    refute_receive {:stream_chunk, ^stream, _chunk, _reference}, 100
    send(stream, {:stream_chunk_ack, reference})
    assert_receive {:stream_chunk, ^stream, "two", reference}
    send(stream, {:stream_chunk_ack, reference})
    assert_receive {:stream_end, ^stream}
  end

  test "upload source production blocks while peer does not read a large body" do
    owner = self()
    chunk = :binary.copy("x", 65_536)

    source =
      Stream.map(1..16_384, fn number ->
        send(owner, {:produced, number})
        chunk
      end)

    {:ok, body} = HTTP.Stream.from_enumerable(source, telemetry: false)
    controller = HTTP.AbortController.new()

    {endpoint, _server} =
      WireServer.start(fn listener, owner ->
        {:ok, socket} = :gen_tcp.accept(listener, 5_000)
        {:ok, head} = :gen_tcp.recv(socket, 0, 5_000)
        send(owner, {:accepted_upload, head})

        receive do
          :stop -> :ok
        after
          5_000 -> :ok
        end

        :gen_tcp.close(socket)
      end)

    promise =
      HTTP.fetch(endpoint,
        method: :put,
        body: body,
        duplex: :half,
        headers: [{"Content-Length", "1073741824"}],
        signal: controller,
        redirect: :manual,
        decode_body: false,
        timeout: 3_000
      )

    assert_receive {:accepted_upload, _head}, 2_000
    Process.sleep(100)
    produced = drain_produced(0)
    assert produced < 256, "producer buffered #{produced} chunks despite stalled peer"
    assert produced > 0
    assert :ok = HTTP.AbortController.abort(controller)
    assert {:error, :aborted} = HTTP.Promise.await(promise)
    Agent.stop(controller)
  end

  test "abort after response headers closes socket and stream" do
    {endpoint, _server} = stalled_response()
    controller = HTTP.AbortController.new()

    assert %HTTP.Response{body: stream} =
             fetch(endpoint, signal: controller, stream_response: true)

    monitor = Process.monitor(stream)
    send(stream, {:read_chunk, self(), :ack})
    assert_receive {:stream_chunk, ^stream, "one", reference}
    send(stream, {:stream_chunk_ack, reference})
    assert :ok = HTTP.AbortController.abort(controller)
    assert_receive {:stream_error, ^stream, :aborted}, 2_000
    assert_receive {:peer_closed, {:error, :closed}}, 2_000
    assert_receive {:DOWN, ^monitor, :process, ^stream, :normal}, 2_000
    Agent.stop(controller)
  end

  test "request deadline continues through streaming body and closes socket" do
    {endpoint, _server} = stalled_response()
    assert %HTTP.Response{body: stream} = fetch(endpoint, timeout: 300, stream_response: true)
    monitor = Process.monitor(stream)
    send(stream, {:read_chunk, self(), :ack})
    assert_receive {:stream_chunk, ^stream, "one", reference}
    send(stream, {:stream_chunk_ack, reference})
    assert_receive {:stream_error, ^stream, :request_timeout}, 2_000
    assert_receive {:peer_closed, {:error, :closed}}, 2_000
    assert_receive {:DOWN, ^monitor, :process, ^stream, :normal}, 2_000
  end

  test "HTTP/1 response trailers follow body acknowledgement before end" do
    {endpoint, _server} =
      WireServer.start(fn listener, _owner ->
        {:ok, socket} = :gen_tcp.accept(listener, 5_000)
        WireServer.request(socket)

        :ok =
          :gen_tcp.send(
            socket,
            "HTTP/1.1 200 OK\r\nTransfer-Encoding: chunked\r\nTrailer: x-amz-checksum-crc32\r\nConnection: close\r\n\r\n3\r\nabc\r\n0\r\nx-amz-checksum-crc32: NSRBwg==\r\n\r\n"
          )

        :gen_tcp.close(socket)
      end)

    assert %HTTP.Response{body: stream} = fetch(endpoint, stream_response: true)
    send(stream, {:read_chunk, self(), :ack})
    assert_receive {:stream_chunk, ^stream, "abc", reference}
    refute_receive {:stream_trailers, ^stream, _fields}, 50
    send(stream, {:stream_chunk_ack, reference})
    assert_receive {:stream_trailers, ^stream, fields}
    assert HTTP.Headers.get(fields, "x-amz-checksum-crc32") == "NSRBwg=="
    assert_receive {:stream_end, ^stream}
  end

  test "HTTP/1 upload trailers reach wire without Content-Length" do
    {endpoint, _server} =
      WireServer.start(fn listener, owner ->
        {:ok, socket} = :gen_tcp.accept(listener, 5_000)
        {head, data} = WireServer.head(socket)
        data = until_contains(socket, data, "x-amz-checksum-crc32: NSRBwg==\r\n\r\n")
        send(owner, {:trailer_request, head, data})
        WireServer.response(socket, 200, [], "")
        :gen_tcp.close(socket)
      end)

    {:ok, body} = HTTP.Stream.start_link(0, [], telemetry: false)

    task =
      Task.async(fn ->
        assert :ok = HTTP.Stream.chunk(body, "abc")
        assert :ok = HTTP.Stream.finish(body, [{"x-amz-checksum-crc32", "NSRBwg=="}])
      end)

    assert %HTTP.Response{status: 200} =
             fetch(endpoint,
               method: :put,
               body: body,
               duplex: :half,
               headers: [{"Trailer", "x-amz-checksum-crc32"}]
             )

    Task.await(task)
    assert_receive {:trailer_request, head, wire}
    assert String.downcase(head) =~ "transfer-encoding: chunked"
    refute String.downcase(head) =~ "content-length:"
    assert wire == "3\r\nabc\r\n0\r\nx-amz-checksum-crc32: NSRBwg==\r\n\r\n"
  end

  defp fetch(endpoint, opts) do
    endpoint
    |> HTTP.fetch(Keyword.merge([decode_body: false, redirect: :manual, timeout: 5_000], opts))
    |> HTTP.Promise.await()
  end

  defp stalled_response do
    WireServer.start(fn listener, owner ->
      {:ok, socket} = :gen_tcp.accept(listener, 5_000)
      WireServer.request(socket)

      :ok =
        :gen_tcp.send(
          socket,
          "HTTP/1.1 200 OK\r\nTransfer-Encoding: chunked\r\nConnection: close\r\n\r\n3\r\none\r\n"
        )

      send(owner, {:peer_closed, :gen_tcp.recv(socket, 0, 5_000)})
      :gen_tcp.close(socket)
    end)
  end

  defp until_contains(socket, data, terminal) do
    if String.ends_with?(data, terminal) do
      data
    else
      {:ok, chunk} = :gen_tcp.recv(socket, 0, 5_000)
      until_contains(socket, data <> chunk, terminal)
    end
  end

  defp drain_produced(count) do
    receive do
      {:produced, _number} -> drain_produced(count + 1)
    after
      0 -> count
    end
  end
end
