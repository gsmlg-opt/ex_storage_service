defmodule ExStorageServiceCluster.Transport.HTTPTest do
  use ExUnit.Case, async: true

  alias ExStorageService.BlobStore.Source
  alias ExStorageService.Cluster.{BlobDescriptor, ReplicaAck}
  alias ExStorageService.{Context, InstanceConfig}
  alias ExStorageServiceCluster.{InternalAuth, Router}
  alias ExStorageServiceCluster.Transport.HTTP

  @secret "phase5-test-secret-at-least-32-bytes"

  defmodule ErrorBodyRouter do
    use Plug.Router

    import Plug.Conn

    plug(:match)
    plug(:dispatch)

    head "/internal/v1/blobs/:sha256" do
      conn
      |> put_resp_header("content-length", "8")
      |> put_resp_header("x-ess-blob-sha256", sha256)
      |> send_resp(200, "")
    end

    get "/internal/v1/blobs/:sha256" do
      send_resp(conn, 404, "do-not-forward")
    end
  end

  @tag :tmp_dir
  test "HTTP adapter streams PUT, HEAD, full GET, and Range GET", %{tmp_dir: tmp_dir} do
    %{url: url, context: context} = start_transport(tmp_dir)
    data = String.duplicate("streamed-through-http-", 2_048)
    hash = sha256(data)
    source_path = Path.join(tmp_dir, "source.bin")
    File.write!(source_path, data)

    descriptor = descriptor(hash, byte_size(data))

    assert {:ok,
            %ReplicaAck{
              node_id: "data-target",
              node_generation: 11,
              hash: ^hash,
              size: size,
              fencing_or_request_id: request_id
            }} =
             HTTP.put_blob(context, url, Source.file(source_path, 0, byte_size(data)), descriptor,
               secret: @secret
             )

    assert size == byte_size(data)
    assert is_binary(request_id)

    assert {:ok, %{hash: ^hash, size: ^size}} =
             HTTP.head_blob(context, url, hash, secret: @secret)

    assert {:ok, {:stream, full_stream, ^size}} =
             HTTP.open_blob(context, url, hash, nil, secret: @secret)

    assert {:ok, ^data} = collect(full_stream)

    assert {:ok, {:stream, range_stream, 9}} =
             HTTP.open_blob(context, url, hash, {7, 9}, secret: @secret)

    expected_range = binary_part(data, 7, 9)
    assert {:ok, ^expected_range} = collect(range_stream)
  end

  @tag :tmp_dir
  test "lazy upload enumeration is consumed incrementally", %{tmp_dir: tmp_dir} do
    %{url: url, context: context} = start_transport(tmp_dir)
    parent = self()
    chunk = String.duplicate("x", 64 * 1_024)
    chunks = 64

    stream =
      Stream.map(1..chunks, fn index ->
        send(parent, {:enumerated, index})
        chunk
      end)

    data_size = byte_size(chunk) * chunks

    hash =
      Enum.reduce(1..chunks, :crypto.hash_init(:sha256), fn _, digest ->
        :crypto.hash_update(digest, chunk)
      end)
      |> :crypto.hash_final()
      |> Base.encode16(case: :lower)

    refute_receive {:enumerated, _index}

    assert {:ok, %ReplicaAck{size: ^data_size}} =
             HTTP.put_blob(
               context,
               url,
               Source.stream(stream, data_size),
               descriptor(hash, data_size),
               secret: @secret
             )

    assert_receive {:enumerated, 1}
    assert_receive {:enumerated, ^chunks}
  end

  @tag :tmp_dir
  test "an interrupted raw PUT leaves neither staged nor ready content", %{tmp_dir: tmp_dir} do
    %{port: port, router_opts: router_opts} = start_transport(tmp_dir)
    expected = String.duplicate("z", 1_024)
    hash = sha256(expected)
    path = "/internal/v1/blobs/#{hash}"

    headers =
      InternalAuth.sign(:put, hash, byte_size(expected), @secret,
        path: path,
        request_id: "interrupted-request-phase5"
      )

    {:ok, socket} = :gen_tcp.connect(~c"127.0.0.1", port, [:binary, active: false])

    request = [
      "PUT #{path} HTTP/1.1\r\n",
      "host: 127.0.0.1\r\n",
      "content-length: #{byte_size(expected)}\r\n",
      Enum.map(headers, fn {name, value} -> "#{name}: #{value}\r\n" end),
      "connection: close\r\n\r\n",
      "partial"
    ]

    :ok = :gen_tcp.send(socket, request)
    :ok = :gen_tcp.close(socket)

    eventually(fn -> upload_files(router_opts) == [] end)

    assert {:error, :not_found} =
             ExStorageService.BlobStore.LocalCAS.stat(hash, router_opts[:blob_store_opts])
  end

  test "download error bodies are not forwarded to the caller sink" do
    server =
      start_supervised!(
        {Bandit,
         plug: ErrorBodyRouter,
         ip: {127, 0, 0, 1},
         port: 0,
         startup_log: false}
      )

    assert {:ok, {_address, port}} = ThousandIsland.listener_info(server)
    {:ok, config} = InstanceConfig.new(internal_secret: @secret)
    context = Context.new(config)
    hash = sha256("12345678")

    assert {:ok, {:stream, stream, 8}} =
             HTTP.open_blob(context, "http://127.0.0.1:#{port}", hash, nil, secret: @secret)

    parent = self()

    assert {:error, :not_found} =
             stream.(fn chunk ->
               send(parent, {:sink_chunk, chunk})
               :ok
             end)

    refute_receive {:sink_chunk, _chunk}
  end

  defp start_transport(tmp_dir) do
    table = :ets.new(:phase5_http_replay, [:set, :public])

    router_opts = [
      secret: @secret,
      replay_table: table,
      auth_skew_seconds: 60,
      node_id: "data-target",
      node_generation: 11,
      blob_store_opts: [
        root: Path.join(tmp_dir, "cas"),
        tmp_dir: Path.join(tmp_dir, "stage"),
        pack_module: nil
      ],
      max_blob_size: 32 * 1_024 * 1_024,
      read_timeout: 2_000
    ]

    server =
      start_supervised!(
        {Bandit, plug: {Router, router_opts}, ip: {127, 0, 0, 1}, port: 0, startup_log: false}
      )

    assert {:ok, {_address, port}} = ThousandIsland.listener_info(server)

    {:ok, config} = InstanceConfig.new(internal_secret: @secret)

    %{
      context: Context.new(config),
      port: port,
      router_opts: router_opts,
      url: "http://127.0.0.1:#{port}"
    }
  end

  defp descriptor(hash, size) do
    %BlobDescriptor{
      schema: 2,
      hash: hash,
      algorithm: :sha256,
      size: size,
      desired_replication_factor: 2,
      created_at: DateTime.utc_now()
    }
  end

  defp collect(stream_fun) do
    {:ok, io} = StringIO.open("")

    case stream_fun.(fn chunk -> IO.binwrite(io, chunk) end) do
      :ok ->
        {_input, output} = StringIO.contents(io)
        {:ok, output}

      {:error, _reason} = error ->
        error
    end
  end

  defp upload_files(router_opts) do
    router_opts[:blob_store_opts][:tmp_dir]
    |> File.ls()
    |> case do
      {:ok, files} -> files
      {:error, :enoent} -> []
    end
  end

  defp eventually(fun, attempts \\ 50)

  defp eventually(fun, attempts) do
    cond do
      fun.() ->
        :ok

      attempts == 0 ->
        flunk("condition did not become true")

      true ->
        Process.sleep(20)
        eventually(fun, attempts - 1)
    end
  end

  defp sha256(data), do: :sha256 |> :crypto.hash(data) |> Base.encode16(case: :lower)
end
