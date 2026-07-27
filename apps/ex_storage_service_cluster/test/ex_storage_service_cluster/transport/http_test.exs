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
      |> put_resp_header("x-ess-node-id", "error-router")
      |> put_resp_header("x-ess-node-generation", "1")
      |> put_resp_header("x-ess-blob-sha256", sha256)
      |> put_resp_header("x-ess-blob-size", "8")
      |> put_resp_header("x-ess-verified-at", "1")
      |> put_resp_header(
        "x-ess-request-id",
        conn |> Plug.Conn.get_req_header("x-ess-request-id") |> List.first()
      )
      |> send_resp(200, "")
    end

    get "/internal/v1/blobs/:sha256" do
      send_resp(conn, 404, "do-not-forward")
    end
  end

  defmodule SlowDownloadPlug do
    import Plug.Conn

    @chunk_size 262_144
    @chunk_count 64
    @content_length @chunk_size * @chunk_count

    def init(opts), do: opts

    def call(%Plug.Conn{method: "HEAD"} = conn, opts) do
      hash = opts[:hash]

      conn
      |> put_resp_header("content-length", Integer.to_string(@content_length))
      |> put_resp_header("x-ess-node-id", "slow-target")
      |> put_resp_header("x-ess-node-generation", "3")
      |> put_resp_header("x-ess-blob-sha256", hash)
      |> put_resp_header("x-ess-blob-size", Integer.to_string(@content_length))
      |> put_resp_header("x-ess-verified-at", "1")
      |> put_resp_header("x-ess-request-id", request_id(conn))
      |> send_resp(200, "")
    end

    def call(%Plug.Conn{method: "GET"} = conn, opts) do
      conn =
        conn
        |> put_resp_header("content-length", Integer.to_string(@content_length))
        |> send_chunked(200)

      stream_chunks(conn, opts[:owner], :binary.copy(<<0>>, @chunk_size), @chunk_count, 1)
    end

    defp request_id(conn) do
      conn
      |> get_req_header("x-ess-request-id")
      |> List.first()
    end

    defp stream_chunks(conn, owner, chunk_data, remaining, attempt) do
      case chunk(conn, chunk_data) do
        {:ok, next_conn} when remaining > 1 ->
          if attempt == 1, do: send(owner, {:slow_download, :first_chunk})
          Process.sleep(10)
          stream_chunks(next_conn, owner, chunk_data, remaining - 1, attempt + 1)

        {:ok, next_conn} ->
          send(owner, {:slow_download, :completed})
          next_conn

        {:error, reason} ->
          send(owner, {:slow_download, :closed, attempt, reason})
          conn
      end
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

    head_request_id = "head-request-phase6"

    assert {:ok,
            %{
              hash: ^hash,
              size: ^size,
              node_id: "data-target",
              node_generation: 11,
              verified_at: verified_at,
              fencing_or_request_id: ^head_request_id
            }} =
             HTTP.head_blob(
               context,
               %{internal_endpoint: url},
               hash,
               secret: @secret,
               request_id: head_request_id
             )

    assert is_integer(verified_at) and verified_at > 0

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
        {Bandit, plug: ErrorBodyRouter, ip: {127, 0, 0, 1}, port: 0, startup_log: false}
      )

    assert {:ok, {_address, port}} = ThousandIsland.listener_info(server)
    {:ok, config} = InstanceConfig.new(internal_secret: @secret)
    context = Context.new(config)
    hash = sha256("12345678")

    assert {:ok, {:stream, _producer, 8} = source} =
             HTTP.open_blob(context, "http://127.0.0.1:#{port}", hash, nil, secret: @secret)

    parent = self()

    assert {:error, :not_found, :consumer} =
             Source.reduce(source, :consumer, fn chunk, consumer ->
               send(parent, {:sink_chunk, chunk})
               {:cont, consumer}
             end)

    refute_receive {:sink_chunk, _chunk}
  end

  @tag :tmp_dir
  test "open rejects replica identity or size mismatches before returning a source", %{
    tmp_dir: tmp_dir
  } do
    %{url: url, context: context} = start_transport(tmp_dir)
    data = "identity-checked"
    hash = sha256(data)
    source_path = Path.join(tmp_dir, "identity-source.bin")
    File.write!(source_path, data)

    assert {:ok, %ReplicaAck{}} =
             HTTP.put_blob(
               context,
               url,
               Source.file(source_path, 0, byte_size(data)),
               descriptor(hash, byte_size(data)),
               secret: @secret
             )

    for expected <- [
          [expected_size: byte_size(data) + 1],
          [expected_node_id: "other-node"],
          [expected_node_generation: 12]
        ] do
      assert {:error, :replica_identity_mismatch} =
               HTTP.open_blob(context, url, hash, nil, [secret: @secret] ++ expected)
    end

    assert {:ok, {:stream, _producer, length}} =
             HTTP.open_blob(context, url, hash, nil,
               secret: @secret,
               expected_size: byte_size(data),
               expected_node_id: "data-target",
               expected_node_generation: 11
             )

    assert length == byte_size(data)
  end

  test "downstream halt preserves reducer state and closes the upstream HTTP/1 request" do
    hash = sha256("slow-download")
    content_length = 262_144 * 64

    server =
      start_supervised!(
        {Bandit,
         plug: {SlowDownloadPlug, owner: self(), hash: hash},
         ip: {127, 0, 0, 1},
         port: 0,
         startup_log: false}
      )

    assert {:ok, {_address, port}} = ThousandIsland.listener_info(server)
    {:ok, config} = InstanceConfig.new(internal_secret: @secret)
    context = Context.new(config)
    url = "http://127.0.0.1:#{port}"

    assert {:ok, {:stream, _producer, ^content_length} = source} =
             HTTP.open_blob(context, url, hash, nil,
               secret: @secret,
               expected_node_id: "slow-target",
               expected_node_generation: 3,
               expected_size: content_length
             )

    assert {:error, {:sink, :client_closed}, [received]} =
             Source.reduce(source, [], fn chunk, received ->
               {:halt, :client_closed, [chunk | received]}
             end)

    assert is_binary(received)
    assert byte_size(received) > 0
    assert byte_size(received) < content_length
    assert_receive {:slow_download, :first_chunk}
    assert_receive {:slow_download, :closed, attempt, _reason}, 2_000
    assert attempt < 64
    refute_receive {:slow_download, :completed}
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

  defp collect({:stateful, _producer} = stream) do
    {:ok, io} = StringIO.open("")
    source = {:stream, stream, 0}

    case Source.reduce(source, io, fn chunk, current ->
           :ok = IO.binwrite(current, chunk)
           {:cont, current}
         end) do
      {:ok, _io} ->
        {_input, output} = StringIO.contents(io)
        {:ok, output}

      {:error, reason, _io} ->
        {:error, reason}
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
