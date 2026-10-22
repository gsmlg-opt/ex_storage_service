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
        |> put_resp_header(
          "content-length",
          Integer.to_string(Keyword.get(opts, :advertised_length, @content_length))
        )
        |> send_chunked(200)

      stream_chunks(conn, opts[:owner], :binary.copy(<<0>>, @chunk_size), @chunk_count, 1)
    end

    defp request_id(conn) do
      conn
      |> get_req_header("x-ess-request-id")
      |> List.first()
    end

    defp stream_chunks(conn, owner, chunk_data, remaining, attempt) do
      # A downstream cancellation can interrupt the first bounded chunk send.
      if attempt == 1, do: send(owner, {:slow_download, :first_attempt})

      case chunk(conn, chunk_data) do
        {:ok, next_conn} when remaining > 1 ->
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

  test "replica acknowledgement does not buffer an arbitrary response document" do
    {:ok, listener} = :gen_tcp.listen(0, [:binary, active: false, ip: {127, 0, 0, 1}])
    {:ok, {_, port}} = :inet.sockname(listener)
    hash = sha256("x")

    server =
      spawn_link(fn ->
        {:ok, socket} = :gen_tcp.accept(listener, 5_000)

        read_head = fn recurse, bytes ->
          if String.contains?(bytes, "\r\n\r\n"),
            do: bytes,
            else:
              with(
                {:ok, chunk} <- :gen_tcp.recv(socket, 0, 5_000),
                do: recurse.(recurse, bytes <> chunk)
              )
        end

        request = read_head.(read_head, "")
        [_, request_id] = Regex.run(~r/x-ess-request-id: ([^\r]+)\r/i, request)

        headers = [
          {"content-length", "1073741824"},
          {"connection", "close"},
          {"x-ess-node-id", "peer"},
          {"x-ess-node-generation", "1"},
          {"x-ess-blob-sha256", hash},
          {"x-ess-blob-size", "1"},
          {"x-ess-verified-at", "1"},
          {"x-ess-request-id", request_id}
        ]

        :ok =
          :gen_tcp.send(socket, [
            "HTTP/1.1 200 OK\r\n",
            Enum.map(headers, fn {k, v} -> [k, ": ", v, "\r\n"] end),
            "\r\n"
          ])

        :gen_tcp.close(socket)
      end)

    on_exit(fn ->
      Process.exit(server, :kill)
      :gen_tcp.close(listener)
    end)

    {:ok, config} = InstanceConfig.new(internal_secret: @secret)

    assert {:ok, %ReplicaAck{node_id: "peer", hash: ^hash}} =
             HTTP.put_blob(
               Context.new(config),
               "http://127.0.0.1:#{port}",
               Source.stream(["x"], 1),
               descriptor(hash, 1),
               secret: @secret
             )
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

    path = "/internal/v1/blobs/#{hash}"
    headers = InternalAuth.sign(:head, hash, "-", @secret, path: path)
    response = wire_request(URI.parse(url).port, "HEAD", path, headers)
    assert response =~ "HTTP/1.1 200"
    [response_head, ""] = String.split(response, "\r\n\r\n", parts: 2)
    assert "content-length: #{size}" in String.split(String.downcase(response_head), "\r\n")

    assert {:ok, {:stream, full_stream, ^size}} =
             HTTP.open_blob(context, url, hash, nil, secret: @secret)

    assert {:ok, ^data} = collect(full_stream)

    assert {:ok, {:stream, range_stream, 9}} =
             HTTP.open_blob(context, url, hash, {7, 9}, secret: @secret)

    expected_range = binary_part(data, 7, 9)
    assert {:ok, ^expected_range} = collect(range_stream)
  end

  @tag :tmp_dir
  test "the private listener enforces authentication and one-use request IDs", %{tmp_dir: tmp_dir} do
    %{port: port} = start_transport(tmp_dir)
    hash = sha256("absent")
    path = "/internal/v1/blobs/#{hash}"

    assert wire_request(port, "HEAD", path, []) =~ "HTTP/1.1 401"

    headers =
      InternalAuth.sign(:head, hash, "-", @secret,
        path: path,
        request_id: "one-use-wire-request"
      )

    assert wire_request(port, "HEAD", path, headers) =~ "HTTP/1.1 404"
    assert wire_request(port, "HEAD", path, headers) =~ "HTTP/1.1 401"
  end

  @tag :tmp_dir
  test "a progressing upload outlives the socket inactivity window", %{tmp_dir: tmp_dir} do
    %{url: url, context: context} = start_transport(tmp_dir, read_timeout: 500)
    chunk = String.duplicate("paced", 256)
    count = 12
    data = String.duplicate(chunk, count)
    size = byte_size(data)
    hash = sha256(data)

    stream =
      Stream.map(1..count, fn _ ->
        Process.sleep(100)
        chunk
      end)

    started = System.monotonic_time(:millisecond)

    assert {:ok, %ReplicaAck{size: ^size}} =
             HTTP.put_blob(context, url, Source.stream(stream, size), descriptor(hash, size),
               secret: @secret
             )

    assert System.monotonic_time(:millisecond) - started >= 1_200

    assert {:ok, {:stream, full_stream, ^size}} =
             HTTP.open_blob(context, url, hash, nil, secret: @secret)

    assert {:ok, ^data} = collect(full_stream)
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
  test "fixed-length upload rejects short and overlong sources without publishing content", %{
    tmp_dir: tmp_dir
  } do
    %{url: url, context: context, router_opts: router_opts} = start_transport(tmp_dir)
    data = "expected"
    hash = sha256(data)
    size = byte_size(data)

    for payload <- ["short", "overlong-source"] do
      assert {:error, :content_length_mismatch} =
               HTTP.put_blob(context, url, Source.stream([payload], size), descriptor(hash, size),
                 secret: @secret
               )

      eventually(fn -> upload_files(router_opts) == [] end)

      assert {:error, :not_found} =
               ExStorageService.BlobStore.LocalCAS.stat(hash, router_opts[:blob_store_opts])
    end
  end

  @tag :tmp_dir
  test "upload propagates a stateful source failure and removes staged content", %{
    tmp_dir: tmp_dir
  } do
    %{url: url, context: context, router_opts: router_opts} = start_transport(tmp_dir)
    hash = sha256("expected")

    source =
      Source.stateful_stream(
        fn initial, reducer ->
          {:cont, next} = reducer.("part", initial)
          {:error, :source_failed, next}
        end,
        8
      )

    assert {:error, %Source.RequestBodyError{reason: :source_failed}} =
             HTTP.put_blob(context, url, source, descriptor(hash, 8), secret: @secret)

    eventually(fn -> upload_files(router_opts) == [] end)

    assert {:error, :not_found} =
             ExStorageService.BlobStore.LocalCAS.stat(hash, router_opts[:blob_store_opts])
  end

  @tag :tmp_dir
  test "stateful remote source streams into PUT and authenticated DELETE is idempotent", %{
    tmp_dir: tmp_dir
  } do
    %{url: url, context: context, router_opts: router_opts} = start_transport(tmp_dir)
    chunks = Enum.map(1..32, &String.duplicate(Integer.to_string(&1), 4_096))
    data = IO.iodata_to_binary(chunks)
    hash = sha256(data)

    source =
      Source.stateful_stream(
        fn initial, reducer ->
          Enum.reduce_while(chunks, {:ok, initial}, fn chunk, {:ok, state} ->
            case reducer.(chunk, state) do
              {:cont, next} -> {:cont, {:ok, next}}
              {:halt, reason, next} -> {:halt, {:error, reason, next}}
            end
          end)
        end,
        byte_size(data)
      )

    assert {:ok, %ReplicaAck{hash: ^hash, size: size}} =
             HTTP.put_blob(context, url, source, descriptor(hash, byte_size(data)),
               secret: @secret
             )

    assert size == byte_size(data)

    assert {:ok, %{size: ^size}} =
             ExStorageService.BlobStore.LocalCAS.stat(hash, router_opts[:blob_store_opts])

    node = %{internal_endpoint: url, node_id: "data-target", generation: 11}

    assert :ok = HTTP.health(context, node, secret: @secret)

    assert {:error, :invalid_health_response} =
             HTTP.health(context, %{node | generation: 12}, secret: @secret)

    cleanup_opts = [
      secret: @secret,
      cleanup_job_id: "cleanup-job",
      cleanup_target_generation: 11,
      cleanup_owner_node: "data-owner",
      cleanup_owner_generation: 7,
      cleanup_fencing_token: 3
    ]

    assert :ok = HTTP.delete_blob(context, node, hash, cleanup_opts)
    assert :ok = HTTP.delete_blob(context, node, hash, cleanup_opts)

    assert {:error, :not_found} =
             ExStorageService.BlobStore.LocalCAS.stat(hash, router_opts[:blob_store_opts])
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
    port = start_http(ErrorBodyRouter)
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

    port = start_http({SlowDownloadPlug, owner: self(), hash: hash})
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
    assert_receive {:slow_download, :first_attempt}
    assert_receive {:slow_download, :closed, attempt, _reason}, 2_000
    assert attempt < 64
    refute_receive {:slow_download, :completed}
  end

  test "invalid download length rejects bytes before the sink and closes the request" do
    hash = sha256("invalid-download-length")
    content_length = 262_144 * 64

    port =
      start_http(
        {SlowDownloadPlug, owner: self(), hash: hash, advertised_length: content_length - 1}
      )

    {:ok, config} = InstanceConfig.new(internal_secret: @secret)
    context = Context.new(config)

    assert {:ok, source} =
             HTTP.open_blob(context, "http://127.0.0.1:#{port}", hash, nil, secret: @secret)

    owner = self()

    assert {:error, :invalid_content_length, :initial} =
             Source.reduce(source, :initial, fn chunk, state ->
               send(owner, {:unexpected_chunk, chunk})
               {:cont, state}
             end)

    refute_receive {:unexpected_chunk, _chunk}
    assert_receive {:slow_download, :closed, attempt, _reason}, 2_000
    assert attempt < 64
    refute_receive {:slow_download, :completed}
  end

  defp start_transport(tmp_dir, overrides \\ []) do
    table = :ets.new(:phase5_http_replay, [:set, :public])

    router_opts =
      [
        secret: @secret,
        replay_table: table,
        auth_skew_seconds: 60,
        node_id: "data-target",
        node_generation: 11,
        cleanup_authorizer: fn
          _hash,
          _node_id,
          _target_generation,
          _job_id,
          _owner_node,
          _owner_generation,
          _fencing_token,
          _opts ->
            :ok
        end,
        blob_store_opts: [
          root: Path.join(tmp_dir, "cas"),
          tmp_dir: Path.join(tmp_dir, "stage"),
          pack_module: nil
        ],
        max_blob_size: 32 * 1_024 * 1_024,
        read_timeout: 2_000
      ]
      |> Keyword.merge(overrides)

    port =
      start_http({Router, router_opts}, %{
        max_body_bytes: router_opts[:max_blob_size],
        read_timeout: router_opts[:read_timeout]
      })

    {:ok, config} = InstanceConfig.new(internal_secret: @secret)

    %{
      context: Context.new(config),
      port: port,
      router_opts: router_opts,
      url: "http://127.0.0.1:#{port}"
    }
  end

  defp start_http(plug, http_limits \\ %{}) do
    plug = if is_atom(plug), do: {plug, []}, else: plug
    id = :cluster_transport_test

    server =
      start_supervised!(
        {Gaoxy.HTTP,
         id: id,
         plug: plug,
         ip: {127, 0, 0, 1},
         port: 0,
         protocols: [:http1],
         limits: %{max_bytes: 64 * 1_024 * 1_024},
         http_limits:
           Map.merge(
             %{request_timeout: :infinity, max_read_chunk: 65_536, write_timeout: 60_000},
             http_limits
           )}
      )

    assert %{local: {:ok, {_address, port}}} = Gaoxy.info(server, id)
    port
  end

  defp wire_request(port, method, path, headers) do
    {:ok, socket} = :gen_tcp.connect(~c"127.0.0.1", port, [:binary, active: false])

    try do
      :ok =
        :gen_tcp.send(socket, [
          "#{method} #{path} HTTP/1.1\r\n",
          "host: 127.0.0.1\r\nconnection: close\r\n",
          Enum.map(headers, fn {name, value} -> [name, ": ", value, "\r\n"] end),
          "\r\n"
        ])

      receive_response(socket, "")
    after
      :gen_tcp.close(socket)
    end
  end

  defp receive_response(socket, response) do
    case :gen_tcp.recv(socket, 0, 5_000) do
      {:ok, chunk} -> receive_response(socket, response <> chunk)
      {:error, :closed} -> response
    end
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
