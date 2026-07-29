defmodule ExStorageServiceCluster.BlobHandlerTest do
  use ExUnit.Case, async: true

  import Plug.Conn
  import Plug.Test

  alias ExStorageService.BlobStore.LocalCAS
  alias ExStorageServiceCluster.{BlobHandler, InternalAuth, Router}

  @secret "phase5-test-secret-at-least-32-bytes"
  @moduletag :tmp_dir

  defmodule SliceStore do
    def stat(_hash, opts), do: {:ok, %{size: opts[:logical_size]}}
    def verify(_hash, _opts), do: :ok

    def open(_hash, nil, opts),
      do: {:ok, {:file, opts[:path], opts[:base_offset], opts[:logical_size]}}

    def open(_hash, {offset, length}, opts),
      do: {:ok, {:file, opts[:path], opts[:base_offset] + offset, length}}
  end

  defmodule FailingStore do
    def verify(_hash, _opts), do: {:error, :eio}
    def stat(_hash, _opts), do: {:error, :eio}
  end

  setup %{tmp_dir: tmp_dir} do
    table = :ets.new(:phase5_handler_replay, [:set, :public])

    opts = [
      secret: @secret,
      replay_table: table,
      auth_skew_seconds: 60,
      node_id: "data-a",
      node_generation: 7,
      blob_store_opts: [
        root: Path.join(tmp_dir, "cas"),
        tmp_dir: Path.join(tmp_dir, "stage"),
        pack_module: nil
      ],
      max_blob_size: 4 * 1_024 * 1_024,
      read_timeout: 1_000
    ]

    %{opts: opts}
  end

  @tag :tmp_dir
  test "PUT is durable and duplicate content is idempotent", %{opts: opts} do
    data = "streamed-internal-blob"
    hash = sha256(data)

    first = request(:put, hash, data, byte_size(data), opts)
    assert first.status == 200
    assert get_resp_header(first, "x-ess-node-id") == ["data-a"]
    assert get_resp_header(first, "x-ess-node-generation") == ["7"]

    second = request(:put, hash, data, byte_size(data), opts)
    assert second.status == 200
    assert {:ok, %{size: 22}} = LocalCAS.stat(hash, opts[:blob_store_opts])
  end

  @tag :tmp_dir
  test "wrong hash and declared size never publish a ready blob", %{opts: opts} do
    data = "wrong-content"
    wrong_hash = sha256("different")

    assert request(:put, wrong_hash, data, byte_size(data), opts).status == 422
    assert {:error, :not_found} = LocalCAS.stat(wrong_hash, opts[:blob_store_opts])
    assert upload_files(opts) == []

    hash = sha256(data <> "-expected-longer")
    conn = signed_conn(:put, hash, data, byte_size(data) + 16, opts)
    conn = put_req_header(conn, "content-length", Integer.to_string(byte_size(data) + 16))

    assert Router.call(conn, opts).status == 422
    assert {:error, :not_found} = LocalCAS.stat(hash, opts[:blob_store_opts])
    assert upload_files(opts) == []
  end

  @tag :tmp_dir
  test "HEAD and GET return exact lengths and one byte range", %{opts: opts} do
    data = "0123456789"
    hash = sha256(data)
    assert request(:put, hash, data, byte_size(data), opts).status == 200

    head_request_id = request_id()
    head = request(:head, hash, "", "-", opts, request_id: head_request_id)
    assert head.status == 200
    assert get_resp_header(head, "content-length") == ["10"]
    assert get_resp_header(head, "x-ess-node-id") == ["data-a"]
    assert get_resp_header(head, "x-ess-node-generation") == ["7"]
    assert get_resp_header(head, "x-ess-blob-sha256") == [hash]
    assert get_resp_header(head, "x-ess-blob-size") == ["10"]
    assert get_resp_header(head, "x-ess-request-id") == [head_request_id]
    assert [verified_at] = get_resp_header(head, "x-ess-verified-at")
    assert {timestamp, ""} = Integer.parse(verified_at)
    assert timestamp > 0

    get = request(:get, hash, "", "-", opts)
    assert get.status == 200
    assert get.resp_body == data
    assert get_resp_header(get, "content-length") == ["10"]

    range = request(:get, hash, "", "-", opts, range: "bytes=2-5")
    assert range.status == 206
    assert range.resp_body == "2345"
    assert get_resp_header(range, "content-length") == ["4"]
    assert get_resp_header(range, "content-range") == ["bytes 2-5/10"]

    invalid_range = request(:get, hash, "", "-", opts, range: "bytes=10-11")
    assert invalid_range.status == 416
    assert get_resp_header(invalid_range, "content-range") == ["bytes */10"]
  end

  @tag :tmp_dir
  test "HEAD verifies the checksum before acknowledging a ready replica", %{opts: opts} do
    data = "same-length-content"
    hash = sha256(data)
    assert request(:put, hash, data, byte_size(data), opts).status == 200

    ready_path = LocalCAS.blob_path(hash, opts[:blob_store_opts])
    File.write!(ready_path, String.duplicate("x", byte_size(data)))

    head = request(:head, hash, "", "-", opts)
    assert head.status == 422
    assert get_resp_header(head, "x-ess-node-id") == []
    assert get_resp_header(head, "x-ess-verified-at") == []
  end

  @tag :tmp_dir
  test "unauthenticated and replayed requests are rejected", %{opts: opts} do
    data = "authenticated"
    hash = sha256(data)
    path = path(hash)

    unauthenticated =
      :put
      |> conn(path, data)
      |> put_req_header("content-length", Integer.to_string(byte_size(data)))

    assert unauthenticated |> Router.call(opts) |> Map.fetch!(:status) == 401

    request_id = request_id()
    first = signed_conn(:put, hash, data, byte_size(data), opts, request_id: request_id)
    second = signed_conn(:put, hash, data, byte_size(data), opts, request_id: request_id)

    assert Router.call(first, opts).status == 200
    assert Router.call(second, opts).status == 401
  end

  @tag :tmp_dir
  test "packed-style file slices stream without exposing adjacent bytes", %{
    opts: opts,
    tmp_dir: tmp_dir
  } do
    path = Path.join(tmp_dir, "pack.bin")
    File.write!(path, "prefix" <> "PACKED" <> "suffix")
    hash = sha256("PACKED")

    slice_opts =
      opts
      |> Keyword.put(:blob_store, SliceStore)
      |> Keyword.put(:blob_store_opts,
        path: path,
        base_offset: byte_size("prefix"),
        logical_size: byte_size("PACKED")
      )

    full = request(:get, hash, "", "-", slice_opts)
    assert full.status == 200
    assert full.resp_body == "PACKED"

    range = request(:get, hash, "", "-", slice_opts, range: "bytes=1-3")
    assert range.status == 206
    assert range.resp_body == "ACK"
  end

  test "authenticated storage failures return 500 instead of 401", %{opts: opts} do
    hash = sha256("unavailable")
    failing_opts = Keyword.put(opts, :blob_store, FailingStore)

    assert request(:head, hash, "", "-", failing_opts).status == 500
    assert request(:get, hash, "", "-", failing_opts).status == 500
  end

  @tag :tmp_dir
  test "DELETE requires and revalidates the durable cleanup fence", %{opts: opts} do
    data = "cleanup-fenced"
    hash = sha256(data)
    assert request(:put, hash, data, byte_size(data), opts).status == 200

    assert :delete
           |> signed_conn(hash, "", "-", opts)
           |> Router.call(opts)
           |> Map.fetch!(:status) == 400

    stale_opts =
      Keyword.put(opts, :cleanup_authorizer, fn
        ^hash, "data-a", 7, "cleanup-job", "worker-a", 2, 9, _metadata_opts ->
          {:error, :stale_cleanup_fence}
      end)

    assert cleanup_request(hash, stale_opts).status == 409
    assert {:ok, %{size: size}} = LocalCAS.stat(hash, opts[:blob_store_opts])
    assert size == byte_size(data)

    wrong_generation_opts =
      Keyword.put(opts, :cleanup_authorizer, fn
        _hash,
        _node_id,
        _target_generation,
        _job_id,
        _owner_node,
        _owner_generation,
        _fencing_token,
        _metadata_opts ->
          flunk("wrong target generation must fail before metadata authorization")
      end)

    assert cleanup_request(hash, wrong_generation_opts, 6).status == 409
    assert {:ok, %{size: ^size}} = LocalCAS.stat(hash, opts[:blob_store_opts])

    valid_opts =
      Keyword.put(opts, :cleanup_authorizer, fn
        ^hash, "data-a", 7, "cleanup-job", "worker-a", 2, 9, _metadata_opts -> :ok
      end)

    assert cleanup_request(hash, valid_opts).status == 204
    assert {:error, :not_found} = LocalCAS.stat(hash, opts[:blob_store_opts])
  end

  @tag :tmp_dir
  test "checksum failure emits bounded telemetry without filesystem paths", %{opts: opts} do
    handler_id = "phase5-checksum-#{System.unique_integer([:positive])}"
    parent = self()

    :ok =
      :telemetry.attach(
        handler_id,
        [:ex_storage_service, :cluster, :blob_transport, :checksum_failure],
        fn event, measurements, metadata, _config ->
          send(parent, {event, measurements, metadata})
        end,
        nil
      )

    on_exit(fn -> :telemetry.detach(handler_id) end)

    data = "bad-checksum"
    hash = sha256("expected")
    assert request(:put, hash, data, byte_size(data), opts).status == 422

    assert_receive {[:ex_storage_service, :cluster, :blob_transport, :checksum_failure],
                    %{bytes: 12, count: 1}, %{hash: ^hash, peer: "127.0.0.1"}}
  end

  test "successful upload telemetry records received bytes, duration, and peer", %{opts: opts} do
    handler_id = "phase5-upload-#{System.unique_integer([:positive])}"
    parent = self()

    :ok =
      :telemetry.attach(
        handler_id,
        [:ex_storage_service, :cluster, :blob_transport, :stop],
        fn event, measurements, metadata, _config ->
          send(parent, {event, measurements, metadata})
        end,
        nil
      )

    on_exit(fn -> :telemetry.detach(handler_id) end)

    data = "telemetry-upload"
    hash = sha256(data)
    assert request(:put, hash, data, byte_size(data), opts).status == 200

    assert_receive {[:ex_storage_service, :cluster, :blob_transport, :stop],
                    %{bytes: bytes, duration: duration},
                    %{direction: :server, operation: :put_blob, peer: "127.0.0.1", hash: ^hash}}

    assert bytes == byte_size(data)
    assert duration > 0
  end

  test "range parser supports closed, open, and suffix forms" do
    assert {:ok, %{offset: 2, length: 3}} = BlobHandler.parse_range("bytes=2-4", 10)
    assert {:ok, %{offset: 7, length: 3}} = BlobHandler.parse_range("bytes=7-", 10)
    assert {:ok, %{offset: 6, length: 4}} = BlobHandler.parse_range("bytes=-4", 10)
    assert {:error, :invalid_range} = BlobHandler.parse_range("bytes=10-11", 10)
    assert {:error, :invalid_range} = BlobHandler.parse_range("bytes=1-2,4-5", 10)
  end

  defp request(method, hash, body, size, opts, auth_opts \\ []) do
    method
    |> signed_conn(hash, body, size, opts, auth_opts)
    |> Router.call(opts)
  end

  defp cleanup_request(hash, opts, target_generation \\ 7) do
    :delete
    |> signed_conn(hash, "", "-", opts)
    |> put_req_header("x-ess-cleanup-job-id", "cleanup-job")
    |> put_req_header("x-ess-cleanup-target-generation", Integer.to_string(target_generation))
    |> put_req_header("x-ess-cleanup-owner-node", "worker-a")
    |> put_req_header("x-ess-cleanup-owner-generation", "2")
    |> put_req_header("x-ess-cleanup-fencing-token", "9")
    |> Router.call(opts)
  end

  defp signed_conn(method, hash, body, size, _opts, auth_opts \\ []) do
    path = path(hash)
    range = Keyword.get(auth_opts, :range)

    headers =
      InternalAuth.sign(method, hash, size, @secret,
        path: path,
        timestamp: System.system_time(:second),
        request_id: Keyword.get_lazy(auth_opts, :request_id, &request_id/0),
        range: range
      )

    conn =
      method
      |> conn(path, body)
      |> put_headers(headers)

    if method == :put and is_integer(size) do
      put_req_header(conn, "content-length", Integer.to_string(size))
    else
      conn
    end
  end

  defp put_headers(conn, headers) do
    Enum.reduce(headers, conn, fn {name, value}, conn -> put_req_header(conn, name, value) end)
  end

  defp upload_files(opts) do
    opts[:blob_store_opts][:tmp_dir]
    |> File.ls()
    |> case do
      {:ok, files} -> files
      {:error, :enoent} -> []
    end
  end

  defp sha256(data), do: :sha256 |> :crypto.hash(data) |> Base.encode16(case: :lower)
  defp path(hash), do: "/internal/v1/blobs/#{hash}"

  defp request_id do
    "request-#{System.unique_integer([:positive, :monotonic])}-phase5"
  end
end
