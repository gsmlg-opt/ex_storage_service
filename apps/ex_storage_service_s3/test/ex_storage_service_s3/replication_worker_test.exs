defmodule ExStorageServiceS3.ReplicationWorkerTest do
  use ExUnit.Case, async: false

  import ExUnit.CaptureLog

  alias ExStorageService.BlobStore.Source
  alias ExStorageService.BlobStore.Source.RequestBodyError
  alias ExStorageService.CrossClusterReplication.Worker, as: CrossClusterWorker
  alias ExStorageService.Replication.Config.Replica
  alias ExStorageService.Replication.Worker

  @s3_port Application.compile_env(:ex_storage_service, :s3_port, 9001)
  @base_url "http://localhost:#{@s3_port}"

  # The skip/stale decisions are reported at :info, which config/test.exs
  # filters (level: :warning). Raise the level so capture_log sees them.
  setup do
    previous_level = Logger.level()
    Logger.configure(level: :info)
    on_exit(fn -> Logger.configure(level: previous_level) end)
    :ok
  end

  defp unique_bucket, do: "repl-#{:erlang.unique_integer([:positive])}"

  defp create_bucket(bucket) do
    {:ok, %{status: 201}} = Req.put("#{@base_url}/#{bucket}", body: "")
    bucket
  end

  defp replica_for(dest_bucket) do
    %Replica{endpoint: @base_url, access_key: nil, secret_key_enc: nil, bucket: dest_bucket}
  end

  defp object_info(bucket, key) do
    {:ok, meta} = ExStorageService.Metadata.get_object_meta(bucket, key)

    %{
      version_id: Map.get(meta, :version_id),
      content_hash: meta.content_hash,
      etag: meta.etag,
      size: meta.size,
      content_type: Map.get(meta, :content_type, "application/octet-stream")
    }
  end

  test "replicates the pinned version to the destination bucket" do
    src = create_bucket(unique_bucket())
    dst = create_bucket(unique_bucket())
    data = "replicate-me-#{System.unique_integer()}"

    {:ok, %{status: 200}} = Req.put("#{@base_url}/#{src}/a.txt", body: data)

    assert :ok = Worker.replicate_put(src, "a.txt", replica_for(dst), object_info(src, "a.txt"))

    {:ok, %{status: 200, body: body}} = Req.get("#{@base_url}/#{dst}/a.txt")
    assert body == data
  end

  test "skips transfer when the destination already has identical content" do
    src = create_bucket(unique_bucket())
    dst = create_bucket(unique_bucket())
    data = "skip-me-#{System.unique_integer()}"

    {:ok, %{status: 200}} = Req.put("#{@base_url}/#{src}/b.txt", body: data)
    info = object_info(src, "b.txt")

    assert :ok = Worker.replicate_put(src, "b.txt", replica_for(dst), info)

    log =
      capture_log(fn ->
        assert :ok = Worker.replicate_put(src, "b.txt", replica_for(dst), info)
      end)

    assert log =~ "already present"
  end

  test "skips as stale when the pinned version was superseded and its content is gone" do
    src = create_bucket(unique_bucket())
    dst = create_bucket(unique_bucket())

    {:ok, %{status: 200}} = Req.put("#{@base_url}/#{src}/c.txt", body: "current-content")

    stale_info = %{
      version_id: "ancient",
      content_hash:
        Base.encode16(:crypto.hash(:sha256, "collected-#{System.unique_integer()}"),
          case: :lower
        ),
      etag: "deadbeef",
      size: 9,
      content_type: "text/plain"
    }

    log =
      capture_log(fn ->
        assert :ok = Worker.replicate_put(src, "c.txt", replica_for(dst), stale_info)
      end)

    assert log =~ "stale"
    # nothing was written to the destination
    {:ok, %{status: 404}} = Req.get("#{@base_url}/#{dst}/c.txt")
  end

  test "errors when pinned content is missing and the key still points at it" do
    src = create_bucket(unique_bucket())
    dst = create_bucket(unique_bucket())

    {:ok, %{status: 200}} = Req.put("#{@base_url}/#{src}/d.txt", body: "will-lose-content")
    info = object_info(src, "d.txt")

    # simulate content loss (e.g. manual deletion) while the ref still points at it
    File.rm!(ExStorageService.Storage.CAS.blob_path(info.content_hash))

    assert {:error, _} = Worker.replicate_put(src, "d.txt", replica_for(dst), info)
  end

  test "replicate_delete removes the destination object and is idempotent" do
    src = create_bucket(unique_bucket())
    dst = create_bucket(unique_bucket())

    {:ok, %{status: 200}} = Req.put("#{@base_url}/#{src}/e.txt", body: "bye")
    :ok = Worker.replicate_put(src, "e.txt", replica_for(dst), object_info(src, "e.txt"))

    assert :ok = Worker.replicate_delete(src, "e.txt", replica_for(dst))
    {:ok, %{status: 404}} = Req.get("#{@base_url}/#{dst}/e.txt")
    # 404 on repeat is success
    assert :ok = Worker.replicate_delete(src, "e.txt", replica_for(dst))
  end

  test "canonical worker resolves and streams a stateful source with explicit content length" do
    caller = self()
    chunk_size = 32_768
    chunk_count = 128
    size = chunk_size * chunk_count
    expected_hash = repeated_hash("r", chunk_size, chunk_count)

    source_opener = fn object, opts ->
      send(caller, {:opened, object, opts})

      {:ok,
       Source.stateful_stream(
         fn initial, reducer ->
           Enum.reduce_while(1..chunk_count, {:ok, initial}, fn _, {:ok, current} ->
             case reducer.(:binary.copy("r", chunk_size), current) do
               {:cont, next} -> {:cont, {:ok, next}}
               {:halt, reason, next} -> {:halt, {:error, reason, next}}
             end
           end)
         end,
         size
       )}
    end

    request = fn options ->
      case options[:method] do
        :head ->
          {:ok, Req.Response.new(status: 404)}

        :put ->
          refute is_binary(options[:body])

          {bytes, max_chunk, hash} =
            Enum.reduce(
              options[:body],
              {0, 0, :crypto.hash_init(:sha256)},
              fn chunk, {bytes, max_chunk, hash} ->
                {
                  bytes + byte_size(chunk),
                  max(max_chunk, byte_size(chunk)),
                  :crypto.hash_update(hash, chunk)
                }
              end
            )

          send(caller, {
            :uploaded,
            options[:headers],
            bytes,
            max_chunk,
            :crypto.hash_final(hash),
            options[:connect_options]
          })

          {:ok, Req.Response.new(status: 200)}
      end
    end

    object = %{
      version_id: "v-stream",
      content_hash: String.duplicate("a", 64),
      etag: "etag-stream",
      size: size,
      content_type: "application/octet-stream"
    }

    assert :ok =
             CrossClusterWorker.replicate_put(
               "source",
               "large.bin",
               replica_for("destination"),
               object,
               source_opener: source_opener,
               request: request
             )

    assert_receive {:opened, ^object, source_opts}
    assert source_opts[:bucket] == "source"

    assert_receive {:uploaded, headers, ^size, ^chunk_size, ^expected_hash, [protocols: [:http1]]}

    assert {"content-length", Integer.to_string(size)} in headers
  end

  test "canonical worker returns a source failure instead of buffering or crashing" do
    source =
      Source.stateful_stream(
        fn initial, reducer ->
          {:cont, next} = reducer.("partial", initial)
          {:error, :upstream_closed, next}
        end,
        100
      )

    request = fn options ->
      case options[:method] do
        :head ->
          {:ok, Req.Response.new(status: 404)}

        :put ->
          Enum.reduce(options[:body], 0, fn chunk, size -> size + byte_size(chunk) end)
          {:ok, Req.Response.new(status: 200)}
      end
    end

    assert {:error, {:request_failed, %RequestBodyError{reason: :upstream_closed}}} =
             CrossClusterWorker.replicate_put(
               "source",
               "broken.bin",
               replica_for("destination"),
               %{
                 version_id: "v-broken",
                 content_hash: String.duplicate("b", 64),
                 etag: "etag-broken",
                 size: 100,
                 content_type: "application/octet-stream"
               },
               source_opener: fn _object, _opts -> {:ok, source} end,
               request: request
             )
  end

  defp repeated_hash(byte, chunk_size, count) do
    hash =
      Enum.reduce(1..count, :crypto.hash_init(:sha256), fn _, hash ->
        :crypto.hash_update(hash, :binary.copy(byte, chunk_size))
      end)

    :crypto.hash_final(hash)
  end
end
