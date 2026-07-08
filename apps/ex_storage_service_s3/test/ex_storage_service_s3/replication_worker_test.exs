defmodule ExStorageServiceS3.ReplicationWorkerTest do
  use ExUnit.Case, async: false

  import ExUnit.CaptureLog

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
end
