defmodule ExStorageServiceS3.ConditionalWriteTest do
  use ExUnit.Case, async: false
  @url "http://localhost:#{Application.compile_env(:ex_storage_service, :s3_port, 9001)}"

  setup do
    bucket = "conditional-#{System.unique_integer([:positive])}"
    assert {:ok, %{status: 201}} = Req.put("#{@url}/#{bucket}", body: "")
    %{url: "#{@url}/#{bucket}/key"}
  end

  test "32 synchronized create-only writers publish exactly one object", %{url: url} do
    parent = self()

    tasks =
      for n <- 1..32 do
        Task.async(fn ->
          send(parent, {:ready, self()})

          receive do
            :go -> :ok
          end

          Req.put!(url, body: "writer-#{n}", headers: [{"if-none-match", "*"}]).status
        end)
      end

    pids =
      for _ <- 1..32 do
        receive do
          {:ready, pid} -> pid
        after
          5_000 -> flunk("writer failed to synchronize")
        end
      end

    Enum.each(pids, &send(&1, :go))
    statuses = Enum.map(tasks, &Task.await(&1, 30_000))
    assert Enum.count(statuses, &(&1 == 200)) == 1
    assert Enum.count(statuses, &(&1 == 412)) == 31
  end

  test "stale If-Match cannot replace an updated head", %{url: url} do
    response = Req.put!(url, body: "first")
    [etag] = response.headers["etag"]
    assert Req.put!(url, body: "second", headers: [{"if-match", etag}]).status == 200
    assert Req.put!(url, body: "stale", headers: [{"if-match", etag}]).status == 412
    assert Req.get!(url).body == "second"
  end

  test "copy source conditions apply to the pinned source identity", %{url: url} do
    source = Req.put!(url, body: "source-body")
    [etag] = source.headers["etag"]
    path = URI.parse(url).path
    destination = url <> "-copy"

    assert Req.put!(destination,
             body: "",
             headers: [
               {"x-amz-copy-source", path},
               {"x-amz-copy-source-if-match", "\"stale\""}
             ]
           ).status == 412

    assert Req.get!(destination).status == 404

    # S3 gives a successful If-Match precedence over If-Unmodified-Since.
    assert Req.put!(destination,
             body: "",
             headers: [
               {"x-amz-copy-source", path},
               {"x-amz-copy-source-if-match", etag},
               {"x-amz-copy-source-if-unmodified-since", "Thu, 01 Jan 1970 00:00:00 GMT"}
             ]
           ).status == 200

    assert Req.get!(destination).body == "source-body"

    assert Req.put!(destination,
             body: "",
             headers: [
               {"x-amz-copy-source", path},
               {"x-amz-copy-source-if-none-match", etag},
               {"x-amz-copy-source-if-modified-since", "Thu, 01 Jan 1970 00:00:00 GMT"}
             ]
           ).status == 412

    assert Req.get!(destination).body == "source-body"
  end
end
