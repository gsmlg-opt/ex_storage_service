defmodule ExStorageService.CrossClusterReplication.WorkerTest do
  use ExUnit.Case, async: true

  alias ExStorageService.BlobStore.Source
  alias ExStorageService.CrossClusterReplication.Worker

  test "equal bytes with a different destination Content-Type are transferred again" do
    parent = self()
    object = object()

    request = fn opts ->
      case opts[:method] do
        :head ->
          {:ok, head("text/plain")}

        :put ->
          send(parent, {:put, opts[:headers], Enum.to_list(opts[:body])})
          {:ok, %{status: 200}}
      end
    end

    assert :ok =
             Worker.replicate_put("bucket", "key", %{endpoint: "https://dr.example"}, object,
               request: request,
               source_opener: fn _, _ -> {:ok, Source.stream(["same"], 4)} end
             )

    assert_received {:put, headers, ["same"]}
    assert {"content-type", "application/json"} in headers
    assert {"content-length", "4"} in headers
  end

  test "matching content identity and Content-Type keep HEAD deduplication" do
    parent = self()

    request = fn opts ->
      send(parent, {:request, opts[:method]})
      {:ok, head("application/json")}
    end

    assert :ok =
             Worker.replicate_put("bucket", "key", %{endpoint: "https://dr.example"}, object(),
               request: request,
               source_opener: fn _, _ -> flunk("matching destination should reuse its bytes") end
             )

    assert_received {:request, :head}
    refute_received {:request, :put}
  end

  defp object do
    %{content_hash: "hash", etag: "same-etag", size: 4, content_type: "application/json"}
  end

  defp head(content_type) do
    %{
      status: 200,
      headers: [
        {"etag", "\"same-etag\""},
        {"content-length", "4"},
        {"content-type", content_type}
      ]
    }
  end
end
