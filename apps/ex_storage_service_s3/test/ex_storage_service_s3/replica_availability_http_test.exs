defmodule ExStorageServiceS3.ReplicaAvailabilityHTTPTest do
  use ExUnit.Case, async: true

  alias ExStorageService.Cluster.{BlobDescriptor, ReplicaAck, WriteCoordinator}
  alias ExStorageServiceS3.Handlers.Shared

  defmodule Server do
    def init(opts), do: opts

    def call(conn, _opts) do
      hash = String.duplicate("a", 64)

      descriptor = %BlobDescriptor{
        schema: 2,
        hash: hash,
        algorithm: :sha256,
        size: 7,
        desired_replication_factor: 2,
        created_at: "2026-10-18T00:00:00Z"
      }

      stale = %ReplicaAck{
        node_id: "data-b",
        node_generation: 1,
        hash: hash,
        size: 7,
        verified_at: "2026-10-18T00:00:00Z",
        fencing_or_request_id: "replica-attempt"
      }

      # This is the coordinator result reproduced by the real strict RF=2/W=2
      # cluster when registration generation 2 disagrees with the running peer.
      {:error, reason} =
        WriteCoordinator.validate_acknowledgements(
          [stale],
          [%{node: %{node_id: "data-b", generation: 2}}],
          descriptor
        )

      Shared.storage_error_response(conn, reason, "/bucket/key", "stale-replica-http")
    end
  end

  test "a rejected stale-generation replica is an S3 availability error over HTTP" do
    server =
      start_supervised!(
        {Gaoxy.HTTP, plug: {Server, []}, port: 0, limits: %{max_bytes: 4_194_304}}
      )

    %{local: {:ok, {_, port}}} = Gaoxy.info(server, :http)
    url = "http://127.0.0.1:#{port}/bucket/key"

    for method <- [:put, :head] do
      response = Req.request!(method: method, url: url, retry: false)
      assert response.status == 503
      assert response.headers["x-amz-request-id"] == ["stale-replica-http"]

      if method == :head do
        assert response.body == ""
      else
        assert response.body =~ "<Code>ServiceUnavailable</Code>"
        refute response.body =~ "invalid_replica_ack"
        refute response.body =~ "data-b"
      end
    end
  end
end
