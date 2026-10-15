Code.require_file("../../support/wire_server.exs", __DIR__)

defmodule ExStorageServiceCli.S3.RetryIntegrationTest do
  use ExUnit.Case, async: true
  alias ExStorageServiceCli.{Context, Error, S3.Client}
  alias ExStorageServiceCli.Test.WireServer
  alias ExStorageServiceCli.Transfer.Coordinator

  setup_all do
    {:ok, _} = Application.ensure_all_started(:http_fetch)
    :ok
  end

  defp context(endpoint, options \\ %{}) do
    %Context{
      frontend: :aws,
      endpoint: endpoint,
      region: "us-east-1",
      addressing: :path,
      access_key_id: "fixture-access",
      secret_access_key: "fixture-secret",
      options: Map.merge(%{resolved_endpoint: endpoint}, options)
    }
  end

  test "301 correction re-signs on the original custom endpoint with one request lease" do
    {attacker, _} = WireServer.fixture([{200, [], ""}])

    {endpoint, _} =
      WireServer.fixture([
        {301, [{"x-amz-bucket-region", "us-west-2"}, {"Location", attacker}], ""},
        {200, [], ""}
      ])

    {:ok, coordinator} = start_supervised({Coordinator, max_requests: 1})
    context = %{context(endpoint, %{max_attempts: 1}) | coordinator: coordinator}
    worker = Task.async(fn -> Client.call(context, "HeadBucket", %{"Bucket" => "my-bucket"}) end)

    assert {:ok, _} = Task.await(worker, 2000)
    assert_receive {:wire_request, first}
    assert_receive {:wire_request, second}
    assert first.headers["host"] == second.headers["host"]
    assert first.headers["host"] == URI.parse(endpoint).authority
    assert first.target == "/my-bucket"
    assert second.target == first.target
    assert first.headers["authorization"] =~ "/us-east-1/s3/aws4_request"
    assert second.headers["authorization"] =~ "/us-west-2/s3/aws4_request"
    refute first.headers["authorization"] == second.headers["authorization"]
    refute_receive {:wire_request, _}
    assert %{active: 0, bytes: 0, high_requests: 1} = Coordinator.stats(coordinator)
  end

  test "a Location without validated regional correction receives no credentials" do
    {attacker, _} = WireServer.fixture([{200, [], ""}])
    {endpoint, _} = WireServer.fixture([{307, [{"Location", attacker}], ""}])

    assert {:error, %Error{category: :service, http_status: 307}} =
             Client.call(context(endpoint), "HeadBucket", %{"Bucket" => "my-bucket"})

    assert_receive {:wire_request, %{headers: headers}}
    assert headers["authorization"] =~ "AWS4-HMAC-SHA256"
    refute_receive {:wire_request, _}
  end

  test "resolved retry settings bound actual HTTP requests" do
    body = "<Error><Code>SlowDown</Code><Message>fixture</Message></Error>"
    {endpoint, _} = WireServer.fixture([{503, [], body}, {503, [], body}, {200, [], ""}])
    settings = %{retry_mode: "standard", max_attempts: 2}

    assert {:error, %Error{code: "SlowDown", attempt: 2}} =
             Client.call(context(endpoint, settings), "ListBuckets", %{}, sleep: fn _ -> :ok end)

    assert_receive {:wire_request, _}
    assert_receive {:wire_request, _}
    refute_receive {:wire_request, _}
  end

  test "coordinator cancellation during worker retry backoff prevents further network admission" do
    body = "<Error><Code>SlowDown</Code><Message>fixture</Message></Error>"
    {endpoint, _} = WireServer.fixture([{503, [], body}, {200, [], ""}])
    {:ok, coordinator} = start_supervised({Coordinator, max_requests: 1})
    context = %{context(endpoint) | coordinator: coordinator}

    worker =
      Task.async(fn -> Client.call(context, "ListBuckets", %{}, jitter: fn _ -> 5000 end) end)

    assert_receive {:wire_request, _}, 2_000
    :ok = Coordinator.cancel(coordinator)
    assert {:error, %Error{category: :interrupted}} = Task.await(worker, 1000)
    refute_receive {:wire_request, _}
    assert %{active: 0, bytes: 0} = Coordinator.stats(coordinator)
  end
end
