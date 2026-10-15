Code.require_file("../../support/wire_server.exs", __DIR__)

defmodule ExStorageServiceCli.S3.ClientRegionTest do
  use ExUnit.Case, async: true
  alias ExStorageServiceCli.{Context, Error, S3.Client, S3.Retry, Transfer.Coordinator}
  alias ExStorageServiceCli.Test.WireServer

  @listing "<ListBucketResult><Contents><Key>key</Key><Size>0</Size></Contents><IsTruncated>false</IsTruncated></ListBucketResult>"

  defp context(endpoint, options \\ %{}) do
    %Context{
      frontend: :aws,
      endpoint: endpoint,
      region: "us-east-1",
      access_key_id: "fixture-access",
      secret_access_key: "fixture-secret",
      options: Map.put(options, :resolved_endpoint, endpoint)
    }
  end

  test "XML region fallback signs a fresh request and caches the region within this invocation" do
    {endpoint, _} =
      WireServer.fixture([
        {400, [{"location", "https://attacker.invalid/steal"}],
         "<Error><Code>AuthorizationHeaderMalformed</Code><Region>eu-west-1</Region></Error>"},
        {200, [], @listing},
        {200, [], @listing}
      ])

    state = start_supervised!({Retry.State, []})
    coordinator = start_supervised!({Coordinator, max_requests: 1, memory_budget: 131_072})
    context = %{context(endpoint, %{retry_state: state}) | coordinator: coordinator}

    assert {:ok, _} = Client.call(context, "ListObjectsV2", %{"Bucket" => "bucket"})
    assert {:ok, _} = Client.call(context, "ListObjectsV2", %{"Bucket" => "bucket"})
    assert_receive {:wire_request, first}
    assert_receive {:wire_request, corrected}
    assert_receive {:wire_request, cached}
    assert first.headers["authorization"] =~ "/us-east-1/s3/"
    assert corrected.headers["authorization"] =~ "/eu-west-1/s3/"
    assert cached.headers["authorization"] =~ "/eu-west-1/s3/"
    assert corrected.target == first.target
    assert corrected.headers["host"] == first.headers["host"]
    assert %{active: 0, queued: 0, high_requests: 1} = Coordinator.stats(coordinator)
  end

  test "missing redirect region triggers one HeadBucket discovery after releasing the failed request" do
    {endpoint, _} =
      WireServer.fixture([
        {301, [], "<Error><Code>PermanentRedirect</Code></Error>"},
        {403, [{"x-amz-bucket-region", "us-west-2"}], ""},
        {200, [], @listing}
      ])

    coordinator = start_supervised!({Coordinator, max_requests: 1, memory_budget: 131_072})
    context = %{context(endpoint) | coordinator: coordinator}
    assert {:ok, _} = Client.call(context, "ListObjectsV2", %{"Bucket" => "bucket"})
    assert_receive {:wire_request, %{method: "GET"}}
    assert_receive {:wire_request, %{method: "HEAD", target: "/bucket"}}
    assert_receive {:wire_request, %{method: "GET", headers: headers}}
    assert headers["authorization"] =~ "/us-west-2/s3/"
    assert %{active: 0, high_requests: 1} = Coordinator.stats(coordinator)
  end

  test "ordinary access denial never probes or redirects" do
    {endpoint, _} = WireServer.fixture([{403, [], "<Error><Code>AccessDenied</Code></Error>"}])

    assert {:error, %Error{code: "AccessDenied"}} =
             Client.call(context(endpoint), "ListObjectsV2", %{"Bucket" => "bucket"})

    assert_receive {:wire_request, _}
    refute_receive {:wire_request, _}, 50
  end

  test "repeated redirects are returned without an unbounded correction loop" do
    {endpoint, _} =
      WireServer.fixture([
        {301, [{"x-amz-bucket-region", "us-west-2"}], ""},
        {301, [{"x-amz-bucket-region", "eu-west-1"}], ""}
      ])

    assert {:error, %Error{http_status: 301}} =
             Client.call(context(endpoint), "ListObjectsV2", %{"Bucket" => "bucket"})

    assert_receive {:wire_request, _}
    assert_receive {:wire_request, _}
    refute_receive {:wire_request, _}, 50
  end

  test "transient retries retain the corrected signing region without an invocation retry state" do
    {endpoint, _} =
      WireServer.fixture([
        {301, [{"x-amz-bucket-region", "us-west-2"}], ""},
        {503, [], "<Error><Code>SlowDown</Code></Error>"},
        {200, [], @listing}
      ])

    assert {:ok, _} =
             Client.call(context(endpoint), "ListObjectsV2", %{"Bucket" => "bucket"},
               sleep: fn _ -> :ok end
             )

    assert_receive {:wire_request, initial}
    assert_receive {:wire_request, corrected}
    assert_receive {:wire_request, retried}
    assert initial.headers["authorization"] =~ "/us-east-1/s3/"
    assert corrected.headers["authorization"] =~ "/us-west-2/s3/"
    assert retried.headers["authorization"] =~ "/us-west-2/s3/"
  end

  test "a transient retry cannot reset the operation's regional correction budget" do
    {endpoint, _} =
      WireServer.fixture([
        {301, [{"x-amz-bucket-region", "us-west-2"}], ""},
        {503, [], "<Error><Code>SlowDown</Code></Error>"},
        {301, [{"x-amz-bucket-region", "eu-west-1"}], ""},
        {200, [], @listing}
      ])

    state = start_supervised!({Retry.State, []})

    assert {:error, %Error{http_status: 301}} =
             Client.call(
               context(endpoint, %{retry_state: state}),
               "ListObjectsV2",
               %{"Bucket" => "bucket"},
               sleep: fn _ -> :ok end
             )

    assert_receive {:wire_request, _}
    assert_receive {:wire_request, _}
    assert_receive {:wire_request, _}
    refute_receive {:wire_request, _}, 50
  end

  test "discovery and corrected request share the original overall deadline" do
    {endpoint, _} =
      WireServer.start(fn listener, owner ->
        {:ok, socket} = :gen_tcp.accept(listener, 1000)
        send(owner, {:wire_request, WireServer.request(socket)})
        Process.sleep(150)
        WireServer.response(socket, 301, [{"x-amz-bucket-region", "us-west-2"}], "")
        :gen_tcp.close(socket)
      end)

    started = System.monotonic_time(:millisecond)

    assert {:error, %Error{category: :transport}} =
             Client.call(context(endpoint), "ListObjectsV2", %{"Bucket" => "bucket"},
               overall_timeout: 50,
               max_attempts: 1
             )

    assert System.monotonic_time(:millisecond) - started < 500
    assert_receive {:wire_request, _}
    refute_receive {:wire_request, _}, 50
  end
end
