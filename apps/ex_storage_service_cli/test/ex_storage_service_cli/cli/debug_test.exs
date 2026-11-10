Code.require_file("../../support/wire_server.exs", __DIR__)

defmodule ExStorageServiceCli.CLI.DebugTest do
  use ExUnit.Case, async: true
  alias ExStorageServiceSdk.Debug
  alias ExStorageServiceSdk.S3.{Client, Request}
  alias ExStorageServiceCli.Test.WireServer

  test "debug has an observable effect without disclosing input, authentication or signed queries" do
    owner = self()
    write = fn bytes -> send(owner, {:debug, JSON.decode!(bytes)}) end

    request = %Request{
      operation: "PutObject",
      method: "PUT",
      body: "private-body",
      headers: [{"Authorization", "secret-authorization"}, {"x-amz-security-token", "token"}]
    }

    context = %ExStorageServiceSdk.Config{
      options: %{debug: true},
      secret_access_key: "secret",
      session_token: "token"
    }

    assert :ok =
             Debug.request(
               context,
               request,
               "https://objects.test/private-key?X-Amz-Signature=sensitive",
               debug_write: write
             )

    assert_receive {:debug, event}
    assert event["event"] == "request"
    assert event["host"] == "objects.test"
    text = JSON.encode!(event)

    for private <- ~w(private-key private-body secret token sensitive Authorization),
        do: refute(text =~ private)

    assert :ok =
             Debug.request(%{context | options: %{}}, request, "https://objects.test",
               debug_write: write
             )

    refute_receive {:debug, _}, 20
  end

  test "modeled client emits request and response metadata only when enabled" do
    {endpoint, _} =
      WireServer.fixture([
        {200, [], "<ListAllMyBucketsResult><Buckets/></ListAllMyBucketsResult>"}
      ])

    owner = self()
    write = fn bytes -> send(owner, {:debug, JSON.decode!(bytes)}) end

    assert {:ok, _} =
             Client.call(
               %ExStorageServiceSdk.Config{
                 endpoint: endpoint,
                 unsigned: true,
                 options: %{debug: true}
               },
               "ListBuckets",
               %{},
               debug_write: write
             )

    assert_receive {:debug, %{"event" => "request", "operation" => "ListBuckets"}}
    assert_receive {:debug, %{"event" => "response", "status" => 200}}
  end
end
