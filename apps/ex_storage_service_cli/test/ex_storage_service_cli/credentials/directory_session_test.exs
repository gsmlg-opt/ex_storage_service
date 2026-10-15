Code.require_file("../../support/wire_server.exs", __DIR__)

defmodule ExStorageServiceCli.Credentials.DirectorySessionTest do
  use ExUnit.Case, async: true
  alias ExStorageServiceCli.{Context, Error, Test.WireServer}
  alias ExStorageServiceCli.Credentials.{DirectorySession, Record}
  @now ~U[2026-10-15 00:00:00Z]

  setup_all do
    {:ok, _} = Application.ensure_all_started(:http_fetch)
    :ok
  end

  test "concurrent requests share IAM-signed CreateSession with no directory token borrowing" do
    {endpoint, _} = WireServer.fixture([{200, [], response("session-a", "2026-10-15T01:00:00Z")}])
    context = context(endpoint)
    contract = contract(endpoint)
    {:ok, manager} = DirectorySession.start_link(now: @now, clock: fn -> @now end)

    tasks =
      for _ <- 1..8,
          do:
            Task.async(fn ->
              DirectorySession.resolve(manager, context, "bucket--usw2-az1--x-s3", contract)
            end)

    results = Enum.map(tasks, &Task.await/1)

    assert Enum.all?(
             results,
             &match?(
               {:ok, %Record{access_key_id: "session-a", session_token: "session-a-token"}},
               &1
             )
           )

    assert_receive {:wire_request, %{method: "GET", target: target, headers: headers}}
    assert URI.decode_query(URI.parse(target).query) == %{"session" => ""}

    assert headers["authorization"] =~
             "Credential=iam-a/20261015/us-west-2/s3express/aws4_request"

    assert headers["x-amz-security-token"] == "iam-token"
    refute Map.has_key?(headers, "x-amz-s3session-token")
    refute_receive {:wire_request, _}, 50
    GenServer.stop(manager)
  end

  test "near-expiry credentials refresh and modes plus IAM identities do not share cache" do
    {endpoint, _} =
      WireServer.fixture([
        {200, [], response("first", "2026-10-15T00:01:00Z")},
        {200, [], response("renewed", "2026-10-15T01:00:00Z")},
        {200, [], response("readonly", "2026-10-15T01:00:00Z")},
        {200, [], response("other-iam", "2026-10-15T01:00:00Z")}
      ])

    {:ok, clock} = Agent.start_link(fn -> @now end)

    {:ok, manager} =
      DirectorySession.start_link(now: @now, clock: fn -> Agent.get(clock, & &1) end)

    context = context(endpoint)
    contract = contract(endpoint)

    assert {:ok, %Record{access_key_id: "first"}} =
             DirectorySession.resolve(manager, context, "bucket--usw2-az1--x-s3", contract)

    Agent.update(clock, fn _ -> ~U[2026-10-15 00:00:45Z] end)

    assert {:ok, %Record{access_key_id: "renewed"}} =
             DirectorySession.resolve(manager, context, "bucket--usw2-az1--x-s3", contract)

    assert {:ok, %Record{access_key_id: "readonly"}} =
             DirectorySession.resolve(
               manager,
               context,
               "bucket--usw2-az1--x-s3",
               contract,
               "ReadOnly"
             )

    assert {:ok, %Record{access_key_id: "other-iam"}} =
             DirectorySession.resolve(
               manager,
               %{context | access_key_id: "other"},
               "bucket--usw2-az1--x-s3",
               contract
             )

    for _ <- 1..2, do: assert_receive({:wire_request, _})
    assert_receive {:wire_request, %{headers: %{"x-amz-create-session-mode" => "ReadOnly"}}}
    assert_receive {:wire_request, %{headers: %{"authorization" => authorization}}}
    assert authorization =~ "Credential=other/"
    GenServer.stop(manager)
  end

  test "cache cap rejects a different key and invalid IAM/session combinations before I/O" do
    {endpoint, _} = WireServer.fixture([{200, [], response("one", "2026-10-15T01:00:00Z")}])

    {:ok, manager} =
      DirectorySession.start_link(now: @now, clock: fn -> @now end, max_contexts: 1)

    context = context(endpoint)
    assert {:ok, _} = DirectorySession.resolve(manager, context, "bucket", contract(endpoint))

    assert {:error, %Error{code: :directory_session_cache_full}} =
             DirectorySession.resolve(manager, context, "different", contract(endpoint))

    assert {:error, %Error{code: :invalid_directory_session_context}} =
             DirectorySession.resolve(
               manager,
               %{context | unsigned: true},
               "bucket",
               contract(endpoint)
             )

    assert {:error, %Error{code: :invalid_directory_session_context}} =
             DirectorySession.resolve(
               manager,
               %{context | credentials: %Record{provenance: %{provider: :s3express}}},
               "bucket",
               contract(endpoint)
             )

    assert {:error, %Error{code: :invalid_directory_session_context}} =
             DirectorySession.resolve(manager, context, "bucket", %{
               contract(endpoint)
               | session_auth: nil
             })

    refute inspect(:sys.get_status(manager)) =~ "iam-token"
    GenServer.stop(manager)
  end

  test "expired or malformed session responses fail without caching usable credentials" do
    {endpoint, _} =
      WireServer.fixture([
        {200, [], response("expired-secret", "2020-01-01T00:00:00Z")},
        {200, [], "<CreateSessionResult><Credentials/></CreateSessionResult>"}
      ])

    {:ok, manager} = DirectorySession.start_link(now: @now, clock: fn -> @now end)

    for _ <- 1..2 do
      assert {:error, %Error{} = error} =
               DirectorySession.resolve(manager, context(endpoint), "bucket", contract(endpoint))

      refute inspect(error) =~ "expired-secret"
    end

    GenServer.stop(manager)
  end

  defp context(endpoint),
    do: %Context{
      endpoint: endpoint,
      region: "us-west-2",
      access_key_id: "iam-a",
      secret_access_key: "iam-secret",
      session_token: "iam-token"
    }

  defp contract(endpoint),
    do: %{endpoint: endpoint, region: "us-west-2", service: "s3express", session_auth: :s3express}

  defp response(access, expiration),
    do:
      "<CreateSessionResult><Credentials><AccessKeyId>#{access}</AccessKeyId><SecretAccessKey>secret</SecretAccessKey><SessionToken>#{access}-token</SessionToken><Expiration>#{expiration}</Expiration></Credentials></CreateSessionResult>"
end
