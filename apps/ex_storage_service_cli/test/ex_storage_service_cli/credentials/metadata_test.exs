Code.require_file("../../support/wire_server.exs", __DIR__)

defmodule ExStorageServiceCli.Credentials.MetadataTest do
  # The real 150 ms socket deadlines must not compete with unrelated async cases.
  use ExUnit.Case, async: false
  alias ExStorageServiceCli.Credentials.{Metadata, Provider, Record}
  alias ExStorageServiceCli.{Error, Test.WireServer}
  @moduletag :tmp_dir
  @now ~U[2026-10-15 00:00:00Z]

  setup_all do
    {:ok, _} = Application.ensure_all_started(:http_fetch)
    :ok
  end

  test "container response includes coherent temporary fields and authorization without redirects" do
    {endpoint, _} = WireServer.fixture([{200, [], response()}])

    env = %{
      "AWS_CONTAINER_CREDENTIALS_FULL_URI" => endpoint <> "/credentials",
      "AWS_CONTAINER_AUTHORIZATION_TOKEN" => "authorization-marker"
    }

    assert {:ok,
            %Record{
              access_key_id: "container-a",
              session_token: "container-t",
              expires_at: ~U[2026-10-15 01:00:00Z]
            }} = Metadata.container(env, now: @now)

    assert_receive {:wire_request,
                    %{
                      target: "/credentials",
                      headers: %{"authorization" => "authorization-marker"}
                    }}

    {destination, _} = WireServer.fixture([{200, [], response()}])
    {redirect, _} = WireServer.fixture([{302, [{"Location", destination}], ""}])

    assert {:error, %Error{code: :credential_service_error, http_status: 302}} =
             Metadata.container(%{env | "AWS_CONTAINER_CREDENTIALS_FULL_URI" => redirect},
               now: @now
             )

    assert_receive {:wire_request, %{headers: %{"authorization" => "authorization-marker"}}}
    refute_receive {:wire_request, _}, 100
  end

  test "unsafe endpoints tokens and malformed expiring credentials fail safely", %{tmp_dir: root} do
    for endpoint <- [
          "http://example.test/creds",
          "http://127.0.0.1:2/creds#private",
          "https://user@example.test/creds"
        ] do
      assert {:error, %Error{code: :unsafe_metadata_endpoint}} =
               Metadata.container(%{"AWS_CONTAINER_CREDENTIALS_FULL_URI" => endpoint})
    end

    token_file = Path.join(root, "token")
    File.write!(token_file, "bad\nheader")

    assert {:error, %Error{code: :invalid_container_authorization}} =
             Metadata.container(%{
               "AWS_CONTAINER_CREDENTIALS_FULL_URI" => "http://127.0.0.1:2",
               "AWS_CONTAINER_AUTHORIZATION_TOKEN_FILE" => token_file
             })

    for body <- [
          "invalid",
          response("2020-01-01T00:00:00Z"),
          JSON.encode!(%{"AccessKeyId" => "a", "SecretAccessKey" => "s"})
        ] do
      {endpoint, _} = WireServer.fixture([{200, [], body}])

      assert {:error, %Error{} = error} =
               Metadata.container(%{"AWS_CONTAINER_CREDENTIALS_FULL_URI" => endpoint}, now: @now)

      refute inspect(error) =~ "container-t"
    end

    {endpoint, _} = WireServer.fixture([{200, [], String.duplicate("x", 65_537)}])

    assert {:error, %Error{code: :credential_transport_error}} =
             Metadata.container(%{"AWS_CONTAINER_CREDENTIALS_FULL_URI" => endpoint}, now: @now)
  end

  test "IMDSv2 obtains token before role and uses it on both credential GETs" do
    {endpoint, _} =
      WireServer.fixture([
        {200, [], "metadata-token"},
        {200, [], "instance-role\n"},
        {200, [], response()}
      ])

    assert {:ok, %Record{provenance: %{provider: :imds}}} =
             Metadata.imds(%{}, metadata_endpoint: endpoint, now: @now)

    assert_receive {:wire_request,
                    %{
                      method: "PUT",
                      target: "/latest/api/token",
                      headers: %{"x-aws-ec2-metadata-token-ttl-seconds" => "21600"}
                    }}

    assert_receive {:wire_request,
                    %{
                      method: "GET",
                      target: "/latest/meta-data/iam/security-credentials/",
                      headers: %{"x-aws-ec2-metadata-token" => "metadata-token"}
                    }}

    assert_receive {:wire_request,
                    %{
                      target: "/latest/meta-data/iam/security-credentials/instance-role",
                      headers: %{"x-aws-ec2-metadata-token" => "metadata-token"}
                    }}
  end

  test "disabled IMDSv1 and token redirects cannot downgrade or forward tokens" do
    {endpoint, _} = WireServer.fixture([{403, [], "refused"}])

    assert {:error, %Error{code: :imds_v1_disabled}} =
             Metadata.imds(%{"AWS_EC2_METADATA_V1_DISABLED" => "true"},
               metadata_endpoint: endpoint
             )

    assert_receive {:wire_request, %{method: "PUT"}}
    refute_receive {:wire_request, _}, 100

    assert {:error, %Error{code: :unsafe_metadata_endpoint}} =
             Metadata.imds(%{"AWS_EC2_METADATA_SERVICE_ENDPOINT" => "http://127.0.0.1:2"})
  end

  test "pinned token refusal statuses permit IMDSv1 only when enabled" do
    for status <- [401, 403, 404, 405, 500] do
      {endpoint, _} =
        WireServer.fixture([
          {status, [], "no-token"},
          {200, [], "instance-role"},
          {200, [], response()}
        ])

      assert {:ok, %Record{}} = Metadata.imds(%{}, metadata_endpoint: endpoint, now: @now)
      assert_receive {:wire_request, %{method: "PUT"}}
      assert_receive {:wire_request, %{method: "GET", headers: first}}
      assert_receive {:wire_request, %{method: "GET", headers: second}}
      refute Map.has_key?(first, "x-aws-ec2-metadata-token")
      refute Map.has_key?(second, "x-aws-ec2-metadata-token")
    end
  end

  test "bad token requests, malformed bodies, and authenticated GET401 never downgrade" do
    for responses <- [
          [{400, [], "bad"}],
          [{307, [{"Location", "http://127.0.0.1:1"}], ""}],
          [{200, [], "invalid\nheader"}],
          [{200, [], "v2-token"}, {401, [], "unauthorized"}]
        ] do
      {endpoint, _} = WireServer.fixture(responses)
      assert {:error, %Error{}} = Metadata.imds(%{}, metadata_endpoint: endpoint, now: @now)
      for _ <- responses, do: assert_receive({:wire_request, _})
      refute_receive {:wire_request, _}, 20
    end
  end

  test "profile IMDSv1 policy is enforced and an explicit environment policy wins", %{
    tmp_dir: root
  } do
    File.write!(
      Path.join(root, "config"),
      "[profile role]\nrole_arn=arn:role\ncredential_source=Ec2InstanceMetadata\nec2_metadata_v1_disabled=true\n"
    )

    {endpoint, _} = WireServer.fixture([{403, [], "refused"}])

    opts = [
      env: %{},
      config_file: Path.join(root, "config"),
      credentials_file: Path.join(root, "missing"),
      metadata_endpoint: endpoint,
      now: @now
    ]

    assert {:error, %Error{code: :imds_v1_disabled}} = Provider.resolve(%{profile: "role"}, opts)
    assert_receive {:wire_request, %{method: "PUT"}}
    refute_receive {:wire_request, _}, 20

    {endpoint, _} =
      WireServer.fixture([
        {403, [], "refused"},
        {200, [], "instance-role"},
        {200, [], response()}
      ])

    assert {:ok, %Record{}} =
             Metadata.imds(%{"AWS_EC2_METADATA_V1_DISABLED" => "false"},
               metadata_endpoint: endpoint,
               metadata_config: %{"ec2_metadata_v1_disabled" => "true"},
               now: @now
             )

    for _ <- 1..3, do: assert_receive({:wire_request, _})

    assert {:error, %Error{}} =
             Metadata.imds(%{"AWS_EC2_METADATA_DISABLED" => "true"}, metadata_endpoint: endpoint)

    refute_receive {:wire_request, _}, 20
  end

  test "provider selects container without static fallback", %{tmp_dir: root} do
    {endpoint, _} = WireServer.fixture([{200, [], response()}])

    assert {:ok, %Record{provenance: %{provider: :container}}, _} =
             Provider.resolve(%{},
               home: root,
               env: %{"AWS_CONTAINER_CREDENTIALS_FULL_URI" => endpoint},
               now: @now
             )
  end

  test "actual stalled token response falls back to tokenless IMDSv1 and closes its request" do
    {endpoint, server} = stalled_token_peer(:allow)

    assert {:ok, %Record{provenance: %{provider: :imds}}} =
             Metadata.imds(%{}, metadata_endpoint: endpoint, provider_timeout: 150, now: @now)

    assert_receive {:wire_request, %{method: "PUT", target: "/latest/api/token"}}
    assert_receive {:stalled_token_closed, {:error, :closed}}

    assert_receive {:wire_request,
                    %{
                      method: "GET",
                      target: "/latest/meta-data/iam/security-credentials/",
                      headers: first
                    }}

    assert_receive {:wire_request,
                    %{
                      method: "GET",
                      target: "/latest/meta-data/iam/security-credentials/instance-role",
                      headers: second
                    }}

    refute Map.has_key?(first, "x-aws-ec2-metadata-token")
    refute Map.has_key?(second, "x-aws-ec2-metadata-token")
    assert_receive {:metadata_peer_done, ^server}
  end

  test "actual token read timeout never makes credential GETs when IMDSv1 is disabled" do
    {endpoint, server} = stalled_token_peer(:deny)

    assert {:error, %Error{code: :imds_v1_disabled}} =
             Metadata.imds(%{"AWS_EC2_METADATA_V1_DISABLED" => "true"},
               metadata_endpoint: endpoint,
               provider_timeout: 150,
               now: @now
             )

    assert_receive {:wire_request, %{method: "PUT"}}
    assert_receive {:stalled_token_closed, {:error, :closed}}
    assert_receive {:metadata_extra_connection, {:error, :timeout}}, 1000
    assert_receive {:metadata_peer_done, ^server}
    refute_receive {:wire_request, %{method: "GET"}}, 20
  end

  test "metadata invocation death cancels stalled token request without a v1 downgrade" do
    {endpoint, server} = stalled_token_peer(:deny)

    owner =
      spawn(fn ->
        Metadata.imds(%{}, metadata_endpoint: endpoint, provider_timeout: 5000, now: @now)
      end)

    monitor = Process.monitor(owner)
    assert_receive {:wire_request, %{method: "PUT"}}, 1000
    Process.exit(owner, :kill)
    assert_receive {:DOWN, ^monitor, :process, ^owner, :killed}
    assert_receive {:stalled_token_closed, {:error, :closed}}, 1000
    assert_receive {:metadata_extra_connection, {:error, :timeout}}, 1000
    assert_receive {:metadata_peer_done, ^server}
    refute_receive {:wire_request, %{method: "GET"}}, 20
  end

  defp stalled_token_peer(mode) do
    WireServer.start(fn listener, owner ->
      {:ok, socket} = :gen_tcp.accept(listener, 2000)
      send(owner, {:wire_request, WireServer.request(socket)})
      WireServer.response(socket, 200, [{"Content-Length", "8"}], "")
      send(owner, {:stalled_token_closed, :gen_tcp.recv(socket, 0, 2000)})
      :gen_tcp.close(socket)

      if mode == :allow do
        for body <- ["instance-role", response()] do
          {:ok, socket} = :gen_tcp.accept(listener, 2000)
          send(owner, {:wire_request, WireServer.request(socket)})
          WireServer.response(socket, 200, [], body)
          :gen_tcp.close(socket)
        end
      else
        result = :gen_tcp.accept(listener, 250)

        if match?({:ok, _}, result) do
          {:ok, socket} = result
          send(owner, {:wire_request, WireServer.request(socket)})
          :gen_tcp.close(socket)
        end

        send(owner, {:metadata_extra_connection, result})
      end

      send(owner, {:metadata_peer_done, self()})
    end)
  end

  defp response(expiration \\ "2026-10-15T01:00:00Z"),
    do:
      JSON.encode!(%{
        "Code" => "Success",
        "AccessKeyId" => "container-a",
        "SecretAccessKey" => "container-s",
        "Token" => "container-t",
        "Expiration" => expiration
      })
end
