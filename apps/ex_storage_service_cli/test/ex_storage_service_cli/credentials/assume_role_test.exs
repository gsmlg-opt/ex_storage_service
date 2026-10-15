Code.require_file("../../support/wire_server.exs", __DIR__)

defmodule ExStorageServiceCli.Credentials.AssumeRoleTest do
  use ExUnit.Case, async: true
  alias ExStorageServiceCli.Credentials.{Provider, Record}
  alias ExStorageServiceCli.{Error, Test.WireServer}
  @moduletag :tmp_dir
  @now ~U[2026-10-15 00:00:00Z]

  setup_all do
    {:ok, _} = Application.ensure_all_started(:http_fetch)
    :ok
  end

  test "source_profile signs STS with source temporary credentials and does not mix environment",
       %{tmp_dir: root} do
    {endpoint, _} = WireServer.fixture([{200, [], response("AssumeRole")}])

    opts =
      files(
        root,
        "[profile role]\nrole_arn=arn:aws:iam::123456789012:role/test\nsource_profile=base\nregion=eu-west-1\nrole_session_name=session\nexternal_id=external\nduration_seconds=900\n",
        "[base]\naws_access_key_id=base-a\naws_secret_access_key=base-s\naws_session_token=base-t\n"
      )
      |> Keyword.put(:sts_endpoint, endpoint)
      |> Keyword.put(:env, %{"AWS_ACCESS_KEY_ID" => "wrong", "AWS_SECRET_ACCESS_KEY" => "wrong"})

    assert {:ok,
            %Record{
              access_key_id: "role-a",
              session_token: "role-t",
              provenance: %{provider: :assume_role}
            }, _} = Provider.resolve(%{profile: "role", region: "ap-south-1"}, opts)

    assert_receive {:wire_request, %{method: "POST", headers: headers, body: body}}
    assert headers["authorization"] =~ "Credential=base-a/20261015/ap-south-1/sts/aws4_request"
    assert headers["x-amz-security-token"] == "base-t"
    params = URI.decode_query(body)
    assert params["Action"] == "AssumeRole"
    assert params["RoleSessionName"] == "session"
    assert params["ExternalId"] == "external"
    assert params["DurationSeconds"] == "900"
  end

  test "web identity uses bounded token file without source credentials or signing", %{
    tmp_dir: root
  } do
    token_file = Path.join(root, "web-token")
    File.write!(token_file, "web-token-marker")
    {endpoint, _} = WireServer.fixture([{200, [], response("AssumeRoleWithWebIdentity")}])

    opts =
      files(
        root,
        "[profile role]\nrole_arn=arn:aws:iam::123456789012:role/web\nweb_identity_token_file=#{token_file}\nrole_session_name=web\n",
        "[role]\naws_access_key_id=wrong\naws_secret_access_key=wrong\n"
      )
      |> Keyword.put(:sts_endpoint, endpoint)

    assert {:ok, %Record{provenance: %{provider: :web_identity}}, _} =
             Provider.resolve(%{profile: "role"}, opts)

    assert_receive {:wire_request, %{headers: headers, body: body}}
    refute Map.has_key?(headers, "authorization")
    assert URI.decode_query(body)["WebIdentityToken"] == "web-token-marker"
  end

  test "environment web identity is selected without borrowing absent credentials", %{
    tmp_dir: root
  } do
    token_file = Path.join(root, "token")
    File.write!(token_file, "token")
    {endpoint, _} = WireServer.fixture([{200, [], response("AssumeRoleWithWebIdentity")}])

    opts =
      files(root, "", "")
      |> Keyword.put(:sts_endpoint, endpoint)
      |> Keyword.put(:env, %{
        "AWS_WEB_IDENTITY_TOKEN_FILE" => token_file,
        "AWS_ROLE_ARN" => "arn:aws:iam::123456789012:role/web"
      })

    assert {:ok, %Record{provenance: %{provider: :web_identity}}, _} = Provider.resolve(%{}, opts)
  end

  test "role cycles and invalid sources fail before networking", %{tmp_dir: root} do
    opts =
      files(
        root,
        "[profile a]\nrole_arn=arn:a\nsource_profile=b\n[profile b]\nrole_arn=arn:b\nsource_profile=a\n",
        ""
      )

    assert {:error, %Error{code: :credential_profile_cycle}} =
             Provider.resolve(%{profile: "a"}, opts)

    opts =
      files(
        root,
        "[profile a]\nrole_arn=arn:a\nsource_profile=base\ncredential_source=Environment\n",
        "[base]\naws_access_key_id=a\naws_secret_access_key=s\n"
      )

    assert {:error, %Error{code: :invalid_role_config}} = Provider.resolve(%{profile: "a"}, opts)
  end

  test "credential_source Environment and explicit MFA never enter result provenance", %{
    tmp_dir: root
  } do
    {endpoint, _} = WireServer.fixture([{200, [], response("AssumeRole")}])

    opts =
      files(
        root,
        "[profile role]\nrole_arn=arn:role\ncredential_source=Environment\nmfa_serial=arn:mfa\n",
        ""
      )
      |> Keyword.put(:sts_endpoint, endpoint)
      |> Keyword.put(:env, %{
        "AWS_ACCESS_KEY_ID" => "source-a",
        "AWS_SECRET_ACCESS_KEY" => "source-s"
      })

    assert {:error, %Error{code: :mfa_required}} = Provider.resolve(%{profile: "role"}, opts)

    assert {:ok, record, _} =
             Provider.resolve(%{profile: "role"}, Keyword.put(opts, :mfa_code, "123456"))

    refute inspect(record) =~ "123456"
    assert_receive {:wire_request, %{body: body}}
    assert URI.decode_query(body)["TokenCode"] == "123456"
  end

  test "STS redirects cannot forward signed source or identity tokens", %{tmp_dir: root} do
    {destination, _} = WireServer.fixture([{200, [], response("AssumeRole")}])
    {endpoint, _} = WireServer.fixture([{307, [{"Location", destination}], ""}])

    opts =
      files(root, "[profile role]\nrole_arn=arn:role\ncredential_source=Environment\n", "")
      |> Keyword.put(:sts_endpoint, endpoint)
      |> Keyword.put(:env, %{
        "AWS_ACCESS_KEY_ID" => "source-a",
        "AWS_SECRET_ACCESS_KEY" => "source-s"
      })

    assert {:error, %Error{code: :credential_service_error, http_status: 307}} =
             Provider.resolve(%{profile: "role"}, opts)

    assert_receive {:wire_request, %{headers: %{"authorization" => _}}}
    refute_receive {:wire_request, _}, 100
  end

  test "MFA prompt callback supplies a coherent token and invalid prompt values fail before transport",
       %{tmp_dir: root} do
    {endpoint, _} = WireServer.fixture([{200, [], response("AssumeRole")}])

    opts =
      files(
        root,
        "[profile role]\nrole_arn=arn:role\ncredential_source=Environment\nmfa_serial=arn:mfa\n",
        ""
      )
      |> Keyword.put(:sts_endpoint, endpoint)
      |> Keyword.put(:env, %{
        "AWS_ACCESS_KEY_ID" => "source-a",
        "AWS_SECRET_ACCESS_KEY" => "source-s"
      })

    parent = self()

    prompt = fn serial ->
      send(parent, {:mfa_prompt, serial})
      {:ok, "654321"}
    end

    assert {:ok, record, _} =
             Provider.resolve(%{profile: "role"}, Keyword.put(opts, :mfa_prompt, prompt))

    assert_receive {:mfa_prompt, "arn:mfa"}
    assert_receive {:wire_request, %{body: body}}
    assert URI.decode_query(body)["TokenCode"] == "654321"
    refute inspect(record) =~ "654321"

    assert {:error, %Error{code: :invalid_mfa_code} = error} =
             Provider.resolve(
               %{profile: "role"},
               Keyword.put(opts, :mfa_prompt, fn _ -> {:ok, "invalid-secret"} end)
             )

    refute inspect(error) =~ "invalid-secret"
    refute_receive {:wire_request, _}
  end

  defp response(action),
    do:
      "<#{action}Response><#{action}Result><Credentials><AccessKeyId>role-a</AccessKeyId><SecretAccessKey>role-s</SecretAccessKey><SessionToken>role-t</SessionToken><Expiration>2026-10-15T01:00:00Z</Expiration></Credentials></#{action}Result></#{action}Response>"

  defp files(root, config, credentials) do
    config_file = Path.join(root, "config")
    credentials_file = Path.join(root, "credentials")
    File.write!(config_file, config)
    File.write!(credentials_file, credentials)

    [
      env: %{},
      home: root,
      config_file: config_file,
      credentials_file: credentials_file,
      now: @now
    ]
  end
end
