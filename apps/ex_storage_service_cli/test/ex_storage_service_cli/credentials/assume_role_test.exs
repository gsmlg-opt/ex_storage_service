Code.require_file("../../support/wire_server.exs", __DIR__)

defmodule ExStorageServiceCli.Credentials.AssumeRoleTest do
  use ExUnit.Case, async: true
  alias ExStorageServiceCli.Credentials.{AssumeRole, Provider, Record}
  alias ExStorageServiceCli.{Error, Test.WireServer}
  @moduletag :tmp_dir
  @now ~U[2026-10-15 00:00:00Z]

  setup_all do
    {:ok, _} = Application.ensure_all_started(:http_fetch)
    :ok
  end

  # Expected endpoints/signing scopes come from pinned AWS CLI 2.31.18 STS rules.
  for {region, suffix} <- [
        {"us-east-1", "amazonaws.com"},
        {"eu-west-1", "amazonaws.com"},
        {"af-south-1", "amazonaws.com"},
        {"cn-north-1", "amazonaws.com.cn"},
        {"us-gov-west-1", "amazonaws.com"},
        {"us-iso-east-1", "c2s.ic.gov"},
        {"us-isob-east-1", "sc2s.sgov.gov"},
        {"eu-isoe-west-1", "cloud.adc-e.uk"},
        {"us-isof-east-1", "csp.hci.ic.gov"}
      ] do
    test "STS default selects pinned regional endpoint and scope for #{region}" do
      region = unquote(region)
      expected = "https://sts.#{region}.#{unquote(suffix)}"

      assert {:ok, %{endpoint: ^expected, signing_region: ^region}} =
               AssumeRole.endpoint(%{"region" => region}, %{}, [])

      assert {:ok, %{endpoint: ^expected, signing_region: ^region}} =
               AssumeRole.endpoint(
                 %{"region" => region, "sts_regional_endpoints" => "regional"},
                 %{},
                 []
               )
    end
  end

  test "legacy mode uses global only for the pinned legacy regions" do
    assert {:ok, %{endpoint: "https://sts.amazonaws.com", signing_region: "us-east-1"}} =
             AssumeRole.endpoint(
               %{"region" => "eu-west-1", "sts_regional_endpoints" => "legacy"},
               %{},
               []
             )

    for {region, suffix} <- [
          {"af-south-1", "amazonaws.com"},
          {"cn-north-1", "amazonaws.com.cn"},
          {"us-gov-west-1", "amazonaws.com"},
          {"us-iso-east-1", "c2s.ic.gov"},
          {"us-isob-east-1", "sc2s.sgov.gov"},
          {"eu-isoe-west-1", "cloud.adc-e.uk"},
          {"us-isof-east-1", "csp.hci.ic.gov"}
        ] do
      expected = "https://sts.#{region}.#{suffix}"

      assert {:ok, %{endpoint: ^expected, signing_region: ^region}} =
               AssumeRole.endpoint(
                 %{"region" => region, "sts_regional_endpoints" => "legacy"},
                 %{},
                 []
               )
    end
  end

  test "aws-global always signs the pinned global endpoint with us-east-1" do
    for mode <- ["regional", "legacy"] do
      assert {:ok, %{endpoint: "https://sts.amazonaws.com", signing_region: "us-east-1"}} =
               AssumeRole.endpoint(
                 %{"region" => "aws-global", "sts_regional_endpoints" => mode},
                 %{},
                 []
               )
    end
  end

  test "STS environment and explicit options retain endpoint region and mode precedence" do
    config = %{"region" => "cn-north-1", "sts_regional_endpoints" => "legacy"}

    env = %{
      "AWS_REGION" => "eu-west-1",
      "AWS_DEFAULT_REGION" => "us-west-2",
      "AWS_STS_REGIONAL_ENDPOINTS" => "regional",
      "AWS_ENDPOINT_URL_STS" => "https://env.example/sts"
    }

    assert {:ok, %{endpoint: "https://env.example/sts", signing_region: "eu-west-1"}} =
             AssumeRole.endpoint(config, env, [])

    assert {:ok, %{endpoint: "http://127.0.0.1:9000/sts", signing_region: "us-iso-east-1"}} =
             AssumeRole.endpoint(config, env,
               sts_endpoint: "http://127.0.0.1:9000/sts",
               region: "us-iso-east-1"
             )

    assert {:ok, %{endpoint: "https://sts.eu-west-1.amazonaws.com", signing_region: "eu-west-1"}} =
             AssumeRole.endpoint(config, Map.delete(env, "AWS_ENDPOINT_URL_STS"), [])

    assert {:ok, %{endpoint: "https://sts.us-west-2.amazonaws.com", signing_region: "us-west-2"}} =
             AssumeRole.endpoint(
               config,
               Map.drop(env, ["AWS_ENDPOINT_URL_STS", "AWS_REGION"]),
               []
             )
  end

  test "STS configured endpoints follow service environment global environment services and profile precedence" do
    config = %{
      "region" => "us-west-2",
      "services" => "local",
      "endpoint_url" => "https://profile.example"
    }

    opts = [
      credential_sections: %{
        "services local" => %{"sts" => %{"endpoint_url" => "https://section.example"}}
      }
    ]

    env = %{
      "AWS_ENDPOINT_URL_STS" => "https://service.example",
      "AWS_ENDPOINT_URL" => "https://global.example"
    }

    assert {:ok, %{endpoint: "https://service.example"}} = AssumeRole.endpoint(config, env, opts)

    assert {:ok, %{endpoint: "https://global.example"}} =
             AssumeRole.endpoint(config, Map.delete(env, "AWS_ENDPOINT_URL_STS"), opts)

    assert {:ok, %{endpoint: "https://section.example"}} = AssumeRole.endpoint(config, %{}, opts)

    for sections <- [
          %{"services local" => %{}},
          %{"services local" => %{"sts" => %{}}},
          %{"services local" => %{"sts" => %{"endpoint_url" => ""}}}
        ] do
      assert {:ok, %{endpoint: "https://profile.example"}} =
               AssumeRole.endpoint(config, %{}, credential_sections: sections)
    end

    assert {:ok, %{endpoint: "https://profile.example"}} =
             AssumeRole.endpoint(Map.delete(config, "services"), %{}, [])

    assert {:ok, %{endpoint: "https://sts.us-west-2.amazonaws.com"}} =
             AssumeRole.endpoint(
               %{"region" => "us-west-2"},
               %{"AWS_ENDPOINT_URL_STS" => "", "AWS_ENDPOINT_URL" => ""},
               endpoint_url: "https://data-command.example"
             )
  end

  test "STS ignore configured endpoints overrides environment and profile endpoints but not explicit STS option" do
    config = %{
      "region" => "us-west-2",
      "endpoint_url" => "https://profile.example",
      "ignore_configured_endpoint_urls" => "true"
    }

    env = %{
      "AWS_ENDPOINT_URL_STS" => "https://service.example",
      "AWS_ENDPOINT_URL" => "https://global.example"
    }

    assert {:ok, %{endpoint: "https://sts.us-west-2.amazonaws.com"}} =
             AssumeRole.endpoint(config, env, [])

    assert {:ok, %{endpoint: "https://explicit.example"}} =
             AssumeRole.endpoint(config, env, sts_endpoint: "https://explicit.example")

    assert {:ok, %{endpoint: "https://service.example"}} =
             AssumeRole.endpoint(
               config,
               Map.put(env, "AWS_IGNORE_CONFIGURED_ENDPOINT_URLS", "false"),
               []
             )

    assert {:ok, %{endpoint: "https://sts.us-west-2.amazonaws.com"}} =
             AssumeRole.endpoint(
               Map.delete(config, "ignore_configured_endpoint_urls"),
               Map.put(env, "AWS_IGNORE_CONFIGURED_ENDPOINT_URLS", "true"),
               []
             )
  end

  test "STS rejects missing or malformed services configuration before transport" do
    config = %{"region" => "us-west-2", "services" => "local"}

    for sections <- [
          %{},
          %{"services local" => %{"sts" => "malformed"}},
          %{"services local" => %{"sts" => %{"endpoint_url" => 42}}}
        ] do
      assert {:error, %Error{code: :invalid_role_config}} =
               AssumeRole.endpoint(config, %{}, credential_sections: sections)
    end

    assert {:ok, %{endpoint: "https://sts.us-west-2.amazonaws.com"}} =
             AssumeRole.endpoint(config, %{"AWS_IGNORE_CONFIGURED_ENDPOINT_URLS" => "true"}, [])
  end

  test "STS FIPS and dualstack policy uses pinned rule variants and environment precedence" do
    config = %{
      "region" => "us-east-1",
      "use_fips_endpoint" => "true",
      "use_dualstack_endpoint" => "true"
    }

    assert {:ok, %{endpoint: "https://sts-fips.us-east-1.api.aws", signing_region: "us-east-1"}} =
             AssumeRole.endpoint(config, %{}, [])

    assert {:ok, %{endpoint: "https://sts.us-east-1.api.aws"}} =
             AssumeRole.endpoint(config, %{"AWS_USE_FIPS_ENDPOINT" => "false"}, [])

    assert {:ok, %{endpoint: "https://sts-fips.us-east-1.amazonaws.com"}} =
             AssumeRole.endpoint(config, %{"AWS_USE_DUALSTACK_ENDPOINT" => "false"}, [])

    assert {:ok, %{endpoint: "https://sts.us-east-1.amazonaws.com"}} =
             AssumeRole.endpoint(
               config,
               %{"AWS_USE_FIPS_ENDPOINT" => "false", "AWS_USE_DUALSTACK_ENDPOINT" => "false"},
               []
             )

    assert {:ok, %{endpoint: "https://custom.example"}} =
             AssumeRole.endpoint(
               config,
               %{"AWS_ENDPOINT_URL_STS" => "https://custom.example"},
               []
             )

    assert {:ok, %{endpoint: "https://sts.us-iso-east-1.api.aws.ic.gov"}} =
             AssumeRole.endpoint(
               %{"region" => "us-iso-east-1", "use_dualstack_endpoint" => "true"},
               %{},
               []
             )
  end

  test "role and web identity use configured STS service over verified TLS independently of data endpoint",
       %{tmp_dir: root} do
    {cert, key, ca} = certificates(root)
    token = Path.join(root, "web-token")
    File.write!(token, "web-token")

    for {action, provider, credential_config} <- [
          {"AssumeRole", :assume_role, "source_profile=base\n"},
          {"AssumeRoleWithWebIdentity", :web_identity, "web_identity_token_file=#{token}\n"}
        ] do
      endpoint = tls_fixture(cert, key, response(action))

      opts =
        files(
          root,
          "[profile role]\nrole_arn=arn:aws:iam::123456789012:role/test\nregion=us-west-2\nservices=local\nuse_fips_endpoint=true\nuse_dualstack_endpoint=true\n#{credential_config}[services local]\nsts=\n endpoint_url=#{endpoint}\n",
          "[base]\naws_access_key_id=source-a\naws_secret_access_key=source-s\n"
        )
        |> Keyword.put(:provider_ca_bundle, ca)
        |> Keyword.put(:metadata_transport_errors, true)

      result = Provider.resolve(%{profile: "role", endpoint_url: "https://data.invalid"}, opts)
      stages = tls_stages()

      assert match?(
               {:ok, %Record{provenance: %{provider: ^provider}},
                %{endpoint_url: "https://data.invalid"}},
               result
             ),
             "STS TLS provider failed; fixture stages: #{inspect(stages)}; result: #{inspect(result)}"

      assert_receive {:sts_tls_request, request}
      assert request =~ "Action=#{action}"

      if provider == :assume_role do
        assert request =~ "Credential=source-a/20261015/us-west-2/sts/aws4_request"
      else
        refute String.downcase(request) =~ "authorization:"
        assert request =~ "WebIdentityToken=web-token"
      end
    end
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

    # This invocation is noninteractive even when the test runner owns a terminal.
    assert ExUnit.CaptureIO.capture_io(fn ->
             assert {:error, %Error{code: :mfa_required}} =
                      Provider.resolve(%{profile: "role"}, opts)
           end) == ""

    refute_received {:wire_request, _}

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

  defp certificates(root) do
    path = &Path.join(root, &1)

    command = fn args ->
      {output, status} =
        System.cmd(System.find_executable("openssl"), args, stderr_to_stdout: true)

      assert status == 0, output
    end

    command.([
      "req",
      "-x509",
      "-newkey",
      "ec",
      "-pkeyopt",
      "ec_paramgen_curve:P-256",
      "-nodes",
      "-keyout",
      path.("ca.key"),
      "-out",
      path.("ca.pem"),
      "-subj",
      "/CN=ESS STS test CA",
      "-days",
      "1"
    ])

    command.([
      "req",
      "-new",
      "-newkey",
      "ec",
      "-pkeyopt",
      "ec_paramgen_curve:P-256",
      "-nodes",
      "-keyout",
      path.("server.key"),
      "-out",
      path.("server.csr"),
      "-subj",
      "/CN=localhost"
    ])

    File.write!(
      path.("server.ext"),
      "subjectAltName=DNS:localhost,IP:127.0.0.1\nextendedKeyUsage=serverAuth\n"
    )

    command.([
      "x509",
      "-req",
      "-in",
      path.("server.csr"),
      "-CA",
      path.("ca.pem"),
      "-CAkey",
      path.("ca.key"),
      "-set_serial",
      "1",
      "-days",
      "1",
      "-extfile",
      path.("server.ext"),
      "-out",
      path.("server.pem")
    ])

    {path.("server.pem"), path.("server.key"), path.("ca.pem")}
  end

  defp tls_fixture(cert, key, body) do
    {:ok, listener} =
      :ssl.listen(0, [
        :binary,
        active: false,
        reuseaddr: true,
        certfile: String.to_charlist(cert),
        keyfile: String.to_charlist(key)
      ])

    {:ok, {_, port}} = :ssl.sockname(listener)
    owner = self()

    peer =
      spawn(fn ->
        try do
          {:ok, socket} =
            tls_step(owner, :accept, fn -> :ssl.transport_accept(listener, 5000) end)

          {:ok, socket} = tls_step(owner, :handshake, fn -> :ssl.handshake(socket, 5000) end)

          try do
            request = tls_step(owner, :request, fn -> tls_request(socket, "") end)
            send(owner, {:sts_tls_request, request})

            :ok =
              tls_step(owner, :response, fn ->
                :ssl.send(
                  socket,
                  "HTTP/1.1 200 OK\r\nContent-Length: #{byte_size(body)}\r\nConnection: close\r\n\r\n#{body}"
                )
              end)
          after
            :ssl.close(socket)
          end
        after
          :ssl.close(listener)
        end
      end)

    on_exit(fn ->
      :ssl.close(listener)
      if Process.alive?(peer), do: Process.exit(peer, :kill)
    end)

    "https://127.0.0.1:#{port}"
  end

  defp tls_step(owner, stage, action) do
    started = System.monotonic_time(:millisecond)
    send(owner, {:sts_tls_stage, stage, :started})
    result = action.()

    outcome =
      case result do
        {:error, reason} -> {:error, reason}
        _ -> :ok
      end

    send(owner, {:sts_tls_stage, stage, {outcome, System.monotonic_time(:millisecond) - started}})
    result
  end

  defp tls_stages(stages \\ []) do
    receive do
      {:sts_tls_stage, stage, result} -> tls_stages([{stage, result} | stages])
    after
      0 -> Enum.reverse(stages)
    end
  end

  defp tls_request(socket, data) do
    case String.split(data, "\r\n\r\n", parts: 2) do
      [head, body] ->
        [_, length] = Regex.run(~r/\r\ncontent-length:\s*(\d+)/i, head)

        if byte_size(body) >= String.to_integer(length),
          do: data,
          else: tls_read(socket, data)

      [_] ->
        tls_read(socket, data)
    end
  end

  defp tls_read(socket, data) do
    {:ok, chunk} = :ssl.recv(socket, 0, 5000)
    tls_request(socket, data <> chunk)
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
