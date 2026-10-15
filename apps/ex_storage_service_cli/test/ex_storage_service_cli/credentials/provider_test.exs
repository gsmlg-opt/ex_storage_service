defmodule ExStorageServiceCli.Credentials.ProviderTest do
  use ExUnit.Case, async: true
  alias ExStorageServiceCli.Credentials.{Provider, Record}
  alias ExStorageServiceCli.Error
  @moduletag :tmp_dir

  test "AWS_PROFILE keeps complete environment credentials while explicit profile disables them",
       %{tmp_dir: root} do
    opts =
      files(
        root,
        "[profile team]\nregion=us-west-2\ns3=\n addressing_style=virtual\n",
        "[team]\naws_access_key_id=profile-access\naws_secret_access_key=profile-secret\naws_session_token=profile-token\n"
      )

    env = %{
      "AWS_PROFILE" => "team",
      "AWS_ACCESS_KEY_ID" => "env-access",
      "AWS_SECRET_ACCESS_KEY" => "env-secret",
      "AWS_SESSION_TOKEN" => "env-token",
      "AWS_REGION" => "eu-west-1"
    }

    assert {:ok, environment_credentials, settings} =
             Provider.resolve(%{}, Keyword.put(opts, :env, env))

    assert %Record{
             access_key_id: "env-access",
             secret_access_key: "env-secret",
             session_token: "env-token",
             provenance: %{provider: :environment}
           } = environment_credentials

    assert %{region: "eu-west-1", profile: "team", s3: %{"addressing_style" => "virtual"}} =
             settings

    assert {:ok, profile_credentials, %{region: "eu-west-1"}} =
             Provider.resolve(%{profile: "team"}, Keyword.put(opts, :env, env))

    assert %Record{
             access_key_id: "profile-access",
             session_token: "profile-token",
             provenance: %{provider: :shared_credentials}
           } = profile_credentials
  end

  test "partial credential pairs and tokens never mix between files or environment", %{
    tmp_dir: root
  } do
    opts =
      files(
        root,
        "[default]\naws_access_key_id=config-access\naws_secret_access_key=config-secret\naws_session_token=config-token\n",
        "[default]\naws_access_key_id=stored-access\naws_secret_access_key=stored-secret\n"
      )

    assert {:ok, credentials, _} =
             Provider.resolve(%{}, Keyword.put(opts, :env, %{"AWS_SESSION_TOKEN" => "env-token"}))

    assert %Record{
             access_key_id: "stored-access",
             secret_access_key: "stored-secret",
             session_token: nil
           } = credentials

    assert {:error, %Error{code: :invalid_credentials}} =
             Provider.resolve(
               %{},
               Keyword.put(opts, :env, %{"AWS_ACCESS_KEY_ID" => "env-access"})
             )

    File.write!(opts[:credentials_file], "[default]\naws_access_key_id=stored-access\n")
    assert {:error, %Error{code: :invalid_credentials}} = Provider.resolve(%{}, opts)
  end

  test "endpoint and region precedence respects service settings and configured endpoint bypass",
       %{tmp_dir: root} do
    opts =
      files(
        root,
        "[default]\nregion=ap-south-1\nendpoint_url=https://global.example\nservices=local\n[services local]\ns3=\n endpoint_url=http://localhost:9100\n",
        "[default]\naws_access_key_id=a\naws_secret_access_key=s\n"
      )

    assert {:ok, _, %{region: "ap-south-1", endpoint_url: "http://localhost:9100"}} =
             Provider.resolve(%{}, opts)

    env = %{
      "AWS_ENDPOINT_URL_S3" => "https://service.example",
      "AWS_ENDPOINT_URL" => "https://env.example",
      "AWS_DEFAULT_REGION" => "us-east-2"
    }

    assert {:ok, _, %{endpoint_url: "https://service.example", region: "us-east-2"}} =
             Provider.resolve(%{}, Keyword.put(opts, :env, env))

    assert {:ok, _, %{endpoint_url: "https://explicit.example", region: "eu-west-2"}} =
             Provider.resolve(
               %{endpoint_url: "https://explicit.example", region: "eu-west-2"},
               Keyword.put(opts, :env, env)
             )

    assert {:ok, _, %{endpoint_url: nil}} =
             Provider.resolve(
               %{},
               Keyword.put(
                 opts,
                 :env,
                 Map.put(env, "AWS_IGNORE_CONFIGURED_ENDPOINT_URLS", "true")
               )
             )
  end

  test "missing profiles and malformed config fail for signed and unsigned settings", %{
    tmp_dir: root
  } do
    opts = files(root, "[default]\nregion=us-east-1\n", "")

    assert {:error, %Error{code: :profile_not_found}} =
             Provider.resolve(%{profile: "missing"}, opts)

    File.write!(opts[:config_file], "malformed secret")
    assert {:error, %Error{code: :invalid_shared_config}} = Provider.resolve(%{}, opts)

    assert {:error, %Error{code: :invalid_shared_config}} =
             Provider.resolve(%{no_sign_request: true, profile: "missing"}, opts)

    assert File.read!(opts[:config_file]) == "malformed secret"
  end

  test "selected invalid SSO does not use lower static credentials", %{tmp_dir: root} do
    opts =
      files(
        root,
        "[default]\nsso_session=my-session\n",
        "[default]\naws_access_key_id=a\naws_secret_access_key=s\n"
      )

    assert {:error, %Error{code: :invalid_sso_config}} =
             Provider.resolve(%{}, opts)

    opts = files(root, "", "")

    assert {:error, %Error{code: :credentials_not_found}} =
             Provider.resolve(
               %{},
               Keyword.put(opts, :env, %{"AWS_EC2_METADATA_DISABLED" => "true"})
             )
  end

  test "configured process precedes config static credentials and shared credentials precede process",
       %{tmp_dir: root} do
    helper = Path.join(root, "credential-helper")
    marker = Path.join(root, "called")

    File.write!(
      helper,
      "#!/bin/sh\nprintf called > '#{marker}'\nprintf '%s' '{\"Version\":1,\"AccessKeyId\":\"process-access\",\"SecretAccessKey\":\"process-secret\"}'\n"
    )

    File.chmod!(helper, 0o700)

    opts =
      files(
        root,
        "[default]\ncredential_process=#{helper}\naws_access_key_id=config-access\naws_secret_access_key=config-secret\n",
        "[default]\naws_access_key_id=stored-access\naws_secret_access_key=stored-secret\n"
      )

    assert {:ok, %Record{access_key_id: "stored-access"}, _} = Provider.resolve(%{}, opts)
    refute File.exists?(marker)
    File.write!(opts[:credentials_file], "")

    assert {:ok,
            %Record{
              access_key_id: "process-access",
              provenance: %{provider: :credential_process}
            }, _} = Provider.resolve(%{}, opts)

    assert File.read!(marker) == "called"
  end

  test "selected default must exist while implicit default may be absent", %{tmp_dir: root} do
    opts = files(root, "", "")
    env = %{"AWS_ACCESS_KEY_ID" => "a", "AWS_SECRET_ACCESS_KEY" => "s"}
    assert {:ok, _, _} = Provider.resolve(%{}, Keyword.put(opts, :env, env))

    assert {:error, %Error{code: :profile_not_found}} =
             Provider.resolve(%{profile: "default"}, opts)

    assert {:error, %Error{code: :profile_not_found}} =
             Provider.resolve(
               %{},
               Keyword.put(opts, :env, Map.put(env, "AWS_PROFILE", "default"))
             )
  end

  test "nested values for scalar settings fail safely", %{tmp_dir: root} do
    for key <- ~w(region services ignore_configured_endpoint_urls aws_access_key_id) do
      opts =
        files(
          root,
          "[default]\n#{key}=\n nested=secret\n",
          "[default]\naws_access_key_id=a\naws_secret_access_key=s\n"
        )

      assert {:error, %Error{code: :invalid_shared_config} = error} = Provider.resolve(%{}, opts)
      refute inspect(error) =~ "secret"
    end
  end

  test "unsigned mode keeps environment endpoint and bypasses unreadable credentials", %{
    tmp_dir: root
  } do
    opts =
      files(root, "", "malformed")
      |> Keyword.put(:env, %{
        "AWS_ENDPOINT_URL_S3" => "https://unsigned.test",
        "AWS_CA_BUNDLE" => "/tmp/ca"
      })

    assert {:ok, nil, %{endpoint_url: "https://unsigned.test", ca_bundle: "/tmp/ca"}} =
             Provider.resolve(%{no_sign_request: true}, opts)
  end

  test "unsigned profiles retain nonsecret settings without starting credential providers", %{
    tmp_dir: root
  } do
    opts =
      files(
        root,
        """
        [profile team]
        region=eu-west-2
        ca_bundle=/profile/ca
        services=local
        credential_process=/must/not/run
        output=yaml
        cli_binary_format=raw-in-base64-out
        retry_mode=adaptive
        max_attempts=7
        cli_read_timeout=45
        cli_connect_timeout=15
        use_fips_endpoint=true
        use_dualstack_endpoint=false
        request_checksum_calculation=when_supported
        response_checksum_validation=when_required
        s3=
          addressing_style=path
          multipart_threshold=8MB
        [services local]
        s3=
          endpoint_url=https://profile.example
        """,
        "malformed credentials must not be parsed"
      )

    assert {:ok, nil,
            %{
              unsigned: true,
              profile: "team",
              region: "eu-west-2",
              ca_bundle: "/profile/ca",
              endpoint_url: "https://profile.example",
              s3: %{"addressing_style" => "path", "multipart_threshold" => "8MB"},
              options: settings
            }} = Provider.resolve(%{no_sign_request: true, profile: "team"}, opts)

    assert settings == %{
             output: "yaml",
             cli_binary_format: "raw-in-base64-out",
             retry_mode: "adaptive",
             max_attempts: 7,
             cli_read_timeout: 45,
             cli_connect_timeout: 15,
             use_fips_endpoint: true,
             use_dualstack_endpoint: false,
             request_checksum_calculation: "when_supported",
             response_checksum_validation: "when_required"
           }

    opts =
      Keyword.put(opts, :env, %{
        "AWS_DEFAULT_OUTPUT" => "text",
        "AWS_MAX_ATTEMPTS" => "4",
        "AWS_USE_DUALSTACK_ENDPOINT" => "true"
      })

    assert {:ok, nil,
            %{
              options: %{
                output: "text",
                max_attempts: 4,
                use_fips_endpoint: false,
                use_dualstack_endpoint: true
              }
            }} =
             Provider.resolve(
               %{no_sign_request: true, profile: "team", use_fips_endpoint: false},
               opts
             )

    assert {:error, %Error{code: :profile_not_found}} =
             Provider.resolve(%{no_sign_request: true, profile: "missing"}, opts)
  end

  test "invalid nonsecret shared settings fail without leaking values", %{tmp_dir: root} do
    for {key, value} <- [
          {"output", "secret"},
          {"cli_read_timeout", "-1"},
          {"max_attempts", "0"},
          {"use_fips_endpoint", "wrong"},
          {"retry_mode", "legacy"},
          {"retry_mode", "unknown"}
        ] do
      opts = files(root, "[default]\n#{key}=#{value}\n", "malformed")

      assert {:error, %Error{code: :invalid_shared_config} = error} =
               Provider.resolve(%{no_sign_request: true}, opts)

      refute inspect(error) =~ "secret"
    end
  end

  defp files(root, config, credentials) do
    config_file = Path.join(root, "config")
    credentials_file = Path.join(root, "credentials")
    File.write!(config_file, config)
    File.write!(credentials_file, credentials)
    [env: %{}, home: root, config_file: config_file, credentials_file: credentials_file]
  end
end
