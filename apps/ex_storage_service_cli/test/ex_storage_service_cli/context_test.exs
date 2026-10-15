defmodule ExStorageServiceCli.ContextTest do
  use ExUnit.Case, async: true
  alias ExStorageServiceCli.{Context, Error, Target}
  alias ExStorageServiceCli.Credentials.Record
  @moduletag :tmp_dir

  test "AWS defaults use regional endpoints and a coherent temporary credential record", %{
    tmp_dir: root
  } do
    opts =
      files(
        root,
        "[default]\nregion=eu-west-1\n",
        "[default]\naws_access_key_id=a\naws_secret_access_key=s\naws_session_token=t\n"
      )

    assert {:ok,
            %Context{
              frontend: :aws,
              endpoint: "https://s3.eu-west-1.amazonaws.com",
              region: "eu-west-1",
              addressing: :virtual,
              session_token: "t",
              credentials: %Record{session_token: "t"},
              tls: [verify: :verify_peer]
            }} = Context.aws(%{}, opts)

    assert {:ok, %Context{endpoint: "http://localhost:9100/base", addressing: :path}} =
             Context.aws(%{endpoint_url: "http://localhost:9100/base"}, opts)

    assert {:ok, %Context{endpoint: "https://s3.cn-north-1.amazonaws.com.cn"}} =
             Context.aws(%{region: "cn-north-1"}, opts)
  end

  test "AWS profile TLS addressing and timeouts remain per-context", %{tmp_dir: root} do
    ca = Path.join(root, "ca.pem")
    File.write!(ca, "fixture")

    opts =
      files(
        root,
        "[default]\nca_bundle=#{ca}\ns3=\n addressing_style=virtual\n",
        "[default]\naws_access_key_id=a\naws_secret_access_key=s\n"
      )

    assert {:ok,
            %Context{
              addressing: :virtual,
              timeout: :infinity,
              connect_timeout: 3000,
              tls: [verify: :verify_peer, cacertfile: path]
            }} =
             Context.aws(
               %{
                 endpoint_url: "https://custom.test",
                 cli_read_timeout: 0,
                 cli_connect_timeout: 3
               },
               opts
             )

    assert path == String.to_charlist(ca)

    assert {:ok, %Context{tls: [verify: :verify_none]}} =
             Context.aws(%{no_verify_ssl: true}, opts)

    assert {:error, %Error{category: :configuration}} = Context.aws(%{cli_read_timeout: -1}, opts)

    assert {:error, %Error{category: :configuration}} =
             Context.aws(%{ca_bundle: Path.join(root, "missing")}, opts)

    File.write!(opts[:config_file], "[default]\ns3=\n addressing_style=invalid\n")
    assert {:error, %Error{category: :configuration}} = Context.aws(%{}, opts)
  end

  test "unsigned AWS avoids credential processes and malformed credential files", %{tmp_dir: root} do
    opts =
      files(root, "[default]\noutput = json\n", "malformed")
      |> Keyword.put(:env, %{
        "AWS_REGION" => "us-west-2",
        "AWS_ENDPOINT_URL_S3" => "https://public.test"
      })

    assert {:ok,
            %Context{
              unsigned: true,
              credentials: nil,
              endpoint: "https://public.test",
              region: "us-west-2"
            }} = Context.aws(%{no_sign_request: true}, opts)

    File.write!(opts[:config_file], "malformed")

    assert {:error, %Error{code: :invalid_shared_config}} =
             Context.aws(%{no_sign_request: true}, opts)
  end

  test "native explicit pairs never inherit a profile token and partial pairs fail" do
    config = %{
      "default" => %{
        "access_key_id" => "profile-a",
        "secret_access_key" => "profile-s",
        "session_token" => "profile-t",
        "endpoint" => "https://ess.test"
      },
      "aliases" => %{}
    }

    assert {:ok,
            %Context{
              access_key_id: "explicit-a",
              secret_access_key: "explicit-s",
              session_token: nil
            }} =
             Context.native(nil, %{access_key: "explicit-a", secret_key: "explicit-s"}, config)

    assert {:ok, %Context{session_token: "explicit-t"}} =
             Context.native(
               nil,
               %{access_key: "explicit-a", secret_key: "explicit-s", session_token: "explicit-t"},
               config
             )

    assert {:error, %Error{code: :invalid_credentials}} =
             Context.native(nil, %{access_key: "explicit-a"}, config)

    assert {:error, %Error{category: :configuration}} =
             Context.native(nil, %{session_token: "orphan"}, config)

    assert {:ok, %Context{unsigned: true, credentials: nil, access_key_id: nil}} =
             Context.native(nil, %{no_sign_request: true}, config)
  end

  test "native environment provider rejects incomplete pairs without mixing ESS credentials" do
    config = %{
      "profiles" => %{},
      "aliases" => %{"env" => %{"endpoint" => "https://env.test", "provider" => "environment"}}
    }

    target = %Target{alias: "env"}

    assert {:error, %Error{code: :invalid_credentials}} =
             Context.native(target, %{}, config, env: %{"AWS_ACCESS_KEY_ID" => "a"})

    assert {:ok, %Context{access_key_id: "a", session_token: "t"}} =
             Context.native(target, %{}, config,
               env: %{
                 "AWS_ACCESS_KEY_ID" => "a",
                 "AWS_SECRET_ACCESS_KEY" => "s",
                 "AWS_SESSION_TOKEN" => "t"
               }
             )
  end

  test "native AWS aliases explicitly select AWS profile independently of ESS", %{tmp_dir: root} do
    opts =
      files(
        root,
        "[profile team]\nregion=ap-south-1\n",
        "[team]\naws_access_key_id=aws-a\naws_secret_access_key=aws-s\naws_session_token=aws-t\n"
      )
      |> Keyword.put(:env, %{"AWS_ACCESS_KEY_ID" => "wrong", "AWS_SECRET_ACCESS_KEY" => "wrong"})

    config = %{
      "default" => %{"access_key_id" => "ess-a", "secret_access_key" => "ess-s"},
      "aliases" => %{
        "aws" => %{
          "endpoint" => "https://alias.test",
          "provider" => "aws-profile",
          "profile" => "team"
        }
      }
    }

    assert {:ok,
            %Context{
              endpoint: "https://alias.test",
              region: "ap-south-1",
              profile: "team",
              access_key_id: "aws-a",
              session_token: "aws-t"
            }} = Context.native(%Target{alias: "aws"}, %{}, config, opts)
  end

  test "invalid endpoints fail without exposing supplied credentials" do
    for endpoint <- [
          "ftp://example.test",
          "https://secret@example.test",
          "https://example.test?secret",
          "https://example.test:70000",
          "https://example.test:invalid"
        ] do
      assert {:error, %Error{} = error} = Context.validate_endpoint(endpoint)
      refute inspect(error) =~ "secret"
    end
  end

  test "Context inspection excludes credentials and secret-bearing options" do
    context = %Context{
      access_key_id: "access-marker",
      secret_access_key: "secret-marker",
      session_token: "token-marker",
      options: %{secret_key: "options-marker"}
    }

    for marker <- ~w(access-marker secret-marker token-marker options-marker),
        do: refute(inspect(context) =~ marker)
  end

  defp files(root, config, credentials) do
    config_file = Path.join(root, "config")
    credentials_file = Path.join(root, "credentials")
    File.write!(config_file, config)
    File.write!(credentials_file, credentials)
    [env: %{}, home: root, config_file: config_file, credentials_file: credentials_file]
  end
end
