Code.require_file("../../support/wire_server.exs", __DIR__)

defmodule ExStorageServiceCli.Credentials.SSOTest do
  use ExUnit.Case, async: true
  alias ExStorageServiceCli.Credentials.{Provider, Record}
  alias ExStorageServiceCli.{Error, Test.WireServer}
  @moduletag :tmp_dir
  @now ~U[2026-10-15 00:00:00Z]

  setup_all do
    {:ok, _} = Application.ensure_all_started(:http_fetch)
    :ok
  end

  test "legacy cached login gets bounded role credentials with bearer header", %{tmp_dir: root} do
    opts =
      files(
        root,
        "[default]\nsso_start_url=https://login.example/start\nsso_region=eu-west-1\nsso_account_id=123456789012\nsso_role_name=reader\n"
      )

    cache(root, "https://login.example/start", %{
      "accessToken" => "sso-marker",
      "expiresAt" => "2026-10-15T01:00:00Z"
    })

    {endpoint, _} = WireServer.fixture([{200, [], response()}])

    assert {:ok,
            %Record{
              access_key_id: "sso-a",
              session_token: "sso-t",
              provenance: %{provider: :sso}
            }, _} = Provider.resolve(%{}, Keyword.put(opts, :sso_endpoint, endpoint))

    assert_receive {:wire_request,
                    %{target: target, headers: %{"x-amz-sso_bearer_token" => "sso-marker"}}}

    assert URI.parse(target).path == "/federation/credentials"

    assert URI.decode_query(URI.parse(target).query) == %{
             "account_id" => "123456789012",
             "role_name" => "reader"
           }
  end

  test "session token refresh uses OIDC and atomic owner-only cache before role request", %{
    tmp_dir: root
  } do
    opts =
      files(
        root,
        "[default]\nsso_session=work\nsso_account_id=123456789012\nsso_role_name=reader\n[sso-session work]\nsso_start_url=https://login.example/start\nsso_region=eu-west-1\n"
      )

    path =
      cache(root, "work", %{
        "accessToken" => "expired-marker",
        "expiresAt" => "2026-10-14T23:00:00Z",
        "refreshToken" => "refresh-marker",
        "clientId" => "client",
        "clientSecret" => "client-secret",
        "registrationExpiresAt" => "2026-10-16T00:00:00Z"
      })

    {oidc, _} =
      WireServer.fixture([
        {200, [],
         JSON.encode!(%{
           "accessToken" => "refreshed-marker",
           "expiresIn" => 3600,
           "refreshToken" => "new-refresh"
         })}
      ])

    {sso, _} = WireServer.fixture([{200, [], response()}])

    assert {:ok, %Record{}, _} =
             Provider.resolve(
               %{},
               opts |> Keyword.put(:oidc_endpoint, oidc) |> Keyword.put(:sso_endpoint, sso)
             )

    assert_receive {:wire_request, %{target: "/token", body: body, headers: headers}}
    refute Map.has_key?(headers, "authorization")
    assert JSON.decode!(body)["grantType"] == "refresh_token"
    assert JSON.decode!(body)["refreshToken"] == "refresh-marker"
    assert_receive {:wire_request, %{headers: %{"x-amz-sso_bearer_token" => "refreshed-marker"}}}
    assert JSON.decode!(File.read!(path))["refreshToken"] == "new-refresh"
    assert {:ok, stat} = File.stat(path)
    assert Bitwise.band(stat.mode, 0o777) == 0o600
    assert Path.wildcard(path <> ".ess-*") == []
  end

  test "missing and expired legacy logins fail without lower credential fallback", %{
    tmp_dir: root
  } do
    opts =
      files(
        root,
        "[default]\nsso_start_url=https://login.example/start\nsso_region=eu-west-1\nsso_account_id=123456789012\nsso_role_name=reader\n"
      )

    assert {:error, %Error{code: :sso_login_required}} = Provider.resolve(%{}, opts)

    cache(root, "https://login.example/start", %{
      "accessToken" => "expired-secret",
      "expiresAt" => "2020-01-01T00:00:00Z"
    })

    assert {:error, %Error{code: :sso_login_required} = error} = Provider.resolve(%{}, opts)
    refute inspect(error) =~ "expired-secret"
  end

  test "explicit device login surfaces URL code and saves refreshed session atomically", %{
    tmp_dir: root
  } do
    opts =
      files(
        root,
        "[default]\nsso_session=work\nsso_account_id=123456789012\nsso_role_name=reader\n[sso-session work]\nsso_start_url=https://login.example/start\nsso_region=eu-west-1\n"
      )

    registration =
      JSON.encode!(%{
        "clientId" => "client",
        "clientSecret" => "registration-secret",
        "clientSecretExpiresAt" => DateTime.to_unix(~U[2026-10-16 00:00:00Z])
      })

    device =
      JSON.encode!(%{
        "deviceCode" => "device-secret",
        "verificationUri" => "https://verify.example",
        "verificationUriComplete" => "https://verify.example/?code=display-code",
        "userCode" => "display-code",
        "expiresIn" => 60,
        "interval" => 1
      })

    success =
      JSON.encode!(%{
        "accessToken" => "login-token",
        "expiresIn" => 3600,
        "refreshToken" => "refresh-token"
      })

    {endpoint, _} =
      WireServer.fixture([
        {200, [], registration},
        {200, [], device},
        {400, [], JSON.encode!(%{"error" => "authorization_pending"})},
        {200, [], success}
      ])

    parent = self()

    assert :ok =
             Provider.login(
               %{profile: "default"},
               opts
               |> Keyword.put(:use_device_code, true)
               |> Keyword.put(:oidc_endpoint, endpoint)
               |> Keyword.put(:no_browser, false)
               |> Keyword.put(:open_browser, fn url -> send(parent, {:opened, url}) end)
               |> Keyword.put(:on_verification, fn data -> send(parent, {:verification, data}) end)
             )

    assert_receive {:verification,
                    %{verification_uri: "https://verify.example", user_code: "display-code"} =
                      verification}

    refute inspect(verification) =~ "device-secret"
    assert_receive {:opened, "https://verify.example/?code=display-code"}
    assert_receive {:wire_request, %{target: "/client/register"}}
    assert_receive {:wire_request, %{target: "/device_authorization"}}
    assert_receive {:wire_request, %{target: "/token", body: body}}
    assert JSON.decode!(body)["grantType"] == "urn:ietf:params:oauth:grant-type:device_code"
    assert_receive {:wire_request, %{target: "/token"}}
    [path] = Path.wildcard(Path.join([root, ".aws", "sso", "cache", "*.json"]))
    assert JSON.decode!(File.read!(path))["accessToken"] == "login-token"
    assert Bitwise.band(File.stat!(path).mode, 0o777) == 0o600
  end

  test "invalid login deadline and malicious cached scalar fields fail explicitly", %{
    tmp_dir: root
  } do
    opts =
      files(
        root,
        "[default]\nsso_start_url=https://login.example/start\nsso_region=eu-west-1\nsso_account_id=123456789012\nsso_role_name=reader\n"
      )

    assert {:error, %Error{code: :invalid_sso_login}} =
             Provider.login(%{}, Keyword.put(opts, :login_timeout, 0))

    cache(root, "https://login.example/start", %{
      "accessToken" => "secret-marker",
      "expiresAt" => %{"secret" => "secret-marker"}
    })

    assert {:error, %Error{code: :sso_login_required} = error} = Provider.resolve(%{}, opts)
    refute inspect(error) =~ "secret-marker"
  end

  test "PKCE login validates loopback state and exchanges an independent S256 verifier", %{
    tmp_dir: root
  } do
    opts =
      files(
        root,
        "[default]\nsso_session=work\n[sso-session work]\nsso_start_url=https://login.example/start\nsso_region=eu-west-1\nsso_registration_scopes=sso:account:access,custom:scope\n"
      )

    {endpoint, _} =
      WireServer.fixture([
        {200, [], registration()},
        {200, [],
         JSON.encode!(%{
           "accessToken" => "login-token",
           "expiresIn" => 3600,
           "refreshToken" => "refresh-token"
         })}
      ])

    parent = self()

    callback = fn %{verification_uri: url, user_code: nil} ->
      parameters = URI.decode_query(URI.parse(url).query)
      send(parent, {:authorization, parameters})
      browser_callback(parameters, parameters["state"])
    end

    assert :ok =
             Provider.login(
               %{},
               opts
               |> Keyword.put(:oidc_endpoint, endpoint)
               |> Keyword.put(:on_verification, callback)
             )

    assert_receive {:authorization, parameters}
    assert parameters["scopes"] == "sso:account:access custom:scope"
    assert_receive {:wire_request, %{target: "/client/register", body: registration}}
    assert JSON.decode!(registration)["redirectUris"] == ["http://127.0.0.1/oauth/callback"]
    assert JSON.decode!(registration)["grantTypes"] == ["authorization_code", "refresh_token"]
    assert_receive {:wire_request, %{target: "/token", body: token}}
    token = JSON.decode!(token)
    assert token["grantType"] == "authorization_code"
    assert token["code"] == "callback-secret"
    assert token["redirectUri"] == parameters["redirect_uri"]
    assert byte_size(token["codeVerifier"]) == 64

    assert Base.url_encode64(:crypto.hash(:sha256, token["codeVerifier"]), padding: false) ==
             parameters["code_challenge"]

    [path] = Path.wildcard(Path.join([root, ".aws", "sso", "cache", "*.json"]))
    assert JSON.decode!(File.read!(path))["accessToken"] == "login-token"
    assert Bitwise.band(File.stat!(path).mode, 0o777) == 0o600
    refute File.read!(path) =~ "callback-secret"
    refute File.read!(path) =~ token["codeVerifier"]
  end

  test "PKCE mismatch and deadline refuse credentials and close callback listener", %{
    tmp_dir: root
  } do
    opts =
      files(root, "[default]\nsso_start_url=https://login.example/start\nsso_region=eu-west-1\n")

    for behavior <- [:wrong_state, :timeout] do
      {endpoint, _} = WireServer.fixture([{200, [], registration()}])
      parent = self()

      callback = fn %{verification_uri: url} ->
        parameters = URI.decode_query(URI.parse(url).query)
        send(parent, {:redirect, URI.parse(parameters["redirect_uri"])})
        if behavior == :wrong_state, do: browser_callback(parameters, "wrong-state")
      end

      expected = if behavior == :wrong_state, do: :sso_login_refused, else: :sso_login_timeout

      assert {:error, %Error{code: ^expected} = error} =
               Provider.login(
                 %{},
                 opts
                 |> Keyword.put(:oidc_endpoint, endpoint)
                 |> Keyword.put(:login_timeout, 100)
                 |> Keyword.put(:on_verification, callback)
               )

      refute inspect(error) =~ "callback-secret"
      assert_receive {:redirect, redirect}

      assert {:error, :econnrefused} =
               :gen_tcp.connect({127, 0, 0, 1}, redirect.port, [:binary, active: false], 200)

      refute_receive {:wire_request, %{target: "/token"}}
    end

    assert Path.wildcard(Path.join([root, ".aws", "sso", "cache", "*.json"])) == []
  end

  defp registration,
    do:
      JSON.encode!(%{
        "clientId" => "client",
        "clientSecret" => "registration-secret",
        "clientSecretExpiresAt" => DateTime.to_unix(~U[2026-10-16 00:00:00Z])
      })

  defp browser_callback(parameters, state) do
    redirect = URI.parse(parameters["redirect_uri"])

    spawn(fn ->
      {:ok, socket} =
        :gen_tcp.connect({127, 0, 0, 1}, redirect.port, [:binary, active: false], 1_000)

      target =
        redirect.path <> "?" <> URI.encode_query(%{"code" => "callback-secret", "state" => state})

      :ok =
        :gen_tcp.send(
          socket,
          "GET " <>
            target <> " HTTP/1.1\r\nHost: 127.0.0.1:#{redirect.port}\r\nConnection: close\r\n\r\n"
        )

      :gen_tcp.recv(socket, 0, 1_000)
      :gen_tcp.close(socket)
    end)
  end

  defp response,
    do:
      JSON.encode!(%{
        "roleCredentials" => %{
          "accessKeyId" => "sso-a",
          "secretAccessKey" => "sso-s",
          "sessionToken" => "sso-t",
          "expiration" => DateTime.to_unix(~U[2026-10-15 01:00:00Z], :millisecond)
        }
      })

  defp cache(root, key, token) do
    dir = Path.join([root, ".aws", "sso", "cache"])
    File.mkdir_p!(dir)
    path = Path.join(dir, Base.encode16(:crypto.hash(:sha, key), case: :lower) <> ".json")
    File.write!(path, JSON.encode!(token))
    path
  end

  defp files(root, config) do
    path = Path.join(root, "config")
    File.write!(path, config)

    [
      env: %{},
      home: root,
      config_file: path,
      credentials_file: Path.join(root, "missing"),
      no_browser: true,
      now: @now
    ]
  end
end
