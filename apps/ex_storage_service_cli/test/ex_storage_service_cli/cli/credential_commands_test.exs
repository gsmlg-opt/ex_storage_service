Code.require_file("../../support/wire_server.exs", __DIR__)

defmodule ExStorageServiceCli.CLI.CredentialCommandsTest do
  use ExUnit.Case, async: true
  import ExUnit.CaptureIO
  alias ExStorageServiceCli.CLI.CredentialCommands
  alias ExStorageServiceCli.{Error, Test.WireServer}
  @moduletag :tmp_dir

  setup_all do
    {:ok, _} = Application.ensure_all_started(:http_fetch)
    :ok
  end

  test "login requires an explicit profile and prints only a nonsecret success result", %{
    tmp_dir: root
  } do
    opts = files(root)

    assert {:error, %Error{code: :sso_profile_required}} =
             CredentialCommands.run("login", %{}, opts)

    {endpoint, _} =
      WireServer.fixture([
        {200, [],
         JSON.encode!(%{
           "clientId" => "client",
           "clientSecret" => "registration-secret",
           "clientSecretExpiresAt" => DateTime.to_unix(~U[2099-01-01 00:00:00Z])
         })},
        {200, [],
         JSON.encode!(%{
           "deviceCode" => "device-secret",
           "verificationUri" => "https://verify.example",
           "userCode" => "visible-code",
           "expiresIn" => 60
         })},
        {200, [],
         JSON.encode!(%{
           "accessToken" => "access-secret",
           "refreshToken" => "refresh-secret",
           "expiresIn" => 3600
         })}
      ])

    opts =
      opts
      |> Keyword.put(:oidc_endpoint, endpoint)
      |> Keyword.put(:on_verification, fn _ -> :ok end)

    output =
      capture_io(fn ->
        assert :ok =
                 CredentialCommands.run(
                   "login",
                   %{profile: "work", use_device_code: true, no_browser: true, json: true},
                   opts
                 )
      end)

    assert JSON.decode!(output) == %{"status" => "ok", "operation" => "login"}
    refute output =~ "secret"
    [path] = Path.wildcard(Path.join([root, ".aws", "sso", "cache", "*.json"]))
    assert JSON.decode!(File.read!(path))["accessToken"] == "access-secret"
  end

  test "logout deletes only the selected session and remains idempotent", %{tmp_dir: root} do
    opts = files(root)
    directory = Path.join([root, ".aws", "sso", "cache"])
    File.mkdir_p!(directory)
    path = Path.join(directory, hash("work-session") <> ".json")
    unrelated = Path.join(directory, "unrelated.json")
    File.write!(path, "selected-secret")
    File.write!(unrelated, "unrelated-secret")

    output =
      capture_io(fn ->
        assert :ok = CredentialCommands.run("logout", %{profile: "work", json: true}, opts)
      end)

    assert JSON.decode!(output) == %{"status" => "ok", "operation" => "logout"}
    refute output =~ "secret"
    refute File.exists?(path)
    assert File.read!(unrelated) == "unrelated-secret"
    assert File.dir?(directory)

    assert capture_io(fn ->
             assert :ok = CredentialCommands.run("logout", %{profile: "work", quiet: true}, opts)
           end) == ""

    assert {:error, %Error{code: :profile_not_found}} =
             CredentialCommands.run("logout", %{profile: "missing"}, opts)

    assert File.read!(unrelated) == "unrelated-secret"
  end

  test "logout refuses token and cache-directory symlinks and preserves their targets", %{
    tmp_dir: root
  } do
    opts = files(root)
    directory = Path.join([root, ".aws", "sso", "cache"])
    File.mkdir_p!(directory)
    target = Path.join(root, "target")
    File.write!(target, "keep-secret")
    link = Path.join(directory, hash("work-session") <> ".json")
    File.ln_s!(target, link)

    assert {:error, %Error{code: :sso_cache_delete_failed}} =
             CredentialCommands.run("logout", %{profile: "work"}, opts)

    assert File.read!(target) == "keep-secret"
    File.rm!(link)
    File.rmdir!(directory)
    outside = Path.join(root, "outside")
    File.mkdir!(outside)
    File.ln_s!(outside, directory)

    assert {:error, %Error{code: :sso_cache_delete_failed}} =
             CredentialCommands.run("logout", %{profile: "work"}, opts)

    assert File.dir?(outside)
  end

  defp hash(value), do: :crypto.hash(:sha, value) |> Base.encode16(case: :lower)

  defp files(root) do
    config = Path.join(root, "config")
    credentials = Path.join(root, "credentials")

    File.write!(
      config,
      "[profile work]\nsso_session=work-session\n[sso-session work-session]\nsso_start_url=https://login.example/start\nsso_region=eu-west-1\n"
    )

    File.write!(credentials, "malformed credentials must never be loaded for login/logout")
    [home: root, env: %{}, config_file: config, credentials_file: credentials]
  end
end
