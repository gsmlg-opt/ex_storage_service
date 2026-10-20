defmodule ExStorageServiceCli.Credentials.BrowserTest do
  use ExUnit.Case, async: false
  import ExUnit.CaptureIO
  alias ExStorageServiceCli.Credentials.{Browser, PKCE}
  @moduletag :tmp_dir

  test "default launcher receives a literal URL and discards both output streams", %{
    tmp_dir: root
  } do
    arguments = Path.join(root, "arguments")
    url = "https://verify.example/?literal=$(id);code=private-marker"

    executable =
      helper(
        root,
        "printf '%s\\n' \"$@\" > '#{arguments}'\nprintf stdout-secret\nprintf stderr-secret >&2"
      )

    output =
      capture_io(:stderr, fn ->
        assert :ok = PKCE.notify(url, browser_executable: executable)
      end)

    assert File.read!(arguments) == url <> "\n"
    assert output == "Open this URL to sign in:\n" <> url <> "\n"
    refute output =~ "stdout-secret"
    refute output =~ "stderr-secret"
    refute_receive {_port, _message}
  end

  test "no-browser suppresses both default launch and the injected callback", %{tmp_dir: root} do
    marker = Path.join(root, "must-not-exist")
    executable = helper(root, "touch '#{marker}'")

    for opener <- [nil, fn _ -> flunk("must not open") end] do
      assert :ok =
               PKCE.notify("https://verify.example/",
                 no_browser: true,
                 on_verification: fn _ -> :ok end,
                 browser_executable: executable,
                 open_browser: opener
               )
    end

    refute File.exists?(marker)
  end

  test "device notification preserves fallback URL and code and opens the complete URL" do
    parent = self()

    output =
      capture_io(:stderr, fn ->
        assert :ok =
                 Browser.notify(
                   %{
                     verification_uri: "https://verify.example/",
                     verification_uri_complete: "https://verify.example/?code=display-code",
                     user_code: "display-code"
                   },
                   open_browser: fn url -> send(parent, {:opened, url}) end
                 )
      end)

    assert output ==
             "Open this URL to sign in:\nhttps://verify.example/\nThen enter code: display-code\n"

    assert_receive {:opened, "https://verify.example/?code=display-code"}
  end

  test "injected opener failures are nonfatal and never print their exception details" do
    for opener <- [fn _ -> raise "secret-error" end, fn _ -> throw("secret-throw") end] do
      assert capture_io(:stderr, fn ->
               assert :ok = PKCE.notify("https://verify.example/", open_browser: opener)
             end) == "Open this URL to sign in:\nhttps://verify.example/\n"
    end
  end

  test "unavailable programs and unsuccessful launchers leave manual authorization usable", %{
    tmp_dir: root
  } do
    for executable <- [Path.join(root, "missing"), helper(root, "exit 7")] do
      assert :ok =
               PKCE.notify("https://verify.example/",
                 on_verification: fn _ -> :ok end,
                 browser_executable: executable
               )
    end
  end

  test "launcher timeout kills its owned process and descendants", %{tmp_dir: root} do
    {executable, files} = sleeping_helper(root)
    started = System.monotonic_time(:millisecond)

    assert :timeout =
             Browser.launch("https://verify.example/",
               browser_executable: executable,
               browser_timeout: 300
             )

    assert System.monotonic_time(:millisecond) - started < 3000
    for file <- files, do: assert(stopped?(File.read!(file)))
    refute_receive {_port, _message}
  end

  test "invocation death cancels the launcher and its owned descendants", %{tmp_dir: root} do
    {executable, files} = sleeping_helper(root)

    owner =
      spawn(fn -> Browser.launch("https://verify.example/", browser_executable: executable) end)

    assert await_file(List.last(files))
    Process.exit(owner, :kill)
    for file <- files, do: assert(stopped?(File.read!(file)))
  end

  test "short launcher timeouts preserve their result during process cleanup", %{tmp_dir: root} do
    executable = helper(root, "exec '#{System.find_executable("sleep")}' 60")

    for _ <- 1..32 do
      assert :timeout =
               Browser.launch("https://verify.example/",
                 browser_executable: executable,
                 browser_timeout: 1
               )
    end
  end

  test "unsafe complete verification URLs are never passed to a browser" do
    for url <- ["file:///private", "javascript:alert(1)", "https://user:password@verify.example/"] do
      assert :ok =
               Browser.notify(
                 %{
                   verification_uri: "https://verify.example/",
                   verification_uri_complete: url,
                   user_code: nil
                 },
                 on_verification: fn _ -> :ok end,
                 open_browser: fn _ -> flunk("unsafe URL must not open") end
               )
    end
  end

  defp sleeping_helper(root) do
    own = Path.join(root, "launcher-pid")
    child = Path.join(root, "child-pid")

    executable =
      helper(
        root,
        "printf '%s' \"$$\" > '#{own}'\n'#{System.find_executable("sleep")}' 60 &\nprintf '%s' \"$!\" > '#{child}'\nwait"
      )

    {executable, [own, child]}
  end

  defp helper(root, body) do
    path = Path.join(root, "browser helper")
    File.write!(path, "#!/bin/sh\n" <> body <> "\n")
    File.chmod!(path, 0o700)
    path
  end

  defp await_file(path, attempts \\ 100)
  defp await_file(_path, 0), do: false

  defp await_file(path, attempts) do
    if File.exists?(path),
      do: true,
      else:
        (
          Process.sleep(10)
          await_file(path, attempts - 1)
        )
  end

  defp stopped?(pid, attempts \\ 50)
  defp stopped?(_pid, 0), do: false

  defp stopped?(pid, attempts) do
    {status, _} = System.cmd("ps", ["-o", "stat=", "-p", pid], stderr_to_stdout: true)
    status = String.trim(status)

    if status == "" or String.starts_with?(status, "Z"),
      do: true,
      else:
        (
          Process.sleep(10)
          stopped?(pid, attempts - 1)
        )
  end
end
