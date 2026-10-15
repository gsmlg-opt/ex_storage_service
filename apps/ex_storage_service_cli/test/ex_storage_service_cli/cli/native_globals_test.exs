Code.require_file("../../support/wire_server.exs", __DIR__)

defmodule ExStorageServiceCli.CLI.NativeGlobalsTest do
  # stderr capture replaces a VM-wide named IO device.
  use ExUnit.Case, async: false
  import ExUnit.CaptureIO
  alias ExStorageServiceCli.{Aliases, Config, Context, Error, S3.Client}
  alias ExStorageServiceCli.CLI.{NativeGlobals, Parser, Resolver, Runner}
  alias ExStorageServiceCli.Test.WireServer
  @moduletag :tmp_dir

  test "native aliases retain repeated custom headers in order" do
    assert {:ok, invocation} =
             Parser.parse([
               "-C",
               "/tmp/config",
               "--dp",
               "--insecure",
               "ls",
               "-H",
               " X-Trace : first:part ",
               "--custom-header=x-trace:second"
             ])

    assert %{
             config_dir: "/tmp/config",
             no_cli_pager: true,
             no_verify_ssl: true,
             request_headers: [{"x-trace", "first:part"}, {"x-trace", "second"}]
           } = invocation.options

    for flag <- ["-C", "-H"] do
      assert {:error, %Error{category: :syntax}} = Parser.parse(["s3", "ls", flag, "value"])
    end
  end

  test "invalid and reserved headers fail before dispatch without exposing values" do
    for value <- [
          "missing-colon",
          "bad name:secret",
          "x-test:secret\r\ninjected:yes",
          "x-test:secret\0",
          "\nx-test:secret"
        ] do
      assert {:error, %Error{category: :syntax, message: message}} =
               Parser.parse(["ls", "-H", value])

      refute message =~ "secret"
    end

    for name <-
          ~w(Authorization HOST Content-Length Transfer-Encoding Connection Trailer TE Expect Content-MD5 Content-Encoding X-Amz-Date X-Amz-Security-Token X-Amz-S3session-Token X-Amz-Content-Sha256 X-Amz-Checksum-Sha256 X-Amz-Sdk-Checksum-Algorithm X-Amz-Decoded-Content-Length X-Amz-Trailer X-Amz-Trailer-Signature) do
      assert {:error, %Error{category: :syntax}} = Parser.parse(["ls", "-H", name <> ":secret"])
    end

    assert {:error, %Error{category: :syntax}} = Parser.parse(["ls", "--config-dir="])
  end

  test "config directory selects alias reads and mutations without environment changes", %{
    tmp_dir: directory
  } do
    before = System.get_env("ESS_CONFIG_DIR")

    assert "" =
             capture_io(fn ->
               assert :ok =
                        Runner.run([
                          "--config-dir",
                          directory,
                          "alias",
                          "set",
                          "selected",
                          "https://selected.test",
                          "--provider",
                          "anonymous",
                          "--output",
                          "off"
                        ])
             end)

    assert {:ok, %{"selected" => %{"endpoint" => "https://selected.test"}}} =
             Aliases.list(path: Path.join(directory, "config.toml"))

    assert {:ok, invocation} = Parser.parse(["ls", "selected/bucket", "-C", directory])

    assert {:ok, %{contexts: [%Context{endpoint: "https://selected.test", unsigned: true}]}} =
             Resolver.resolve(invocation, path: Path.join(directory, "wrong.toml"))

    assert {:ok, _} = Config.read(path: Path.join(directory, "config.toml"))
    assert System.get_env("ESS_CONFIG_DIR") == before

    assert "" =
             capture_io(fn ->
               assert :ok =
                        Runner.run([
                          "alias",
                          "remove",
                          "selected",
                          "-C",
                          directory,
                          "--output",
                          "off"
                        ])
             end)

    assert {:ok, %{}} = Aliases.list(path: Path.join(directory, "config.toml"))
  end

  test "insecure emits one stderr warning and leaves structured stdout clean" do
    stderr =
      capture_io(:stderr, fn ->
        stdout =
          capture_io(fn -> assert :ok = Runner.run(["version", "--insecure", "--json"]) end)

        assert %{"version" => _} = JSON.decode!(stdout)
      end)

    assert [_, ""] = String.split(stderr, "\n")
    assert stderr =~ "TLS certificate verification is disabled"

    assert "" =
             capture_io(:stderr, fn ->
               capture_io(fn -> Runner.run(["help", "--insecure"]) end)
             end)
  end

  test "interactive configure writes its selected directory", %{tmp_dir: directory} do
    output =
      capture_io("https://configured.test\nfixture-access\nfixture-secret\nus-west-2\n", fn ->
        assert :ok = Runner.run(["configure", "--config-dir", directory])
      end)

    path = Path.join(directory, "config.toml")
    assert output =~ path

    assert {:ok,
            %{
              endpoint: "https://configured.test",
              access_key_id: "fixture-access",
              secret_access_key: "fixture-secret",
              region: "us-west-2"
            }} = Config.read_profile("default", path: path)
  end

  test "custom headers replace modeled values before signing and preserve duplicate order" do
    assert {:ok, options} =
             NativeGlobals.prepare(%{
               custom_headers: ["content-type:text/custom", "x-trace:first", "X-Trace:second"]
             })

    {endpoint, _} =
      WireServer.start(fn listener, owner ->
        {:ok, socket} = :gen_tcp.accept(listener, 15_000)
        {head, rest} = WireServer.head(socket)
        send(owner, {:wire_request, head, WireServer.body(socket, rest, 4)})
        WireServer.response(socket, 200, [], "")
        :gen_tcp.close(socket)
      end)

    context = %Context{
      endpoint: endpoint,
      access_key_id: "fixture-access",
      secret_access_key: "fixture-secret",
      options: options
    }

    assert {:ok, _} =
             Client.call(
               context,
               "PutObject",
               %{
                 "Bucket" => "bucket",
                 "Key" => "key",
                 "Body" => "body",
                 "ContentType" => "text/plain"
               },
               allow_streaming: true
             )

    assert_receive {:wire_request, head, "body"}
    [_request_line | fields] = String.split(head, "\r\n")

    headers =
      Enum.map(fields, fn field ->
        [name, value] = String.split(field, ":", parts: 2)
        {String.downcase(name), String.trim(value)}
      end)

    assert [{"x-trace", "first"}, {"x-trace", "second"}] =
             Enum.filter(headers, &(elem(&1, 0) == "x-trace"))

    assert [{"content-type", "text/custom"}] =
             Enum.filter(headers, &(elem(&1, 0) == "content-type"))

    authorization = List.keyfind(headers, "authorization", 0) |> elem(1)
    assert authorization =~ "content-type"
    assert authorization =~ "x-trace"
  end
end
