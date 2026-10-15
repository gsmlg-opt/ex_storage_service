Code.require_file("../../support/wire_server.exs", __DIR__)

defmodule ExStorageServiceCli.Transport.ResolutionTest do
  use ExUnit.Case, async: true
  alias ExStorageServiceCli.{Context, Error, S3Client}
  alias ExStorageServiceCli.CLI.{NativeGlobals, Parser}
  alias ExStorageServiceCli.S3.Client
  alias ExStorageServiceCli.Test.WireServer
  alias ExStorageServiceCli.Transport.Resolution

  @buckets "<ListAllMyBucketsResult><Buckets/></ListAllMyBucketsResult>"

  test "locked mc capture independently retains its original authority with the last pin" do
    bytes =
      File.read!(Path.expand("../../fixtures/differential/native-resolve-wire.json", __DIR__))

    assert Base.encode16(:crypto.hash(:sha256, bytes), case: :lower) ==
             "6df6e17cdecd92f773c83d05900b841cf9d4987350efc5aa1c8fe9f021064e65"

    reference = JSON.decode!(bytes)

    assert reference["binary_sha256"] ==
             "a62c4c85f9efbe5d07e03e2ff9923625fbf659a9f272a077a0cd846279f1ccc2"

    assert Enum.map(reference["cases"], & &1["id"]) == ["exact-authority", "last-entry-wins"]

    for row <- reference["cases"] do
      assert row["exit_status"] == 0
      assert Enum.all?(row["wire"], &(&1["host"] == "pin.invalid:PORT"))
      assert Enum.any?(row["wire"], &(&1["method"] == "HEAD" and &1["target"] == "/bucket/key"))
    end
  end

  test "native repeated resolve entries are invocation-local and last occurrence wins" do
    assert {:ok, invocation} =
             Parser.parse([
               "--resolve",
               "EXAMPLE.test=127.0.0.2",
               "ls",
               "--resolve=example.test=127.0.0.1",
               "--resolve",
               "example.test:9000=::1"
             ])

    assert invocation.options.resolve_entries == %{
             {"example.test", nil} => {127, 0, 0, 1},
             {"example.test", 9000} => {0, 0, 0, 0, 0, 0, 0, 1}
           }

    for namespace <- [["s3", "ls"], ["s3api", "list-buckets"]] do
      assert {:error, %Error{category: :syntax}} =
               Parser.parse(namespace ++ ["--resolve", "example.test=127.0.0.1"])
    end
  end

  test "exact port wins over a host pin and unmatched authorities keep their normal options" do
    assert {:ok, pins} =
             Resolution.parse(["host.test=127.0.0.1", "host.test:9000=127.0.0.2", "[::1]:80=::1"])

    assert {:ok, opts} = Resolution.options("http://HOST.test:9000/key", resolve_entries: pins)
    assert opts[:connect_address] == {127, 0, 0, 2}
    assert {:ok, opts} = Resolution.options("https://host.test/key", resolve_entries: pins)
    assert opts[:connect_address] == {127, 0, 0, 1}
    assert {:ok, opts} = Resolution.options("http://[::1]/key", resolve_entries: pins)
    assert opts[:connect_address] == {0, 0, 0, 0, 0, 0, 0, 1}
    assert {:ok, opts} = Resolution.options("http://other.test/key", resolve_entries: pins)
    refute opts[:connect_address]
    assert {:ok, [timeout: 150]} = Resolution.options("http://host.test", timeout: 150)
  end

  test "invalid authorities, ports and nonliteral addresses fail before resolution without echo" do
    for value <- [
          "secret",
          "=127.0.0.1",
          "host.test=secret",
          "host.test=127.1",
          "host.test=127.0.0.1:9000",
          "host.test=127.0.0.1=secret",
          "host.test:0=127.0.0.1",
          "host.test:65536=127.0.0.1",
          "host.test:secret=127.0.0.1",
          "host.test/secret=127.0.0.1",
          "user@host.test=127.0.0.1",
          "*.test=127.0.0.1",
          " host.test=127.0.0.1",
          "host.test=127.0.0.1\n",
          "host.test=fe80::1%secret",
          "[not-ip]:80=::1"
        ] do
      assert {:error, %Error{code: :invalid_resolve, message: message}} =
               Parser.parse(["ls", "--resolve", value])

      refute message =~ "secret"
    end
  end

  test "modeled signed request dials the pin while retaining the original Host and signature" do
    {endpoint, _} = WireServer.fixture([{200, [], @buckets}])
    port = URI.parse(endpoint).port
    context = context("http://signed.invalid:#{port}", ["signed.invalid:#{port}=127.0.0.1"])

    assert {:ok, %{"Buckets" => []}} = Client.call(context, "ListBuckets", %{}, max_attempts: 1)
    assert_receive {:wire_request, %{headers: headers}}
    assert headers["host"] == "signed.invalid:#{port}"
    assert headers["authorization"] =~ "SignedHeaders=host;"
    refute headers["authorization"] =~ "127.0.0.1"
  end

  test "legacy requests use the same pin without rewriting their target or authority" do
    {endpoint, _} = WireServer.fixture([{200, [], @buckets}])
    port = URI.parse(endpoint).port
    context = context("http://legacy.invalid:#{port}", ["legacy.invalid=127.0.0.1"])
    client = S3Client.new(Map.from_struct(context))

    assert {:ok, []} = S3Client.list_buckets(client)
    assert_receive {:wire_request, %{method: "GET", target: "/", headers: headers}}
    assert headers["host"] == "legacy.invalid:#{port}"
  end

  test "a failed pinned connection never falls back to the original reachable address" do
    {endpoint, _} = WireServer.fixture([{200, [], @buckets}])
    context = context(endpoint, ["127.0.0.1=127.0.0.2"])

    assert {:error, %Error{category: :transport}} =
             Client.call(context, "ListBuckets", %{}, max_attempts: 1, timeout: 1000)

    refute_receive {:wire_request, _}, 100
  end

  test "manual redirects do not forward the pin or signed request to the destination" do
    {destination, _} = WireServer.fixture([{200, [], @buckets}])
    {endpoint, _} = WireServer.fixture([{302, [{"Location", destination}], ""}])
    port = URI.parse(endpoint).port
    context = context("http://redirect.invalid:#{port}", ["redirect.invalid=127.0.0.1"])

    assert {:error, %Error{http_status: 302}} =
             Client.call(context, "ListBuckets", %{}, max_attempts: 1)

    assert_receive {:wire_request, %{headers: %{"host" => host}}}
    assert host == "redirect.invalid:#{port}"
    refute_receive {:wire_request, _}, 100
  end

  defp context(endpoint, entries) do
    assert {:ok, options} = NativeGlobals.prepare(%{resolve: entries})

    %Context{
      endpoint: endpoint,
      access_key_id: "fixture-access",
      secret_access_key: "fixture-secret",
      options: options
    }
  end
end
