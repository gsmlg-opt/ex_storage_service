Code.require_file("../../support/wire_server.exs", __DIR__)

defmodule ExStorageServiceCli.CLI.APITest do
  use ExUnit.Case, async: true
  import ExUnit.CaptureIO
  alias ExStorageServiceCli.{CLI.Parser, CLI.Runner, Error}
  alias ExStorageServiceCli.Test.WireServer
  @moduletag :tmp_dir

  test "modeled members and global options parse in either position" do
    assert {:ok,
            %{options: %{profile: "s3api", api_input: %{"Bucket" => "bucket", "Key" => "key"}}}} =
             Parser.parse([
               "--profile",
               "s3api",
               "s3api",
               "head-object",
               "--key",
               "key",
               "--bucket",
               "bucket"
             ])

    assert {:error, %Error{category: :syntax, details: %{frontend: :s3api}}} =
             Parser.parse(["s3api", "head-object", "--bucket", "bucket"])

    assert {:ok, %{command: ["wait", "object-exists"], options: %{waiter: "ObjectExists"}}} =
             Parser.parse([
               "s3api",
               "wait",
               "object-exists",
               "--bucket",
               "bucket",
               "--key",
               "key"
             ])
  end

  test "control requests execute and output the service structure without an ESS envelope" do
    {endpoint, _} =
      WireServer.fixture([
        {200, [],
         "<Tagging><TagSet><Tag><Key>project</Key><Value>fixture</Value></Tag></TagSet></Tagging>"}
      ])

    assert {:ok, invocation} =
             Parser.parse([
               "s3api",
               "get-bucket-tagging",
               "--bucket",
               "bucket",
               "--no-sign-request",
               "--endpoint-url",
               endpoint
             ])

    output = capture_io(fn -> assert :ok = Runner.execute(invocation) end)
    assert JSON.decode!(output) == %{"TagSet" => [%{"Key" => "project", "Value" => "fixture"}]}
    assert_receive {:wire_request, %{target: "/bucket?tagging=", headers: headers}}
    refute Map.has_key?(headers, "authorization")
  end

  test "GetObject atomically writes the raw payload and stdout contains metadata only", %{
    tmp_dir: root
  } do
    bytes = <<0, 255, 128, 1>>
    {endpoint, _} = WireServer.fixture([{200, [{"ETag", "\"tag\""}], bytes}])
    filename = Path.join(root, "out")

    assert {:ok, invocation} =
             Parser.parse([
               "s3api",
               "get-object",
               "--bucket",
               "bucket",
               "--key",
               "key",
               "--no-sign-request",
               "--endpoint-url",
               endpoint,
               filename
             ])

    output = capture_io(fn -> assert :ok = Runner.execute(invocation) end)
    assert %{"ETag" => "\"tag\"", "ContentLength" => 4} = JSON.decode!(output)
    assert File.read!(filename) == bytes
    refute Map.has_key?(JSON.decode!(output), "Body")
  end

  test "help and skeletons remain pure with an invalid credential configuration" do
    for args <- [
          ["s3api", "head-object", "help"],
          ["s3api", "head-object", "--generate-cli-skeleton"]
        ] do
      assert {:ok, invocation} = Parser.parse(args)
      output = capture_io(fn -> assert :ok = Runner.execute(invocation) end)
      assert output != ""
    end
  end

  test "Select saves only Records and publishes after its validated End event", %{tmp_dir: root} do
    filename = Path.join(root, "selected")

    for {stream, expected} <- [
          {event("Records", <<0, 255, "rows">>) <> event("End", ""), :ok},
          {event("Records", "partial"), :missing_select_end}
        ] do
      File.write!(filename, "old")

      {endpoint, _} =
        WireServer.fixture([
          {200, [{"Content-Type", "application/vnd.amazon.eventstream"}], stream}
        ])

      argv = [
        "s3api",
        "select-object-content",
        "--bucket",
        "bucket",
        "--key",
        "key",
        "--expression",
        "SELECT * FROM S3Object",
        "--expression-type",
        "SQL",
        "--input-serialization",
        "CSV={}",
        "--output-serialization",
        "CSV={}",
        "--no-sign-request",
        "--endpoint-url",
        endpoint,
        filename
      ]

      assert {:ok, invocation} = Parser.parse(argv)

      assert capture_io(fn ->
               case expected do
                 :ok -> assert :ok = Runner.execute(invocation)
                 code -> assert {:error, %Error{code: ^code}} = Runner.execute(invocation)
               end
             end) == ""

      assert File.read!(filename) == if(expected == :ok, do: <<0, 255, "rows">>, else: "old")
    end
  end

  test "Object Lambda file output uses the modeled RequestRoute host prefix", %{tmp_dir: root} do
    body = <<0, 255, 128, "raw">>
    source = Path.join(root, "response")
    File.write!(source, body)
    {endpoint, _} = WireServer.fixture([{200, [], ""}])
    endpoint = String.replace(endpoint, "127.0.0.1", "0.0.1")

    assert {:ok, invocation} =
             Parser.parse([
               "s3api",
               "write-get-object-response",
               "--request-route",
               "127",
               "--request-token",
               "fixture",
               "--body",
               source,
               "--no-sign-request",
               "--endpoint-url",
               endpoint
             ])

    assert capture_io(fn -> assert :ok = Runner.execute(invocation) end) == ""

    assert_receive {:wire_request,
                    %{body: ^body, headers: headers, target: "/WriteGetObjectResponse"}}

    assert String.starts_with?(headers["host"], "127.0.0.1:")
    assert headers["content-length"] == to_string(byte_size(body))
  end

  test "table and YAML output handlers execute through the frontend" do
    for {format, marker} <- [
          {"table", "project"},
          {"yaml", "TagSet:"},
          {"yaml-stream", "TagSet:"}
        ] do
      {endpoint, _} =
        WireServer.fixture([
          {200, [],
           "<Tagging><TagSet><Tag><Key>project</Key><Value>fixture</Value></Tag></TagSet></Tagging>"}
        ])

      assert {:ok, invocation} =
               Parser.parse([
                 "s3api",
                 "get-bucket-tagging",
                 "--bucket",
                 "bucket",
                 "--no-sign-request",
                 "--endpoint-url",
                 endpoint,
                 "--output",
                 format
               ])

      assert capture_io(fn -> assert :ok = Runner.execute(invocation) end) =~ marker
    end

    {:ok, invocation} =
      Parser.parse(["s3api", "head-object", "--generate-cli-skeleton", "yaml-input"])

    assert capture_io(fn -> assert :ok = Runner.execute(invocation) end) =~ "Bucket:"
  end

  defp event(type, payload) do
    headers =
      Enum.map_join([{":message-type", "event"}, {":event-type", type}], fn {key, value} ->
        <<byte_size(key), key::binary, 7, byte_size(value)::16, value::binary>>
      end)

    prelude = <<byte_size(headers) + byte_size(payload) + 16::32, byte_size(headers)::32>>
    message = prelude <> <<:erlang.crc32(prelude)::32>> <> headers <> payload
    message <> <<:erlang.crc32(message)::32>>
  end
end
