defmodule ExStorageServiceCli.CLI.NativeRoutesTest do
  use ExUnit.Case, async: true
  alias ExStorageServiceCli.{Context, Error, Target}
  alias ExStorageServiceCli.CLI.{HighLevel, Invocation, NativeUtility, Parser, Runner}
  import ExUnit.CaptureIO
  @moduletag :tmp_dir

  test "native mirror and watch flags are registered and help lists advanced options" do
    assert {:ok,
            %{
              command: ["mirror"],
              options: %{watch: true, adapter: "polling", overwrite: true, remove: true}
            }} =
             Parser.parse([
               "mirror",
               "./source",
               "s3://bucket/",
               "--watch",
               "--adapter",
               "polling",
               "--overwrite",
               "--remove",
               "--count",
               "1"
             ])

    assert {:ok, %{command: ["watch"], options: %{interval: 2, count: 1}}} =
             Parser.parse([
               "watch",
               "./source",
               "--adapter",
               "polling",
               "--interval",
               "2",
               "--count",
               "1"
             ])

    output = capture_io(fn -> assert :ok = Runner.run(["cp", "--help"]) end)
    assert output =~ "--checksum-algorithm"
    assert output =~ "--copy-props"
    assert output =~ "--tmpdir"
  end

  test "raw cat/head rejects structured output before network" do
    for command <- ["cat", "head"], options <- [%{json: true}, %{output: "yaml"}] do
      invocation = %Invocation{
        command: [command],
        args: ["s3://bucket/key"],
        options: Map.merge(options, %{no_sign_request: true, endpoint: "http://127.0.0.1:1"})
      }

      assert {:error, %Error{category: :syntax}} = Runner.execute(invocation)
    end
  end

  test "version inspection has an explicit result limit and partial marker", %{tmp_dir: root} do
    invocation = %Invocation{command: ["ls"], options: %{versions: true, json: true, limit: 1}}
    target = %Target{kind: :remote, bucket: "bucket", key: ""}

    fetch = fn "ListObjectVersions", %{"Bucket" => "bucket"} ->
      {:ok,
       %{
         "IsTruncated" => false,
         "Versions" => [%{"Key" => "a", "VersionId" => "one", "Size" => 3}],
         "DeleteMarkers" => [%{"Key" => "b", "VersionId" => "two"}]
       }}
    end

    assert {:ok,
            %{
              "Returned" => 1,
              "Partial" => true,
              "Entries" => [%{"key" => "a", "version_id" => "one"}]
            }} =
             HighLevel.run(invocation, [target], [%Context{unsigned: true}],
               fetch: fetch,
               tmp_dir: root
             )
  end

  test "diagnostics select S3 metadata and never invent an ESS health route" do
    target = %Target{kind: :remote, bucket: "bucket", key: ""}
    context = %Context{unsigned: true, capabilities: %{provider: "s3"}}
    call = fn "HeadBucket", %{"Bucket" => "bucket"} -> {:ok, %{}} end

    assert {:ok, %{"Ready" => true, "Operation" => "HeadBucket"}} =
             NativeUtility.run(%Invocation{command: ["ready"]}, target, context, call: call)

    assert {:ok, %{"Signed" => false, "CredentialProvider" => "anonymous"}} =
             NativeUtility.run(%Invocation{command: ["doctor"]}, target, context)
  end
end
