defmodule ExStorageServiceCli.CLI.AWSInputTest do
  use ExUnit.Case, async: true

  alias ExStorageServiceCli.{
    Error,
    CLI.AWSInput,
    CLI.Skeleton,
    CLI.Shorthand,
    S3.OperationCatalog
  }

  @fixtures __DIR__
            |> Path.join("../../fixtures/aws_input/vectors.json")
            |> File.read!()
            |> JSON.decode!()
  @moduletag :tmp_dir

  for vector <- @fixtures["shorthand"] do
    @vector vector
    test "matches pinned AWS shorthand: #{@vector["operation"]}/#{@vector["member"]}" do
      vector = @vector
      {:ok, operation} = OperationCatalog.operation(vector["operation"])
      shape = OperationCatalog.shape(operation["input"]["shape"])

      required =
        Enum.flat_map((shape["required"] || []) -- [vector["member"]], fn member ->
          ["--" <> flag(member), String.downcase(member)]
        end)

      assert {:ok, parsed} =
               AWSInput.parse(
                 vector["operation"],
                 required ++ ["--" <> flag(vector["member"]), vector["value"]]
               )

      assert parsed.input[vector["member"]] == vector["expected"]
    end
  end

  for vector <- @fixtures["skeletons"] do
    @vector vector
    test "matches pinned AWS skeleton: #{@vector["operation"]}/#{@vector["kind"]}" do
      kind = if @vector["kind"] == "input", do: :input, else: :output
      assert {:ok, skeleton} = Skeleton.generate(@vector["operation"], kind)
      assert skeleton == @vector["expected"]
    end
  end

  test "profile binary format is resolved before blob parsing without credentials", %{
    tmp_dir: dir
  } do
    config = Path.join(dir, "aws-config")
    credentials = Path.join(dir, "aws-credentials")

    File.write!(
      config,
      "[profile raw]\ncli_binary_format=raw-in-base64-out\ncredential_process=must-not-run\n"
    )

    File.write!(credentials, "malformed credentials must not be parsed")
    args = ["--bucket", "bucket", "--key", "key", "--sse-customer-key", "unused"]

    assert {:ok, options} =
             AWSInput.resolve_options(%{profile: "raw"},
               env: %{},
               config_file: config,
               credentials_file: credentials
             )

    assert options.cli_binary_format == "raw-in-base64-out"

    assert {:ok, explicit} =
             AWSInput.resolve_options(%{profile: "raw", cli_binary_format: "base64"},
               env: %{},
               config_file: config,
               credentials_file: credentials
             )

    assert explicit.cli_binary_format == "base64"

    assert {:ok, _} =
             AWSInput.parse("HeadObject", args ++ ["--profile", "raw"],
               env: %{},
               config_file: config,
               credentials_file: credentials
             )

    assert {:ok, _} =
             AWSInput.parse("HeadObject", ["--help"],
               config_file: credentials,
               credentials_file: credentials
             )

    assert {:ok, _} =
             AWSInput.parse("HeadObject", ["--generate-cli-skeleton"],
               config_file: credentials,
               credentials_file: credentials
             )
  end

  test "every modeled operation exposes binary member flags without generating atoms" do
    for {operation, model} <- OperationCatalog.operations() do
      shape = OperationCatalog.shape(model["input"]["shape"])
      flags = AWSInput.flags(operation)

      assert Map.new(flags, fn {flag, {member, _type}} -> {flag, member} end) ==
               @fixtures["member_flags"][operation]

      for member <- Map.keys((shape || %{})["members"] || %{}) do
        assert {^member, _type} = Map.fetch!(flags, flag(member))
      end
    end
  end

  for vector <- @fixtures["syntax"] || [] do
    @syntax vector
    test "matches pinned AWS shorthand syntax: #{@syntax["value"]}" do
      assert {:ok, @syntax["expected"]} == Shorthand.parse(@syntax["value"])
    end
  end

  test "shorthand rejects duplicate keys, trailing root comma and malformed nesting" do
    for invalid <- ["a=b,a=c", "a=b,", "a={b=x", "a=[x", "a=\"unfinished", "a=[x],z"] do
      assert {:error, %Error{code: :invalid_shorthand}} = Shorthand.parse(invalid)
    end
  end

  test "nested @= file references are bounded and malformed fileb structured input is typed", %{
    tmp_dir: dir
  } do
    one = Path.join(dir, "one")
    two = Path.join(dir, "two")
    File.write!(one, "first")
    File.write!(two, "second")
    value = "CORSRules=[{AllowedMethods=[GET],AllowedOrigins@=[file://#{one},file://#{two}]}]"

    assert {:ok, parsed} =
             AWSInput.parse("PutBucketCors", ["--bucket", "bucket", "--cors-configuration", value])

    assert hd(parsed.input["CORSConfiguration"]["CORSRules"])["AllowedOrigins"] == [
             "first",
             "second"
           ]

    assert {:error, %Error{}} =
             AWSInput.parse("PutBucketCors", [
               "--bucket",
               "bucket",
               "--cors-configuration",
               "fileb://" <> one
             ])

    assert {:error, %Error{}} =
             AWSInput.parse("HeadObject", ["--bucket", "bucket", "--key", "file://" <> one],
               max_input_bytes: 3
             )
  end

  test "timestamps normalize date-only, naive ISO, RFC1123 and fractional Unix seconds" do
    for {value, expected} <- [
          {"2024-01-01", "2024-01-01T00:00:00Z"},
          {"2024-01-01T12:30:45", "2024-01-01T12:30:45Z"},
          {"Mon, 01 Jan 2024 12:30:45 GMT", "2024-01-01T12:30:45Z"},
          {"1704067200.125", 1_704_067_200.125}
        ] do
      assert {:ok, parsed} =
               AWSInput.parse("HeadObject", [
                 "--bucket",
                 "bucket",
                 "--key",
                 "key",
                 "--if-modified-since",
                 value
               ])

      assert parsed.input["IfModifiedSince"] == expected
    end
  end

  test "JSON input is merged with explicit parameters, keeps absent/empty/false and exact long integers",
       %{tmp_dir: dir} do
    input = Path.join(dir, "input.json")

    File.write!(
      input,
      JSON.encode!(%{"Bucket" => "json-bucket", "Key" => "original", "Metadata" => %{}})
    )

    assert {:ok, parsed} =
             AWSInput.parse("PutObject", [
               "--cli-input-json",
               "file://" <> input,
               "--key",
               "override",
               "--no-bucket-key-enabled",
               "--content-length",
               "9007199254740993"
             ])

    assert parsed.input == %{
             "Bucket" => "json-bucket",
             "Key" => "override",
             "Metadata" => %{},
             "BucketKeyEnabled" => false,
             "ContentLength" => 9_007_199_254_740_993
           }

    refute Map.has_key?(parsed.input, "Body")
    assert {:error, %Error{}} = AWSInput.parse("PutObject", ["--cli-input-json", "null"])

    assert {:error, %Error{}} =
             AWSInput.parse("PutObject", ["--cli-input-yaml", "Bucket: bucket"])
  end

  test "blob modes and file references retain exact binary bytes", %{tmp_dir: dir} do
    binary = <<0, 255, 128, 13, 10>>
    file = Path.join(dir, "binary")
    File.write!(file, binary)
    # WriteGetObjectResponse Body is streaming; SSECustomerKey is a string,
    # while ObjectLambda WriteGetObjectResponse supports the streaming path.
    assert {:ok, parsed} =
             AWSInput.parse("PutObject", [
               "--bucket",
               "bucket",
               "--key",
               "key",
               "--body",
               "fileb://" <> file
             ])

    assert parsed.input["Body"] == file
    assert parsed.options[:streaming_body_path] == file

    assert {:ok, parsed} =
             AWSInput.parse("PutObject", [
               "--bucket",
               "bucket",
               "--key",
               "key",
               "--metadata",
               "{\"empty\":\"\"}"
             ])

    assert parsed.input["Metadata"] == %{"empty" => ""}
    text_file = Path.join(dir, "key")
    File.write!(text_file, "a b\n")

    assert {:ok, parsed} =
             AWSInput.parse("HeadObject", ["--bucket", "bucket", "--key", "file://" <> text_file])

    assert parsed.input["Key"] == "a b\n"
  end

  test "YAML input preserves scalar types and CLI overrides without opening object bodies" do
    yaml = """
    Bucket: yaml-bucket
    Key: original
    Metadata: {first: one}
    BucketKeyEnabled: false
    ContentLength: 9007199254740993
    Body: /unavailable/streaming-body
    """

    assert {:ok, parsed} =
             AWSInput.parse("PutObject", ["--cli-input-yaml", yaml, "--key", "override"])

    assert parsed.input["Bucket"] == "yaml-bucket"
    assert parsed.input["Key"] == "override"
    assert parsed.input["BucketKeyEnabled"] == false
    assert parsed.input["ContentLength"] === 9_007_199_254_740_993
    assert parsed.input["Metadata"] == %{"first" => "one"}
    assert parsed.options.streaming_body_path == "/unavailable/streaming-body"
  end

  test "YAML file and binary-file control input share the bounded loader", %{tmp_dir: dir} do
    path = Path.join(dir, "input.yaml")
    File.write!(path, "Bucket: bucket\nKey: key\n")

    for prefix <- ["file://", "fileb://"] do
      assert {:ok, parsed} = AWSInput.parse("PutObject", ["--cli-input-yaml", prefix <> path])
      assert parsed.input == %{"Bucket" => "bucket", "Key" => "key"}
    end

    assert {:error, %Error{}} =
             AWSInput.parse("PutObject", ["--cli-input-yaml", "file://" <> path],
               max_input_bytes: 5
             )
  end

  test "YAML date and timestamp nodes are coerced only for modeled timestamps" do
    yaml = """
    Bucket: bucket
    LifecycleConfiguration:
      Rules:
      - ID: expiration
        Status: Enabled
        Filter: {Prefix: docs/}
        Expiration: {Date: 2024-01-02}
    """

    assert {:ok, parsed} =
             AWSInput.parse("PutBucketLifecycleConfiguration", ["--cli-input-yaml", yaml])

    assert hd(parsed.input["LifecycleConfiguration"]["Rules"])["Expiration"]["Date"] ==
             "2024-01-02T00:00:00Z"

    assert {:error, %Error{}} =
             AWSInput.parse("PutObject", ["--cli-input-yaml", "Bucket: 2024-01-02\nKey: key\n"])
  end

  test "YAML input roots and mutually exclusive structured input options are validated" do
    assert {:error, %Error{code: :invalid_input_yaml}} =
             AWSInput.parse("PutObject", ["--cli-input-yaml", "null"])

    assert {:error, %Error{code: :invalid_input}} =
             AWSInput.parse("PutObject", ["--cli-input-json", "{}", "--cli-input-yaml", "{}"])
  end

  test "YAML input skeleton mode remains pure and bypasses required object parameters" do
    assert {:ok, parsed} = AWSInput.parse("PutObject", ["--generate-cli-skeleton", "yaml-input"])
    assert parsed.input == %{}
    assert parsed.options.generate_cli_skeleton == "yaml-input"
  end

  test "ordered modeled lists, timestamps, pagination/global switches and output filename" do
    assert {:ok, parsed} =
             AWSInput.parse("GetObject", [
               "--bucket",
               "bucket",
               "--key",
               "key",
               "--if-modified-since",
               "2024-01-01T00:00:00Z",
               "--checksum-mode",
               "ENABLED",
               "output.bin",
               "--cli-read-timeout",
               "0",
               "--no-sign-request"
             ])

    assert parsed.args == ["output.bin"]
    assert parsed.options[:cli_read_timeout] == 0
    assert parsed.options[:no_sign_request] == true
    assert parsed.input["IfModifiedSince"] == "2024-01-01T00:00:00Z"

    assert {:ok, parsed} =
             AWSInput.parse("GetObjectAttributes", [
               "--bucket",
               "bucket",
               "--key",
               "key",
               "--object-attributes",
               "ETag",
               "ObjectSize",
               "Checksum"
             ])

    assert parsed.input["ObjectAttributes"] == ["ETag", "ObjectSize", "Checksum"]

    assert {:ok, parsed} =
             AWSInput.parse("ListObjectsV2", [
               "--bucket",
               "bucket",
               "--no-paginate",
               "--starting-token",
               "token",
               "--page-size",
               "12",
               "--max-items",
               "3"
             ])

    assert parsed.options == %{
             no_paginate: true,
             starting_token: "token",
             page_size: 12,
             max_items: 3
           }
  end

  test "help and input skeleton need no required parameters, files or credentials" do
    assert {:ok, parsed} = AWSInput.parse("PutObject", ["--body", "/not/present", "--help"])
    assert parsed.options[:help]
    assert {:ok, parsed} = AWSInput.parse("PutObject", ["--generate-cli-skeleton"])
    assert parsed.options[:generate_cli_skeleton] == "input"

    assert {:error, %Error{}} =
             AWSInput.parse("PutObject", ["--bucket", "bucket", "--key", "key", "--body", "-"])

    assert {:error, %Error{}} =
             AWSInput.parse("HeadObject", [
               "--bucket",
               "bucket",
               "--key",
               "key",
               "--unknown",
               "private"
             ])

    assert {:error, %Error{}} =
             AWSInput.parse("HeadObject", [
               "--bucket",
               "bucket",
               "--key",
               "key",
               "--content-length",
               "1"
             ])
  end

  defp flag(member),
    do:
      member
      |> String.replace(~r/([A-Z]+)([A-Z][a-z])/, "\\1-\\2")
      |> String.replace(~r/([a-z0-9])([A-Z])/, "\\1-\\2")
      |> String.downcase()
end
