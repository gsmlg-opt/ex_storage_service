defmodule ExStorageServiceCli.CLI.ParserTest do
  use ExUnit.Case, async: true
  alias ExStorageServiceCli.{CLI.Parser, Error}

  test "global options work before and after the native command" do
    assert {:ok, invocation} =
             Parser.parse([
               "--json",
               "--profile",
               "prod",
               "cp",
               "./a",
               "s3://b/a",
               "--endpoint",
               "https://objects.example",
               "-r"
             ])

    assert invocation.frontend == :native
    assert invocation.command == ["cp"]
    assert invocation.args == ["./a", "s3://b/a"]

    assert invocation.options == %{
             json: true,
             profile: "prod",
             endpoint: "https://objects.example",
             recursive: true
           }
  end

  test "nested frontends and ordered filters retain their own option names" do
    assert {:ok, invocation} =
             Parser.parse([
               "s3",
               "cp",
               "./a",
               "s3://b/",
               "--exclude",
               "*",
               "--include",
               "*.txt",
               "--endpoint-url",
               "https://objects.example"
             ])

    assert invocation.frontend == :s3
    assert invocation.command == ["cp"]
    assert invocation.filters == [{:exclude, "*"}, {:include, "*.txt"}]
    assert invocation.options.endpoint_url == "https://objects.example"

    assert {:error, %Error{category: :syntax}} =
             Parser.parse(["cp", "./a", "s3://b/", "--endpoint-url", "https://objects.example"])
  end

  test "AWS high-level scalar flags use the last occurrence" do
    assert {:ok, invocation} =
             Parser.parse([
               "--region",
               "us-east-1",
               "s3",
               "mb",
               "s3://bucket",
               "--region",
               "us-west-2"
             ])

    assert invocation.options.region == "us-west-2"

    assert {:error, %Error{category: :syntax}} =
             Parser.parse(["--region", "us-east-1", "mb", "s3://bucket", "--region", "us-west-2"])
  end

  test "rejects unknown, invalid, conflicting and inapplicable flags without printing values" do
    for args <- [
          ["ls", "--wat", "private-secret"],
          ["ls", "--max-keys", "NaN"],
          ["version", "--recursive"],
          ["ls", "--json", "--no-json"],
          ["ls", "--profile"]
        ] do
      assert {:error, %Error{category: :syntax, message: message}} = Parser.parse(args)
      refute message =~ "private-secret"
      refute message =~ "NaN"
    end
  end

  test "end of options and version preserve explicit command grammar" do
    assert {:ok, %{args: ["-literal"], options: %{}}} = Parser.parse(["ls", "--", "-literal"])
    assert {:ok, %{command: ["version"]}} = Parser.parse(["--version"])

    assert {:ok, %{command: ["version", "enable"], args: ["s3://bucket"]}} =
             Parser.parse(["version", "enable", "s3://bucket"])

    assert {:ok, %{command: ["alias", "set"], args: ["prod", "https://objects.example"]}} =
             Parser.parse(["alias", "set", "prod", "https://objects.example"])

    assert {:ok, %{help: true, command: ["cp"]}} = Parser.parse(["help", "cp"])
    assert {:ok, %{help: true, command: ["cp"]}} = Parser.parse(["cp", "--help"])
    assert {:error, %Error{}} = Parser.parse(["cp", "./only-source"])
  end

  test "legacy clustered and attached short options remain valid" do
    assert {:ok, %{help: true, options: %{recursive: true, help: true}}} =
             Parser.parse(["cp", "-rh"])

    assert {:ok, %{options: %{expires: 3600}}} =
             Parser.parse(["presign", "s3://bucket/key", "-e3600"])

    assert {:ok, %{options: %{json: true}}} = Parser.parse(["--json=true", "version"])
  end

  test "AWS syntax failures carry their frontend for exit status mapping" do
    for argv <- [
          ["s3", "ls", "--unknown"],
          ["--endpoint-url", "https://example.test", "s3", "cp", "./a"],
          ["s3api", "unknown-operation"]
        ] do
      assert {:error, %Error{details: %{frontend: frontend}}} = Parser.parse(argv)
      assert frontend in [:s3, :s3api]
    end
  end

  test "native find predicates and replication conveniences resolve through the public registry" do
    assert {:ok, %{command: ["find"], options: options}} =
             Parser.parse([
               "find",
               "s3://bucket/prefix",
               "--path",
               "a/*",
               "--ignore",
               "*.tmp",
               "--regex",
               "[.]txt$",
               "--larger",
               "1KiB",
               "--smaller",
               "1MB",
               "--older-than",
               "1d",
               "--newer-than",
               "2026-10-01",
               "--maxdepth",
               "2",
               "--versions"
             ])

    assert options == %{
             path: "a/*",
             ignore: "*.tmp",
             regex: "[.]txt$",
             larger: "1KiB",
             smaller: "1MB",
             older_than: "1d",
             newer_than: "2026-10-01",
             maxdepth: 2,
             versions: true
           }

    assert {:ok, %{command: ["replicate", "add"], args: ["s3://bucket"], options: replication}} =
             Parser.parse([
               "replicate",
               "add",
               "s3://bucket",
               "--id",
               "copy",
               "--role",
               "arn:aws:iam::123456789012:role/replicate",
               "--remote-bucket",
               "arn:aws:s3:::destination",
               "--replicate",
               "delete-marker"
             ])

    assert replication.id == "copy"
    assert replication.remote_bucket == "arn:aws:s3:::destination"
    assert {:error, %Error{}} = Parser.parse(["find", "s3://bucket", "--exec", "private-command"])
  end

  test "namespace and command-group help validate options without resolving credentials" do
    assert {:ok, %{frontend: :native, command: ["alias"], help: true}} =
             Parser.parse(["alias", "--help"])

    assert {:ok, %{frontend: :s3, command: ["cp"], help: true}} =
             Parser.parse(["s3", "cp", "help"])

    assert {:error, %Error{}} = Parser.parse(["--recursive"])

    assert {:ok, %{frontend: :s3, command: ["version"]}} =
             Parser.parse(["s3", "--version"])
  end
end
