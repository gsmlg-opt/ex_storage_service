defmodule ExStorageServiceCli.CLI.S3OptionsTest do
  use ExUnit.Case, async: false
  alias ExStorageServiceCli.{CLI.Parser, CLI.Registry, CLI.S3Options, Error}

  @fixtures JSON.decode!(
              File.read!(Path.expand("../../fixtures/s3-options/vectors.json", __DIR__))
            )

  for vector <- @fixtures["vectors"] do
    @vector vector
    test "pinned high-level #{@vector["command"]} #{@vector["flag"]}" do
      result = Parser.parse(["s3", @vector["command"] | @vector["argv"]])

      if @vector["error"] do
        assert {:error, %Error{category: :syntax, message: message}} = result
        refute message =~ "invalid-private-value"
      else
        assert {:ok, invocation} = result

        assert Enum.find_value(invocation.options, fn {key, value} ->
                 if Atom.to_string(key) == @vector["key"], do: {:found, value}
               end) == {:found, @vector["value"]}
      end
    end
  end

  test "complete nine-command flags are discoverable and scoped" do
    assert map_size(Registry.commands(:s3)) == 9

    for {command, {_arity, flags, _summary}} <- Registry.commands(:s3) do
      assert flags == S3Options.flags(command)
    end

    assert S3Options.flags("cp")["expires"] == {:expires, :string}
    assert S3Options.flags("cp")["grants"] == {:grants, :string_list}
    assert S3Options.defaults("cp")[:follow_symlinks] == true
    assert S3Options.defaults("cp")[:guess_mime_type] == true
    assert S3Options.defaults("presign")[:expires_in] == 3600

    assert {:error, %Error{}} =
             Parser.parse(["s3", "rm", "s3://bucket/key", "--content-type", "text/plain"])

    assert {:error, %Error{}} = Parser.parse(["s3", "sync", "./a", "s3://bucket/", "--recursive"])
  end

  test "native and AWS Expires keep distinct types" do
    assert {:ok, %{options: %{expires: 3600}}} =
             Parser.parse(["presign", "s3://bucket/key", "--expires", "3600"])

    assert {:ok, %{options: %{expires: "2026-10-15T10:00:00Z"}}} =
             Parser.parse([
               "s3",
               "cp",
               "./a",
               "s3://bucket/key",
               "--expires",
               "2026-10-15T10:00:00Z"
             ])

    assert {:error, %Error{}} =
             Parser.parse(["presign", "s3://bucket/key", "--expires", "tomorrow"])
  end

  test "AWS optional arguments and explicit booleans are strict" do
    assert {:ok, %{options: %{sse: "AES256", quiet: true}}} =
             Parser.parse(["s3", "cp", "./a", "s3://bucket/key", "--sse", "--quiet"])

    assert {:ok, %{options: %{sse: "aws:kms"}}} =
             Parser.parse(["s3", "cp", "./a", "s3://bucket/key", "--sse=aws:kms"])

    for argv <- [
          ["s3", "cp", "./a", "s3://bucket/key", "--quiet=true"],
          ["s3", "cp", "./a", "s3://bucket/key", "--no-recursive"],
          ["s3", "cp", "./a", "s3://bucket/key", "--grants"],
          ["s3", "cp", "./a", "s3://bucket/key", "--sse="]
        ],
        do: assert(match?({:error, %Error{}}, Parser.parse(argv)))

    for command <- ["cp", "mv", "sync"],
        {flags, expected} <- [
          {["--follow-symlinks", "--no-follow-symlinks"], false},
          {["--no-follow-symlinks", "--follow-symlinks"], true}
        ] do
      assert {:ok, %{options: %{follow_symlinks: ^expected}}} =
               Parser.parse(["s3", command, "./a", "s3://bucket/key" | flags])
    end
  end

  test "AWS globals before command do not consume namespace words" do
    assert {:ok, %{frontend: :s3, options: %{debug: true, output: "yaml"}}} =
             Parser.parse(["--debug", "--output", "yaml", "s3", "ls"])

    assert {:ok, %{options: %{cli_auto_prompt: true}}} =
             Parser.parse(["s3", "ls", "--cli-auto-prompt", "--help"])

    assert {:error, %Error{}} = Parser.parse(["s3", "ls", "--output", "off"])
    assert {:error, %Error{}} = Parser.parse(["s3", "ls", "--color", "purple"])

    assert {:error, %Error{}} =
             Parser.parse(["s3", "ls", "--cli-auto-prompt", "--no-cli-auto-prompt"])
  end

  test "native TLS pairs and resource bounds validate without reading files" do
    assert {:ok, %{options: options}} =
             Parser.parse([
               "ls",
               "--client-cert",
               "/absent/cert",
               "--client-key",
               "/absent/key",
               "--ca-bundle",
               "/absent/ca",
               "--max-requests",
               "2",
               "--max-concurrency",
               "4",
               "--memory-budget",
               "1024",
               "--output",
               "off",
               "--quiet",
               "--debug"
             ])

    assert options.client_cert == "/absent/cert"
    assert options.memory_budget == 1024

    for argv <- [
          ["ls", "--client-cert", "/absent/cert"],
          ["ls", "--client-key", "/absent/key"],
          ["ls", "--max-concurrency", "0"],
          ["ls", "--max-requests", "-1"],
          ["ls", "--memory-budget", "0"],
          ["ls", "--json", "--output", "text"]
        ],
        do: assert(match?({:error, %Error{}}, Parser.parse(argv)))

    assert {:ok, _} = Parser.parse(["ls", "--json", "--output", "json"])
  end

  test "untrusted option names do not grow the atom table" do
    Parser.parse(["s3", "cp", "./a", "s3://bucket/key", "--unknown-warmup"])
    before = :erlang.system_info(:atom_count)

    for index <- 1..100 do
      assert {:error, %Error{}} = Parser.parse(["s3", "ls", "--unique-untrusted-#{index}"])
    end

    assert :erlang.system_info(:atom_count) - before < 5
  end
end
