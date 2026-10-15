defmodule ExStorageServiceCli.CLI.ResolverTest do
  use ExUnit.Case, async: true
  alias ExStorageServiceCli.{CLI.Parser, CLI.Resolver, Config, Aliases, Context, Error, Target}
  @moduletag :tmp_dir

  test "each alias selects a coherent independent profile and endpoint", %{tmp_dir: directory} do
    opts = [path: Path.join(directory, "config.toml")]
    :ok = Config.save_profile("source", %{access_key_id: "a", secret_access_key: "sa"}, opts)
    :ok = Config.save_profile("destination", %{access_key_id: "b", secret_access_key: "sb"}, opts)
    :ok = Aliases.set("first", %{"endpoint" => "https://first.test", "profile" => "source"}, opts)

    :ok =
      Aliases.set(
        "second",
        %{"endpoint" => "https://second.test", "profile" => "destination"},
        opts
      )

    {:ok, invocation} = Parser.parse(["cp", "ess://first/bucket/a//../b", "second/other/key"])

    assert {:ok, resolved} = Resolver.resolve(invocation, opts)
    assert [%Target{key: "a//../b"}, %Target{key: "key"}] = resolved.targets

    assert [
             %Context{
               endpoint: "https://first.test",
               access_key_id: "a",
               secret_access_key: "sa"
             },
             %Context{
               endpoint: "https://second.test",
               access_key_id: "b",
               secret_access_key: "sb"
             }
           ] = resolved.contexts

    assert resolved.legacy_args == ["s3://bucket/a//../b", "s3://other/key"]
  end

  test "alias profile is independent of the invocation default", %{tmp_dir: directory} do
    opts = [path: Path.join(directory, "config.toml")]

    :ok =
      Config.save_profile("default", %{access_key_id: "wrong", secret_access_key: "wrong"}, opts)

    :ok =
      Config.save_profile("prod", %{access_key_id: "right", secret_access_key: "secret"}, opts)

    :ok = Aliases.set("prod", %{"endpoint" => "https://prod.test", "profile" => "prod"}, opts)
    {:ok, invocation} = Parser.parse(["rm", "prod/bucket/key"])

    assert {:ok, %{contexts: [%Context{profile: "prod", access_key_id: "right"}]}} =
             Resolver.resolve(invocation, opts)
  end

  test "explicit local paths are retained and alias roots select bucket enumeration", %{
    tmp_dir: directory
  } do
    opts = [path: Path.join(directory, "config.toml")]

    :ok =
      Aliases.set("prod", %{"endpoint" => "https://prod.test", "provider" => "anonymous"}, opts)

    {:ok, upload} = Parser.parse(["cp", "file:///tmp/input", "ess://prod/bucket/key"])

    assert {:ok,
            %{
              legacy_args: ["/tmp/input", "s3://bucket/key"],
              contexts: [nil, %Context{unsigned: true}]
            }} =
             Resolver.resolve(upload, opts)

    {:ok, list} = Parser.parse(["ls", "ess://prod"])

    assert {:ok, %{legacy_args: [], contexts: [%Context{endpoint: "https://prod.test"}]}} =
             Resolver.resolve(list, opts)

    {:ok, invalid} = Parser.parse(["mb", "ess://prod"])
    assert {:error, %Error{category: :syntax}} = Resolver.resolve(invalid, opts)
  end

  test "missing profiles and corrupt config fail before execution", %{tmp_dir: directory} do
    opts = [path: Path.join(directory, "config.toml")]
    {:ok, invocation} = Parser.parse(["ls", "--profile", "missing"])
    assert {:error, %Error{code: :profile_missing}} = Resolver.resolve(invocation, opts)
    File.write!(opts[:path], "[broken")
    {:ok, invocation} = Parser.parse(["ls"])
    assert {:error, %Error{category: :config}} = Resolver.resolve(invocation, opts)
  end
end
