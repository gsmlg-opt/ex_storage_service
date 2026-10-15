defmodule ExStorageServiceCli.AliasesTest do
  use ExUnit.Case, async: true
  alias ExStorageServiceCli.{Aliases, Config, Error}
  @moduletag :tmp_dir

  test "aliases reference profiles and coexist with unrelated profiles", %{tmp_dir: dir} do
    opts = [path: Path.join(dir, "ess/config.toml")]
    assert :ok = Config.save_profile("production", %{endpoint: "https://profile.example"}, opts)

    record = %{
      "endpoint" => "https://alias.example",
      "profile" => "production",
      "provider" => "ess-profile"
    }

    assert :ok = Aliases.set("store", record, opts)
    assert {:ok, ^record} = Aliases.get("store", opts)
    assert {:ok, %{"store" => ^record}} = Aliases.list(opts)
    assert :ok = Aliases.remove("store", opts)
    assert {:error, %Error{category: :config}} = Aliases.get("store", opts)
    assert {:ok, %{endpoint: "https://profile.example"}} = Config.read_profile("production", opts)
  end

  test "list and default export redact static credentials; explicit export retains them", %{
    tmp_dir: dir
  } do
    opts = [path: Path.join(dir, "config.toml")]

    record = %{
      "endpoint" => "https://example.test",
      "provider" => "static",
      "access_key_id" => "key",
      "secret_access_key" => "secret-never-print",
      "session_token" => "token-never-print"
    }

    assert :ok = Aliases.set("store", record, opts)
    assert {:ok, aliases} = Aliases.list(opts)
    refute inspect(aliases) =~ "secret-never-print"
    refute inspect(aliases) =~ "token-never-print"
    assert {:ok, exported} = Aliases.export(opts)
    refute exported =~ "secret-never-print"
    assert {:ok, complete} = Aliases.export(Keyword.put(opts, :include_secrets, true))
    assert complete =~ "secret-never-print"
    assert {:error, %Error{category: :config}} = Aliases.import(complete, opts)
  end

  test "import preview does not write and import merges only with explicit overwrite", %{
    tmp_dir: dir
  } do
    opts = [path: Path.join(dir, "config.toml")]

    document =
      JSON.encode!(%{
        "version" => 1,
        "aliases" => %{
          "store" => %{
            "endpoint" => "https://example.test",
            "provider" => "ess-profile",
            "profile" => "production"
          }
        }
      })

    assert {:ok, [%{name: "store", action: :create}]} = Aliases.preview_import(document, opts)
    refute File.exists?(opts[:path])
    assert :ok = Aliases.import(document, opts)
    assert {:error, %Error{code: :alias_exists}} = Aliases.import(document, opts)

    assert {:ok, [%{name: "store", action: :replace}]} =
             Aliases.preview_import(document, Keyword.put(opts, :overwrite, true))

    assert :ok = Aliases.import(document, Keyword.put(opts, :overwrite, true))
  end

  test "invalid aliases and credentials fail without leaking values or partial writes", %{
    tmp_dir: dir
  } do
    opts = [path: Path.join(dir, "config.toml")]

    for {name, record} <- [
          {"../bad", %{"endpoint" => "https://example.test"}},
          {"bad", %{"endpoint" => "https://user:secret-never-print@example.test"}},
          {"bad", %{"endpoint" => "file:///tmp/object"}},
          {"bad",
           %{
             "endpoint" => "https://example.test",
             "provider" => "static",
             "access_key_id" => "key"
           }},
          {"bad",
           %{
             "endpoint" => "https://example.test",
             "profile" => "production",
             "access_key_id" => "key",
             "secret_access_key" => "secret-never-print"
           }}
        ] do
      assert {:error, %Error{} = error} = Aliases.set(name, record, opts)
      refute inspect(error) =~ "secret-never-print"
    end

    refute File.exists?(opts[:path])
    assert {:error, %Error{}} = Aliases.import("not JSON secret-never-print", opts)
  end
end
