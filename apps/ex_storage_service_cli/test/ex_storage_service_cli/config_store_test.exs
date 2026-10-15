defmodule ExStorageServiceCli.ConfigStoreTest do
  use ExUnit.Case, async: true
  alias ExStorageServiceCli.{Config, Error}
  @moduletag :tmp_dir

  test "profile writes escape values and preserve unrelated TOML data", %{tmp_dir: dir} do
    path = Path.join(dir, "ess/config.toml")
    File.mkdir_p!(Path.dirname(path))

    original = """
    title = "other application"
    [unrelated]
    integers = [1, 2, 9007199254740993]
    enabled = true
    date = 2026-10-15
    [profiles.old]
    endpoint = "http://old.example"
    extra = "keep me"
    """

    File.write!(path, original)

    profile = %{
      endpoint: "https://example.test",
      secret_access_key: "quote\" slash\\ line\nnext",
      access_key_id: "key",
      region: "us-east-1"
    }

    assert :ok = Config.save_profile("name.with\"quotes", profile, path: path)
    assert {:ok, ^profile} = Config.read_profile("name.with\"quotes", path: path)
    assert {:ok, data} = Config.read(path: path)
    assert data["unrelated"]["integers"] == [1, 2, 9_007_199_254_740_993]
    assert data["unrelated"]["date"] == ~D[2026-10-15]
    assert data["profiles"]["old"]["extra"] == "keep me"
    assert data["title"] == "other application"
    assert Bitwise.band(File.stat!(path).mode, 0o777) == 0o600
    assert Bitwise.band(File.stat!(Path.dirname(path)).mode, 0o777) == 0o700
    assert Path.wildcard(Path.join(Path.dirname(path), "*.tmp.*")) == []
  end

  test "malformed configuration is reported without overwriting its bytes", %{tmp_dir: dir} do
    path = Path.join(dir, "config.toml")
    contents = "[default\nsecret_access_key = \"never-print-this\""
    File.write!(path, contents)
    assert {:error, %Error{category: :config} = error} = Config.read(path: path)
    refute inspect(error) =~ "never-print-this"

    assert {:error, %Error{category: :config}} =
             Config.save_profile("default", %{endpoint: "http://example.test"}, path: path)

    assert File.read!(path) == contents
  end

  test "default absence stays compatible and named absence is an explicit error", %{tmp_dir: dir} do
    opts = [path: Path.join(dir, "config.toml")]
    assert {:ok, %{}} = Config.read_profile("default", opts)
    assert {:error, %Error{category: :config}} = Config.read_profile("missing", opts)
  end

  test "symlink files and ancestors are rejected without touching target", %{tmp_dir: dir} do
    target = Path.join(dir, "target.toml")
    File.write!(target, "[default]\nendpoint = \"http://safe.example\"\n")
    link = Path.join(dir, "config.toml")
    File.ln_s!(target, link)
    assert {:error, %Error{category: :config}} = Config.read(path: link)

    assert {:error, %Error{category: :config}} =
             Config.save_profile("default", %{endpoint: "http://changed.example"}, path: link)

    File.mkdir_p!(Path.join(dir, "actual"))
    File.ln_s!(Path.join(dir, "actual"), Path.join(dir, "linked"))

    assert {:error, %Error{category: :config}} =
             Config.save_profile("default", %{}, path: Path.join(dir, "linked/config.toml"))

    assert File.read!(target) =~ "safe.example"
  end

  test "exclusive config lock prevents competing write and is preserved", %{tmp_dir: dir} do
    path = Path.join(dir, "ess/config.toml")
    File.mkdir_p!(Path.dirname(path))
    lock = path <> ".lock"
    File.mkdir!(lock)
    assert {:error, %Error{code: :locked}} = Config.save_profile("default", %{}, path: path)
    assert File.dir?(lock)
    refute File.exists?(path)
    File.rmdir!(lock)
    assert :ok = Config.save_profile("default", %{region: "us-east-1"}, path: path)
    refute File.exists?(lock)
  end

  test "explicit backup retains exact bytes and private permissions", %{tmp_dir: dir} do
    path = Path.join(dir, "config.toml")
    original = "# migration source\n[default]\nendpoint = \"http://example.test\"\n"
    File.write!(path, original)
    assert {:ok, backup} = Config.backup(path: path)
    assert File.read!(backup) == original
    assert File.read!(path) == original
    assert Bitwise.band(File.stat!(backup).mode, 0o777) == 0o600
  end

  test "encoding failure preserves bytes and releases owned lock", %{tmp_dir: dir} do
    path = Path.join(dir, "config.toml")
    original = "title = \"keep\"\n"
    File.write!(path, original)

    assert {:error, %Error{code: :unsupported_value}} =
             Config.update([path: path], fn data -> {:ok, Map.put(data, "invalid", nil)} end)

    assert File.read!(path) == original
    refute File.exists?(path <> ".lock")
    assert Path.wildcard(path <> ".tmp.*") == []
  end

  test "non-table profile namespaces produce typed errors", %{tmp_dir: dir} do
    path = Path.join(dir, "config.toml")
    File.write!(path, "profiles = 4\n")
    assert {:error, %Error{category: :config}} = Config.read_profile("missing", path: path)
    assert {:error, %Error{category: :config}} = Config.save_profile("missing", %{}, path: path)
    assert File.read!(path) == "profiles = 4\n"
  end
end
