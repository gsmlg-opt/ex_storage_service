defmodule ExStorageServiceCli.CLI.NativeShareTest do
  use ExUnit.Case, async: true
  import ExUnit.CaptureIO
  alias ExStorageServiceCli.{Context, Error, Target}
  alias ExStorageServiceCli.CLI.{Invocation, NativeShare, Parser, Runner}
  alias ExStorageServiceCli.CLI.NativeShare.History
  alias ExStorageServiceCli.S3.{PostPolicy, Presign}
  @moduletag :tmp_dir
  @now ~U[2026-10-15 00:00:00Z]
  @reference JSON.decode!(
               File.read!(Path.expand("../../fixtures/s3/sharing-mc-capture.json", __DIR__))
             )

  defp context do
    %Context{
      frontend: :native,
      endpoint: "https://s3.us-east-1.amazonaws.com",
      region: "us-east-1",
      access_key_id: "fixture-access",
      secret_access_key: "fixture-secret",
      session_token: "fixture-token"
    }
  end

  defp target(key \\ "folder/a +雪"), do: %Target{kind: :remote, bucket: "bucket", key: key}

  defp invocation(action, options \\ %{}),
    do: %Invocation{frontend: :native, command: ["share", action], options: options}

  defp opts(dir), do: [config_dir: dir, now: @now]

  test "semantic options match normalized actual pinned mc captures", %{tmp_dir: dir} do
    for recorded <- @reference["cases"] do
      assert {:ok, parsed} =
               Parser.parse(
                 ["share", recorded["action"], "s3://bucket/folder/a"] ++ recorded["options"]
               )

      assert {:ok, share} = NativeShare.run(parsed, target("folder/a"), context(), opts(dir))

      if expected = recorded["query"] do
        query = URI.decode_query(URI.parse(share["url"]).query)
        assert Map.take(query, Map.keys(expected)) == expected
      else
        policy = share["fields"]["policy"] |> Base.decode64!() |> JSON.decode!()
        assert recorded["policy_condition"] in policy["conditions"]
      end
    end
  end

  test "duration aliases repeat with last value winning and preserve legacy seconds", %{
    tmp_dir: dir
  } do
    assert {:ok, parsed} =
             Parser.parse([
               "share",
               "download",
               "s3://bucket/key",
               "--expire",
               "1h",
               "--expire",
               "2m30s"
             ])

    assert {:ok, share} = NativeShare.run(parsed, target(), context(), opts(dir))
    assert share["expires"] == 150

    assert {:ok, share} =
             NativeShare.run(
               invocation("download", %{expires: 42}),
               target(),
               context(),
               opts(dir)
             )

    assert share["expires"] == 42

    for options <- [
          %{expire: "0s"},
          %{expire: "8d"},
          %{expire: "169h"},
          %{expire: "1s", expires: 2}
        ] do
      assert {:error, %Error{}} =
               NativeShare.run(invocation("download", options), target(), context(), opts(dir))
    end
  end

  test "single downloads reject every explicit batch limit before signing or history", %{
    tmp_dir: dir
  } do
    for limit <- [-1, 1, 100_000], recursive <- [%{}, %{recursive: false}] do
      options = Map.put(recursive, :limit, limit)

      assert {:error,
              %Error{
                category: :syntax,
                code: :invalid_share,
                message: "--limit requires recursive download sharing"
              }} =
               NativeShare.run(
                 invocation("download", options),
                 target(),
                 %{context() | unsigned: true},
                 opts(dir)
               )

      assert File.ls!(dir) == []
    end
  end

  test "selected GET version is encoded before signing without changing PUT or unselected defaults",
       %{tmp_dir: dir} do
    version = "v +/雪"

    assert {:ok, share} =
             NativeShare.run(
               invocation("download", %{version_id: version}),
               target(),
               context(),
               opts(dir)
             )

    query = URI.decode_query(URI.parse(share["url"]).query)
    assert query["versionId"] == version
    assert query["X-Amz-Security-Token"] == "fixture-token"
    assert {:ok, unselected} = Presign.url(context(), target(), "GET", now: @now)
    refute URI.decode_query(URI.parse(unselected["url"]).query)["versionId"]

    refute query["X-Amz-Signature"] ==
             URI.decode_query(URI.parse(unselected["url"]).query)["X-Amz-Signature"]

    assert {:error, %Error{}} =
             Presign.url(context(), target(), "PUT", now: @now, version_id: version)

    assert {:error, %Error{}} =
             NativeShare.run(
               invocation("download", %{version_id: version, recursive: true}),
               target(),
               context(),
               opts(dir)
             )
  end

  test "private offline lists redact capability material and filter expired shares", %{
    tmp_dir: dir
  } do
    assert {:ok, share} =
             NativeShare.run(
               invocation("download", %{expires: 10}),
               target(),
               context(),
               opts(dir)
             )

    assert {:ok, %{"Shares" => [record]}} = History.list("download", opts(dir))
    refute JSON.encode!(record) =~ "X-Amz"
    refute Map.has_key?(record, "share")

    assert {:ok, %{"Shares" => [%{"share" => ^share}]}} =
             History.list("download", opts(dir) ++ [reveal: true])

    assert {:ok, %{"Shares" => []}} = History.list("upload", opts(dir))

    assert {:ok, %{"Shares" => []}} =
             History.list("download", config_dir: dir, now: DateTime.add(@now, 10))

    assert {:ok, stat} = File.stat(Path.join(dir, "shares.toml"))
    assert Bitwise.band(stat.mode, 0o777) == 0o600
    refute File.read!(Path.join(dir, "shares.toml")) =~ "fixture-secret"

    output =
      capture_io(fn ->
        assert :ok =
                 Runner.run([
                   "share",
                   "list",
                   "download",
                   "--config-dir",
                   dir,
                   "--output",
                   "json"
                 ])
      end)

    refute output =~ "X-Amz"
    assert JSON.decode!(output)["Shares"] == []
  end

  test "history is fail-closed on unsafe permissions, corruption, symlinks and writer locks", %{
    tmp_dir: dir
  } do
    assert {:ok, _} = NativeShare.run(invocation("download"), target(), context(), opts(dir))
    path = Path.join(dir, "shares.toml")
    original = File.read!(path)
    File.chmod!(path, 0o644)
    assert {:error, %Error{}} = History.list("download", opts(dir) ++ [reveal: true])
    File.chmod!(path, 0o600)
    File.mkdir!(path <> ".lock")

    assert {:error, %Error{code: :locked}} =
             NativeShare.run(invocation("download"), target("other"), context(), opts(dir))

    assert File.read!(path) == original
    File.rmdir!(path <> ".lock")
    File.write!(path, "broken = [")
    assert {:error, %Error{}} = History.list("download", opts(dir))
    File.rm!(path)
    File.ln_s!(Path.join(dir, "missing"), path)
    assert {:error, %Error{}} = History.list("download", opts(dir))
  end

  test "recursive download publishes history only after complete bounded inventory", %{
    tmp_dir: dir
  } do
    fetch = fn "ListObjectsV2", %{"Prefix" => "folder/"} ->
      {:ok,
       %{
         "IsTruncated" => false,
         "Contents" => [
           %{"Key" => "folder/a", "Size" => 1},
           %{"Key" => "folder/b", "Size" => 2},
           %{"Key" => "folder/", "Size" => 0}
         ]
       }}
    end

    assert {:ok, %{"Shares" => shares, "Count" => 2}} =
             NativeShare.run(
               invocation("download", %{recursive: true}),
               target("folder"),
               context(),
               opts(dir) ++ [fetch: fetch]
             )

    assert Enum.map(shares, & &1["key"]) == ["folder/a", "folder/b"]
    assert {:ok, %{"Shares" => records}} = History.list("download", opts(dir))
    assert length(records) == 2
    original = File.read!(Path.join(dir, "shares.toml"))

    assert {:error, %Error{code: :share_limit}} =
             NativeShare.run(
               invocation("download", %{recursive: true, limit: 1}),
               target("folder"),
               context(),
               opts(dir) ++ [fetch: fetch]
             )

    assert File.read!(Path.join(dir, "shares.toml")) == original

    failing = fn "ListObjectsV2", input ->
      if input["ContinuationToken"],
        do: {:error, Error.new(:service, "fixture denied")},
        else:
          {:ok,
           %{
             "Contents" => [%{"Key" => "folder/c", "Size" => 1}],
             "IsTruncated" => true,
             "NextContinuationToken" => "next"
           }}
    end

    assert {:error, %Error{}} =
             NativeShare.run(
               invocation("download", %{recursive: true}),
               target("folder"),
               context(),
               opts(dir) ++ [fetch: failing]
             )

    assert File.read!(Path.join(dir, "shares.toml")) == original
  end

  test "recursive upload signs a real constrained POST prefix while exact POST stays exact", %{
    tmp_dir: dir
  } do
    assert {:ok, share} =
             NativeShare.run(
               invocation("upload", %{
                 recursive: true,
                 content_type: "image/png",
                 min_bytes: 1,
                 max_bytes: 100
               }),
               target("images/"),
               context(),
               opts(dir)
             )

    assert share["method"] == "POST"
    assert share["fields"]["key"] == "images/${filename}"
    policy = share["fields"]["policy"] |> Base.decode64!() |> JSON.decode!()
    assert ["starts-with", "$key", "images/"] in policy["conditions"]
    assert ["content-length-range", 1, 100] in policy["conditions"]
    assert %{"Content-Type" => "image/png"} in policy["conditions"]
    assert %{"x-amz-security-token" => "fixture-token"} in policy["conditions"]
    refute Enum.any?(policy["conditions"], &(is_map(&1) and Map.has_key?(&1, "key")))
    assert policy["expiration"] == "2026-10-15T01:00:00Z"
    key = :crypto.mac(:hmac, :sha256, "AWS4fixture-secret", "20261015")

    key =
      Enum.reduce(["us-east-1", "s3", "aws4_request"], key, fn part, acc ->
        :crypto.mac(:hmac, :sha256, acc, part)
      end)

    assert share["fields"]["x-amz-signature"] ==
             Base.encode16(:crypto.mac(:hmac, :sha256, key, share["fields"]["policy"]),
               case: :lower
             )

    assert {:ok, exact} = PostPolicy.generate(context(), target("images/a"), now: @now)

    assert %{"key" => "images/a"} in (exact["fields"]["policy"]
                                      |> Base.decode64!()
                                      |> JSON.decode!())["conditions"]

    assert {:error, %Error{}} =
             NativeShare.run(
               invocation("upload", %{recursive: true}),
               target(""),
               context(),
               opts(dir)
             )

    assert {:error, %Error{}} =
             NativeShare.run(
               invocation("upload", %{recursive: true}),
               target("images/"),
               %{context() | capabilities: %{provider: "ess"}},
               opts(dir)
             )

    assert {:ok, %{"Shares" => [record]}} = History.list("upload", opts(dir))
    refute JSON.encode!(record) =~ "fixture-token"
  end

  test "history paginates, prunes only expired entries on write, and rejects oversized batches atomically",
       %{tmp_dir: dir} do
    for key <- ~w(a b c) do
      assert {:ok, _} =
               NativeShare.run(
                 invocation("download", %{expires: 10}),
                 target(key),
                 context(),
                 opts(dir)
               )
    end

    assert {:ok, %{"Shares" => first, "NextCursor" => 2}} =
             History.list("download", opts(dir) ++ [limit: 2])

    assert {:ok, %{"Shares" => last}} =
             History.list("download", opts(dir) ++ [limit: 2, cursor: 2])

    assert length(first) == 2 and length(last) == 1
    assert length(Enum.uniq_by(first ++ last, & &1["id"])) == 3
    path = Path.join(dir, "shares.toml")
    original = File.read!(path)

    shares =
      for n <- 1..1001,
          do:
            {%{"url" => "https://example.invalid/" <> Integer.to_string(n), "expires" => 10},
             target(Integer.to_string(n)), nil}

    assert {:error, %Error{code: :share_history_limit}} =
             History.record("download", shares, opts(dir))

    assert File.read!(path) == original

    oversized =
      {%{
         "url" => "https://example.invalid/" <> String.duplicate("x", 1_048_576),
         "expires" => 10
       }, target("large"), nil}

    assert {:error, %Error{code: :too_large}} = History.record("download", [oversized], opts(dir))
    assert File.read!(path) == original

    assert {:ok, _} =
             NativeShare.run(invocation("download"), target("fresh"), context(),
               config_dir: dir,
               now: DateTime.add(@now, 10)
             )

    refute File.read!(path) =~ ~s("key" = "a")

    assert {:ok, %{"Shares" => [%{"key" => "fresh"}]}} =
             History.list("download", config_dir: dir, now: DateTime.add(@now, 10))

    for params <- [[limit: 0], [limit: 1001], [cursor: -1]] do
      assert {:error, %Error{}} = History.list("download", opts(dir) ++ params)
    end
  end

  test "legacy upload stays PUT and an empty recursive download remains an empty complete batch",
       %{tmp_dir: dir} do
    assert {:ok, %{"method" => "PUT", "url" => url}} =
             NativeShare.run(invocation("upload"), target(), context(), opts(dir))

    assert URI.decode_query(URI.parse(url).query)["X-Amz-Expires"] == "3600"
    fetch = fn "ListObjectsV2", _ -> {:ok, %{"IsTruncated" => false, "Contents" => []}} end

    assert {:ok, %{"Count" => 0, "Shares" => []}} =
             NativeShare.run(
               invocation("download", %{recursive: true}),
               target("folder/"),
               context(),
               opts(dir) ++ [fetch: fetch]
             )
  end

  test "list variants are explicit zero-target commands and unsupported forms fail" do
    for kind <- ~w(download upload) do
      assert {:ok, %Invocation{command: ["share", "list", ^kind], args: []}} =
               Parser.parse(["share", "list", kind])

      assert {:error, %Error{}} = Parser.parse(["share", "list", kind, "s3://bucket/key"])
    end

    assert {:error, %Error{}} = Parser.parse(["share", "list"])

    assert {:error, %Error{}} =
             Parser.parse(["share", "upload", "s3://bucket/key", "--version-id", "v"])
  end
end
