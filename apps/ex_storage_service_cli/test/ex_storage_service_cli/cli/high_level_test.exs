Code.require_file("../../support/wire_server.exs", __DIR__)

defmodule ExStorageServiceCli.CLI.HighLevelTest do
  use ExUnit.Case, async: true
  alias ExStorageServiceCli.{Context, Error, Target, Test.WireServer}
  alias ExStorageServiceCli.CLI.{HighLevel, HighLevelInput, Invocation}
  @moduletag :tmp_dir
  @listing "<ListBucketResult><IsTruncated>false</IsTruncated></ListBucketResult>"
  defp remote(bucket, key \\ ""), do: %Target{kind: :remote, bucket: bucket, key: key}
  defp local(path), do: %Target{kind: :local, path: path}

  defp invocation(command, options \\ %{}, filters \\ []),
    do: %Invocation{
      frontend: :s3,
      command: [command],
      options: Map.put(options, :progress, false),
      filters: filters
    }

  defp opts, do: [max_attempts: 1, emit: fn _ -> :ok end, progress_write: fn _ -> :ok end]

  defp item(key),
    do:
      "<Contents><Key>" <>
        key <>
        "</Key><Size>1</Size><ETag>&quot;e&quot;</ETag><LastModified>2026-01-01T00:00:00Z</LastModified></Contents>"

  defp listing(items),
    do: "<ListBucketResult><IsTruncated>false</IsTruncated>" <> items <> "</ListBucketResult>"

  test "AWS dry-run displays local paths relative to cwd, including paths outside it", %{
    tmp_dir: root
  } do
    path = Path.join(root, "a.txt")
    File.write!(path, "abc")
    owner = self()

    assert :ok =
             HighLevel.run(
               invocation("cp", %{dryrun: true}),
               [local(path), remote("bucket", "a.txt")],
               [nil, %Context{unsigned: true}],
               Keyword.put(opts(), :emit, &send(owner, {:line, &1}))
             )

    relative = Path.relative_to(path, File.cwd!(), force: true)
    assert_receive {:line, "(dryrun) upload: " <> line}
    assert line == relative <> " to s3://bucket/a.txt"
  end

  test "AWS download preflight failure retains structured service data and transfer status", %{
    tmp_dir: root
  } do
    {endpoint, _} = WireServer.fixture([{403, [], ""}])

    assert {:error, error} =
             HighLevel.run(
               invocation("cp"),
               [remote("bucket", "key"), local(Path.join(root, "destination"))],
               [%Context{endpoint: endpoint, unsigned: true}, nil],
               opts()
             )

    assert error.category == :service
    assert error.operation == "HeadObject"
    assert error.details.high_level_prefix == "fatal error: "
    assert ExStorageServiceCli.ExitStatus.code(error, :s3) == 1
    refute File.exists?(Path.join(root, "destination"))
  end

  test "modeled high-level input maps metadata, grants, SSE-C, and checksum mode", %{
    tmp_dir: root
  } do
    path = Path.join(root, "key")
    File.write!(path, String.duplicate("k", 32))

    assert {:ok, result} =
             HighLevelInput.decode(%{
               metadata: "team=ops",
               grants: ["read=id=owner"],
               sse_c: "AES256",
               sse_c_key: "fileb://" <> path,
               checksum_mode: "ENABLED",
               sse_kms_key_id: "kms-key"
             })

    assert result.input["Metadata"] == %{"team" => "ops"}
    assert result.input["GrantRead"] == "id=owner"
    assert result.input["SSECustomerKey"] == Base.encode64(String.duplicate("k", 32))
    assert result.input["SSEKMSKeyId"] == "kms-key"
    assert result.input["ChecksumMode"] == "ENABLED"
    File.write!(path, String.duplicate("x", 100_000))

    assert {:error, %Error{category: :syntax}} =
             HighLevelInput.decode(%{sse_c_key: "fileb://" <> path})
  end

  test "grant principals retain literal quoting and repeated permissions use the last value" do
    assert {:ok, result} =
             HighLevelInput.decode(%{
               grants: [
                 "read=id=first",
                 "read=uri=http://example.test/group",
                 "full=id=\"owner\""
               ]
             })

    assert result.input["GrantRead"] == "uri=http://example.test/group"
    assert result.input["GrantFullControl"] == "id=\"owner\""

    assert {:error, %Error{category: :syntax}} =
             HighLevelInput.decode(%{grants: ["read=id=owner\r\nx-test: injected"]})
  end

  test "native find name glob matches basenames independently from full-key filters" do
    body = listing(item("dir/a.txt") <> item("dir/a.bin") <> item("secret/b.txt"))
    {endpoint, _} = WireServer.fixture([{200, [], body}])

    invocation = %{
      invocation("find", %{name: "a.*"}, exclude: "secret/*")
      | frontend: :native
    }

    assert {:ok, result} =
             HighLevel.run(
               invocation,
               [remote("bucket")],
               [%Context{endpoint: endpoint, unsigned: true}],
               opts()
             )

    assert result["Entries"] == ["dir/a.bin", "dir/a.txt"]
    assert result["MatchedObjects"] == 2
    refute result["Partial"]
  end

  test "mb region and website fields reach the shared wire encoder" do
    {endpoint, _} = WireServer.fixture([{200, [], ""}, {200, [], ""}])
    context = %Context{endpoint: endpoint, region: "eu-west-1", unsigned: true}
    assert :ok = HighLevel.run(invocation("mb"), [remote("bucket")], [context], opts())
    assert_receive {:wire_request, %{method: "PUT", target: "/bucket", body: body}}
    assert body =~ "<LocationConstraint>eu-west-1</LocationConstraint>"

    assert :ok =
             HighLevel.run(
               invocation("website", %{index_document: "index.html", error_document: "404.html"}),
               [remote("bucket")],
               [context],
               opts()
             )

    assert_receive {:wire_request, %{method: "PUT", target: "/bucket?website=", body: website}}
    assert website =~ "<Suffix>index.html</Suffix>"
    assert website =~ "<Key>404.html</Key>"
  end

  test "presign retains key bytes and produces a timed session signature" do
    owner = self()

    context = %Context{
      endpoint: "https://example.test/base",
      access_key_id: "AKID",
      secret_access_key: "secret",
      session_token: "token"
    }

    assert :ok =
             HighLevel.run(
               invocation("presign", %{expires_in: 60}),
               [remote("bucket", "a +雪")],
               [context],
               now: ~U[2026-01-01 00:00:00Z],
               emit: &send(owner, {:line, &1})
             )

    assert_receive {:line, url}
    assert url =~ "/base/bucket/a%20%2B%E9%9B%AA?"
    assert url =~ "X-Amz-Expires=60"
    assert url =~ "X-Amz-Security-Token=token"
    assert url =~ "X-Amz-Signature="
  end

  test "upload flags and raw bytes flow through Engine", %{tmp_dir: root} do
    path = Path.join(root, "a.json")
    File.write!(path, "{raw}")
    {endpoint, _} = WireServer.fixture([{200, [{"ETag", "\"e\""}], ""}])
    context = %Context{endpoint: endpoint, unsigned: true}

    assert :ok =
             HighLevel.run(
               invocation("cp", %{
                 metadata: "team=ops",
                 content_encoding: "gzip",
                 storage_class: "STANDARD_IA"
               }),
               [local(path), remote("bucket", "key")],
               [nil, context],
               opts()
             )

    assert_receive {:wire_request, %{method: "PUT", body: "{raw}", headers: headers}}
    assert headers["content-type"] == "application/json"
    assert headers["content-encoding"] == "gzip"
    assert headers["x-amz-meta-team"] == "ops"
    assert headers["x-amz-storage-class"] == "STANDARD_IA"
    assert headers["content-length"] == "5"
  end

  test "download pins selected ETag and restores timestamp after atomic publication", %{
    tmp_dir: root
  } do
    {endpoint, _} =
      WireServer.fixture([
        {200, [{"ETag", "\"e\""}, {"Content-Length", "3"}], ""},
        {200, [{"ETag", "\"e\""}, {"Last-Modified", "Thu, 01 Jan 2026 00:00:00 GMT"}], "raw"}
      ])

    context = %Context{endpoint: endpoint, unsigned: true}
    path = Path.join(root, "object")

    assert :ok =
             HighLevel.run(
               invocation("cp"),
               [remote("b", "key"), local(path)],
               [context, nil],
               opts()
             )

    assert File.read!(path) == "raw"
    assert_receive {:wire_request, %{method: "HEAD"}}
    assert_receive {:wire_request, %{method: "GET", headers: headers}}
    assert headers["if-match"] == "\"e\""
    assert File.stat!(path, time: :posix).mtime == 1_767_225_600
  end

  test "failed later inventory page prevents any destination deletion", %{tmp_dir: root} do
    File.write!(Path.join(root, "keep"), "x")

    first =
      "<ListBucketResult><IsTruncated>true</IsTruncated><NextContinuationToken>next</NextContinuationToken>" <>
        item("source/key") <> "</ListBucketResult>"

    {endpoint, _} =
      WireServer.fixture([
        {200, [], first},
        {500, [], "<Error><Code>InternalError</Code></Error>"}
      ])

    context = %Context{endpoint: endpoint, unsigned: true}

    assert {:error, %Error{}} =
             HighLevel.run(
               invocation("sync", %{delete: true}),
               [remote("bucket", "source"), local(root)],
               [context, nil],
               opts()
             )

    assert File.read!(Path.join(root, "keep")) == "x"
    assert_receive {:wire_request, %{method: "GET"}}
    assert_receive {:wire_request, %{method: "GET"}}
    refute_receive {:wire_request, %{method: "DELETE"}}
  end

  test "native get selects the exact source version for preflight and bytes", %{tmp_dir: root} do
    version = "v+ %雪"

    {endpoint, _} =
      WireServer.fixture([
        {200, [{"ETag", "selected"}, {"Content-Length", "3"}, {"x-amz-version-id", version}], ""},
        {200, [{"ETag", "selected"}, {"x-amz-version-id", version}], "raw"}
      ])

    invocation = %{invocation("get", %{version_id: version}) | frontend: :native}
    context = %Context{endpoint: endpoint, unsigned: true}
    path = Path.join(root, "version")

    assert :ok =
             HighLevel.run(
               invocation,
               [remote("bucket", "key"), local(path)],
               [context, nil],
               opts()
             )

    assert File.read!(path) == "raw"

    for method <- ["HEAD", "GET"] do
      assert_receive {:wire_request, %{method: ^method, target: target}}
      assert target == "/bucket/key?versionId=v%2B%20%25%E9%9B%AA"
    end

    assert {:ok, %{source_input: %{"VersionId" => ^version}, input: input}} =
             HighLevelInput.decode(%{version_id: version})

    refute Map.has_key?(input, "VersionId")
  end

  test "native selected versions reject local and recursive sources before requests", %{
    tmp_dir: root
  } do
    File.write!(Path.join(root, "source"), "raw")
    context = %Context{endpoint: "http://unused.invalid", unsigned: true}
    invocation = %{invocation("cp", %{version_id: "selected"}) | frontend: :native}

    assert {:error, %Error{code: :version_selection}} =
             HighLevel.run(
               invocation,
               [local(Path.join(root, "source")), remote("bucket", "key")],
               [nil, context],
               opts()
             )

    assert {:error, %Error{code: :version_selection}} =
             HighLevel.run(
               %{invocation | options: Map.put(invocation.options, :recursive, true)},
               [remote("bucket", "key"), local(root)],
               [context, nil],
               opts()
             )

    refute_receive {:wire_request, _}
  end

  test "a provider ignoring selected version preserves the existing local destination", %{
    tmp_dir: root
  } do
    {endpoint, _} =
      WireServer.fixture([{200, [{"ETag", "current"}, {"Content-Length", "3"}], ""}])

    path = Path.join(root, "existing")
    File.write!(path, "keep")
    invocation = %{invocation("get", %{version_id: "selected"}) | frontend: :native}

    assert {:error, %Error{code: :source_version_mismatch}} =
             HighLevel.run(
               invocation,
               [remote("bucket", "key"), local(path)],
               [%Context{endpoint: endpoint, unsigned: true}, nil],
               opts()
             )

    assert File.read!(path) == "keep"
    refute_receive {:wire_request, %{method: "GET"}}
  end

  test "failed transfer prevents destination-delete phase", %{tmp_dir: root} do
    File.write!(Path.join(root, "new"), "x")

    {endpoint, _} =
      WireServer.fixture([
        {200, [], listing(item("dest/old"))},
        {404, [], ""},
        {403, [], "<Error><Code>AccessDenied</Code></Error>"}
      ])

    context = %Context{endpoint: endpoint, unsigned: true}

    assert {:error, %Error{}} =
             HighLevel.run(
               invocation("sync", %{delete: true}),
               [local(root), remote("bucket", "dest")],
               [nil, context],
               opts()
             )

    assert_receive {:wire_request, %{method: "GET"}}
    assert_receive {:wire_request, %{method: "HEAD"}}
    assert_receive {:wire_request, %{method: "PUT"}}
    refute_receive {:wire_request, %{method: "DELETE"}}
  end

  test "excluded destination entries are protected from deletion", %{tmp_dir: root} do
    {endpoint, _} = WireServer.fixture([{200, [], listing(item("dest/keep.txt"))}])
    context = %Context{endpoint: endpoint, unsigned: true}

    assert :ok =
             HighLevel.run(
               invocation("sync", %{delete: true}, exclude: "*.txt"),
               [local(root), remote("bucket", "dest")],
               [nil, context],
               opts()
             )

    assert_receive {:wire_request, %{method: "GET"}}
    refute_receive {:wire_request, %{method: "DELETE"}}
  end

  test "dryrun reads full inventory and makes no changes", %{tmp_dir: root} do
    File.write!(Path.join(root, "new"), "x")
    {endpoint, _} = WireServer.fixture([{200, [], listing(item("dest/old"))}])
    context = %Context{endpoint: endpoint, unsigned: true}

    assert :ok =
             HighLevel.run(
               invocation("sync", %{delete: true, dryrun: true}),
               [local(root), remote("bucket", "dest")],
               [nil, context],
               opts()
             )

    assert_receive {:wire_request, %{method: "GET"}}
    refute_receive {:wire_request, %{method: "PUT"}}
    refute_receive {:wire_request, %{method: "DELETE"}}
  end

  test "same and overlapping remote prefixes fail before any request" do
    context = %Context{}

    assert {:error, %Error{code: :overlapping_targets}} =
             HighLevel.run(
               invocation("sync"),
               [remote("b", "a"), remote("b", "a/child")],
               [context, context],
               opts()
             )
  end

  test "unsafe remote-to-local names fail before mutation", %{tmp_dir: root} do
    {endpoint, _} = WireServer.fixture([{200, [], listing(item("source/../escape"))}])
    context = %Context{endpoint: endpoint, unsigned: true}

    assert {:error, %Error{code: :unsafe_destination}} =
             HighLevel.run(
               invocation("sync", %{delete: true}),
               [remote("b", "source"), local(root)],
               [context, nil],
               opts()
             )

    assert File.ls!(root) == []
    assert_receive {:wire_request, %{method: "GET"}}
    refute_receive {:wire_request, %{method: "DELETE"}}
  end

  test "ls pagination renders summaries and does not buffer listings" do
    owner = self()
    {endpoint, _} = WireServer.fixture([{200, [], listing(item("prefix/a"))}])
    context = %Context{endpoint: endpoint, unsigned: true}

    assert :ok =
             HighLevel.run(
               invocation("ls", %{summarize: true}),
               [remote("b", "prefix/")],
               [context],
               Keyword.put(opts(), :emit, &send(owner, {:line, &1}))
             )

    assert_receive {:line, line}
    assert line =~ "a"
    assert_receive {:line, "\nTotal Objects: 1"}
    assert_receive {:line, "   Total Size: 1"}
    assert_receive {:wire_request, %{target: target}}
    assert target =~ "delimiter=%2F"
  end

  test "empty force rb removes bucket after completed current-object listing" do
    {endpoint, _} = WireServer.fixture([{200, [], @listing}, {204, [], ""}])
    context = %Context{endpoint: endpoint, unsigned: true}

    assert :ok =
             HighLevel.run(
               invocation("rb", %{force: true}),
               [remote("bucket")],
               [context],
               opts()
             )

    assert_receive {:wire_request, %{method: "GET"}}
    assert_receive {:wire_request, %{method: "DELETE", target: "/bucket"}}
  end
end
