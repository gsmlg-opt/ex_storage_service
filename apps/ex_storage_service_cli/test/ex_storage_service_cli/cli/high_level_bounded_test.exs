Code.require_file("../../support/wire_server.exs", __DIR__)

defmodule ExStorageServiceCli.CLI.HighLevelBoundedTest do
  use ExUnit.Case, async: true
  alias ExStorageServiceCli.{Context, Error, Target, Test.WireServer}
  alias ExStorageServiceCli.CLI.{HighLevel, Invocation}
  @moduletag :tmp_dir
  defp target(key \\ "key"), do: %Target{kind: :remote, bucket: "bucket", key: key}

  defp invocation(command, options \\ %{}),
    do: %Invocation{
      frontend: :native,
      command: [command],
      options: Map.put(options, :progress, false)
    }

  defp opts, do: [max_attempts: 1, emit: fn _ -> :ok end]

  defp version_listing do
    "<ListVersionsResult><IsTruncated>false</IsTruncated><Version><Key>key</Key><VersionId>v1</VersionId><Size>1</Size><ETag>&quot;e&quot;</ETag></Version><DeleteMarker><Key>key</Key><VersionId>v2</VersionId></DeleteMarker></ListVersionsResult>"
  end

  test "head emits raw byte prefix and closes the unfinished stream promptly" do
    {endpoint, _} =
      WireServer.start(fn listener, owner ->
        {:ok, socket} = :gen_tcp.accept(listener, 5000)
        request = WireServer.request(socket)
        send(owner, {:wire_request, request})

        :ok =
          :gen_tcp.send(
            socket,
            "HTTP/1.1 200 OK\r\nContent-Length: 1000000\r\nETag: \"e\"\r\n\r\n" <>
              <<0, 255, 10, 99, 100>>
          )

        send(owner, {:head_stream_closed, :gen_tcp.recv(socket, 0, 3000)})
        :gen_tcp.close(socket)
      end)

    owner = self()
    context = %Context{endpoint: endpoint, unsigned: true}

    assert :ok =
             HighLevel.run(
               invocation("head", %{bytes: 3}),
               [target()],
               [context],
               Keyword.put(opts(), :write, fn bytes ->
                 send(owner, {:bytes, bytes})
                 :ok
               end)
             )

    assert_receive {:bytes, <<0, 255, 10>>}
    assert_receive {:head_stream_closed, {:error, :closed}}, 2000
    refute_receive {:bytes, _}
  end

  test "head defaults to ten lines and stat remains metadata" do
    body = Enum.map_join(1..20, "", &"line#{&1}\n")

    {endpoint, _} =
      WireServer.fixture([
        {200, [], body},
        {200, [{"ETag", "\"e\""}, {"Content-Length", "5"}], ""}
      ])

    owner = self()
    context = %Context{endpoint: endpoint, unsigned: true}

    assert :ok =
             HighLevel.run(
               invocation("head"),
               [target()],
               [context],
               Keyword.put(opts(), :write, fn bytes ->
                 send(owner, {:bytes, bytes})
                 :ok
               end)
             )

    assert_receive {:bytes, prefix}
    assert prefix == Enum.map_join(1..10, "", &"line#{&1}\n")

    assert {:ok, %{"ContentLength" => 5}} =
             HighLevel.run(invocation("stat"), [target()], [context], opts())
  end

  test "head zero and invalid result limits make no network request" do
    assert :ok = HighLevel.run(invocation("head", %{lines: 0}), [target()], [%Context{}], opts())

    assert {:error, %Error{code: :head_limit}} =
             HighLevel.run(
               invocation("head", %{lines: 1, bytes: 1}),
               [target()],
               [%Context{}],
               opts()
             )

    assert {:error, %Error{code: :result_limit}} =
             HighLevel.run(invocation("find", %{limit: 10001}), [target()], [%Context{}], opts())
  end

  test "find and du limit results and explicitly mark partial filtered inventories", %{
    tmp_dir: root
  } do
    File.write!(Path.join(root, "a.txt"), "abc")
    File.write!(Path.join(root, "b.txt"), "de")
    File.write!(Path.join(root, "c.bin"), "xxxx")
    local = %Target{kind: :local, path: root}
    find = %{invocation("find", %{limit: 1}) | filters: [exclude: "*", include: "*.txt"]}

    assert {:ok,
            %{"Entries" => ["a.txt"], "Returned" => 1, "Partial" => true, "MatchedObjects" => 2}} =
             HighLevel.run(find, [local], [nil], opts())

    du = %{find | command: ["du"]}

    assert {:ok, %{"Objects" => 1, "Bytes" => 3, "Partial" => true, "MatchedObjects" => 2}} =
             HighLevel.run(du, [local], [nil], opts())

    assert {:ok, %{"Entries" => [], "Partial" => true}} =
             HighLevel.run(%{find | options: %{limit: 0}}, [local], [nil], opts())
  end

  test "single-file no-follow-symlinks skips the upload", %{tmp_dir: root} do
    path = Path.join(root, "link")
    File.write!(Path.join(root, "actual"), "data")
    File.ln_s!("actual", path)

    assert :ok =
             HighLevel.run(
               invocation("cp", %{follow_symlinks: false}),
               [%Target{kind: :local, path: path}, target()],
               [nil, %Context{}],
               opts()
             )
  end

  test "source region controls signed preflight and GET", %{tmp_dir: root} do
    {endpoint, _} =
      WireServer.fixture([
        {200, [{"ETag", "\"e\""}, {"Content-Length", "1"}], ""},
        {200, [{"ETag", "\"e\""}], "x"}
      ])

    context = %Context{
      endpoint: endpoint,
      access_key_id: "AKID",
      secret_access_key: "secret",
      region: "us-east-1"
    }

    assert :ok =
             HighLevel.run(
               invocation("get", %{source_region: "eu-west-1"}),
               [target(), %Target{kind: :local, path: Path.join(root, "file")}],
               [context, nil],
               opts()
             )

    for method <- ["HEAD", "GET"] do
      assert_receive {:wire_request, %{method: ^method, headers: headers}}
      assert headers["authorization"] =~ "/eu-west-1/s3/aws4_request"
    end
  end

  test "Glacier warns via stderr and ignore suppresses warnings and destination deletion", %{
    tmp_dir: root
  } do
    owner = self()

    {endpoint, _} =
      WireServer.fixture([
        {200, [{"ETag", "\"e\""}, {"Content-Length", "1"}, {"x-amz-storage-class", "GLACIER"}],
         ""}
      ])

    context = %Context{endpoint: endpoint, unsigned: true}

    assert {:error, %Error{category: :skipped, code: :glacier_object}} =
             HighLevel.run(
               invocation("get"),
               [target(), %Target{kind: :local, path: Path.join(root, "file")}],
               [context, nil],
               Keyword.put(opts(), :stderr, &send(owner, {:warning, &1}))
             )

    assert_receive {:warning, "warning: " <> _}
    File.write!(Path.join(root, "old"), "preserve")

    listing =
      "<ListBucketResult><IsTruncated>false</IsTruncated><Contents><Key>prefix/new</Key><Size>1</Size><ETag>&quot;e&quot;</ETag></Contents></ListBucketResult>"

    {endpoint, _} =
      WireServer.fixture([
        {200, [], listing},
        {200, [{"ETag", "\"e\""}, {"Content-Length", "1"}, {"x-amz-storage-class", "GLACIER"}],
         ""}
      ])

    context = %{context | endpoint: endpoint}

    assert :ok =
             HighLevel.run(
               invocation("sync", %{ignore_glacier_warnings: true, delete: true}),
               [target("prefix"), %Target{kind: :local, path: root}],
               [context, nil],
               Keyword.put(opts(), :stderr, &send(owner, {:warning, &1}))
             )

    assert File.read!(Path.join(root, "old")) == "preserve"
    refute_receive {:warning, _}
  end

  test "selected version removal sends its exact identifier" do
    {endpoint, _} = WireServer.fixture([{204, [], ""}])

    assert :ok =
             HighLevel.run(
               invocation("rm", %{version_id: "delete marker+ %"}),
               [target()],
               [%Context{endpoint: endpoint, unsigned: true}],
               opts()
             )

    assert_receive {:wire_request, %{method: "DELETE", target: path}}
    assert path =~ "versionId=delete%20marker%2B%20%25"
  end

  test "all-version removal batches versions and delete markers explicitly" do
    deleted =
      "<DeleteResult><Deleted><Key>key</Key><VersionId>v1</VersionId></Deleted><Deleted><Key>key</Key><VersionId>v2</VersionId></Deleted></DeleteResult>"

    {endpoint, _} = WireServer.fixture([{200, [], version_listing()}, {200, [], deleted}])

    assert :ok =
             HighLevel.run(
               invocation("rm", %{all_versions: true}),
               [target()],
               [%Context{endpoint: endpoint, unsigned: true}],
               opts()
             )

    assert_receive {:wire_request, %{method: "GET", target: path}}
    assert path =~ "versions="
    assert_receive {:wire_request, %{method: "POST", body: body}}
    assert body =~ "<VersionId>v1</VersionId>"
    assert body =~ "<VersionId>v2</VersionId>"
    refute body =~ "<BypassGovernanceRetention>"
  end

  test "batch HTTP success still fails on per-key errors" do
    response =
      "<DeleteResult><Deleted><Key>key</Key><VersionId>v1</VersionId></Deleted><Error><Key>key</Key><VersionId>v2</VersionId><Code>AccessDenied</Code><Message>Denied</Message></Error></DeleteResult>"

    {endpoint, _} = WireServer.fixture([{200, [], version_listing()}, {200, [], response}])

    assert {:error,
            %Error{code: :batch_delete_failed, details: %{failed: 1, codes: ["AccessDenied"]}}} =
             HighLevel.run(
               invocation("rm", %{all_versions: true}),
               [target()],
               [%Context{endpoint: endpoint, unsigned: true}],
               opts()
             )
  end

  test "current recursive removal revalidates ETag and never supplies historical version" do
    listing =
      "<ListBucketResult><IsTruncated>false</IsTruncated><Contents><Key>prefix/key</Key><Size>1</Size><ETag>&quot;e&quot;</ETag></Contents></ListBucketResult>"

    response =
      "<DeleteResult><Deleted><Key>prefix/key</Key><DeleteMarker>true</DeleteMarker><DeleteMarkerVersionId>new-marker</DeleteMarkerVersionId></Deleted></DeleteResult>"

    {endpoint, _} =
      WireServer.fixture([
        {200, [], listing},
        {200, [{"ETag", "\"e\""}, {"Content-Length", "1"}, {"x-amz-version-id", "historical"}],
         ""},
        {200, [], response}
      ])

    context = %Context{
      endpoint: endpoint,
      unsigned: true,
      capabilities: %{conditional_delete: true}
    }

    assert :ok =
             HighLevel.run(
               invocation("rm", %{recursive: true}),
               [target("prefix")],
               [context],
               opts()
             )

    assert_receive {:wire_request, %{method: "GET"}}
    assert_receive {:wire_request, %{method: "HEAD"}}
    assert_receive {:wire_request, %{method: "POST", body: body}}
    assert body =~ "<ETag>"
    refute body =~ "<VersionId>"
  end

  test "version inventory failure prevents all batch requests" do
    first =
      String.replace(
        version_listing(),
        "<IsTruncated>false</IsTruncated>",
        "<IsTruncated>true</IsTruncated><NextKeyMarker>key</NextKeyMarker><NextVersionIdMarker>v2</NextVersionIdMarker>"
      )

    {endpoint, _} =
      WireServer.fixture([
        {200, [], first},
        {500, [], "<Error><Code>InternalError</Code></Error>"}
      ])

    assert {:error, %Error{}} =
             HighLevel.run(
               invocation("rm", %{all_versions: true}),
               [target()],
               [%Context{endpoint: endpoint, unsigned: true}],
               opts()
             )

    assert_receive {:wire_request, %{method: "GET"}}
    assert_receive {:wire_request, %{method: "GET"}}
    refute_receive {:wire_request, %{method: "POST"}}
  end
end
