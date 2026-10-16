Code.require_file("../../support/wire_server.exs", __DIR__)

defmodule ExStorageServiceCli.CLI.NativeStatOptionsTest do
  use ExUnit.Case, async: true
  alias ExStorageServiceCli.{Context, Error, Target, Test.WireServer}
  alias ExStorageServiceCli.CLI.{NativeInspection, Parser}
  @moduletag :tmp_dir

  defp target(key), do: %Target{kind: :remote, bucket: "bucket", key: key}
  defp context(endpoint), do: %Context{endpoint: endpoint, unsigned: true}
  defp opts(root), do: [tmp_dir: root, max_attempts: 1]

  test "native stat parses no-list independently of list selection" do
    assert {:ok, %{options: %{no_list: true}}} =
             Parser.parse(["stat", "--no-list", "s3://bucket/key"])

    assert {:ok, %{options: %{no_list: false}}} =
             Parser.parse(["stat", "--no-list=false", "s3://bucket/key"])
  end

  test "no-list preserves an exact missing-object error without prefix discovery", %{
    tmp_dir: root
  } do
    {endpoint, _} = WireServer.fixture([{404, [], ""}])

    assert {:error, %Error{http_status: 404}} =
             NativeInspection.stat(
               target("prefix"),
               context(endpoint),
               %{no_list: true},
               opts(root)
             )

    assert_receive {:wire_request, %{method: "HEAD", target: "/bucket/prefix"}}
    refute_receive {:wire_request, _}
  end

  test "no-list HEADs a slash-suffixed object marker literally", %{tmp_dir: root} do
    {endpoint, _} = WireServer.fixture([{200, [{"ETag", "\"marker\""}], ""}])

    assert {:ok, %{"ETag" => "\"marker\""}} =
             NativeInspection.stat(
               target("prefix/"),
               context(endpoint),
               %{no_list: true},
               opts(root)
             )

    assert_receive {:wire_request, %{method: "HEAD", target: "/bucket/prefix/"}}
    refute_receive {:wire_request, _}
  end

  test "no-list on a bucket performs only HeadBucket", %{tmp_dir: root} do
    {endpoint, _} = WireServer.fixture([{200, [{"x-amz-bucket-region", "us-west-2"}], ""}])

    assert {:ok, _} =
             NativeInspection.stat(target(""), context(endpoint), %{no_list: true}, opts(root))

    assert_receive {:wire_request, %{method: "HEAD", target: "/bucket"}}
    refute_receive {:wire_request, _}
  end

  test "bucket no-list preserves the expected owner without object-only members", %{tmp_dir: root} do
    parent = self()

    assert {:ok, %{}} =
             NativeInspection.stat(target(""), %Context{}, %{no_list: true},
               call: fn operation, input ->
                 send(parent, {:bucket_call, operation, input})
                 {:ok, %{}}
               end,
               input: %{"ExpectedBucketOwner" => "123456789012", "SSECustomerKey" => "unused"},
               tmp_dir: root
             )

    assert_receive {:bucket_call, "HeadBucket", input}
    assert input == %{"Bucket" => "bucket", "ExpectedBucketOwner" => "123456789012"}
  end

  test "bucket no-list rejects rewind before provider access", %{tmp_dir: root} do
    assert {:error, %Error{code: :stat_no_list}} =
             NativeInspection.stat(
               target(""),
               %Context{},
               %{no_list: true, rewind: "2026-10-01T00:00:00Z"},
               opts(root)
             )

    refute_receive {:wire_request, _}
    assert File.ls!(root) == []
  end

  test "no-list confirms the selected version of a slash marker", %{tmp_dir: root} do
    {endpoint, _} =
      WireServer.fixture([
        {200, [{"ETag", "\"marker\""}, {"x-amz-version-id", "v1"}], ""}
      ])

    assert {:ok, %{"VersionId" => "v1"}} =
             NativeInspection.stat(
               target("prefix/"),
               context(endpoint),
               %{no_list: true, version_id: "v1"},
               opts(root)
             )

    assert_receive {:wire_request, %{method: "HEAD", target: "/bucket/prefix/?versionId=v1"}}
    refute_receive {:wire_request, _}
  end

  test "no-list rejects an unconfirmed selected slash-marker version", %{tmp_dir: root} do
    for headers <- [[{"x-amz-version-id", "other"}], []] do
      {endpoint, _} = WireServer.fixture([{200, headers, ""}])

      assert {:error, %Error{code: :source_version_mismatch}} =
               NativeInspection.stat(
                 target("prefix/"),
                 context(endpoint),
                 %{no_list: true, version_id: "v1"},
                 opts(root)
               )

      assert_receive {:wire_request, %{method: "HEAD"}}
      refute_receive {:wire_request, _}
    end
  end

  test "no-list rejects conflicting inventory selections before provider access", %{tmp_dir: root} do
    for options <- [%{no_list: true, recursive: true}, %{no_list: true, versions: true}] do
      assert {:error, %Error{code: :stat_no_list}} =
               NativeInspection.stat(target("key"), %Context{}, options, opts(root))
    end

    refute_receive {:wire_request, _}
    assert File.ls!(root) == []
  end

  test "explicitly disabled no-list retains the missing-object prefix fallback", %{tmp_dir: root} do
    {endpoint, _} =
      WireServer.fixture([
        {404, [], ""},
        {200, [], "<ListBucketResult><IsTruncated>false</IsTruncated></ListBucketResult>"}
      ])

    assert {:ok, %{"Entries" => [], "Partial" => false}} =
             NativeInspection.stat(
               target("prefix"),
               context(endpoint),
               %{no_list: false},
               opts(root)
             )

    assert_receive {:wire_request, %{method: "HEAD"}}
    assert_receive {:wire_request, %{method: "GET"}}
    refute_receive {:wire_request, _}
  end
end
