Code.require_file("../../support/wire_server.exs", __DIR__)

defmodule ExStorageServiceCli.Transfer.RemoteSourceTest do
  use ExUnit.Case, async: true
  alias ExStorageServiceCli.{Context, Error, Target, Test.WireServer}
  alias ExStorageServiceCli.Transfer.{Copy, RemoteSource, Source}
  @moduletag :tmp_dir
  @head [{"ETag", "\"selected\""}, {"Content-Length", "3"}, {"x-amz-version-id", "v1"}]

  test "full-object staging rejects missing or mismatched GET identity before publication", %{
    tmp_dir: root
  } do
    for headers <- [
          [{"ETag", "\"wrong\""}, {"x-amz-version-id", "v1"}],
          [{"x-amz-version-id", "v1"}],
          [{"ETag", "\"selected\""}, {"x-amz-version-id", "wrong"}],
          [{"ETag", "\"selected\""}]
        ] do
      {endpoint, _} =
        WireServer.fixture([{200, @head, ""}, {200, @head, ""}, {200, headers, "bad"}])

      context = %Context{endpoint: endpoint, unsigned: true}

      assert {:ok, source} =
               RemoteSource.capture(context, "bucket", "key",
                 source_input: %{"VersionId" => "v1"}
               )

      assert {:error, %Error{code: :source_version_mismatch}} =
               RemoteSource.range(source, 0, 3, state_root: root)

      assert Path.wildcard(Path.join(root, "staging/bridge-*")) == []
      assert Path.wildcard(Path.join(root, "staging/.ess-download-*")) == []
      assert_receive {:wire_request, %{method: "HEAD"}}
      assert_receive {:wire_request, %{method: "HEAD"}}
      assert_receive {:wire_request, %{method: "GET", headers: sent}}
      assert sent["if-match"] == "\"selected\""
      refute Map.has_key?(sent, "range")
    end
  end

  test "cross-endpoint copy never uploads a full GET that ignored the selected identity", %{
    tmp_dir: root
  } do
    {source, _} =
      WireServer.fixture([
        {200, @head, ""},
        {200, @head, ""},
        {200, [{"ETag", "\"wrong\""}, {"x-amz-version-id", "v1"}], "bad"},
        {200, @head, ""}
      ])

    {destination, _} =
      WireServer.fixture([
        {200, [{"ETag", "\"uploaded\""}], ""},
        {200, [{"ETag", "\"uploaded\""}, {"Content-Length", "3"}], ""}
      ])

    assert {:error, %Error{code: :source_version_mismatch}} =
             Copy.remote(
               %Context{endpoint: source, unsigned: true},
               %Target{kind: :remote, bucket: "source", key: "key"},
               %Context{endpoint: destination, unsigned: true},
               %Target{kind: :remote, bucket: "destination", key: "key"},
               copy_props: "none",
               state_root: root,
               source_input: %{"VersionId" => "v1"}
             )

    refute_receive {:wire_request, %{method: "PUT"}}
    assert Path.wildcard(Path.join(root, "staging/bridge-*")) == []
  end

  test "matching full-object GET identity publishes the original raw bytes", %{tmp_dir: root} do
    bytes = <<0, 255, 10>>
    {endpoint, _} = WireServer.fixture([{200, @head, ""}, {200, @head, ""}, {200, @head, bytes}])

    assert {:ok, source} =
             RemoteSource.capture(%Context{endpoint: endpoint, unsigned: true}, "bucket", "key")

    assert {:ok, staged} = RemoteSource.range(source, 0, 3, state_root: root)

    try do
      assert File.read!(staged.path) == bytes
    after
      Source.cleanup(staged)
    end

    assert Path.wildcard(Path.join(root, "staging/bridge-*")) == []
  end
end
