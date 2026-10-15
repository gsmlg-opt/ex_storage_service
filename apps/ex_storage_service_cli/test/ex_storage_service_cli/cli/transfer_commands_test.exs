Code.require_file("../../support/wire_server.exs", __DIR__)

defmodule ExStorageServiceCli.CLI.TransferCommandsTest do
  # Journal mutex ports share the host namespace with concurrent wire fixtures.
  use ExUnit.Case, async: false
  alias ExStorageServiceCli.{Context, Error}
  alias ExStorageServiceCli.CLI.{Runtime, TransferCommands}
  alias ExStorageServiceCli.Transfer.{Journal, Multipart, Planner, Source}
  alias ExStorageServiceCli.Test.WireServer
  @moduletag :tmp_dir
  @create "<InitiateMultipartUploadResult><UploadId>owned</UploadId></InitiateMultipartUploadResult>"
  @denied "<Error><Code>AccessDenied</Code></Error>"
  @limits %{
    max_single_put_bytes: 5,
    min_nonfinal_part_bytes: 5,
    max_part_bytes: 10,
    max_parts: 10,
    max_object_bytes: 100
  }

  defp retained(root, endpoint) do
    path = Path.join(root, "source")
    File.write!(path, "abcdefghij")
    {:ok, source} = Source.file(path)
    {:ok, plan} = Planner.plan(10, @limits)

    assert {:error, %Error{details: %{journal_id: id, cleanup: "retained"}}} =
             Multipart.upload(
               %Context{endpoint: endpoint, unsigned: true},
               "bucket",
               "key",
               source,
               plan,
               state_root: Path.join(root, "journals"),
               keep_incomplete: true
             )

    id
  end

  test "list is bounded and selected abort preserves the journal outcome", %{tmp_dir: root} do
    {endpoint, _} = WireServer.fixture([{200, [], @create}, {403, [], @denied}, {204, [], ""}])
    id = retained(root, endpoint)
    opts = [state_root: Path.join(root, "journals"), config_dir: Path.join(root, "config")]

    assert {:ok, %{entries: [entry], next: cursor}} =
             TransferCommands.run("list", [], %{limit: 1}, nil, opts)

    assert is_integer(cursor)

    assert {:ok, %{entries: [], next: nil}} =
             TransferCommands.run("list", [], %{limit: 1, cursor: cursor}, nil, opts)

    assert entry["id"] == id
    assert entry["phase"] == "failed"

    assert {:ok, %{cleanup: "aborted"}} =
             Runtime.with_invocation(fn runtime ->
               TransferCommands.run("abort", [id], %{}, runtime, opts)
             end)

    assert {:ok, %{"cleanup" => "aborted"}} = Journal.get(opts[:state_root], id)
    assert_receive {:wire_request, %{method: "POST"}}
    assert_receive {:wire_request, %{method: "PUT"}}
    assert_receive {:wire_request, %{method: "DELETE", target: "/bucket/key?uploadId=owned"}}
  end

  test "resume resolves the journal context and reconciles parts before replaying", %{
    tmp_dir: root
  } do
    {endpoint, _} =
      WireServer.fixture([
        {200, [], @create},
        {403, [], @denied},
        {200, [], "<ListPartsResult><IsTruncated>false</IsTruncated></ListPartsResult>"},
        {200, [{"ETag", "\"part1\""}], ""},
        {200, [{"ETag", "\"part2\""}], ""},
        {200, [],
         "<CompleteMultipartUploadResult><ETag>&quot;done-2&quot;</ETag></CompleteMultipartUploadResult>"},
        {200, [{"ETag", "\"done-2\""}, {"Content-Length", "10"}], ""}
      ])

    id = retained(root, endpoint)
    opts = [state_root: Path.join(root, "journals"), config_dir: Path.join(root, "config")]

    assert {:ok, %{bytes: 10, journal_id: ^id}} =
             Runtime.with_invocation(fn runtime ->
               TransferCommands.run("resume", [id], %{}, runtime, opts)
             end)

    assert {:ok, %{"phase" => "completed"}} = Journal.get(opts[:state_root], id)
    assert_receive {:wire_request, %{method: "POST"}}
    assert_receive {:wire_request, %{method: "PUT"}}

    assert_receive {:wire_request, %{method: "GET", target: "/bucket/key?uploadId=owned"}}
  end

  test "missing state lists empty and changed endpoint cannot abort an owned upload", %{
    tmp_dir: root
  } do
    assert {:ok, %{entries: [], next: nil}} =
             TransferCommands.run("list", [], %{}, nil, state_root: Path.join(root, "absent"))

    {endpoint, _} = WireServer.fixture([{200, [], @create}, {403, [], @denied}])
    id = retained(root, endpoint)
    opts = [state_root: Path.join(root, "journals"), config_dir: Path.join(root, "config")]

    assert {:error, %Error{code: :resume_conflict}} =
             Runtime.with_invocation(fn runtime ->
               TransferCommands.run(
                 "abort",
                 [id],
                 %{endpoint: "http://127.0.0.1:1"},
                 runtime,
                 opts
               )
             end)

    assert_receive {:wire_request, _}
    assert_receive {:wire_request, _}
    refute_receive {:wire_request, _}, 20
  end
end
