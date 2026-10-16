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
  @journal_setup_attempts 4
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

    Enum.find_value(0..(@journal_setup_attempts - 1), fn attempt ->
      state_root = Path.join(root, "journals-#{attempt}")

      case Multipart.upload(
             %Context{endpoint: endpoint, unsigned: true},
             "bucket",
             "key",
             source,
             plan,
             state_root: state_root,
             keep_incomplete: true
           ) do
        {:error, %Error{code: :journal_busy, details: details}} when map_size(details) == 0 ->
          # Initial acquisition can collide with host listeners or TCP TIME_WAIT.
          # Never retry a transfer that reached the wire or acquired upload state.
          refute_receive {:wire_request, _}, 0
          nil

        result ->
          assert {:error, %Error{details: %{journal_id: id, cleanup: "retained"}}} = result
          {id, state_root}
      end
    end) || flunk("No journal fixture mutex available after #{@journal_setup_attempts} attempts")
  end

  test "list is bounded and selected abort preserves the journal outcome", %{tmp_dir: root} do
    {endpoint, _} = WireServer.fixture([{200, [], @create}, {403, [], @denied}, {204, [], ""}])
    {id, state_root} = retained(root, endpoint)
    opts = [state_root: state_root, config_dir: Path.join(root, "config")]

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

    {id, state_root} = retained(root, endpoint)
    opts = [state_root: state_root, config_dir: Path.join(root, "config")]

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
    {id, state_root} = retained(root, endpoint)
    opts = [state_root: state_root, config_dir: Path.join(root, "config")]

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

  test "retained setup advances past an occupied mutex before sending HTTP", %{tmp_dir: root} do
    {root, blocked_root, listener} =
      Enum.find_value(1..10, fn attempt ->
        candidate = Path.join(root, "collision-#{attempt}")
        blocked_root = Path.join(candidate, "journals-0")
        File.mkdir_p!(blocked_root)
        File.chmod!(blocked_root, 0o700)
        {:ok, port} = Journal.lock_port(blocked_root, "index")

        case :gen_tcp.listen(port, [:binary, active: false, ip: {127, 0, 0, 1}]) do
          {:ok, listener} -> {candidate, blocked_root, listener}
          {:error, :eaddrinuse} -> nil
        end
      end) || flunk("Could not reserve an occupied journal fixture mutex")

    try do
      {endpoint, _} = WireServer.fixture([{200, [], @create}, {403, [], @denied}])
      {id, state_root} = retained(root, endpoint)

      assert state_root != blocked_root
      refute File.exists?(Path.join(blocked_root, "index.jsonl"))

      assert {:ok, %{"cleanup" => "retained", "upload_id" => "owned"}} =
               Journal.get(state_root, id)

      assert_receive {:wire_request, %{method: "POST"}}
      assert_receive {:wire_request, %{method: "PUT"}}
      refute_receive {:wire_request, _}, 20
      assert {:ok, _} = :inet.sockname(listener)
    after
      :gen_tcp.close(listener)
    end
  end
end
