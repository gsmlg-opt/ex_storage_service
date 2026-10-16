Code.require_file("../../support/wire_server.exs", __DIR__)

defmodule ExStorageServiceCli.CLI.NativePutParallelTest do
  use ExUnit.Case, async: false
  alias ExStorageServiceCli.{Context, Error, Target, Test.WireServer}
  alias ExStorageServiceCli.CLI.{HighLevel, Invocation, Parser}
  alias ExStorageServiceCli.Transfer.{Coordinator, Journal}
  alias ExStorageServiceCli.{S3.Request, Transport.HTTPFetch}
  @moduletag :tmp_dir
  @limits %{
    max_single_put_bytes: 100,
    min_nonfinal_part_bytes: 5,
    max_part_bytes: 100,
    max_parts: 100,
    max_object_bytes: 1000
  }
  @create "<InitiateMultipartUploadResult><UploadId>owned</UploadId></InitiateMultipartUploadResult>"
  @complete "<CompleteMultipartUploadResult><ETag>&quot;done&quot;</ETag></CompleteMultipartUploadResult>"

  defp invocation(options),
    do: %Invocation{
      frontend: :native,
      command: ["put"],
      options: Map.merge(%{part_size: "5B", progress: false}, options)
    }

  defp run(root, endpoint, coordinator, data, flags, opts) do
    path = Path.join(root, "source")
    File.write!(path, data)

    context = %Context{
      endpoint: endpoint,
      unsigned: true,
      coordinator: coordinator,
      options: %{request_checksum_calculation: "when_required"}
    }

    HighLevel.run(
      invocation(flags),
      [%Target{kind: :local, path: path}, %Target{kind: :remote, bucket: "bucket", key: "key"}],
      [nil, context],
      Keyword.merge(
        [
          limits: @limits,
          state_root: Path.join(root, "journals"),
          max_attempts: 1,
          emit: fn _ -> :ok end
        ],
        opts
      )
    )
  end

  defp async_run(root, endpoint, coordinator, data, flags, opts \\ []) do
    owner = self()

    {pid, monitor} =
      spawn_monitor(fn ->
        send(owner, {:upload_result, run(root, endpoint, coordinator, data, flags, opts)})
      end)

    on_exit(fn -> if Process.alive?(pid), do: Process.exit(pid, :kill) end)
    {pid, monitor}
  end

  defp journal(root) do
    {:ok, %{entries: [entry]}} = Journal.list(Path.join(root, "journals"))
    {:ok, record} = Journal.get(Path.join(root, "journals"), entry["id"])
    record
  end

  defp reply(peer, number, status \\ 200) do
    headers = if status == 200, do: [{"ETag", "\"part#{number}\""}], else: []
    body = if status == 200, do: "", else: "<Error><Code>AccessDenied</Code></Error>"
    send(peer, {:reply, status, headers, body})
  end

  defp peer(size) do
    {:ok, sockets} = Agent.start_link(fn -> %{} end)
    ExUnit.Callbacks.on_exit(fn -> if Process.alive?(sockets), do: Agent.stop(sockets) end)
    WireServer.start(fn listener, owner -> accept(listener, owner, sockets, size) end)
  end

  defp accept(listener, owner, sockets, size) do
    case :gen_tcp.accept(listener, 10_000) do
      {:ok, socket} ->
        spawn_link(fn ->
          try do
            request = WireServer.request(socket)
            query = URI.decode_query(URI.parse(request.target).query || "")

            cond do
              request.method == "POST" and Map.has_key?(query, "uploads") ->
                send(owner, {:create, request})
                WireServer.response(socket, 200, [], @create)

              request.method == "PUT" and query["partNumber"] ->
                number = String.to_integer(query["partNumber"])
                peer = self()
                Agent.update(sockets, &Map.put(&1, peer, socket))
                send(owner, {:part, number, self(), request})

                receive do
                  {:reply, status, headers, body} ->
                    WireServer.response(socket, status, headers, body)

                  :hold ->
                    receive do
                      :finish -> :ok
                    after
                      10_000 -> :ok
                    end
                after
                  10_000 -> :ok
                end

              request.method == "DELETE" ->
                # Check the stalled peers' actual passive sockets before acknowledging abort.
                status =
                  Agent.get(sockets, fn peers ->
                    Map.new(peers, fn {pid, peer} -> {pid, :gen_tcp.recv(peer, 0, 0)} end)
                  end)

                send(owner, {:abort, request, status})
                WireServer.response(socket, 204, [], "")
                Agent.get(sockets, &Map.keys/1) |> Enum.each(&send(&1, :finish))

              request.method == "POST" ->
                send(owner, {:complete, request})

                WireServer.response(
                  socket,
                  200,
                  [{"x-amz-version-id", "created-version"}],
                  @complete
                )

              request.method == "HEAD" ->
                send(owner, {:head, request})

                WireServer.response(
                  socket,
                  200,
                  [
                    {"Content-Length", to_string(size)},
                    {"ETag", "\"done\""},
                    {"x-amz-version-id", "created-version"}
                  ],
                  ""
                )

              true ->
                send(owner, {:unexpected, request})
            end
          rescue
            error -> send(owner, {:peer_error, error})
          after
            :gen_tcp.close(socket)
          end
        end)

        accept(listener, owner, sockets, size)

      {:error, _} ->
        :ok
    end
  end

  test "parallel flag is put-only and invalid limits or directions fail before source access", %{
    tmp_dir: root
  } do
    assert {:ok, %{options: %{parallel: 2}}} =
             Parser.parse(["put", "source", "s3://bucket/key", "--parallel", "2"])

    for command <- [["cp"], ["s3", "cp"], ["get"]] do
      assert {:error, %Error{}} =
               Parser.parse(command ++ ["source", "s3://bucket/key", "--parallel", "2"])
    end

    for value <- [0, -1, 65, "2"] do
      assert {:error, %Error{category: :syntax, code: :native_put_option}} =
               HighLevel.run(
                 invocation(%{parallel: value}),
                 [
                   %Target{kind: :local, path: Path.join(root, "missing")},
                   %Target{kind: :remote, bucket: "bucket", key: "key"}
                 ],
                 [nil, %Context{}],
                 limits: @limits
               )
    end

    assert {:error, %Error{category: :syntax, code: :native_put_option}} =
             HighLevel.run(
               invocation(%{parallel: 2, disable_multipart: true}),
               [
                 %Target{kind: :local, path: "missing"},
                 %Target{kind: :remote, bucket: "bucket", key: "key"}
               ],
               [nil, %Context{}],
               limits: @limits
             )
  end

  test "two actual concurrent parts finish out of order with fenced checkpoints and ordered completion",
       %{tmp_dir: root} do
    pool = start_supervised!(Coordinator)
    signal = HTTP.AbortController.new()
    original_signal = Agent.get(signal, & &1)
    on_exit(fn -> if Process.alive?(signal), do: Agent.stop(signal) end)
    {endpoint, _} = peer(15)

    {_owner, _monitor} =
      async_run(
        root,
        endpoint,
        pool,
        "abcdefghijklmno",
        %{
          parallel: 2,
          if_not_exists: true,
          checksum: "SHA256"
        },
        signal: signal
      )

    assert_receive {:create, _}, 1000
    assert_receive {:part, a, pa, _}, 1000
    assert_receive {:part, b, pb, _}, 1000
    assert Enum.sort([a, b]) == [1, 2]
    refute_receive {:part, 3, _, _}
    peers = %{a => pa, b => pb}
    reply(peers[2], 2)
    assert_receive {:part, 3, p3, _}, 1000
    reply(p3, 3)
    refute_receive {:complete, _}
    reply(peers[1], 1)
    assert_receive {:complete, complete}, 1000
    assert complete.headers["if-none-match"] == "*"

    assert [_, one, two, three] =
             Regex.run(
               ~r/.*<PartNumber>(1)<\/PartNumber>.*<PartNumber>(2)<\/PartNumber>.*<PartNumber>(3)<\/PartNumber>/,
               complete.body
             )

    assert [one, two, three] == ["1", "2", "3"]
    assert_receive {:head, %{target: "/bucket/key?versionId=created-version"}}, 1000
    assert_receive {:upload_result, :ok}, 1000
    assert %{active: 0, bytes: 0, high_requests: 2} = Coordinator.stats(pool)
    record = journal(root)
    assert record["phase"] == "completed"
    assert Enum.all?(record["parts"], &(&1["status"] == "completed"))
    assert Enum.all?(record["parts"], &is_binary(&1["checksums"]["SHA256"]))
    assert Agent.get(signal, & &1) == original_signal
  end

  test "global request and byte admission still bounds an explicitly wider worker pool", %{
    tmp_dir: root
  } do
    pool = start_supervised!({Coordinator, max_requests: 4, memory_budget: 262_144})
    {endpoint, _} = peer(20)
    async_run(root, endpoint, pool, "abcdefghijklmnopqrst", %{parallel: 4})
    assert_receive {:part, a, pa, _}, 1000
    assert_receive {:part, b, pb, _}, 1000
    refute_receive {:part, _, _, _}
    assert %{active: 2, bytes: 262_144, queued: 2} = Coordinator.stats(pool)
    reply(pa, a)
    assert_receive {:part, c, pc, _}, 1000
    reply(pb, b)
    assert_receive {:part, d, pd, _}, 1000
    reply(pc, c)
    reply(pd, d)
    assert_receive {:upload_result, :ok}, 1000
    assert %{active: 0, bytes: 0, high_requests: 2, high_bytes: 262_144} = Coordinator.stats(pool)
  end

  test "legacy default and explicit parallel one keep the serial worker path", %{tmp_dir: root} do
    for flags <- [%{}, %{parallel: 1}] do
      pool = start_supervised!({Coordinator, []}, id: make_ref())
      {endpoint, _} = peer(10)
      async_run(root, endpoint, pool, "abcdefghij", flags)
      assert_receive {:part, 1, p1, _}, 1000
      refute_receive {:part, 2, _, _}
      reply(p1, 1)
      assert_receive {:part, 2, p2, _}, 1000
      reply(p2, 2)
      assert_receive {:upload_result, :ok}, 1000
      assert %{high_requests: 1} = Coordinator.stats(pool)
    end
  end

  test "a failed sibling closes owned requests before abort and never completes", %{tmp_dir: root} do
    pool = start_supervised!(Coordinator)
    {endpoint, _} = peer(10)
    async_run(root, endpoint, pool, "abcdefghij", %{parallel: 2})
    assert_receive {:part, a, pa, _}, 1000
    assert_receive {:part, b, pb, _}, 1000
    send(pa, :hold)
    reply(pb, b, 403)
    assert_receive {:abort, %{target: "/bucket/key?uploadId=owned"}, sockets}, 2000
    assert sockets[pa] == {:error, :closed}

    assert_receive {:upload_result,
                    {:error, %Error{code: "AccessDenied", details: %{cleanup: "aborted"}}}},
                   1000

    assert a != b
    refute_receive {:complete, _}
    assert %{active: 0, bytes: 0} = Coordinator.stats(pool)
    assert journal(root)["phase"] == "failed"
  end

  test "a worker crash closes siblings before abort without losing the journal claimant", %{
    tmp_dir: root
  } do
    pool = start_supervised!(Coordinator)
    {endpoint, _} = peer(10)
    async_run(root, endpoint, pool, "abcdefghij", %{parallel: 2})
    assert_receive {:part, _, pa, _}, 1000
    assert_receive {:part, _, pb, _}, 1000
    send(pa, :hold)
    send(pb, :hold)
    workers = :sys.get_state(pool).held |> Enum.map(fn {_, held} -> elem(held.from, 0) end)
    Process.exit(hd(workers), :kill)
    assert_receive {:abort, _, sockets}, 2000
    assert sockets[pa] == {:error, :closed}
    assert sockets[pb] == {:error, :closed}

    assert_receive {:upload_result,
                    {:error, %Error{code: :part_worker_failed, details: %{cleanup: "aborted"}}}},
                   1000

    refute_receive {:complete, _}
    assert %{active: 0, bytes: 0} = Coordinator.stats(pool)
  end

  test "claimant death shuts down workers and releases request and journal ownership", %{
    tmp_dir: root
  } do
    pool = start_supervised!(Coordinator)
    {endpoint, _} = peer(10)
    {owner, monitor} = async_run(root, endpoint, pool, "abcdefghij", %{parallel: 2})
    assert_receive {:part, _, pa, _}, 1000
    assert_receive {:part, _, pb, _}, 1000
    send(pa, :hold)
    send(pb, :hold)
    workers = :sys.get_state(pool).held |> Enum.map(fn {_, held} -> elem(held.from, 0) end)
    monitors = Enum.map(workers, &{&1, Process.monitor(&1)})
    Process.exit(owner, :kill)
    assert_receive {:DOWN, ^monitor, :process, ^owner, :killed}, 1000
    for {worker, ref} <- monitors, do: assert_receive({:DOWN, ^ref, :process, ^worker, _}, 2000)
    eventually(fn -> Coordinator.stats(pool).active == 0 end)
    record = journal(root)
    assert record["phase"] == "transferring"
    assert {:ok, claim} = Journal.claim(Path.join(root, "journals"), record["id"])
    Journal.release(claim)
    refute_receive {:complete, _}
    refute_receive {:abort, _, _}
    send(pa, :finish)
    send(pb, :finish)
  end

  test "a stopped worker group drains requests and still aborts the owned upload", %{
    tmp_dir: root
  } do
    pool = start_supervised!(Coordinator)
    {endpoint, _} = peer(10)
    async_run(root, endpoint, pool, "abcdefghij", %{parallel: 2})
    assert_receive {:part, _, pa, _}, 1000
    assert_receive {:part, _, pb, _}, 1000
    send(pa, :hold)
    send(pb, :hold)
    worker = :sys.get_state(pool).held |> Enum.find_value(fn {_, held} -> elem(held.from, 0) end)
    {:dictionary, dictionary} = Process.info(worker, :dictionary)
    [_supervisor, group | _] = Keyword.fetch!(dictionary, :"$ancestors")
    GenServer.stop(group, :normal, :infinity)
    assert_receive {:abort, _, sockets}, 2000
    assert sockets[pa] == {:error, :closed}
    assert sockets[pb] == {:error, :closed}

    assert_receive {:upload_result,
                    {:error, %Error{code: :part_worker_failed, details: %{cleanup: "aborted"}}}},
                   1000

    refute_receive {:complete, _}
    assert %{active: 0, bytes: 0} = Coordinator.stats(pool)
  end

  test "borrowed signal cancellation stops workers before cleanup without cancelling the invocation",
       %{tmp_dir: root} do
    pool = start_supervised!(Coordinator)
    signal = HTTP.AbortController.new()
    on_exit(fn -> if Process.alive?(signal), do: Agent.stop(signal) end)
    {endpoint, _} = peer(10)
    async_run(root, endpoint, pool, "abcdefghij", %{parallel: 2}, signal: signal)
    assert_receive {:part, _, pa, _}, 1000
    assert_receive {:part, _, pb, _}, 1000
    send(pa, :hold)
    send(pb, :hold)
    HTTP.AbortController.abort(signal)
    assert_receive {:abort, _, sockets}, 2000
    assert sockets[pa] == {:error, :closed}
    assert sockets[pb] == {:error, :closed}

    assert_receive {:upload_result,
                    {:error, %Error{category: :interrupted, details: %{cleanup: "aborted"}}}},
                   1000

    assert Coordinator.check(pool) == :ok
    assert Process.alive?(signal)
    refute_receive {:complete, _}
  end

  test "mutating a selected source cancels siblings instead of completing", %{tmp_dir: root} do
    pool = start_supervised!(Coordinator)
    {endpoint, _} = peer(10)
    owner = self()

    progress = fn _bytes ->
      send(owner, {:body_producer, self()})
      receive do: (:produce -> :ok)
    end

    async_run(root, endpoint, pool, "abcdefghij", %{parallel: 2}, on_progress: progress)

    producers =
      for _ <- 1..2 do
        assert_receive {:body_producer, producer}, 1000
        monitor = Process.monitor(producer)
        send(producer, :produce)
        {producer, monitor}
      end

    assert_receive {:part, a, pa, _}, 1000
    assert_receive {:part, b, pb, _}, 1000
    send(pa, :hold)

    # Wire bytes can arrive before the producer's final source check. Await
    # normal body completion so mutation exercises post-response validation.
    for {producer, monitor} <- producers,
        do: assert_receive({:DOWN, ^monitor, :process, ^producer, :normal}, 1000)

    File.write!(Path.join(root, "source"), "XXXXXXXXXX")
    reply(pb, b)
    assert_receive {:abort, _, sockets}, 2000
    assert sockets[pa] == {:error, :closed}
    assert_receive {:upload_result, {:error, %Error{code: :source_changed}}}, 1000
    assert File.read!(Path.join(root, "source")) == "XXXXXXXXXX"
    assert a != b
    refute_receive {:complete, _}
  end

  test "request-count admission independently limits a wider worker pool", %{tmp_dir: root} do
    pool = start_supervised!({Coordinator, max_requests: 1})
    {endpoint, _} = peer(15)
    async_run(root, endpoint, pool, "abcdefghijklmno", %{parallel: 3})

    for _ <- 1..3 do
      assert_receive {:part, number, part, _}, 1000
      refute_receive {:part, _, _, _}
      reply(part, number)
    end

    assert_receive {:upload_result, :ok}, 1000
    assert %{active: 0, bytes: 0, high_requests: 1} = Coordinator.stats(pool)
  end

  test "concurrent UploadPart retries keep the same range identity and acknowledge it once", %{
    tmp_dir: root
  } do
    pool = start_supervised!(Coordinator)
    {endpoint, _} = peer(10)

    async_run(root, endpoint, pool, "abcdefghij", %{parallel: 2},
      max_attempts: 2,
      sleep: fn _ -> :ok end
    )

    assert_receive {:part, a, pa, ra}, 1000
    assert_receive {:part, b, pb, rb}, 1000
    peers = %{a => {pa, ra}, b => {pb, rb}}
    {first, request} = peers[1]
    {second, _} = peers[2]
    send(first, {:reply, 503, [], "<Error><Code>SlowDown</Code></Error>"})
    reply(second, 2)
    assert_receive {:part, 1, retried, again}, 1000
    assert again.target == request.target
    assert again.body == request.body
    refute_receive {:complete, _}
    reply(retried, 1)
    assert_receive {:upload_result, :ok}, 1000
    assert length(journal(root)["parts"]) == 2
    assert Enum.all?(journal(root)["parts"], &(&1["status"] == "completed"))
  end

  test "an unavailable part group releases its request without socket I/O" do
    pool = start_supervised!(Coordinator)
    {:ok, lease} = Coordinator.acquire(pool, 131_072)
    {group, monitor} = spawn_monitor(fn -> :ok end)
    assert_receive {:DOWN, ^monitor, :process, ^group, :normal}
    {endpoint, _} = WireServer.fixture([{200, [], "unexpected"}])
    request = %Request{method: "GET", path: "/", query: [], headers: [], body: "", lease: lease}

    assert {:error, %Error{code: :part_group_closed}} =
             HTTPFetch.request(endpoint, request, part_group: group)

    eventually(fn -> Coordinator.stats(pool).active == 0 end)
    refute_receive {:wire_request, _}
  end

  defp eventually(check, remaining \\ 100)

  defp eventually(check, remaining) when remaining > 0 do
    if check.(),
      do: :ok,
      else:
        (
          Process.sleep(10)
          eventually(check, remaining - 1)
        )
  end

  defp eventually(_, 0), do: flunk("owned resources did not stop")
end
